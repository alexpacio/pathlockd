//! Daemon-level stress tests against a real TiKV cluster.
//!
//! Run with `scripts/test-e2e-stress.sh`. The test starts the compiled
//! `pathlockd` binary, hammers it over gRPC, and lets the daemon's normal logical
//! GC + TiKV MVCC GC loops run.

use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::Context;
use pathlockd::proto::{
    path_lock_client::PathLockClient, AcquireRequest, AcquireResponse, AcquireStatus,
    ClearWaitEdgeRequest, EventType, HealthRequest, LockRequest, LockState, Mode,
    ReleaseAllRequest, SetWaitEdgeRequest, SubscribeRequest,
};
use pathlockd::store;
use tikv_client::TransactionClient;
use tokio::net::TcpListener;
use tonic::transport::Channel;
use tonic::Code;

fn pd() -> String {
    std::env::var("PATHLOCKD_PD_ENDPOINTS").unwrap_or_else(|_| "127.0.0.1:2379".to_string())
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

async fn free_port() -> anyhow::Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    Ok(listener.local_addr()?.port())
}

struct Daemon {
    child: Child,
}

impl Daemon {
    fn spawn(port: u16, peers: &[String]) -> anyhow::Result<Self> {
        let bin = env!("CARGO_BIN_EXE_pathlockd");
        let child = Command::new(bin)
            .env("PATHLOCKD_LISTEN", format!("127.0.0.1:{port}"))
            .env("PATHLOCKD_PD_ENDPOINTS", pd())
            .env("PATHLOCKD_PEERS", peers.join(","))
            .env("PATHLOCKD_GC_INTERVAL_SECS", "1")
            .env("PATHLOCKD_GC_PAGE", "64")
            .env("PATHLOCKD_MVCC_GC_INTERVAL_SECS", "1")
            .env("PATHLOCKD_MVCC_GC_SAFE_POINT_RETENTION_SECS", "120")
            .env("PATHLOCKD_REQUEST_TIMEOUT_MS", "30000")
            .env("PATHLOCKD_MAX_CONCURRENT_REQUESTS_PER_CONNECTION", "1024")
            .env("PATHLOCKD_LOG_LEVEL", "pathlockd=debug")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        Ok(Self { child })
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn retry_delay(attempt: u32) -> Duration {
    Duration::from_millis(20 * u64::from((attempt + 1).min(10)))
}

fn retryable_status(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        Code::Unavailable | Code::Cancelled | Code::DeadlineExceeded | Code::ResourceExhausted
    )
}

async fn acquire_with_retry(
    client: &mut PathLockClient<Channel>,
    req: AcquireRequest,
    label: &str,
) -> anyhow::Result<AcquireResponse> {
    let mut attempt = 0;
    loop {
        match client.acquire(req.clone()).await {
            Ok(resp) => return Ok(resp.into_inner()),
            Err(status) if retryable_status(&status) && attempt < 40 => {
                attempt += 1;
                tokio::time::sleep(retry_delay(attempt)).await;
            }
            Err(status) => return Err(status).with_context(|| label.to_string()),
        }
    }
}

async fn set_wait_edge_with_retry(
    client: &mut PathLockClient<Channel>,
    req: SetWaitEdgeRequest,
    label: &str,
) -> anyhow::Result<()> {
    let mut attempt = 0;
    loop {
        match client.set_wait_edge(req.clone()).await {
            Ok(_) => return Ok(()),
            Err(status) if retryable_status(&status) && attempt < 40 => {
                attempt += 1;
                tokio::time::sleep(retry_delay(attempt)).await;
            }
            Err(status) => return Err(status).with_context(|| label.to_string()),
        }
    }
}

async fn clear_wait_edge_with_retry(
    client: &mut PathLockClient<Channel>,
    req: ClearWaitEdgeRequest,
    label: &str,
) -> anyhow::Result<()> {
    let mut attempt = 0;
    loop {
        match client.clear_wait_edge(req.clone()).await {
            Ok(_) => return Ok(()),
            Err(status) if retryable_status(&status) && attempt < 40 => {
                attempt += 1;
                tokio::time::sleep(retry_delay(attempt)).await;
            }
            Err(status) => return Err(status).with_context(|| label.to_string()),
        }
    }
}

async fn release_all_with_retry(
    client: &mut PathLockClient<Channel>,
    req: ReleaseAllRequest,
    label: &str,
) -> anyhow::Result<()> {
    let mut attempt = 0;
    loop {
        match client.release_all(req.clone()).await {
            Ok(_) => return Ok(()),
            Err(status) if retryable_status(&status) && attempt < 40 => {
                attempt += 1;
                tokio::time::sleep(retry_delay(attempt)).await;
            }
            Err(status) => return Err(status).with_context(|| label.to_string()),
        }
    }
}

async fn wait_for_health(endpoint: &str, daemon: &mut Daemon) -> anyhow::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = daemon.child.try_wait()? {
            anyhow::bail!("pathlockd exited before becoming healthy: {status}");
        }

        match PathLockClient::connect(endpoint.to_string()).await {
            Ok(mut client) => {
                if let Ok(resp) = client.health(HealthRequest {}).await {
                    if resp.into_inner().ok {
                        return Ok(());
                    }
                }
            }
            Err(_) => {}
        }

        if Instant::now() >= deadline {
            anyhow::bail!("pathlockd did not become healthy before timeout");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn worker(
    endpoint: String,
    worker_id: usize,
    ops: usize,
    ttl_ms: u64,
    handlers: usize,
) -> anyhow::Result<usize> {
    let mut client = PathLockClient::connect(endpoint).await?;
    let handler = format!("stress{}", worker_id % handlers.max(1));
    for op in 0..ops {
        let owner = format!("stress-{worker_id}-{op}");
        let path = format!("{handler}:/w{worker_id}/bucket{}/leaf{op}", op % 64);
        let resp = acquire_with_retry(
            &mut client,
            AcquireRequest {
                owner_id: owner.clone(),
                ttl_ms,
                requests: vec![LockRequest {
                    path: path.clone(),
                    mode: Mode::Read as i32,
                    state: LockState::New as i32,
                }],
                fencing_token: 0,
                release_requests: vec![],
                emit_release: false,
            },
            &format!("acquire {owner}"),
        )
        .await?;

        if resp.status != AcquireStatus::Ok as i32 {
            anyhow::bail!(
                "unexpected acquire status={} owner={} path={} reason={}",
                resp.status,
                owner,
                resp.path,
                resp.reason
            );
        }

        if op % 4 == 0 {
            set_wait_edge_with_retry(
                &mut client,
                SetWaitEdgeRequest {
                    owner_id: owner.clone(),
                    conflict_owner: format!("blocker-{worker_id}-{op}"),
                    ttl_ms,
                    conflict_path: String::new(),
                    reason: String::new(),
                },
                &format!("set_wait_edge {owner}"),
            )
            .await?;
            clear_wait_edge_with_retry(
                &mut client,
                ClearWaitEdgeRequest {
                    owner_id: owner.clone(),
                },
                &format!("clear_wait_edge {owner}"),
            )
            .await?;
        }

        if op % 5 == 0 {
            release_all_with_retry(
                &mut client,
                ReleaseAllRequest {
                    owner_id: owner,
                    del_wait_key: true,
                },
                "release_all",
            )
            .await?;
        }
    }
    Ok(ops)
}

async fn wait_for_logical_drain(
    client: &TransactionClient,
    endpoints: &[String],
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    let mut health = Vec::with_capacity(endpoints.len());
    for endpoint in endpoints {
        health.push(PathLockClient::connect(endpoint.clone()).await?);
    }

    loop {
        let keys = store::count_all(client).await?;
        if keys == 0 {
            return Ok(());
        }

        for client in &mut health {
            let resp = client.health(HealthRequest {}).await?.into_inner();
            if !resp.ok {
                anyhow::bail!(
                    "daemon unhealthy while waiting for GC drain: {}",
                    resp.detail
                );
            }
        }

        if Instant::now() >= deadline {
            anyhow::bail!("logical keyspace did not drain; {keys} fslock keys remain");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn assert_cross_replica_release_event(endpoints: &[String]) -> anyhow::Result<()> {
    if endpoints.len() < 2 {
        return Ok(());
    }

    let owner = "cross-replica-release-owner";
    let mut subscriber = PathLockClient::connect(endpoints[0].clone()).await?;
    let mut mutator = PathLockClient::connect(endpoints[1].clone()).await?;
    let mut stream = subscriber
        .subscribe(SubscribeRequest {
            owner_id: owner.to_string(),
        })
        .await?
        .into_inner();

    let resp = acquire_with_retry(
        &mut mutator,
        AcquireRequest {
            owner_id: owner.to_string(),
            ttl_ms: 5_000,
            requests: vec![LockRequest {
                path: "events:/cross-replica".to_string(),
                mode: Mode::Read as i32,
                state: LockState::New as i32,
            }],
            fencing_token: 0,
            release_requests: vec![],
            emit_release: false,
        },
        "cross-replica acquire",
    )
    .await?;
    if resp.status != AcquireStatus::Ok as i32 {
        anyhow::bail!(
            "cross-replica acquire failed status={} reason={}",
            resp.status,
            resp.reason
        );
    }

    release_all_with_retry(
        &mut mutator,
        ReleaseAllRequest {
            owner_id: owner.to_string(),
            del_wait_key: true,
        },
        "cross-replica release_all",
    )
    .await?;

    let event = tokio::time::timeout(Duration::from_secs(10), stream.message())
        .await
        .context("timed out waiting for cross-replica release event")??
        .context("cross-replica release event stream closed")?;
    if event.owner_id != owner || event.r#type != EventType::Released as i32 {
        anyhow::bail!(
            "unexpected cross-replica event owner={} type={}",
            event.owner_id,
            event.r#type
        );
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn daemon_gc_survives_short_lived_massive_read_workload() -> anyhow::Result<()> {
    let replicas = env_usize("PATHLOCKD_E2E_STRESS_REPLICAS", 2);
    let workers = env_usize("PATHLOCKD_E2E_STRESS_WORKERS", 8);
    let ops_per_worker = env_usize("PATHLOCKD_E2E_STRESS_OPS_PER_WORKER", 100);
    let handlers = env_usize("PATHLOCKD_E2E_STRESS_HANDLERS", 8);
    let ttl_ms = env_u64("PATHLOCKD_E2E_STRESS_TTL_MS", 250);
    let drain_timeout_secs = env_u64("PATHLOCKD_E2E_STRESS_DRAIN_TIMEOUT_SECS", 60);

    let direct = TransactionClient::new(vec![pd()]).await?;
    store::flush_all(&direct).await?;

    let mut ports = Vec::with_capacity(replicas);
    for _ in 0..replicas {
        ports.push(free_port().await?);
    }
    let endpoints: Vec<String> = ports
        .iter()
        .map(|port| format!("http://127.0.0.1:{port}"))
        .collect();

    let mut daemons = Vec::with_capacity(replicas);
    for (idx, port) in ports.iter().copied().enumerate() {
        let peers: Vec<String> = endpoints
            .iter()
            .enumerate()
            .filter_map(|(peer_idx, endpoint)| (peer_idx != idx).then(|| endpoint.clone()))
            .collect();
        let mut daemon = Daemon::spawn(port, &peers)?;
        let endpoint = endpoints[idx].clone();
        wait_for_health(&endpoint, &mut daemon).await?;
        daemons.push(daemon);
    }
    assert_cross_replica_release_event(&endpoints).await?;

    let mut handles = Vec::with_capacity(workers);
    for worker_id in 0..workers {
        let endpoint = endpoints[worker_id % endpoints.len()].clone();
        handles.push(tokio::spawn(worker(
            endpoint,
            worker_id,
            ops_per_worker,
            ttl_ms,
            handlers,
        )));
    }

    let mut completed = 0usize;
    for handle in handles {
        completed += handle.await??;
    }
    assert_eq!(completed, workers * ops_per_worker);

    tokio::time::sleep(Duration::from_millis(ttl_ms.saturating_mul(2).max(1_000))).await;
    wait_for_logical_drain(&direct, &endpoints, Duration::from_secs(drain_timeout_secs)).await?;

    store::flush_all(&direct).await?;
    Ok(())
}

// ===========================================================================
// Hierarchical contention + deadlock safety/liveness stress
//
// The test above proves GC drains a read-only flood. This one is the adversarial
// counterpart: it hammers the daemon with WRITE/READ locks across a rich path
// hierarchy (ancestors, descendants, siblings, disjoint subtrees, many handlers),
// deliberately manufactures real deadlock cycles, and resolves them with the full
// documented client protocol (wait edges → DetectCycle → cooperative RequestRevoke
// with a preemption claim → ForceRelease). While doing so it continuously checks
// four properties:
//
//   1. SAFETY (mutual exclusion). An in-memory oracle mirrors the documented
//      conflict matrix. A worker enters its critical section ONLY after Acquire
//      returns OK (and AssertFencing confirms it still holds), and leaves the CS
//      BEFORE releasing — so the CS is strictly nested inside the real lock-held
//      interval. Because the hold is microseconds while the lease TTL is seconds,
//      any moment where two owners are simultaneously inside conflicting CSs is a
//      genuine mutual-exclusion bug, not a lease-expiry artifact. Any overlap is
//      recorded as a violation and fails the test.
//
//   2. LIVENESS (no deadlock/livelock/hang of the *system*). Every worker must
//      finish within a hard wall-clock budget; the join is wrapped in a timeout so
//      a wedged cluster fails loudly instead of hanging. Consistent victim
//      selection (max owner id in the cycle) guarantees the manufactured deadlocks
//      always break and the workload terminates.
//
//   3. DEADLOCKS ARE ACTUALLY EXERCISED. DetectCycle must report cycles a non-zero
//      number of times, proving the resolution path really ran.
//
//   4. NO STATE POISONING OVER TIME. After the storm, all transient lock state
//      must drain to zero (lazy expiry + GC), only the by-design long-lived fence
//      tombstones may remain, those are bounded by the fixed path universe and do
//      not grow, fencing tokens stay strictly monotonic, and the cluster still
//      grants fresh locks afterward.
// ===========================================================================

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use pathlockd::engine;
use pathlockd::proto::{
    AssertFencingRequest, AssertStatus, CycleKind, DetectCycleRequest, ForceReleaseRequest,
    IncrFencingTokenRequest, IsBlockingRequest, IsOwnerAliveRequest, RenewRequest, RenewStatus,
    RequestRevokeRequest,
};

const R: Ordering = Ordering::Relaxed;

/// A small, dependency-free PRNG so the workload is varied yet reproducible
/// without pulling in `rand`.
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Capped, jittered client-side backoff for the contention/resolution loops.
async fn jittered_backoff(attempt: u32) {
    static CTR: AtomicU64 = AtomicU64::new(0x1234_5678_9abc_def0);
    // Gentle start so a fast-releasing blocker is re-checked within a few ms,
    // capped low so a long wait still polls several times a second.
    let base = (5u64.wrapping_mul(1u64 << attempt.min(4))).min(120);
    let seed = CTR.fetch_add(0x9E37_79B9_7F4A_7C15, R)
        ^ std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos() as u64)
            .unwrap_or(0);
    let jitter = splitmix64(seed) % (base + 1);
    tokio::time::sleep(Duration::from_millis(base / 2 + jitter)).await;
}

/// Backoff for a deadlock victim that just yielded. It must clearly exceed a
/// waiting peer's immediate re-acquire (which polls every ≤120ms) so the freed
/// resource is taken by the waiter, not re-grabbed by us — otherwise the same
/// ring reforms (livelock). Grows with the attempt so it is guaranteed to win
/// out even when load slows the waiter's acquire.
async fn victim_backoff(attempt: u32) {
    static CTR: AtomicU64 = AtomicU64::new(0xC0FFEE_1234_5678);
    let base = (200u64.wrapping_mul(1u64 << attempt.min(3))).min(1200); // 200,400,800,1200
    let seed = CTR.fetch_add(0x9E37_79B9_7F4A_7C15, R)
        ^ std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos() as u64)
            .unwrap_or(0);
    let jitter = splitmix64(seed) % (base / 2 + 1);
    tokio::time::sleep(Duration::from_millis(base + jitter)).await;
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum CsMode {
    Read,
    Write,
}

impl CsMode {
    fn proto(self) -> Mode {
        match self {
            CsMode::Read => Mode::Read,
            CsMode::Write => Mode::Write,
        }
    }
}

fn lreq(path: &str, m: CsMode) -> LockRequest {
    LockRequest {
        path: path.to_string(),
        mode: m.proto() as i32,
        state: LockState::New as i32,
    }
}

/// `a` is an ancestor of (or equal to) `b`, computed per handler. Reuses the
/// production ancestor walk so the oracle can't disagree with the engine about
/// what "contains" means.
fn anc_or_eq(a: &str, b: &str) -> bool {
    a == b || engine::get_ancestors(b).iter().any(|x| x == a)
}

/// The documented conflict rule: two locks from different owners conflict iff at
/// least one is a write and their claimed regions intersect, where a write claims
/// the whole subtree rooted at its path and a read claims only its point.
fn regions_conflict(p1: &str, m1: CsMode, p2: &str, m2: CsMode) -> bool {
    match (m1, m2) {
        (CsMode::Read, CsMode::Read) => false,
        (CsMode::Write, CsMode::Write) => anc_or_eq(p1, p2) || anc_or_eq(p2, p1),
        (CsMode::Write, CsMode::Read) => anc_or_eq(p1, p2), // read point inside write subtree
        (CsMode::Read, CsMode::Write) => anc_or_eq(p2, p1),
    }
}

struct Holder {
    owner: String,
    mode: CsMode,
    token: i64,
}

/// Mutual-exclusion oracle: which owners are *currently inside a critical
/// section* on each path. Entering checks the new locks against every other
/// owner already inside; an overlap is a safety violation.
#[derive(Default)]
struct Oracle {
    held: HashMap<String, Vec<Holder>>,
    violations: Vec<String>,
    enters: u64,
    current: usize,
    peak: usize,
}

impl Oracle {
    fn enter(&mut self, owner: &str, token: i64, locks: &[(String, CsMode)]) {
        for (p, m) in locks {
            for (hp, holders) in &self.held {
                for h in holders {
                    if h.owner == owner {
                        continue; // an owner never conflicts with itself
                    }
                    if regions_conflict(p, *m, hp, h.mode) {
                        self.violations.push(format!(
                            "MUTEX VIOLATION: {owner} entered CS {:?} {p} (token {token}) while {} already in CS {:?} {hp} (token {})",
                            m, h.owner, h.mode, h.token
                        ));
                    }
                }
            }
        }
        for (p, m) in locks {
            self.held.entry(p.clone()).or_default().push(Holder {
                owner: owner.to_string(),
                mode: *m,
                token,
            });
        }
        self.enters += 1;
        self.current += 1;
        self.peak = self.peak.max(self.current);
    }

    fn exit(&mut self, owner: &str, locks: &[(String, CsMode)]) {
        for (p, _) in locks {
            if let Some(v) = self.held.get_mut(p) {
                if let Some(i) = v.iter().position(|h| h.owner == owner) {
                    v.remove(i);
                }
                if v.is_empty() {
                    self.held.remove(p);
                }
            }
        }
        self.current = self.current.saturating_sub(1);
    }
}

/// Shared run-wide state: the safety oracle plus instrumentation counters.
struct World {
    oracle: Mutex<Oracle>,
    /// Detail of every AssertFencing failure (owner/path/reason/token), bounded,
    /// so a non-zero `assert_fail` can be root-caused instead of just counted.
    assert_fail_details: Mutex<Vec<String>>,
    /// Per-worker liveness heartbeat: owner → (phase, last-update). Lets a
    /// liveness timeout name exactly which workers are stuck and in what phase.
    heartbeat: Mutex<HashMap<String, (String, Instant)>>,
    ops_ok: AtomicU64,
    cs_entered: AtomicU64,
    acquires: AtomicU64,
    conflicts: AtomicU64,
    lost: AtomicU64,
    deadlocks: AtomicU64,
    revokes: AtomicU64,
    force_releases: AtomicU64,
    self_yields: AtomicU64,
    revoke_events: AtomicU64,
    kill_events: AtomicU64,
    assert_ok: AtomicU64,
    assert_fail: AtomicU64,
    token_anomalies: AtomicU64,
    min_token: AtomicI64,
    max_token: AtomicI64,
}

impl World {
    fn new() -> Self {
        World {
            oracle: Mutex::new(Oracle::default()),
            assert_fail_details: Mutex::new(Vec::new()),
            heartbeat: Mutex::new(HashMap::new()),
            ops_ok: AtomicU64::new(0),
            cs_entered: AtomicU64::new(0),
            acquires: AtomicU64::new(0),
            conflicts: AtomicU64::new(0),
            lost: AtomicU64::new(0),
            deadlocks: AtomicU64::new(0),
            revokes: AtomicU64::new(0),
            force_releases: AtomicU64::new(0),
            self_yields: AtomicU64::new(0),
            revoke_events: AtomicU64::new(0),
            kill_events: AtomicU64::new(0),
            assert_ok: AtomicU64::new(0),
            assert_fail: AtomicU64::new(0),
            token_anomalies: AtomicU64::new(0),
            min_token: AtomicI64::new(i64::MAX),
            max_token: AtomicI64::new(0),
        }
    }

    /// Record an issued fencing token and flag any monotonicity anomaly (a
    /// non-positive token, or one that did not strictly advance within a worker).
    fn record_token(&self, token: i64, last_local: &mut i64) {
        if token <= 0 || (*last_local != 0 && token <= *last_local) {
            self.token_anomalies.fetch_add(1, R);
        }
        *last_local = token;
        self.min_token.fetch_min(token, R);
        self.max_token.fetch_max(token, R);
    }

    fn note_assert_fail(&self, detail: String) {
        self.assert_fail.fetch_add(1, R);
        let mut v = self.assert_fail_details.lock().unwrap();
        if v.len() < 50 {
            v.push(detail);
        }
    }

    fn beat(&self, owner: &str, phase: &str) {
        self.heartbeat
            .lock()
            .unwrap()
            .insert(owner.to_string(), (phase.to_string(), Instant::now()));
    }

    /// Workers whose heartbeat is older than `idle` — the ones stuck during a
    /// liveness timeout — as "owner[phase, Ns idle]" lines.
    fn stuck_report(&self, idle: Duration) -> String {
        let hb = self.heartbeat.lock().unwrap();
        let now = Instant::now();
        let mut lines: Vec<String> = hb
            .iter()
            .filter(|(_, (_, t))| now.duration_since(*t) > idle)
            .map(|(o, (p, t))| format!("  {o} [{p}, {}s idle]", now.duration_since(*t).as_secs()))
            .collect();
        lines.sort();
        if lines.is_empty() {
            "  (no workers idle beyond threshold)".to_string()
        } else {
            lines.join("\n")
        }
    }

    fn summary(&self) -> String {
        format!(
            "ops_ok={} cs={} acquires={} conflicts={} lost={} deadlocks={} revokes={} force_releases={} self_yields={} revoke_evt={} kill_evt={} assert_ok={} assert_fail={} tokens=[{}..{}]",
            self.ops_ok.load(R), self.cs_entered.load(R), self.acquires.load(R), self.conflicts.load(R),
            self.lost.load(R), self.deadlocks.load(R), self.revokes.load(R), self.force_releases.load(R),
            self.self_yields.load(R), self.revoke_events.load(R), self.kill_events.load(R),
            self.assert_ok.load(R), self.assert_fail.load(R), self.min_token.load(R), self.max_token.load(R)
        )
    }

    /// Summary plus any captured AssertFencing-failure detail — used in failure
    /// messages so even a liveness timeout surfaces the assert reasons.
    fn report(&self) -> String {
        let mut s = self.summary();
        let details = self.assert_fail_details.lock().unwrap();
        if !details.is_empty() {
            s.push_str(&format!(
                "\nassert_fail details ({}):\n{}",
                details.len(),
                details.join("\n")
            ));
        }
        s
    }
}

/// Latched preemption signals delivered to a worker over its own event stream.
#[derive(Default)]
struct OwnerFlags {
    killed: AtomicBool,
    revoked: AtomicBool,
}

impl OwnerFlags {
    fn note_killed(&self) {
        self.killed.store(true, Ordering::SeqCst);
    }
    fn note_revoked(&self) {
        self.revoked.store(true, Ordering::SeqCst);
    }
    /// Consume any pending revoke/kill signal (so acting on it once does not loop).
    fn take_yield_signal(&self) -> bool {
        let k = self.killed.swap(false, Ordering::SeqCst);
        let r = self.revoked.swap(false, Ordering::SeqCst);
        k || r
    }
}

/// One acquired set the worker wants to hold simultaneously. `groups` are the
/// separate Acquire calls (one path each for the deadlock workers, so they hold
/// the first while blocking on the next — a real hold-and-wait); `cs` is the
/// union of held (path, mode) pairs handed to the oracle.
struct Plan {
    groups: Vec<Vec<LockRequest>>,
    cs: Vec<(String, CsMode)>,
}

// --- control-plane RPC helpers (bounded transient retry, like acquire_with_retry) ---

async fn incr_token(c: &mut PathLockClient<Channel>) -> anyhow::Result<i64> {
    let mut attempt = 0;
    loop {
        match c.incr_fencing_token(IncrFencingTokenRequest {}).await {
            Ok(r) => return Ok(r.into_inner().token),
            Err(s) if retryable_status(&s) && attempt < 40 => {
                attempt += 1;
                tokio::time::sleep(retry_delay(attempt)).await;
            }
            Err(s) => return Err(s).context("incr_fencing_token"),
        }
    }
}

async fn detect_cycle(
    c: &mut PathLockClient<Channel>,
    owner: &str,
) -> anyhow::Result<(i32, Vec<String>)> {
    let mut attempt = 0;
    loop {
        match c
            .detect_cycle(DetectCycleRequest {
                start_owner_id: owner.to_string(),
                max_depth: 64,
            })
            .await
        {
            Ok(r) => {
                let r = r.into_inner();
                return Ok((r.kind, r.chain));
            }
            Err(s) if retryable_status(&s) && attempt < 40 => {
                attempt += 1;
                tokio::time::sleep(retry_delay(attempt)).await;
            }
            Err(s) => return Err(s).context("detect_cycle"),
        }
    }
}

async fn is_blocking(
    c: &mut PathLockClient<Channel>,
    path: &str,
    owner: &str,
    reason: &str,
) -> anyhow::Result<bool> {
    let mut attempt = 0;
    loop {
        match c
            .is_blocking(IsBlockingRequest {
                conflict_path: path.to_string(),
                conflict_owner: owner.to_string(),
                reason: reason.to_string(),
            })
            .await
        {
            Ok(r) => return Ok(r.into_inner().blocking),
            Err(s) if retryable_status(&s) && attempt < 40 => {
                attempt += 1;
                tokio::time::sleep(retry_delay(attempt)).await;
            }
            Err(s) => return Err(s).context("is_blocking"),
        }
    }
}

async fn request_revoke(
    c: &mut PathLockClient<Channel>,
    victim: &str,
    claim_path: &str,
    claimant: &str,
    claim_ttl_ms: u64,
) -> anyhow::Result<()> {
    let mut attempt = 0;
    loop {
        match c
            .request_revoke(RequestRevokeRequest {
                owner_id: victim.to_string(),
                claim_path: claim_path.to_string(),
                claimant_owner_id: claimant.to_string(),
                claim_ttl_ms,
            })
            .await
        {
            Ok(_) => return Ok(()),
            Err(s) if retryable_status(&s) && attempt < 40 => {
                attempt += 1;
                tokio::time::sleep(retry_delay(attempt)).await;
            }
            Err(s) => return Err(s).context("request_revoke"),
        }
    }
}

async fn force_release(c: &mut PathLockClient<Channel>, victim: &str) -> anyhow::Result<()> {
    let mut attempt = 0;
    loop {
        match c
            .force_release(ForceReleaseRequest {
                victim_id: victim.to_string(),
            })
            .await
        {
            Ok(_) => return Ok(()),
            Err(s) if retryable_status(&s) && attempt < 40 => {
                attempt += 1;
                tokio::time::sleep(retry_delay(attempt)).await;
            }
            Err(s) => return Err(s).context("force_release"),
        }
    }
}

/// Returns `None` when the owner still holds every path at `token`, or
/// `Some((path, reason))` for the first path that no longer asserts.
async fn assert_fencing(
    c: &mut PathLockClient<Channel>,
    owner: &str,
    token: i64,
    paths: &[String],
) -> anyhow::Result<Option<(String, String)>> {
    let mut attempt = 0;
    loop {
        match c
            .assert_fencing(AssertFencingRequest {
                owner_id: owner.to_string(),
                fencing_token: token,
                paths: paths.to_vec(),
            })
            .await
        {
            Ok(r) => {
                let r = r.into_inner();
                return Ok(if r.status == AssertStatus::Ok as i32 {
                    None
                } else {
                    Some((r.path, r.reason))
                });
            }
            Err(s) if retryable_status(&s) && attempt < 40 => {
                attempt += 1;
                tokio::time::sleep(retry_delay(attempt)).await;
            }
            Err(s) => return Err(s).context("assert_fencing"),
        }
    }
}

async fn renew(c: &mut PathLockClient<Channel>, owner: &str, ttl_ms: u64) -> anyhow::Result<i32> {
    let mut attempt = 0;
    loop {
        match c
            .renew(RenewRequest {
                owner_id: owner.to_string(),
                ttl_ms,
            })
            .await
        {
            Ok(r) => return Ok(r.into_inner().status),
            Err(s) if retryable_status(&s) && attempt < 40 => {
                attempt += 1;
                tokio::time::sleep(retry_delay(attempt)).await;
            }
            Err(s) => return Err(s).context("renew"),
        }
    }
}

async fn is_owner_alive(c: &mut PathLockClient<Channel>, owner: &str) -> anyhow::Result<bool> {
    let mut attempt = 0;
    loop {
        match c
            .is_owner_alive(IsOwnerAliveRequest {
                owner_id: owner.to_string(),
            })
            .await
        {
            Ok(r) => return Ok(r.into_inner().alive),
            Err(s) if retryable_status(&s) && attempt < 40 => {
                attempt += 1;
                tokio::time::sleep(retry_delay(attempt)).await;
            }
            Err(s) => return Err(s).context("is_owner_alive"),
        }
    }
}

async fn release_all(c: &mut PathLockClient<Channel>, owner: &str, label: &str) {
    let _ = release_all_with_retry(
        c,
        ReleaseAllRequest {
            owner_id: owner.to_string(),
            del_wait_key: true,
        },
        label,
    )
    .await;
}

/// Bring up `replicas` peered daemons (debug + fast GC enabled) and wait until
/// each is healthy. Mirrors the wiring of the read-stress test above.
async fn spawn_cluster(replicas: usize) -> anyhow::Result<(Vec<Daemon>, Vec<String>)> {
    let mut ports = Vec::with_capacity(replicas);
    for _ in 0..replicas {
        ports.push(free_port().await?);
    }
    let endpoints: Vec<String> = ports
        .iter()
        .map(|port| format!("http://127.0.0.1:{port}"))
        .collect();
    let mut daemons = Vec::with_capacity(replicas);
    for (idx, port) in ports.iter().copied().enumerate() {
        let peers: Vec<String> = endpoints
            .iter()
            .enumerate()
            .filter_map(|(peer_idx, ep)| (peer_idx != idx).then(|| ep.clone()))
            .collect();
        let mut daemon = Daemon::spawn(port, &peers)?;
        wait_for_health(&endpoints[idx], &mut daemon).await?;
        daemons.push(daemon);
    }
    Ok((daemons, endpoints))
}

/// Subscribe to one owner's event stream and latch REVOKE/KILLED into `flags`.
/// Reconnects on stream end; the caller aborts the handle when the worker is done.
fn spawn_subscription(
    endpoint: String,
    owner: String,
    flags: Arc<OwnerFlags>,
    world: Arc<World>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let mut client = match PathLockClient::connect(endpoint.clone()).await {
                Ok(c) => c,
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
            };
            let mut stream = match client
                .subscribe(SubscribeRequest {
                    owner_id: owner.clone(),
                })
                .await
            {
                Ok(s) => s.into_inner(),
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
            };
            loop {
                match stream.message().await {
                    Ok(Some(ev)) => match EventType::try_from(ev.r#type) {
                        Ok(EventType::Revoke) => {
                            flags.note_revoked();
                            world.revoke_events.fetch_add(1, R);
                        }
                        Ok(EventType::Killed) => {
                            flags.note_killed();
                            world.kill_events.fetch_add(1, R);
                        }
                        _ => {}
                    },
                    Ok(None) | Err(_) => break,
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
}

enum Acq {
    Ok,
    Yield,
    Lost,
}

/// Acquire one group, blocking politely on CONFLICT with the full deadlock
/// protocol: wait edge (with conflict metadata) → IsBlocking recheck →
/// DetectCycle → resolve. Returns `Yield` if we were the chosen victim or were
/// preempted, `Lost` if a held key vanished (caller re-mints its token).
#[allow(clippy::too_many_arguments)]
async fn acquire_group_blocking(
    lk: &mut PathLockClient<Channel>,
    owner: &str,
    token: i64,
    group: &[LockRequest],
    ttl_ms: u64,
    world: &World,
    flags: &OwnerFlags,
    deadline: Instant,
    // True only when the owner ALREADY holds locks while blocking on this group
    // (e.g. a deadlock member holding its first resource while reaching for the
    // next). Then we must renew to keep them alive, and a LOST renew means we were
    // force-released. When false (a first/sole acquire that holds nothing yet),
    // renewing is meaningless — the owner has no alive key, so renew would always
    // report LOST — so we skip it entirely.
    holds_locks: bool,
) -> anyhow::Result<Acq> {
    loop {
        if Instant::now() > deadline {
            anyhow::bail!("acquire deadline exceeded for owner {owner}");
        }
        if flags.take_yield_signal() {
            return Ok(Acq::Yield);
        }
        world.acquires.fetch_add(1, R);
        let resp = acquire_with_retry(
            lk,
            AcquireRequest {
                owner_id: owner.to_string(),
                ttl_ms,
                requests: group.to_vec(),
                fencing_token: token,
                release_requests: vec![],
                emit_release: false,
            },
            &format!("acquire {owner}"),
        )
        .await?;

        if resp.status == AcquireStatus::Ok as i32 {
            return Ok(Acq::Ok);
        }
        if resp.status == AcquireStatus::Lost as i32 {
            world.lost.fetch_add(1, R);
            return Ok(Acq::Lost);
        }
        // CONFLICT
        world.conflicts.fetch_add(1, R);
        let cp = resp.path;
        let blocker = resp.owner;
        let reason = resp.reason;
        // A stale token isn't a "wait on a held lock" condition; re-mint and retry.
        if reason == "stale_fencing_token" {
            world.lost.fetch_add(1, R);
            return Ok(Acq::Lost);
        }

        set_wait_edge_with_retry(
            lk,
            SetWaitEdgeRequest {
                owner_id: owner.to_string(),
                conflict_owner: blocker.clone(),
                ttl_ms,
                conflict_path: cp.clone(),
                reason: reason.clone(),
            },
            &format!("set_wait_edge {owner}"),
        )
        .await?;

        let mut attempt = 1u32;
        let mut last_renew = Instant::now();
        let wait_started = Instant::now();
        let mut last_detect = Instant::now();
        loop {
            if Instant::now() > deadline {
                anyhow::bail!("wait deadline exceeded for owner {owner} on {cp}");
            }
            world.beat(owner, &format!("wait:{reason}@{cp}"));
            if flags.take_yield_signal() {
                let _ = clear_wait_edge_with_retry(
                    lk,
                    ClearWaitEdgeRequest {
                        owner_id: owner.to_string(),
                    },
                    "clear on yield",
                )
                .await;
                return Ok(Acq::Yield);
            }
            if !is_blocking(lk, &cp, &blocker, &reason).await? {
                break; // blocker let go → retry the acquire
            }
            // Escalate to the (expensive, multi-read) cycle walk only after we have
            // actually been stuck a beat, and then at a throttled rate. Most
            // contention clears via the cheap is_blocking recheck; a storm of
            // DetectCycle walks from many stuck waiters is what overloads a small
            // cluster (and starves the renew/clear path → spurious lease loss).
            if wait_started.elapsed() > Duration::from_millis(200)
                && last_detect.elapsed() > Duration::from_millis(200)
            {
                last_detect = Instant::now();
                let (kind, chain) = detect_cycle(lk, owner).await?;
                if kind == CycleKind::Found as i32 {
                    world.deadlocks.fetch_add(1, R);
                    if resolve_deadlock(lk, owner, &chain, &cp, &blocker, &reason, world).await? {
                        let _ = clear_wait_edge_with_retry(
                            lk,
                            ClearWaitEdgeRequest {
                                owner_id: owner.to_string(),
                            },
                            "clear on self-yield",
                        )
                        .await;
                        return Ok(Acq::Yield);
                    }
                }
            }
            // Periodically keep our wait edge (and, if we hold locks, our lease)
            // alive across a long wait. Refreshing the wait edge is essential:
            // SetWaitEdge carries the lease TTL, and if it lapsed while we (and our
            // blockers) are still deadlocked, the wait-for graph would lose this
            // edge and DetectCycle could no longer see the cycle — the deadlock
            // would become permanent and undetectable. The lease renew is gated on
            // `holds_locks`: a holds-nothing first acquire has no alive key, so
            // renewing it would always report LOST (a false signal). When we DO
            // hold locks, a LOST renew means we were force-released → restart.
            if last_renew.elapsed() > Duration::from_millis((ttl_ms / 3).max(1)) {
                if holds_locks && renew(lk, owner, ttl_ms).await? == RenewStatus::Lost as i32 {
                    world.lost.fetch_add(1, R);
                    let _ = clear_wait_edge_with_retry(
                        lk,
                        ClearWaitEdgeRequest {
                            owner_id: owner.to_string(),
                        },
                        "clear on renew-lost",
                    )
                    .await;
                    return Ok(Acq::Lost);
                }
                let _ = set_wait_edge_with_retry(
                    lk,
                    SetWaitEdgeRequest {
                        owner_id: owner.to_string(),
                        conflict_owner: blocker.clone(),
                        ttl_ms,
                        conflict_path: cp.clone(),
                        reason: reason.clone(),
                    },
                    "refresh_wait_edge",
                )
                .await;
                last_renew = Instant::now();
            }
            jittered_backoff(attempt).await;
            attempt = attempt.saturating_add(1);
        }
    }
}

/// Resolve a detected cycle. Victim = the max owner id in the chain, chosen
/// identically by every participant so the cycle always breaks. Returns `true`
/// iff *we* were the victim and yielded everything.
#[allow(clippy::too_many_arguments)]
async fn resolve_deadlock(
    lk: &mut PathLockClient<Channel>,
    owner: &str,
    chain: &[String],
    cp: &str,
    blocker: &str,
    reason: &str,
    world: &World,
) -> anyhow::Result<bool> {
    let victim = match chain.iter().max() {
        Some(v) => v.clone(),
        None => return Ok(false),
    };
    if victim == owner {
        // We are the agreed victim: drop everything to break the cycle. The
        // caller backs off (asymmetrically longer than a waiting peer's poll —
        // see victim_backoff) before retrying, so the freed resource goes to the
        // waiter and the ring cannot reform (livelock).
        world.self_yields.fetch_add(1, R);
        release_all(lk, owner, "victim self-yield").await;
        return Ok(true);
    }
    // Cooperative first: ask the victim to yield. We deliberately do NOT plant a
    // preemption claim here: in a symmetric ring every resource is one member's
    // hold and another's want, so a claim on a want-path would block the holder
    // from re-acquiring it — a tangle. Pure revoke + self-yield + force-release is
    // a complete resolution. (The claim/preempt path is covered separately by
    // `preemption_claim_blocks_victim_reacquire`.)
    world.revokes.fetch_add(1, R);
    request_revoke(lk, &victim, "", "", 0).await?;
    // The chosen victim self-yields the instant its own DetectCycle fires, so the
    // grace loop usually exits early; keep it short and escalate to force if not.
    let grace = Instant::now() + Duration::from_millis(300);
    while Instant::now() < grace {
        if !is_blocking(lk, cp, blocker, reason).await? {
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    if is_blocking(lk, cp, blocker, reason).await? && is_owner_alive(lk, &victim).await? {
        world.force_releases.fetch_add(1, R);
        force_release(lk, &victim).await?;
    }
    Ok(false)
}

/// One full operation: acquire every group (re-minting the token and restarting
/// on yield/lost), then run the oracle-checked critical section, then release.
#[allow(clippy::too_many_arguments)]
async fn run_op(
    lk: &mut PathLockClient<Channel>,
    owner: &str,
    plan: &Plan,
    ttl_ms: u64,
    world: &World,
    flags: &OwnerFlags,
    deadline: Instant,
    last_token: &mut i64,
    inter_group_delay_ms: u64,
) -> anyhow::Result<()> {
    let mut attempt = 0u32;
    'attempt: loop {
        if Instant::now() > deadline {
            anyhow::bail!("op deadline exceeded for owner {owner}");
        }
        attempt += 1;
        let _ = flags.take_yield_signal(); // start the attempt clean
        world.beat(owner, "mint-token");
        let token = incr_token(lk).await?;
        world.record_token(token, last_token);

        let n = plan.groups.len();
        for (gi, group) in plan.groups.iter().enumerate() {
            world.beat(owner, &format!("acquire-g{gi}"));
            // `holds_locks` = we already acquired an earlier group in this op
            // (so blocking now must keep those alive). The first group holds
            // nothing yet.
            let holds_locks = gi > 0;
            match acquire_group_blocking(
                lk,
                owner,
                token,
                group,
                ttl_ms,
                world,
                flags,
                deadline,
                holds_locks,
            )
            .await?
            {
                Acq::Ok => {}
                Acq::Yield | Acq::Lost => {
                    release_all(lk, owner, "restart release").await;
                    jittered_backoff(attempt).await;
                    continue 'attempt;
                }
            }
            // Hold what we have for a beat so the other cycle members can grab
            // theirs — this is what forces the manufactured deadlock to form.
            if inter_group_delay_ms > 0 && gi + 1 < n {
                let j = splitmix64(token as u64 ^ gi as u64) % (inter_group_delay_ms + 1);
                tokio::time::sleep(Duration::from_millis(inter_group_delay_ms / 2 + j)).await;
            }
        }

        // Refresh the lease right before the critical section. A real client
        // renews before a fenced operation; here it also closes the window where a
        // slow clear/assert under load could let the lease lapse between acquire
        // and the CS (which would otherwise surface as a spurious AssertFencing
        // failure). A LOST renew means we genuinely lost the lock — restart.
        if renew(lk, owner, ttl_ms).await? == RenewStatus::Lost as i32 {
            world.lost.fetch_add(1, R);
            release_all(lk, owner, "pre-cs renew lost").await;
            jittered_backoff(attempt).await;
            continue 'attempt;
        }
        // Close the wait-edge window BEFORE the CS so we can never be seen as a
        // cycle node (and thus a victim) while inside it.
        let _ = clear_wait_edge_with_retry(
            lk,
            ClearWaitEdgeRequest {
                owner_id: owner.to_string(),
            },
            "clear before cs",
        )
        .await;

        // Fence-verify every write path immediately before the critical section.
        let write_paths: Vec<String> = plan
            .cs
            .iter()
            .filter(|(_, m)| *m == CsMode::Write)
            .map(|(p, _)| p.clone())
            .collect();
        if !write_paths.is_empty() {
            match assert_fencing(lk, owner, token, &write_paths).await? {
                None => {
                    world.assert_ok.fetch_add(1, R);
                }
                Some((path, reason)) => {
                    world.note_assert_fail(format!(
                        "owner={owner} path={path} reason={reason} token={token} held={write_paths:?}"
                    ));
                    release_all(lk, owner, "assert-fail release").await;
                    jittered_backoff(attempt).await;
                    continue 'attempt;
                }
            }
        }
        if flags.take_yield_signal() {
            release_all(lk, owner, "preempt release").await;
            jittered_backoff(attempt).await;
            continue 'attempt;
        }

        // ---- critical section: enter AFTER OK, exit BEFORE release ----
        world.beat(owner, "cs");
        world.oracle.lock().unwrap().enter(owner, token, &plan.cs);
        world.cs_entered.fetch_add(1, R);
        // Hold for microseconds — orders of magnitude under the lease TTL, so the
        // CS is provably nested inside the real lock-held interval.
        let hold_us = splitmix64(token as u64 ^ 0x5151) % 2500;
        if hold_us > 0 {
            tokio::time::sleep(Duration::from_micros(hold_us)).await;
        }
        world.oracle.lock().unwrap().exit(owner, &plan.cs);

        world.beat(owner, "release");
        release_all(lk, owner, "op release").await;
        world.ops_ok.fetch_add(1, R);
        return Ok(());
    }
}

fn work_path(h: usize, seed: u64) -> String {
    let d = splitmix64(seed) % 3;
    // Bias toward deeper (leaf) nodes: a write on a shallow node is subtree-
    // exclusive and can starve behind a stream of descendant lockers. Leaf-heavy
    // contention keeps conflicts mostly point-exact and fast to clear.
    let depth = match splitmix64(seed ^ 0xaa) % 10 {
        0..=5 => 2,
        6..=7 => 1,
        _ => 0,
    };
    let s = splitmix64(seed ^ 0xbb) % 3;
    let f = splitmix64(seed ^ 0xcc) % 4;
    // Contention lives in its own `w*` handler namespace, disjoint from the
    // deadlock groups' `d*` handlers — so the two never share a per-handler
    // serialization key (which would otherwise make them collide and starve).
    match depth {
        0 => format!("w{h}:/work/d{d}"),
        1 => format!("w{h}:/work/d{d}/s{s}"),
        _ => format!("w{h}:/work/d{d}/s{s}/f{f}"),
    }
}

/// A random contention op over the shared `/work` hierarchy: 1–2 paths at random
/// depths and modes (write-biased), acquired atomically in one group.
fn contention_plan(handlers: usize, seed: u64) -> Plan {
    let h = (splitmix64(seed) as usize) % handlers;
    let n = if splitmix64(seed ^ 0x11) % 3 == 0 {
        2
    } else {
        1
    };
    let mut group = Vec::new();
    let mut cs = Vec::new();
    let mut used = std::collections::HashSet::new();
    for k in 0..n {
        let s = seed ^ (0x100u64.wrapping_mul(k as u64 + 1));
        let path = work_path(h, s);
        if !used.insert(path.clone()) {
            continue;
        }
        let m = if splitmix64(s ^ 0x55) % 2 == 0 {
            CsMode::Write
        } else {
            CsMode::Read
        };
        group.push(lreq(&path, m));
        cs.push((path, m));
    }
    Plan {
        groups: vec![group],
        cs,
    }
}

/// The (hold, want) write paths for one member of a manufactured K-cycle. Each
/// member holds its own resource and wants the next member's, in a rotation. The
/// shape varies which hierarchical conflict reason drives the ring:
/// write_locked / ancestor_locked / descendant_write_locked.
fn deadlock_paths(
    group: usize,
    member: usize,
    round: usize,
    k: usize,
    shape: u8,
) -> (String, String) {
    // Each group gets its OWN handler `d{group}` (its own serialization domain),
    // disjoint from the contention `w*` handlers and from every other group — so
    // groups never collide with each other or with contention on a shared
    // serialization key. Each round additionally gets its own subtree (n{round}),
    // so a member racing ahead can't entangle with siblings finishing the prior
    // round; the per-round ring stays clean and DetectCycle-able.
    let base = format!("d{group}:/dl/n{round}");
    let hold_i = member;
    let want_i = (member + 1) % k;
    match shape {
        0 => (format!("{base}/r{hold_i}"), format!("{base}/r{want_i}")),
        1 => (format!("{base}/r{hold_i}"), format!("{base}/r{want_i}/x/y")),
        _ => (
            format!("{base}/r{hold_i}/leaf"),
            format!("{base}/r{want_i}"),
        ),
    }
}

/// One round of a manufactured K-cycle, synchronized by TWO per-round barriers:
///
///   b_start — waited at the top holding NOTHING, so a slow sibling (e.g. a
///             victim still backing off from the previous round) makes peers wait
///             with no lease to lapse. This is what keeps the long inter-round
///             wait from expiring a held lock.
///   b_g1    — waited right after each member grabs its own resource (a fast,
///             always-free, round-private acquire), so by the time anyone reaches
///             for the next member's resource, all K hold theirs → a full
///             wait-for ring that DetectCycle must find and the protocol resolve.
///
/// While blocked on the next resource the worker renews (see the wait loop), so
/// no lock is ever held across an un-renewed wait. On a victim self-yield / lock
/// loss the round restarts WITHOUT re-waiting either (single-use) barrier.
#[allow(clippy::too_many_arguments)]
async fn run_deadlock_round(
    lk: &mut PathLockClient<Channel>,
    owner: &str,
    hold: &str,
    want: &str,
    ttl_ms: u64,
    world: &World,
    flags: &OwnerFlags,
    deadline: Instant,
    last_token: &mut i64,
    b_start: &tokio::sync::Barrier,
    b_g1: &tokio::sync::Barrier,
) -> anyhow::Result<()> {
    let cs = [
        (hold.to_string(), CsMode::Write),
        (want.to_string(), CsMode::Write),
    ];
    // Sync the round start holding nothing: everyone has finished (and released)
    // the previous round, so this wait — however long it takes a straggler — can
    // never lapse a held lease.
    let _ = tokio::time::timeout(Duration::from_secs(20), b_start.wait()).await;

    let mut attempt = 0u32;
    let mut staged = false;
    'attempt: loop {
        if Instant::now() > deadline {
            anyhow::bail!("deadlock round deadline exceeded for owner {owner}");
        }
        attempt += 1;
        let _ = flags.take_yield_signal();
        let token = incr_token(lk).await?;
        world.record_token(token, last_token);

        // g1: our own round-private resource (distinct per member → always free at
        // round start → this acquire is fast, so the b_g1 hold below is brief).
        match acquire_group_blocking(
            lk,
            owner,
            token,
            &[lreq(hold, CsMode::Write)],
            ttl_ms,
            world,
            flags,
            deadline,
            false, // g1: we hold nothing yet
        )
        .await?
        {
            Acq::Ok => {}
            _ => {
                release_all(lk, owner, "dl-round g1 restart").await;
                victim_backoff(attempt).await;
                continue 'attempt;
            }
        }

        // Sync once, now that we hold our g1 (reached fast since g1 is free), so
        // reaching for g2 is a guaranteed full ring. Timeout-guarded so a dead
        // sibling can't wedge it. The hold across this wait is brief — every peer
        // reaches it within a fast round-private acquire.
        if !staged {
            let _ = tokio::time::timeout(Duration::from_secs(15), b_g1.wait()).await;
            staged = true;
        }

        // g2: the neighbor's resource → CONFLICT → wait → DetectCycle → resolve.
        match acquire_group_blocking(
            lk,
            owner,
            token,
            &[lreq(want, CsMode::Write)],
            ttl_ms,
            world,
            flags,
            deadline,
            true, // g2: we hold g1, must renew it while blocked
        )
        .await?
        {
            Acq::Ok => {}
            _ => {
                // A Yield here is almost always our own victim self-yield; back
                // off asymmetrically (longer than a waiting peer's poll) so the
                // freed resource goes to the waiter and the ring cannot reform.
                release_all(lk, owner, "dl-round g2 restart").await;
                victim_backoff(attempt).await;
                continue 'attempt;
            }
        }

        // Hold both → oracle-checked critical section. Refresh the lease first
        // (see run_op) so a slow clear/assert under load can't lapse it.
        if renew(lk, owner, ttl_ms).await? == RenewStatus::Lost as i32 {
            world.lost.fetch_add(1, R);
            release_all(lk, owner, "dl pre-cs renew lost").await;
            jittered_backoff(attempt).await;
            continue 'attempt;
        }
        let _ = clear_wait_edge_with_retry(
            lk,
            ClearWaitEdgeRequest {
                owner_id: owner.to_string(),
            },
            "dl clear before cs",
        )
        .await;
        match assert_fencing(lk, owner, token, &[hold.to_string(), want.to_string()]).await? {
            None => {
                world.assert_ok.fetch_add(1, R);
            }
            Some((path, reason)) => {
                world.note_assert_fail(format!(
                    "owner={owner} path={path} reason={reason} token={token} held=[{hold}, {want}]"
                ));
                release_all(lk, owner, "dl assert-fail release").await;
                jittered_backoff(attempt).await;
                continue 'attempt;
            }
        }
        if flags.take_yield_signal() {
            release_all(lk, owner, "dl preempt release").await;
            jittered_backoff(attempt).await;
            continue 'attempt;
        }
        world.oracle.lock().unwrap().enter(owner, token, &cs);
        world.cs_entered.fetch_add(1, R);
        let hold_us = splitmix64(token as u64 ^ 0x5151) % 2500;
        if hold_us > 0 {
            tokio::time::sleep(Duration::from_micros(hold_us)).await;
        }
        world.oracle.lock().unwrap().exit(owner, &cs);
        release_all(lk, owner, "dl op release").await;
        world.ops_ok.fetch_add(1, R);
        return Ok(());
    }
}

async fn run_contention_worker(
    endpoint: String,
    wid: usize,
    ops: usize,
    handlers: usize,
    ttl_ms: u64,
    world: Arc<World>,
    deadline: Instant,
) -> anyhow::Result<usize> {
    let owner = format!("cont-{wid:03}");
    let mut lk = PathLockClient::connect(endpoint.clone()).await?;
    let flags = Arc::new(OwnerFlags::default());
    let sub = spawn_subscription(endpoint, owner.clone(), flags.clone(), world.clone());
    let mut last_token = 0i64;
    let mut done = 0usize;
    for op in 0..ops {
        if Instant::now() > deadline {
            sub.abort();
            anyhow::bail!("contention worker {owner} exceeded deadline at op {op}");
        }
        let seed = splitmix64(((wid as u64) << 32) ^ op as u64 ^ 0xC0FFEE);
        let plan = contention_plan(handlers, seed);
        run_op(
            &mut lk,
            &owner,
            &plan,
            ttl_ms,
            &world,
            &flags,
            deadline,
            &mut last_token,
            0,
        )
        .await?;
        done += 1;
    }
    sub.abort();
    Ok(done)
}

#[allow(clippy::too_many_arguments)]
async fn run_deadlock_member(
    endpoint: String,
    group: usize,
    member: usize,
    k: usize,
    shape: u8,
    rounds: usize,
    ttl_ms: u64,
    world: Arc<World>,
    deadline: Instant,
    barriers: Arc<Vec<(Arc<tokio::sync::Barrier>, Arc<tokio::sync::Barrier>)>>,
) -> anyhow::Result<usize> {
    let owner = format!("dl-g{group:03}-m{member:03}");
    let mut lk = PathLockClient::connect(endpoint.clone()).await?;
    let flags = Arc::new(OwnerFlags::default());
    let sub = spawn_subscription(endpoint, owner.clone(), flags.clone(), world.clone());
    let mut last_token = 0i64;
    let mut done = 0usize;
    for round in 0..rounds {
        if Instant::now() > deadline {
            sub.abort();
            anyhow::bail!("deadlock member {owner} exceeded deadline at round {round}");
        }
        let (hold, want) = deadlock_paths(group, member, round, k, shape);
        let (b_start, b_g1) = &barriers[round];
        run_deadlock_round(
            &mut lk,
            &owner,
            &hold,
            &want,
            ttl_ms,
            &world,
            &flags,
            deadline,
            &mut last_token,
            b_start,
            b_g1,
        )
        .await?;
        done += 1;
    }
    sub.abort();
    Ok(done)
}

/// Wait until ALL transient lock state has drained (lazy expiry + GC), keeping
/// every replica healthy throughout. Returns the final census.
async fn wait_for_transient_drain(
    client: &TransactionClient,
    endpoints: &[String],
    timeout: Duration,
) -> anyhow::Result<store::KeyCensus> {
    let deadline = Instant::now() + timeout;
    let mut health = Vec::with_capacity(endpoints.len());
    for ep in endpoints {
        health.push(PathLockClient::connect(ep.clone()).await?);
    }
    loop {
        let census = store::census(client).await?;
        if census.transient == 0 {
            return Ok(census);
        }
        for hc in &mut health {
            let resp = hc.health(HealthRequest {}).await?.into_inner();
            if !resp.ok {
                anyhow::bail!("daemon unhealthy while draining: {}", resp.detail);
            }
        }
        if Instant::now() >= deadline {
            anyhow::bail!("transient keyspace did not drain; census={census:?}");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Prove the cluster still grants and releases a fresh lock after the storm.
async fn post_drain_sanity(endpoint: &str) -> anyhow::Result<()> {
    let mut c = PathLockClient::connect(endpoint.to_string()).await?;
    let token = incr_token(&mut c).await?;
    let owner = "post-drain-sanity";
    let resp = acquire_with_retry(
        &mut c,
        AcquireRequest {
            owner_id: owner.to_string(),
            ttl_ms: 5_000,
            requests: vec![lreq("sanity:/probe", CsMode::Write)],
            fencing_token: token,
            release_requests: vec![],
            emit_release: false,
        },
        "post-drain acquire",
    )
    .await?;
    if resp.status != AcquireStatus::Ok as i32 {
        anyhow::bail!(
            "post-drain sanity acquire failed: status={} reason={}",
            resp.status,
            resp.reason
        );
    }
    release_all(&mut c, owner, "post-drain release").await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn hierarchical_contention_and_deadlocks_stay_safe_and_drain() -> anyhow::Result<()> {
    // Defaults are tuned to complete comfortably on a single-node TiKV while
    // still manufacturing real deadlocks and broad contention. Per-handler
    // serialization is intentional ("serial within a handler, parallel across
    // handlers"), so HANDLERS bounds how many writers contend on one hot
    // serialization key — keep it high enough that no worker starves. Scale any
    // knob up via env for a heavier soak (see scripts/test-e2e-stress usage).
    // Contention is cheap per op (acquire/CS/release) so it carries the bulk of
    // the load; the deadlock groups are kept modest because each manufactured
    // ring drives the expensive DetectCycle/revoke resolution, and too many
    // concurrent rings overwhelm a single-node TiKV (slow RPCs then lapse leases
    // and desync the per-round barriers). All knobs scale up via env for a bigger
    // cluster / heavier soak.
    let replicas = env_usize("PATHLOCKD_E2E_SAFETY_REPLICAS", 2);
    let contenders = env_usize("PATHLOCKD_E2E_SAFETY_CONTENDERS", 12);
    let cont_ops = env_usize("PATHLOCKD_E2E_SAFETY_OPS", 30);
    let dl_groups = env_usize("PATHLOCKD_E2E_SAFETY_DEADLOCK_GROUPS", 3);
    let dl_size = env_usize("PATHLOCKD_E2E_SAFETY_DEADLOCK_SIZE", 3).max(2);
    let dl_rounds = env_usize("PATHLOCKD_E2E_SAFETY_DEADLOCK_ROUNDS", 4);
    let handlers = env_usize("PATHLOCKD_E2E_SAFETY_HANDLERS", 8).max(1);
    let ttl_ms = env_u64("PATHLOCKD_E2E_SAFETY_TTL_MS", 20_000);
    let budget_secs = env_u64("PATHLOCKD_E2E_SAFETY_DEADLINE_SECS", 240);
    let drain_secs = env_u64("PATHLOCKD_E2E_SAFETY_DRAIN_SECS", 150);

    let direct = TransactionClient::new(vec![pd()]).await?;
    store::flush_all(&direct).await?;

    let (daemons, endpoints) = spawn_cluster(replicas).await?;

    let world = Arc::new(World::new());
    let deadline = Instant::now() + Duration::from_secs(budget_secs);

    let mut handles: Vec<tokio::task::JoinHandle<anyhow::Result<usize>>> = Vec::new();
    for wid in 0..contenders {
        let ep = endpoints[wid % endpoints.len()].clone();
        let w = world.clone();
        handles.push(tokio::spawn(run_contention_worker(
            ep, wid, cont_ops, handlers, ttl_ms, w, deadline,
        )));
    }
    // Two single-use barriers per (group, round): one to sync the round start
    // (held-nothing) and one to sync "all hold g1" so the K members form a full
    // wait-for ring each round.
    let group_barriers: Vec<Arc<Vec<(Arc<tokio::sync::Barrier>, Arc<tokio::sync::Barrier>)>>> = (0
        ..dl_groups)
        .map(|_| {
            Arc::new(
                (0..dl_rounds)
                    .map(|_| {
                        (
                            Arc::new(tokio::sync::Barrier::new(dl_size)),
                            Arc::new(tokio::sync::Barrier::new(dl_size)),
                        )
                    })
                    .collect::<Vec<_>>(),
            )
        })
        .collect();
    for g in 0..dl_groups {
        let shape = (g % 3) as u8;
        for m in 0..dl_size {
            let ep = endpoints[(g + m) % endpoints.len()].clone();
            let w = world.clone();
            let barriers = group_barriers[g].clone();
            handles.push(tokio::spawn(run_deadlock_member(
                ep, g, m, dl_size, shape, dl_rounds, ttl_ms, w, deadline, barriers,
            )));
        }
    }
    let total_workers = handles.len();

    // Live progress so a slow run is visibly *progressing* (not livelocked).
    let reporter = {
        let w = world.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(15));
            tick.tick().await;
            loop {
                tick.tick().await;
                println!("[e2e-safety] progress: {}", w.summary());
            }
        })
    };

    // The join is the liveness proof: wrap it in a hard timeout so a wedged
    // cluster fails the test loudly instead of hanging forever. Collect EVERY
    // worker's outcome so a starvation/hang shows the full picture, not just the
    // first task awaited.
    let join_all = async {
        let mut completed = 0usize;
        let mut errors = Vec::new();
        for (i, h) in handles.into_iter().enumerate() {
            match h.await {
                Ok(Ok(n)) => completed += n,
                Ok(Err(e)) => errors.push(format!("worker[{i}]: {e}")),
                Err(e) => errors.push(format!("worker[{i}] panicked: {e}")),
            }
        }
        if !errors.is_empty() {
            anyhow::bail!("{} worker(s) failed:\n{}", errors.len(), errors.join("\n"));
        }
        Ok::<usize, anyhow::Error>(completed)
    };
    let completed = tokio::time::timeout(Duration::from_secs(budget_secs + 60), join_all)
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "LIVENESS FAILURE: workers did not finish within {}s (possible deadlock/livelock/hang).\n{}\nstuck workers:\n{}",
                budget_secs + 60,
                world.report(),
                world.stuck_report(Duration::from_secs(20))
            )
        })?;
    reporter.abort();
    let completed = completed?;

    // ---- safety + correctness assertions ----
    println!("[e2e-safety] {}", world.summary());
    {
        let details = world.assert_fail_details.lock().unwrap();
        if !details.is_empty() {
            println!(
                "[e2e-safety] assert_fail details ({}):\n{}",
                details.len(),
                details.join("\n")
            );
        }
    }
    {
        let oracle = world.oracle.lock().unwrap();
        assert!(
            oracle.violations.is_empty(),
            "MUTUAL-EXCLUSION VIOLATIONS ({}):\n{}",
            oracle.violations.len(),
            oracle.violations.join("\n")
        );
        assert!(oracle.enters > 0, "no critical sections were ever entered");
        println!(
            "[e2e-safety] oracle: {} CS entries, peak {} concurrent, 0 violations",
            oracle.enters, oracle.peak
        );
    }
    assert_eq!(
        world.token_anomalies.load(R),
        0,
        "fencing-token monotonicity was violated"
    );
    let (mn, mx) = (world.min_token.load(R), world.max_token.load(R));
    assert!(mn > 0, "a non-positive fencing token was issued (min={mn})");
    assert!(
        mx > mn,
        "fencing tokens did not advance (min={mn}, max={mx})"
    );
    // AssertFencing failing means the fencing token correctly caught a lease that
    // lapsed before the holder reached its critical section. The worker then
    // aborts WITHOUT entering the CS, so this is never a safety violation — the
    // mutual-exclusion oracle above is the safety guarantee. With the pre-CS renew
    // it should be ~0; a flood would mean the workload is overloading TiKV (a
    // tuning issue, not a correctness bug). Allow a small fraction; detail printed
    // above.
    let assert_fail = world.assert_fail.load(R);
    let total_asserts = world.assert_ok.load(R) + assert_fail;
    assert!(
        assert_fail * 5 <= total_asserts.max(1),
        "AssertFencing failed too often ({assert_fail}/{total_asserts}) — cluster likely overloaded (detail above)"
    );
    assert!(
        world.deadlocks.load(R) > 0,
        "no deadlock cycles were detected — the scenario never exercised DetectCycle"
    );

    // ---- poisoning check: transient state must fully drain ----
    tokio::time::sleep(Duration::from_millis(ttl_ms.max(1_000))).await;
    let census1 =
        wait_for_transient_drain(&direct, &endpoints, Duration::from_secs(drain_secs)).await?;
    assert_eq!(
        census1.transient, 0,
        "transient state did not drain: {census1:?}"
    );

    // Each (group, round) uses its own disjoint subtree, so distinct write paths
    // (hence fence tombstones) scale with groups × rounds × members.
    let durable_cap = (handlers as u64) * 64
        + (dl_groups as u64) * (dl_rounds as u64) * (dl_size as u64) * 4
        + 128;
    assert!(
        census1.durable <= durable_cap,
        "durable (fence) keys {} exceed the fixed-universe bound {durable_cap} — possible unbounded accumulation",
        census1.durable
    );
    // Re-census after a further settle: transient must stay drained and durable
    // must not grow once the workload has stopped.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let census2 = store::census(&direct).await?;
    assert_eq!(
        census2.transient, 0,
        "transient state reappeared after draining: {census2:?}"
    );
    assert!(
        census2.durable <= census1.durable,
        "durable keys grew after the workload stopped ({} -> {})",
        census1.durable,
        census2.durable
    );
    println!(
        "[e2e-safety] drained clean: transient=0, durable(fence)={} (cap {durable_cap})",
        census2.durable
    );

    // ---- the cluster is still fully functional after the storm ----
    for ep in &endpoints {
        post_drain_sanity(ep).await?;
    }
    println!(
        "[e2e-safety] {replicas} replicas, {total_workers} workers, {completed} ops — SAFE, LIVE, no poisoning"
    );

    store::flush_all(&direct).await?;
    drop(daemons);
    Ok(())
}

/// Focused coverage for the cooperative-revoke preemption *claim* (the path the
/// stress test deliberately skips to avoid ring tangles): a winner reserves the
/// contended path so the revoked victim cannot re-grab it before the winner does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preemption_claim_blocks_victim_reacquire() -> anyhow::Result<()> {
    let direct = TransactionClient::new(vec![pd()]).await?;
    store::flush_all(&direct).await?;
    let (daemons, endpoints) = spawn_cluster(1).await?;
    let mut c = PathLockClient::connect(endpoints[0].clone()).await?;

    let path = "claimtest:/p";
    let (victim, winner, third) = ("claim-victim", "claim-winner", "claim-third");

    // 1. Victim holds a write lock on the path.
    let tv = incr_token(&mut c).await?;
    let r = acquire_with_retry(
        &mut c,
        AcquireRequest {
            owner_id: victim.into(),
            ttl_ms: 30_000,
            requests: vec![lreq(path, CsMode::Write)],
            fencing_token: tv,
            release_requests: vec![],
            emit_release: false,
        },
        "victim acquire",
    )
    .await?;
    assert_eq!(r.status, AcquireStatus::Ok as i32);

    // 2. The winner must be ALIVE for its claim to be honored — a claim by a dead
    //    claimant self-heals (is pruned), mirroring dead-owner pruning of locks.
    //    In the real flow the winner holds its first lock while waiting; mirror
    //    that with an anchor lock on a disjoint path.
    let tw_anchor = incr_token(&mut c).await?;
    let anchor = acquire_with_retry(
        &mut c,
        AcquireRequest {
            owner_id: winner.into(),
            ttl_ms: 30_000,
            requests: vec![lreq("claimtest:/winner-anchor", CsMode::Write)],
            fencing_token: tw_anchor,
            release_requests: vec![],
            emit_release: false,
        },
        "winner anchor",
    )
    .await?;
    assert_eq!(anchor.status, AcquireStatus::Ok as i32);

    // 3. Winner plants a preemption claim reserving the path for itself (via
    //    RequestRevoke), then the victim yields.
    request_revoke(&mut c, victim, path, winner, 5_000).await?;
    release_all(&mut c, victim, "victim yields").await;

    // 4. The claim must block ANY other owner from grabbing the freed path.
    let t3 = incr_token(&mut c).await?;
    let blocked = acquire_with_retry(
        &mut c,
        AcquireRequest {
            owner_id: third.into(),
            ttl_ms: 30_000,
            requests: vec![lreq(path, CsMode::Write)],
            fencing_token: t3,
            release_requests: vec![],
            emit_release: false,
        },
        "third blocked by claim",
    )
    .await?;
    assert_eq!(
        blocked.status,
        AcquireStatus::Conflict as i32,
        "a non-claimant must be blocked by the live claim"
    );
    assert_eq!(
        blocked.reason, "preempt_claimed",
        "reason was {}",
        blocked.reason
    );
    assert_eq!(blocked.owner, winner, "the claim should name the winner");

    // 5. The winner acquires over its own claim, which is then consumed.
    let tw = incr_token(&mut c).await?;
    let won = acquire_with_retry(
        &mut c,
        AcquireRequest {
            owner_id: winner.into(),
            ttl_ms: 30_000,
            requests: vec![lreq(path, CsMode::Write)],
            fencing_token: tw,
            release_requests: vec![],
            emit_release: false,
        },
        "winner acquire",
    )
    .await?;
    assert_eq!(
        won.status,
        AcquireStatus::Ok as i32,
        "winner should acquire over its own claim; reason={}",
        won.reason
    );

    // 6. With the claim consumed, a third party now sees an ordinary write
    //    conflict against the winner's real lock — not preempt_claimed.
    let t3b = incr_token(&mut c).await?;
    let after = acquire_with_retry(
        &mut c,
        AcquireRequest {
            owner_id: third.into(),
            ttl_ms: 30_000,
            requests: vec![lreq(path, CsMode::Write)],
            fencing_token: t3b,
            release_requests: vec![],
            emit_release: false,
        },
        "third after winner",
    )
    .await?;
    assert_eq!(after.status, AcquireStatus::Conflict as i32);
    assert_eq!(after.reason, "write_locked");
    assert_eq!(after.owner, winner);

    release_all(&mut c, winner, "cleanup").await;
    store::flush_all(&direct).await?;
    drop(daemons);
    Ok(())
}
