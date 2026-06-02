//! Daemon-level state-machine e2e test against a real TiKV cluster.
//!
//! This is deliberately not a load test. It drives one pathlockd daemon through
//! the public gRPC API, then reads TiKV directly and decodes the stored
//! `fslock:*` values to prove the expected writes, TTL extensions, lazy expiry,
//! release cleanup and GC cleanup are actually happening in storage.

use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::Context;
use pathlockd::proto::{
    path_lock_client::PathLockClient, AcquireRequest, AcquireResponse, AcquireStatus,
    AssertFencingRequest, AssertStatus, ClearWaitEdgeRequest, HealthRequest,
    IncrFencingTokenRequest, LockRequest, LockState, Mode, ReleaseAllRequest, ReleaseLocksRequest,
    ReleaseRequest, RenewRequest, RenewStatus, SetWaitEdgeRequest,
};
use pathlockd::store::{self, Stored};
use tikv_client::TransactionClient;
use tokio::net::TcpListener;
use tonic::transport::Channel;
use tonic::Code;

const INITIAL_TTL_MS: u64 = 2_000;
const RENEWED_TTL_MS: u64 = 7_000;
const SHORT_TTL_MS: u64 = 700;

fn pd() -> String {
    std::env::var("PATHLOCKD_PD_ENDPOINTS").unwrap_or_else(|_| "127.0.0.1:2379".to_string())
}

async fn free_port() -> anyhow::Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    Ok(listener.local_addr()?.port())
}

struct Daemon {
    child: Child,
}

impl Daemon {
    fn spawn(port: u16) -> anyhow::Result<Self> {
        let bin = env!("CARGO_BIN_EXE_pathlockd");
        let child = Command::new(bin)
            .env("PATHLOCKD_LISTEN", format!("127.0.0.1:{port}"))
            .env("PATHLOCKD_PD_ENDPOINTS", pd())
            .env("PATHLOCKD_GC_INTERVAL_SECS", "1")
            .env("PATHLOCKD_GC_PAGE", "64")
            .env("PATHLOCKD_MVCC_GC_INTERVAL_SECS", "0")
            .env("PATHLOCKD_REQUEST_TIMEOUT_MS", "30000")
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

async fn wait_for_health(endpoint: &str, daemon: &mut Daemon) -> anyhow::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = daemon.child.try_wait()? {
            anyhow::bail!("pathlockd exited before becoming healthy: {status}");
        }

        if let Ok(mut client) = PathLockClient::connect(endpoint.to_string()).await {
            if let Ok(resp) = client.health(HealthRequest {}).await {
                if resp.into_inner().ok {
                    return Ok(());
                }
            }
        }

        if Instant::now() >= deadline {
            anyhow::bail!("pathlockd did not become healthy before timeout");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
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

async fn raw_stored(client: &TransactionClient, key: &str) -> anyhow::Result<Option<Stored>> {
    let mut txn = client.begin_optimistic().await?;
    let bytes = txn.get(key.as_bytes().to_vec()).await?;
    let _ = txn.rollback().await;
    let Some(bytes) = bytes else {
        return Ok(None);
    };
    let (stored, _) =
        bincode::serde::decode_from_slice(bytes.as_ref(), bincode::config::standard())
            .with_context(|| format!("decode raw TiKV key {key}"))?;
    Ok(Some(stored))
}

async fn raw_str(client: &TransactionClient, key: &str) -> anyhow::Result<(String, u64)> {
    match raw_stored(client, key).await? {
        Some(Stored::Str { v, exp }) => Ok((v, exp)),
        Some(other) => anyhow::bail!("key {key} stored {other:?}, expected string"),
        None => anyhow::bail!("key {key} is absent"),
    }
}

async fn raw_absent(client: &TransactionClient, key: &str) -> anyhow::Result<()> {
    if let Some(stored) = raw_stored(client, key).await? {
        anyhow::bail!("key {key} still exists as {stored:?}");
    }
    Ok(())
}

async fn logical_str_absent(client: &TransactionClient, key: &str) -> anyhow::Result<()> {
    let mut tx = store::Tx::begin(client).await?;
    let value = tx.get_str(key).await?;
    tx.rollback().await?;
    if value.is_some() {
        anyhow::bail!("key {key} is still logically live");
    }
    Ok(())
}

async fn logical_set_empty(client: &TransactionClient, key: &str) -> anyhow::Result<()> {
    let mut tx = store::Tx::begin(client).await?;
    let members = tx.smembers(key).await?;
    tx.rollback().await?;
    if !members.is_empty() {
        anyhow::bail!("set {key} still has live members: {members:?}");
    }
    Ok(())
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

fn set_member_key(key: &str, member: &str) -> String {
    format!(
        "fslock:setm:{}:{}",
        hex_encode(key.as_bytes()),
        hex_encode(member.as_bytes())
    )
}

fn lock_req(path: &str, mode: Mode, state: LockState) -> LockRequest {
    LockRequest {
        path: path.to_string(),
        mode: mode as i32,
        state: state as i32,
    }
}

fn release_req(path: &str, mode: Mode) -> ReleaseRequest {
    ReleaseRequest {
        path: path.to_string(),
        mode: mode as i32,
    }
}

async fn sleep_until_after(client: &TransactionClient, exp: u64) -> anyhow::Result<()> {
    let now = store::cluster_now_ms(client).await?;
    let wait_ms = exp.saturating_sub(now).saturating_add(250);
    if wait_ms > 0 {
        tokio::time::sleep(Duration::from_millis(wait_ms)).await;
    }
    Ok(())
}

async fn wait_for_transient_drain(
    client: &TransactionClient,
    timeout: Duration,
) -> anyhow::Result<store::KeyCensus> {
    let deadline = Instant::now() + timeout;
    loop {
        let census = store::census(client).await?;
        if census.transient == 0 {
            return Ok(census);
        }
        if Instant::now() >= deadline {
            anyhow::bail!("transient keyspace did not drain: {census:?}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn daemon_persists_expected_tikv_state_transitions() -> anyhow::Result<()> {
    let direct = TransactionClient::new(vec![pd()]).await?;
    store::flush_all(&direct).await?;

    let port = free_port().await?;
    let endpoint = format!("http://127.0.0.1:{port}");
    let mut daemon = Daemon::spawn(port)?;
    wait_for_health(&endpoint, &mut daemon).await?;
    let mut client = PathLockClient::connect(endpoint).await?;

    let owner = "state-owner";
    let write_path = "state:/tree/file";
    let read_path = "state:/tree/read-point";
    let write_member = format!("write:{write_path}");
    let read_member = format!("read:{read_path}");

    let token = client
        .incr_fencing_token(IncrFencingTokenRequest {})
        .await?
        .into_inner()
        .token;

    let acquire = acquire_with_retry(
        &mut client,
        AcquireRequest {
            owner_id: owner.to_string(),
            ttl_ms: INITIAL_TTL_MS,
            requests: vec![
                lock_req(write_path, Mode::Write, LockState::New),
                lock_req(read_path, Mode::Read, LockState::New),
            ],
            fencing_token: token,
            release_requests: vec![],
            emit_release: false,
        },
        "initial acquire",
    )
    .await?;
    assert_eq!(acquire.status, AcquireStatus::Ok as i32);

    let alive_key = store::alive_key(owner);
    let own_key = store::own_key(owner);
    let wr_key = store::wr_key(write_path);
    let fence_key = store::fence_key(write_path);
    let rd_key = store::rd_key(read_path);
    let own_write_member_key = set_member_key(&own_key, &write_member);
    let own_read_member_key = set_member_key(&own_key, &read_member);
    let read_set_member_key = set_member_key(&rd_key, owner);
    let wrdesc_tree_key = set_member_key(&store::wrdesc_key("state:/tree"), write_path);
    let wrdesc_root_key = set_member_key(&store::wrdesc_key("state:/"), write_path);
    let rddesc_tree_key = set_member_key(&store::rddesc_key("state:/tree"), read_path);
    let rddesc_root_key = set_member_key(&store::rddesc_key("state:/"), read_path);

    let (alive_v1, alive_exp1) = raw_str(&direct, &alive_key).await?;
    assert_eq!(alive_v1, "1");
    let (write_owner_v1, write_exp1) = raw_str(&direct, &wr_key).await?;
    assert_eq!(write_owner_v1, owner);
    let (fence_v1, fence_exp1) = raw_str(&direct, &fence_key).await?;
    assert_eq!(fence_v1, token.to_string());
    assert!(fence_exp1 > write_exp1, "fence TTL should outlive lock TTL");
    assert_eq!(raw_str(&direct, &own_write_member_key).await?.0, "1");
    assert_eq!(raw_str(&direct, &own_read_member_key).await?.0, "1");
    assert_eq!(raw_str(&direct, &read_set_member_key).await?.0, "1");
    assert_eq!(raw_str(&direct, &wrdesc_tree_key).await?.0, "1");
    assert_eq!(raw_str(&direct, &wrdesc_root_key).await?.0, "1");
    assert_eq!(raw_str(&direct, &rddesc_tree_key).await?.0, "1");
    assert_eq!(raw_str(&direct, &rddesc_root_key).await?.0, "1");

    tokio::time::sleep(Duration::from_millis(250)).await;
    let renew = client
        .renew(RenewRequest {
            owner_id: owner.to_string(),
            ttl_ms: RENEWED_TTL_MS,
        })
        .await?
        .into_inner();
    assert_eq!(renew.status, RenewStatus::Ok as i32);

    let (_, alive_exp2) = raw_str(&direct, &alive_key).await?;
    let (_, write_exp2) = raw_str(&direct, &wr_key).await?;
    let (_, fence_exp2) = raw_str(&direct, &fence_key).await?;
    let (_, own_write_exp2) = raw_str(&direct, &own_write_member_key).await?;
    let (_, own_read_exp2) = raw_str(&direct, &own_read_member_key).await?;
    let (_, read_set_exp2) = raw_str(&direct, &read_set_member_key).await?;
    let (_, wrdesc_exp2) = raw_str(&direct, &wrdesc_tree_key).await?;
    let (_, rddesc_exp2) = raw_str(&direct, &rddesc_tree_key).await?;
    assert!(alive_exp2 > alive_exp1, "renew must extend alive TTL");
    assert!(write_exp2 > write_exp1, "renew must extend write-owner TTL");
    assert!(
        own_write_exp2 > write_exp1,
        "renew must extend owner write membership TTL"
    );
    assert!(
        own_read_exp2 > write_exp1,
        "renew must extend owner read membership TTL"
    );
    assert!(
        read_set_exp2 > write_exp1,
        "renew must extend read-set member TTL"
    );
    assert!(
        wrdesc_exp2 > write_exp1,
        "renew must extend write index TTL"
    );
    assert!(rddesc_exp2 > write_exp1, "renew must extend read index TTL");
    assert!(fence_exp2 >= fence_exp1, "renew must not shorten fence TTL");

    sleep_until_after(&direct, write_exp1).await?;
    let now_after_original_ttl = store::cluster_now_ms(&direct).await?;
    assert!(
        write_exp2 > now_after_original_ttl,
        "renewed write key should still be live after original TTL"
    );
    let assert_fencing = client
        .assert_fencing(AssertFencingRequest {
            owner_id: owner.to_string(),
            fencing_token: token,
            paths: vec![write_path.to_string()],
        })
        .await?
        .into_inner();
    assert_eq!(assert_fencing.status, AssertStatus::Ok as i32);

    client
        .release(ReleaseLocksRequest {
            owner_id: owner.to_string(),
            requests: vec![release_req(read_path, Mode::Read)],
            del_wait_key: false,
        })
        .await?;
    raw_absent(&direct, &own_read_member_key).await?;
    raw_absent(&direct, &read_set_member_key).await?;
    raw_absent(&direct, &rddesc_tree_key).await?;
    raw_absent(&direct, &rddesc_root_key).await?;
    assert_eq!(raw_str(&direct, &alive_key).await?.0, "1");
    assert_eq!(raw_str(&direct, &wr_key).await?.0, owner);

    client
        .set_wait_edge(SetWaitEdgeRequest {
            owner_id: owner.to_string(),
            conflict_owner: "blocker-owner".to_string(),
            ttl_ms: RENEWED_TTL_MS,
            conflict_path: write_path.to_string(),
            reason: "write_locked".to_string(),
        })
        .await?;
    let (wait_v, wait_exp) = raw_str(&direct, &store::wait_key(owner)).await?;
    assert!(
        wait_v.starts_with("v1:"),
        "wait edge should include metadata"
    );
    assert!(wait_exp > store::cluster_now_ms(&direct).await?);
    client
        .clear_wait_edge(ClearWaitEdgeRequest {
            owner_id: owner.to_string(),
        })
        .await?;
    raw_absent(&direct, &store::wait_key(owner)).await?;

    client
        .release_all(ReleaseAllRequest {
            owner_id: owner.to_string(),
            del_wait_key: true,
        })
        .await?;
    raw_absent(&direct, &alive_key).await?;
    raw_absent(&direct, &wr_key).await?;
    raw_absent(&direct, &own_write_member_key).await?;
    raw_absent(&direct, &wrdesc_tree_key).await?;
    raw_absent(&direct, &wrdesc_root_key).await?;
    assert_eq!(
        raw_str(&direct, &fence_key).await?.0,
        token.to_string(),
        "release must leave durable fence tombstone"
    );

    let ttl_owner = "ttl-owner";
    let ttl_path = "state:/ttl/read";
    let ttl_acquire = acquire_with_retry(
        &mut client,
        AcquireRequest {
            owner_id: ttl_owner.to_string(),
            ttl_ms: SHORT_TTL_MS,
            requests: vec![lock_req(ttl_path, Mode::Read, LockState::New)],
            fencing_token: 0,
            release_requests: vec![],
            emit_release: false,
        },
        "short ttl acquire",
    )
    .await?;
    assert_eq!(ttl_acquire.status, AcquireStatus::Ok as i32);
    let ttl_alive_key = store::alive_key(ttl_owner);
    let ttl_own_key = store::own_key(ttl_owner);
    let ttl_rd_key = store::rd_key(ttl_path);
    let ttl_read_member = format!("read:{ttl_path}");
    let ttl_own_member_key = set_member_key(&ttl_own_key, &ttl_read_member);
    let ttl_rd_member_key = set_member_key(&ttl_rd_key, ttl_owner);
    let (_, ttl_exp) = raw_str(&direct, &ttl_alive_key).await?;
    assert_eq!(raw_str(&direct, &ttl_own_member_key).await?.0, "1");
    assert_eq!(raw_str(&direct, &ttl_rd_member_key).await?.0, "1");

    sleep_until_after(&direct, ttl_exp).await?;
    logical_str_absent(&direct, &ttl_alive_key).await?;
    logical_set_empty(&direct, &ttl_own_key).await?;
    logical_set_empty(&direct, &ttl_rd_key).await?;

    let _ = store::gc_once(&direct, 64).await?;
    let census = wait_for_transient_drain(&direct, Duration::from_secs(10)).await?;
    assert_eq!(census.transient, 0);
    assert!(
        census.durable >= 1,
        "durable fence tombstone should remain after transient state drains"
    );

    store::flush_all(&direct).await?;
    Ok(())
}
