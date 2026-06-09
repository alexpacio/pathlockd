//! Path/owner -> group routing and the serialized writer.
//!
//! The router determines which Raft group owns a given operation. In the
//! current single-process mode it executes every mutating command on one
//! dedicated writer thread, which restores the single-writer invariant the
//! lock engine assumes (commands applied one at a time, exactly as a Raft
//! apply loop would) and adds two production properties the old
//! mutex-in-spawn_blocking design lacked:
//!
//! - **Bounded queueing / fail-fast backpressure.** Writes are submitted over
//!   a bounded channel; when the writer can't keep up, new writes are rejected
//!   immediately with [`WriteQueueFull`] (surfaced as gRPC `UNAVAILABLE`)
//!   instead of parking hundreds of blocking-pool threads behind a mutex
//!   until every client times out.
//! - **Group commit.** Each drained batch of commands is applied unsynced and
//!   then made durable with a single WAL fsync before any of them is
//!   acknowledged — same durability contract as fsync-per-command at a small
//!   fraction of the cost.
//!
//! A failed WAL fsync poisons the writer (fail-stop): already-applied batches
//! are in an unknown durability state, so the node stops accepting writes and
//! reports unhealthy, letting the orchestrator restart it instead of silently
//! acknowledging maybe-lost commands.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};

use crate::engine::{
    AcquireArgs, AcquireOutcome, AssertOutcome, CycleOutcome, LockDumpPage, OwnedLock, PathInfo,
    RelReq, RenewOutcome, WaitEdgeMetadata,
};
use crate::raft::command::{ApplyResponse, Command, Op};

#[derive(Debug, Clone, thiserror::Error)]
#[error("write queue full")]
pub struct WriteQueueFull;

#[derive(Debug, Clone, thiserror::Error)]
#[error("writer unavailable")]
pub struct WriterUnavailable;

#[derive(Debug, Clone, thiserror::Error)]
#[error("all paths in one acquire must share a lock domain")]
pub struct MultiDomainUnsupported;

/// Max commands applied between WAL fsyncs. Bounds ack latency for the first
/// command of a large drained group.
const WRITE_GROUP_MAX: usize = 256;

struct WriteJob {
    cmd: Command,
    resp: oneshot::Sender<anyhow::Result<ApplyResponse>>,
}

#[derive(Debug, Clone)]
pub struct WriterOptions {
    /// Bounded queue depth; submissions beyond it fail with [`WriteQueueFull`].
    pub queue_depth: usize,
    /// Fsync the WAL once per drained group before acknowledging it.
    pub wal_sync: bool,
}

impl Default for WriterOptions {
    fn default() -> Self {
        Self {
            queue_depth: 1024,
            wal_sync: true,
        }
    }
}

pub struct Router {
    db: Arc<rocksdb::DB>,
    write_tx: mpsc::Sender<WriteJob>,
    queue_depth: Arc<AtomicUsize>,
    healthy: Arc<AtomicBool>,
}

impl Router {
    pub fn new(db: Arc<rocksdb::DB>, opts: WriterOptions) -> Self {
        let (write_tx, write_rx) = mpsc::channel(opts.queue_depth.max(1));
        let queue_depth = Arc::new(AtomicUsize::new(0));
        let healthy = Arc::new(AtomicBool::new(true));

        {
            let db = db.clone();
            let queue_depth = queue_depth.clone();
            let healthy = healthy.clone();
            let wal_sync = opts.wal_sync;
            std::thread::Builder::new()
                .name("pathlockd-writer".into())
                .spawn(move || writer_loop(db, write_rx, queue_depth, healthy, wal_sync))
                .expect("spawning writer thread");
        }

        Self {
            db,
            write_tx,
            queue_depth,
            healthy,
        }
    }

    /// Commands currently queued for the writer (observability gauge).
    pub fn write_queue_depth(&self) -> Arc<AtomicUsize> {
        self.queue_depth.clone()
    }

    /// False after a WAL fsync failure poisoned the writer.
    pub fn writer_healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }

    /// Enqueue a command for the serialized writer and await its result.
    async fn apply_serialized(&self, cmd: Command) -> anyhow::Result<ApplyResponse> {
        if !self.writer_healthy() {
            return Err(WriterUnavailable.into());
        }
        let (resp_tx, resp_rx) = oneshot::channel();
        match self.write_tx.try_send(WriteJob { cmd, resp: resp_tx }) {
            Ok(()) => {
                self.queue_depth.fetch_add(1, Ordering::Relaxed);
            }
            Err(mpsc::error::TrySendError::Full(_)) => return Err(WriteQueueFull.into()),
            Err(mpsc::error::TrySendError::Closed(_)) => return Err(WriterUnavailable.into()),
        }
        match resp_rx.await {
            Ok(result) => result,
            // Writer dropped the job without responding (it is exiting).
            Err(_) => Err(WriterUnavailable.into()),
        }
    }

    /// Round-trip a no-op command through the writer. Proves the queue is
    /// accepting work and the apply loop is draining within `timeout`.
    pub async fn probe_writer(&self, timeout: Duration) -> anyhow::Result<()> {
        let cmd = Command {
            request_id: None,
            now_ms: crate::store_keys::now_ms(),
            op: Op::Noop,
        };
        match tokio::time::timeout(timeout, self.apply_serialized(cmd)).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => anyhow::bail!("writer did not respond within {timeout:?}"),
        }
    }

    /// Run one expiry GC sweep through the serialized writer. Returns
    /// `(scanned, reclaimed)`; `scanned == batch` means backlog remains.
    pub async fn gc_sweep(&self, batch: u32) -> anyhow::Result<(u32, u64)> {
        let now_ms = crate::store_keys::now_ms();
        let cmd = Command {
            request_id: None,
            now_ms,
            op: Op::GcSweep { now_ms, batch },
        };
        match self.apply_serialized(cmd).await? {
            ApplyResponse::Gc { scanned, reclaimed } => Ok((scanned, reclaimed)),
            _ => anyhow::bail!("unexpected response type"),
        }
    }

    pub async fn acquire(&self, args: AcquireArgs) -> anyhow::Result<AcquireOutcome> {
        let domains: std::collections::HashSet<&str> = args
            .requests
            .iter()
            .map(|r| crate::store_keys::lock_domain(&r.path))
            .chain(
                args.release_requests
                    .iter()
                    .map(|r| crate::store_keys::lock_domain(&r.path)),
            )
            .collect();

        if domains.len() > 1 {
            return Err(MultiDomainUnsupported.into());
        }

        if args.requests.is_empty() && args.release_requests.is_empty() {
            return Ok(AcquireOutcome::Ok);
        }

        let cmd = Command {
            request_id: None,
            now_ms: crate::store_keys::now_ms(),
            op: Op::Acquire(args),
        };

        match self.apply_serialized(cmd).await? {
            ApplyResponse::Acquire(outcome) => Ok(outcome),
            _ => anyhow::bail!("unexpected response type"),
        }
    }

    pub async fn release(
        &self,
        owner: &str,
        reqs: &[RelReq],
        del_wait: bool,
    ) -> anyhow::Result<()> {
        if reqs.is_empty() {
            return Ok(());
        }
        let cmd = Command {
            request_id: None,
            now_ms: crate::store_keys::now_ms(),
            op: Op::Release {
                owner: owner.to_string(),
                reqs: reqs.to_vec(),
                del_wait,
            },
        };
        self.apply_serialized(cmd).await?;
        Ok(())
    }

    pub async fn release_all(&self, owner: &str, del_wait: bool) -> anyhow::Result<()> {
        let cmd = Command {
            request_id: None,
            now_ms: crate::store_keys::now_ms(),
            op: Op::ReleaseAll {
                owner: owner.to_string(),
                del_wait,
            },
        };
        self.apply_serialized(cmd).await?;
        Ok(())
    }

    pub async fn renew(&self, owner: &str, ttl_ms: u64) -> anyhow::Result<RenewOutcome> {
        let cmd = Command {
            request_id: None,
            now_ms: crate::store_keys::now_ms(),
            op: Op::Renew {
                owner: owner.to_string(),
                ttl_ms,
            },
        };
        match self.apply_serialized(cmd).await? {
            ApplyResponse::Renew(outcome) => Ok(outcome),
            _ => anyhow::bail!("unexpected response type"),
        }
    }

    pub async fn force_release(&self, victim: &str) -> anyhow::Result<()> {
        let cmd = Command {
            request_id: None,
            now_ms: crate::store_keys::now_ms(),
            op: Op::ForceRelease {
                victim: victim.to_string(),
            },
        };
        self.apply_serialized(cmd).await?;
        Ok(())
    }

    pub async fn incr_fencing_token(&self) -> anyhow::Result<i64> {
        let cmd = Command {
            request_id: None,
            now_ms: crate::store_keys::now_ms(),
            op: Op::IncrFence,
        };
        match self.apply_serialized(cmd).await? {
            ApplyResponse::IncrFence(token) => Ok(token),
            _ => anyhow::bail!("unexpected response type"),
        }
    }

    pub async fn set_wait_edge(
        &self,
        owner: &str,
        conflict_owner: &str,
        ttl_ms: u64,
        metadata: Option<&WaitEdgeMetadata>,
    ) -> anyhow::Result<()> {
        let cmd = Command {
            request_id: None,
            now_ms: crate::store_keys::now_ms(),
            op: Op::SetWaitEdge {
                owner: owner.to_string(),
                edge: crate::raft::command::WaitEdge {
                    conflict_owner: conflict_owner.to_string(),
                    metadata: metadata.cloned(),
                },
                ttl_ms,
            },
        };
        self.apply_serialized(cmd).await?;
        Ok(())
    }

    pub async fn clear_wait_edge(&self, owner: &str) -> anyhow::Result<()> {
        let cmd = Command {
            request_id: None,
            now_ms: crate::store_keys::now_ms(),
            op: Op::ClearWaitEdge {
                owner: owner.to_string(),
            },
        };
        self.apply_serialized(cmd).await?;
        Ok(())
    }

    pub async fn set_claim(&self, path: &str, claimant: &str, ttl_ms: u64) -> anyhow::Result<()> {
        let cmd = Command {
            request_id: None,
            now_ms: crate::store_keys::now_ms(),
            op: Op::SetClaim {
                path: path.to_string(),
                claimant: claimant.to_string(),
                ttl_ms,
            },
        };
        self.apply_serialized(cmd).await?;
        Ok(())
    }

    // --- Read-only operations ---
    //
    // RocksDB reads can block (tombstone scans, cold blocks, compaction
    // stalls); they run on the blocking pool so a slow scan never freezes the
    // async runtime that serves every other RPC, including Health.

    async fn read_blocking<T, F>(&self, op: F) -> anyhow::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(crate::store_rocksdb::RocksDbTxn) -> anyhow::Result<T> + Send + 'static,
    {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let now_ms = crate::store_keys::now_ms();
            op(crate::store_rocksdb::RocksDbTxn::new(db, now_ms))
        })
        .await
        .map_err(|e| anyhow::anyhow!("read task failed: {e}"))?
    }

    pub async fn assert_fencing(
        &self,
        owner: &str,
        token: i64,
        paths: &[String],
    ) -> anyhow::Result<AssertOutcome> {
        let owner = owner.to_string();
        let paths = paths.to_vec();
        self.read_blocking(move |mut txn| {
            crate::engine::assert_fencing_inner(&mut txn, &owner, token, &paths)
        })
        .await
    }

    pub async fn detect_cycle(&self, start: &str, max_depth: u32) -> anyhow::Result<CycleOutcome> {
        let start = start.to_string();
        self.read_blocking(move |mut txn| {
            crate::engine::detect_cycle_inner(&mut txn, &start, max_depth)
        })
        .await
    }

    pub async fn is_blocking(&self, path: &str, owner: &str, reason: &str) -> anyhow::Result<bool> {
        let path = path.to_string();
        let owner = owner.to_string();
        let reason = reason.to_string();
        self.read_blocking(move |mut txn| {
            crate::engine::is_blocking_inner(&mut txn, &path, &owner, &reason)
        })
        .await
    }

    pub async fn is_owner_alive(&self, owner: &str) -> anyhow::Result<bool> {
        let owner = owner.to_string();
        self.read_blocking(move |mut txn| crate::engine::is_owner_alive_inner(&mut txn, &owner))
            .await
    }

    pub async fn inspect_path(&self, path: &str) -> anyhow::Result<PathInfo> {
        let path = path.to_string();
        self.read_blocking(move |mut txn| crate::engine::inspect_path_inner(&mut txn, &path))
            .await
    }

    pub async fn list_owner_locks(&self, owner: &str) -> anyhow::Result<(bool, Vec<OwnedLock>)> {
        let owner = owner.to_string();
        self.read_blocking(move |mut txn| crate::engine::list_owner_locks_inner(&mut txn, &owner))
            .await
    }

    pub async fn dump_locks(
        &self,
        cursor: Option<Vec<u8>>,
        owner_page: u32,
    ) -> anyhow::Result<LockDumpPage> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let now_ms = crate::store_keys::now_ms();
            crate::store_rocksdb::dump_owner_holds(&db, now_ms, cursor, owner_page as usize)
        })
        .await
        .map_err(|e| anyhow::anyhow!("read task failed: {e}"))?
    }
}

// ---------------------------------------------------------------------------
// Writer thread
// ---------------------------------------------------------------------------

fn writer_loop(
    db: Arc<rocksdb::DB>,
    mut rx: mpsc::Receiver<WriteJob>,
    queue_depth: Arc<AtomicUsize>,
    healthy: Arc<AtomicBool>,
    wal_sync: bool,
) {
    // Monotone clamp over command timestamps. Commands are stamped at enqueue
    // time; a clock step backwards (NTP, VM resume) must never make later
    // commands apply with earlier timestamps, or lease expiries would jump
    // around non-deterministically.
    let mut last_now_ms = 0u64;

    while let Some(first) = rx.blocking_recv() {
        let mut jobs = vec![first];
        while jobs.len() < WRITE_GROUP_MAX {
            match rx.try_recv() {
                Ok(job) => jobs.push(job),
                Err(_) => break,
            }
        }
        queue_depth.fetch_sub(jobs.len(), Ordering::Relaxed);

        let mut results: Vec<anyhow::Result<ApplyResponse>> = Vec::with_capacity(jobs.len());
        let mut wrote_any = false;
        for job in &mut jobs {
            job.cmd.now_ms = job.cmd.now_ms.max(last_now_ms);
            last_now_ms = job.cmd.now_ms;
            match crate::raft::state_machine::apply_committing(&db, &job.cmd) {
                Ok((resp, wrote)) => {
                    wrote_any |= wrote;
                    results.push(Ok(resp));
                }
                Err(e) => results.push(Err(e)),
            }
        }

        // Group commit: one fsync makes every command in the group durable
        // before any of them is acknowledged.
        if wal_sync && wrote_any {
            if let Err(e) = db.flush_wal(true) {
                // Durability of the already-applied group is unknown: poison
                // the writer (fail-stop) so the node stops acknowledging
                // writes and health turns not-ready.
                error!(error = %e, "WAL fsync failed; poisoning writer (node needs restart)");
                healthy.store(false, Ordering::Relaxed);
                let err = || anyhow::anyhow!(WriterUnavailable);
                for job in jobs {
                    let _ = job.resp.send(Err(err()));
                }
                // Drain and reject everything still queued, then exit; the
                // closed channel rejects all future submissions.
                while let Ok(job) = rx.try_recv() {
                    queue_depth.fetch_sub(1, Ordering::Relaxed);
                    let _ = job.resp.send(Err(err()));
                }
                return;
            }
        }

        for (job, result) in jobs.into_iter().zip(results) {
            // Client gone (timeout/cancel) before the result landed: the
            // command was still applied; surface only failed outcomes.
            if let Err(Err(e)) = job.resp.send(result) {
                warn!(error = %e, "write completed with error after client disconnected");
            }
        }
    }
    info!("serialized writer stopped");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{LockReq, Mode, State};

    fn test_router() -> (Arc<Router>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db = crate::store_rocksdb::open_db(
            &dir.path().join("db"),
            &crate::store_rocksdb::DbTuning::default(),
        )
        .unwrap();
        (Arc::new(Router::new(db, WriterOptions::default())), dir)
    }

    #[tokio::test]
    async fn writer_round_trips_commands_and_probe() {
        let (router, _dir) = test_router();

        router
            .probe_writer(Duration::from_secs(5))
            .await
            .expect("probe must round-trip the writer");

        let outcome = router
            .acquire(AcquireArgs {
                owner_id: "owner-1".into(),
                ttl_ms: 5_000,
                requests: vec![LockReq {
                    path: "h:/r".into(),
                    mode: Mode::Write,
                    state: State::New,
                }],
                fencing_token: 1,
                release_requests: vec![],
            })
            .await
            .unwrap();
        assert_eq!(outcome, AcquireOutcome::Ok);

        let info = router.inspect_path("h:/r").await.unwrap();
        assert_eq!(info.write_owner.as_deref(), Some("owner-1"));

        let (scanned, _reclaimed) = router.gc_sweep(128).await.unwrap();
        assert!(scanned <= 128);
        assert_eq!(router.write_queue_depth().load(Ordering::Relaxed), 0);
        assert!(router.writer_healthy());
    }

    #[tokio::test]
    async fn concurrent_writes_serialize_correctly() {
        let (router, _dir) = test_router();

        // Many tasks contend for the same write lock; exactly one may win.
        let mut handles = Vec::new();
        for i in 0..32 {
            let router = router.clone();
            handles.push(tokio::spawn(async move {
                router
                    .acquire(AcquireArgs {
                        owner_id: format!("owner-{i}"),
                        ttl_ms: 30_000,
                        requests: vec![LockReq {
                            path: "h:/contended".into(),
                            mode: Mode::Write,
                            state: State::New,
                        }],
                        fencing_token: 1,
                        release_requests: vec![],
                    })
                    .await
                    .unwrap()
            }));
        }
        let mut winners = 0;
        for handle in handles {
            if matches!(handle.await.unwrap(), AcquireOutcome::Ok) {
                winners += 1;
            }
        }
        assert_eq!(winners, 1, "exactly one owner may hold the write lock");
    }
}
