//! Path/owner -> group routing and leader forwarding.
//!
//! The router determines which Raft group owns a given operation, sends
//! commands to the group leader, and handles leader-forwarding on
//! `NotLeader` responses. In P1-P2 single-process mode, it executes
//! operations directly against the local RocksDB state machine.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::engine::{
    AcquireArgs, AcquireOutcome, AssertOutcome, CycleOutcome, LockDumpPage, OwnedLock, PathInfo,
    RelReq, RenewOutcome, WaitEdgeMetadata,
};
use crate::raft::command::{ApplyResponse, Command, Op};

#[derive(Debug, Clone, thiserror::Error)]
#[error("not leader")]
pub struct NotLeader;

#[derive(Debug, Clone, thiserror::Error)]
#[error("raft quorum unavailable")]
pub struct QuorumUnavailable;

#[derive(Debug, Clone, thiserror::Error)]
#[error("routing error: request belongs to a different group")]
pub struct WrongGroup;

#[derive(Debug, Clone, thiserror::Error)]
#[error("all paths in one acquire must share a lock domain")]
pub struct MultiDomainUnsupported;

pub struct Router {
    #[allow(dead_code)]
    group_count: u32,
    local_db: Option<Arc<rocksdb::DB>>,
    /// Serializes state-machine applies (see [`Router::apply_serialized`]).
    apply_lock: Arc<std::sync::Mutex<()>>,
    #[allow(dead_code)]
    leaders: RwLock<HashMap<u64, u64>>,
}

impl Router {
    pub fn new(group_count: u32) -> Self {
        Self {
            group_count,
            local_db: None,
            apply_lock: Arc::new(std::sync::Mutex::new(())),
            leaders: RwLock::new(HashMap::new()),
        }
    }

    pub fn set_local_db(&mut self, db: Arc<rocksdb::DB>) {
        self.local_db = Some(db);
    }

    /// Apply a mutating command through the single serialized writer.
    ///
    /// The lock engine assumes commands are applied one-at-a-time, exactly as a
    /// Raft apply loop guarantees. Until per-group Raft lands, multiple gRPC
    /// handlers would otherwise call `apply` concurrently on the shared DB and
    /// interleave their read-modify-write passes — two acquires could each read
    /// "unlocked" and both commit, double-granting a write lock. We restore the
    /// single-writer invariant here.
    ///
    /// The work runs on the blocking pool (RocksDB is sync and may fsync/stall),
    /// and the mutex is taken *inside* the blocking closure so that a cancelled
    /// client future cannot drop the guard while the write is still in flight.
    async fn apply_serialized(&self, cmd: Command) -> anyhow::Result<ApplyResponse> {
        let db = self
            .local_db
            .clone()
            .ok_or_else(|| anyhow::anyhow!("no local store configured"))?;
        let lock = self.apply_lock.clone();
        tokio::task::spawn_blocking(move || {
            let _guard = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            crate::raft::state_machine::apply(&db, &cmd)
        })
        .await
        .map_err(|e| anyhow::anyhow!("apply task failed: {e}"))?
    }

    /// Run an expiry GC sweep through the serialized writer; returns the number
    /// of expired records reclaimed.
    pub async fn gc_sweep(&self, batch: u32) -> anyhow::Result<u64> {
        let now_ms = crate::store_keys::now_ms();
        let cmd = Command {
            request_id: None,
            now_ms,
            op: Op::GcSweep { now_ms, batch },
        };
        match self.apply_serialized(cmd).await? {
            ApplyResponse::Gc(reclaimed) => Ok(reclaimed),
            _ => anyhow::bail!("unexpected response type"),
        }
    }

    pub async fn acquire(&self, args: AcquireArgs) -> anyhow::Result<AcquireOutcome> {
        let domains: HashSet<&str> = args
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

        let now_ms = crate::store_keys::now_ms();
        let cmd = Command {
            request_id: None,
            now_ms,
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
        let now_ms = crate::store_keys::now_ms();
        let cmd = Command {
            request_id: None,
            now_ms,
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
        let now_ms = crate::store_keys::now_ms();
        let cmd = Command {
            request_id: None,
            now_ms,
            op: Op::ReleaseAll {
                owner: owner.to_string(),
                del_wait,
            },
        };
        self.apply_serialized(cmd).await?;
        Ok(())
    }

    pub async fn renew(&self, owner: &str, ttl_ms: u64) -> anyhow::Result<RenewOutcome> {
        let now_ms = crate::store_keys::now_ms();
        let cmd = Command {
            request_id: None,
            now_ms,
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
        let now_ms = crate::store_keys::now_ms();
        let cmd = Command {
            request_id: None,
            now_ms,
            op: Op::ForceRelease {
                victim: victim.to_string(),
            },
        };
        self.apply_serialized(cmd).await?;
        Ok(())
    }

    pub async fn assert_fencing(
        &self,
        owner: &str,
        token: i64,
        paths: &[String],
    ) -> anyhow::Result<AssertOutcome> {
        if let Some(db) = &self.local_db {
            let now_ms = crate::store_keys::now_ms();
            let mut txn = crate::store_rocksdb::RocksDbTxn::new(db.clone(), now_ms);
            crate::engine::assert_fencing_inner(&mut txn, owner, token, paths)
        } else {
            anyhow::bail!("no local store configured")
        }
    }

    pub async fn incr_fencing_token(&self) -> anyhow::Result<i64> {
        let now_ms = crate::store_keys::now_ms();
        let cmd = Command {
            request_id: None,
            now_ms,
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
        let now_ms = crate::store_keys::now_ms();
        let cmd = Command {
            request_id: None,
            now_ms,
            op: Op::SetWaitEdge {
                owner: owner.to_string(),
                edge: crate::raft::command::WaitEdge {
                    conflict_owner: conflict_owner.to_string(),
                    metadata: metadata.map(|m| m.clone()),
                },
                ttl_ms,
            },
        };
        self.apply_serialized(cmd).await?;
        Ok(())
    }

    pub async fn clear_wait_edge(&self, owner: &str) -> anyhow::Result<()> {
        let now_ms = crate::store_keys::now_ms();
        let cmd = Command {
            request_id: None,
            now_ms,
            op: Op::ClearWaitEdge {
                owner: owner.to_string(),
            },
        };
        self.apply_serialized(cmd).await?;
        Ok(())
    }

    pub async fn detect_cycle(&self, start: &str, max_depth: u32) -> anyhow::Result<CycleOutcome> {
        if let Some(db) = &self.local_db {
            let now_ms = crate::store_keys::now_ms();
            let mut txn = crate::store_rocksdb::RocksDbTxn::new(db.clone(), now_ms);
            crate::engine::detect_cycle_inner(&mut txn, start, max_depth)
        } else {
            anyhow::bail!("no local store configured")
        }
    }

    pub async fn is_blocking(&self, path: &str, owner: &str, reason: &str) -> anyhow::Result<bool> {
        if let Some(db) = &self.local_db {
            let now_ms = crate::store_keys::now_ms();
            let mut txn = crate::store_rocksdb::RocksDbTxn::new(db.clone(), now_ms);
            crate::engine::is_blocking_inner(&mut txn, path, owner, reason)
        } else {
            anyhow::bail!("no local store configured")
        }
    }

    pub async fn is_owner_alive(&self, owner: &str) -> anyhow::Result<bool> {
        if let Some(db) = &self.local_db {
            let now_ms = crate::store_keys::now_ms();
            let mut txn = crate::store_rocksdb::RocksDbTxn::new(db.clone(), now_ms);
            crate::engine::is_owner_alive_inner(&mut txn, owner)
        } else {
            Ok(false)
        }
    }

    pub async fn set_claim(&self, path: &str, claimant: &str, ttl_ms: u64) -> anyhow::Result<()> {
        let now_ms = crate::store_keys::now_ms();
        let cmd = Command {
            request_id: None,
            now_ms,
            op: Op::SetClaim {
                path: path.to_string(),
                claimant: claimant.to_string(),
                ttl_ms,
            },
        };
        self.apply_serialized(cmd).await?;
        Ok(())
    }

    pub async fn inspect_path(&self, path: &str) -> anyhow::Result<PathInfo> {
        if let Some(db) = &self.local_db {
            let now_ms = crate::store_keys::now_ms();
            let mut txn = crate::store_rocksdb::RocksDbTxn::new(db.clone(), now_ms);
            crate::engine::inspect_path_inner(&mut txn, path)
        } else {
            anyhow::bail!("no local store configured")
        }
    }

    pub async fn list_owner_locks(&self, owner: &str) -> anyhow::Result<(bool, Vec<OwnedLock>)> {
        if let Some(db) = &self.local_db {
            let now_ms = crate::store_keys::now_ms();
            let mut txn = crate::store_rocksdb::RocksDbTxn::new(db.clone(), now_ms);
            crate::engine::list_owner_locks_inner(&mut txn, owner)
        } else {
            Ok((false, Vec::new()))
        }
    }

    pub async fn dump_locks(
        &self,
        cursor: Option<Vec<u8>>,
        owner_page: u32,
    ) -> anyhow::Result<LockDumpPage> {
        if let Some(db) = &self.local_db {
            let now_ms = crate::store_keys::now_ms();
            let mut txn = crate::store_rocksdb::RocksDbTxn::new(db.clone(), now_ms);
            crate::engine::dump_locks_inner(&mut txn, cursor, owner_page)
        } else {
            anyhow::bail!("no local store configured")
        }
    }
}
