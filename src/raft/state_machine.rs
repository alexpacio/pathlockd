//! State machine: apply(Command) to RocksDB-backed store.
//!
//! Each applied command reads current state from a RocksDB snapshot, builds
//! output + a WriteBatch, writes the batch atomically, and advances
//! `last_applied` in the same batch.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use rocksdb::DB;

use crate::engine::{self};
use crate::raft::command::{ApplyResponse, Command, Op};
use crate::store_keys;
use crate::store_rocksdb::{StoreTxn, StoredRecord};

/// Whether committed state-machine batches fsync the RocksDB WAL.
///
/// Set once at startup from `rocksdb_wal_sync`. When true, every applied
/// command is durable across power loss before it is acknowledged; when false,
/// writes survive a process crash but may be lost on power loss.
static WAL_SYNC: AtomicBool = AtomicBool::new(false);

/// Configure WAL fsync behaviour for all subsequent applies.
pub fn set_wal_sync(enabled: bool) {
    WAL_SYNC.store(enabled, Ordering::Relaxed);
}

/// Applies a committed command to the RocksDB state machine.
///
/// This is called deterministically on every replica (leader and followers) with
/// the same command. The implementation does not call wall-clock time; it uses
/// only `cmd.now_ms`.
pub fn apply(db: &Arc<DB>, cmd: &Command) -> anyhow::Result<ApplyResponse> {
    let mut txn = RocksDbStoreTxn::new(db.clone(), cmd.now_ms);

    let resp = match &cmd.op {
        Op::Acquire(args) => {
            let outcome = engine::acquire_inner(&mut txn, args)?;
            ApplyResponse::Acquire(outcome)
        }
        Op::Release {
            owner,
            reqs,
            del_wait,
        } => {
            engine::release_inner(&mut txn, owner, reqs, *del_wait)?;
            ApplyResponse::Unit
        }
        Op::ReleaseAll { owner, del_wait } => {
            engine::release_all_inner(&mut txn, owner, *del_wait)?;
            ApplyResponse::Unit
        }
        Op::Renew { owner, ttl_ms } => {
            let outcome = engine::renew_inner(&mut txn, owner, *ttl_ms)?;
            ApplyResponse::Renew(outcome)
        }
        Op::ForceRelease { victim } => {
            engine::force_release_inner(&mut txn, victim)?;
            ApplyResponse::Unit
        }
        Op::SetClaim {
            path,
            claimant,
            ttl_ms,
        } => {
            engine::set_claim_inner(&mut txn, path, claimant, *ttl_ms)?;
            ApplyResponse::Unit
        }
        Op::SetWaitEdge {
            owner,
            edge,
            ttl_ms,
        } => {
            engine::set_wait_edge_inner(
                &mut txn,
                owner,
                &edge.conflict_owner,
                *ttl_ms,
                edge.metadata.as_ref(),
            )?;
            ApplyResponse::Unit
        }
        Op::ClearWaitEdge { owner } => {
            engine::clear_wait_edge_inner(&mut txn, owner)?;
            ApplyResponse::Unit
        }
        Op::GcSweep { now_ms, batch } => {
            let reclaimed = gc_sweep(&mut txn, *now_ms, *batch)?;
            ApplyResponse::Gc(reclaimed)
        }
        Op::IncrFence => {
            let token = incr_fence_inner(&mut txn)?;
            ApplyResponse::IncrFence(token)
        }
    };

    txn.commit()?;
    Ok(resp)
}

/// A StoreTxn implementation backed by a RocksDB WriteBatch.
/// This is used by the Raft state machine during apply.
struct RocksDbStoreTxn {
    db: Arc<DB>,
    batch: rocksdb::WriteBatch,
    now_ms: u64,
}

impl RocksDbStoreTxn {
    fn new(db: Arc<DB>, now_ms: u64) -> Self {
        Self {
            db,
            batch: rocksdb::WriteBatch::default(),
            now_ms,
        }
    }

    fn commit(self) -> anyhow::Result<()> {
        let mut opts = rocksdb::WriteOptions::default();
        opts.set_sync(WAL_SYNC.load(Ordering::Relaxed));
        self.db.write_opt(self.batch, &opts)?;
        Ok(())
    }

    fn write_expiry(&mut self, exp: u64, cf: &'static str, primary_key: &[u8]) {
        let db = self.db.clone();
        let cf_handle = db.cf_handle(store_keys::CF_EXPIRY).unwrap();
        let ek = store_keys::expiry_key(exp, cf, primary_key);
        let record = bincode::serde::encode_to_vec(
            &StoredRecord::Str {
                v: String::new(),
                exp,
            },
            bincode::config::standard(),
        )
        .unwrap_or_default();
        self.batch.put_cf(&cf_handle, &ek, &record);
    }
}

impl StoreTxn for RocksDbStoreTxn {
    fn now_ms(&self) -> u64 {
        self.now_ms
    }

    fn get_str(&mut self, cf: &'static str, key: &[u8]) -> anyhow::Result<Option<String>> {
        let cf_handle = self.db.cf_handle(cf).unwrap();
        match self.db.get_cf(&cf_handle, key)? {
            Some(v) => {
                let (rec, _): (StoredRecord, _) =
                    bincode::serde::decode_from_slice(&v, bincode::config::standard())?;
                match rec {
                    StoredRecord::Str { v, exp } => {
                        if store_keys::expired(exp, self.now_ms) {
                            Ok(None)
                        } else {
                            Ok(Some(v))
                        }
                    }
                    StoredRecord::Counter { .. } => Ok(None),
                }
            }
            None => Ok(None),
        }
    }

    fn set_str(
        &mut self,
        cf: &'static str,
        key: &[u8],
        value: &str,
        ttl_ms: u64,
    ) -> anyhow::Result<()> {
        let db = self.db.clone();
        let cf_handle = db.cf_handle(cf).unwrap();
        let exp = store_keys::expiry_at(self.now_ms, ttl_ms);
        let record = bincode::serde::encode_to_vec(
            &StoredRecord::Str {
                v: value.to_string(),
                exp,
            },
            bincode::config::standard(),
        )?;
        self.batch.put_cf(&cf_handle, key, &record);
        if ttl_ms > 0 {
            self.write_expiry(exp, cf, key);
        }
        Ok(())
    }

    fn pexpire_str(&mut self, cf: &'static str, key: &[u8], ttl_ms: u64) -> anyhow::Result<()> {
        if let Some(v) = self.get_str(cf, key)? {
            self.set_str(cf, key, &v, ttl_ms)?;
        }
        Ok(())
    }

    fn del(&mut self, cf: &'static str, key: &[u8]) -> anyhow::Result<()> {
        let db = self.db.clone();
        let cf_handle = db.cf_handle(cf).unwrap();
        self.batch.delete_cf(&cf_handle, key);
        Ok(())
    }

    fn sadd(
        &mut self,
        cf: &'static str,
        key: &[u8],
        member: &str,
        ttl_ms: u64,
    ) -> anyhow::Result<()> {
        let db = self.db.clone();
        let cf_handle = db.cf_handle(cf).unwrap();
        let exp = store_keys::expiry_at(self.now_ms, ttl_ms);
        let record = bincode::serde::encode_to_vec(
            &StoredRecord::Str {
                v: member.to_string(),
                exp,
            },
            bincode::config::standard(),
        )?;
        let mk = member_key(key, member);
        self.batch.put_cf(&cf_handle, &mk, &record);
        if ttl_ms > 0 {
            self.write_expiry(exp, cf, &mk);
        }
        Ok(())
    }

    fn srem(&mut self, cf: &'static str, key: &[u8], member: &str) -> anyhow::Result<()> {
        let db = self.db.clone();
        let cf_handle = db.cf_handle(cf).unwrap();
        let mk = member_key(key, member);
        self.batch.delete_cf(&cf_handle, &mk);
        Ok(())
    }

    fn smembers_limited(
        &mut self,
        cf: &'static str,
        key: &[u8],
        limit: usize,
    ) -> anyhow::Result<Vec<String>> {
        let cf_handle = self.db.cf_handle(cf).unwrap();
        let prefix = member_prefix(key);
        let mut members = Vec::new();
        let mut iter = self.db.raw_iterator_cf(&cf_handle);
        iter.seek(&prefix);
        let mut count = 0;
        while iter.valid() {
            let k = iter.key().unwrap();
            if !k.starts_with(&prefix) {
                break;
            }
            if count >= limit {
                return Err(SetScanLimitExceeded {
                    operation: "smembers",
                    limit,
                }
                .into());
            }
            if let Some(v) = iter.value() {
                if let Ok((rec, _)) = bincode::serde::decode_from_slice::<StoredRecord, _>(
                    v,
                    bincode::config::standard(),
                ) {
                    match rec {
                        StoredRecord::Str { v: member, exp } => {
                            if !store_keys::expired(exp, self.now_ms) {
                                members.push(member);
                            }
                        }
                        StoredRecord::Counter { .. } => {}
                    }
                }
            }
            count += 1;
            iter.next();
        }
        Ok(members)
    }

    fn sismember(&mut self, cf: &'static str, key: &[u8], member: &str) -> anyhow::Result<bool> {
        let cf_handle = self.db.cf_handle(cf).unwrap();
        let mk = member_key(key, member);
        match self.db.get_cf(&cf_handle, &mk)? {
            Some(v) => {
                let (rec, _): (StoredRecord, _) =
                    bincode::serde::decode_from_slice(&v, bincode::config::standard())?;
                match rec {
                    StoredRecord::Str { exp, .. } => Ok(!store_keys::expired(exp, self.now_ms)),
                    StoredRecord::Counter { .. } => Ok(false),
                }
            }
            None => Ok(false),
        }
    }

    fn has_live_member(&mut self, cf: &'static str, key: &[u8]) -> anyhow::Result<bool> {
        let cf_handle = self.db.cf_handle(cf).unwrap();
        let prefix = member_prefix(key);
        let mut iter = self.db.raw_iterator_cf(&cf_handle);
        iter.seek(&prefix);
        while iter.valid() {
            let k = iter.key().unwrap();
            if !k.starts_with(&prefix) {
                return Ok(false);
            }
            if let Some(v) = iter.value() {
                if let Ok((rec, _)) = bincode::serde::decode_from_slice::<StoredRecord, _>(
                    v,
                    bincode::config::standard(),
                ) {
                    match rec {
                        StoredRecord::Str { exp, .. } => {
                            if !store_keys::expired(exp, self.now_ms) {
                                return Ok(true);
                            }
                        }
                        StoredRecord::Counter { .. } => {}
                    }
                }
            }
            iter.next();
        }
        Ok(false)
    }

    fn pexpire_set(&mut self, cf: &'static str, key: &[u8], ttl_ms: u64) -> anyhow::Result<()> {
        let members = self.smembers_limited(cf, key, store_keys::MAX_SET_ENUM_MEMBERS)?;
        for member in members {
            self.sadd(cf, key, &member, ttl_ms)?;
        }
        Ok(())
    }

    fn del_set(&mut self, cf: &'static str, key: &[u8]) -> anyhow::Result<()> {
        let db = self.db.clone();
        let cf_handle = db.cf_handle(cf).unwrap();
        let prefix = member_prefix(key);
        let mut iter = db.raw_iterator_cf(&cf_handle);
        iter.seek(&prefix);
        while iter.valid() {
            let k = iter.key().unwrap().to_vec();
            if !k.starts_with(&prefix) {
                break;
            }
            self.batch.delete_cf(&cf_handle, &k);
            iter.next();
        }
        Ok(())
    }
}

// --- set member key helpers ---

fn member_prefix(key: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(key.len() + 1);
    buf.extend_from_slice(key);
    buf.push(0);
    buf
}

fn member_key(key: &[u8], member: &str) -> Vec<u8> {
    let mut buf = member_prefix(key);
    buf.extend_from_slice(member.as_bytes());
    buf
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("{operation} on set would enumerate more than {limit} live members")]
pub struct SetScanLimitExceeded {
    pub operation: &'static str,
    pub limit: usize,
}

// --- GC sweep ---

/// Reclaim entries whose expiry index timestamp has passed.
///
/// Each expiry index entry points at the real record (`cf` + `primary_key`) it
/// shadows. For every index entry older than `now_ms` we delete the underlying
/// record **iff it is still expired** — a record refreshed since the index was
/// written carries a newer index entry and must survive — and always drop the
/// processed index entry. Returns the number of underlying records reclaimed.
fn gc_sweep(txn: &mut RocksDbStoreTxn, now_ms: u64, batch: u32) -> anyhow::Result<u64> {
    let db = txn.db.clone();
    let expiry_handle = db.cf_handle(store_keys::CF_EXPIRY).unwrap();
    let upper = store_keys::expiry_scan_upper(now_ms);
    let mut iter = db.raw_iterator_cf(&expiry_handle);
    iter.seek_to_first();
    let mut scanned = 0u32;
    let mut reclaimed = 0u64;
    while iter.valid() && scanned < batch {
        let k = iter.key().unwrap();
        if k >= &*upper {
            break;
        }
        if let Some((_exp, cf, primary_key)) = store_keys::decode_expiry_entry(k) {
            if let Some(data_handle) = db.cf_handle(cf) {
                if let Some(v) = db.get_cf(&data_handle, primary_key)? {
                    if let Ok((StoredRecord::Str { exp, .. }, _)) =
                        bincode::serde::decode_from_slice::<StoredRecord, _>(
                            &v,
                            bincode::config::standard(),
                        )
                    {
                        if store_keys::expired(exp, now_ms) {
                            txn.batch.delete_cf(&data_handle, primary_key);
                            reclaimed += 1;
                        }
                    }
                }
            }
        }
        txn.batch.delete_cf(&expiry_handle, k);
        scanned += 1;
        iter.next();
    }
    Ok(reclaimed)
}

// --- incr fence ---

fn incr_fence_inner(txn: &mut RocksDbStoreTxn) -> anyhow::Result<i64> {
    let db = txn.db.clone();
    let cf_handle = db.cf_handle(store_keys::CF_META).unwrap();
    let fence_key = b"fence_counter";
    let current: i64 = match db.get_cf(&cf_handle, fence_key)? {
        Some(v) => {
            let (rec, _): (StoredRecord, _) =
                bincode::serde::decode_from_slice(&v, bincode::config::standard())?;
            match rec {
                StoredRecord::Counter { v } => v,
                _ => 0,
            }
        }
        None => 0,
    };
    let next = current.checked_add(1).unwrap_or(i64::MAX);
    let record = bincode::serde::encode_to_vec(
        &StoredRecord::Counter { v: next },
        bincode::config::standard(),
    )?;
    txn.batch.put_cf(&cf_handle, fence_key, &record);
    Ok(next)
}
