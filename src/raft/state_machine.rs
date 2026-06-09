//! State machine: apply(Command) to the RocksDB-backed store.
//!
//! Each applied command runs against a [`WriteTxn`]: reads observe both
//! committed state and the command's own pending writes, and the whole command
//! commits atomically — or not at all. Commands whose outcome is a rejection
//! (`Conflict` / `Lost`) are *discarded* rather than committed, so a failed
//! acquire can never leave partial state (e.g. an owner-set entry for a lock
//! that was ultimately refused) behind.
//!
//! Durability: the WriteBatch is written without fsync here; the serialized
//! writer (see `cluster::router`) fsyncs the WAL once per drained group of
//! commands before acknowledging any of them.

use std::sync::Arc;

use rocksdb::DB;

use crate::engine::{self, AcquireOutcome, RenewOutcome};
use crate::raft::command::{ApplyResponse, Command, Op};
use crate::store_keys;
use crate::store_rocksdb::{decode_record, encode_record, StoredRecord, WriteTxn};

/// Applies a committed command to the RocksDB state machine.
///
/// This is called deterministically on every replica (leader and followers)
/// with the same command. The implementation does not call wall-clock time; it
/// uses only `cmd.now_ms` (stamped and monotonically clamped by the writer).
pub fn apply(db: &Arc<DB>, cmd: &Command) -> anyhow::Result<ApplyResponse> {
    apply_committing(db, cmd).map(|(resp, _wrote)| resp)
}

/// Like [`apply`], additionally reporting whether anything was written (used
/// by the writer to decide whether a group needs a WAL fsync).
pub fn apply_committing(db: &Arc<DB>, cmd: &Command) -> anyhow::Result<(ApplyResponse, bool)> {
    let mut txn = WriteTxn::new(db.clone(), cmd.now_ms);

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
            let (scanned, reclaimed) = gc_sweep(&mut txn, *now_ms, *batch)?;
            ApplyResponse::Gc { scanned, reclaimed }
        }
        Op::IncrFence => {
            let token = incr_fence_inner(&mut txn)?;
            ApplyResponse::IncrFence(token)
        }
        Op::Noop => ApplyResponse::Unit,
    };

    // A rejected command must not commit: its writes (lease refreshes, lazy
    // prunes, partially-executed grants) were made under the assumption the
    // whole operation would succeed.
    let commit = !matches!(
        &resp,
        ApplyResponse::Acquire(AcquireOutcome::Conflict { .. } | AcquireOutcome::Lost { .. })
            | ApplyResponse::Renew(RenewOutcome::Lost { .. })
    );

    let wrote = if commit { txn.commit()? } else { false };
    Ok((resp, wrote))
}

// ---------------------------------------------------------------------------
// GC sweep
// ---------------------------------------------------------------------------

/// Reclaim entries whose expiry index timestamp has passed.
///
/// The expiry index is queue-shaped: keys are ordered by timestamp and are
/// only ever consumed from the front. The sweep resumes from a persisted
/// cursor (`meta/gc_cursor`) instead of `seek_to_first`, so it never re-walks
/// the growing wall of tombstones left by previous sweeps — the degradation
/// that previously slowed the whole write path down over time.
///
/// For every index entry older than `now_ms` the underlying record is deleted
/// **iff it is still expired** — a record refreshed since the index entry was
/// written carries a newer index entry and must survive — and the processed
/// index entry is always dropped. Returns `(scanned, reclaimed)`; a `scanned`
/// equal to `batch` signals remaining backlog, letting the caller loop until
/// caught up.
fn gc_sweep(txn: &mut WriteTxn, now_ms: u64, batch: u32) -> anyhow::Result<(u32, u64)> {
    let cursor = txn.get_raw(store_keys::CF_META, store_keys::META_GC_CURSOR_KEY)?;
    let upper = store_keys::expiry_scan_upper(now_ms);

    let mut keys: Vec<Vec<u8>> = Vec::new();
    txn.scan_merged(
        store_keys::CF_EXPIRY,
        cursor.as_deref(),
        Some(&upper),
        |k, _v| {
            keys.push(k.to_vec());
            Ok(keys.len() < batch as usize)
        },
    )?;

    let mut reclaimed = 0u64;
    for key in &keys {
        if let Some((_exp, cf, primary_key)) = store_keys::decode_expiry_entry(key) {
            // The expiry entry names its CF by string; map to the static name
            // so the overlay (keyed by &'static str) stays coherent.
            if let Some(static_cf) = static_cf_name(cf) {
                if let Some(bytes) = txn.get_raw(static_cf, primary_key)? {
                    if let Ok(StoredRecord::Str { exp, .. }) = decode_record(&bytes) {
                        if store_keys::expired(exp, now_ms) {
                            txn.delete_raw(static_cf, primary_key)?;
                            reclaimed += 1;
                        }
                    }
                }
            }
        }
        txn.delete_raw(store_keys::CF_EXPIRY, key)?;
    }

    if let Some(last) = keys.last() {
        // Resume strictly after the last processed entry. Timestamps are
        // monotone (the writer clamps now_ms), so no future index entry can
        // ever sort below the cursor.
        txn.put_raw(
            store_keys::CF_META,
            store_keys::META_GC_CURSOR_KEY,
            crate::store_rocksdb::key_successor(last),
        )?;
    }

    Ok((keys.len() as u32, reclaimed))
}

/// Map a CF name decoded from an expiry-index key to its `&'static str`
/// equivalent (the overlay and CF lookups are keyed by static names).
fn static_cf_name(name: &str) -> Option<&'static str> {
    store_keys::ALL_CFS.iter().copied().find(|cf| *cf == name)
}

// ---------------------------------------------------------------------------
// Fencing counter
// ---------------------------------------------------------------------------

fn incr_fence_inner(txn: &mut WriteTxn) -> anyhow::Result<i64> {
    let current: i64 = match txn.get_raw(store_keys::CF_META, store_keys::META_FENCE_COUNTER_KEY)? {
        Some(bytes) => match decode_record(&bytes)? {
            StoredRecord::Counter { v } => v,
            _ => 0,
        },
        None => 0,
    };
    let next = current.saturating_add(1);
    let record = encode_record(&StoredRecord::Counter { v: next })?;
    txn.put_raw(
        store_keys::CF_META,
        store_keys::META_FENCE_COUNTER_KEY,
        record,
    )?;
    Ok(next)
}
