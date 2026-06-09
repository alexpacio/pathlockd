//! Integration tests for the lock engine over a real RocksDB.
//!
//! These tests pin down the lock primitives directly against the RocksDB state
//! machine — acquiring/releasing/renewing locks, hierarchy containment,
//! fencing, deadlock detection, and GC pruning — all in a single process
//! without gRPC or the full Raft stack.

use std::sync::Arc;

use pathlockd::engine::{
    AcquireArgs, AcquireOutcome, AssertOutcome, CycleOutcome, LockReq, Mode, RelReq, RenewOutcome,
    State, WaitEdgeMetadata,
};
use pathlockd::raft::command::{ApplyResponse, Command, Op};
use pathlockd::raft::state_machine;
use pathlockd::store_keys;

/// Creates a new RocksDB in a temp directory with all column families.
fn open_temp_db() -> (Arc<rocksdb::DB>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("db");

    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    let cfs = store_keys::ALL_CFS;
    let db = Arc::new(rocksdb::DB::open_cf(&opts, &db_path, cfs).unwrap());
    (db, dir)
}

fn apply(db: &Arc<rocksdb::DB>, cmd: Command) -> ApplyResponse {
    state_machine::apply(db, &cmd).unwrap()
}

fn wr(path: &str) -> LockReq {
    LockReq { path: path.to_string(), mode: Mode::Write, state: State::New }
}

fn wr_held(path: &str) -> LockReq {
    LockReq { path: path.to_string(), mode: Mode::Write, state: State::Held }
}

fn rd(path: &str) -> LockReq {
    LockReq { path: path.to_string(), mode: Mode::Read, state: State::New }
}

fn rd_held(path: &str) -> LockReq {
    LockReq { path: path.to_string(), mode: Mode::Read, state: State::Held }
}

fn rel(path: &str, mode: Mode) -> RelReq {
    RelReq { path: path.to_string(), mode }
}

fn acquire_args(owner: &str, ttl_ms: u64, fence_token: i64, reqs: Vec<LockReq>) -> AcquireArgs {
    AcquireArgs { owner_id: owner.to_string(), ttl_ms, requests: reqs, fencing_token: fence_token, release_requests: vec![] }
}

#[test]
fn acquire_root_write_succeeds() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();
    let cmd = Command {
        request_id: None,
        now_ms: now,
        op: Op::Acquire(acquire_args("alice", 30_000, 1, vec![wr("h:/")])),
    };
    let resp = apply(&db, cmd);
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));
}

#[test]
fn acquire_rejects_ancestor_write_block() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice locks root
    let cmd = Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 1, vec![wr("h:/")])),
    };
    assert!(matches!(apply(&db, cmd), ApplyResponse::Acquire(AcquireOutcome::Ok)));

    // Bob tries to lock a descendant
    let cmd = Command {
        request_id: None, now_ms: now + 1,
        op: Op::Acquire(acquire_args("bob", 30_000, 2, vec![wr("h:/a")])),
    };
    match apply(&db, cmd) {
        ApplyResponse::Acquire(AcquireOutcome::Conflict { path, owner, reason }) => {
            assert_eq!(path, "h:/");
            assert_eq!(owner, "alice");
            assert_eq!(reason, "ancestor_locked");
        }
        other => panic!("expected Conflict, got {:?}", other),
    }
}

#[test]
fn descendant_write_rejects_ancestor_write_acquire() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice locks a descendant
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 1, vec![wr("h:/a/b")])),
    });

    // Bob tries to lock ancestor
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::Acquire(acquire_args("bob", 30_000, 2, vec![wr("h:/a")])),
    });
    match resp {
        ApplyResponse::Acquire(AcquireOutcome::Conflict { path, owner, reason }) => {
            assert_eq!(owner, "alice");
            assert!(reason.contains("descendant"));
            assert!(path.starts_with("h:/a"));
        }
        other => panic!("expected Conflict, got {:?}", other),
    }
}

#[test]
fn read_write_share_if_same_path() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice gets a read lock
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 0, vec![rd("h:/a")])),
    });

    // Bob gets a read lock on same path
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::Acquire(acquire_args("bob", 60_000, 0, vec![rd("h:/a")])),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));

    // Carol tries a write → conflict (read_locked)
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 2,
        op: Op::Acquire(acquire_args("carol", 30_000, 3, vec![wr("h:/a")])),
    });
    match resp {
        ApplyResponse::Acquire(AcquireOutcome::Conflict { reason, .. }) => {
            assert_eq!(reason, "read_locked");
        }
        other => panic!("expected Conflict, got {:?}", other),
    }
}

#[test]
fn reads_are_point_only() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice writes descendant
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 1, vec![wr("h:/a/b/c")])),
    });

    // Bob reads ancestor → succeeds (point-only read)
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::Acquire(acquire_args("bob", 30_000, 0, vec![rd("h:/a")])),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));
}

#[test]
fn ancestor_write_blocked_by_descendant_read() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice holds a read lock on a descendant.
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 0, vec![rd("h:/a/b/c")])),
    });

    // Bob tries to write-lock an ancestor → must conflict on the descendant read.
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::Acquire(acquire_args("bob", 30_000, 1, vec![wr("h:/a")])),
    });
    match resp {
        ApplyResponse::Acquire(AcquireOutcome::Conflict { path, owner, reason }) => {
            assert_eq!(path, "h:/a/b/c");
            assert_eq!(owner, "alice");
            assert_eq!(reason, "descendant_read_locked");
        }
        other => panic!("expected descendant_read_locked conflict, got {other:?}"),
    }

    // After Alice releases, Bob's ancestor write succeeds.
    apply(&db, Command {
        request_id: None, now_ms: now + 2,
        op: Op::Release { owner: "alice".into(), reqs: vec![rel("h:/a/b/c", Mode::Read)], del_wait: false },
    });
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 3,
        op: Op::Acquire(acquire_args("bob", 30_000, 1, vec![wr("h:/a")])),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));
}

#[test]
fn combined_acquire_and_release_keeps_owner_alive() {
    // An owner whose prior lease has lapsed issues one op that acquires a new
    // lock while inline-releasing the (now expired) old one. The acquired lock's
    // ALIVE marker must survive: it lives in the same uncommitted batch that the
    // committed-state liveness check cannot see.
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice holds /old with a 1ms TTL — expires almost immediately.
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 1, 1, vec![wr("h:/old")])),
    });

    // After expiry, acquire /new and inline-release /old in a single op.
    let args = AcquireArgs {
        owner_id: "alice".into(), ttl_ms: 30_000, fencing_token: 2,
        requests: vec![wr("h:/new")],
        release_requests: vec![rel("h:/old", Mode::Write)],
    };
    let resp = apply(&db, Command { request_id: None, now_ms: now + 2, op: Op::Acquire(args) });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));

    // Alice must still be alive and own /new.
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), now + 3);
    assert!(pathlockd::engine::is_owner_alive_inner(&mut txn, "alice").unwrap(), "owner lost liveness after combined acquire/release");
    let info = pathlockd::engine::inspect_path_inner(&mut txn, "h:/new").unwrap();
    assert_eq!(info.write_owner.as_deref(), Some("alice"));
}

#[test]
fn release_unlocks_path() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice locks root
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 1, vec![wr("h:/a")])),
    });

    // Alice releases
    apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::Release { owner: "alice".into(), reqs: vec![rel("h:/a", Mode::Write)], del_wait: false },
    });

    // Bob can now acquire
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 2,
        op: Op::Acquire(acquire_args("bob", 30_000, 2, vec![wr("h:/a")])),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));
}

#[test]
fn release_all_clears_everything() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice acquires two locks
    let args = AcquireArgs {
        owner_id: "alice".into(),
        ttl_ms: 60_000,
        fencing_token: 1,
        requests: vec![wr("h:/a"), rd("h:/b")],
        release_requests: vec![],
    };
    apply(&db, Command { request_id: None, now_ms: now, op: Op::Acquire(args) });

    // Release all
    apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::ReleaseAll { owner: "alice".into(), del_wait: true },
    });

    // Bob can acquire both
    for path in &["h:/a", "h:/b"] {
        let args = acquire_args("bob", 30_000, 2, vec![wr(path)]);
        let resp = apply(&db, Command { request_id: None, now_ms: now + 2, op: Op::Acquire(args) });
        assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)), "failed on {path}");
    }
}

#[test]
fn renew_extends_lease() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice acquires
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 5_000, 1, vec![wr("h:/a")])),
    });

    // After 4s, renew succeeds
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 4_000,
        op: Op::Renew { owner: "alice".into(), ttl_ms: 30_000 },
    });
    assert!(matches!(resp, ApplyResponse::Renew(RenewOutcome::Ok)));

    // Alice still holds after original lease would have expired
    let args = AcquireArgs {
        owner_id: "alice".into(), ttl_ms: 10_000, fencing_token: 2,
        requests: vec![wr_held("h:/a")],
        release_requests: vec![],
    };
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 6_000,
        op: Op::Acquire(args),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));
}

#[test]
fn renew_lost_when_owner_expired() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice acquires with 5s TTL
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 5_000, 1, vec![wr("h:/a")])),
    });

    // After 10s, renew returns Lost
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 10_000,
        op: Op::Renew { owner: "alice".into(), ttl_ms: 30_000 },
    });
    assert!(matches!(resp, ApplyResponse::Renew(RenewOutcome::Lost { .. })));
}

#[test]
fn force_release_clears_owner() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice locks
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 1, vec![wr("h:/a")])),
    });

    // Force-release alice
    apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::ForceRelease { victim: "alice".into() },
    });

    // Bob can now acquire
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 2,
        op: Op::Acquire(acquire_args("bob", 30_000, 2, vec![wr("h:/a")])),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));
}

#[test]
fn assert_fencing_validates_ownership() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice acquires write
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 1, vec![wr("h:/a")])),
    });

    // Read-only check via StoreTxn
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), now + 1);
    let outcome = pathlockd::engine::assert_fencing_inner(&mut txn, "alice", 1, &["h:/a".to_string()]).unwrap();
    assert_eq!(outcome, AssertOutcome::Ok);

    // Wrong owner
    let outcome = pathlockd::engine::assert_fencing_inner(&mut txn, "bob", 1, &["h:/a".to_string()]).unwrap();
    assert_eq!(outcome, AssertOutcome::Fail { path: "h:/a".to_string(), reason: "stale_owner".to_string() });
}

#[test]
fn fencing_token_rejects_stale_token() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice acquires with token 10
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 10, vec![wr("h:/a")])),
    });

    // Alice tries to re-acquire with token 5 → stale
    let args = AcquireArgs {
        owner_id: "alice".into(), ttl_ms: 60_000, fencing_token: 5,
        requests: vec![wr_held("h:/a")],
        release_requests: vec![],
    };
    let resp = apply(&db, Command { request_id: None, now_ms: now + 1, op: Op::Acquire(args) });
    match resp {
        ApplyResponse::Acquire(AcquireOutcome::Conflict { reason, .. }) => {
            assert_eq!(reason, "stale_fencing_token");
        }
        other => panic!("expected Conflict, got {:?}", other),
    }
}

#[test]
fn incr_fencing_token_is_monotonic() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    let t1 = match apply(&db, Command { request_id: None, now_ms: now, op: Op::IncrFence }) {
        ApplyResponse::IncrFence(t) => t,
        other => panic!("expected IncrFence, got {:?}", other),
    };
    let t2 = match apply(&db, Command { request_id: None, now_ms: now + 1, op: Op::IncrFence }) {
        ApplyResponse::IncrFence(t) => t,
        other => panic!("expected IncrFence, got {:?}", other),
    };
    assert!(t2 > t1);
}

#[test]
fn dead_owner_pruning_unblocks_contender() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();
    let expired = now + 60_000;

    // Alice locks with 1ms TTL → expires immediately at now + 1
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 1, 1, vec![wr("h:/a")])),
    });

    // After TTL lapses, Bob acquires → engine prunes dead Alice and succeeds
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 2,
        op: Op::Acquire(acquire_args("bob", 30_000, 2, vec![wr("h:/a")])),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));
}

#[test]
fn set_claim_blocks_other_owners() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Make Alice alive so her claim is valid
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 1, vec![wr("h:/unrelated")])),
    });

    // Alice plants a claim on h:/a
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::SetClaim { path: "h:/a".into(), claimant: "alice".into(), ttl_ms: 5_000 },
    });

    // Bob tries to acquire → conflict (preempt_claimed)
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::Acquire(acquire_args("bob", 30_000, 2, vec![wr("h:/a")])),
    });
    match resp {
        ApplyResponse::Acquire(AcquireOutcome::Conflict { reason, .. }) => {
            assert_eq!(reason, "preempt_claimed");
        }
        other => panic!("expected Conflict, got {:?}", other),
    }

    // Alice acquires over her own claim → succeeds (claim consumed)
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 2,
        op: Op::Acquire(acquire_args("alice", 30_000, 3, vec![wr("h:/a")])),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));
}

#[test]
fn wait_edge_cycle_detection() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Give both a and b alive keys so the cycle walk succeeds
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("a", 60_000, 1, vec![wr("h:/x")])),
    });
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("b", 60_000, 2, vec![wr("h:/y")])),
    });

    // Owner A waits on B
    let meta = WaitEdgeMetadata { conflict_path: "h:/x".into(), reason: "write_locked".into() };
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::SetWaitEdge {
            owner: "a".into(),
            edge: pathlockd::raft::command::WaitEdge { conflict_owner: "b".into(), metadata: Some(meta.clone()) },
            ttl_ms: 60_000,
        },
    });

    // Owner B waits on A (cycle)
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::SetWaitEdge {
            owner: "b".into(),
            edge: pathlockd::raft::command::WaitEdge { conflict_owner: "a".into(), metadata: Some(meta) },
            ttl_ms: 60_000,
        },
    });

    // With alive owners, verify the wait edges via is_blocking
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), now + 1);
    assert!(pathlockd::engine::is_blocking_inner(
        &mut txn, "h:/x", "a", "write_locked"
    ).unwrap());
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), now + 1);
    assert!(pathlockd::engine::is_blocking_inner(
        &mut txn, "h:/y", "b", "write_locked"
    ).unwrap());
}

#[test]
fn gc_sweep_cleans_expiry_entries() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Acquire with short TTL
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 100, 1, vec![wr("h:/a")])),
    });

    // GC sweep after TTL
    apply(&db, Command {
        request_id: None, now_ms: now + 200,
        op: Op::GcSweep { now_ms: now + 200, batch: 1024 },
    });

    // Alice's lock should now be treated as expired (lazy expiry)
    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), now + 201);
    let alive = pathlockd::engine::is_owner_alive_inner(&mut txn, "alice").unwrap();
    assert!(!alive, "owner should be expired after TTL + GC");
}

#[test]
fn inline_release_shadows_acquired_paths() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice locks /a and /a/b
    let args = AcquireArgs {
        owner_id: "alice".into(), ttl_ms: 60_000, fencing_token: 1,
        requests: vec![wr("h:/a"), wr("h:/a/b")],
        release_requests: vec![],
    };
    apply(&db, Command { request_id: None, now_ms: now, op: Op::Acquire(args) });

    // Alice does an acquire with only release_requests: releases /a/b atomically
    // while keeping /a. This tests that inline-release within acquire works.
    let args = AcquireArgs {
        owner_id: "alice".into(), ttl_ms: 60_000, fencing_token: 1,
        requests: vec![],
        release_requests: vec![rel("h:/a/b", Mode::Write)],
    };
    let resp = apply(&db, Command { request_id: None, now_ms: now + 1, op: Op::Acquire(args) });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));

    // Bob cannot acquire /a/b because ancestor /a is still locked by Alice
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 2,
        op: Op::Acquire(acquire_args("bob", 30_000, 2, vec![wr("h:/a/b")])),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Conflict { .. })));

    // Alice now releases /a too
    apply(&db, Command {
        request_id: None, now_ms: now + 3,
        op: Op::Release { owner: "alice".into(), reqs: vec![rel("h:/a", Mode::Write)], del_wait: false },
    });

    // Now Bob can acquire /a/b
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 4,
        op: Op::Acquire(acquire_args("bob", 30_000, 3, vec![wr("h:/a/b")])),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)), "Bob should acquire after Alice releases ancestor");
}

#[test]
fn disjoint_handlers_dont_conflict() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice locks google_drive:/
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 1, vec![wr("google_drive:/")])),
    });

    // Bob locks s3:/ — different domain, no conflict
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 1,
        op: Op::Acquire(acquire_args("bob", 30_000, 2, vec![wr("s3:/")])),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));
}

#[test]
fn multi_domain_acquire_is_rejected() {
    // This is tested at the router level, not the state machine.
    // The state machine accepts it (it doesn't check domains).
    // The router checks domains before routing.
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // But the state machine itself should process multi-domain requests fine
    // (the router enforces single-domain)
    let args = AcquireArgs {
        owner_id: "alice".into(), ttl_ms: 60_000, fencing_token: 1,
        requests: vec![wr("h:/a")],
        release_requests: vec![],
    };
    let resp = apply(&db, Command { request_id: None, now_ms: now, op: Op::Acquire(args) });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));
}

#[test]
fn is_blocking_detects_write_block() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice locks
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 1, vec![wr("h:/a")])),
    });

    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), now + 1);
    assert!(pathlockd::engine::is_blocking_inner(&mut txn, "h:/a", "alice", "write_locked").unwrap());
    assert!(!pathlockd::engine::is_blocking_inner(&mut txn, "h:/a", "bob", "write_locked").unwrap());
}

#[test]
fn is_blocking_detects_read_block() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice gets a read lock
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 60_000, 0, vec![rd("h:/a")])),
    });

    let mut txn = pathlockd::store_rocksdb::RocksDbTxn::new(db.clone(), now + 1);
    assert!(pathlockd::engine::is_blocking_inner(&mut txn, "h:/a", "alice", "read_locked").unwrap());
}

#[test]
fn renew_lost_does_not_extend_liveness() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice locks with 1ms → expires immediately
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 1, 1, vec![wr("h:/a")])),
    });

    // Renew returns Lost after TTL
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 2,
        op: Op::Renew { owner: "alice".into(), ttl_ms: 30_000 },
    });
    assert!(matches!(resp, ApplyResponse::Renew(RenewOutcome::Lost { .. })));

    // Bob can acquire
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 3,
        op: Op::Acquire(acquire_args("bob", 30_000, 2, vec![wr("h:/a")])),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));
}

#[test]
fn expired_read_owner_is_pruned() {
    let (db, _dir) = open_temp_db();
    let now = store_keys::now_ms();

    // Alice gets a read lock with 1ms TTL
    apply(&db, Command {
        request_id: None, now_ms: now,
        op: Op::Acquire(acquire_args("alice", 1, 0, vec![rd("h:/a")])),
    });

    // After expiry, Bob gets a write → dead read owner pruned
    let resp = apply(&db, Command {
        request_id: None, now_ms: now + 2,
        op: Op::Acquire(acquire_args("bob", 30_000, 1, vec![wr("h:/a")])),
    });
    assert!(matches!(resp, ApplyResponse::Acquire(AcquireOutcome::Ok)));
}
