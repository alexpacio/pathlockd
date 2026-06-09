//! openraft log/vote storage over RocksDB.
//!
//! This module defines the type configuration and storage stubs for openraft.
//! In P1-P2 single-process mode, the state machine is called directly without
//! going through the full Raft protocol. Full Raft integration occurs in P3.

use std::sync::Arc;

use rocksdb::DB;

use crate::raft::command::{ApplyResponse, Command};

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Command,
        R = ApplyResponse,
        Node = openraft::BasicNode,
);

/// Storage adapter for a single Raft group backed by RocksDB.
pub struct Store {
    pub db: Arc<DB>,
}

impl Store {
    pub fn new(db: Arc<DB>) -> Self {
        Self { db }
    }
}

/// A minimal Raft node that wraps the Storage and Network layers.
/// In P1-P2, this is not used; the state machine is called directly.
pub struct RaftNode {
    pub group_id: u64,
    pub db: Arc<DB>,
}

impl RaftNode {
    pub fn new(group_id: u64, db: Arc<DB>) -> Self {
        Self { group_id, db }
    }
}
