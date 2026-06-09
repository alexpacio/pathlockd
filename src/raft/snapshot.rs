//! RocksDB checkpoint/restore snapshots.

/// Snapshot data for a single Raft group.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SnapshotData {
    pub last_applied: Option<u64>,
}

/// A snapshot that can be installed on a follower.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub group_id: u64,
    pub data: SnapshotData,
}
