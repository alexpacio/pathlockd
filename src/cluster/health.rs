//! Readiness and liveness checks based on Raft and RocksDB health.


/// Health status for the local node.
#[derive(Debug, Clone)]
pub struct HealthStatus {
    pub ready: bool,
    pub detail: String,
}

impl HealthStatus {
    pub fn ready() -> Self {
        Self {
            ready: true,
            detail: "ready".into(),
        }
    }

    pub fn not_ready(reason: impl Into<String>) -> Self {
        Self {
            ready: false,
            detail: reason.into(),
        }
    }
}

/// Check whether the local node is ready to serve.
///
/// Ready if:
/// - RocksDB opened all local groups
/// - gossip started
/// - internal raft transport started
/// - g_sys has known leader
/// - enough groups have leader/quorum
pub fn check_ready() -> HealthStatus {
    // P0-P2 stub: always ready
    HealthStatus::ready()
}
