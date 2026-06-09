//! Internal Raft RPC transport over tonic/gRPC.
//!
//! In P3, this module provides the gRPC service that carries openraft's
//! `append_entries` and `install_snapshot` RPCs between nodes.

/// gRPC-based Raft network transport.
pub struct RaftNetwork {
    pub node_id: u64,
}

impl RaftNetwork {
    pub fn new(node_id: u64) -> Self {
        Self { node_id }
    }
}
