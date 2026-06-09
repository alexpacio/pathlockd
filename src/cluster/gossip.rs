//! foca/SWIM membership and health hints.
//!
//! SWIM/foca handles node discovery, failure suspicion, health gossip, and
//! routing hint dissemination. It does NOT commit group membership, release
//! locks, or declare lock owner death — Raft remains authoritative for
//! correctness.

use std::collections::HashSet;
use std::net::SocketAddr;

use tokio::sync::watch;

/// The set of currently active (non-suspected) cluster members according to SWIM.
#[derive(Clone)]
pub struct ClusterMembers {
    /// Sender to broadcast membership changes.
    tx: watch::Sender<HashSet<u64>>,
    /// Local node ID.
    pub node_id: u64,
}

impl ClusterMembers {
    pub fn new(node_id: u64) -> (Self, watch::Receiver<HashSet<u64>>) {
        let (tx, rx) = watch::channel(HashSet::new());
        (Self { tx, node_id }, rx)
    }

    pub fn update(&self, members: HashSet<u64>) {
        let _ = self.tx.send(members);
    }
}

/// Start the SWIM gossip layer. In P0-P2 this is a stub that maintains a static
/// member set. In P3, foca runs a real SWIM protocol.
pub async fn start_gossip(
    node_id: u64,
    _gossip_addr: SocketAddr,
    seed_nodes: Vec<String>,
) -> anyhow::Result<ClusterMembers> {
    let (members, _rx) = ClusterMembers::new(node_id);

    // P0-P2 stub: add self and seed nodes as static members
    let mut set = HashSet::new();
    set.insert(node_id);
    // For now, seed nodes are just strings; in P3, SWIM resolves and joins them.
    for seed in seed_nodes {
        if let Some(id_str) = seed.split('.').next() {
            if let Ok(id) = id_str.parse::<u64>() {
                set.insert(id);
            }
        }
    }
    members.update(set);

    Ok(members)
}
