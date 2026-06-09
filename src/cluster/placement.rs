//! HRW (Rendezvous Hashing) group placement and leader balancing.
//!
//! Placement assigns each VFS lock domain to a Raft group using consistent
//! hashing. Each group has `replication_factor` voters placed across the
//! available nodes.

use xxhash_rust::xxh3::xxh3_64;

/// Compute the Raft group ID for a given lock domain using HRW.
pub fn place_domain(domain: &str, group_count: u32) -> u64 {
    let mut best_group = 0u64;
    let mut best_weight = 0u64;

    for g in 0..group_count {
        let seed = (g as u64).to_le_bytes();
        let mut buf = Vec::with_capacity(seed.len() + domain.len());
        buf.extend_from_slice(&seed);
        buf.extend_from_slice(domain.as_bytes());
        let weight = xxh3_64(&buf);
        if weight > best_weight {
            best_weight = weight;
            best_group = g as u64;
        }
    }

    best_group
}

/// Select the voters for a given group using HRW across all available nodes.
pub fn select_voters(group_id: u64, nodes: &[u64], replication_factor: u32) -> Vec<u64> {
    let mut weights: Vec<(u64, u64)> = nodes
        .iter()
        .map(|&node_id| {
            let seed = group_id.to_le_bytes();
            let node_bytes = node_id.to_le_bytes();
            let mut buf = Vec::with_capacity(seed.len() + node_bytes.len());
            buf.extend_from_slice(&seed);
            buf.extend_from_slice(&node_bytes);
            let weight = xxh3_64(&buf);
            (node_id, weight)
        })
        .collect();

    weights.sort_by_key(|&(_, weight)| std::cmp::Reverse(weight));
    weights
        .into_iter()
        .take(replication_factor as usize)
        .map(|(id, _)| id)
        .collect()
}

/// Special system group ID for fencing tokens and cluster-wide metadata.
pub const SYS_GROUP_ID: u64 = u64::MAX;
