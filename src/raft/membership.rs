//! Controlled add-learner / promote / remove-voter flow for Raft group membership.

/// Tracks membership changes for a single Raft group.
pub struct MembershipManager {
    pub group_id: u64,
}

impl MembershipManager {
    pub fn new(group_id: u64) -> Self {
        Self { group_id }
    }
}
