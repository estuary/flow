use proto_flow::shuffle;

/// Immutable log configuration: session identity and member topology.
/// Set once during Log startup and never modified.
pub struct Topology {
    /// Unique identifier for this session, assigned by the coordinator.
    pub session_id: u64,
    /// Ordered member topology: each member owns a disjoint key range.
    pub members: Vec<shuffle::Member>,
    /// Index of this Log RPC within `members`.
    pub log_member_index: u32,
}
