use proto_flow::shuffle;

/// Immutable queue configuration: session identity and member topology.
/// Set once during Queue startup and never modified.
pub struct Topology {
    /// Unique identifier for this session, assigned by the coordinator.
    pub session_id: u64,
    /// Ordered member topology: each member owns a disjoint key range.
    pub members: Vec<shuffle::Member>,
    /// Index of this Queue RPC within `members`.
    pub queue_member_index: u32,
}
