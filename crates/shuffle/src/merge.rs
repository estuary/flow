//! Merge order of shuffled documents (`Position`), which Slices and Logs share.
//! See "Log Merge and Output" of the crate README.

/// Position of a document within the merge order of shuffled documents:
/// priority DESC, then adjusted clock (clock + read_delay) ASC.
/// `Ord` follows merge order, so a position which merges earlier is Less.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Position {
    pub priority: i32,
    pub adjusted_clock: proto_gazette::uuid::Clock,
}

impl Position {
    /// The position which merges after all others.
    pub const MAX: Self = Self {
        priority: i32::MIN,
        adjusted_clock: proto_gazette::uuid::Clock::from_u64(u64::MAX),
    };

    /// Merge position of an Append.
    pub const fn from_append(append: &proto_flow::shuffle::log_request::Append) -> Self {
        Self {
            priority: append.priority,
            adjusted_clock: adjusted_clock(append),
        }
    }
}

/// Adjusted clock (clock + read_delay) of an Append, as both its Slice and
/// Log compute it. It saturates rather than overflows, because Logs compute
/// it from peer-supplied Append fields.
pub const fn adjusted_clock(
    append: &proto_flow::shuffle::log_request::Append,
) -> proto_gazette::uuid::Clock {
    proto_gazette::uuid::Clock::from_u64(append.clock.saturating_add(append.read_delay))
}

impl Ord for Position {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .priority
            .cmp(&self.priority)
            .then(self.adjusted_clock.cmp(&other.adjusted_clock))
    }
}

impl PartialOrd for Position {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
