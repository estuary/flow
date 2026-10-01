//! Concerns shared by Slices and Logs in merging shuffled documents:
//! merge order (`Position`), a Slice's merge constraint (`Constraint`), and
//! the round and byte windows of Slice-to-Log flow control.
//! See "Rounds and Merge Constraints" and "Log Merge and Output"
//! of the crate README.

/// Position of a document within the merge order of shuffled documents:
/// priority DESC, then adjusted clock (clock + read_delay) ASC.
/// `Ord` follows merge order, so a position which merges earlier is Less.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Position {
    pub priority: i32,
    pub adjusted_clock: proto_gazette::uuid::Clock,
}

impl Position {
    /// The position which merges before all others.
    pub const MIN: Self = Self {
        priority: i32::MAX,
        adjusted_clock: proto_gazette::uuid::Clock::zero(),
    };

    /// The position which merges after all others.
    pub const MAX: Self = Self {
        priority: i32::MIN,
        adjusted_clock: proto_gazette::uuid::Clock::from_u64(u64::MAX),
    };

    /// Merge position of a document having `priority`, `clock`, and `read_delay`.
    /// Slices (`Binding::merge_position`) and Logs (`from_append`) must agree
    /// exactly. The adjusted clock saturates rather than overflows, because
    /// Logs compute it from peer-supplied Append fields.
    pub fn new(priority: i32, clock: u64, read_delay: u64) -> Self {
        Self {
            priority,
            adjusted_clock: proto_gazette::uuid::Clock::from_u64(clock.saturating_add(read_delay)),
        }
    }

    /// Merge position of an Append, as its Binding orders it.
    pub fn from_append(append: &proto_flow::shuffle::log_request::Append) -> Self {
        Self::new(append.priority, append.clock, append.read_delay)
    }

    /// The minimally-later position within the same priority.
    pub fn successor(self) -> Self {
        Self {
            priority: self.priority,
            adjusted_clock: proto_gazette::uuid::Clock::from_u64(
                self.adjusted_clock.as_u64().saturating_add(1),
            ),
        }
    }
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

/// A Slice's merge constraint: what it tells Logs of the Appends it may yet send.
/// A Log may merge an Append only if it's at or before the constraint of every
/// other Slice lacking a queued Append.
///
/// Slices and Logs both begin at `Constraint::INITIAL`, which is implied and
/// needn't be sent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Constraint {
    /// The Slice's position: it exceeds every Append the Slice has sent which
    /// a Log may not yet have merged, and its next Appends are expected at or
    /// after it. They may occasionally fall below it (a replay, a newly-started
    /// read, or higher-priority documents), and Logs merge those on arrival.
    At(Position),
    /// All reads are tailing and no documents are ready. The Slice places no
    /// constraint on Log merges: its future Appends are live data, which Logs
    /// don't await.
    Tailing,
}

impl Constraint {
    /// Constraint of a Slice which knows nothing of its position.
    pub const INITIAL: Self = Self::At(Position::MIN);

    pub fn from_proto(constraint: &proto_flow::shuffle::log_request::MergeConstraint) -> Self {
        if constraint.tailing {
            return Self::Tailing;
        }
        Self::At(Position {
            priority: constraint.priority,
            adjusted_clock: proto_gazette::uuid::Clock::from_u64(constraint.adjusted_clock),
        })
    }

    pub fn to_proto(self) -> proto_flow::shuffle::log_request::MergeConstraint {
        let (position, tailing) = match self {
            Self::At(position) => (position, false),
            Self::Tailing => (Position::MIN, true),
        };
        proto_flow::shuffle::log_request::MergeConstraint {
            priority: position.priority,
            adjusted_clock: position.adjusted_clock.as_u64(),
            tailing,
        }
    }
}

/// Maximum dequeues by a Slice's round, or a Log's merge, before the actor
/// yields to service its other events, and then to the tokio runtime.
///
/// A Slice yields so that a long run of documents which aren't appended
/// (duplicates, filtered documents, or ACKs) still keeps Logs informed of its
/// progress, and to regularly service other Session and Journal I/O.
///
/// A Log yields so that it reads Slices' rounds and sends Flushed responses
/// while a long merge runs. Each bounds how long a ready flush awaits.
pub(crate) const MAX_DEQUEUES: usize = 1024;

/// Flow-control window of Append bytes (`append_bytes`) between each Slice and Log.
///
/// It's a Slice's send window: the Slice may have this much outstanding to a
/// Log, queued into a round or in flight in its channel until the receiver
/// takes the round, which acks it (`slice::rounds::LogChannel`), or any single
/// Append if nothing else is outstanding.
///
/// It's also a Log's receive window: the Log reads ahead this much of each
/// Slice's Appends before its merge (`log::read_ahead::SliceReadAhead`). A Log
/// reads a whole round while its window is open, which may overfill it by up
/// to a round. A Slice's lead over the slowest Slice feeding a Log is
/// therefore bounded by about three windows.
pub(crate) const APPEND_WINDOW_BYTES: usize = 4 * 1024 * 1024;

/// Bytes of an Append accounted against `APPEND_WINDOW_BYTES`. The fixed size
/// of an Append dominates small documents. Documents and packed keys are
/// shared `Bytes`, and are counted in full by the window of each Log they're
/// sent to.
/// Journal name suffixes are delta-encoded, usually empty, and not counted.
pub(crate) fn append_bytes(doc_archived: &[u8], packed_key: &[u8]) -> usize {
    std::mem::size_of::<proto_flow::shuffle::log_request::Append>()
        + doc_archived.len()
        + packed_key.len()
}
