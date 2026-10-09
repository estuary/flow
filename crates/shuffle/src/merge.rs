//! Concerns shared by Slices and Logs in merging shuffled documents:
//! merge order (`Position`), and the Append credits of Slice-to-Log flow
//! control. See "Rounds and Credits" and "Log Merge and Output" of the
//! crate README.

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

    /// Merge position of an Append of a Slice having lane `priority`.
    pub fn from_append(priority: i32, append: &proto_flow::shuffle::log_request::Append) -> Self {
        Self {
            priority,
            adjusted_clock: adjusted_clock(append),
        }
    }
}

/// Adjusted clock (clock + read_delay) of an Append, as both its Slice and
/// Log compute it. It saturates rather than overflows, because Logs compute
/// it from peer-supplied Append fields.
pub fn adjusted_clock(
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

/// Maximum dequeues by a Slice's round, or a Log's merge, before the actor
/// yields to service its other events, and then to the tokio runtime.
///
/// A Slice yields so that a long run of documents which aren't appended
/// (duplicates, filtered documents, or ACKs) still regularly services other
/// Session and Journal I/O.
///
/// A Log yields so that it reads Slices' rounds, returns their credits, and
/// sends Flushed responses while a long merge runs. Each bounds how long a
/// ready flush awaits.
pub(crate) const MAX_DEQUEUES: usize = 1024;

/// Credits of Append bytes (`append_bytes`) between each Slice and Log.
///
/// A Slice may have this much outstanding to a Log: queued into a round, in
/// flight, or read ahead by the Log and not yet merged. The Log returns
/// credits as it merges (`LogResponse.Acked`). Any single Append fits if
/// nothing else is outstanding, which allows one oversize document at a time.
///
/// It bounds the memory a Log holds for each Slice's read-ahead
/// (`log::read_ahead::SliceReadAhead`), which a Log enforces, and a Slice's
/// lead over its peers at a Log. A Log advertises it to each Slice
/// (`LogResponse.Opened`), and a Slice uses each Log's advertised budget,
/// so a Log's value alone governs its Slices. Credits don't bound rounds
/// themselves, which a Log always reads (`slice::rounds::LogChannel`).
pub(crate) const APPEND_CREDIT_BYTES: u64 = 4 * 1024 * 1024;

/// Bytes accounted for each Append against Append credits, in addition to its
/// document and packed key: its fixed size, which dominates small documents.
/// Like `APPEND_CREDIT_BYTES`, a Log advertises it to each Slice
/// (`LogResponse.Opened`), so Slices and Logs always agree on accounting.
pub(crate) const APPEND_OVERHEAD_BYTES: u64 =
    std::mem::size_of::<proto_flow::shuffle::log_request::Append>() as u64;

/// Bytes of an Append accounted against Append credits, given the per-Append
/// `overhead` advertised by its Log. Documents and packed keys are shared
/// `Bytes`, and are counted in full by the credits of each Log they're sent to.
/// Journal name suffixes are delta-encoded, usually empty, and not counted.
pub(crate) fn append_bytes(overhead: u64, doc_archived: &[u8], packed_key: &[u8]) -> u64 {
    overhead + doc_archived.len() as u64 + packed_key.len() as u64
}
