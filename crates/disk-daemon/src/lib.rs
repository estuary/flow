mod device;
mod journal;
mod replay;

/// Attach RPC and journal-record protocol messages.
pub use proto_flow::disk as proto;

/// Fixed granularity of chunks, hole punching, bitmaps, and ext4.
pub const BLOCK_SIZE: u32 = 4096;

/// Required journal content type.
pub const CONTENT_TYPE_DISK: &str = "application/x-journal-backed-disk";

/// A chunk the live device applied, which the promoted attachment appends to its
/// journal: a device write or punch, or a horizon copy.
///
/// Installing a floor takes two chunks, because the device never learns where a
/// served chunk's record lands. A candidate's record lands at an offset the writer
/// confirms and remembers. A later chunk names it as the floor once recovery from
/// it would be complete, and the writer stores that floor when the naming chunk's
/// delta commits. A chunk may be both, and then it names itself.
pub(crate) struct LiveChunk {
    /// The blocks it writes or punches.
    pub chunk: proto::Chunk,
    /// This chunk's own record may become the floor later, but isn't one yet.
    pub floor_candidate: bool,
    /// The floor this chunk's delta installs once it commits: an earlier
    /// candidate's record, a record replay found, or this chunk's own if it's
    /// also a candidate.
    pub floor: Option<Floor>,
}

/// A recovery floor which a live chunk names.
#[derive(Debug, PartialEq)]
pub(crate) enum Floor {
    /// The record of the last floor candidate the writer appended.
    LastCandidate,
    /// A record replay found at this journal offset, which an earlier primary
    /// appended.
    Replayed(i64),
}

/// A failure of something the attachment asked for, which fails the attachment with
/// `INVALID_ARGUMENT`: no retry of the same request can succeed.
pub(crate) fn invalid_argument(message: String) -> anyhow::Error {
    proto_grpc::status_to_anyhow(tonic::Status::invalid_argument(message))
}
