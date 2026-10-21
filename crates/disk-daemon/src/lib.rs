pub mod client;
pub mod daemon;

mod attachment;
mod device;
mod filesystem;
mod journal;

/// Broker fixtures and replay helpers shared by unit tests.
#[cfg(test)]
mod test_support;

/// Attach RPC and journal-record protocol messages, generated from
/// `go/protocols/disk/disk.proto`.
pub use proto_flow::disk as proto;

/// Fixed granularity of chunks, hole punching, bitmaps, and ext4. Varying it
/// would require persisting and restoring it to interpret journal blocks.
pub const BLOCK_SIZE: u32 = 4096;

/// Required journal content type, preventing disk records from being appended
/// to a collection journal or shard recovery log.
pub const CONTENT_TYPE_DISK: &str = "application/x-journal-backed-disk";

/// A chunk the live device applied, which the journal's writer records: a device
/// write or punch, or a horizon copy.
pub(crate) struct LiveChunk {
    /// The blocks it writes or punches.
    pub chunk: proto::Chunk,
    /// A recovery horizon opened at this chunk.
    pub opens_horizon: bool,
    /// This chunk discharged the open horizon's last pending block.
    pub discharges_horizon: bool,
}

/// A committed chunk the journal replayed, which the device's rebuilder applies.
/// It carries no discharge: the device's compactor finds each one again as it
/// applies the chunks.
pub(crate) struct ReplayedChunk {
    /// The blocks it writes or punches.
    pub chunk: proto::Chunk,
    /// Its record opened a recovery horizon.
    pub opens_horizon: bool,
}

/// A failure of something the attachment asked for, which fails the attachment with
/// `INVALID_ARGUMENT`: no retry of the same request can succeed.
pub(crate) fn invalid_argument(message: String) -> anyhow::Error {
    proto_grpc::status_to_anyhow(tonic::Status::invalid_argument(message))
}

/// A request which the attachment's state does not allow, which fails the attachment
/// with `FAILED_PRECONDITION`.
pub(crate) fn failed_precondition(message: &str) -> anyhow::Error {
    proto_grpc::status_to_anyhow(tonic::Status::failed_precondition(message))
}

/// Work given up because its attachment was cancelled, which fails the attachment
/// with `UNAVAILABLE`.
pub(crate) fn cancelled() -> anyhow::Error {
    proto_grpc::status_to_anyhow(tonic::Status::unavailable("the attachment was cancelled"))
}
