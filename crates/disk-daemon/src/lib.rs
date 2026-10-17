pub mod client;
pub mod daemon;

mod attachment;
mod chunk;
mod device;
mod filesystem;
mod horizon;
mod image;
mod journal;
mod ublk;

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
