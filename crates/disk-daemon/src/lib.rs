//! Linux block devices whose durable state lives in Gazette journals.
//!
//! The daemon exposes disposable sparse images through `ublk` and records block
//! mutations. [`client`] coordinates disk commits with the caller's transactions
//! over a tenure gRPC stream. See the crate README for the module map and setup.

pub mod args;
pub mod client;
pub mod daemon;

mod chunk;
mod device;
mod filesystem;
mod horizon;
mod image;
mod journal;
mod tenure;
mod ublk;

/// Broker fixtures and replay helpers shared by unit tests.
#[cfg(test)]
mod test_support;

/// Tenure and journal-record protocol messages, generated from
/// `go/protocols/disk/disk.proto`.
pub use proto_flow::disk as proto;

/// Fixed granularity of chunks, hole punching, bitmaps, and ext4. Varying it
/// would require persisting and restoring it to interpret journal blocks.
pub const BLOCK_SIZE: u32 = 4096;

/// Required journal content type, preventing disk records from being appended
/// to a collection journal or shard recovery log.
pub const CONTENT_TYPE_DISK: &str = "application/x-journal-backed-disk";

/// Journal label holding the earliest offset needed to rebuild the disk.
/// Activation preserves it as a data-plane label; external fragment pruners use
/// it to identify obsolete history.
pub const DISK_RECOVERY_FLOOR: &str = labels::DISK_RECOVERY_FLOOR;

/// Encode an offset as 16 lowercase hex digits, preserving numeric order under
/// string comparison for monotonic floor updates.
pub fn recovery_floor_value(offset: u64) -> String {
    format!("{offset:016x}")
}

/// Parse a [`DISK_RECOVERY_FLOOR`] value back into its journal offset.
pub fn parse_recovery_floor(value: &str) -> std::result::Result<u64, std::num::ParseIntError> {
    u64::from_str_radix(value, 16)
}

/// A failure of something the tenure asked for, which ends its stream with
/// `INVALID_ARGUMENT`: no retry of the same request can succeed.
pub(crate) fn invalid_argument(message: String) -> anyhow::Error {
    proto_grpc::status_to_anyhow(tonic::Status::invalid_argument(message))
}
