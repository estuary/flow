//! Serves block devices whose durable state lives in Gazette journals.
//!
//! The daemon exposes a sparse local image through Linux `ublk`. It appends
//! every device mutation it accepts to a per-disk journal, as that mutation
//! arrives. A client then advances the disk over its tenure gRPC, atomically
//! with the client's own commit. The local image is disposable. The journal is
//! the disk.
//!
//! [`client`] is the caller's side of that gRPC. What sits beside it here is what a
//! caller and the daemon must agree on without a request. Everything else is private.
//!
//! The crate README is the durable design and operating record.

pub mod args;
pub mod client;
pub mod daemon;

mod bitmap;
mod capture;
mod chunk;
mod device;
mod failure;
mod filesystem;
mod horizon;
mod image;
mod journal;
mod serving;
mod tenure;
mod ublk;
mod wake;

/// Prerequisites of the crate's own tests: a real `ublk` device, and a real broker.
/// The cases which use them live beside the code they cover.
#[cfg(test)]
mod test_support;

/// Tenure and journal-record protocol messages, generated from
/// `go/protocols/disk/disk.proto`.
pub use proto_flow::disk as proto;

/// Block size of every disk, in bytes. It is the granularity of the chunk
/// encoding, of hole punching, of the daemon's bitmaps, and of the ext4 it
/// formats.
///
/// It is a constant rather than a per-disk input. A block size which varied
/// would be a durable fact of each disk, one every later `Open` had to present
/// again or else misplace every chunk a replay applies. 4 KiB is the page size
/// of the hosts this daemon runs on and the ext4 default, so nothing was buying
/// that risk.
pub const BLOCK_SIZE: u32 = 4096;

/// Value of Gazette's `content-type` label which a journal must carry to serve as a
/// disk, alongside `application/x-gazette-recoverylog` and the rest.
///
/// Gazette requires a content type of a journal which serves as a shard recovery
/// log, and this is the same rule for the same reason: it is what keeps an `Open`
/// of some other journal from fencing it and appending disk records over content it
/// cannot read.
pub const CONTENT_TYPE_DISK: &str = "application/x-journal-backed-disk";

/// Flow's label naming a disk journal's recovery floor: the offset of the earliest
/// record a replay must read to rebuild the disk.
///
/// It is defined here so that a client of a disk, or whatever prunes its journal's
/// fragments, finds it beside the other constants the client and daemon agree on.
pub const DISK_RECOVERY_FLOOR: &str = "estuary.dev/disk-recovery-floor";

/// Format `offset` as a [`DISK_RECOVERY_FLOOR`] value: fixed-width, 16-character
/// lowercase hex, so that comparing two values as strings compares the offsets they
/// carry. The label is therefore advanced by a string comparison, and only ever
/// moves forward.
pub fn recovery_floor_value(offset: u64) -> String {
    format!("{offset:016x}")
}

/// Parse a [`DISK_RECOVERY_FLOOR`] value back into its journal offset.
pub fn parse_recovery_floor(value: &str) -> std::result::Result<u64, std::num::ParseIntError> {
    u64::from_str_radix(value, 16)
}
