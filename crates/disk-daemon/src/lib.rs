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

#![allow(dead_code)] // Until the tenure service assembles these modules.

mod bitmap;
mod chunk;
mod horizon;
mod image;
mod ublk;

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
