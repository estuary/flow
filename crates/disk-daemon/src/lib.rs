//! Serves block devices whose durable state lives in Gazette journals.
//!
//! The daemon exposes a sparse local image through Linux `ublk`. It appends
//! every device mutation it accepts to a per-disk journal, as that mutation
//! arrives. A client then advances the disk over its session gRPC, atomically
//! with the client's own commit. The local image is disposable. The journal is
//! the disk.
//!
//! The crate README is the durable design and operating record.

pub mod args;
pub mod bitmap;
pub mod capture;
pub mod chunk;
pub mod client;
pub mod daemon;
pub mod disk;
pub mod filesystem;
pub mod horizon;
pub mod image;
pub mod inflight;
pub mod journal;
pub mod metrics;
pub mod owner;
pub mod session;
pub mod ublk;
pub mod wake;

/// Session and journal-record protocol messages, generated from
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

/// A failure caused by what a session asked for, rather than by this daemon,
/// its host, or its brokers.
///
/// A retry of an invalid request can never succeed. A retry after a broker
/// outage or a lost fence can. [`session`] therefore reports this as
/// `INVALID_ARGUMENT`. It travels as an error cause rather than as text, so it
/// survives `anyhow::Context`.
#[derive(Debug)]
pub struct Invalid(pub String);

impl std::fmt::Display for Invalid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Invalid {}

/// A request which is well-formed, but out of turn for what the session owes.
///
/// A second `Prepare` before its `Commit`, a `Commit` with nothing prepared, or a
/// `Commit` of bytes no `Prepare` returned, is a client which lost track of the
/// delta it owes. No retry of it can succeed, but nothing about the request itself
/// is wrong, so [`session`] reports this as `FAILED_PRECONDITION` rather than
/// `INVALID_ARGUMENT`. It travels as an error cause so that it survives
/// `anyhow::Context`.
#[derive(Debug)]
pub struct OutOfOrder(pub String);

impl std::fmt::Display for OutOfOrder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for OutOfOrder {}

/// `anyhow::ensure!` for a rule a session broke. A validator states each rule in
/// one place, and the session stream reports the right code for it.
macro_rules! ensure_valid {
    ($condition:expr, $($message:tt)*) => {
        if !$condition {
            return Err(::anyhow::Error::new($crate::Invalid(format!($($message)*))));
        }
    };
}

pub(crate) use ensure_valid;
