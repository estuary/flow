//! Serves block devices whose durable state lives in Gazette journals.
//!
//! The daemon exposes a sparse local image through Linux `ublk`, appends every
//! accepted device mutation to a per-disk journal as it arrives, and advances
//! the disk atomically with an external commit driven over its session gRPC.
//! The local image is disposable and the journal is the disk.
//!
//! See the crate README for what exists today, and
//! `plans/block-backed-connector-disks.md` for the design.

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

/// A failure caused by what a session asked for, rather than by this daemon,
/// its host, or its brokers.
///
/// It is the one distinction a message cannot carry: an invalid request cannot
/// succeed however often it is retried, while a broker outage or a lost fence
/// may. [`session`] reports it as `INVALID_ARGUMENT`, and it survives
/// `anyhow::Context` because it travels as a cause rather than as text.
#[derive(Debug)]
pub struct Invalid(pub String);

impl std::fmt::Display for Invalid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Invalid {}

/// `anyhow::ensure!` for a rule a session broke, so that a validator states its
/// rule in one place and the session stream reports the right code for it.
macro_rules! ensure_valid {
    ($condition:expr, $($message:tt)*) => {
        if !$condition {
            return Err(::anyhow::Error::new($crate::Invalid(format!($($message)*))));
        }
    };
}

pub(crate) use ensure_valid;
