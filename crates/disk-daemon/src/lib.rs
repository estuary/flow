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
