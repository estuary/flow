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
pub mod daemon;
pub mod disk;
pub mod filesystem;
pub mod image;
pub mod inflight;
pub mod journal;
pub mod owner;
pub mod session;
pub mod ublk;
pub mod wake;

/// Session and journal-record protocol messages, generated from
/// `go/protocols/disk/disk.proto`.
pub use proto_flow::disk as proto;
