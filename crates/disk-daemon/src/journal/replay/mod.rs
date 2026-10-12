//! Recovery from acknowledged journal deltas, shared by standby playback and
//! promotion.
//!
//! - Begin at the recovery floor. Backfill and promotion read to a broker-confirmed
//!   head; a standby then tails new records.
//! - [`proto_gazette::uuid::sequence`] orders each producer and removes duplicates.
//!   Chunks apply in physical journal order, which the writer's acknowledgement
//!   barrier preserves as commit order.
//! - Fences change no content and leave the pending delta available for ack repair.
//!   Another producer's delta records displace it. A displaced delta cannot resume:
//!   its initial records were dropped, so acknowledging a later fragment fails.
//! - A range may begin within a delta: a completed horizon makes earlier records
//!   unnecessary. Only the first read may skip pruned content below the current
//!   floor. Any later gap fails recovery.
//! - A horizon snapshots allocation before its opening record's chunks apply.
//!   Committed chunks discharge covered blocks; the commit discharging the last
//!   block advances the floor to the opening offset. A later horizon replaces an
//!   earlier one, and promotion resumes any horizon still open.
//!
//! [`UncommittedDelta`](uncommitted::UncommittedDelta) holds chunks and horizon
//! changes until acknowledgement. Applying them early could put the image ahead
//! of the client's checkpoint; ext4 recovery cannot restore that external boundary.
//! Buffering also avoids rereading a standby's journal to undo an uncommitted tail.
//!
//! `playback.rs` owns the background task. `rebuild.rs` reads and applies records;
//! `sequencer.rs` decides what to hold, drop, or commit without I/O;
//! `uncommitted.rs` stores the pending delta and spills large deltas to disk.

mod playback;
mod rebuild;
pub(super) mod sequencer;
pub mod uncommitted;

pub use playback::Playback;
pub(super) use playback::Reading;
pub(super) use rebuild::{OpenHorizon, read};
