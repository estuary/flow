//! On-disk recovery state for runtime-next tasks.
//!
//! `codec` defines the encoding of `Persist` messages onto RocksDB
//! `WriteBatch` operations and the inverse decode of scanned key/value
//! pairs into a [`State`] snapshot. `frontier_mapping` translates between
//! legacy `consumer.Checkpoint` and `shuffle::Frontier`, the single source
//! of truth shared with the legacy runtime for its rollback path so that
//! forward migration and rollback stay byte-identical on unchanged fields.

pub mod codec;
pub mod frontier_mapping;

pub use codec::State;
