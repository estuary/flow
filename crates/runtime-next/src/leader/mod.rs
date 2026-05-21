// Protocol primitives for joining over connecting shards.
mod join;
use join::{JoinOutcome, JoinSlot, PendingJoin, validate as validate_join};

pub mod close_policy;
pub mod frontier_mapping;
mod service;

// Task-specific handling.
pub mod capture; // `pub` because it's directly used by shard actor.
mod materialize;
// mod derive;  // TODO: implement.

pub use service::Service;

/// Shard-label feature flag (under the `estuary.dev/flag/` prefix) that, when
/// set to `"true"`, tells the leader to drop V1 rollback support for the task.
const DROP_V1_ROLLBACK_FLAG: &str = "drop-runtime-v1-rollback";

/// Reports whether `flags` (an `ops::ShardLabeling.flags` map) sets `flag` to
/// `"true"`, mirroring the Go runtime's feature-flag convention.
fn flag_enabled(flags: &std::collections::BTreeMap<String, String>, flag: &str) -> bool {
    flags.get(flag).map(String::as_str) == Some("true")
}
