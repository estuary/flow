mod actor;
mod frontier_mapping;
mod fsm;
mod handler;
mod startup;
mod task;
mod triggers;

use super::close_policy;

pub(crate) use handler::serve;

#[derive(Clone)]
pub(crate) struct Metrics {
    /// Total transactions completed by this leader session.
    transactions: metrics::Counter,
    /// Aggregate bytes-behind across all bindings, observed at each frontier.
    bytes_behind: metrics::Gauge,
}

impl Metrics {
    pub(crate) fn new(shard_zero: &str) -> Self {
        static DESCRIBE: std::sync::Once = std::sync::Once::new();
        DESCRIBE.call_once(|| {
            metrics::describe_counter!(
                "runtime_leader_transactions",
                metrics::Unit::Count,
                "transactions completed by this leader session",
            );
            metrics::describe_gauge!(
                "runtime_leader_behind",
                metrics::Unit::Bytes,
                "aggregate bytes-behind across all bindings, observed when writing stats",
            );
        });

        let shard_zero = || shard_zero.to_string();
        Self {
            transactions: metrics::counter!(
                "runtime_leader_transactions",
                "shard_zero" => shard_zero(),
            ),
            bytes_behind: metrics::gauge!(
                "runtime_leader_behind",
                "shard_zero" => shard_zero(),
            ),
        }
    }
}

// Task configuration, as understood by the leader.
struct Task {
    // Collection name of each materialization binding.
    binding_collection_names: Vec<String>,
    // Journal read suffix of each materialization binding.
    binding_journal_read_suffixes: Vec<String>,
    // Policy for how transactions close.
    close_policy: close_policy::Policy,
    // OCI image of the connector, or empty if not an image connector.
    connector_image: String,
    // Maximum completed transactions before graceful stop. Zero means unlimited.
    max_transactions: u32,
    // Number of shards lead by this leader.
    n_shards: usize,
    // Descriptive peer names of each shard, for logging and errors.
    peers: Vec<String>,
    // ShardRef embedded in every stats document.
    shard_ref: ops::ShardRef,
    // Compiled triggers, or None if the task has no triggers configured.
    triggers: Option<std::sync::Arc<triggers::CompiledTriggers>>,
}
