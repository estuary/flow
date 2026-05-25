mod actor;
mod fsm;
mod handler;
mod startup;
mod task;

use super::close_policy;

pub(crate) use handler::serve;

#[derive(Clone)]
pub(crate) struct Metrics {
    /// Total transactions completed by this leader session.
    transactions: metrics::Counter,
    /// Aggregate bytes-behind across all transforms, observed at each frontier.
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
    // Source collection name of each derivation transform.
    binding_collection_names: Vec<String>,
    // Journal read suffix of each derivation transform.
    binding_journal_read_suffixes: Vec<String>,
    // Transform name of each derivation transform (for stats keying).
    binding_transform_names: Vec<String>,
    // Policy for how transactions close.
    close_policy: close_policy::Policy,
    // Maximum completed transactions before graceful stop. Zero means unlimited.
    max_transactions: u32,
    // Number of shards led by this leader.
    n_shards: usize,
    // Descriptive peer names of each shard, for logging and errors.
    peers: Vec<String>,
    // True if the connector is remote-authoritative (it returned a Some
    // runtime_checkpoint at Opened; only derive-sqlite does today). Such tasks
    // are sent StartCommit each transaction, and the Head FSM holds back from
    // opening the next transaction until the current one has passed StartCommit.
    // Set after the Opened fan-in, since Open precedes it.
    remote_authoritative: bool,
    // ShardRef embedded in every stats document.
    shard_ref: ops::ShardRef,
}
