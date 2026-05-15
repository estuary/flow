mod actor;
mod handler;
mod state;

pub(crate) use handler::serve_session;

#[derive(Clone)]
pub(crate) struct Metrics {
    /// Total NextCheckpoint responses sent to the coordinator.
    checkpoints: metrics::Counter,
}

impl Metrics {
    fn new(shard_id: &str) -> Self {
        static DESCRIBE: std::sync::Once = std::sync::Once::new();
        DESCRIBE.call_once(|| {
            metrics::describe_counter!(
                "shuffle_session_checkpoints",
                metrics::Unit::Count,
                "NextCheckpoint responses sent to coordinator",
            );
        });

        Self {
            checkpoints: metrics::counter!("shuffle_session_checkpoints", "shard_id" => shard_id.to_string()),
        }
    }
}
