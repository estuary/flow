use proto_gazette::{broker, uuid};

mod actor;
mod handler;
mod heap;
mod listing;
mod producer;
mod read;
mod routing;
mod state;

use actor::SliceActor;
pub(crate) use handler::serve_slice;

/// LazyJournalClient uses a LazyCell to defer initialization of the Client.
///
/// An instantiated Client requires a background task to perform token refreshes,
/// but at scale not every Slice will interact with every binding and collection,
/// so avoid building a Client until we know it's needed.
pub type LazyJournalClient = std::cell::LazyCell<
    gazette::journal::Client,
    Box<dyn FnOnce() -> gazette::journal::Client + Send>,
>;

/// ReadLines using a type-erased inner Stream. Pin-boxed so that `StreamFuture` works
/// (`StreamFuture` requires `Unpin`, which `Pin<Box<T>>` always satisfies).
pub type ReadLines = std::pin::Pin<
    Box<
        gazette::journal::read::ReadLines<
            1_000_000,
            64,
            futures::stream::BoxStream<'static, gazette::RetryResult<broker::ReadResponse>>,
        >,
    >,
>;

/// Accumulated causal hints from ACK documents, keyed by (journal name, binding index).
/// Drained into the flush frontier each flush cycle.
pub type CausalHints =
    std::collections::HashMap<(Box<str>, u16), Vec<(uuid::Producer, uuid::Clock)>>;

#[derive(Clone)]
pub(crate) struct Metrics {
    /// Total bytes read from journals, accumulated from each progress flush.
    bytes_read: metrics::Counter,
    /// Total flush cycles started (broadcast Flush to Log shards).
    flushes: metrics::Counter,
    /// Total journal reads started over the session lifetime.
    reads_started: metrics::Counter,
    /// Total journal reads that terminated (EOF, JOURNAL_NOT_FOUND, SUSPENDED).
    reads_stopped: metrics::Counter,
    /// Number of active reads currently tailing their journal write head.
    tailing_reads: metrics::Gauge,
}

impl Metrics {
    fn new(shard_id: &str) -> Self {
        static DESCRIBE: std::sync::Once = std::sync::Once::new();
        DESCRIBE.call_once(|| {
            metrics::describe_counter!(
                "shuffle_slice_bytes_read",
                metrics::Unit::Bytes,
                "bytes read from journals, observed at each progress flush",
            );
            metrics::describe_counter!(
                "shuffle_slice_flushes",
                metrics::Unit::Count,
                "flush cycles broadcast to Log shards",
            );
            metrics::describe_counter!(
                "shuffle_slice_reads_started",
                metrics::Unit::Count,
                "journal reads started over the session lifetime",
            );
            metrics::describe_counter!(
                "shuffle_slice_reads_stopped",
                metrics::Unit::Count,
                "journal reads that terminated (EOF, JOURNAL_NOT_FOUND, SUSPENDED)",
            );
            metrics::describe_gauge!(
                "shuffle_slice_tailing_reads",
                metrics::Unit::Count,
                "active reads currently tailing their journal write head",
            );
        });

        Self {
            bytes_read: metrics::counter!("shuffle_slice_bytes_read", "shard_id" => shard_id.to_string()),
            flushes: metrics::counter!("shuffle_slice_flushes", "shard_id" => shard_id.to_string()),
            reads_started: metrics::counter!("shuffle_slice_reads_started", "shard_id" => shard_id.to_string()),
            reads_stopped: metrics::counter!("shuffle_slice_reads_stopped", "shard_id" => shard_id.to_string()),
            tailing_reads: metrics::gauge!("shuffle_slice_tailing_reads", "shard_id" => shard_id.to_string()),
        }
    }
}
