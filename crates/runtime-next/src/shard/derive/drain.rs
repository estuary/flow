//! Derive output combiner drain.
//!
//! [`drain_and_publish`] runs as the actor's parked `drain_fut`: it consumes a
//! rotated output combiner, publishes each derived document as a `CONTINUE_TXN`
//! journal append, folds the document into the collection's inferred write
//! shape, flushes the publisher, and snapshots its commit position. The shard
//! reports the resulting per-transaction `drained` stats and `publisher_commit`
//! to the leader in `L:Stored`.

use super::Task;
use anyhow::Context;
use bytes::Bytes;

/// Resources and results handed back to the actor when a drain completes.
pub(super) struct Output {
    /// The drained combiner, recycled as the next transaction's accumulator.
    pub accumulator: crate::Accumulator,
    /// Documents drained from the combiner and published this transaction.
    pub drained_docs: u64,
    /// Bytes drained from the combiner and published this transaction.
    pub drained_bytes: u64,
    /// This shard's publisher commit, or None when nothing was published.
    pub publisher_commit: Option<crate::proto::derive::stored::PublisherCommit>,
    /// The publisher, borrowed for the drain's journal appends.
    pub publisher: crate::Publisher,
    /// The collection's inferred write shape, possibly widened this transaction.
    pub write_shape: doc::Shape,
}

/// Drain the rotated output combiner: publish each derived document, fold it
/// into inference, then flush and snapshot the publisher commit.
pub(super) async fn drain_and_publish(
    drainer: doc::combine::Drainer,
    parser: simd_doc::Parser,
    mut publisher: crate::Publisher,
    task: std::sync::Arc<Task>,
    mut write_shape: doc::Shape,
    metrics: super::Metrics,
) -> anyhow::Result<Output> {
    // Resync the publisher clock to wall-clock time at the start of this
    // transaction's stream of published documents. Each `publish_doc` and the
    // closing `commit_intents` then tick it up by a single microsecond, so
    // stamped UUIDs cluster at the transaction's time of initial write.
    // `Clock::update` is monotonic and never regresses.
    publisher.update_clock();

    let mut drainer = drainer;
    let mut drained_docs: u64 = 0;
    let mut drained_bytes: u64 = 0;
    let mut updated_inference = false;
    let mut count = 1u64;

    while let Some(doc::combine::DrainedDoc { meta: _, root: doc }) = drainer.drain_next()? {
        if write_shape.widen_owned(&doc) {
            let limit = complexity_limit(&write_shape);
            doc::shape::limits::enforce_shape_complexity_limit(
                &mut write_shape,
                limit,
                doc::shape::limits::DEFAULT_SCHEMA_DEPTH_LIMIT,
            );
            updated_inference = true;
        }

        let bytes_written = publisher
            .publish_doc(0, doc, &task.document_uuid_ptr)
            .await
            .context("publishing derived document")?;

        drained_docs += 1;
        drained_bytes += bytes_written as u64;

        // This loop is CPU-heavy. Yield to the runtime for cooperative liveness.
        if count % 100 == 0 {
            tokio::task::yield_now().await;
        }
        count += 1;
    }

    // Flush enqueued documents, then snapshot this shard's commit position.
    () = publisher
        .flush()
        .await
        .context("flushing derived documents")?;

    let publisher_commit = publisher
        .commit_intents()
        .map(
            |(producer, clock, journals)| crate::proto::derive::stored::PublisherCommit {
                producer: Bytes::copy_from_slice(producer.as_bytes()),
                clock: clock.as_u64(),
                journals,
            },
        );

    if updated_inference {
        let serialized = doc::shape::schema::to_schema(write_shape.clone());
        tracing::info!(
            schema = ?ops::DebugJson(serialized),
            collection_name = %task.collection_name,
            "inferred schema updated",
        );
        metrics.inferred_schema_updates.increment(1);
    }

    Ok(Output {
        accumulator: crate::Accumulator::from_drainer(drainer, parser)?,
        drained_docs,
        drained_bytes,
        publisher_commit,
        publisher,
        write_shape,
    })
}

/// The schema-complexity limit recorded in the shape's `x-complexity-limit`
/// annotation, falling back to the inference default when it is unset.
fn complexity_limit(shape: &doc::Shape) -> usize {
    shape
        .annotations
        .get(doc::shape::X_COMPLEXITY_LIMIT)
        .and_then(serde_json::Value::as_u64)
        .map_or(doc::shape::limits::DEFAULT_SCHEMA_COMPLEXITY_LIMIT, |n| {
            n as usize
        })
}
