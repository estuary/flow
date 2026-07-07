//! `TestPublisher`: the [`runtime_next::Publisher`] seam for the catalog-test
//! runner.
//!
//! It performs no journal IO. Instead, the derived documents a derivation shard
//! publishes are appended, as they are published, to the shared
//! [`CollectionStore`] under their output collection's partition journal —
//! stamped with the run's monotonic synthetic publication clock. Downstream
//! derivations then read these documents back (via the segment feeder), and
//! Verify steps read them from the store window written during a test case.
//!
//! Appending in `publish_doc` (the drain phase) rather than at the commit step
//! is safe in the single-process harness: a drained transaction always commits
//! (no rollback), and the drain strictly precedes the leader's committing
//! `Persist` — the runner's commit signal — so a Stat observes all its derived
//! documents already resident when its transaction reports done.
//!
//! `publish_stats` is a no-op (catalog tests ignore ops stats), and the
//! auto-split / throttle hooks are inert (no journal IO means no back-pressure).

use crate::partitions::{self, Partitioning};
use crate::store::CollectionStore;
use bytes::Bytes;
use proto_gazette::uuid;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Shared handle threaded into every derivation's publisher: the collection
/// store all derived documents land in, and the run's monotonic publication
/// clock. The clock only orders stored documents; the graph's scheduling clocks
/// are document counts tracked separately.
#[derive(Clone)]
pub struct TestPublisherFactory {
    store: Arc<Mutex<CollectionStore>>,
    clock: Arc<AtomicU64>,
}

impl TestPublisherFactory {
    pub fn new(store: Arc<Mutex<CollectionStore>>, clock: Arc<AtomicU64>) -> Self {
        Self { store, clock }
    }
}

impl runtime_next::PublisherFactory for TestPublisherFactory {
    type Publisher = TestPublisher;

    fn open(
        &self,
        _authz_subject: String,
        _producer: uuid::Producer,
        _stats_journal: &str,
        collection_specs: &[&proto_flow::flow::CollectionSpec],
    ) -> anyhow::Result<TestPublisher> {
        let routings = collection_specs
            .iter()
            .map(|s| Partitioning::for_collection(s))
            .collect::<anyhow::Result<_>>()?;
        Ok(TestPublisher {
            store: self.store.clone(),
            clock: self.clock.clone(),
            routings,
        })
    }
}

/// [`runtime_next::Publisher`] that appends derived documents to a
/// [`CollectionStore`] instead of writing journals.
pub struct TestPublisher {
    store: Arc<Mutex<CollectionStore>>,
    clock: Arc<AtomicU64>,
    /// Logical-partition routing for each output collection, indexed by binding.
    /// Empty for a leader's stats-only publisher (it publishes no documents).
    routings: Vec<Partitioning>,
}

impl runtime_next::Publisher for TestPublisher {
    fn update_clock(&mut self) {
        // No journal IO: there are no document UUIDs to stamp. The store's
        // per-document clock is drawn from the shared counter on append.
    }

    async fn publish_stats(&mut self, _stats: ops::proto::Stats) -> tonic::Result<()> {
        // Catalog tests ignore ops stats.
        Ok(())
    }

    async fn publish_doc(
        &mut self,
        binding_index: usize,
        doc: doc::OwnedNode,
        _uuid_ptr: &json::Pointer,
    ) -> tonic::Result<usize> {
        let routing = &self.routings[binding_index];

        // Route by the document's partition fields, so a partitioned derived
        // collection lands in `{collection}/{field=value}/.../pivot=00` and a
        // Verify partition selector can filter it.
        let value = serde_json::to_value(doc::SerPolicy::noop().on_owned(&doc)).map_err(|err| {
            tonic::Status::internal(format!("serializing derived document: {err}"))
        })?;
        let body = serde_json::to_vec(&value).expect("serializing a serde_json::Value cannot fail");
        let len = body.len();

        // Append in publish (drain) order: each key routes to a single shard, so
        // no cross-shard interleave disturbs a key's reduction sequence.
        let clock = self.clock.fetch_add(1, Ordering::Relaxed);
        partitions::append_routed(
            &mut self.store.lock().unwrap(),
            routing,
            &value,
            body,
            clock,
        )
        .map_err(|err| {
            tonic::Status::internal(format!("routing derived document to partition: {err:#}"))
        })?;
        Ok(len)
    }

    async fn flush(&mut self) -> tonic::Result<()> {
        // Documents are appended as they are published; nothing to flush.
        Ok(())
    }

    fn commit_intents(&mut self) -> Option<(uuid::Producer, uuid::Clock, Vec<String>)> {
        // No journals, so there are no ACK commit positions to encode.
        None
    }

    async fn write_intents(
        &mut self,
        _journal_intents: BTreeMap<String, Bytes>,
    ) -> tonic::Result<()> {
        // No journals: derived documents were already appended in `publish_doc`.
        Ok(())
    }

    fn take_throttle_samples(&mut self) -> Vec<publisher::ThrottleSample<'_>> {
        // No journal IO happens, so there is no append back-pressure to sample.
        Vec::new()
    }

    fn split_partition(
        &self,
        _journal: &str,
    ) -> Option<futures::future::BoxFuture<'static, tonic::Result<publisher::SplitOutcome>>> {
        None
    }
}
