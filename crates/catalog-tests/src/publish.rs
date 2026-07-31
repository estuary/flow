//! `TestPublisher`: the [`runtime_next::Publisher`] seam for the catalog-test
//! runner. It performs no journal IO, and it is where the runner learns that a
//! transaction committed.
//!
//! # Derived documents
//!
//! Documents a derivation shard publishes are appended, as they are published, to
//! the shared [`CollectionStore`] under their output collection's logical
//! partition, stamped with the run's monotonic publication clock. Downstream
//! derivations read them back through the segment feeder, and Verify steps read
//! the window written during a test case.
//!
//! # The commit signal
//!
//! `write_intents` sends the runner's per-transaction commit signal. The
//! `Publisher` seam's contract *is* the transaction lifecycle (`update_clock` →
//! `publish_stats` / `publish_doc` → `flush` → `commit_intents` →
//! `write_intents`), which makes it the honest place to observe a commit — as
//! opposed to `LogEvent`, an observability channel whose enum and variants are
//! both `#[non_exhaustive]`.
//!
//! Three properties make this exact:
//!
//! - **It fires once per transaction.** The leader's Tail FSM reaches
//!   `WriteIntents` unconditionally on every transaction, via
//!   `Recover → WriteIntents → Done`.
//! - **It fires only on the leader.** On the derive path `write_intents` is
//!   leader-exclusive; a shard's drain calls only `commit_intents`. The
//!   discriminator is [`PublisherFactory::open`](runtime_next::PublisherFactory),
//!   documented as passing empty `collection_specs` for a leader's stats-only
//!   publisher — so a publisher with no bindings is the leader's.
//! - **Every derived document is already resident when it fires.** Shard
//!   publishers publish documents during drain, and those commits fan into the
//!   leader, which only *then* runs `WriteOpsStats` → `WriteIntents`. The
//!   ordering is structural, enforced by the FSM, not a convention.
//!
//! One caveat the runner handles: `Recover` is also the Tail's *initial* state
//! after session start, replaying any recovered ACK intents, so exactly one extra
//! signal fires at session startup, strictly before the first transaction's. The
//! runner consumes it during [`start`](crate::runner::DerivationRunner::start).

use crate::partitions::{self, Partitioning};
use crate::store::CollectionStore;
use bytes::Bytes;
use proto_gazette::uuid;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

/// Opens the publishers of one derivation: shard publishers which append derived
/// documents to `store`, and the leader's stats-only publisher which signals each
/// transaction commit on `commit_tx`.
///
/// One factory per derivation, so its commit channel is per-derivation too —
/// runners must not observe each other's commits.
#[derive(Clone)]
pub struct TestPublisherFactory {
    store: Arc<Mutex<CollectionStore>>,
    clock: Arc<AtomicU64>,
    commit_tx: mpsc::UnboundedSender<()>,
}

impl TestPublisherFactory {
    pub fn new(
        store: Arc<Mutex<CollectionStore>>,
        clock: Arc<AtomicU64>,
        commit_tx: mpsc::UnboundedSender<()>,
    ) -> Self {
        Self {
            store,
            clock,
            commit_tx,
        }
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
            .collect::<anyhow::Result<Vec<_>>>()?;

        // No collection bindings identifies the leader's stats-only publisher —
        // the one whose `write_intents` marks a committed transaction.
        let commit_tx = routings.is_empty().then(|| self.commit_tx.clone());

        Ok(TestPublisher {
            store: self.store.clone(),
            clock: self.clock.clone(),
            routings,
            commit_tx,
        })
    }
}

/// [`runtime_next::Publisher`] that appends derived documents to a
/// [`CollectionStore`] instead of writing journals.
pub struct TestPublisher {
    store: Arc<Mutex<CollectionStore>>,
    clock: Arc<AtomicU64>,
    /// Logical-partition routing for each output collection, indexed by binding.
    /// Empty for the leader's stats-only publisher, which publishes no documents.
    routings: Vec<Partitioning>,
    /// Present only on the leader's publisher; see the module docs.
    commit_tx: Option<mpsc::UnboundedSender<()>>,
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

    async fn marker_commit(
        &mut self,
        _binding_index: usize,
    ) -> tonic::Result<Option<(uuid::Producer, uuid::Clock, Vec<String>)>> {
        // No journal IO: no backfill marker is broadcast.
        Ok(None)
    }

    async fn apply_truncated_at_labels(
        &mut self,
        _active_backfills: &BTreeMap<u32, u64>,
    ) -> tonic::Result<()> {
        // No journal IO: there are no `truncated-at` journal labels to apply.
        Ok(())
    }

    fn commit_intents(&mut self) -> Option<(uuid::Producer, uuid::Clock, Vec<String>)> {
        // No journals, so there are no ACK commit positions to encode. Note this
        // `Option` discriminates *implementations*, not empty transactions.
        None
    }

    async fn write_intents(
        &mut self,
        _journal_intents: BTreeMap<String, Bytes>,
    ) -> tonic::Result<()> {
        // Derived documents were already appended in `publish_doc`; the only work
        // here is signalling the transaction commit.
        if let Some(commit_tx) = &self.commit_tx {
            // A closed receiver means the runner is shutting down, which is not
            // an error for an in-flight transaction.
            let _ = commit_tx.send(());
        }
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
