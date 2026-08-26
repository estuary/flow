//! `TestPublisher`: the [`runtime_next::Publisher`] seam for the catalog-test
//! harness. It performs no journal IO.
//!
//! Documents a derivation shard publishes are appended, as they are published, to
//! the shared [`CollectionStore`] under their output collection's logical
//! partition. Downstream derivations read them back through the segment feeder,
//! and Verify steps read the window written during a test case.
//!
//! `write_intents` is also the session's per-transaction commit signal, because
//! the `Publisher` seam's contract *is* the transaction lifecycle. Only the
//! leader's stats-only publisher sends it, discriminated by the empty
//! `collection_specs` that
//! [`PublisherFactory::open`](runtime_next::PublisherFactory) documents for it;
//! the leader's Tail FSM reaches `WriteIntents` once per transaction, after
//! every shard's drain has fanned in. One extra signal fires at session startup,
//! when the Tail replays recovered ACK intents, and is consumed by
//! [`start`](crate::session::DerivationSession::start).

use crate::partitions::{self, Partitioning};
use crate::store::CollectionStore;
use anyhow::Context as _;
use bytes::Bytes;
use proto_gazette::uuid;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

/// Opens the publishers of one derivation: shard publishers which append derived
/// documents to `store`, and the leader's stats-only publisher which signals each
/// transaction commit on `commit_tx`.
///
/// One factory per derivation, so its commit channel is per-derivation too —
/// sessions must not observe each other's commits.
#[derive(Clone)]
pub struct TestPublisherFactory {
    store: Arc<Mutex<CollectionStore>>,
    commit_tx: mpsc::UnboundedSender<()>,
}

impl TestPublisherFactory {
    pub fn new(store: Arc<Mutex<CollectionStore>>, commit_tx: mpsc::UnboundedSender<()>) -> Self {
        Self { store, commit_tx }
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
        binding_targets: &[u32],
    ) -> anyhow::Result<TestPublisher> {
        let routings = binding_targets
            .iter()
            .enumerate()
            .map(|(binding, &target)| {
                let spec = collection_specs.get(target as usize).with_context(|| {
                    format!("binding {binding} maps to unknown target {target}")
                })?;
                Partitioning::for_collection(spec)
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        // No collection bindings identifies the leader's stats-only publisher.
        let commit_tx = routings.is_empty().then(|| self.commit_tx.clone());

        Ok(TestPublisher {
            store: self.store.clone(),
            routings,
            commit_tx,
        })
    }
}

/// [`runtime_next::Publisher`] that appends derived documents to a
/// [`CollectionStore`] instead of writing journals.
pub struct TestPublisher {
    store: Arc<Mutex<CollectionStore>>,
    /// Logical-partition routing for each output collection, indexed by binding.
    /// Empty for the leader's stats-only publisher, which publishes no documents.
    routings: Vec<Partitioning>,
    /// Present only on the leader's publisher; see the module docs.
    commit_tx: Option<mpsc::UnboundedSender<()>>,
}

impl runtime_next::Publisher for TestPublisher {
    fn update_clock(&mut self) {
        // No journal IO: there are no document UUIDs to stamp.
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

        let value = serde_json::to_value(doc::SerPolicy::noop().on_owned(&doc)).map_err(|err| {
            tonic::Status::internal(format!("serializing derived document: {err}"))
        })?;
        let body = serde_json::to_vec(&value).expect("serializing a serde_json::Value cannot fail");
        let len = body.len();

        // Append order across shards is a race, exactly as competing journal
        // appends are in production: it's the *shuffle* key that pins a
        // document to one shard, and a derivation's output key need not be a
        // function of it. Documents of one shuffle key therefore reduce in
        // source order, and nothing more is guaranteed.
        partitions::append_routed(&mut self.store.lock().unwrap(), routing, &value, body).map_err(
            |err| {
                tonic::Status::internal(format!("routing derived document to partition: {err:#}"))
            },
        )?;
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
