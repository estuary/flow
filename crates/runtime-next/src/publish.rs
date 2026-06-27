//! Output publisher used by leader and shard actors.
//!
//! Two traits form the seam between runtime-next and how a given deployment
//! emits its output documents. Production installs [`JournalPublisherFactory`],
//! which performs Gazette journal IO. An in-process harness (`flowctl preview`)
//! installs its own factory that writes documents to stdout. This crate is
//! unaware of either context: it always [`PublisherFactory::open`]s a
//! [`Publisher`], and the installed implementation decides what that means.
//!
//! The seam is generic, not dynamic: a leader / shard `Service` is monomorphized
//! over its concrete [`PublisherFactory`] (`JournalPublisherFactory` in
//! production), so the hot `publish_doc` path is a static call with no vtable or
//! boxed future. The associated [`PublisherFactory::Publisher`] type carries the
//! concrete `Publisher` through actors and drains.
//!
//! - [`Publisher`] is the per-session output publisher. The leader publishes
//!   stats and ACK intents through it; shards additionally publish captured /
//!   derived collection documents.
//! - [`PublisherFactory`] is the long-lived object held by each leader / shard
//!   `Service`. It opens a [`Publisher`] per session.
//!
//! Runtime *events* (connector-state persists, Apply actions, ...) are reported
//! through the separate [`Observer`](crate::observe) seam, not this one.

use bytes::Bytes;
use proto_gazette::uuid;
use std::collections::BTreeMap;

/// Per-session output publisher. The leader and shards obtain one from a
/// [`PublisherFactory`] at the start of each session, and park it across IO
/// futures. Async methods are return-position `impl Future + Send` (not
/// `dyn`-dispatched), so a concrete `Publisher` is called statically.
pub trait Publisher: Send + 'static {
    /// Advance the publisher's clock to the current wall-clock time.
    ///
    /// Called once at the start of each transaction's stream of published
    /// documents (the shard drain, or the leader stats + ACK write) so that
    /// stamped UUIDs reflect the transaction's write time and then tick up
    /// minimally between documents. The clock is monotonic, so this never
    /// regresses below a prior written clock.
    fn update_clock(&mut self);

    /// Enqueue and flush a single stats document as a `CONTINUE_TXN`.
    fn publish_stats(
        &mut self,
        stats: ops::proto::Stats,
    ) -> impl std::future::Future<Output = tonic::Result<()>> + Send;

    /// Enqueue one captured or derived collection document. `binding_index`
    /// is zero-based within the task bindings. Returns the serialized document
    /// byte length, excluding any framing bytes.
    fn publish_doc(
        &mut self,
        binding_index: usize,
        doc: doc::OwnedNode,
        uuid_ptr: &json::Pointer,
    ) -> impl std::future::Future<Output = tonic::Result<usize>> + Send;

    /// Flush all currently buffered documents.
    fn flush(&mut self) -> impl std::future::Future<Output = tonic::Result<()>> + Send;

    /// Snapshot this publisher's contribution to the current transaction's
    /// ACK intents, or `None` when no real publishes happened (so there are no
    /// commit positions to encode).
    fn commit_intents(&mut self) -> Option<(uuid::Producer, uuid::Clock, Vec<String>)>;

    /// Write per-journal ACK intent documents to their journals.
    fn write_intents(
        &mut self,
        journal_intents: BTreeMap<String, Bytes>,
    ) -> impl std::future::Future<Output = tonic::Result<()>> + Send;

    /// Take accumulated per-journal append-throttle samples since the last call.
    fn take_throttle_samples(&mut self) -> Vec<publisher::ThrottleSample<'_>>;

    /// Build a detached future which attempts to split partition `journal` at
    /// its key-range midpoint. Returns `None` when `journal` is not a partition
    /// of any Mapped binding (e.g. the fixed ops-stats journal) — such journals
    /// can never be split — or when the publisher performs no real journal IO.
    fn split_partition(
        &self,
        journal: &str,
    ) -> Option<futures::future::BoxFuture<'static, tonic::Result<publisher::SplitOutcome>>>;
}

/// Opens a [`Publisher`] for each leader / shard session. Held by the leader
/// [`Service`](crate::leader::Service) and shard [`Service`](crate::shard::Service),
/// which are monomorphized over it. Production installs [`JournalPublisherFactory`].
pub trait PublisherFactory: Clone + Send + Sync + 'static {
    /// Concrete per-session publisher this factory produces.
    type Publisher: Publisher;

    /// Open a [`Publisher`] for the given task bindings. `collection_specs` are
    /// the capture / derive collection bindings (empty for a leader's
    /// stats-only publisher); `stats_journal` is the fixed ops-stats binding.
    /// `authz_subject` and `producer` identify the publisher.
    fn open(
        &self,
        authz_subject: String,
        producer: uuid::Producer,
        stats_journal: &str,
        collection_specs: &[&proto_flow::flow::CollectionSpec],
    ) -> anyhow::Result<Self::Publisher>;
}

/// Production [`PublisherFactory`]: opens [`JournalPublisher`]s that perform
/// Gazette journal IO. Runtime events are reported through the separate
/// [`Observer`](crate::observe) seam, not this one.
#[derive(Clone)]
pub struct JournalPublisherFactory {
    client_factory: gazette::journal::ClientFactory,
}

impl JournalPublisherFactory {
    pub fn new(client_factory: gazette::journal::ClientFactory) -> Self {
        Self { client_factory }
    }
}

impl PublisherFactory for JournalPublisherFactory {
    type Publisher = JournalPublisher;

    fn open(
        &self,
        authz_subject: String,
        producer: uuid::Producer,
        stats_journal: &str,
        collection_specs: &[&proto_flow::flow::CollectionSpec],
    ) -> anyhow::Result<JournalPublisher> {
        let mut bindings = Vec::with_capacity(collection_specs.len() + 1);

        // Binding zero is the fixed ops-stats journal.
        bindings.push(publisher::Binding::for_fixed_journal(stats_journal));

        for spec in collection_specs {
            bindings.push(publisher::Binding::from_collection_spec(spec)?);
        }

        let mut publisher = publisher::Publisher::new(
            authz_subject,
            bindings,
            self.client_factory.clone(),
            producer,
            uuid::Clock::zero(),
        );
        publisher.update_clock();

        Ok(JournalPublisher(publisher))
    }
}

/// Production [`Publisher`]: wraps a [`publisher::Publisher`] and performs
/// Gazette journal IO. The inner `publisher::Publisher` is an implementation
/// detail; from the leader / shard perspective the operative publisher is the
/// [`Publisher`] trait.
pub struct JournalPublisher(publisher::Publisher);

impl JournalPublisher {
    /// Access the wrapped [`publisher::Publisher`] for low-level enqueues.
    /// Used only by the `split_e2e` integration test, which drives raw appends
    /// against a live broker; not part of the leader / shard hot path.
    #[doc(hidden)]
    pub fn inner_mut(&mut self) -> &mut publisher::Publisher {
        &mut self.0
    }
}

impl Publisher for JournalPublisher {
    fn update_clock(&mut self) {
        self.0.update_clock()
    }

    async fn publish_stats(&mut self, mut stats: ops::proto::Stats) -> tonic::Result<()> {
        self.0
            .enqueue(
                |uuid| {
                    // Binding index 0 is the fixed ops_stats journal.
                    let meta = stats.meta.as_mut().ok_or_else(|| {
                        tonic::Status::internal("stats document is missing required `meta`")
                    })?;
                    meta.uuid = uuid.to_string();

                    let value = serde_json::to_value(&stats).map_err(|err| {
                        tonic::Status::internal(format!("serializing stats document: {err}"))
                    })?;
                    Ok((0, value))
                },
                uuid::Flags::CONTINUE_TXN,
            )
            .await?;
        self.0.flush().await
    }

    async fn publish_doc(
        &mut self,
        binding_index: usize,
        mut doc: doc::OwnedNode,
        uuid_ptr: &json::Pointer,
    ) -> tonic::Result<usize> {
        // Publisher binding zero is reserved for the fixed ops stats journal.
        let publisher_binding = binding_index + 1;
        let (_, bytes_written) = self
            .0
            .enqueue_owned(
                |uuid| {
                    patch_document_uuid(&mut doc, uuid_ptr, uuid)?;
                    Ok((publisher_binding, doc))
                },
                uuid::Flags::CONTINUE_TXN,
            )
            .await?;
        Ok(bytes_written)
    }

    async fn flush(&mut self) -> tonic::Result<()> {
        self.0.flush().await
    }

    fn commit_intents(&mut self) -> Option<(uuid::Producer, uuid::Clock, Vec<String>)> {
        Some(self.0.commit_intents())
    }

    fn take_throttle_samples(&mut self) -> Vec<publisher::ThrottleSample<'_>> {
        self.0.take_throttle_samples()
    }

    fn split_partition(
        &self,
        journal: &str,
    ) -> Option<futures::future::BoxFuture<'static, tonic::Result<publisher::SplitOutcome>>> {
        self.0.split_partition(journal)
    }

    async fn write_intents(
        &mut self,
        journal_intents: BTreeMap<String, Bytes>,
    ) -> tonic::Result<()> {
        self.0.write_intents(journal_intents).await
    }
}

#[cfg(test)]
impl JournalPublisher {
    /// Build a real `JournalPublisher` over clients of an unreachable local
    /// endpoint, for tests that exercise real publisher plumbing (e.g. partition
    /// splitting) without a live Gazette.
    pub(crate) fn new_test_real<'a, I>(collection_specs: I) -> Self
    where
        I: IntoIterator<Item = &'a proto_flow::flow::CollectionSpec>,
    {
        let fragment_client = gazette::journal::Client::new_fragment_client();
        let factory: gazette::journal::ClientFactory =
            std::sync::Arc::new(move |_subject, _object| {
                gazette::journal::Client::new(
                    "http://localhost:0".to_string(),
                    fragment_client.clone(),
                    proto_grpc::Metadata::new(),
                    gazette::Router::new("local"),
                )
            });
        let collection_specs: Vec<&proto_flow::flow::CollectionSpec> =
            collection_specs.into_iter().collect();
        JournalPublisherFactory::new(factory)
            .open(
                "test".to_string(),
                new_producer(),
                "test/ops/stats",
                &collection_specs,
            )
            .unwrap()
    }
}

/// Generate a fresh random Gazette `Producer` identity.
///
/// The producer is the vector-clock key under which a publisher's documents
/// are sequenced.
pub fn new_producer() -> uuid::Producer {
    let mut producer: [u8; 6] = rand::random();
    producer[0] |= 0x01; // Set multicast bit (mark as not a real MAC address).
    uuid::Producer::from_bytes(producer)
}

/// Decode a 6-byte `Producer`.
pub fn producer_from_bytes(publisher_id: &[u8]) -> anyhow::Result<uuid::Producer> {
    use anyhow::Context;
    let bytes: [u8; 6] = publisher_id
        .try_into()
        .context("Task.publisher_id is not a 6-byte producer identity")?;
    Ok(uuid::Producer::from_bytes(bytes))
}

/// Patch a document UUID placeholder in-place after the publisher has assigned
/// the transaction UUID.
fn patch_document_uuid(
    doc: &mut doc::OwnedNode,
    uuid_ptr: &json::Pointer,
    uuid: uuid::Uuid,
) -> tonic::Result<()> {
    let cell = match doc {
        doc::OwnedNode::Archived(archived) => {
            let Some(doc::ArchivedNode::String(s)) = uuid_ptr.query(archived.get()) else {
                return Err(missing_uuid_placeholder(uuid_ptr));
            };
            s.as_bytes().as_ptr_range()
        }
        doc::OwnedNode::Heap(heap) => match heap.access() {
            Ok(node) => {
                let Some(doc::HeapNode::String(s)) = uuid_ptr.query(&node) else {
                    return Err(missing_uuid_placeholder(uuid_ptr));
                };
                s.as_bytes().as_ptr_range()
            }
            Err(embedded) => {
                let Some(doc::ArchivedNode::String(s)) = uuid_ptr.query(embedded.get()) else {
                    return Err(missing_uuid_placeholder(uuid_ptr));
                };
                s.as_bytes().as_ptr_range()
            }
        },
    };

    // SAFETY: We have sole ownership of doc::OwnedNode.
    let cell = unsafe {
        std::slice::from_raw_parts_mut(
            cell.start as *mut u8,
            cell.end.offset_from_unsigned(cell.start),
        )
    };
    if cell.len() != ::uuid::fmt::Hyphenated::LENGTH {
        return Err(tonic::Status::internal(format!(
            "document UUID placeholder at {uuid_ptr} is {} bytes, but a hyphenated UUID requires {}",
            cell.len(),
            ::uuid::fmt::Hyphenated::LENGTH,
        )));
    }
    _ = ::uuid::fmt::Hyphenated::from_uuid(uuid).encode_lower(cell);

    Ok(())
}

fn missing_uuid_placeholder(uuid_ptr: &json::Pointer) -> tonic::Status {
    tonic::Status::internal(format!(
        "document is missing a string UUID placeholder at {uuid_ptr}"
    ))
}

/// Test [`Publisher`] performing no journal IO: the in-crate analogue of the
/// preview harness's publisher, letting actor tests run without Gazette.
#[cfg(test)]
pub(crate) struct NoopPublisher;

#[cfg(test)]
impl Publisher for NoopPublisher {
    fn update_clock(&mut self) {}

    async fn publish_stats(&mut self, _stats: ops::proto::Stats) -> tonic::Result<()> {
        Ok(())
    }

    async fn publish_doc(
        &mut self,
        _binding_index: usize,
        _doc: doc::OwnedNode,
        _uuid_ptr: &json::Pointer,
    ) -> tonic::Result<usize> {
        Ok(0)
    }

    async fn flush(&mut self) -> tonic::Result<()> {
        Ok(())
    }

    fn commit_intents(&mut self) -> Option<(uuid::Producer, uuid::Clock, Vec<String>)> {
        None
    }

    async fn write_intents(
        &mut self,
        _journal_intents: BTreeMap<String, Bytes>,
    ) -> tonic::Result<()> {
        Ok(())
    }

    fn take_throttle_samples(&mut self) -> Vec<publisher::ThrottleSample<'_>> {
        Vec::new()
    }

    fn split_partition(
        &self,
        _journal: &str,
    ) -> Option<futures::future::BoxFuture<'static, tonic::Result<publisher::SplitOutcome>>> {
        None
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn noop_take_throttle_samples_is_empty() {
        // The auto-split signal path stays inert without journal IO: the
        // NoopPublisher performs no appends, so there are no throttle samples.
        let mut publisher = NoopPublisher;
        assert!(publisher.take_throttle_samples().is_empty());
    }

    #[test]
    fn producer_round_trips_through_task_bytes() {
        // new_producer sets the multicast bit, and producer_from_bytes recovers
        // the exact identity shard zero forwards in Task.publisher_id.
        let producer = new_producer();
        assert_eq!(producer.as_bytes()[0] & 0x01, 0x01, "multicast bit set");

        let recovered = producer_from_bytes(producer.as_bytes()).unwrap();
        assert_eq!(producer, recovered);
    }

    #[test]
    fn producer_from_bytes_rejects_wrong_length() {
        // Empty (controller never stamps it) and over-length both error rather
        // than silently truncating into a different identity.
        assert!(producer_from_bytes(b"").is_err());
        assert!(producer_from_bytes(b"\x01\x02\x03\x04\x05").is_err());
        assert!(producer_from_bytes(b"\x01\x02\x03\x04\x05\x06\x07").is_err());
    }
}
