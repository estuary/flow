//! Publishing surface used by leader actors.
//!
//! `Publisher` is the unified entry point. Two variants:
//!
//! - `Publisher::Real` wraps a real `publisher::Publisher` and performs
//!   Gazette journal IO (stats / logs / ACK intents / future capture &
//!   derive collection writes).
//! - `Publisher::Preview` performs no journal IO. Stats and log documents
//!   are emitted as `tracing::info!` events. Captured documents are written
//!   as NDJSON to stdout; one JSON object per line, flushed once per
//!   transaction commit.
//!
//! Construction is decided in `startup::run` based on the `preview` flag in
//! `L:Task`: `false` ⇒ `Real`, `true` ⇒ `Preview`. The leader actor parks the
//! `Publisher` across IO futures.

use bytes::Bytes;
use proto_gazette::uuid;
use std::collections::BTreeMap;
use std::io::Write as _;

/// Publishing entity used by leader sessions and runtime-next shards.
/// `crate::publish::Publisher` is the operative publisher from the
/// perspective of the `leader` and `runtime-next` crates; the inner
/// `publisher::Publisher` is an implementation detail of the `Real`
/// variant.
///
/// Methods mirror the subset of `publisher::Publisher` the leader actor
/// uses, plus `publish_stats` which factors out the actor's
/// stats-enqueue-then-flush idiom. The `Real` arm delegates to
/// `publisher::Publisher`; the `Preview` arm performs no IO.
pub enum Publisher {
    Real(publisher::Publisher),
    Preview {
        /// Collection names indexed by binding.
        collection_names: Vec<String>,
        /// Buffered stdout handle.
        stdout: std::io::BufWriter<std::io::Stdout>,
        /// Temporary scratch buffer for serialization.
        scratch: Vec<u8>,
    },
}

impl Publisher {
    /// Build a real `Publisher` backed by a `publisher::Publisher` for the
    /// pre-created `ops_stats_journal` plus any additional supplied collection
    /// specs.
    pub fn new_real<'a, I>(
        authz_subject: String,
        client_factory: &gazette::journal::ClientFactory,
        ops_stats_journal: &str,
        collection_specs: I,
    ) -> anyhow::Result<Self>
    where
        I: IntoIterator<Item = &'a proto_flow::flow::CollectionSpec>,
    {
        let mut bindings = Vec::new();

        bindings.push(publisher::Binding::for_fixed_journal(ops_stats_journal));

        for spec in collection_specs {
            bindings.push(publisher::Binding::from_collection_spec(spec)?);
        }

        let mut producer: [u8; 6] = rand::random();
        producer[0] |= 0x01; // Set multicast bit.
        let producer = uuid::Producer::from_bytes(producer);

        let mut publisher = publisher::Publisher::new(
            authz_subject,
            bindings,
            client_factory.clone(),
            producer,
            uuid::Clock::zero(),
        );
        publisher.update_clock();

        Ok(Self::Real(publisher))
    }

    /// Build a preview publisher that performs no journal IO. Stats are emitted
    /// to `tracing::info!`; captured documents are written to stdout.
    pub fn new_preview<'a, I>(collection_specs: I) -> Self
    where
        I: IntoIterator<Item = &'a proto_flow::flow::CollectionSpec>,
    {
        Self::Preview {
            collection_names: collection_specs
                .into_iter()
                .map(|s| s.name.clone())
                .collect(),
            stdout: std::io::BufWriter::new(std::io::stdout()),
            scratch: Vec::new(),
        }
    }

    /// Advance the publisher's clock to the current wall-clock time.
    /// No-op in preview mode.
    pub fn update_clock(&mut self) {
        match self {
            Self::Real(p) => p.update_clock(),
            Self::Preview { .. } => {}
        }
    }

    /// Enqueue and flush a single stats document as a `CONTINUE_TXN`.
    ///
    /// This consolidates the leader actor's prior "enqueue stats, then
    /// flush" pattern into one method so the parking pattern stays
    /// symmetric across `Real` and `Preview` arms.
    pub async fn publish_stats(&mut self, mut stats: ops::proto::Stats) -> tonic::Result<()> {
        match self {
            Self::Real(p) => {
                p.enqueue(
                    |uuid| {
                        // Binding index 0 is the fixed ops_stats journal.
                        stats.meta.as_mut().unwrap().uuid = uuid.to_string();
                        (0, serde_json::to_value(&stats).unwrap())
                    },
                    uuid::Flags::CONTINUE_TXN,
                )
                .await?;
                p.flush().await
            }
            Self::Preview { .. } => {
                tracing::info!(stats = ?ops::DebugJson(stats), "transaction stats");
                Ok(())
            }
        }
    }

    /// Enqueue one captured or derived collection document. `binding_index`
    /// is zero-based within the task bindings; binding zero of the underlying
    /// publisher is reserved for the fixed ops stats journal. Returns the
    /// serialized document byte length, excluding the framing bytes.
    pub async fn publish_doc(
        &mut self,
        binding_index: usize,
        mut doc: doc::OwnedNode,
        uuid_ptr: &json::Pointer,
    ) -> tonic::Result<usize> {
        match self {
            Self::Real(p) => {
                let publisher_binding = binding_index + 1;
                let (_, bytes_written) = p
                    .enqueue_owned(
                        |uuid| {
                            patch_document_uuid(&mut doc, uuid_ptr, uuid);
                            (publisher_binding, doc)
                        },
                        uuid::Flags::CONTINUE_TXN,
                    )
                    .await?;
                Ok(bytes_written)
            }
            Self::Preview {
                collection_names,
                stdout,
                scratch,
            } => {
                scratch.clear();
                serde_json::to_writer(&mut *scratch, &doc::SerPolicy::noop().on_owned(&doc))
                    .unwrap();

                let collection_name = &collection_names[binding_index];
                write!(stdout, "[{collection_name:?},").unwrap();
                stdout.write_all(&*scratch).unwrap();
                stdout.write_all(b"]\n").unwrap();
                Ok(scratch.len())
            }
        }
    }

    /// Flush all currently buffered documents.
    pub async fn flush(&mut self) -> tonic::Result<()> {
        match self {
            Self::Real(p) => p.flush().await,
            Self::Preview { stdout, .. } => {
                stdout.flush().unwrap();
                Ok(())
            }
        }
    }

    /// Snapshot this producer's contribution to the current transaction's
    /// ACK intents. In preview mode, returns an empty list — no real
    /// publishes happened, so there are no commit positions to encode.
    pub fn commit_intents(&mut self) -> Option<(uuid::Producer, uuid::Clock, Vec<String>)> {
        match self {
            Self::Real(p) => Some(p.commit_intents()),
            Self::Preview { .. } => None,
        }
    }

    /// Write per-journal ACK intent documents to their journals.
    /// No-op in preview mode (intents are necessarily empty).
    pub async fn write_intents(
        &mut self,
        journal_intents: BTreeMap<String, Bytes>,
    ) -> tonic::Result<()> {
        match self {
            Self::Real(p) => p.write_intents(journal_intents).await,
            Self::Preview { .. } => {
                debug_assert!(
                    journal_intents.is_empty(),
                    "Publisher::Preview received non-empty ACK intents",
                );
                Ok(())
            }
        }
    }
}

/// Patch a document UUID placeholder in-place after the publisher has assigned
/// the transaction UUID.
fn patch_document_uuid(doc: &mut doc::OwnedNode, uuid_ptr: &json::Pointer, uuid: uuid::Uuid) {
    if uuid_ptr.0.is_empty() {
        return;
    }

    let cell = match doc {
        doc::OwnedNode::Archived(archived) => {
            let Some(doc::ArchivedNode::String(s)) = uuid_ptr.query(archived.get()) else {
                return;
            };
            s.as_bytes().as_ptr_range()
        }
        doc::OwnedNode::Heap(heap) => match heap.access() {
            Ok(node) => {
                let Some(doc::HeapNode::String(s)) = uuid_ptr.query(&node) else {
                    return;
                };
                s.as_bytes().as_ptr_range()
            }
            Err(embedded) => {
                let Some(doc::ArchivedNode::String(s)) = uuid_ptr.query(embedded.get()) else {
                    return;
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
        return; // Return, rather than panic-ing if the length is somehow wrong.
    }
    _ = ::uuid::fmt::Hyphenated::from_uuid(uuid).encode_lower(cell);
}
