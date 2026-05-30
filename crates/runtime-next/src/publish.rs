//! Publishing surface used by leader actors.
//!
//! `Publisher` is the unified entry point. Two variants:
//!
//! - `Publisher::Real` wraps a real `publisher::Publisher` and performs
//!   Gazette journal IO (stats / logs / ACK intents / future capture &
//!   derive collection writes).
//! - `Publisher::Preview` performs no journal IO. Stats and log documents
//!   are emitted as `tracing::info!` events. Captured documents are written
//!   as NDJSON to stdout, one JSON object per line.
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
        /// Accumulates complete `["<name>",<body>]\n` lines. Flushed to stdout
        /// with a single atomic `write_all` once it crosses [`PREVIEW_FLUSH_THRESHOLD`]
        /// or at transaction commit. Preview spawns one publisher per shard, all
        /// writing the process-global stdout `LineWriter`; flushing whole lines
        /// under the stdout lock keeps shards' output from splicing together.
        line_buf: Vec<u8>,
    },
}

/// Flush `Publisher::Preview`'s `line_buf` to stdout once it reaches this many
/// bytes. Sized to amortize the stdout lock + `write(2)` across many documents
/// while bounding buffered memory.
const PREVIEW_FLUSH_THRESHOLD: usize = 32 * 1024;

impl Publisher {
    /// Build a real `Publisher` backed by a `publisher::Publisher` for the
    /// pre-created `ops_stats_journal` plus any additional supplied collection
    /// specs. `producer` is chosen by the caller (see [`new_producer`]).
    pub fn new_real<'a, I>(
        authz_subject: String,
        producer: uuid::Producer,
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
            line_buf: Vec::new(),
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
                            patch_document_uuid(&mut doc, uuid_ptr, uuid)?;
                            Ok((publisher_binding, doc))
                        },
                        uuid::Flags::CONTINUE_TXN,
                    )
                    .await?;
                Ok(bytes_written)
            }
            Self::Preview {
                collection_names,
                line_buf,
            } => {
                let collection_name = &collection_names[binding_index];
                write!(line_buf, "[{collection_name:?},").unwrap();

                // Serialize the body directly into the line buffer, sampling its
                // length to report body bytes (excluding framing). Serializing a
                // valid OwnedNode cannot fail, so the body can never be left
                // partially written ahead of a flush.
                let body_start = line_buf.len();
                serde_json::to_writer(&mut *line_buf, &doc::SerPolicy::noop().on_owned(&doc))
                    .unwrap();
                let body_len = line_buf.len() - body_start;

                line_buf.extend_from_slice(b"]\n");

                // Flush whole lines under the stdout lock in a single atomic
                // write_all so concurrent shards' output can't splice together.
                if line_buf.len() >= PREVIEW_FLUSH_THRESHOLD {
                    std::io::stdout().write_all(line_buf).unwrap();
                    line_buf.clear();
                }
                Ok(body_len)
            }
        }
    }

    /// Flush all currently buffered documents.
    pub async fn flush(&mut self) -> tonic::Result<()> {
        match self {
            Self::Real(p) => p.flush().await,
            Self::Preview { line_buf, .. } => {
                if !line_buf.is_empty() {
                    std::io::stdout().write_all(line_buf).unwrap();
                    line_buf.clear();
                }
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

#[cfg(test)]
mod test {
    use super::*;

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
