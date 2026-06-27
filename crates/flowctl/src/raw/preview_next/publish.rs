//! Output-capturing publisher for `flowctl preview`.
//!
//! Installs into runtime-next through its [`runtime_next::PublisherFactory`]
//! seam: instead of Gazette journal IO, captured / derived documents are
//! written to stdout as NDJSON (`["<collection>",<doc>]` lines). runtime-next is
//! unaware this is "preview"; it simply publishes through the factory, and the
//! monomorphized factory decides what that means.
//!
//! This is the document half of the legacy combined preview opener. The
//! observation half — connector-state updates (`--output-state`) and Apply
//! actions (`--output-apply`) — now flows through the separate
//! [`Observer`](runtime_next::Observer) seam in [`super::observe`].

use bytes::Bytes;
use std::collections::BTreeMap;
use std::io::Write as _;

/// Flush [`PreviewPublisher`]'s `line_buf` to stdout once it reaches this many
/// bytes. Sized to amortize the stdout lock + `write(2)` across many documents
/// while bounding buffered memory.
const FLUSH_THRESHOLD: usize = 32 * 1024;

/// [`runtime_next::PublisherFactory`] that captures output to stdout instead of
/// publishing to journals. Stateless: captured / derived documents always print,
/// so there's nothing to configure (the `--output-state` / `--output-apply`
/// gating lives on the preview [`Observer`](super::observe::PreviewObserver)).
#[derive(Clone)]
pub struct PreviewPublisherFactory;

impl runtime_next::PublisherFactory for PreviewPublisherFactory {
    type Publisher = PreviewPublisher;

    fn open(
        &self,
        _authz_subject: String,
        _producer: proto_gazette::uuid::Producer,
        _stats_journal: &str,
        collection_specs: &[&proto_flow::flow::CollectionSpec],
    ) -> anyhow::Result<PreviewPublisher> {
        Ok(PreviewPublisher {
            collection_names: collection_specs.iter().map(|s| s.name.clone()).collect(),
            line_buf: Vec::new(),
        })
    }
}

/// [`runtime_next::Publisher`] that performs no journal IO: captured / derived
/// documents are buffered as stdout lines.
pub struct PreviewPublisher {
    /// Collection names indexed by binding, for the `["<name>",<doc>]` framing.
    /// Empty for a leader's stats-only publisher (it publishes no documents).
    collection_names: Vec<String>,
    /// Accumulates complete lines, flushed to stdout as a single atomic
    /// `write_all` once it crosses [`FLUSH_THRESHOLD`] or on `flush`. One
    /// publisher exists per shard, all writing the process-global stdout;
    /// flushing whole lines under the stdout lock keeps shards' output from
    /// splicing together.
    line_buf: Vec<u8>,
}

impl runtime_next::Publisher for PreviewPublisher {
    fn update_clock(&mut self) {
        // No journal IO: there are no document UUIDs to stamp.
    }

    async fn publish_stats(&mut self, stats: ops::proto::Stats) -> tonic::Result<()> {
        tracing::info!(stats = ?ops::DebugJson(stats), "transaction stats");
        Ok(())
    }

    async fn publish_doc(
        &mut self,
        binding_index: usize,
        doc: doc::OwnedNode,
        _uuid_ptr: &json::Pointer,
    ) -> tonic::Result<usize> {
        let collection_name = &self.collection_names[binding_index];
        write!(self.line_buf, "[{collection_name:?},").unwrap();

        // Serialize the body directly into the line buffer, sampling its length
        // to report body bytes (excluding framing). Serializing a valid
        // OwnedNode cannot fail, so the body can never be left partially written
        // ahead of a flush.
        let body_start = self.line_buf.len();
        serde_json::to_writer(&mut self.line_buf, &doc::SerPolicy::noop().on_owned(&doc)).unwrap();
        let body_len = self.line_buf.len() - body_start;

        self.line_buf.extend_from_slice(b"]\n");
        self.maybe_flush();
        Ok(body_len)
    }

    async fn flush(&mut self) -> tonic::Result<()> {
        if !self.line_buf.is_empty() {
            std::io::stdout().write_all(&self.line_buf).unwrap();
            self.line_buf.clear();
        }
        Ok(())
    }

    fn commit_intents(
        &mut self,
    ) -> Option<(
        proto_gazette::uuid::Producer,
        proto_gazette::uuid::Clock,
        Vec<String>,
    )> {
        // No real publishes happened, so there are no commit positions.
        None
    }

    async fn write_intents(
        &mut self,
        journal_intents: BTreeMap<String, Bytes>,
    ) -> tonic::Result<()> {
        debug_assert!(
            journal_intents.is_empty(),
            "PreviewPublisher received non-empty ACK intents",
        );
        Ok(())
    }

    fn take_throttle_samples(&mut self) -> Vec<publisher::ThrottleSample<'_>> {
        // No journal IO happens in preview, so there is no append back-pressure
        // to sample and no auto-splitting to drive.
        Vec::new()
    }

    fn split_partition(
        &self,
        _journal: &str,
    ) -> Option<futures::future::BoxFuture<'static, tonic::Result<publisher::SplitOutcome>>> {
        None
    }
}

impl PreviewPublisher {
    fn maybe_flush(&mut self) {
        if self.line_buf.len() >= FLUSH_THRESHOLD {
            std::io::stdout().write_all(&self.line_buf).unwrap();
            self.line_buf.clear();
        }
    }
}
