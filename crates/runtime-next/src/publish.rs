//! Publishing surface used by leader actors.
//!
//! `Publisher` is the unified entry point. Two variants:
//!
//! - `Publisher::Real` wraps a real `publisher::Publisher` and performs
//!   Gazette journal IO (stats / logs / ACK intents / future capture &
//!   derive collection writes).
//! - `Publisher::Preview` performs no journal IO. Stats and log documents
//!   are emitted as `tracing::debug!` events; output documents (capture /
//!   derive — TODO when those task types land) will print to stdout in the
//!   `["{collection}",{...doc...}]` format used by `flowctl preview`.
//!   ACK intents are implicitly empty: in preview mode no documents are
//!   actually published, so there are no per-journal commit positions to
//!   write back. `commit_intents` returns `None` and `write_intents` is
//!   a no-op.
//!
//! Construction is decided in `startup::run` based on the presence of
//! ops_logs / ops_stats specs in `L:Open`: present ⇒ `Real`, absent ⇒
//! `Preview`. The leader actor parks the `Publisher` across IO futures.

use bytes::Bytes;
use proto_flow::flow;
use proto_gazette::uuid;
use std::collections::BTreeMap;

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
    Preview,
}

impl Publisher {
    /// Build a real `Publisher` backed by a `publisher::Publisher` for
    /// `ops_logs_spec` / `ops_stats_spec` journals plus any additional
    /// supplied collection specs.
    pub fn new_real<'a, I>(
        authz_subject: String,
        client_factory: &gazette::journal::ClientFactory,
        ops_logs_journal: &str,
        ops_logs_spec: &flow::CollectionSpec,
        ops_stats_journal: &str,
        ops_stats_spec: &flow::CollectionSpec,
        collection_specs: I,
    ) -> anyhow::Result<Self>
    where
        I: IntoIterator<Item = &'a flow::CollectionSpec>,
    {
        let mut bindings = Vec::new();

        bindings.push(publisher::Binding::from_collection_spec(
            ops_logs_spec,
            Some(ops_logs_journal),
        )?);
        bindings.push(publisher::Binding::from_collection_spec(
            ops_stats_spec,
            Some(ops_stats_journal),
        )?);

        for spec in collection_specs {
            bindings.push(publisher::Binding::from_collection_spec(spec, None)?);
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

    /// Build a preview `Mode` that performs no journal IO. Stats are emitted
    /// to `tracing::debug!`; ACK intents are implicitly empty.
    pub fn new_preview() -> Self {
        Self::Preview
    }

    /// Advance the publisher's clock to the current wall-clock time.
    /// No-op in preview mode.
    pub fn update_clock(&mut self) {
        match self {
            Self::Real(p) => p.update_clock(),
            Self::Preview => {}
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
                        // Binding index 1: ops_stats_spec (see `new_real`).
                        stats.meta.as_mut().unwrap().uuid = uuid.to_string();
                        (1, serde_json::to_value(&stats).unwrap())
                    },
                    uuid::Flags::CONTINUE_TXN,
                )
                .await?;
                p.flush().await
            }
            Self::Preview => {
                tracing::debug!(stats = ?ops::DebugJson(stats), "transaction stats");
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
            Self::Preview => None,
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
            Self::Preview => {
                debug_assert!(
                    journal_intents.is_empty(),
                    "Publisher::Preview received non-empty ACK intents",
                );
                Ok(())
            }
        }
    }
}
