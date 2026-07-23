//! Gapped-producer recovery
//!
//! On restart, a `(binding, journal)` read starts at the furthest journal
//! position justified by its checkpoint: the maximum offset magnitude `M`
//! across producer entries. An uncommitted producer span whose begin offset
//! `F` falls before `M` is *gapped*: the main read skips `[F, M)` and the
//! producer is frozen until it resolves. A gapped entry is marked in-memory by
//! the non-zero `max_continue == last_commit` sentinel (`ProducerState::is_gapped`);
//! while set, its `offset` is the pinned `F`, and `uuid::sequence` classifies its
//! documents correctly by construction. Three resolutions clear the gap — a
//! replay trigger (the first newer CONTINUE or ACK), a clean rollback, or a
//! deep rollback. An OUTSIDE_TXN is *not* a trigger: the sentinel is a real
//! (if unread) open span, and `uuid::sequence` rejects an OUTSIDE while a span
//! is pending with `OutsideWithPrecedingContinue`.
//!
//! Upon a triggering document, the actor:
//!
//! 1. reconstructs the recovered open span directly into the read's ordinary
//!    `pending` map as `{last_commit, max_continue: 0, offset: F}` — this both
//!    installs the span and overwrites the sentinel. Re-sequencing the span's
//!    own documents drives `max_continue` to zero or strictly above `last_commit`,
//!    never back onto the `{L, L, F}` sentinel, so the replay cannot re-trigger
//!    itself.
//! 2. opens a bounded historical read of `[F, trigger.begin)`, held in the single
//!    actor-owned `Replay` (`SliceActor::replay`).
//! 3. processes the replay read in its entirety, directly appending its
//!    sequenced documents, and leaving the ready heap untouched. Semantically,
//!    all replay documents happen *before* documents contained in the heap,
//!    including the triggering document. As that trigger already cleared a
//!    clock-delay gate, its preceding documents must also by construction.
//!
//! This is durably safe because `max_continue` is NOT persisted in
//! `ProducerFrontier`, and the checkpoint's `F` is by definition the first pending
//! CONTINUE's begin offset, so a recovering `ContinueBeginSpan` re-derives
//! `offset = F`. An interim flush mid-replay therefore carries exactly the
//! `(last_commit, F)` the durable checkpoint already records; a crash mid-replay
//! recovers the unchanged positive `F` and re-gaps idempotently. (On fragment loss
//! the first found document begins at `F' > F`; if that `F'` leaks to a durable
//! base and the session then crashes, recovery re-gaps at `F'`, and `[F, F')` was
//! unreadable anyway)

use super::actor::{Buffers, SliceActor};
use super::producer::ProducerState;
use super::read::{self, Meta, ReadyRead};
use super::state;
use futures::StreamExt;
use proto_flow::shuffle;
use proto_gazette::{broker, uuid};
use tokio::sync::mpsc;

/// All live state for the single active replay of a gapped producer's pending
/// transaction, owned by the actor in `SliceActor::replay`.
pub struct Replay {
    /// ID of `SliceActor::reads` whose gapped producer triggered this replay.
    pub read_id: usize,
    /// The gapped producer whose newer document triggered this replay.
    pub target: uuid::Producer,
    /// Inclusive begin offset of the replay; the producer's `offset`
    /// which begins its uncommitted span.
    pub begin_offset: i64,
    /// Exclusive end offset of the replay; the begin offset of the document
    /// which triggered the replay.
    pub end_offset: i64,
    /// The historical read's I/O state: either `Reading` a next batch
    /// or `Draining` a resolved batch document-by-document.
    pub io: ReplayIo,
}

pub enum ReplayIo {
    /// Awaiting the next historical batch from Gazette.
    Reading(super::ReadLines),
    /// A resolved batch being drained document-by-document.
    Draining(Box<ReadyRead>),
}

impl Replay {
    pub async fn next_batch(
        replay: &mut Option<Self>,
    ) -> (
        Self,
        Option<gazette::RetryResult<gazette::journal::read::LinesBatch>>,
    ) {
        match replay {
            Some(Replay {
                io: ReplayIo::Reading(read),
                ..
            }) => {
                let result = read.next().await;
                (replay.take().unwrap(), result) // Take only after I/O completion.
            }
            // No replay, or one that is `Draining`: never resolves.
            _ => std::future::pending().await,
        }
    }
}

impl SliceActor {
    /// Begin a replay for a gapped `producer_state` given `trigger`.
    pub(super) fn start_replay(
        &mut self,
        read_id: usize,
        mut producer_state: ProducerState,
        trigger: Meta,
    ) -> anyhow::Result<()> {
        // Expected pre-conditions of a replay:
        anyhow::ensure!(producer_state.is_gapped());
        anyhow::ensure!(producer_state.offset >= 0, "producer has an open span");
        anyhow::ensure!(producer_state.offset < trigger.begin_offset);
        anyhow::ensure!(self.replay.is_none());

        let read_state = &mut self.reads[read_id];
        let binding = &self.topology.bindings[read_state.binding_index as usize];

        // Start the bounded, non-blocking historical read.
        let request = broker::ReadRequest {
            journal: format!("{};{}", read_state.journal, binding.journal_read_suffix),
            begin_mod_time: binding.not_before.to_unix().0 as i64,
            block: false,
            do_not_proxy: true,
            offset: producer_state.offset,
            end_offset: trigger.begin_offset,
            metadata_only: false,
            min_etcd_revision: 0,
            header: None,
        };
        let client = (*self.topology.journal_clients[binding.index as usize]).clone();

        let read: super::ReadLines = Box::pin(gazette::journal::read::ReadLines::new(
            client.read(request).boxed(),
            read_id as u32,
            false, // Never tailing: the range is bounded and historical.
        ));

        // Emplace within tracking `Replay` to be attached to SliceActor.
        let replay = Replay {
            read_id,
            target: trigger.producer,
            begin_offset: producer_state.offset,
            end_offset: trigger.begin_offset,
            io: ReplayIo::Reading(read),
        };

        // Clear the `max_continue` sentinel, marking the producer as no longer
        // gapped, so it begins to sequence normally against replay documents.
        producer_state.max_continue = uuid::Clock::zero();
        _ = read_state.pending.insert(trigger.producer, producer_state);

        // Observability: track as event + metric.
        let trigger_kind = if trigger.flags.is_ack() {
            "ack"
        } else if trigger.flags.is_continue() {
            "continue"
        } else {
            "outside"
        };
        service_kit::event!(
            tracing::Level::DEBUG,
            "replay",
            read_id,
            binding = binding.index,
            journal = read_state.journal.to_string(),
            target = service_kit::event::debug(replay.target),
            last_commit = service_kit::event::debug(producer_state.last_commit),
            trigger_clock = service_kit::event::debug(trigger.clock),
            begin_offset = replay.begin_offset,
            end_offset = replay.end_offset,
            trigger_kind,
            "replay started",
        );
        self.metrics.replays_started.increment(1);

        self.replay = Some(replay);
        Ok(())
    }

    /// Drain the active replay's batch cursor as far as it will go,
    /// returning None when more broker I/O is required, or Some with
    /// a Log RPC channel that must await send capacity.
    pub(super) fn try_drain_replay(
        &mut self,
        mut replay: Replay,
        buffers: &mut Buffers,
    ) -> anyhow::Result<Option<mpsc::Sender<shuffle::LogRequest>>> {
        let read_state = &mut self.reads[replay.read_id];
        let binding = &self.topology.bindings[read_state.binding_index as usize];
        let mut producer_state = read_state.producer_state(replay.target);

        let maybe_tx = loop {
            let ReplayIo::Draining(mut ready_read) = replay.io else {
                break None;
            };
            let Meta {
                begin_offset,
                producer,
                ..
            } = ready_read.meta;

            if producer == replay.target {
                let sequenced = state::sequence_producer(
                    producer_state,
                    &read_state.journal,
                    read_state.truncated_at,
                    binding,
                    &ready_read.meta,
                )?;

                // We should not see an ACK within this uncommitted span.
                if sequenced.is_commit {
                    anyhow::bail!(
                        "replay of journal {} (binding {}) producer {producer:?} \
                         encountered an unexpected ACK at offset {begin_offset} \
                         (trigger document is offset {})",
                        read_state.journal,
                        binding.state_key(),
                        replay.end_offset,
                    );
                }
                if sequenced.is_append {
                    if let Err(tx) = Self::try_log_request_append_tx(
                        binding,
                        buffers,
                        &read_state.journal,
                        &self.topology.shards,
                        &mut self.log_prev_journal,
                        &self.log_request_tx,
                        &ready_read,
                    ) {
                        // Put back, await capacity, and retry.
                        replay.io = ReplayIo::Draining(ready_read);
                        break Some(tx);
                    }
                }

                // Append is complete; commit sequenced update.
                producer_state = sequenced.producer_state;
            }

            let ReadyRead {
                inner: read,
                doc: _consumed_doc,
                meta: _consumed_meta,
                mut doc_tail,
                mut meta_tail,
            } = *ready_read;

            match (doc_tail.next(), meta_tail.next()) {
                (Some((doc, _)), Some(meta)) => {
                    // Re-structure into the existing Box to re-use it.
                    *ready_read = ReadyRead {
                        doc,
                        meta,
                        doc_tail,
                        meta_tail,
                        inner: read,
                    };
                    replay.io = ReplayIo::Draining(ready_read);
                }
                (None, None) => {
                    replay.io = ReplayIo::Reading(read);
                }
                _ => unreachable!("doc_tail and meta_tail have equal length"),
            }
        };

        // On the way out, persist updated `producer_state` and return the `replay`.
        _ = read_state.pending.insert(replay.target, producer_state);
        self.replay = Some(replay);

        Ok(maybe_tx)
    }

    pub(super) fn on_replay_read_resolved(
        &mut self,
        mut replay: Replay,
        result: Option<gazette::RetryResult<gazette::journal::read::LinesBatch>>,
    ) -> anyhow::Result<()> {
        let read_state = &mut self.reads[replay.read_id];
        let binding = &self.topology.bindings[read_state.binding_index as usize];
        let journal = read_state.journal.to_string();

        let ReplayIo::Reading(read) = replay.io else {
            unreachable!("a batch resolves only while `Reading`");
        };

        // Is the replay complete (read finished with clean EOF)?
        let Some(result) = result else {
            let span_empty =
                read_state.producer_state(replay.target).max_continue == uuid::Clock::zero();

            service_kit::event!(
                tracing::Level::DEBUG,
                "replay",
                read_id = replay.read_id,
                binding = binding.index,
                journal,
                target = service_kit::event::debug(replay.target),
                begin_offset = replay.begin_offset,
                end_offset = replay.end_offset,
                span_empty,
                "replay complete",
            );
            self.metrics.replays_stopped.increment(1);
            return Ok(());
        };

        let lines_batch = match result {
            Err(err) => match read::classify_read_failure(err) {
                read::ReadFailure::JournalRemoved(status) => {
                    // Semantically treated as clean EOF.
                    service_kit::event!(
                        tracing::Level::DEBUG,
                        "replay",
                        read_id = replay.read_id,
                        binding = binding.index,
                        journal,
                        target = service_kit::event::debug(replay.target),
                        begin_offset = replay.begin_offset,
                        end_offset = replay.end_offset,
                        status = status.as_str_name(),
                        "replay stopped due to journal removal",
                    );
                    self.metrics.replays_stopped.increment(1);
                    return Ok(());
                }
                read::ReadFailure::Transient(err, attempt) => {
                    let level = if attempt == 0 {
                        tracing::Level::TRACE
                    } else {
                        tracing::Level::WARN
                    };
                    service_kit::event!(
                        level,
                        "replay",
                        read_id = replay.read_id,
                        binding = binding.index,
                        journal,
                        attempt,
                        err = service_kit::event::debug(err),
                        "transient error reading from journal during replay (will retry)",
                    );
                    // Re-emplace to re-poll.
                    replay.io = ReplayIo::Reading(read);
                    self.replay = Some(replay);

                    return Ok(());
                }
                read::ReadFailure::Terminal(err) => {
                    self.metrics.replays_stopped.increment(1);
                    return Err(read::map_read_error(
                        err,
                        &read_state.journal,
                        binding.state_key(),
                        "reading replay lines",
                    ));
                }
            },
            Ok(lines_batch) => lines_batch,
        };

        // Same journal as the main read, so the replay's write head is
        // authoritative; refreshing it keeps the main read's `bytes_behind`
        // delta accurate for a flush that lands mid-replay.
        read_state.write_head = read.write_head();
        let length = lines_batch.content.len();

        service_kit::event!(
            tracing::Level::TRACE,
            "read",
            read_id = replay.read_id,
            binding = binding.index,
            journal,
            offset = lines_batch.offset,
            length,
            tailing = lines_batch.tailing,
            "received replay LinesBatch",
        );
        self.metrics.bytes_read.increment(length as u64);

        let ready_read = match read::parse_lines_batch(
            &mut self.parser,
            &mut self.validators[read_state.binding_index as usize],
            binding,
            &read_state.journal,
            read,
            lines_batch,
            "transcoding replay documents",
        ) {
            Ok(ready_read) => ready_read,
            Err(err) => {
                self.metrics.replays_stopped.increment(1);
                return Err(err);
            }
        };

        // Re-emplace to drain.
        replay.io = ReplayIo::Draining(Box::new(ready_read));
        self.replay = Some(replay);

        Ok(())
    }
}
