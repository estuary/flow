//! HeadFSM and TailFSM: the derive Leader's pipelined transaction FSMs.
//!
//! HeadFSM drives the currently-open transaction until the combiner is closed:
//!   Stop ← Idle ↔ Extend
//!          Idle → Flush → Rotate
//!
//! TailFSM drives an accumulated transaction towards commit after rotation:
//!   Begin → Store → WriteStats → StartCommit → Persist → Recover → WriteIntents → Done
//!
//! Head and Tail are pipelined. When Head's Flush completes and it Rotates,
//! Head is free to begin accumulating the next transaction while Tail handles
//! commit and post-commit activity of the current one. Head can only close its
//! next transaction once Tail is Done, which bounds the pipeline to at most one
//! in-flight transaction on each side.
//!
//! The single Persist commits the transaction: it durably records the ACK
//! intents (so a crash before journal-append is recoverable) and the connector
//! state. Documents become visible once WriteIntents appends those ACKs to
//! their journals.
//!
//! Recover is the Tail's initial state after session start, seeded with
//! recovered ACK intents from RocksDB, so that a prior session's interrupted
//! WriteIntents is replayed before any new transaction begins.

use super::super::frontier_mapping;
use super::{Task, close_policy};
use crate::proto;
use gazette::consumer;
use proto_gazette::uuid;
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

/// Per-transaction aggregated state threaded through the HeadFSM.
#[derive(Debug, Default)]
pub struct Extents {
    // Clock at which the transaction started (first applied ready frontier).
    open: uuid::Clock,
    // Clock at which the transaction began to close.
    close: uuid::Clock,
    // Frontier delta processed by this transaction.
    frontier: shuffle::Frontier,
    // Sparse per-binding map of bindings having changed extents in this transaction.
    bindings: HashMap<u32, BindingExtents>,
    // Measures of documents published by the connector (C:Published), before combining.
    published: ops::proto::stats::DocsAndBytes,
    // Measures of documents drained from output combiners into journal documents.
    drained: ops::proto::stats::DocsAndBytes,
    // Aggregated connector state patches gathered across this transaction's
    // Flush iterations, persisted with the committing Persist. State Update Wire Format.
    connector_patches: bytes::Bytes,
}

#[derive(Debug, Default)]
pub struct BindingExtents {
    // Maximum source clock (flow_published_at) read by this binding.
    max_source_clock: uuid::Clock,
    // Minimum source clock (flow_published_at) read by this binding.
    min_source_clock: uuid::Clock,
    // Measures of documents read from source journals.
    sourced: ops::proto::stats::DocsAndBytes,
}

#[derive(Debug)]
pub enum Head {
    Idle(HeadIdle),
    Extend(HeadExtend),
    Flush(HeadFlush),
    Stop,
}

#[derive(Debug)]
pub enum Tail {
    Begin(TailBegin),
    Persist(TailPersist),
    Store(TailStore),
    WriteStats(TailWriteStats),
    StartCommit(TailStartCommit),
    Recover(TailRecover),
    WriteIntents(TailWriteIntents),
    Done(TailDone),
}

/// `Action` is the next outgoing IO, or an actor-loop control edge.
#[derive(Debug)]
pub enum Action {
    /// Park until new IO arrives.
    Idle,
    /// Immediately re-poll without blocking. Sugar for waking immediately.
    PollAgain,
    /// Sleep for the indicated duration before re-polling.
    Sleep { wake_after: Duration },
    /// Broadcast a `L:Load` Frontier.
    Load { frontier: shuffle::Frontier },
    /// Broadcast `L:Flush` for one Flush iteration.
    Flush {
        // Aggregated connector patches from the previous Flush iteration of this
        // transaction (empty on the first iteration).
        state_patches: bytes::Bytes,
    },
    /// Broadcast `L:Store`.
    Store,
    /// Broadcast `L:StartCommit`.
    StartCommit {
        connector_checkpoint: consumer::Checkpoint,
    },
    /// Publish a stats document as CONTINUE_TXN to the ops stats journal,
    /// and build ACK intents from all shard publisher commits + leader commit.
    WriteStats {
        stats: ops::proto::Stats,
        publisher_commits: Vec<proto::derive::stored::PublisherCommit>,
    },
    /// Persist one `proto::Persist` WriteBatch to shard zero.
    Persist { persist: proto::Persist },
    /// Write ACK intents to their journals.
    WriteIntents {
        ack_intents: BTreeMap<String, bytes::Bytes>,
    },
    /// Transition Tail from Done to Begin with the closed transaction's extents.
    Rotate { extents: Extents },
}

impl Action {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Idle => "Idle",
            Self::PollAgain => "PollAgain",
            Self::Sleep { .. } => "Sleep",
            Self::Load { .. } => "Load",
            Self::Flush { .. } => "Flush",
            Self::Store => "Store",
            Self::WriteStats { .. } => "WriteStats",
            Self::StartCommit { .. } => "StartCommit",
            Self::Persist { .. } => "Persist",
            Self::WriteIntents { .. } => "WriteIntents",
            Self::Rotate { .. } => "Rotate",
        }
    }
}

impl Head {
    /// Dispatch to the current sub-state's `step()`.
    pub fn step(
        self,
        close_requested: &mut bool,
        now: uuid::Clock,
        ready_frontier: &mut Option<shuffle::Frontier>,
        shard_rx: &mut Option<(usize, proto::Derive)>,
        stopping: bool,
        tail: &mut Tail,
        task: &Task,
    ) -> (Action, Head) {
        match self {
            Head::Idle(s) => s.step(now, close_requested, ready_frontier, stopping, tail, task),
            Head::Extend(s) => s.step(shard_rx),
            Head::Flush(s) => s.step(shard_rx),
            Head::Stop => panic!("HeadFSM::Stop observed at step boundary"),
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Idle(_) => "Idle",
            Self::Extend(_) => "Extend",
            Self::Flush(_) => "Flush",
            Self::Stop => "Stop",
        }
    }
}

impl Tail {
    /// Dispatch to the current sub-state's `step()`.
    pub fn step(
        self,
        binding_bytes_behind: &mut [i64],
        intents_write_idle: bool,
        legacy_checkpoint: &mut Option<(shuffle::Frontier, consumer::Checkpoint)>,
        now: uuid::Clock,
        shard_rx: &mut Option<(usize, proto::Derive)>,
        stats_write_idle: Option<&mut BTreeMap<String, bytes::Bytes>>,
        task: &Task,
    ) -> (Action, Tail) {
        match self {
            Tail::Begin(s) => s.step(task),
            Tail::Store(s) => s.step(binding_bytes_behind, shard_rx, task),
            Tail::WriteStats(s) => s.step(legacy_checkpoint, now, stats_write_idle, task),
            Tail::StartCommit(s) => s.step(shard_rx),
            Tail::Persist(s) => s.step(shard_rx),
            Tail::Recover(s) => s.step(),
            Tail::WriteIntents(s) => s.step(intents_write_idle),
            Tail::Done(_) => (Action::Idle, self),
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Begin(_) => "Begin",
            Self::Store(_) => "Store",
            Self::WriteStats(_) => "WriteStats",
            Self::StartCommit(_) => "StartCommit",
            Self::Persist(_) => "Persist",
            Self::Recover(_) => "Recover",
            Self::WriteIntents(_) => "WriteIntents",
            Self::Done(_) => "Done",
        }
    }
}

/// HeadIdle evaluates the close policy between Load rounds.
#[derive(Debug, Default)]
pub struct HeadIdle {
    /// Accumulated extents of the current transaction (zero open means none started yet).
    pub extents: Extents,
    /// Running disk usage of per-shard combiners.
    pub combiner_usage_bytes: Vec<u64>,
    /// Close Clock of the last transaction, which may be recovered from a
    /// prior session, or zero.
    pub last_close: uuid::Clock,
}

impl HeadIdle {
    pub fn step(
        mut self,
        now: uuid::Clock,
        close_requested: &mut bool,
        ready_frontier: &mut Option<shuffle::Frontier>,
        stopping: bool,
        tail: &mut Tail,
        task: &Task,
    ) -> (Action, Head) {
        let is_open = self.extents.open != uuid::Clock::zero();
        let tail_done = matches!(tail, Tail::Done(_));

        // Termination condition: stay unstarted if `stopping`; let Tail finish.
        if stopping && !is_open {
            if tail_done {
                return (Action::PollAgain, Head::Stop);
            } else {
                return (Action::Idle, Head::Idle(self));
            }
        }
        // Clear stale close_requested from after prior transaction close.
        if !is_open {
            *close_requested = false;
        }

        let open_age = if !is_open {
            Duration::ZERO
        } else {
            uuid::Clock::delta(now, self.extents.open)
        };
        let combiner_bytes = self.combiner_usage_bytes.iter().copied().max().unwrap_or(0);
        let (read_docs, read_bytes) = self
            .extents
            .bindings
            .values()
            .map(|e| (e.sourced.docs_total, e.sourced.bytes_total))
            .fold((0, 0), |(a1, a2), (b1, b2)| (a1 + b1, a2 + b2));

        let close_policy::Decision {
            may_extend,
            may_close,
            wake_after,
        } = task.close_policy.evaluate(close_policy::Inputs {
            close_requested: *close_requested,
            idempotent_replay: false, // N/A.
            last_age: uuid::Clock::delta(now, self.last_close),
            combiner_bytes,
            open_age,
            read_bytes,
            read_docs,
            stopping,
            tail_done,
            unresolved_hints: self.extents.frontier.unresolved_hints != 0,
        });

        // Remote-authoritative connectors cannot receive C:Read of the next
        // transaction until C:StartCommit of the current one. Hold back opening
        // a new transaction until the Tail has passed StartCommit; once open,
        // further extends within the transaction are unconstrained.
        let tail_post_commit = matches!(
            tail,
            Tail::Persist(_) | Tail::Recover(_) | Tail::WriteIntents(_) | Tail::Done(_)
        );
        let extend_blocked = task.remote_authoritative && !is_open && !tail_post_commit;

        // Should we extend with a ready next Frontier?
        if may_extend
            && !extend_blocked
            && let Some(frontier) = ready_frontier.take()
        {
            if !is_open {
                self.extents.open = now;
                self.combiner_usage_bytes = vec![0; task.n_shards];
            }
            self.extents.frontier = self.extents.frontier.reduce(frontier.clone());

            return (
                Action::Load { frontier },
                Head::Extend(HeadExtend {
                    inner: self,
                    shard_loaded: vec![false; task.n_shards],
                }),
            );
        }

        // Should we begin to close the transaction?
        if !is_open {
            return (Action::Idle, Head::Idle(self));
        } else if may_close {
            assert!(matches!(tail, Tail::Done(_)));
            let Self { mut extents, .. } = self;
            extents.close = now;

            // The first Flush iteration carries no patches; subsequent iterations
            // propagate the prior iteration's aggregate (see HeadFlush).
            return (
                Action::Flush {
                    state_patches: bytes::Bytes::new(),
                },
                Head::Flush(HeadFlush {
                    extents,
                    n_shards: task.n_shards,
                    shard_flushed: vec![false; task.n_shards],
                    iteration_patches: Vec::new(),
                    all_patches: Vec::new(),
                    any_more: false,
                }),
            );
        }

        if let Some(wake_after) = wake_after {
            (Action::Sleep { wake_after }, Head::Idle(self))
        } else {
            (Action::Idle, Head::Idle(self))
        }
    }
}

/// HeadExtend waits for Loaded responses from all shards, then returns to
/// HeadIdle for close-policy evaluation.
#[derive(Debug)]
pub struct HeadExtend {
    /// HeadIdle state to return to once all Loaded responses arrive.
    pub inner: HeadIdle,
    /// Per-shard tracking of Loaded response receipt.
    pub shard_loaded: Vec<bool>,
}

impl HeadExtend {
    pub fn step(mut self, shard_rx: &mut Option<(usize, proto::Derive)>) -> (Action, Head) {
        if let Some((
            shard_index,
            proto::Derive {
                loaded: Some(loaded),
                ..
            },
        )) = shard_rx
            && self.shard_loaded.get(*shard_index) == Some(&false)
        {
            let proto::derive::Loaded {
                bindings,
                combiner_usage_bytes,
            } = std::mem::take(loaded);

            for proto::derive::loaded::Binding {
                index,
                min_source_clock,
                max_source_clock,
                sourced_docs_total,
                sourced_bytes_total,
            } in bindings
            {
                let min_source_clock = uuid::Clock::from_u64(min_source_clock);
                let max_source_clock = uuid::Clock::from_u64(max_source_clock);
                let extent = self.inner.extents.bindings.entry(index).or_default();

                if extent.sourced.docs_total == 0 {
                    extent.max_source_clock = max_source_clock;
                    extent.min_source_clock = min_source_clock;
                } else {
                    extent.max_source_clock = extent.max_source_clock.max(max_source_clock);
                    extent.min_source_clock = extent.min_source_clock.min(min_source_clock);
                }
                extent.sourced.bytes_total += sourced_bytes_total;
                extent.sourced.docs_total += sourced_docs_total;
            }
            self.inner.combiner_usage_bytes[*shard_index] = combiner_usage_bytes;

            // Mark received and consume `shard_rx`.
            self.shard_loaded[*shard_index] = true;
            _ = shard_rx.take();

            if self.shard_loaded.iter().all(|b| *b) {
                self.shard_loaded.clear(); // All received.
            }
        }

        if !self.shard_loaded.is_empty() {
            return (Action::Idle, Head::Extend(self));
        }

        // All shards have loaded.
        // Re-poll immediately so HeadIdle evaluates the close policy now.
        return (Action::PollAgain, Head::Idle(self.inner));
    }
}

/// HeadFlush drives the iterative Flush scatter/gather. Each iteration awaits a
/// Flushed response from every shard, then either begins another iteration
/// (propagating this iteration's aggregated patches) or, once every shard
/// reports `more = false`, Rotates the transaction to the Tail.
#[derive(Debug)]
pub struct HeadFlush {
    pub extents: Extents,
    /// Number of shards, for re-initializing `shard_flushed` between iterations.
    pub n_shards: usize,
    /// Per-shard tracking of Flushed response receipt for the current iteration.
    pub shard_flushed: Vec<bool>,
    /// Patches gathered this iteration, propagated into the next L:Flush.
    pub iteration_patches: Vec<u8>,
    /// Patches gathered across all iterations, persisted with the transaction.
    pub all_patches: Vec<u8>,
    /// Did any shard request a further Flush iteration (L:Flushed.more) this iteration?
    pub any_more: bool,
}

impl HeadFlush {
    pub fn step(mut self, shard_rx: &mut Option<(usize, proto::Derive)>) -> (Action, Head) {
        // Did we receive an expected Flushed response?
        if let Some((
            shard_index,
            proto::Derive {
                flushed: Some(flushed),
                ..
            },
        )) = shard_rx
            && self.shard_flushed.get(*shard_index) == Some(&false)
        {
            let proto::derive::Flushed {
                connector_patches_json,
                more,
            } = std::mem::take(flushed);

            if !connector_patches_json.is_empty() {
                crate::patches::extend_state_patches(
                    &mut self.iteration_patches,
                    &connector_patches_json,
                );
                crate::patches::extend_state_patches(
                    &mut self.all_patches,
                    &connector_patches_json,
                );
            }
            self.any_more |= more;

            // Mark received and consume `shard_rx`.
            self.shard_flushed[*shard_index] = true;
            _ = shard_rx.take();

            if self.shard_flushed.iter().all(|b| *b) {
                self.shard_flushed.clear(); // All received this iteration.
            }
        }

        if !self.shard_flushed.is_empty() {
            return (Action::Idle, Head::Flush(self));
        }
        // We've received all Flushed responses for this iteration.

        // If any shard requested another iteration, run one, propagating this
        // iteration's aggregated patches to all shards.
        if self.any_more {
            let state_patches: bytes::Bytes = std::mem::take(&mut self.iteration_patches).into();
            self.shard_flushed = vec![false; self.n_shards];
            self.any_more = false;

            return (Action::Flush { state_patches }, Head::Flush(self));
        }
        // All shards reported `more = false`: the Flush phase is complete.

        let Self {
            mut extents,
            all_patches,
            ..
        } = self;
        extents.connector_patches = all_patches.into();
        let last_close = extents.close;

        (
            Action::Rotate { extents },
            Head::Idle(HeadIdle {
                last_close,
                ..Default::default()
            }),
        )
    }
}

/// TailBegin receives closed transaction Extents and starts the commit pipeline.
#[derive(Debug)]
pub struct TailBegin {
    pub extents: Extents,
}

impl TailBegin {
    pub fn step(self, task: &Task) -> (Action, Tail) {
        let Self { extents } = self;

        (
            Action::Store,
            Tail::Store(TailStore {
                extents,
                shard_stored: vec![false; task.n_shards],
                publisher_commits: Vec::new(),
            }),
        )
    }
}

/// TailPersist awaits a Persisted response from shard zero,
/// and chains its contained action and state.
#[derive(Debug)]
pub struct TailPersist {
    pub seq_no: u64,
    pub next_action: Action,
    pub next_state: Box<Tail>,
}

impl TailPersist {
    pub fn step(self, shard_rx: &mut Option<(usize, proto::Derive)>) -> (Action, Tail) {
        if let Some((
            0,
            proto::Derive {
                persisted: Some(proto::Persisted { seq_no }),
                ..
            },
        )) = shard_rx
            && *seq_no == self.seq_no
        {
            shard_rx.take();

            let Self {
                seq_no: _,
                next_action,
                next_state,
            } = self;

            return (next_action, *next_state);
        }

        (Action::Idle, Tail::Persist(self))
    }
}

/// TailStore awaits Stored responses from all shards, collecting per-shard
/// publisher commits for the stats write.
#[derive(Debug)]
pub struct TailStore {
    pub extents: Extents,
    pub publisher_commits: Vec<proto::derive::stored::PublisherCommit>,

    /// Per-shard tracking of Stored response receipt.
    pub shard_stored: Vec<bool>,
}

impl TailStore {
    pub fn step(
        mut self,
        binding_bytes_behind: &mut [i64],
        shard_rx: &mut Option<(usize, proto::Derive)>,
        task: &Task,
    ) -> (Action, Tail) {
        // Did we receive an expected Stored response?
        if let Some((
            shard_index,
            proto::Derive {
                stored: Some(stored),
                ..
            },
        )) = shard_rx
            && self.shard_stored.get(*shard_index) == Some(&false)
        {
            let proto::derive::Stored {
                published_docs_total,
                published_bytes_total,
                drained_docs_total,
                drained_bytes_total,
                publisher_commit,
            } = std::mem::take(stored);

            self.extents.published.docs_total += published_docs_total;
            self.extents.published.bytes_total += published_bytes_total;
            self.extents.drained.docs_total += drained_docs_total;
            self.extents.drained.bytes_total += drained_bytes_total;

            if let Some(commit) = publisher_commit {
                self.publisher_commits.push(commit);
            }

            // Mark received and consume `shard_rx`.
            self.shard_stored[*shard_index] = true;
            shard_rx.take();

            if self.shard_stored.iter().all(|b| *b) {
                self.shard_stored.clear(); // All received.
            }
        }

        if !self.shard_stored.is_empty() {
            return (Action::Idle, Tail::Store(self));
        }
        // We've received all expected Stored responses.

        let Self {
            extents,
            publisher_commits,
            ..
        } = self;

        // Fold deltas from the extents Frontier into per-binding "bytes behind" gauges.
        for jf in &extents.frontier.journals {
            let Some(entry) = binding_bytes_behind.get_mut(jf.binding as usize) else {
                continue; // Reachable if shuffle service reports invalid binding indices.
            };
            *entry += jf.bytes_behind_delta;
        }

        let action = Action::WriteStats {
            stats: build_stats_doc(task, &extents, binding_bytes_behind),
            publisher_commits,
        };
        let state = TailWriteStats { extents };

        (action, Tail::WriteStats(state))
    }
}

/// TailWriteStats awaits the completion of a stats document append and flush.
#[derive(Debug)]
pub struct TailWriteStats {
    pub extents: Extents,
}

impl TailWriteStats {
    pub fn step(
        self,
        legacy_checkpoint: &mut Option<(shuffle::Frontier, consumer::Checkpoint)>,
        now: uuid::Clock,
        stats_write_idle: Option<&mut BTreeMap<String, bytes::Bytes>>,
        task: &Task,
    ) -> (Action, Tail) {
        let ack_intents = match stats_write_idle {
            Some(ack_intents) => std::mem::take(ack_intents),
            None => return (Action::Idle, Tail::WriteStats(self)),
        };
        // We've finished publishing to ops stats.

        let Self { extents } = self;

        // We use the existing consumer.Checkpoint `sources` structure to
        // piggyback the close Clock of this transaction under a special key.
        let (committed_close_key, committed_close_source) =
            frontier_mapping::encode_committed_close(extents.close);

        // If `legacy_checkpoint`, then we're preserving a rollback capability to
        // the V1 runtime. Reduce our delta Frontier extents into `full_frontier`,
        // map the result into `full_checkpoint`, and stamp the close Clock and
        // ACK intents. The updated `full_checkpoint` is persisted (as the legacy
        // "checkpoint" key) by the committing Persist built below, and is also
        // the connector_checkpoint sent with StartCommit for remote-authoritative
        // tasks.
        let (start_commit, legacy_checkpoint) =
            if let Some((full_frontier, full_checkpoint)) = legacy_checkpoint {
                *full_frontier = std::mem::take(full_frontier).reduce(extents.frontier.clone());

                frontier_mapping::merge_frontier_into_checkpoint(
                    full_frontier,
                    full_checkpoint,
                    &task.binding_journal_read_suffixes,
                );
                full_checkpoint
                    .sources
                    .insert(committed_close_key.clone(), committed_close_source.clone());
                full_checkpoint.ack_intents = ack_intents.clone();

                (
                    task.remote_authoritative.then(|| full_checkpoint.clone()),
                    Some(full_checkpoint.clone()),
                )
            } else {
                assert!(!task.remote_authoritative);
                (None, None)
            };

        // Build this transaction's committing Persist.
        let Extents {
            open: _,
            close,
            frontier,
            bindings: _,
            published: _,
            drained: _,
            connector_patches,
        } = extents;

        let seq_no = now.as_u64();
        let persist = proto::Persist {
            seq_no,
            ack_intents: ack_intents.clone(),
            committed_close_clock: close.as_u64(),
            committed_frontier: Some(shuffle::JournalFrontier::encode(&frontier.journals)),
            connector_patches_json: connector_patches,
            delete_ack_intents: true,
            legacy_checkpoint,
            ..Default::default()
        };

        let persist_action = Action::Persist { persist };
        let persist_state = Tail::Persist(TailPersist {
            seq_no,
            next_action: Action::PollAgain,
            next_state: Box::new(Tail::Recover(TailRecover { ack_intents })),
        });

        // Remote-authoritative connectors (derive-sqlite) require a StartCommit
        // round-trip ahead of the committing Persist. Send StartCommit and carry
        // the Persist action and state to emit once all shards acknowledge with
        // StartedCommit. Other connectors go straight to Persist.
        if let Some(connector_checkpoint) = start_commit {
            let action = Action::StartCommit {
                connector_checkpoint,
            };
            let state = TailStartCommit {
                next_action: persist_action,
                next_state: Box::new(persist_state),
                shard_started_commit: vec![false; task.n_shards],
            };

            (action, Tail::StartCommit(state))
        } else {
            (persist_action, persist_state)
        }
    }
}

/// TailStartCommit (remote-authoritative only) awaits the (empty) StartedCommit
/// acknowledgement from all shards, then emits the carried committing Persist.
#[derive(Debug)]
pub struct TailStartCommit {
    /// Persist action to emit once all shards acknowledge StartedCommit.
    pub next_action: Action,
    /// Persist state to transition into once all shards acknowledge StartedCommit.
    pub next_state: Box<Tail>,

    /// Per-shard tracking of StartedCommit response receipt.
    pub shard_started_commit: Vec<bool>,
}

impl TailStartCommit {
    pub fn step(mut self, shard_rx: &mut Option<(usize, proto::Derive)>) -> (Action, Tail) {
        // Did we receive an expected StartedCommit response?
        if let Some((
            shard_index,
            proto::Derive {
                started_commit: Some(started_commit),
                ..
            },
        )) = shard_rx
            && self.shard_started_commit.get(*shard_index) == Some(&false)
        {
            let proto::derive::StartedCommit {} = std::mem::take(started_commit);

            // Mark received and consume `shard_rx`.
            self.shard_started_commit[*shard_index] = true;
            shard_rx.take();

            if self.shard_started_commit.iter().all(|b| *b) {
                self.shard_started_commit.clear(); // All received.
            }
        }

        if !self.shard_started_commit.is_empty() {
            return (Action::Idle, Tail::StartCommit(self));
        }
        // We've received all expected StartedCommit responses.

        let Self {
            next_action,
            next_state,
            ..
        } = self;

        (next_action, *next_state)
    }
}

/// Recover is the post-commit handoff. It follows Persist each transaction,
/// and is also the Tail's initial state after recovery.
#[derive(Debug)]
pub struct TailRecover {
    /// ACK intents to publish: recovered from RocksDB at session start, or
    /// the just-committed transaction's own intents after a Persist.
    pub ack_intents: BTreeMap<String, bytes::Bytes>,
}

impl TailRecover {
    pub fn step(self) -> (Action, Tail) {
        let Self { ack_intents } = self;

        (
            Action::WriteIntents { ack_intents },
            Tail::WriteIntents(TailWriteIntents {}),
        )
    }
}

/// TailWriteIntents awaits the completion of ACK intent append and flush.
#[derive(Debug)]
pub struct TailWriteIntents {}

impl TailWriteIntents {
    pub fn step(self, intents_write_idle: bool) -> (Action, Tail) {
        if !intents_write_idle {
            return (Action::Idle, Tail::WriteIntents(self));
        }

        (Action::Idle, Tail::Done(TailDone {}))
    }
}

#[derive(Debug, Default)]
pub struct TailDone {}

/// Build an `ops::Stats` document snapshotting this transaction's extents.
fn build_stats_doc(
    task: &Task,
    extents: &Extents,
    binding_bytes_behind: &[i64],
) -> ops::proto::Stats {
    let mut transforms: BTreeMap<String, ops::proto::stats::derive::Transform> = BTreeMap::new();
    let mut max_published_at: uuid::Clock = uuid::Clock::default();

    for (binding_index, binding) in &extents.bindings {
        let Some(transform_name) = task.binding_transform_names.get(*binding_index as usize) else {
            continue; // Reachable if shards report invalid binding indices.
        };
        let Some(collection_name) = task.binding_collection_names.get(*binding_index as usize)
        else {
            continue;
        };

        let entry = transforms.entry(transform_name.clone()).or_default();
        entry.source = collection_name.clone();
        entry.bytes_behind = entry.bytes_behind.saturating_add_signed(
            binding_bytes_behind
                .get(*binding_index as usize)
                .copied()
                .unwrap_or_default(),
        );
        ops::merge_docs_and_bytes(&binding.sourced, &mut entry.input);
        entry.last_source_published_at = binding.max_source_clock.to_pb_json_timestamp();

        if binding.max_source_clock > max_published_at {
            max_published_at = binding.max_source_clock;
        }
    }

    let open_seconds_total = uuid::Clock::delta(extents.close, extents.open).as_secs_f64();

    ops::proto::Stats {
        meta: Some(ops::proto::Meta {
            uuid: String::new(), // Stamped by Publisher::enqueue()
        }),
        shard: Some(task.shard_ref.clone()),
        timestamp: extents.open.to_pb_json_timestamp(),
        open_seconds_total,
        txn_count: 1,
        derive: Some(ops::proto::stats::Derive {
            transforms,
            published: Some(extents.published),
            out: Some(extents.drained),
            last_published_at: if max_published_at != uuid::Clock::default() {
                max_published_at.to_pb_json_timestamp()
            } else {
                None
            },
        }),
        capture: Default::default(),
        materialize: Default::default(),
        interval: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use gazette::consumer;
    use std::collections::BTreeMap;
    use std::time::Duration;

    struct Ctx {
        binding_bytes_behind: Vec<i64>,
        close_requested: bool,
        intents_idle: bool,
        legacy_checkpoint: Option<(shuffle::Frontier, consumer::Checkpoint)>,
        now: uuid::Clock,
        pending_ack_intents: BTreeMap<String, Bytes>,
        ready_frontier: Option<shuffle::Frontier>,
        shard_rx: Option<(usize, proto::Derive)>,
        stats_idle: bool,
        stopping: bool,
        task: Task,
    }

    impl Ctx {
        fn step_head(&mut self, head: Head, tail: &mut Tail) -> (Action, Head) {
            self.now.tick();
            head.step(
                &mut self.close_requested,
                self.now,
                &mut self.ready_frontier,
                &mut self.shard_rx,
                self.stopping,
                tail,
                &self.task,
            )
        }

        fn step_tail(&mut self, tail: Tail) -> (Action, Tail) {
            self.now.tick();
            tail.step(
                &mut self.binding_bytes_behind,
                self.intents_idle,
                &mut self.legacy_checkpoint,
                self.now,
                &mut self.shard_rx,
                self.stats_idle.then_some(&mut self.pending_ack_intents),
                &self.task,
            )
        }
    }

    fn mk_task(n_shards: usize) -> Task {
        Task {
            binding_collection_names: vec!["source/collection".to_string()],
            binding_journal_read_suffixes: vec!["pivot=00".to_string()],
            binding_transform_names: vec!["my-transform".to_string()],
            close_policy: super::super::close_policy::Policy::new(Duration::ZERO, Duration::MAX),
            max_transactions: 0,
            n_shards,
            peers: (0..n_shards).map(|i| format!("shard-{i}")).collect(),
            remote_authoritative: false,
            shard_ref: ops::ShardRef::default(),
        }
    }

    fn mk_loaded(shard: usize) -> (usize, proto::Derive) {
        (
            shard,
            proto::Derive {
                loaded: Some(proto::derive::Loaded {
                    bindings: vec![proto::derive::loaded::Binding {
                        index: 0,
                        min_source_clock: uuid::Clock::from_unix(1_700_000_005, 0).as_u64(),
                        max_source_clock: uuid::Clock::from_unix(1_700_000_010, 0).as_u64(),
                        sourced_docs_total: 3,
                        sourced_bytes_total: 300,
                    }],
                    combiner_usage_bytes: 0,
                }),
                ..Default::default()
            },
        )
    }

    fn mk_flushed(shard: usize, patches: &'static [u8], more: bool) -> (usize, proto::Derive) {
        (
            shard,
            proto::Derive {
                flushed: Some(proto::derive::Flushed {
                    connector_patches_json: Bytes::from_static(patches),
                    more,
                }),
                ..Default::default()
            },
        )
    }

    fn mk_stored(shard: usize) -> (usize, proto::Derive) {
        (
            shard,
            proto::Derive {
                stored: Some(proto::derive::Stored {
                    published_docs_total: 4,
                    published_bytes_total: 400,
                    drained_docs_total: 3,
                    drained_bytes_total: 250,
                    publisher_commit: Some(proto::derive::stored::PublisherCommit {
                        producer: Bytes::from_static(&[0, 1, 2, 3, 4, 5]),
                        clock: 1000 + shard as u64,
                        journals: vec![format!("my/collection/pivot=0{shard}")],
                    }),
                }),
                ..Default::default()
            },
        )
    }

    fn mk_started_commit(shard: usize) -> (usize, proto::Derive) {
        (
            shard,
            proto::Derive {
                started_commit: Some(proto::derive::StartedCommit {}),
                ..Default::default()
            },
        )
    }

    fn mk_tail_persisted(tail: &Tail) -> (usize, proto::Derive) {
        let seq_no = match tail {
            Tail::Persist(p) => p.seq_no,
            other => panic!("expected Tail::Persist, got {other:?}"),
        };
        (
            0,
            proto::Derive {
                persisted: Some(proto::Persisted { seq_no }),
                ..Default::default()
            },
        )
    }

    /// Walks Tail through recovery replay of previously committed ACK intents,
    /// verifying they are written before new transactions begin.
    #[test]
    fn recovery_replay() {
        let task = mk_task(1);
        let mut ctx = Ctx {
            binding_bytes_behind: vec![0; task.binding_collection_names.len()],
            close_requested: false,
            intents_idle: true,
            legacy_checkpoint: None,
            now: uuid::Clock::from_unix(1_700_000_000, 0),
            pending_ack_intents: BTreeMap::new(),
            ready_frontier: None,
            shard_rx: None,
            stats_idle: false,
            stopping: false,
            task,
        };

        let recovered: BTreeMap<String, Bytes> = BTreeMap::from([(
            "ops/recovered".to_string(),
            Bytes::from_static(b"replay-intent"),
        )]);

        // Session starts with Tail::Recover seeded from RocksDB.
        let mut tail = Tail::Recover(TailRecover {
            ack_intents: recovered.clone(),
        });

        // Recover immediately emits WriteIntents with the recovered intents.
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        match action {
            Action::WriteIntents { ack_intents } => assert_eq!(ack_intents, recovered),
            other => panic!("expected WriteIntents, got {other:?}"),
        }
        assert!(matches!(tail, Tail::WriteIntents(_)));

        // Intents write in flight.
        ctx.intents_idle = false;
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::Idle));
        assert!(matches!(tail, Tail::WriteIntents(_)));

        // Intents write complete → Done.
        ctx.intents_idle = true;
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::Idle));
        assert!(matches!(tail, Tail::Done(_)));
    }

    /// Walks Head and Tail through two pipelined transactions and a graceful stop.
    #[test]
    fn happy_path_two_transactions_then_stop() {
        let task = mk_task(2);
        let mut ctx = Ctx {
            binding_bytes_behind: vec![0; task.binding_collection_names.len()],
            close_requested: false,
            intents_idle: true,
            legacy_checkpoint: None,
            now: uuid::Clock::from_unix(1_700_000_000, 0),
            pending_ack_intents: BTreeMap::new(),
            ready_frontier: None,
            shard_rx: None,
            stats_idle: false,
            stopping: false,
            task,
        };
        let mut head = Head::Idle(HeadIdle::default());
        let mut tail = Tail::Done(TailDone::default());

        // ===== Phase 1: txn 1 accumulate and flush =====

        ctx.ready_frontier = Some(shuffle::Frontier::default());
        let (action, h) = ctx.step_head(head, &mut tail);
        head = h;
        assert!(matches!(action, Action::Load { .. }));
        assert!(matches!(head, Head::Extend(_)));

        ctx.shard_rx = Some(mk_loaded(0));
        let (_action, h) = ctx.step_head(head, &mut tail);
        head = h;
        assert!(matches!(head, Head::Extend(_)));

        // Loaded(1) completes the Load round → HeadExtend re-polls into HeadIdle.
        ctx.shard_rx = Some(mk_loaded(1));
        let (action, h) = ctx.step_head(head, &mut tail);
        head = h;
        assert!(matches!(action, Action::PollAgain));
        assert!(matches!(head, Head::Idle(_)));

        // HeadIdle evaluates close policy: may_extend=true (frontier ready) → Load.
        ctx.ready_frontier = Some(shuffle::Frontier::default());
        let (action, h) = ctx.step_head(head, &mut tail);
        head = h;
        assert!(matches!(action, Action::Load { .. }));
        assert!(matches!(head, Head::Extend(_)));

        ctx.shard_rx = Some(mk_loaded(0));
        let (_action, h) = ctx.step_head(head, &mut tail);
        head = h;

        // Loaded(1) completes second round → HeadIdle re-polls; no frontier
        // queued → Flush.
        ctx.shard_rx = Some(mk_loaded(1));
        let (action, h) = ctx.step_head(head, &mut tail);
        head = h;
        assert!(matches!(action, Action::PollAgain));
        assert!(matches!(head, Head::Idle(_)));

        let (action, h) = ctx.step_head(head, &mut tail);
        head = h;
        assert!(matches!(action, Action::Flush { .. }));
        assert!(matches!(head, Head::Flush(_)));

        // Flushed × 2 → Rotate, Head::Idle.
        ctx.shard_rx = Some(mk_flushed(0, b"", false));
        let (_action, h) = ctx.step_head(head, &mut tail);
        head = h;

        ctx.shard_rx = Some(mk_flushed(1, b"", false));
        let (action, h) = ctx.step_head(head, &mut tail);
        head = h;
        let extents = match action {
            Action::Rotate { extents } => extents,
            other => panic!("expected Action::Rotate, got {other:?}"),
        };
        assert!(matches!(head, Head::Idle(_)));
        tail = Tail::Begin(TailBegin { extents });

        // ===== Phase 2: commit txn 1 (Tail) while Head pipelines txn 2 =====

        // TailBegin → Store.
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::Store));
        assert!(matches!(tail, Tail::Store(_)));

        // While Tail awaits Stored responses, Head opens txn 2.
        ctx.ready_frontier = Some(shuffle::Frontier::default());
        let (action, h) = ctx.step_head(head, &mut tail);
        head = h;
        assert!(matches!(action, Action::Load { .. }));
        assert!(matches!(head, Head::Extend(_)));

        // Loaded × 2 complete the Load round → back to HeadIdle.
        for s in 0..2 {
            ctx.shard_rx = Some(mk_loaded(s));
            let (_action, h) = ctx.step_head(head, &mut tail);
            head = h;
        }
        assert!(matches!(head, Head::Idle(_)));

        // Stored × 2 → WriteStats with publisher_commits.
        ctx.shard_rx = Some(mk_stored(0));
        let (_action, t) = ctx.step_tail(tail);
        tail = t;

        ctx.shard_rx = Some(mk_stored(1));
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(tail, Tail::WriteStats(_)));
        let (stats, publisher_commits) = match action {
            Action::WriteStats {
                stats,
                publisher_commits,
            } => (stats, publisher_commits),
            other => panic!("expected Action::WriteStats, got {other:?}"),
        };
        assert_eq!(publisher_commits.len(), 2);
        insta::assert_json_snapshot!(stats, @r#"
        {
          "_meta": {},
          "shard": {},
          "ts": "2023-11-14T22:13:20.000000004+00:00",
          "openSecondsTotal": 0.000000024,
          "txnCount": 1,
          "derive": {
            "transforms": {
              "my-transform": {
                "source": "source/collection",
                "input": {
                  "docsTotal": 12,
                  "bytesTotal": 1200
                },
                "lastSourcePublishedAt": "2023-11-14T22:13:30+00:00"
              }
            },
            "published": {
              "docsTotal": 8,
              "bytesTotal": 800
            },
            "out": {
              "docsTotal": 6,
              "bytesTotal": 500
            },
            "lastPublishedAt": "2023-11-14T22:13:30+00:00"
          }
        }
        "#);

        // Stats write in flight; TailWriteStats holds.
        ctx.stats_idle = false;
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::Idle));
        assert!(matches!(tail, Tail::WriteStats(_)));

        // Stats write complete; ACK intents available. This is a
        // runtime-authoritative task (remote_authoritative=false), so the Tail
        // commits directly with no StartCommit/StartedCommit round-trip.
        ctx.pending_ack_intents
            .insert("ops/journal".to_string(), Bytes::from_static(b"intent-1"));
        ctx.stats_idle = true;
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        ctx.stats_idle = false;
        assert!(matches!(tail, Tail::Persist(_)));
        let commit_persist = match action {
            Action::Persist { persist } => persist,
            other => panic!("expected Action::Persist, got {other:?}"),
        };
        // The Flush phase returned empty patches, so no connector state was persisted.
        assert!(commit_persist.connector_patches_json.is_empty());
        assert!(commit_persist.delete_ack_intents);
        assert!(!commit_persist.ack_intents.is_empty());

        // ===== Phase 3: stop signal; Tail finishes txn 1; Head closes txn 2 =====
        ctx.stopping = true;

        // Commit Persisted → Recover, with an immediate re-poll.
        ctx.shard_rx = Some(mk_tail_persisted(&tail));
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::PollAgain));
        assert!(matches!(tail, Tail::Recover(_)));

        // Recover → WriteIntents.
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::WriteIntents { .. }));
        assert!(matches!(tail, Tail::WriteIntents(_)));

        // WriteIntents completes → Done.
        ctx.intents_idle = true;
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::Idle));
        assert!(matches!(tail, Tail::Done(_)));

        // Narrow close policy so txn 2 closes immediately (stopping=true, tail=Done).
        // HeadIdle re-evaluates policy and emits Flush directly.
        ctx.task.close_policy.open_duration.end = Duration::from_nanos(1);
        let (action, h) = ctx.step_head(head, &mut tail);
        head = h;
        assert!(matches!(action, Action::Flush { .. }));

        // Rotate fires on the same step as the last Flushed.
        ctx.shard_rx = Some(mk_flushed(0, b"", false));
        let (_action, h) = ctx.step_head(head, &mut tail);
        head = h;

        ctx.shard_rx = Some(mk_flushed(1, b"", false));
        let (action, h) = ctx.step_head(head, &mut tail);
        head = h;
        let extents = match action {
            Action::Rotate { extents } => extents,
            other => panic!("expected Action::Rotate, got {other:?}"),
        };
        assert!(matches!(head, Head::Idle(_)));
        tail = Tail::Begin(TailBegin { extents });

        // Commit txn 2 (all IO completes immediately).
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::Store));
        for s in 0..2 {
            ctx.shard_rx = Some(mk_stored(s));
            let (_action, t) = ctx.step_tail(tail);
            tail = t;
        }
        assert!(matches!(tail, Tail::WriteStats(_)));
        ctx.stats_idle = true;
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        ctx.stats_idle = false;
        assert!(matches!(action, Action::Persist { .. }));
        assert!(matches!(tail, Tail::Persist(_)));
        ctx.shard_rx = Some(mk_tail_persisted(&tail));
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::PollAgain));
        assert!(matches!(tail, Tail::Recover(_)));
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::WriteIntents { .. }));
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::Idle));
        assert!(matches!(tail, Tail::Done(_)));

        // Head::Idle with stopping=true and Tail::Done → Head::Stop.
        let (action, h) = ctx.step_head(head, &mut tail);
        head = h;
        assert!(matches!(action, Action::PollAgain));
        assert!(matches!(head, Head::Stop));
        assert!(matches!(tail, Tail::Done(_)));
    }

    /// The Flush phase iterates while any shard requests another round
    /// (L:Flushed.more), propagating each iteration's aggregate into the next
    /// L:Flush, and halting once every shard reports `more = false`. The
    /// all-iterations aggregate is carried on the rotated Extents for persistence.
    #[test]
    fn flush_loop_aggregates_patches() {
        let task = mk_task(2);
        let mut ctx = Ctx {
            binding_bytes_behind: vec![0; task.binding_collection_names.len()],
            close_requested: false,
            intents_idle: true,
            legacy_checkpoint: None,
            now: uuid::Clock::from_unix(1_700_000_000, 0),
            pending_ack_intents: BTreeMap::new(),
            ready_frontier: None,
            shard_rx: None,
            stats_idle: false,
            stopping: false,
            task,
        };
        let mut tail = Tail::Done(TailDone::default());

        let mut head = Head::Flush(HeadFlush {
            extents: Extents {
                close: ctx.now,
                ..Default::default()
            },
            n_shards: 2,
            shard_flushed: vec![false; 2],
            iteration_patches: Vec::new(),
            all_patches: Vec::new(),
            any_more: false,
        });

        // Iteration 1: both shards contribute a patch and request another round.
        ctx.shard_rx = Some(mk_flushed(0, b"[{\"a\":0}\n]", true));
        let (action, h) = ctx.step_head(head, &mut tail);
        head = h;
        assert!(matches!(action, Action::Idle));
        assert!(matches!(head, Head::Flush(_)));

        ctx.shard_rx = Some(mk_flushed(1, b"[{\"a\":1}\n]", true));
        let (action, h) = ctx.step_head(head, &mut tail);
        head = h;
        // A `more` iteration begins another Flush carrying the aggregate.
        match action {
            Action::Flush { state_patches } => {
                assert_eq!(state_patches.as_ref(), b"[{\"a\":0}\n,{\"a\":1}\n]");
            }
            other => panic!("expected Action::Flush, got {other:?}"),
        }
        assert!(matches!(head, Head::Flush(_)));

        // Iteration 2: both shards report `more = false` → Rotate.
        ctx.shard_rx = Some(mk_flushed(0, b"", false));
        let (action, h) = ctx.step_head(head, &mut tail);
        head = h;
        assert!(matches!(action, Action::Idle));

        ctx.shard_rx = Some(mk_flushed(1, b"", false));
        let (action, h) = ctx.step_head(head, &mut tail);
        head = h;
        let extents = match action {
            Action::Rotate { extents } => extents,
            other => panic!("expected Action::Rotate, got {other:?}"),
        };
        assert!(matches!(head, Head::Idle(_)));
        // The all-iterations aggregate is carried for persistence.
        assert_eq!(
            extents.connector_patches.as_ref(),
            b"[{\"a\":0}\n,{\"a\":1}\n]"
        );
    }

    /// Remote-authoritative tasks send StartCommit, await an empty StartedCommit,
    /// then commit; and the Head holds back from opening a new transaction until
    /// the Tail has passed StartCommit.
    #[test]
    fn remote_authoritative_commit_and_holdback() {
        let task = mk_task(1);
        let mut ctx = Ctx {
            binding_bytes_behind: vec![0; task.binding_collection_names.len()],
            close_requested: false,
            intents_idle: true,
            legacy_checkpoint: None,
            now: uuid::Clock::from_unix(1_700_000_000, 0),
            pending_ack_intents: BTreeMap::new(),
            ready_frontier: None,
            shard_rx: None,
            stats_idle: false,
            stopping: false,
            task,
        };
        ctx.task.remote_authoritative = true;
        // Remote-authoritative tasks retain V1 rollback, so a legacy checkpoint
        // is always present (and supplies the StartCommit connector_checkpoint).
        ctx.legacy_checkpoint = Some((
            shuffle::Frontier::default(),
            consumer::Checkpoint::default(),
        ));

        // ----- Commit: WriteStats → StartCommit → StartedCommit → Persist -----
        ctx.now.tick();
        let close = ctx.now;
        let tail = Tail::WriteStats(TailWriteStats {
            extents: Extents {
                close,
                ..Default::default()
            },
        });
        ctx.pending_ack_intents
            .insert("ops/journal".to_string(), Bytes::from_static(b"intent"));
        ctx.stats_idle = true;

        let (action, tail) = ctx.step_tail(tail);
        assert!(matches!(action, Action::StartCommit { .. }));
        assert!(matches!(tail, Tail::StartCommit(_)));

        ctx.shard_rx = Some(mk_started_commit(0));
        let (action, tail) = ctx.step_tail(tail);
        assert!(matches!(action, Action::Persist { .. }));
        assert!(matches!(tail, Tail::Persist(_)));

        // ----- Holdback: Head waits to open a txn until Tail passes StartCommit -----
        ctx.stats_idle = false;
        ctx.ready_frontier = Some(shuffle::Frontier::default());

        // Tail still pre-commit (Store): the ready Frontier is held back.
        let mut tail = Tail::Store(TailStore {
            extents: Extents::default(),
            shard_stored: vec![false; 1],
            publisher_commits: Vec::new(),
        });
        let head = Head::Idle(HeadIdle::default());
        let (action, head) = ctx.step_head(head, &mut tail);
        assert!(matches!(action, Action::Idle));
        assert!(matches!(head, Head::Idle(_)));
        assert!(
            ctx.ready_frontier.is_some(),
            "frontier must not be consumed while held back"
        );

        // Tail now past StartCommit (Persist): the Head opens the transaction.
        let mut tail = Tail::Persist(TailPersist {
            seq_no: 1,
            next_action: Action::PollAgain,
            next_state: Box::new(Tail::Done(TailDone::default())),
        });
        let (action, head) = ctx.step_head(head, &mut tail);
        assert!(matches!(action, Action::Load { .. }));
        assert!(matches!(head, Head::Extend(_)));
        assert!(
            ctx.ready_frontier.is_none(),
            "frontier consumed once unblocked"
        );
    }
}
