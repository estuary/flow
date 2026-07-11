//! HeadFSM and TailFSM: the materialize Leader's pipelined transaction FSMs.
//!
//! HeadFSM drives the currently-open transaction toward commit:
//!   Stop ← Idle ↔ Extend
//!          Idle → Flush → Persist(hint) → Store → WriteStats → StartCommit → Persist(commit) → Rotate
//!
//! TailFSM drives post-commit work for the prior transaction:
//!   Begin → Acknowledge → (Persist) → WriteIntents → (Trigger)
//!         → (Persist) → Done
//!
//! Head and Tail are pipelined. Tail may spend a long time in the connector's
//! post-commit phases (Acknowledge, Trigger), so Head may keep preparing a next
//! transaction while Tail finishes. When stopping, Head exits only once it is
//! idle with Tail already done, or after its next durable commit. Any post-
//! commit work for that last transaction is recovered and resumed by the next
//! leader session.

use super::super::frontier_mapping;
use super::{Task, close_policy, triggers};
use crate::proto;
use anyhow::Context;
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
}

#[derive(Debug, Default)]
pub struct BindingExtents {
    max_key_delta: bytes::Bytes,
    // Maximum source clock (flow_published_at) read by this binding.
    max_source_clock: uuid::Clock,
    // Minimum source clock (flow_published_at) read by this binding.
    min_source_clock: uuid::Clock,
    // Measures of documents read from source journals.
    sourced: ops::proto::stats::DocsAndBytes,
    // Measures of loaded documents from the materialized endpoint.
    loaded: ops::proto::stats::DocsAndBytes,
    // Measures of stored documents into the materialized endpoint.
    stored: ops::proto::stats::DocsAndBytes,
}

/// Delta state gathered from shard responses, and staged for emission
/// at later protocol points.
#[derive(Debug, Default)]
pub struct PendingDeltas {
    /// ACK Intents to write post-Acknowledge, keyed by journal.
    pub ack_intents: BTreeMap<String, bytes::Bytes>,
    /// Per-binding-index max-loaded-key deltas, for the next Persist.
    pub max_key_deltas: BTreeMap<u32, bytes::Bytes>,
    /// Queued connector state patches for the next Persist.
    pub persist_patches: Vec<u8>,
    /// Queued connector state patches for the next shards broadcast.
    pub shard_patches: Vec<u8>,
    /// Parameters for the post-Acknowledge trigger.
    pub trigger_params: bytes::Bytes,
}

#[derive(Debug)]
pub enum Head {
    Idle(HeadIdle),
    Extend(HeadExtend),
    Flush(HeadFlush),
    Persist(HeadPersist),
    Store(HeadStore),
    WriteStats(HeadWriteStats),
    StartCommit(HeadStartCommit),
    Stop,
}

#[derive(Debug)]
pub enum Tail {
    Begin(TailBegin),
    Acknowledge(TailAcknowledge),
    WriteIntents(TailWriteIntents),
    Trigger(TailTrigger),
    Persist(TailPersist),
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
    /// Broadcast `L:Flush`.
    Flush {
        // Prior transaction's C:Acknowledged patches.
        connector_patches: bytes::Bytes,
        // Backfill-begin markers observed this transaction, forwarded to each
        // shard's connector as a notification.
        backfill_begins: BTreeMap<u16, uuid::Clock>,
        // Backfill-complete markers observed this transaction.
        backfill_completes: BTreeMap<u16, uuid::Clock>,
    },
    /// Broadcast `L:Store`.
    Store,
    /// Broadcast `L:StartCommit` with this txn's C:Flushed patches.
    StartCommit {
        connector_patches: bytes::Bytes,
        connector_checkpoint: consumer::Checkpoint,
    },
    /// Publish a stats document as CONTINUE_TXN to the ops stats journal.
    // NOTE: when mapping this pattern into derivations, pass gathered ACK
    // intents from shards to the Actor from this Action variant, to pick up
    // later from `stats_write_idle`.
    WriteStats { stats: ops::proto::Stats },
    /// Persist one `proto::Persist` WriteBatch to shard zero.
    Persist { persist: proto::Persist },
    /// Write ACK intents to their journals.
    WriteIntents {
        ack_intents: BTreeMap<String, bytes::Bytes>,
    },
    /// Broadcast `L:Acknowledge` with this txn's aggregated StartedCommit patches.
    Acknowledge {
        // This committed transaction's C:StartedCommit patches.
        connector_patches: bytes::Bytes,
    },
    /// Start calling the trigger.
    /// Actor sets `trigger_done = false` upon dispatch.
    CallTrigger {
        triggers: std::sync::Arc<triggers::CompiledTriggers>,
        trigger_params: bytes::Bytes,
    },
    /// Rotate Tail from Done to Begin with the committed transaction's deltas.
    Rotate { pending: PendingDeltas },
    /// Fail the actor with a terminal error.
    Error { error: anyhow::Error },
}

impl Action {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Acknowledge { .. } => "Acknowledge",
            Self::CallTrigger { .. } => "CallTrigger",
            Self::Error { .. } => "Error",
            Self::Flush { .. } => "Flush",
            Self::Idle => "Idle",
            Self::Load { .. } => "Load",
            Self::Persist { .. } => "Persist",
            Self::PollAgain => "PollAgain",
            Self::Rotate { .. } => "Rotate",
            Self::Sleep { .. } => "Sleep",
            Self::StartCommit { .. } => "StartCommit",
            Self::Store => "Store",
            Self::WriteIntents { .. } => "WriteIntents",
            Self::WriteStats { .. } => "WriteStats",
        }
    }
}

impl Head {
    /// Dispatch to the current sub-state's `step()`.
    pub fn step(
        self,
        binding_bytes_behind: &mut [i64],
        close_requested: &mut bool,
        debounce: &mut TriggerDebounce,
        legacy_checkpoint: &mut Option<(shuffle::Frontier, consumer::Checkpoint)>,
        now: uuid::Clock,
        ready_frontier: &mut Option<shuffle::Frontier>,
        shard_rx: &mut Option<(usize, proto::Materialize)>,
        stats_write_idle: Option<&mut BTreeMap<String, bytes::Bytes>>,
        stopping: bool,
        tail: &mut Tail,
        task: &Task,
    ) -> (Action, Head) {
        match self {
            Head::Idle(s) => s.step(
                now,
                close_requested,
                debounce,
                ready_frontier,
                stopping,
                tail,
                task,
            ),
            Head::Extend(s) => s.step(shard_rx),
            Head::Flush(s) => s.step(now, shard_rx, task),
            Head::Persist(s) => s.step(shard_rx),
            Head::Store(s) => s.step(binding_bytes_behind, shard_rx, task),
            Head::WriteStats(s) => s.step(legacy_checkpoint, stats_write_idle, task),
            Head::StartCommit(s) => {
                s.step(debounce, legacy_checkpoint, now, shard_rx, stopping, task)
            }
            Head::Stop => panic!("HeadFSM::Stop observed at step boundary"),
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Idle(_) => "Idle",
            Self::Extend(_) => "Extend",
            Self::Flush(_) => "Flush",
            Self::Persist(_) => "Persist",
            Self::Store(_) => "Store",
            Self::WriteStats(_) => "WriteStats",
            Self::StartCommit(_) => "StartCommit",
            Self::Stop => "Stop",
        }
    }
}

impl Tail {
    /// Dispatch to the current sub-state's `step()`.
    pub fn step(
        self,
        debounce: &TriggerDebounce,
        intents_write_idle: bool,
        now: uuid::Clock,
        shard_rx: &mut Option<(usize, proto::Materialize)>,
        stopping: bool,
        task: &Task,
        trigger_call_running: bool,
    ) -> (Action, Tail) {
        match self {
            Tail::Begin(s) => s.step(stopping, task),
            Tail::WriteIntents(s) => s.step(intents_write_idle),
            Tail::Acknowledge(s) => s.step(now, shard_rx, task),
            Tail::Trigger(s) => s.step(debounce, now, trigger_call_running),
            Tail::Persist(s) => s.step(shard_rx),
            Tail::Done(_) => (Action::Idle, self),
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Begin(_) => "Begin",
            Self::Acknowledge(_) => "Acknowledge",
            Self::WriteIntents(_) => "WriteIntents",
            Self::Trigger(_) => "Trigger",
            Self::Persist(_) => "Persist",
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
    /// Are we replaying recovered transaction extents?
    /// When true, we MUST stop extending as soon as no unresolved hints remain.
    pub idempotent_replay: bool,
    /// Close Clock of the last transaction, which may be recovered from a
    /// prior session, or zero.
    pub last_close: uuid::Clock,
}

impl HeadIdle {
    pub fn step(
        mut self,
        now: uuid::Clock,
        close_requested: &mut bool,
        debounce: &mut TriggerDebounce,
        ready_frontier: &mut Option<shuffle::Frontier>,
        stopping: bool,
        tail: &mut Tail,
        task: &Task,
    ) -> (Action, Head) {
        let is_open = self.extents.open != uuid::Clock::zero();
        let tail_done = matches!(tail, Tail::Done(_));

        // Termination condition: stop at a clean transaction boundary.
        if stopping && !is_open && tail_done {
            return (Action::PollAgain, Head::Stop);
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
            idempotent_replay: self.idempotent_replay,
            last_age: uuid::Clock::delta(now, self.last_close),
            combiner_bytes,
            open_age,
            read_bytes,
            read_docs,
            stopping,
            tail_done,
            unresolved_hints: self.extents.frontier.unresolved_hints != 0
                // An unstarted `idempotent_replay` is itself an unresolved hint: the
                // recovered hints live in the pending `ready_frontier`, not yet in
                // `extents.frontier`, so without this the txn could neither extend
                // (replay suppresses policy extend) nor close — spinning Idle forever.
                || (self.idempotent_replay && !is_open),
        });

        // Should we extend with a ready next Frontier?
        if may_extend && let Some(frontier) = ready_frontier.take() {
            if !is_open {
                self.extents.open = now;
                self.combiner_usage_bytes = vec![0; task.n_shards];
            }

            // Extents fold in the full frontier minus an unresolved peek's
            // backfill markers — they must not reach Flush/durable state until
            // they resolve.
            let mut extents_delta = frontier.clone();
            if extents_delta.unresolved_hints != 0 {
                extents_delta.latest_backfill_begin = Default::default();
                extents_delta.latest_backfill_complete = Default::default();
            }
            self.extents.frontier = self.extents.frontier.reduce(extents_delta);

            return (
                Action::Load { frontier },
                Head::Extend(HeadExtend {
                    inner: self,
                    shard_loaded: vec![false; task.n_shards],
                }),
            );
        }

        // No transaction is open. Fire a debounced window that has come
        // due while the task is quiet.
        if !is_open {
            if let Tail::Done(done) = tail
                && let Some(compiled) = &task.triggers
            {
                if let Some(window) = debounce.take_due(now, compiled.interval) {
                    let shard_patches = std::mem::take(&mut done.shard_patches);
                    *tail = Tail::Trigger(TailTrigger { shard_patches });

                    return (
                        Action::CallTrigger {
                            triggers: compiled.clone(),
                            trigger_params: serde_json::to_vec(&window)
                                .expect("TriggerVariables always serialize")
                                .into(),
                        },
                        Head::Idle(self),
                    );
                }
                // Nothing due: wake when the pending window comes due.
                if let Some(wake_after) = debounce.next_due(now, compiled.interval) {
                    return (Action::Sleep { wake_after }, Head::Idle(self));
                }
            }
            return (Action::Idle, Head::Idle(self));
        } else if may_close {
            let Self { mut extents, .. } = self;
            extents.close = now;

            let connector_patches = match tail {
                Tail::Done(done) => std::mem::take(&mut done.shard_patches),
                _ => unreachable!("may_close requires TailFSM::Done"),
            };

            let max_keys = extents
                .bindings
                .iter()
                .filter_map(|(binding_index, extent)| {
                    if extent.max_key_delta.is_empty() {
                        None
                    } else {
                        Some((*binding_index, extent.max_key_delta.clone()))
                    }
                })
                .collect();

            let pending = PendingDeltas {
                max_key_deltas: max_keys,
                ..Default::default()
            };

            return (
                Action::Flush {
                    connector_patches,
                    backfill_begins: extents.frontier.latest_backfill_begin.clone(),
                    backfill_completes: extents.frontier.latest_backfill_complete.clone(),
                },
                Head::Flush(HeadFlush {
                    extents,
                    pending,
                    shard_flushed: vec![false; task.n_shards],
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
    pub fn step(mut self, shard_rx: &mut Option<(usize, proto::Materialize)>) -> (Action, Head) {
        if let Some((
            shard_index,
            proto::Materialize {
                loaded: Some(loaded),
                ..
            },
        )) = shard_rx
            && self.shard_loaded.get(*shard_index) == Some(&false)
        {
            let proto::materialize::Loaded {
                bindings,
                combiner_usage_bytes,
            } = std::mem::take(loaded);

            for proto::materialize::loaded::Binding {
                index,
                max_key_delta,
                max_source_clock,
                min_source_clock,
                sourced_bytes_total,
                sourced_docs_total,
            } in bindings
            {
                let min_source_clock = uuid::Clock::from_u64(min_source_clock);
                let max_source_clock = uuid::Clock::from_u64(max_source_clock);
                let extent = self.inner.extents.bindings.entry(index).or_default();

                extent.max_key_delta = std::mem::take(&mut extent.max_key_delta).max(max_key_delta);

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

/// HeadFlush awaits Flushed responses from all shards.
#[derive(Debug)]
pub struct HeadFlush {
    pub extents: Extents,
    pub pending: PendingDeltas,

    /// Per-shard tracking of Flushed response receipt.
    pub shard_flushed: Vec<bool>,
}

impl HeadFlush {
    pub fn step(
        mut self,
        now: uuid::Clock,
        shard_rx: &mut Option<(usize, proto::Materialize)>,
        task: &Task,
    ) -> (Action, Head) {
        // Did we receive an expected Flushed response?
        if let Some((
            shard_index,
            proto::Materialize {
                flushed: Some(flushed),
                ..
            },
        )) = shard_rx
            && self.shard_flushed.get(*shard_index) == Some(&false)
        {
            let proto::materialize::Flushed {
                bindings,
                connector_patches_json,
            } = std::mem::take(flushed);

            for crate::proto::materialize::flushed::Binding {
                index,
                loaded_bytes_total,
                loaded_docs_total,
            } in bindings
            {
                let extent = self.extents.bindings.entry(index).or_default();
                extent.loaded.bytes_total += loaded_bytes_total;
                extent.loaded.docs_total += loaded_docs_total;
            }
            extend_patches(&mut self.pending, &connector_patches_json);

            // Mark received and consume `shard_rx`.
            self.shard_flushed[*shard_index] = true;
            shard_rx.take();

            if self.shard_flushed.iter().all(|b| *b) {
                self.shard_flushed.clear(); // All received.
            }
        }

        if !self.shard_flushed.is_empty() {
            return (Action::Idle, Head::Flush(self));
        }
        // We've received all expected Flushed responses.

        let Self {
            extents,
            mut pending,
            ..
        } = self;

        // Persist extents for idempotent transaction replay.
        let persist = proto::Persist {
            seq_no: now.as_u64(),
            connector_patches_json: take_patches(&mut pending.persist_patches),
            delete_hinted_frontier: true,
            hinted_close_clock: extents.close.as_u64(),
            hinted_frontier: Some(shuffle::JournalFrontier::encode(&extents.frontier.journals)),
            max_keys: std::mem::take(&mut pending.max_key_deltas),
            ..Default::default()
        };

        // Chain Store after the Persisted response.
        let store_state = HeadStore {
            extents,
            pending,
            shard_stored: vec![false; task.n_shards],
        };
        let persist_state = HeadPersist {
            seq_no: persist.seq_no,
            next_action: Action::Store,
            next_state: Box::new(Head::Store(store_state)),
        };

        (Action::Persist { persist }, Head::Persist(persist_state))
    }
}

/// HeadPersist awaits a Persisted response from shard zero,
/// and chains its contained action and state.
#[derive(Debug)]
pub struct HeadPersist {
    pub seq_no: u64,
    pub next_action: Action,
    pub next_state: Box<Head>,
}

impl HeadPersist {
    pub fn step(self, shard_rx: &mut Option<(usize, proto::Materialize)>) -> (Action, Head) {
        if let Some((
            0,
            proto::Materialize {
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

        (Action::Idle, Head::Persist(self))
    }
}

/// HeadStore awaits Stored responses from all shards.
#[derive(Debug)]
pub struct HeadStore {
    pub extents: Extents,
    pub pending: PendingDeltas,

    /// Per-shard tracking of Stored response receipt.
    pub shard_stored: Vec<bool>,
}

impl HeadStore {
    pub fn step(
        mut self,
        binding_bytes_behind: &mut [i64],
        shard_rx: &mut Option<(usize, proto::Materialize)>,
        task: &Task,
    ) -> (Action, Head) {
        // Did we receive an expected Stored response?
        if let Some((
            shard_index,
            proto::Materialize {
                stored: Some(stored),
                ..
            },
        )) = shard_rx
            && self.shard_stored.get(*shard_index) == Some(&false)
        {
            let proto::materialize::Stored { bindings } = std::mem::take(stored);

            for proto::materialize::stored::Binding {
                index,
                stored_bytes_total,
                stored_docs_total,
            } in bindings
            {
                let extent = self.extents.bindings.entry(index).or_default();
                extent.stored.bytes_total += stored_bytes_total;
                extent.stored.docs_total += stored_docs_total;
            }

            // Mark received and consume `shard_rx`.
            self.shard_stored[*shard_index] = true;
            shard_rx.take();

            if self.shard_stored.iter().all(|b| *b) {
                self.shard_stored.clear(); // All received.
            }
        }

        if !self.shard_stored.is_empty() {
            return (Action::Idle, Head::Store(self));
        }
        // We've received all expected Stored responses.

        let Self {
            extents, pending, ..
        } = self;

        // Fold deltas from the extents Frontier into per-binding "bytes behind" gauges.
        for jf in &extents.frontier.journals {
            let Some(entry) = binding_bytes_behind.get_mut(jf.binding as usize) else {
                continue; // Reachable if shuffle service reports invalid binding indices.
            };
            *entry += jf.bytes_behind_delta;
        }

        let action = match build_stats_doc(task, &extents, binding_bytes_behind) {
            Ok(stats) => Action::WriteStats { stats },
            Err(error) => Action::Error { error },
        };
        let state = HeadWriteStats { extents, pending };

        (action, Head::WriteStats(state))
    }
}

/// HeadWriteStats awaits the completion of a stats document append and flush.
#[derive(Debug)]
pub struct HeadWriteStats {
    pub extents: Extents,
    pub pending: PendingDeltas,
}

impl HeadWriteStats {
    pub fn step(
        self,
        legacy_checkpoint: &mut Option<(shuffle::Frontier, consumer::Checkpoint)>,
        stats_write_idle: Option<&mut BTreeMap<String, bytes::Bytes>>,
        task: &Task,
    ) -> (Action, Head) {
        let ack_intents = match stats_write_idle {
            Some(ack_intents) => std::mem::take(ack_intents),
            None => return (Action::Idle, Head::WriteStats(self)),
        };
        // We've finished publishing to ops stats.

        let Self {
            extents,
            mut pending,
        } = self;

        // We use the existing consumer.Checkpoint `sources` structure to
        // piggyback the close Clock of this transaction under a special key.
        // This is compatible with deployed connectors which may parse and
        // re-serialize Checkpoints en-route to being stored in the endpoint,
        // whereas a new field would require more roll-out coordination.
        let (committed_close_key, committed_close_source) =
            frontier_mapping::encode_committed_close(extents.close);

        // Build the consumer checkpoint which will be threaded into StartCommit.
        // It must carry ACK intents because it may commit ahead of our own
        // recovery log (remote-authoritative pattern).
        //
        // If `legacy_checkpoint`, then we're preserving a rollback capability
        // to the V1 runtime. We reduce our delta Frontier extents into
        // `full_frontier`, map the result into `full_checkpoint`,
        // and then extend `connector_checkpoint` with `full_checkpoint`.
        let connector_checkpoint = if let Some((full_frontier, full_checkpoint)) = legacy_checkpoint
        {
            *full_frontier = std::mem::take(full_frontier).reduce(extents.frontier.clone());

            frontier_mapping::merge_frontier_into_checkpoint(
                full_frontier,
                full_checkpoint,
                &task.binding_journal_read_suffixes,
            );
            full_checkpoint
                .sources
                .insert(committed_close_key, committed_close_source);

            full_checkpoint.ack_intents = ack_intents.clone();
            full_checkpoint.clone()
        } else {
            consumer::Checkpoint {
                ack_intents: ack_intents.clone(),
                sources: [(committed_close_key, committed_close_source)].into(),
            }
        };

        // Track for future Persist and post-Acknowledge writes.
        pending.ack_intents = ack_intents;

        let action = Action::StartCommit {
            connector_checkpoint,
            connector_patches: take_patches(&mut pending.shard_patches),
        };
        let state = HeadStartCommit {
            extents,
            pending,
            shard_started_commit: vec![false; task.n_shards],
        };

        (action, Head::StartCommit(state))
    }
}

/// HeadStartCommit awaits StartedCommit responses from all shards.
#[derive(Debug)]
pub struct HeadStartCommit {
    pub extents: Extents,
    pub pending: PendingDeltas,

    /// Per-shard tracking of StartedCommit response receipt.
    pub shard_started_commit: Vec<bool>,
}

impl HeadStartCommit {
    pub fn step(
        mut self,
        debounce: &mut TriggerDebounce,
        legacy_checkpoint: &Option<(shuffle::Frontier, consumer::Checkpoint)>,
        now: uuid::Clock,
        shard_rx: &mut Option<(usize, proto::Materialize)>,
        stopping: bool,
        task: &Task,
    ) -> (Action, Head) {
        // Did we receive an expected StartedCommit response?
        if let Some((
            shard_index,
            proto::Materialize {
                started_commit: Some(started_commit),
                ..
            },
        )) = shard_rx
            && self.shard_started_commit.get(*shard_index) == Some(&false)
        {
            let proto::materialize::StartedCommit {
                connector_patches_json,
            } = std::mem::take(started_commit);

            extend_patches(&mut self.pending, &connector_patches_json);

            // Mark received and consume `shard_rx`.
            self.shard_started_commit[*shard_index] = true;
            shard_rx.take();

            if self.shard_started_commit.iter().all(|b| *b) {
                self.shard_started_commit.clear(); // All received.
            }
        }

        if !self.shard_started_commit.is_empty() {
            return (Action::Idle, Head::StartCommit(self));
        }
        // We've received all expected StartedCommit responses.

        let Self {
            extents,
            mut pending,
            ..
        } = self;

        // Merge this transaction's window into the debounce accumulator
        if let Some(window) = compute_trigger_window(task, &extents) {
            debounce.accumulate(&window);
        }
        let (trigger_params_json, delete_trigger_params) = debounce.to_persist();

        let Extents {
            close, frontier, ..
        } = extents;

        // If `legacy_checkpoint` is Some, then persist the legacy "checkpoint"
        // key to maintain a rollback capability to the V1 runtime.
        // The full Frontier was already merged into `full_checkpoint` during
        // HeadWriteStats::step().
        let legacy_checkpoint = legacy_checkpoint
            .as_ref()
            .map(|(_full_frontier, full_checkpoint)| full_checkpoint.clone());

        let persist = proto::Persist {
            seq_no: now.as_u64(),
            ack_intents: pending.ack_intents.clone(),
            committed_close_clock: close.as_u64(),
            committed_frontier: Some(shuffle::JournalFrontier::encode(&frontier.journals)),
            connector_patches_json: take_patches(&mut pending.persist_patches),
            delete_ack_intents: true,
            delete_trigger_params,
            legacy_checkpoint,
            max_keys: std::mem::take(&mut pending.max_key_deltas),
            trigger_params_json,
            ..Default::default()
        };

        // If we're `stopping`, then transition to Stop after Persist.
        let (next_action, next_state) = if stopping {
            // By construction, we know Tail is Done and all post-commit activity
            // of the *prior* transaction is completed. We halt after Persist
            // (commit) without starting any post-commit activity: that's left
            // for the next session, which will recover our commit state and
            // resume from Tail::Begin.
            (Action::PollAgain, Head::Stop)
        } else {
            // Move a due window out of the accumulator; the Tail delivers it
            // post-Acknowledge and then persists the emptied accumulator.
            if let Some(compiled) = &task.triggers
                && let Some(window) = debounce.take_due(now, compiled.interval)
            {
                pending.trigger_params = serde_json::to_vec(&window)
                    .expect("TriggerVariables always serialize")
                    .into();
            }

            // Rotate to begin a next transaction. `idempotent_replay`
            // is one-shot — only the first transaction of a session may replay
            // recovered hints, so post-Rotate HeadIdle is always non-replay.
            (
                Action::Rotate { pending },
                Head::Idle(HeadIdle {
                    last_close: close,
                    ..Default::default()
                }),
            )
        };

        let state = HeadPersist {
            seq_no: persist.seq_no,
            next_action,
            next_state: Box::new(next_state),
        };
        let action = Action::Persist { persist };

        (action, Head::Persist(state))
    }
}

/// TailBegin is the initial state of the Tail FSM.
/// The transaction has committed, but isn't confirmed to have been acknowledged.
#[derive(Debug)]
pub struct TailBegin {
    pub pending: PendingDeltas,
}

impl TailBegin {
    pub fn step(self, stopping: bool, task: &Task) -> (Action, Tail) {
        let Self { mut pending } = self;

        // `stopping` can be true here only if it:
        // a) arrived after commit Persist was emitted, but before Persisted,
        //    in which case Head emitted Rotate and not Stop, or
        // b) because `on_transaction_completed` tripped on `max_transactions`
        //    being reached.
        if stopping {
            let action = Action::Idle;
            let state = TailDone {
                shard_patches: bytes::Bytes::new(),
            };
            (action, Tail::Done(state))
        } else {
            let action = Action::Acknowledge {
                connector_patches: take_patches(&mut pending.shard_patches),
            };
            let state = TailAcknowledge {
                pending,
                shard_acknowledged: vec![false; task.n_shards],
            };

            (action, Tail::Acknowledge(state))
        }
    }
}

/// TailAcknowledge awaits Acknowledged responses from all shards.
#[derive(Debug)]
pub struct TailAcknowledge {
    pub pending: PendingDeltas,

    /// Per-shard tracking of Acknowledged response receipt.
    pub shard_acknowledged: Vec<bool>,
}

impl TailAcknowledge {
    pub fn step(
        mut self,
        now: uuid::Clock,
        shard_rx: &mut Option<(usize, proto::Materialize)>,
        task: &Task,
    ) -> (Action, Tail) {
        // Did we receive an expected Acknowledged response?
        if let Some((
            shard_index,
            proto::Materialize {
                acknowledged: Some(acknowledged),
                ..
            },
        )) = shard_rx
            && self.shard_acknowledged.get(*shard_index) == Some(&false)
        {
            let proto::materialize::Acknowledged {
                connector_patches_json,
            } = std::mem::take(acknowledged);

            extend_patches(&mut self.pending, &connector_patches_json);

            // Mark received and consume `shard_rx`.
            self.shard_acknowledged[*shard_index] = true;
            shard_rx.take();

            if self.shard_acknowledged.iter().all(|b| *b) {
                self.shard_acknowledged.clear(); // All received.
            }
        }

        if !self.shard_acknowledged.is_empty() {
            return (Action::Idle, Tail::Acknowledge(self));
        }
        // We've received all expected Acknowledged responses.

        let Self {
            pending:
                PendingDeltas {
                    ack_intents,
                    max_key_deltas: max_keys,
                    mut persist_patches,
                    mut shard_patches,
                    trigger_params,
                },
            shard_acknowledged: _,
        } = self;

        assert!(max_keys.is_empty());
        let persist_patches = take_patches(&mut persist_patches);
        let shard_patches = take_patches(&mut shard_patches);

        // Base: call the trigger if needed, else go straight to Done.
        let (mut action, mut state) = if let Some(triggers) = task.triggers.clone()
            && !trigger_params.is_empty()
        {
            (
                Action::CallTrigger {
                    triggers,
                    trigger_params,
                },
                Tail::Trigger(TailTrigger { shard_patches }),
            )
        } else {
            (Action::Idle, Tail::Done(TailDone { shard_patches }))
        };

        // Wrap with WriteIntents, so journal ACKs are appended immediately after
        // a post-Acknowledge Persist completes (if one is required).
        state = Tail::WriteIntents(TailWriteIntents {
            next_action: action,
            next_state: Box::new(state),
        });
        action = Action::WriteIntents { ack_intents };

        // If Acknowledged returned patches, wrap with a Persist that runs first.
        if !persist_patches.is_empty() {
            let seq_no = now.as_u64();

            state = Tail::Persist(TailPersist {
                seq_no,
                next_action: action,
                next_state: Box::new(state),
            });
            action = Action::Persist {
                persist: proto::Persist {
                    seq_no,
                    connector_patches_json: persist_patches,
                    ..Default::default()
                },
            };
        }

        (action, state)
    }
}

/// TailWriteIntents awaits the completion of ACK intent append and flush.
#[derive(Debug)]
pub struct TailWriteIntents {
    pub next_action: Action,
    pub next_state: Box<Tail>,
}

impl TailWriteIntents {
    pub fn step(self, intents_write_idle: bool) -> (Action, Tail) {
        if !intents_write_idle {
            return (Action::Idle, Tail::WriteIntents(self));
        }

        let Self {
            next_action,
            next_state,
        } = self;

        (next_action, *next_state)
    }
}

/// TailTrigger awaits the completion of a trigger call
#[derive(Debug)]
pub struct TailTrigger {
    pub shard_patches: bytes::Bytes,
}

impl TailTrigger {
    pub fn step(
        self,
        debounce: &TriggerDebounce,
        now: uuid::Clock,
        trigger_call_running: bool,
    ) -> (Action, Tail) {
        if trigger_call_running {
            return (Action::Idle, Tail::Trigger(self));
        }

        let Self { shard_patches } = self;

        let seq_no = now.as_u64();
        let (trigger_params_json, delete_trigger_params) = debounce.to_persist();
        let action = Action::Persist {
            persist: proto::Persist {
                seq_no,
                delete_trigger_params,
                trigger_params_json,
                ..Default::default()
            },
        };
        let state = TailPersist {
            seq_no,
            next_action: Action::Idle,
            next_state: Box::new(Tail::Done(TailDone { shard_patches })),
        };

        (action, Tail::Persist(state))
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
    pub fn step(self, shard_rx: &mut Option<(usize, proto::Materialize)>) -> (Action, Tail) {
        if let Some((
            0,
            proto::Materialize {
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

#[derive(Debug, Default)]
pub struct TailDone {
    pub shard_patches: bytes::Bytes,
}

/// Leader-lifetime debounce state for materialization triggers. Accumulates
/// per-transaction windows and gates firing to at most once per the task's
/// configured trigger `interval`.
#[derive(Debug, Default)]
pub struct TriggerDebounce {
    /// Accumulated, not-yet-fired window. Persisted.
    pub pending: Option<models::TriggerVariables>,
    /// Wall-clock of the last fire. In-memory only.
    pub last_fire: Option<uuid::Clock>,
}

impl TriggerDebounce {
    /// Merge one committed transaction's `window` into the accumulator.
    pub fn accumulate(&mut self, window: &models::TriggerVariables) {
        match &mut self.pending {
            Some(acc) => acc.merge(window),
            None => self.pending = Some(window.clone()),
        }
    }

    /// Remove and return the accumulated window if it's due to fire now
    pub fn take_due(
        &mut self,
        now: uuid::Clock,
        interval: Option<Duration>,
    ) -> Option<models::TriggerVariables> {
        let due = match (interval, self.last_fire) {
            (Some(interval), Some(last)) => uuid::Clock::delta(now, last) >= interval,
            _ => true, // No interval configured, or never fired.
        };
        if !due {
            return None;
        }
        let window = self.pending.take()?;
        self.last_fire = Some(now);
        Some(window)
    }

    /// Duration until the pending window comes due, or None when no window is
    /// pending or it has no future deadline.
    pub fn next_due(&self, now: uuid::Clock, interval: Option<Duration>) -> Option<Duration> {
        if self.pending.is_none() {
            return None;
        }
        let (Some(interval), Some(last)) = (interval, self.last_fire) else {
            return None;
        };
        Some(interval.saturating_sub(uuid::Clock::delta(now, last)))
    }

    /// Encode the accumulator for a `proto::Persist`
    pub fn to_persist(&self) -> (bytes::Bytes, bool) {
        match &self.pending {
            None => (bytes::Bytes::new(), true),
            Some(window) => (
                serde_json::to_vec(window)
                    .expect("TriggerVariables always serialize")
                    .into(),
                false,
            ),
        }
    }
}

/// Compose this transaction's trigger window from its committed `extents`, or None
fn compute_trigger_window(task: &Task, extents: &Extents) -> Option<models::TriggerVariables> {
    if task.triggers.is_none() || extents.bindings.is_empty() {
        return None;
    }

    let collection_names: Vec<String> = extents
        .bindings
        .keys()
        .filter_map(|idx| task.binding_collection_names.get(*idx as usize).cloned())
        .collect();

    let mut it = extents
        .bindings
        .values()
        .map(|extents| (extents.min_source_clock, extents.max_source_clock));
    let init = it.next().unwrap_or_default();
    let (min, max) = it.fold(init, |(min, max), (a, b)| (min.min(a), max.max(b)));

    Some(models::TriggerVariables {
        collection_names,
        connector_image: task.connector_image.clone(),
        materialization_name: task.shard_ref.name.clone(),
        flow_published_at_min: tokens::DateTime::from(min.to_time()).to_rfc3339(),
        flow_published_at_max: tokens::DateTime::from(max.to_time()).to_rfc3339(),
        run_id: tokens::DateTime::from(extents.open.to_time()).to_rfc3339(),
    })
}

// Extend separate accrued patches for a future Persist vs future shard broadcast,
// into `pending` from `src`.
pub fn extend_patches(pending: &mut PendingDeltas, src: &[u8]) {
    crate::patches::extend_state_patches(&mut pending.shard_patches, src);
    crate::patches::extend_state_patches(&mut pending.persist_patches, src);
}

// Take patches from `src`, leaving it empty, and freeze into Bytes.
pub fn take_patches(src: &mut Vec<u8>) -> bytes::Bytes {
    bytes::Bytes::from(std::mem::take(src))
}

/// Build an `ops::Stats` document snapshotting this transaction's extents.
fn build_stats_doc(
    task: &Task,
    extents: &Extents,
    binding_bytes_behind: &[i64],
) -> anyhow::Result<ops::proto::Stats> {
    let mut materialize: BTreeMap<String, ops::proto::stats::MaterializeBinding> = BTreeMap::new();

    for (binding_index, extents) in &extents.bindings {
        let collection_name = task
            .binding_collection_names
            .get(*binding_index as usize)
            .with_context(|| {
                format!("shard reported out-of-range binding index {binding_index}")
            })?;
        let entry = materialize.entry(collection_name.clone()).or_default();

        // It's possible that multiple bindings source from the same collection.
        // We accumulate when reporting by-collection.
        entry.bytes_behind = entry.bytes_behind.saturating_add_signed(
            binding_bytes_behind
                .get(*binding_index as usize)
                .copied()
                .unwrap_or_default(),
        );
        // Note that this measure can be clobbered if multiple bindings source
        // from the same collection. This is a little unfortunate, and implied by
        // the stats data-model. It's tempting to put a max() here, but that
        // doesn't fundamentally solve the problem (updates can arrive in distinct
        // txns, and then be reduded LWW by reporting). This can happen only when
        // two bindings share the *same* collection and *different* priorities
        // (otherwise they're same-cohort and process in lock-step).
        entry.last_source_published_at = extents.max_source_clock.to_pb_json_timestamp();

        ops::merge_docs_and_bytes(&extents.sourced, &mut entry.right);
        ops::merge_docs_and_bytes(&extents.loaded, &mut entry.left);
        ops::merge_docs_and_bytes(&extents.stored, &mut entry.out);
    }

    let open_seconds_total = uuid::Clock::delta(extents.close, extents.open).as_secs_f64();

    Ok(ops::proto::Stats {
        meta: Some(ops::proto::Meta {
            uuid: String::new(), // Stamped by publisher::Publisher::enqueue()
        }),
        shard: Some(task.shard_ref.clone()),
        timestamp: extents.open.to_pb_json_timestamp(),
        open_seconds_total,
        txn_count: 1,
        materialize,
        capture: Default::default(), // N/A.
        derive: None,                // N/A.
        interval: None,              // N/A.
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use gazette::consumer;
    use std::collections::BTreeMap;

    /// Aggregates the Actor's per-iteration locals so step_head / step_tail
    /// can be driven without recreating the actor's IO scaffolding.
    struct Ctx {
        binding_bytes_behind: Vec<i64>,
        close_requested: bool,
        debounce: TriggerDebounce,
        intents_idle: bool,
        legacy_checkpoint: Option<(shuffle::Frontier, consumer::Checkpoint)>,
        now: uuid::Clock,
        pending_ack_intents: BTreeMap<String, Bytes>,
        ready_frontier: Option<shuffle::Frontier>,
        shard_rx: Option<(usize, proto::Materialize)>,
        stats_idle: bool,
        stopping: bool,
        task: Task,
        trigger_running: bool,
    }

    impl Ctx {
        fn step_head(&mut self, head: Head, tail: &mut Tail) -> (Action, Head) {
            self.now.tick();
            head.step(
                &mut self.binding_bytes_behind,
                &mut self.close_requested,
                &mut self.debounce,
                &mut self.legacy_checkpoint,
                self.now,
                &mut self.ready_frontier,
                &mut self.shard_rx,
                self.stats_idle.then_some(&mut self.pending_ack_intents),
                self.stopping,
                tail,
                &self.task,
            )
        }

        fn step_tail(&mut self, tail: Tail) -> (Action, Tail) {
            self.now.tick();
            tail.step(
                &self.debounce,
                self.intents_idle,
                self.now,
                &mut self.shard_rx,
                self.stopping,
                &self.task,
                self.trigger_running,
            )
        }
    }

    fn mk_task(n_shards: usize) -> Task {
        // Wide thresholds so `policy_extend` is always true and `policy_close`
        // only trips via `close_requested`. This keeps the test free of
        // policy-driven close timing.
        Task {
            binding_collection_names: vec!["test/collection".to_string()],
            binding_journal_read_suffixes: vec!["pivot=00".to_string()],
            close_policy: close_policy::Policy::new(Duration::ZERO, Duration::MAX),
            connector_image: String::new(),
            max_transactions: 0,
            n_shards,
            peers: (0..n_shards).map(|i| format!("shard-{i}")).collect(),
            shard_ref: ops::ShardRef::default(),
            triggers: Some(std::sync::Arc::new(
                super::super::triggers::CompiledTriggers::compile(models::Triggers {
                    // No interval: fire every transaction that materializes data
                    interval: None,
                    config: vec![models::TriggerConfig {
                        url: "https://example.com/hook".to_string(),
                        method: models::HttpMethod::POST,
                        headers: Default::default(),
                        payload_template: "{}".to_string(),
                        timeout: Duration::from_secs(30),
                        max_attempts: 3,
                    }],
                    sops: None,
                })
                .unwrap(),
            )),
        }
    }

    fn mk_loaded(shard: usize) -> (usize, proto::Materialize) {
        (
            shard,
            proto::Materialize {
                loaded: Some(proto::materialize::Loaded {
                    bindings: vec![proto::materialize::loaded::Binding {
                        index: 0,
                        min_source_clock: uuid::Clock::from_unix(1_700_000_005, 0).as_u64(),
                        max_source_clock: uuid::Clock::from_unix(1_700_000_010, 0).as_u64(),
                        sourced_docs_total: 3,
                        sourced_bytes_total: 300,
                        max_key_delta: Bytes::from_static(b"\x05\x06\x07"),
                    }],
                    combiner_usage_bytes: 0,
                }),
                ..Default::default()
            },
        )
    }

    /// `mk_loaded` variant that overrides `max_key_delta` on the (sole)
    /// binding, for tests that exercise its per-binding reduction.
    fn mk_loaded_with_key(shard: usize, key: &'static [u8]) -> (usize, proto::Materialize) {
        let (shard, mut msg) = mk_loaded(shard);
        msg.loaded.as_mut().unwrap().bindings[0].max_key_delta = Bytes::from_static(key);
        (shard, msg)
    }

    fn mk_flushed(shard: usize, patches: &'static [u8]) -> (usize, proto::Materialize) {
        (
            shard,
            proto::Materialize {
                flushed: Some(proto::materialize::Flushed {
                    bindings: vec![proto::materialize::flushed::Binding {
                        index: 0,
                        loaded_docs_total: 2,
                        loaded_bytes_total: 200,
                    }],
                    connector_patches_json: Bytes::from_static(patches),
                }),
                ..Default::default()
            },
        )
    }

    fn mk_stored(shard: usize) -> (usize, proto::Materialize) {
        (
            shard,
            proto::Materialize {
                stored: Some(proto::materialize::Stored {
                    bindings: vec![proto::materialize::stored::Binding {
                        index: 0,
                        stored_docs_total: 4,
                        stored_bytes_total: 400,
                    }],
                }),
                ..Default::default()
            },
        )
    }

    fn mk_started_commit(shard: usize, patches: &'static [u8]) -> (usize, proto::Materialize) {
        (
            shard,
            proto::Materialize {
                started_commit: Some(proto::materialize::StartedCommit {
                    connector_patches_json: Bytes::from_static(patches),
                }),
                ..Default::default()
            },
        )
    }

    fn mk_acknowledged(shard: usize, patches: &'static [u8]) -> (usize, proto::Materialize) {
        (
            shard,
            proto::Materialize {
                acknowledged: Some(proto::materialize::Acknowledged {
                    connector_patches_json: Bytes::from_static(patches),
                }),
                ..Default::default()
            },
        )
    }

    fn mk_head_persisted(head: &Head) -> (usize, proto::Materialize) {
        let seq_no = match head {
            Head::Persist(p) => p.seq_no,
            other => panic!("expected Head::Persist, got {other:?}"),
        };
        (
            0,
            proto::Materialize {
                persisted: Some(proto::Persisted { seq_no }),
                ..Default::default()
            },
        )
    }

    fn mk_tail_persisted(tail: &Tail) -> (usize, proto::Materialize) {
        let seq_no = match tail {
            Tail::Persist(p) => p.seq_no,
            other => panic!("expected Tail::Persist, got {other:?}"),
        };
        (
            0,
            proto::Materialize {
                persisted: Some(proto::Persisted { seq_no }),
                ..Default::default()
            },
        )
    }

    /// Walks Head and Tail through two pipelined transactions and a graceful
    /// stop. No IO; each step mutates Ctx fields and reads back the
    /// (Action, State) tuple.
    ///
    /// Phase 1: txn 1 is an idempotent replay of a recovered transaction.
    ///          The initial HeadIdle carries `idempotent_replay` with empty
    ///          extents, so the first extend is forced by the synthetic
    ///          "unstarted replay is an unresolved hint" bootstrap. A second
    ///          frontier resolves the recovered hints, after which the replay
    ///          closes and drives the full commit sequence to Action::Rotate.
    /// Phase 2: rotation hands `pending` to Tail::Begin. Head opens txn 2
    ///          (one Load); while Head awaits the second Loaded, Tail's full
    ///          post-acknowledge sequence runs interleaved: Acknowledged x2
    ///          (with patches) → Persist → Persisted → WriteIntents. Head
    ///          then receives Loaded(1) and extends txn 2 with another Load
    ///          round.
    /// Phase 3: `stopping` is set; Tail drains WriteIntents → CallTrigger →
    ///          Persist → Persisted → Done.
    /// Phase 4: Head commits txn 2; with `stopping=true` HeadStartCommit
    ///          chains into (Action::Idle, Head::Stop) instead of Rotate.
    #[test]
    fn happy_path_two_transactions_then_stop() {
        let task = mk_task(2);
        let mut ctx = Ctx {
            binding_bytes_behind: vec![0; task.binding_collection_names.len()],
            close_requested: false,
            debounce: TriggerDebounce::default(),
            intents_idle: true,
            legacy_checkpoint: None,
            now: uuid::Clock::from_unix(1_700_000_000, 0),
            pending_ack_intents: BTreeMap::new(),
            ready_frontier: None,
            shard_rx: None,
            stats_idle: false,
            stopping: false,
            task,
            trigger_running: false,
        };
        // txn 1 begins as an idempotent replay: HeadIdle carries
        // `idempotent_replay` with empty extents (as `handler::serve` builds it
        // on recovery of a prepared-but-uncommitted transaction).
        let mut head = Head::Idle(HeadIdle {
            idempotent_replay: true,
            ..Default::default()
        });
        let mut tail = Tail::Done(TailDone::default());

        // ===== Phase 1: txn 1 lifecycle (idempotent replay) =====

        // The recovered hints arrive as a `ready_frontier` peek, not yet in
        // `extents.frontier`. Replay suppresses policy-driven extend, so the
        // first Load is forced only by the "unstarted replay is an unresolved
        // hint" bootstrap. Absent it, HeadIdle would spin without progress.
        ctx.ready_frontier = Some(shuffle::Frontier {
            unresolved_hints: 1,
            ..Default::default()
        });
        let (action, h) = ctx.step_head(head, &mut tail);
        head = h;
        assert!(matches!(action, Action::Load { .. }));
        assert!(matches!(head, Head::Extend(_)));

        // Loaded(0) lands; HeadExtend still awaits Loaded(1) and rests.
        ctx.shard_rx = Some(mk_loaded(0));
        let (_action, h) = ctx.step_head(head, &mut tail);
        head = h;
        assert!(matches!(head, Head::Extend(_)));

        // Loaded(1) completes the Load round → HeadExtend re-polls into HeadIdle.
        // A second frontier with the hints now resolved is available; the still
        // -unresolved extents hints force HeadIdle to extend rather than close.
        ctx.ready_frontier = Some(shuffle::Frontier::default());
        ctx.shard_rx = Some(mk_loaded(1));
        let (action, h) = ctx.step_head(head, &mut tail);
        head = h;
        assert!(matches!(action, Action::PollAgain));
        assert!(matches!(head, Head::Idle(_)));

        let (action, h) = ctx.step_head(head, &mut tail);
        head = h;
        assert!(matches!(action, Action::Load { .. }));
        assert!(matches!(head, Head::Extend(_)));

        // Second Load round applied the hint-resolving frontier, so extents now
        // carry no unresolved hints. Loaded x2 arrive without another frontier
        // queued; HeadExtend re-polls into HeadIdle. With the replay's hints
        // resolved, the close policy force-closes the replay txn → Flush.
        ctx.shard_rx = Some(mk_loaded(0));
        let (_action, h) = ctx.step_head(head, &mut tail);
        head = h;
        assert!(matches!(head, Head::Extend(_)));

        ctx.shard_rx = Some(mk_loaded(1));
        let (action, h) = ctx.step_head(head, &mut tail);
        head = h;
        assert!(matches!(action, Action::PollAgain));
        assert!(matches!(head, Head::Idle(_)));

        let (action, h) = ctx.step_head(head, &mut tail);
        head = h;
        assert!(matches!(action, Action::Flush { .. }));
        assert!(matches!(head, Head::Flush(_)));

        // Flushed x2 with distinct connector state patches → idempotency
        // Persist that carries the merged Flushed patches.
        ctx.shard_rx = Some(mk_flushed(0, b"[{\"phase\":\"flushed\",\"shard\":0}\t]"));
        let (_action, h) = ctx.step_head(head, &mut tail);
        head = h;

        ctx.shard_rx = Some(mk_flushed(1, b"[{\"phase\":\"flushed\",\"shard\":1}\t]"));
        let (action, h) = ctx.step_head(head, &mut tail);
        head = h;
        assert!(matches!(head, Head::Persist(_)));
        let persist = match action {
            Action::Persist { persist } => persist,
            other => panic!("expected Action::Persist, got {other:?}"),
        };
        insta::assert_debug_snapshot!(
            (&persist.connector_patches_json, &persist.max_keys),
            @r#"
        (
            b"[{\"phase\":\"flushed\",\"shard\":0}\t,{\"phase\":\"flushed\",\"shard\":1}\t]",
            {
                0: b"\x05\x06\x07",
            },
        )
        "#);

        // Persisted (shard 0) → Store.
        ctx.shard_rx = Some(mk_head_persisted(&head));
        let (action, h) = ctx.step_head(head, &mut tail);
        head = h;
        assert!(matches!(action, Action::Store));
        assert!(matches!(head, Head::Store(_)));

        // Stored x2 → WriteStats. Capture the stats action of the second
        // step for an inline snapshot of the resulting stats document.
        let mut write_stats_action = None;
        for s in 0..2 {
            ctx.shard_rx = Some(mk_stored(s));
            let (action, h) = ctx.step_head(head, &mut tail);
            head = h;
            if s == 1 {
                write_stats_action = Some(action);
            }
        }
        assert!(matches!(head, Head::WriteStats(_)));
        let stats = match write_stats_action.unwrap() {
            Action::WriteStats { stats } => stats,
            other => panic!("expected Action::WriteStats, got {other:?}"),
        };
        insta::assert_json_snapshot!(stats, @r#"
        {
          "_meta": {},
          "shard": {},
          "ts": "2023-11-14T22:13:20.000001+00:00",
          "openSecondsTotal": 0.000006,
          "txnCount": 1,
          "materialize": {
            "test/collection": {
              "left": {
                "docsTotal": 4,
                "bytesTotal": 400
              },
              "right": {
                "docsTotal": 12,
                "bytesTotal": 1200
              },
              "out": {
                "docsTotal": 8,
                "bytesTotal": 800
              },
              "lastSourcePublishedAt": "2023-11-14T22:13:30+00:00"
            }
          }
        }
        "#);

        // Stats publish completes; ACK intents become available → StartCommit.
        ctx.pending_ack_intents
            .insert("ops/journal".to_string(), Bytes::from_static(b"intent-1"));
        ctx.stats_idle = true;
        let (action, h) = ctx.step_head(head, &mut tail);
        head = h;
        ctx.stats_idle = false;
        assert!(matches!(action, Action::StartCommit { .. }));
        assert!(matches!(head, Head::StartCommit(_)));

        // StartedCommit x2 with distinct connector state patches → committing
        // Persist that carries the merged StartedCommit patches.
        ctx.shard_rx = Some(mk_started_commit(
            0,
            b"[{\"phase\":\"committed\",\"shard\":0}\t]",
        ));
        let (_action, h) = ctx.step_head(head, &mut tail);
        head = h;

        ctx.shard_rx = Some(mk_started_commit(
            1,
            b"[{\"phase\":\"committed\",\"shard\":1}\t]",
        ));
        let (action, h) = ctx.step_head(head, &mut tail);
        head = h;
        assert!(matches!(head, Head::Persist(_)));
        let persist = match action {
            Action::Persist { persist } => persist,
            other => panic!("expected Action::Persist, got {other:?}"),
        };
        insta::assert_debug_snapshot!(
            (&persist.connector_patches_json, &persist.trigger_params_json),
            @r#"
        (
            b"[{\"phase\":\"committed\",\"shard\":0}\t,{\"phase\":\"committed\",\"shard\":1}\t]",
            b"{\"collection_names\":[\"test/collection\"],\"connector_image\":\"\",\"materialization_name\":\"\",\"flow_published_at_min\":\"2023-11-14T22:13:25+00:00\",\"flow_published_at_max\":\"2023-11-14T22:13:30+00:00\",\"run_id\":\"2023-11-14T22:13:20.000001+00:00\"}",
        )
        "#);

        // Final Persisted → Action::Rotate (since !stopping). Head returns to
        // Idle. The Actor's Rotate dispatch transitions Tail::Begin(pending).
        ctx.shard_rx = Some(mk_head_persisted(&head));
        let (action, h) = ctx.step_head(head, &mut tail);
        head = h;
        let pending = match action {
            Action::Rotate { pending } => pending,
            other => panic!("expected Action::Rotate, got {other:?}"),
        };
        assert!(matches!(head, Head::Idle(_)));
        tail = Tail::Begin(TailBegin { pending });

        // ===== Phase 2: pipeline txn 2; Tail's post-acknowledge sequence
        //              interleaves between Head's Load and second Loaded =====

        // TailBegin → Acknowledge.
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::Acknowledge { .. }));
        assert!(matches!(tail, Tail::Acknowledge(_)));

        // Head opens txn 2 via a fresh ready Frontier — pipelined with Tail.
        ctx.ready_frontier = Some(shuffle::Frontier::default());
        let (action, h) = ctx.step_head(head, &mut tail);
        head = h;
        assert!(matches!(action, Action::Load { .. }));
        assert!(matches!(head, Head::Extend(_)));

        // Head receives Loaded(0) (one of two); HeadExtend waits for Loaded(1).
        ctx.shard_rx = Some(mk_loaded(0));
        let (_action, h) = ctx.step_head(head, &mut tail);
        head = h;
        assert!(matches!(head, Head::Extend(_)));

        // --- Interleave: Tail's post-acknowledge work between Loaded(0)
        //     and Loaded(1) of Head's txn 2 Load round ---

        // Acknowledged from shard 0 carries connector patches.
        ctx.shard_rx = Some(mk_acknowledged(0, b"[{\"phase\":\"acked\",\"shard\":0}\t]"));
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::Idle));
        assert!(matches!(tail, Tail::Acknowledge(_)));

        // Acknowledged from shard 1 carries no patches; Tail has now seen all
        // Acknowledged. Because shard 0's patches are non-empty, the chain
        // wraps with TailPersist and emits Action::Persist *first*. The
        // WriteIntents and CallTrigger steps will fire after Persisted.
        ctx.shard_rx = Some(mk_acknowledged(1, b""));
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(tail, Tail::Persist(_)));
        let persist = match action {
            Action::Persist { persist } => persist,
            other => panic!("expected Action::Persist, got {other:?}"),
        };
        insta::assert_debug_snapshot!(&persist.connector_patches_json, @r#"b"[{\"phase\":\"acked\",\"shard\":0}\t]""#);

        // Persisted (post-Acknowledge) → chained next_action = WriteIntents.
        ctx.shard_rx = Some(mk_tail_persisted(&tail));
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::WriteIntents { .. }));
        assert!(matches!(tail, Tail::WriteIntents(_)));

        // --- End interleave; Head receives Loaded(1) to complete the round. ---
        // HeadExtend re-polls into HeadIdle; a new frontier is available → extend.

        ctx.ready_frontier = Some(shuffle::Frontier::default());
        ctx.shard_rx = Some(mk_loaded(1));
        let (action, h) = ctx.step_head(head, &mut tail);
        head = h;
        assert!(matches!(action, Action::PollAgain));
        assert!(matches!(head, Head::Idle(_)));

        let (action, h) = ctx.step_head(head, &mut tail);
        head = h;
        assert!(matches!(action, Action::Load { .. }));
        assert!(matches!(head, Head::Extend(_)));

        // Second Load round of txn 2: Loaded x2 → HeadIdle.
        for s in 0..2 {
            ctx.shard_rx = Some(mk_loaded(s));
            let (_action, h) = ctx.step_head(head, &mut tail);
            head = h;
        }
        assert!(matches!(head, Head::Idle(_)));

        // ===== Phase 3: stop signal; drain Tail through trigger to Done =====

        ctx.stopping = true;

        // WriteIntents → CallTrigger (intents publish completed; task has
        // triggers configured so trigger_params is non-empty).
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::CallTrigger { .. }));
        assert!(matches!(tail, Tail::Trigger(_)));

        // Trigger call completes (trigger_running=false) → final Persist
        // (with delete_trigger_params=true).
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::Persist { .. }));
        assert!(matches!(tail, Tail::Persist(_)));

        // Persisted → Tail::Done.
        ctx.shard_rx = Some(mk_tail_persisted(&tail));
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::Idle));
        assert!(matches!(tail, Tail::Done(_)));

        // ===== Phase 4: commit txn 2 under stopping; observe Head::Stop =====

        // Drive close via policy this time (Phase 1 covered `close_requested`).
        // Shrinking `open_duration.end` below the current `open_age` flips
        // `policy_extend` to false, which lets `policy_close` trip and (under
        // `stopping`) suppresses extend so HeadIdle closes on the next step.
        ctx.task.close_policy.open_duration.end = Duration::from_nanos(1);
        let (action, h) = ctx.step_head(head, &mut tail);
        head = h;
        assert!(matches!(action, Action::Flush { .. }));

        for s in 0..2 {
            ctx.shard_rx = Some(mk_flushed(s, b""));
            let (_action, h) = ctx.step_head(head, &mut tail);
            head = h;
        }
        ctx.shard_rx = Some(mk_head_persisted(&head));
        let (_action, h) = ctx.step_head(head, &mut tail);
        head = h;
        assert!(matches!(head, Head::Store(_)));

        for s in 0..2 {
            ctx.shard_rx = Some(mk_stored(s));
            let (_action, h) = ctx.step_head(head, &mut tail);
            head = h;
        }
        assert!(matches!(head, Head::WriteStats(_)));

        ctx.stats_idle = true;
        let (action, h) = ctx.step_head(head, &mut tail);
        head = h;
        ctx.stats_idle = false;
        assert!(matches!(action, Action::StartCommit { .. }));

        for s in 0..2 {
            ctx.shard_rx = Some(mk_started_commit(s, b""));
            let (_action, h) = ctx.step_head(head, &mut tail);
            head = h;
        }

        // Final Persisted under stopping: HeadStartCommit chained
        // (next_action, next_state) = (PollAgain, Head::Stop) — no Rotate.
        // PollAgain (not Idle) lets the actor loop exit `while !Head::Stop`
        // immediately rather than parking for a 60s ACTOR_TICK_INTERVAL.
        ctx.shard_rx = Some(mk_head_persisted(&head));
        let (action, h) = ctx.step_head(head, &mut tail);
        head = h;
        assert!(matches!(action, Action::PollAgain));
        assert!(matches!(head, Head::Stop));
        assert!(matches!(tail, Tail::Done(_)));
    }

    /// Verifies aggregation of L:Loaded `max_key_delta` across shards and Load cycles.
    // On recovery, `handler` seeds `Tail::Begin` with the persisted trigger
    // accumulator as the window to fire. The Tail must re-fire it verbatim
    // (at-least-once), and it flows independently of the live (empty) accumulator.
    #[test]
    fn recovery_refires_persisted_accumulator() {
        let recovered = models::TriggerVariables::placeholder();
        let serialized = serde_json::to_vec(&recovered).unwrap();

        let mut ctx = Ctx {
            binding_bytes_behind: vec![0; 1],
            close_requested: false,
            debounce: TriggerDebounce::default(),
            intents_idle: true,
            legacy_checkpoint: None,
            now: uuid::Clock::from_unix(1_700_000_000, 0),
            pending_ack_intents: BTreeMap::new(),
            ready_frontier: None,
            shard_rx: None,
            stats_idle: false,
            stopping: false,
            task: mk_task(1),
            trigger_running: false,
        };

        // Recovery injects the persisted accumulator as the Tail's to_fire set.
        let mut tail = Tail::Begin(TailBegin {
            pending: PendingDeltas {
                trigger_params: Bytes::from(serialized),
                ..Default::default()
            },
        });

        // Begin → Acknowledge.
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::Acknowledge { .. }));

        // Single shard Acknowledged, no patches → WriteIntents (CallTrigger chained).
        ctx.shard_rx = Some(mk_acknowledged(0, b""));
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::WriteIntents { .. }));

        // Intents written → CallTrigger carrying the recovered window verbatim.
        let (action, _t) = ctx.step_tail(tail);
        let params = match action {
            Action::CallTrigger { trigger_params, .. } => trigger_params,
            other => panic!("expected CallTrigger, got {other:?}"),
        };
        let fired: models::TriggerVariables = serde_json::from_slice(&params).unwrap();
        assert_eq!(fired, recovered, "recovered accumulator re-fires verbatim");
        assert!(
            ctx.debounce.pending.is_none(),
            "recovery re-fire does not touch the live accumulator",
        );
    }

    // A debounced window fires from Idle once its interval elapses, with no
    // further transaction: HeadIdle sleeps until the deadline, then emits
    // CallTrigger and rotates the Tail through its normal fire →
    // Persist(reduced accumulator) → Done sequence.
    #[test]
    fn idle_fires_debounced_window_after_interval() {
        let mut task = mk_task(1);
        task.triggers = Some(std::sync::Arc::new(
            super::super::triggers::CompiledTriggers::compile(models::Triggers {
                interval: Some(Duration::from_secs(600)),
                config: vec![models::TriggerConfig {
                    url: "https://example.com/hook".to_string(),
                    method: models::HttpMethod::POST,
                    headers: Default::default(),
                    payload_template: "{}".to_string(),
                    timeout: Duration::from_secs(30),
                    max_attempts: 3,
                }],
                sops: None,
            })
            .unwrap(),
        ));

        let t0 = uuid::Clock::from_unix(1_700_000_000, 0);
        let mut ctx = Ctx {
            binding_bytes_behind: vec![0; 1],
            close_requested: false,
            debounce: TriggerDebounce::default(),
            intents_idle: true,
            legacy_checkpoint: None,
            now: t0,
            pending_ack_intents: BTreeMap::new(),
            ready_frontier: None,
            shard_rx: None,
            stats_idle: false,
            stopping: false,
            task,
            trigger_running: false,
        };

        // Seed: triggers last fired at t0, and a window accumulated since.
        ctx.debounce.last_fire = Some(t0);
        ctx.debounce.pending = Some(models::TriggerVariables::placeholder());

        let mut tail = Tail::Done(TailDone {
            shard_patches: Bytes::new(),
        });
        let mut head = Head::Idle(HeadIdle::default());

        // Idle before the deadline: Head sleeps until the window comes due.
        ctx.now = uuid::Clock::from_unix(1_700_000_100, 0); // t0 + 100s.
        let (action, h) = ctx.step_head(head, &mut tail);
        head = h;
        let Action::Sleep { wake_after } = action else {
            panic!("expected Sleep, got {action:?}");
        };
        assert!(
            wake_after > Duration::from_secs(499) && wake_after <= Duration::from_secs(500),
            "expected ~500s until due, got {wake_after:?}",
        );

        // Past the deadline: fire from Idle, with no transaction in flight.
        ctx.now = uuid::Clock::from_unix(1_700_000_601, 0); // t0 + 601s.
        let (action, h) = ctx.step_head(head, &mut tail);
        head = h;
        let params = match action {
            Action::CallTrigger { trigger_params, .. } => trigger_params,
            other => panic!("expected CallTrigger, got {other:?}"),
        };
        let fired: models::TriggerVariables = serde_json::from_slice(&params).unwrap();
        assert_eq!(fired, models::TriggerVariables::placeholder());
        assert!(matches!(head, Head::Idle(_)));
        assert!(matches!(tail, Tail::Trigger(_)));
        assert!(ctx.debounce.pending.is_none());
        assert_eq!(ctx.debounce.last_fire, Some(ctx.now));

        // Trigger completes → Persist deleting the (now empty) accumulator → Done.
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        let persist = match action {
            Action::Persist { persist } => persist,
            other => panic!("expected Persist, got {other:?}"),
        };
        assert!(persist.delete_trigger_params);
        assert!(persist.trigger_params_json.is_empty());

        ctx.shard_rx = Some(mk_tail_persisted(&tail));
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::Idle));
        assert!(matches!(tail, Tail::Done(_)));

        // Head is quiescent again: nothing pending, no timer to arm.
        let (action, _h) = ctx.step_head(head, &mut tail);
        assert!(matches!(action, Action::Idle));
    }

    #[test]
    fn loaded_max_key_delta_reduction() {
        let task = mk_task(2);
        let mut ctx = Ctx {
            binding_bytes_behind: vec![0; task.binding_collection_names.len()],
            close_requested: false,
            debounce: TriggerDebounce::default(),
            intents_idle: true,
            legacy_checkpoint: None,
            now: uuid::Clock::from_unix(1_700_000_000, 0),
            pending_ack_intents: BTreeMap::new(),
            ready_frontier: None,
            shard_rx: None,
            stats_idle: false,
            stopping: false,
            task,
            trigger_running: false,
        };
        let mut head = Head::Idle(HeadIdle::default());
        let mut tail = Tail::Done(TailDone::default());

        // Open the first transaction.
        ctx.ready_frontier = Some(shuffle::Frontier::default());
        let (_a, h) = ctx.step_head(head, &mut tail);
        head = h;

        // Each row is one Load cycle: per-shard Loaded values for `max_key_delta`
        // and the expected aggregated value after the cycle. The cycles share
        // a single open transaction, so reductions must compose across cycles.
        let cycles: &[(&[&'static [u8]], &'static [u8])] = &[
            // Cross-shard reduction: shard 1's "Z" beats shard 0's "M".
            (&[b"M", b"Z"], b"Z"),
            // Smaller "A" and an empty report must not clobber the prior "Z".
            (&[b"", b"A"], b"Z"),
            // Strictly-larger "Z9" ratchets the maximum forward.
            (&[b"Z9", b""], b"Z9"),
        ];

        for (i, (per_shard_keys, expected)) in cycles.iter().enumerate() {
            for (shard, key) in per_shard_keys.iter().enumerate() {
                ctx.shard_rx = Some(mk_loaded_with_key(shard, *key));
                let (_a, h) = ctx.step_head(head, &mut tail);
                head = h;
            }
            // All shards loaded → HeadExtend returned to HeadIdle.
            assert!(
                matches!(head, Head::Idle(_)),
                "expected Head::Idle after cycle {i}",
            );
            let aggregated = match &head {
                Head::Idle(s) => s.extents.bindings[&0].max_key_delta.clone(),
                other => panic!("expected Head::Idle after cycle {i}, got {other:?}"),
            };
            assert_eq!(
                aggregated,
                Bytes::from_static(expected),
                "after cycle {i} keys={per_shard_keys:?}",
            );

            // Queue next frontier and let HeadIdle extend into the next Load cycle.
            ctx.ready_frontier = Some(shuffle::Frontier::default());
            let (_a, h) = ctx.step_head(head, &mut tail);
            head = h;
            assert!(
                matches!(head, Head::Extend(_)),
                "expected Head::Extend at start of cycle {}",
                i + 1,
            );
        }
    }

    /// Fuzz Head and Tail by perturbing every Ctx field at each step.
    /// Random shard responses, frontiers, and idle/stopping flags drive
    /// arbitrary state transitions; the test asserts no panics. The FSMs
    /// are expected to handle malformed or out-of-order inputs gracefully
    /// (ignore unexpected responses, hold their current state) rather than
    /// crashing — most random sequences therefore never advance to commit,
    /// but they also never trip an `unwrap`/`unreachable!`/index panic.
    #[test]
    fn fuzz_head_tail_no_panics() {
        use rand::{Rng, SeedableRng, rngs::SmallRng};

        // Synthesize a Materialize message of a randomly chosen variant. The
        // `expected_seq_no` is plumbed through so Persisted occasionally matches
        // the in-progress seq_no and lets HeadPersist / TailPersist actually
        // chain forward — without it, fuzz traces would rarely leave Persist.
        fn random_message(
            shard: usize,
            expected_seq_no: u64,
            rng: &mut SmallRng,
        ) -> (usize, proto::Materialize) {
            let mut msg = proto::Materialize::default();
            // Cap accumulator inputs to keep `+= bytes_total` etc. far from
            // u64 overflow over the fuzz length (Rust panics on debug overflow).
            match rng.random_range(0..6) {
                0 => {
                    msg.loaded = Some(proto::materialize::Loaded {
                        bindings: vec![proto::materialize::loaded::Binding {
                            index: rng.random_range(0..3),
                            min_source_clock: rng.random(),
                            max_source_clock: rng.random(),
                            sourced_bytes_total: rng.random_range(0..1_000),
                            sourced_docs_total: rng.random_range(0..100),
                            max_key_delta: Bytes::from_static(b"\x01\x02\x03"),
                        }],
                        combiner_usage_bytes: rng.random_range(0..1_000_000),
                    });
                }
                1 => {
                    msg.flushed = Some(proto::materialize::Flushed {
                        bindings: vec![proto::materialize::flushed::Binding {
                            index: rng.random_range(0..3),
                            loaded_bytes_total: rng.random_range(0..1_000),
                            loaded_docs_total: rng.random_range(0..100),
                        }],
                        connector_patches_json: Bytes::from_static(b"[{\"f\":1}\t]"),
                    });
                }
                2 => {
                    msg.stored = Some(proto::materialize::Stored {
                        bindings: vec![proto::materialize::stored::Binding {
                            index: rng.random_range(0..3),
                            stored_bytes_total: rng.random_range(0..1_000),
                            stored_docs_total: rng.random_range(0..100),
                        }],
                    });
                }
                3 => {
                    msg.started_commit = Some(proto::materialize::StartedCommit {
                        connector_patches_json: Bytes::from_static(b"[{\"sc\":1}\t]"),
                    });
                }
                4 => {
                    msg.acknowledged = Some(proto::materialize::Acknowledged {
                        connector_patches_json: Bytes::from_static(b"[{\"ack\":1}\t]"),
                    });
                }
                _ => {
                    // Most of the time, target the in-progress Persist's seq_no so
                    // the FSM can actually chain forward; otherwise emit garbage.
                    let seq_no = if rng.random_bool(0.9) {
                        expected_seq_no
                    } else {
                        rng.random()
                    };
                    msg.persisted = Some(proto::Persisted { seq_no });
                }
            }
            (shard, msg)
        }

        // Pick a "best-guess" seq_no to hand to `random_message`. When Head or
        // Tail is awaiting Persisted we surface its seq_no so the message is
        // sometimes accepted; otherwise return random noise.
        fn pick_seq_no(head: &Head, tail: &Tail, rng: &mut SmallRng) -> u64 {
            if let Head::Persist(p) = head {
                return p.seq_no;
            }
            if let Tail::Persist(p) = tail {
                return p.seq_no;
            }
            rng.random()
        }

        fn perturb(ctx: &mut Ctx, head: &Head, tail: &Tail, rng: &mut SmallRng) {
            ctx.now.tick();

            // Independently flip each Boolean knob with low probability so a
            // run typically spans many distinct (close_requested, stopping,
            // *_idle, trigger_running) combinations.
            if rng.random_bool(0.20) {
                ctx.close_requested = !ctx.close_requested;
            }
            if rng.random_bool(0.20) {
                ctx.intents_idle = !ctx.intents_idle;
            }
            if rng.random_bool(0.20) {
                ctx.stats_idle = !ctx.stats_idle;
            }
            // `stopping` is stickier: flipping rarely lets fuzz traces actually
            // reach Head::Stop instead of toggling out of it on the next step.
            if rng.random_bool(0.05) {
                ctx.stopping = !ctx.stopping;
            }
            if rng.random_bool(0.20) {
                ctx.trigger_running = !ctx.trigger_running;
            }

            // Inject a Frontier with a randomized `unresolved_hints` so we
            // cover the unresolved-hints branch of the close policy. Journals
            // are kept empty to avoid Frontier validation invariants.
            if rng.random_bool(0.30) {
                ctx.ready_frontier = Some(shuffle::Frontier {
                    unresolved_hints: if rng.random_bool(0.7) {
                        0
                    } else {
                        rng.random_range(1..3)
                    },
                    ..Default::default()
                });
            }

            // Inject a shard response. Allow shard index up to n_shards
            // (sometimes out-of-range) to exercise bounds handling.
            if rng.random_bool(0.50) {
                let shard = rng.random_range(0..=ctx.task.n_shards);
                let seq_no = pick_seq_no(head, tail, rng);
                ctx.shard_rx = Some(random_message(shard, seq_no, rng));
            }

            // Add an ACK intent occasionally; HeadWriteStats drains them.
            if rng.random_bool(0.10) {
                ctx.pending_ack_intents.insert(
                    format!("ops/journal-{}", rng.random_range(0..4)),
                    Bytes::from_static(b"intent"),
                );
            }

            // Toggle `legacy_checkpoint` to cover the V1-rollback merge branch.
            if rng.random_bool(0.05) {
                ctx.legacy_checkpoint = if ctx.legacy_checkpoint.is_some() {
                    None
                } else {
                    Some((
                        shuffle::Frontier::default(),
                        consumer::Checkpoint::default(),
                    ))
                };
            }
        }

        fn prop(seed: u64) -> bool {
            let mut rng = SmallRng::seed_from_u64(seed);
            let n_shards = rng.random_range(1..=4);

            // Narrow the close-policy thresholds (vs `mk_task`'s wide ranges)
            // so `policy_extend` flips false after a single typical Loaded
            // response, which lets `policy_close` trip frequently and drives
            // fuzz traces through Flush / Store / Persist / Rotate. Without
            // this, Head spends almost the entire trace in Extend.
            let mut task = mk_task(n_shards);
            task.close_policy.combiner_usage_bytes = 0..10_000;
            task.close_policy.read_bytes = 0..500;
            task.close_policy.read_docs = 0..20;

            let mut ctx = Ctx {
                binding_bytes_behind: vec![0; 3],
                close_requested: false,
                debounce: TriggerDebounce::default(),
                intents_idle: false,
                legacy_checkpoint: None,
                now: uuid::Clock::from_unix(1_700_000_000, 0),
                pending_ack_intents: BTreeMap::new(),
                ready_frontier: None,
                shard_rx: None,
                stats_idle: false,
                stopping: false,
                task,
                trigger_running: false,
            };
            let mut head = Head::Idle(HeadIdle::default());
            let mut tail = Tail::Done(TailDone::default());

            for _ in 0..256 {
                perturb(&mut ctx, &head, &tail, &mut rng);

                if rng.random_bool(0.5) {
                    // Head::Stop panics at the step boundary by contract, so
                    // skip stepping it. The Actor analogously stops dispatching
                    // once Head reaches Stop.
                    if !matches!(head, Head::Stop) {
                        let (action, h) = ctx.step_head(head, &mut tail);
                        head = h;
                        // Mirror the Actor's Rotate dispatch: hand `pending` to
                        // Tail::Begin so fuzz traces actually exercise Tail's
                        // Acknowledge / WriteIntents / Trigger paths after a
                        // Head commit, instead of leaving Tail wedged in Done.
                        if let Action::Rotate { pending } = action {
                            tail = Tail::Begin(TailBegin { pending });
                        }
                    }
                } else {
                    let (_action, t) = ctx.step_tail(tail);
                    tail = t;
                }
            }
            true
        }

        quickcheck::QuickCheck::new()
            .tests(200)
            .max_tests(400)
            .quickcheck(prop as fn(u64) -> bool);
    }
}

#[cfg(test)]
mod debounce_tests {
    use super::*;
    use std::time::Duration;

    fn window(collection: &str, min: &str, max: &str) -> models::TriggerVariables {
        models::TriggerVariables {
            collection_names: vec![collection.to_string()],
            connector_image: "img".to_string(),
            materialization_name: "mat".to_string(),
            flow_published_at_min: min.to_string(),
            flow_published_at_max: max.to_string(),
            run_id: min.to_string(),
        }
    }

    fn t(secs: u64) -> uuid::Clock {
        uuid::Clock::from_unix(secs, 0)
    }

    // A burst of transactions within one interval collapses into a single
    // delivery whose window spans the union of the collapsed transactions.
    #[test]
    fn burst_within_interval_collapses_to_one_fire() {
        let interval = Some(Duration::from_secs(600));
        let mut d = TriggerDebounce::default();

        // First qualifying txn: never fired, so it's due immediately.
        d.accumulate(&window("c", "t00", "t00"));
        assert!(d.take_due(t(0), interval).is_some(), "first txn fires");

        // Two more txns inside the 600s window: accumulated but suppressed.
        d.accumulate(&window("c", "t01", "t01"));
        assert!(d.take_due(t(60), interval).is_none(), "debounced at t=60");
        d.accumulate(&window("c", "t02", "t02"));
        assert!(d.take_due(t(120), interval).is_none(), "debounced at t=120");

        // Once the interval elapses, the single fire covers the merged window.
        let w = d.take_due(t(600), interval).expect("fires once elapsed");
        assert_eq!(w.flow_published_at_min, "t01", "min spans the burst");
        assert_eq!(w.flow_published_at_max, "t02", "max spans the burst");
        assert!(d.pending.is_none(), "accumulator drained after fire");
    }

    // With no interval, every qualifying transaction fires (pre-debounce behavior).
    #[test]
    fn no_interval_fires_every_transaction() {
        let mut d = TriggerDebounce::default();

        for i in 0..3 {
            d.accumulate(&window("c", "t", "t"));
            assert!(d.take_due(t(i), None).is_some(), "fires on txn {i}");
            assert!(d.pending.is_none());
        }
    }
}
