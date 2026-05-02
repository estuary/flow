//! HeadFSM and TailFSM: the materialize Leader's pipelined transaction FSMs.
//!
//! HeadFSM drives the currently-open transaction toward commit:
//!   Idle → Extend → Flush → (Persist) → Store → Stored → WriteStats
//!        → StartCommit → Persist → {Rotate | Stop}
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
use super::{Task, frontier_mapping};
use crate::proto;
use gazette::consumer;
use proto_gazette::uuid;
use std::collections::BTreeMap;

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
    bindings: BTreeMap<u32, BindingExtents>,
}

#[derive(Debug, Default)]
pub struct BindingExtents {
    // Minimum source clock (flow_published_at) read by this binding.
    min_source_clock: uuid::Clock,
    // Maximum source clock (flow_published_at) read by this binding.
    max_source_clock: uuid::Clock,
    // Measures of read documents from source journals.
    read: ops::proto::stats::DocsAndBytes,
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
    pub max_keys: BTreeMap<u32, bytes::Bytes>,
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
/// Every non-terminator maps to exactly one IO primitive in the Actor's `dispatch()`.
#[derive(Debug)]
pub enum Action {
    /// Park until new IO arrives.
    Idle,
    /// Park with a precise deadline.
    Sleep {
        wake_after: std::time::Duration,
    },

    /// Broadcast a `L:Load` Frontier.
    Load {
        frontier: shuffle::Frontier,
    },
    /// Broadcast `L:Flush`.
    Flush {
        // Prior transaction's C:Acknowledged patches.
        connector_patches: bytes::Bytes,
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
    WriteStats {
        stats: ops::proto::Stats,
    },
    /// Persist one `proto::Persist` WriteBatch to shard zero.
    Persist {
        persist: proto::Persist,
    },
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
        trigger_params: bytes::Bytes,
    },

    Rotate {
        pending: PendingDeltas,
    },
}

impl Head {
    /// Dispatch to the current sub-state's `step()`.
    pub fn step(
        self,
        binding_bytes_behind: &mut [i64],
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
            Head::Idle(s) => s.step(now, ready_frontier, stopping, tail, task),
            Head::Extend(s) => s.step(now, ready_frontier, shard_rx, stopping, tail, task),
            Head::Flush(s) => s.step(now, shard_rx, task),
            Head::Persist(s) => s.step(shard_rx),
            Head::Store(s) => s.step(binding_bytes_behind, shard_rx, task),
            Head::WriteStats(s) => s.step(legacy_checkpoint, stats_write_idle, task),
            Head::StartCommit(s) => s.step(legacy_checkpoint, now, shard_rx, stopping),
            Head::Stop => panic!("HeadFSM::Stop observed at step boundary"),
        }
    }
}

impl Tail {
    /// Dispatch to the current sub-state's `step()`.
    pub fn step(
        self,
        intents_write_idle: bool,
        now: uuid::Clock,
        shard_rx: &mut Option<(usize, proto::Materialize)>,
        task: &Task,
        trigger_call_running: bool,
    ) -> (Action, Tail) {
        match self {
            Tail::Begin(s) => s.step(task),
            Tail::WriteIntents(s) => s.step(intents_write_idle),
            Tail::Acknowledge(s) => s.step(now, shard_rx),
            Tail::Trigger(s) => s.step(now, trigger_call_running),
            Tail::Persist(s) => s.step(shard_rx),
            Tail::Done(_) => (Action::Idle, self),
        }
    }
}

/// HeadIdle awaits a first ready Frontier that begins a transaction.
#[derive(Debug, Default)]
pub struct HeadIdle {
    /// Do we expect the next transaction to replay recovered transaction extents?
    pub idempotent_replay: bool,
    /// Close Clock of the last transaction, which may be recovered from a
    /// prior session, or zero.
    pub last_close: uuid::Clock,
}

impl HeadIdle {
    pub fn step(
        self,
        now: uuid::Clock,
        ready_frontier: &mut Option<shuffle::Frontier>,
        stopping: bool,
        tail: &Tail,
        task: &Task,
    ) -> (Action, Head) {
        // If Tail is Done and Head is Idle, stopping can complete without
        // starting another transaction. Otherwise Head may still pipeline
        // a next transaction while Tail finishes post-commit work.
        if stopping && matches!(tail, Tail::Done(_)) {
            return (Action::Idle, Head::Stop);
        }

        let Some(frontier) = ready_frontier.take() else {
            return (Action::Idle, Head::Idle(self));
        };

        // A frontier is ready, and we begin the transaction.
        let Self {
            idempotent_replay,
            last_close,
        } = self;

        let unresolved_hints = frontier.unresolved_hints != 0;
        let action = Action::Load {
            frontier: frontier.clone(),
        };
        let extents = Extents {
            open: now,
            frontier,
            ..Default::default()
        };
        let state = HeadExtend {
            extents,
            combiner_usage_bytes: vec![0; task.n_shards],
            idempotent_replay,
            last_close,
            max_key_deltas: BTreeMap::new(),
            shard_loaded: vec![false; task.n_shards],
            unresolved_hints,
        };
        (action, Head::Extend(state))
    }
}

/// HeadExtend drives ready frontiers into Load/Loaded cycles that
/// extend transaction Extents, until we begin to close.
#[derive(Debug)]
pub struct HeadExtend {
    pub extents: Extents,

    /// Running disk usage usage of per-shard combiners.
    pub combiner_usage_bytes: Vec<u64>,
    /// Are we replaying recovered transaction extents?
    /// When true, we MUST stop extending as soon as no unresolved hints remain.
    pub idempotent_replay: bool,
    /// Close Clock of the prior transaction (which may be from a prior session), or zero.
    pub last_close: uuid::Clock,
    /// Max-key deltas reported by Loaded responses.
    pub max_key_deltas: BTreeMap<u32, bytes::Bytes>,
    /// Per-shard tracking of Loaded response receipt.
    pub shard_loaded: Vec<bool>,
    /// Did the last-extended Frontier have unresolved causal hints?
    /// When true, we MUST extend rather than close.
    pub unresolved_hints: bool,
}

impl HeadExtend {
    pub fn step(
        mut self,
        now: uuid::Clock,
        ready_frontier: &mut Option<shuffle::Frontier>,
        shard_rx: &mut Option<(usize, proto::Materialize)>,
        stopping: bool,
        tail: &mut Tail,
        task: &Task,
    ) -> (Action, Head) {
        // Did we receive an expected Loaded response?
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
                binding_loaded,
                binding_read,
                combiner_usage_bytes,
                max_key_deltas,
            } = std::mem::take(loaded);

            reduce_docs_and_bytes(
                &mut self.extents.bindings,
                binding_loaded,
                |extents, dnb| {
                    extents.loaded.docs_total += dnb.docs_total;
                    extents.loaded.bytes_total += dnb.bytes_total;
                },
            );
            reduce_docs_and_bytes(&mut self.extents.bindings, binding_read, |extents, dnb| {
                extents.read.docs_total += dnb.docs_total;
                extents.read.bytes_total += dnb.bytes_total;
            });
            reduce_max_keys(&mut self.max_key_deltas, max_key_deltas);
            self.combiner_usage_bytes[*shard_index] = combiner_usage_bytes;

            // Mark received and consume `shard_rx`.
            self.shard_loaded[*shard_index] = true;
            shard_rx.take();

            if self.shard_loaded.iter().all(|b| *b) {
                self.shard_loaded.clear(); // All received.
            }
        }

        if !self.shard_loaded.is_empty() {
            return (Action::Idle, Head::Extend(self));
        }
        // We've received all expected Loaded responses.

        // Measures used to evaluate extend and close policy.
        let open_age = uuid::Clock::delta(now, self.extents.open);
        let last_age = uuid::Clock::delta(now, self.last_close);
        let max_combiner = self.combiner_usage_bytes.iter().max().unwrap();
        let (read_docs, read_bytes) = self
            .extents
            .bindings
            .values()
            .map(|extents| (extents.read.docs_total, extents.read.bytes_total))
            .fold((0, 0), |(a1, a2), (b1, b2)| (a1 + b1, a2 + b2));

        // Does our task policy wish us to extend the transaction?
        let policy_extend = open_age < task.open_duration.end
            && last_age < task.last_close_age.end
            && *max_combiner < task.combiner_usage_bytes.end
            && read_bytes < task.read_bytes.end
            && read_docs < task.read_docs.end;

        // Does our task policy wish us to close the transaction?
        // Usage-based measures saturate if !policy_extend (if they didn't,
        // we'd live-lock because the threshold cannot be reached).
        let policy_close = open_age > task.open_duration.start
            && last_age > task.last_close_age.start
            && (!policy_extend || *max_combiner > task.combiner_usage_bytes.start)
            && (!policy_extend || read_bytes > task.read_bytes.start)
            && (!policy_extend || read_docs > task.read_docs.start);

        // In addition to policy, we must hold open a transaction while awaiting
        // acknowledgment activities of the prior transaction, or resolution of
        // hints (notably, the full bounds of an idempotent recovered checkpoint).
        let may_close = policy_close && !self.unresolved_hints && matches!(tail, Tail::Done(_));

        // We may extend if we're NOT performing an exact replay, our policy allows,
        // and we're not stopping - or we are stopping but Tail is still busy.
        // The latter keeps the pipeline full while long post-commit work drains;
        // Head will stop after its next commit.
        let may_extend = !self.idempotent_replay && policy_extend && (!stopping || !may_close);
        // Override: if there are unresolved causal hints then we MUST extend.
        let may_extend = may_extend || self.unresolved_hints;

        // Should we extend with a ready checkpoint?
        if may_extend && let Some(frontier) = ready_frontier.take() {
            self.unresolved_hints = frontier.unresolved_hints != 0;
            self.extents.frontier = self.extents.frontier.reduce(frontier.clone());
            self.shard_loaded.resize(task.n_shards, false);
            return (Action::Load { frontier }, Head::Extend(self));
        }

        // Should we begin to close the transaction?
        if may_close {
            let Self {
                mut extents,
                max_key_deltas,
                ..
            } = self;

            extents.close = now;

            // Take C:Acknowledged patches of the prior transaction.
            let connector_patches = match tail {
                Tail::Done(done) => std::mem::take(&mut done.shard_patches),
                _ => unreachable!("may_close requires TailFSM::Done"),
            };

            let pending = PendingDeltas {
                max_keys: max_key_deltas,
                ..Default::default()
            };

            return (
                Action::Flush { connector_patches },
                Head::Flush(HeadFlush {
                    extents,
                    pending,
                    shard_flushed: vec![false; task.n_shards],
                }),
            );
        }

        // Compute next sleep deadline.
        let wake_after = [
            task.open_duration.start.checked_sub(open_age),
            task.open_duration.end.checked_sub(open_age),
            task.last_close_age.start.checked_sub(last_age),
            task.last_close_age.end.checked_sub(last_age),
        ]
        .into_iter()
        .filter_map(|s| s)
        .min();

        if let Some(wake_after) = wake_after {
            (Action::Sleep { wake_after }, Head::Extend(self))
        } else {
            (Action::Idle, Head::Extend(self))
        }
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
                binding_loaded,
                connector_patches_json,
            } = std::mem::take(flushed);

            reduce_docs_and_bytes(
                &mut self.extents.bindings,
                binding_loaded,
                |extents, dnb| {
                    extents.loaded.docs_total += dnb.docs_total;
                    extents.loaded.bytes_total += dnb.bytes_total;
                },
            );
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
            nonce: now.as_u64(),
            connector_patches_json: take_patches(&mut pending.persist_patches),
            delete_hinted_frontier: true,
            hinted_close_clock: extents.close.as_u64(),
            hinted_frontier: Some(shuffle::JournalFrontier::encode(&extents.frontier.journals)),
            max_keys: std::mem::take(&mut pending.max_keys),
            ..Default::default()
        };

        // Chain Store after the Persisted response.
        let store_state = HeadStore {
            extents,
            pending,
            shard_stored: vec![false; task.n_shards],
        };
        let persist_state = HeadPersist {
            nonce: persist.nonce,
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
    pub nonce: u64,
    pub next_action: Action,
    pub next_state: Box<Head>,
}

impl HeadPersist {
    pub fn step(self, shard_rx: &mut Option<(usize, proto::Materialize)>) -> (Action, Head) {
        if let Some((
            0,
            proto::Materialize {
                persisted: Some(proto::Persisted { nonce }),
                ..
            },
        )) = shard_rx
            && *nonce == self.nonce
        {
            shard_rx.take();

            let Self {
                nonce: _,
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
            let proto::materialize::Stored {
                binding_stored,
                first_source_clock,
                last_source_clock,
            } = std::mem::take(stored);

            reduce_docs_and_bytes(
                &mut self.extents.bindings,
                binding_stored,
                |extents, dnb| {
                    extents.stored.docs_total += dnb.docs_total;
                    extents.stored.bytes_total += dnb.bytes_total;
                },
            );

            // Reduce per-binding source clock bounds into binding extents.
            for (binding, clock) in first_source_clock {
                let entry = self.extents.bindings.entry(binding).or_default();
                let clock = uuid::Clock::from_u64(clock);
                entry.min_source_clock = if entry.min_source_clock == uuid::Clock::zero() {
                    clock
                } else {
                    entry.min_source_clock.min(clock)
                };
            }
            for (binding, clock) in last_source_clock {
                let entry = self.extents.bindings.entry(binding).or_default();
                entry.max_source_clock = uuid::Clock::from_u64(clock).max(entry.max_source_clock);
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
            extents,
            mut pending,
            ..
        } = self;

        // Fold deltas from the extents Frontier into per-binding "bytes behind" gauges.
        for jf in &extents.frontier.journals {
            let Some(entry) = binding_bytes_behind.get_mut(jf.binding as usize) else {
                continue; // Reachable if shuffle service reports invalid binding indices.
            };
            *entry += jf.bytes_behind_delta;
        }

        // Compose the trigger payload now that we have a complete txn-wide view.
        if task.triggers.is_some() {
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

            pending.trigger_params = serde_json::to_vec(&models::TriggerVariables {
                collection_names,
                connector_image: task.connector_image.clone(),
                materialization_name: task.shard_ref.name.clone(),
                flow_published_at_min: tokens::DateTime::from(min.to_time()).to_rfc3339(),
                flow_published_at_max: tokens::DateTime::from(max.to_time()).to_rfc3339(),
                run_id: tokens::DateTime::from(extents.open.to_time()).to_rfc3339(),
            })
            .unwrap()
            .into();
        }

        let action = Action::WriteStats {
            stats: build_stats_doc(task, &extents, binding_bytes_behind),
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
        legacy_checkpoint: &Option<(shuffle::Frontier, consumer::Checkpoint)>,
        now: uuid::Clock,
        shard_rx: &mut Option<(usize, proto::Materialize)>,
        stopping: bool,
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

        let Extents {
            close, frontier, ..
        } = extents;

        // If `legacy_checkpoint` is Some, then persist the legacy "checkpoint"
        // key to maintain a rollback capability to the V1 runtime.
        let legacy_checkpoint = legacy_checkpoint
            .as_ref()
            .map(|(_full_frontier, full_checkpoint)| full_checkpoint.clone());

        let persist = proto::Persist {
            nonce: now.as_u64(),
            ack_intents: pending.ack_intents.clone(),
            committed_close_clock: close.as_u64(),
            committed_frontier: Some(shuffle::JournalFrontier::encode(&frontier.journals)),
            connector_patches_json: take_patches(&mut pending.persist_patches),
            delete_ack_intents: true,
            legacy_checkpoint,
            max_keys: std::mem::take(&mut pending.max_keys),
            trigger_params_json: pending.trigger_params.clone(),
            ..Default::default()
        };

        // If we're `stopping`, then transition to Stop after Persist.
        // Otherwise, rotate to begin a next transaction.
        let (next_action, next_state) = if stopping {
            (Action::Idle, Head::Stop)
        } else {
            (
                Action::Rotate { pending },
                Head::Idle(HeadIdle {
                    idempotent_replay: false,
                    last_close: close,
                }),
            )
        };

        let state = HeadPersist {
            nonce: persist.nonce,
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
    pub fn step(self, task: &Task) -> (Action, Tail) {
        let Self { mut pending } = self;

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
                    max_keys,
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
        let (mut action, mut state) = if trigger_params.is_empty() {
            (Action::Idle, Tail::Done(TailDone { shard_patches }))
        } else {
            (
                Action::CallTrigger { trigger_params },
                Tail::Trigger(TailTrigger { shard_patches }),
            )
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
            let nonce = now.as_u64();

            state = Tail::Persist(TailPersist {
                nonce,
                next_action: action,
                next_state: Box::new(state),
            });
            action = Action::Persist {
                persist: proto::Persist {
                    nonce,
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
    pub fn step(self, now: uuid::Clock, trigger_call_running: bool) -> (Action, Tail) {
        if trigger_call_running {
            return (Action::Idle, Tail::Trigger(self));
        }

        let Self { shard_patches } = self;

        let nonce = now.as_u64();
        let action = Action::Persist {
            persist: proto::Persist {
                nonce,
                delete_trigger_params: true,
                ..Default::default()
            },
        };
        let state = TailPersist {
            nonce,
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
    pub nonce: u64,
    pub next_action: Action,
    pub next_state: Box<Tail>,
}

impl TailPersist {
    pub fn step(self, shard_rx: &mut Option<(usize, proto::Materialize)>) -> (Action, Tail) {
        if let Some((
            0,
            proto::Materialize {
                persisted: Some(proto::Persisted { nonce }),
                ..
            },
        )) = shard_rx
            && *nonce == self.nonce
        {
            shard_rx.take();

            let Self {
                nonce: _,
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

// Reduce per-binding DocsAndBytes by summing into `tgt`.
fn reduce_docs_and_bytes(
    tgt: &mut BTreeMap<u32, BindingExtents>,
    src: BTreeMap<u32, ops::proto::stats::DocsAndBytes>,
    fold: fn(&mut BindingExtents, ops::proto::stats::DocsAndBytes),
) {
    for (index, src_dnb) in src {
        let entry = tgt.entry(index).or_default();
        fold(entry, src_dnb);
    }
}

// Reduce per-binding max-key deltas by taking the max into `tgt`.
pub fn reduce_max_keys(tgt: &mut BTreeMap<u32, bytes::Bytes>, src: BTreeMap<u32, bytes::Bytes>) {
    for (binding, delta) in src {
        let entry = tgt.entry(binding).or_default();
        if *entry < delta {
            *entry = delta;
        }
    }
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
) -> ops::proto::Stats {
    let mut materialize: BTreeMap<String, ops::proto::stats::MaterializeBinding> = BTreeMap::new();

    for (binding_index, extents) in &extents.bindings {
        let Some(collection_name) = task.binding_collection_names.get(*binding_index as usize)
        else {
            continue; // Reachable if shards report invalid binding indices.
        };
        let entry = materialize.entry(collection_name.clone()).or_default();

        // It's possible that multiple bindings source from the same collection.
        // We accumulate when reporting by-collection.
        entry.bytes_behind = entry.bytes_behind.saturating_add_signed(
            binding_bytes_behind
                .get(*binding_index as usize)
                .copied()
                .unwrap_or_default(),
        );
        entry.last_source_published_at = extents.max_source_clock.to_pb_json_timestamp();

        ops::merge_docs_and_bytes(&extents.read, &mut entry.left);
        ops::merge_docs_and_bytes(&extents.loaded, &mut entry.right);
        ops::merge_docs_and_bytes(&extents.stored, &mut entry.out);
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
        materialize,
        capture: Default::default(), // N/A.
        derive: None,                // N/A.
        interval: None,              // N/A.
    }
}
