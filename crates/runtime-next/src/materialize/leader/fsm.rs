//! HeadFSM and TailFSM: the materialize Leader's pipelined transaction FSMs.
//!
//! HeadFSM drives the currently-open transaction toward commit:
//!   Idle → Extend → Flush → (Persist) → StartCommit
//!        → PublishStats → Persist → {Rotate | Stop}
//!
//! TailFSM drives post-commit work for the prior transaction:
//!   Begin → Acknowledge → (Persist) → WriteAcks → (Trigger)
//!         → (Persist) → Done
//!
//! Head and Tail are intentionally pipelined. Tail may spend a long
//! time in the connector's post-commit phases (Acknowledge, Trigger),
//! so Head may keep preparing a next transaction while Tail finishes.
//! When stopping, Head exits only once it is idle with Tail already done,
//! or after its next durable commit; any post-commit work for that last
//! transaction is recovered and resumed by the next leader session.
//!
use crate::materialize::leader::{state, triggers};
use crate::proto;
use proto_gazette::uuid;
use std::collections::BTreeMap;

/// Per-transaction aggregated state threaded through the HeadFSM.
#[derive(Debug, Default)]
pub struct Extents {
    pub opened: uuid::Clock,
    pub closed: uuid::Clock,
    pub frontier: shuffle::Frontier,
    pub binding_read: BTreeMap<u32, ops::proto::stats::DocsAndBytes>,
    pub binding_loaded: BTreeMap<u32, ops::proto::stats::DocsAndBytes>,
    pub binding_stored: BTreeMap<u32, ops::proto::stats::DocsAndBytes>,
    /// Per-binding min source-document `Clock`, reduced (min) across all
    /// shards. Only bindings that received documents are present.
    pub first_source_clock: BTreeMap<u32, u64>,
    /// Per-binding max source-document `Clock`, reduced (max) across all
    /// shards. Only bindings that received documents are present.
    pub last_source_clock: BTreeMap<u32, u64>,
}

/// Delta state gathered from shard responses, and staged for emission
/// at later protocol points.
#[derive(Debug, Default)]
pub struct PendingDeltas {
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
    StartCommit(HeadStartCommit),
    PublishStats(HeadPublishStats),
    Stop,
}

#[derive(Debug)]
pub enum Tail {
    Begin(TailBegin),
    Acknowledge(TailAcknowledge),
    WriteAcks(TailWriteAcks),
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

    /// Broadcast `L:Load` chunks of the Frontier.
    Load {
        frontier: shuffle::Frontier,
    },
    /// Broadcast `L:Flush`.
    Flush {
        // Prior transaction's C:Acknowledged patches.
        connector_patches: bytes::Bytes,
    },
    /// Broadcast `L:StartCommit` with this txn's C:Flushed patches.
    StartCommit {
        connector_patches: bytes::Bytes,
    },
    /// Publish a stats document as CONTINUE_TXN to the ops stats journal.
    /// Actor sets `stats_flushed = false` upon dispatch.
    PublishStats {
        stats: ops::proto::Stats,
    },
    /// Persist a streamed sequence of `proto::Persist` messages to shard zero.
    /// The Actor sends `stack` as-is in order. The final entry carries the
    /// nonce that indicates end-of-sequence.
    Persist {
        /// Pre-built `proto::Persist` messages, terminator included.
        stack: Vec<proto::Persist>,
        /// At dispatch, snapshot `publisher.commit_intents()` into
        /// `Actor::pending_ack_intents`, and emit them as a `proto::Persist`
        /// having both `ack_intents` and `delete_ack_intents` which precedes
        /// `stack`.
        snapshot_ack_intents: bool,
    },
    /// Write staged ACK intents (held by the Actor) to their journals.
    /// Actor sets `ack_intents_flushed = false` upon dispatch.
    WriteAckIntents,
    /// Broadcast `L:Acknowledge` with this txn's aggregated StartedCommit patches.
    Acknowledge {
        // This committed transaction's C:StartedCommit patches.
        connector_patches: bytes::Bytes,
    },
    /// Start calling the trigger.
    /// Actor sets `trigger_done = false` upon dispatch.
    CallTrigger {
        trigger_variables: bytes::Bytes,
    },

    Rotate {
        pending: PendingDeltas,
    },
}

impl Head {
    /// Dispatch to the current sub-state's `step()`.
    pub fn step(
        self,
        now: uuid::Clock,
        ready_frontier: &mut Option<shuffle::Frontier>,
        shard_rx: &mut Option<(usize, proto::Materialize)>,
        stats_flushed: bool,
        stopping: bool,
        tail: &mut Tail,
        task: &state::Task,
    ) -> (Action, Head) {
        match self {
            Head::Idle(s) => s.step(now, ready_frontier, stopping, tail, task),
            Head::Extend(s) => s.step(now, ready_frontier, shard_rx, stopping, tail, task),
            Head::Flush(s) => s.step(now, shard_rx, task),
            Head::Persist(s) => s.step(shard_rx),
            Head::StartCommit(s) => s.step(now, shard_rx, task),
            Head::PublishStats(s) => s.step(now, stats_flushed, stopping),
            Head::Stop => panic!("HeadFSM::Stop observed at step boundary"),
        }
    }
}

impl Tail {
    /// Dispatch to the current sub-state's `step()`.
    pub fn step(
        self,
        ack_intents_flushed: bool,
        now: uuid::Clock,
        shard_rx: &mut Option<(usize, proto::Materialize)>,
        task: &state::Task,
        trigger_done: bool,
    ) -> (Action, Tail) {
        match self {
            Tail::Begin(s) => s.step(task),
            Tail::WriteAcks(s) => s.step(ack_intents_flushed),
            Tail::Acknowledge(s) => s.step(now, shard_rx),
            Tail::Trigger(s) => s.step(now, trigger_done),
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
    /// Commit Clock of the last transaction, which may be recovered from a
    /// prior session, or zero.
    pub last_commit: uuid::Clock,
}

impl HeadIdle {
    pub fn step(
        self,
        now: uuid::Clock,
        ready_frontier: &mut Option<shuffle::Frontier>,
        stopping: bool,
        tail: &Tail,
        task: &state::Task,
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
            last_commit,
        } = self;

        let unresolved_hints = frontier.unresolved_hints != 0;
        let action = Action::Load {
            frontier: frontier.clone(),
        };
        let extents = Extents {
            opened: now,
            frontier,
            ..Default::default()
        };
        let state = HeadExtend {
            extents,
            combiner_usage_bytes: vec![0; task.n_shards],
            idempotent_replay,
            last_commit,
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
    /// Commit Clock of the prior transaction (which may be from a prior session), or zero.
    pub last_commit: uuid::Clock,
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
        task: &state::Task,
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

            reduce_docs_and_bytes(&mut self.extents.binding_loaded, binding_loaded);
            reduce_docs_and_bytes(&mut self.extents.binding_read, binding_read);
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
        let open_age = clock_delta(now, self.extents.opened);
        let last_age = clock_delta(now, self.last_commit);
        let max_combiner = self.combiner_usage_bytes.iter().max().unwrap();
        let (read_docs, read_bytes) = self
            .extents
            .binding_read
            .values()
            .map(|dnb| (dnb.docs_total, dnb.bytes_total))
            .fold((0, 0), |(a1, a2), (b1, b2)| (a1 + b1, a2 + b2));

        // Does our task policy wish us to extend the transaction?
        let policy_extend = open_age < task.open_duration.end
            && last_age < task.last_commit_age.end
            && *max_combiner < task.combiner_usage_bytes.end
            && read_bytes < task.read_bytes.end
            && read_docs < task.read_docs.end;

        // Does our task policy wish us to close the transaction?
        // Usage-based measures saturate if !policy_extend (if they didn't,
        // we'd live-lock because the threshold cannot be reached).
        let policy_close = open_age > task.open_duration.start
            && last_age > task.last_commit_age.start
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

            extents.closed = now;

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
            task.last_commit_age.start.checked_sub(last_age),
            task.last_commit_age.end.checked_sub(last_age),
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
        task: &state::Task,
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
                connector_patches_json,
                binding_loaded,
            } = std::mem::take(flushed);

            extend_patches(&mut self.pending, &connector_patches_json);
            reduce_docs_and_bytes(&mut self.extents.binding_loaded, binding_loaded);

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

        let commit_action = Action::StartCommit {
            connector_patches: take_patches(&mut pending.shard_patches),
        };

        if !task.skip_replay_determinism {
            // Persist extents for idempotent transaction replay.
            let nonce = now.as_u64();
            let mut stack: Vec<proto::Persist> = Vec::new();

            stack.push(proto::Persist {
                delete_hinted_frontier: true,
                ..Default::default()
            });
            push_frontier_chunks(&mut stack, extents.frontier.clone(), true);

            stack.push(proto::Persist {
                nonce, // End-of-sequence.
                connector_patches_json: take_patches(&mut pending.persist_patches),
                max_keys: std::mem::take(&mut pending.max_keys),
                ..Default::default()
            });

            // Chain StartCommit after the Persisted response.
            let persist_action = Action::Persist {
                stack,
                snapshot_ack_intents: false,
            };
            let commit_state = HeadStartCommit {
                extents,
                pending,
                shard_started_commit: vec![false; task.n_shards],
            };
            let persist_state = HeadPersist {
                nonce,
                next_action: commit_action,
                next_state: Box::new(Head::StartCommit(commit_state)),
            };

            (persist_action, Head::Persist(persist_state))
        } else {
            // Skip directly to StartCommit.
            let state = HeadStartCommit {
                extents,
                pending,
                shard_started_commit: vec![false; task.n_shards],
            };

            (commit_action, Head::StartCommit(state))
        }
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
        now: uuid::Clock,
        shard_rx: &mut Option<(usize, proto::Materialize)>,
        task: &state::Task,
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
                binding_stored,
                first_source_clock,
                last_source_clock,
            } = std::mem::take(started_commit);

            extend_patches(&mut self.pending, &connector_patches_json);
            reduce_docs_and_bytes(&mut self.extents.binding_stored, binding_stored);

            // Reduce per-shard, per-binding source-clock extremes into
            // the txn-wide per-binding maps. Bindings that received no
            // documents on a shard are simply absent from that shard's map.
            for (binding, clock) in first_source_clock {
                self.extents
                    .first_source_clock
                    .entry(binding)
                    .and_modify(|prev| *prev = (*prev).min(clock))
                    .or_insert(clock);
            }
            for (binding, clock) in last_source_clock {
                self.extents
                    .last_source_clock
                    .entry(binding)
                    .and_modify(|prev| *prev = (*prev).max(clock))
                    .or_insert(clock);
            }

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

        // Compose the trigger payload now that we have a complete txn-wide
        // view: collection_names from binding_read, per-binding source-clock
        // extremes reduced to a task-wide min/max for the variables (the
        // per-binding maps remain on `extents` for finer-grain reporting in
        // the future), image + materialization name from Task. Skip when the
        // spec has no triggers, or when no shard read documents (no event
        // to fire about).
        if let Some(_compiled) = task.compiled_triggers.as_ref()
            && !extents.binding_read.is_empty()
        {
            let collection_names: Vec<String> = extents
                .binding_read
                .keys()
                .filter_map(|idx| task.collection_names.get(*idx as usize).cloned())
                .collect();

            let first_source_clock_min = extents
                .first_source_clock
                .values()
                .copied()
                .min()
                .map(uuid::Clock::from_u64);
            let last_source_clock_max = extents
                .last_source_clock
                .values()
                .copied()
                .max()
                .map(uuid::Clock::from_u64);

            let started_at = clock_to_system_time(extents.opened);
            let trigger_params = triggers::trigger_variables(&triggers::TriggerInputs {
                collection_names: &collection_names,
                materialization_name: &task.shard_ref.name,
                connector_image: &task.connector_image,
                started_at,
                first_source_clock_min,
                last_source_clock_max,
            });
            pending.trigger_params =
                bytes::Bytes::from(serde_json::to_vec(&trigger_params).expect("infallible"));
        }

        let action = Action::PublishStats {
            stats: build_stats_doc(task, &extents, now),
        };
        let state = HeadPublishStats {
            extents,
            pending,
            commit: now,
        };

        (action, Head::PublishStats(state))
    }
}

/// HeadPublishStats awaits the completion of a stats document append and flush.
#[derive(Debug)]
pub struct HeadPublishStats {
    pub extents: Extents,
    pub pending: PendingDeltas,

    // Time of transaction commit.
    pub commit: uuid::Clock,
}

impl HeadPublishStats {
    pub fn step(self, now: uuid::Clock, stats_flushed: bool, stopping: bool) -> (Action, Head) {
        if !stats_flushed {
            return (Action::Idle, Head::PublishStats(self));
        }
        // We've finished publishing stats, and will next commit.

        let Self {
            extents,
            mut pending,
            commit,
        } = self;

        let Extents { frontier, .. } = extents;

        let nonce = now.as_u64();
        let mut stack: Vec<proto::Persist> = Vec::new();

        push_frontier_chunks(&mut stack, frontier, false);

        stack.push(proto::Persist {
            nonce, // End-of-sequence.
            connector_patches_json: take_patches(&mut pending.persist_patches),
            max_keys: std::mem::take(&mut pending.max_keys),
            trigger_params_json: pending.trigger_params.clone(),
            ..Default::default()
        });

        // If we're `stopping`, then transition to Stop after Persist.
        // Otherwise, rotate to begin a next transaction.
        let (next_action, next_state) = if stopping {
            (Action::Idle, Head::Stop)
        } else {
            (
                Action::Rotate { pending },
                Head::Idle(HeadIdle {
                    idempotent_replay: false,
                    last_commit: commit,
                }),
            )
        };

        let state = HeadPersist {
            nonce,
            next_action,
            next_state: Box::new(next_state),
        };
        let action = Action::Persist {
            stack,
            snapshot_ack_intents: true,
        };

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
    pub fn step(self, task: &state::Task) -> (Action, Tail) {
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
                    max_keys,
                    mut persist_patches,
                    mut shard_patches,
                    trigger_params: trigger_variables,
                },
            shard_acknowledged: _,
        } = self;

        assert!(max_keys.is_empty());
        let persist_patches = take_patches(&mut persist_patches);
        let shard_patches = take_patches(&mut shard_patches);

        // Base: call the trigger if needed, else go straight to Done.
        let (mut action, mut state) = if trigger_variables.is_empty() {
            (Action::Idle, Tail::Done(TailDone { shard_patches }))
        } else {
            (
                Action::CallTrigger { trigger_variables },
                Tail::Trigger(TailTrigger { shard_patches }),
            )
        };

        // Wrap with WriteAcks, so journal ACKs are appended immediately after
        // a post-Acknowledge Persist completes (if one is required).
        state = Tail::WriteAcks(TailWriteAcks {
            next_action: action,
            next_state: Box::new(state),
        });
        action = Action::WriteAckIntents {};

        // If Acknowledged returned patches, wrap with a Persist that runs first.
        if !persist_patches.is_empty() {
            let nonce = now.as_u64();

            state = Tail::Persist(TailPersist {
                nonce,
                next_action: action,
                next_state: Box::new(state),
            });
            action = Action::Persist {
                stack: vec![proto::Persist {
                    nonce, // End-of-sequence.
                    connector_patches_json: persist_patches,
                    ..Default::default()
                }],
                snapshot_ack_intents: false,
            };
        }

        (action, state)
    }
}

/// TailWriteAcks awaits the completion of ACK append and flush.
#[derive(Debug)]
pub struct TailWriteAcks {
    pub next_action: Action,
    pub next_state: Box<Tail>,
}

impl TailWriteAcks {
    pub fn step(self, ack_intents_flushed: bool) -> (Action, Tail) {
        if !ack_intents_flushed {
            return (Action::Idle, Tail::WriteAcks(self));
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
    pub fn step(self, now: uuid::Clock, trigger_done: bool) -> (Action, Tail) {
        if !trigger_done {
            return (Action::Idle, Tail::Trigger(self));
        }

        let Self { shard_patches } = self;

        let nonce = now.as_u64();
        let action = Action::Persist {
            stack: vec![proto::Persist {
                nonce, // End-of-sequence.
                delete_trigger_params: true,
                ..Default::default()
            }],
            snapshot_ack_intents: false,
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

// Saturating difference `a - b` expressed as `Duration`.
fn clock_delta(a: uuid::Clock, b: uuid::Clock) -> std::time::Duration {
    let (a_s, a_n) = a.to_unix();
    let (b_s, b_n) = b.to_unix();
    let a = std::time::Duration::new(a_s, a_n);
    let b = std::time::Duration::new(b_s, b_n);
    a.saturating_sub(b)
}

// Convert a `uuid::Clock` into a protobuf Timestamp.
fn clock_to_timestamp(clock: uuid::Clock) -> proto_flow::Timestamp {
    let system_time = clock_to_system_time(clock);
    proto_flow::as_timestamp(system_time)
}

// Convert a `uuid::Clock` into a `std::time::SystemTime`.
fn clock_to_system_time(clock: uuid::Clock) -> std::time::SystemTime {
    let (secs, nanos) = clock.to_unix();
    std::time::UNIX_EPOCH + std::time::Duration::new(secs, nanos)
}

// Reduce per-binding DocsAndBytes by summing into `tgt`.
fn reduce_docs_and_bytes(
    tgt: &mut BTreeMap<u32, ops::proto::stats::DocsAndBytes>,
    src: BTreeMap<u32, ops::proto::stats::DocsAndBytes>,
) {
    for (index, src_dnb) in src {
        let entry = tgt.entry(index).or_default();
        entry.docs_total += src_dnb.docs_total;
        entry.bytes_total += src_dnb.bytes_total;
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
    for tgt in [&mut pending.shard_patches, &mut pending.persist_patches] {
        if tgt.is_empty() {
            tgt.extend_from_slice(src);
            continue;
        } else if src.is_empty() {
            continue;
        }

        tgt.truncate(tgt.len() - 1); // Remove trailing ']'.
        let src = &src[1..]; // Remove leading '['.

        tgt.push(b','); // Add separator.
        tgt.extend_from_slice(src);
    }
}

// Take patches from `src`, leaving it empty, and freeze into Bytes.
pub fn take_patches(src: &mut Vec<u8>) -> bytes::Bytes {
    bytes::Bytes::from(std::mem::take(src))
}

/// Build an `ops::Stats` document snapshotting this transaction's
/// per-binding DocsAndBytes plus wall-clock bookkeeping.
fn build_stats_doc(
    task: &state::Task,
    extents: &Extents,
    _commit: uuid::Clock,
) -> ops::proto::Stats {
    let mut materialize: BTreeMap<String, ops::proto::stats::MaterializeBinding> = BTreeMap::new();

    let fold_into = |map: &mut BTreeMap<String, ops::proto::stats::MaterializeBinding>,
                     task: &state::Task,
                     src: &BTreeMap<u32, ops::proto::stats::DocsAndBytes>,
                     slot: fn(
        &mut ops::proto::stats::MaterializeBinding,
    ) -> &mut Option<ops::proto::stats::DocsAndBytes>| {
        for (binding_index, dnb) in src {
            let Some(name) = task.collection_names.get(*binding_index as usize) else {
                continue;
            };

            let entry = map.entry(name.clone()).or_default();
            let slot = slot(entry).get_or_insert_with(Default::default);

            slot.docs_total += dnb.docs_total;
            slot.bytes_total += dnb.bytes_total;
        }
    };

    fold_into(&mut materialize, task, &extents.binding_read, |b| {
        &mut b.right
    });
    fold_into(&mut materialize, task, &extents.binding_loaded, |b| {
        &mut b.left
    });
    fold_into(&mut materialize, task, &extents.binding_stored, |b| {
        &mut b.out
    });

    let open_seconds_total = clock_delta(extents.closed, extents.opened).as_secs_f64();

    ops::proto::Stats {
        meta: Some(ops::proto::Meta {
            uuid: String::new(), // Stamped by Publisher::enqueue()
        }),
        shard: Some(task.shard_ref.clone()),
        timestamp: Some(clock_to_timestamp(extents.opened)),
        open_seconds_total,
        txn_count: 1,
        materialize,
        capture: Default::default(), // N/A.
        derive: None,                // N/A.
        interval: None,              // N/A.
    }
}

/// Chunk a `Frontier` into `proto::Persist` messages and append them to
/// `stack`. Each chunk goes into either the `hinted_frontier` or
/// `committed_frontier` slot per `hinted`.
fn push_frontier_chunks(
    stack: &mut Vec<proto::Persist>,
    frontier: shuffle::Frontier,
    hinted: bool,
) {
    let mut drain = shuffle::frontier::Drain::new();
    drain.start(frontier);

    while let Some(chunk) = drain.next_chunk() {
        let mut p = proto::Persist::default();
        if hinted {
            p.hinted_frontier = Some(chunk);
        } else {
            p.committed_frontier = Some(chunk);
        }
        stack.push(p);
    }
}
