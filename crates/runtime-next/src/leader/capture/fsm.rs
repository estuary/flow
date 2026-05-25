//! HeadFSM and TailFSM: capture transaction policy.
//!
//! Captures don't have a leader RPC. The shard actor directly presents
//! connector responses and local IO completions to these FSMs, and maps emitted
//! `Action`s into combiner, publisher, persistence, and connector operations.
//!
//! HeadFSM drives the currently-accumulating combiner:
//!   Stop <- Idle <-> Extend
//!
//! TailFSM drives an accumulated transaction towards commit after rotation:
//!   Begin -> Drain -> WriteStats -> Persist -> Recover -> (Acknowledge) -> WriteIntents -> Done
//!
//! Head and Tail are pipelined: while Tail commits transaction K, Head
//! accumulates K+1 into the other combiner. A transaction therefore batches
//! whatever connector output arrives during the prior transaction's Tail. Head
//! can only Rotate (close K+1) once Tail is Done, so at most one transaction is
//! ever in the Tail.
//!
//! The single Persist commits the transaction: it durably records the ACK
//! intents (so a crash before journal-append is recoverable) and the connector
//! state. Documents become visible once WriteIntents appends those ACKs to
//! their journals.
//!
//! Recover is the post-commit handoff: it acknowledges committed checkpoints
//! and hands the ACK intents to WriteIntents. The Tail passes through it after
//! every Persist, and also starts there — seeded with intents recovered from
//! RocksDB — so a prior session that committed but crashed before its
//! WriteIntents completes that interrupted commit before new work begins.
//!
//! Acknowledge is visited only for connectors that requested explicit
//! acknowledgements; others step from Recover straight to WriteIntents.
//!
//! Captured and SourcedSchema messages begin or continue a connector checkpoint
//! sequence, while Checkpoint completes it. Head closes only at sequence
//! boundaries unless it must inject a synthetic checkpoint to enforce the hard
//! transaction byte bound.

use super::Task;
use crate::leader::close_policy;
use crate::proto;
use proto_flow::capture;
use proto_gazette::uuid;
use std::collections::BTreeMap;
use std::time::Duration;

/// Per-transaction aggregated state threaded through Head/Tail FSMs.
#[derive(Debug, Default, Clone)]
pub struct Extents {
    // Sparse per-binding map of bindings have changed extents in this transaction.
    bindings: BTreeMap<u32, BindingExtents>,
    // Total number of captured connector document bytes of this transaction.
    captured_bytes: u64,
    // Total number of captured connector documents of this transaction.
    captured_docs: u64,
    // Number of connector checkpoints included in this transaction.
    checkpoints: u32,
    // Clock at which the transaction began to close (set on Rotate).
    close: uuid::Clock,
    // Clock at which the transaction opened (its first connector message).
    open: uuid::Clock,
    // Per-binding shapes folded from transaction SourcedSchema messages.
    // A binding may receive several SourcedSchema; they union.
    sourced_schemas: BTreeMap<u32, doc::Shape>,
    // Was a synthetic checkpoint injected due to hard-bound violation?
    synthetic_checkpoint: bool,
}

#[derive(Debug, Default, Clone)]
pub struct BindingExtents {
    // Measures of documents captured by the connector.
    captured: ops::proto::stats::DocsAndBytes,
    // Measures of documents drained from the combiner.
    drained: ops::proto::stats::DocsAndBytes,
}

/// Received but unprocessed connector output.
#[derive(Debug, Default)]
pub enum ConnectorRx {
    /// We're awaiting further connector output.
    #[default]
    Pending,
    /// Connector emitted a Captured document.
    Captured(capture::response::Captured),
    /// Connector emitted a Checkpoint.
    Checkpoint(capture::response::Checkpoint),
    /// Connector emitted a SourcedSchema.
    SourcedSchema { binding: u32, shape: doc::Shape },
    /// Connector closed its output stream.
    Eof,
}

impl ConnectorRx {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Pending => "Pending",
            Self::Captured(_) => "Captured",
            Self::Checkpoint(_) => "Checkpoint",
            Self::SourcedSchema { .. } => "SourcedSchema",
            Self::Eof => "Eof",
        }
    }
}

#[derive(Debug)]
pub enum Head {
    Idle(HeadIdle),
    Extend(HeadExtend),
    Stop,
}

#[derive(Debug)]
pub enum Tail {
    Begin(TailBegin),
    Drain(TailDrain),
    WriteStats(TailWriteStats),
    Persist(TailPersist),
    Recover(TailRecover),
    Acknowledge(TailAcknowledge),
    WriteIntents(TailWriteIntents),
    Done(TailDone),
}

/// `Action` is the next outgoing IO, or an actor-loop control edge.
#[derive(Debug)]
pub enum Action {
    /// Park until new IO arrives. Sugar for waking after the tick interval.
    Idle,
    /// Immediately re-poll without blocking. Sugar for waking immediately.
    PollAgain,
    /// Sleep for the indicated duration before re-polling.
    Sleep { wake_after: Duration },
    /// Accrue a captured document into the accumulating combiner.
    Captured {
        captured: capture::response::Captured,
    },
    /// Accrue connector state into the accumulating combiner.
    Checkpoint {
        checkpoint: capture::response::Checkpoint,
    },
    /// Drain the just-rotated combiner.
    Drain {
        // Per-binding shapes folded from transaction SourcedSchema messages.
        sourced_schemas: BTreeMap<u32, doc::Shape>,
    },
    /// Publish a stats document as CONTINUE_TXN to the ops stats journal.
    WriteStats { stats: ops::proto::Stats },
    /// Persist one `proto::Persist` WriteBatch to RocksDB.
    /// This is the transaction's single, committing Persist.
    Persist { persist: proto::Persist },
    /// Inform the connector that committed checkpoint sequences are complete.
    Acknowledge { checkpoints: u32 },
    /// Write ACK intents to their journals, making transaction documents visible.
    WriteIntents {
        ack_intents: BTreeMap<String, bytes::Bytes>,
    },
    /// Rotate accumulating and draining combiners.
    Rotate { extents: Extents },
    /// Emit an error.
    Error(anyhow::Error),
}

impl Action {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Idle => "Idle",
            Self::PollAgain => "PollAgain",
            Self::Sleep { .. } => "Sleep",
            Self::Captured { .. } => "Captured",
            Self::Checkpoint { .. } => "Checkpoint",
            Self::Drain { .. } => "Drain",
            Self::WriteStats { .. } => "WriteStats",
            Self::Persist { .. } => "Persist",
            Self::Acknowledge { .. } => "Acknowledge",
            Self::WriteIntents { .. } => "WriteIntents",
            Self::Rotate { .. } => "Rotate",
            Self::Error(_) => "Error",
        }
    }
}

impl Head {
    pub fn step(
        self,
        now: uuid::Clock,
        close_requested: &mut bool,
        combiner_bytes: u64,
        ready: &mut ConnectorRx,
        stopping: bool,
        tail: &Tail,
        task: &Task,
    ) -> (Action, Head) {
        match self {
            Self::Idle(s) => s.step(
                now,
                close_requested,
                combiner_bytes,
                ready,
                stopping,
                tail,
                task,
            ),
            Self::Extend(s) => s.step(ready, task),
            Self::Stop => panic!("Capture HeadFSM::Stop observed at step boundary"),
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Idle(_) => "Idle",
            Self::Extend(_) => "Extend",
            Self::Stop => "Stop",
        }
    }
}

impl Tail {
    pub fn step(
        self,
        acknowledge_done: bool,
        drain_finished: &mut Option<DrainedCapture>,
        intents_write_idle: bool,
        now: uuid::Clock,
        persist_done: bool,
        task: &Task,
        stats_write_idle: Option<&mut BTreeMap<String, bytes::Bytes>>,
    ) -> (Action, Tail) {
        match self {
            Self::Begin(s) => s.step(),
            Self::Drain(s) => s.step(drain_finished, task),
            Self::WriteStats(s) => s.step(now, stats_write_idle),
            Self::Persist(s) => s.step(persist_done),
            Self::Recover(s) => s.step(task),
            Self::Acknowledge(s) => s.step(acknowledge_done),
            Self::WriteIntents(s) => s.step(intents_write_idle),
            Self::Done(_) => (Action::Idle, self),
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Begin(_) => "Begin",
            Self::Drain(_) => "Drain",
            Self::WriteStats(_) => "WriteStats",
            Self::Persist(_) => "Persist",
            Self::Recover(_) => "Recover",
            Self::Acknowledge(_) => "Acknowledge",
            Self::WriteIntents(_) => "WriteIntents",
            Self::Done(_) => "Done",
        }
    }
}

/// HeadIdle evaluates the close policy between connector checkpoint sequences.
#[derive(Debug, Default)]
pub struct HeadIdle {
    /// Accumulated extents of this transaction.
    pub extents: Extents,
    /// Clock of the last transaction close.
    pub last_close: uuid::Clock,
}

impl HeadIdle {
    fn step(
        mut self,
        now: uuid::Clock,
        close_requested: &mut bool,
        combiner_bytes: u64,
        ready: &ConnectorRx,
        stopping: bool,
        tail: &Tail,
        task: &Task,
    ) -> (Action, Head) {
        let is_open = self.extents.checkpoints != 0;
        let tail_done = matches!(tail, Tail::Done(_));

        // Termination condition: stay unstarted if `stopping`; let Tail finish.
        if stopping && !is_open {
            if tail_done {
                return (Action::PollAgain, Head::Stop);
            } else {
                return (Action::Idle, Head::Idle(self));
            }
        }
        // Restart condition: hold until `task.restart`, and then Stop this session.
        if matches!(ready, ConnectorRx::Eof) && !is_open {
            return match uuid::Clock::delta(task.restart, now) {
                Duration::ZERO => (Action::PollAgain, Head::Stop),
                wake_after => (Action::Sleep { wake_after }, Head::Idle(self)),
            };
        }
        // Clear stale close_requested from after prior transaction close.
        if !is_open {
            *close_requested = false;
        }

        let open_age = if is_open {
            uuid::Clock::delta(now, self.extents.open)
        } else {
            Duration::ZERO
        };

        let close_policy::Decision {
            may_close,
            may_extend,
            wake_after,
        } = task.close_policy.evaluate(close_policy::Inputs {
            close_requested: *close_requested,
            idempotent_replay: false, // N/A.
            last_age: uuid::Clock::delta(now, self.last_close),
            combiner_bytes,
            open_age,
            read_bytes: self.extents.captured_bytes,
            read_docs: self.extents.captured_docs,
            stopping,
            tail_done,
            unresolved_hints: false, // N/A.
        });

        // Should we extend with a ready next connector checkpoint sequence?
        if self.extents.synthetic_checkpoint {
            // Don't extend transactions after a synthetic checkpoint.
        } else if may_extend
            && matches!(
                ready,
                ConnectorRx::Captured(_)
                    | ConnectorRx::Checkpoint(_)
                    | ConnectorRx::SourcedSchema { .. }
            )
        {
            if !is_open {
                self.extents.open = now;
            }
            return (
                Action::PollAgain,
                Head::Extend(HeadExtend {
                    inner: self,
                    sequence_bytes: 0,
                }),
            );
        }

        // Should we begin to close the transaction?
        if !is_open {
            return (Action::Idle, Head::Idle(self));
        } else if may_close {
            let Self { mut extents, .. } = self;
            extents.close = now;

            return (
                Action::Rotate { extents },
                Head::Idle(HeadIdle {
                    extents: Default::default(),
                    last_close: now,
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

/// HeadExtend waits for a connector sequence to complete, then returns to
/// HeadIdle for close-policy evaluation.
#[derive(Debug, Default)]
pub struct HeadExtend {
    /// HeadIdle state to return to once the checkpoint sequence completes.
    pub inner: HeadIdle,
    /// Captured document bytes of *this* checkpoint sequence (not the txn),
    /// used to enforce an upper bound before synthetic checkpoint.
    pub sequence_bytes: u64,
}

impl HeadExtend {
    pub fn step(mut self, ready: &mut ConnectorRx, task: &Task) -> (Action, Head) {
        if self.sequence_bytes > task.sequence_bytes_limit {
            let Self {
                mut inner,
                sequence_bytes,
            } = self;

            service_kit::event!(
                tracing::Level::WARN,
                "head",
                sequence_bytes,
                sequence_bytes_limit = task.sequence_bytes_limit,
                "connector checkpoint sequence is too large; injecting a \
                synthetic checkpoint. This transaction commits without advancing \
                connector state and so degrades from exactly-once to at-least-once \
                delivery — a connector should checkpoint more frequently to avoid this",
            );

            // HeadIdle refuses to extend a transaction that already injected a
            // synthetic checkpoint, so we reach this at most once per txn.
            assert!(
                !inner.extents.synthetic_checkpoint,
                "a second synthetic checkpoint must never be injected into one transaction",
            );
            inner.extents.checkpoints += 1;
            inner.extents.synthetic_checkpoint = true;

            return (Action::Idle, Head::Idle(inner));
        }

        match std::mem::take(ready) {
            ConnectorRx::Pending => (Action::Idle, Head::Extend(self)),
            ConnectorRx::Captured(captured) => {
                let extents = &mut self.inner.extents;

                let extent = extents.bindings.entry(captured.binding).or_default();
                extent.captured.docs_total += 1;
                extent.captured.bytes_total += captured.doc_json.len() as u64;

                self.sequence_bytes += captured.doc_json.len() as u64;
                extents.captured_bytes += captured.doc_json.len() as u64;
                extents.captured_docs += 1;

                (Action::Captured { captured }, Head::Extend(self))
            }
            ConnectorRx::SourcedSchema { binding, shape } => {
                let extents = &mut self.inner.extents;

                let entry = extents
                    .sourced_schemas
                    .entry(binding)
                    .or_insert(doc::Shape::nothing());
                *entry = doc::Shape::union(std::mem::replace(entry, doc::Shape::nothing()), shape);

                (Action::Idle, Head::Extend(self))
            }
            ConnectorRx::Checkpoint(checkpoint) => {
                let Self {
                    mut inner,
                    sequence_bytes: _,
                } = self;

                inner.extents.checkpoints += 1;

                (Action::Checkpoint { checkpoint }, Head::Idle(inner))
            }
            ConnectorRx::Eof => (
                Action::Error(anyhow::anyhow!(
                    "unexpected connector EOF within a capture checkpoint sequence"
                )),
                Head::Stop,
            ),
        }
    }
}

/// Output of the actor's combiner drain, staged for Tail continuation.
#[derive(Debug, Default)]
pub struct DrainedCapture {
    pub connector_patches: bytes::Bytes,
    pub bindings: BTreeMap<u32, ops::proto::stats::DocsAndBytes>,
}

#[derive(Debug)]
pub struct TailBegin {
    pub extents: Extents,
}

impl TailBegin {
    pub fn step(self) -> (Action, Tail) {
        let Self { mut extents } = self;
        // Sourced shapes belong to the drain, not to stats; lift them out of
        // Extents into the Drain action and let the rest of Extents flow on.
        let sourced_schemas = std::mem::take(&mut extents.sourced_schemas);
        (
            Action::Drain { sourced_schemas },
            Tail::Drain(TailDrain { extents }),
        )
    }
}

#[derive(Debug)]
pub struct TailDrain {
    pub extents: Extents,
}

impl TailDrain {
    fn step(self, drain_finished: &mut Option<DrainedCapture>, task: &Task) -> (Action, Tail) {
        let Some(drained) = drain_finished.take() else {
            return (Action::Idle, Tail::Drain(self));
        };

        let Self { mut extents } = self;
        let DrainedCapture {
            connector_patches,
            bindings,
        } = drained;

        // Fold per-binding drained measures into the transaction extents.
        for (binding, drained) in bindings {
            let extent = extents.bindings.entry(binding).or_default();
            extent.drained.docs_total += drained.docs_total;
            extent.drained.bytes_total += drained.bytes_total;
        }
        let stats = build_stats_doc(task, &extents);

        (
            Action::WriteStats { stats },
            Tail::WriteStats(TailWriteStats {
                connector_patches,
                extents,
            }),
        )
    }
}

#[derive(Debug)]
pub struct TailWriteStats {
    pub connector_patches: bytes::Bytes,
    pub extents: Extents,
}

impl TailWriteStats {
    pub fn step(
        self,
        now: uuid::Clock,
        stats_write_idle: Option<&mut BTreeMap<String, bytes::Bytes>>,
    ) -> (Action, Tail) {
        // The stats write yields this transaction's ACK intents; hold until it
        // completes and they're available.
        let Some(ack_intents) = stats_write_idle else {
            return (Action::Idle, Tail::WriteStats(self));
        };

        let Self {
            connector_patches,
            extents,
        } = self;
        let ack_intents = std::mem::take(ack_intents);
        let seq_no = now.as_u64();

        // The transaction's single, committing Persist. It records the ACK
        // intents (so a crash before WriteIntents is recoverable) and the
        // connector-state patches. `delete_ack_intents` first clears the prior
        // transaction's per-journal intents — a journal written last transaction
        // but not this one would otherwise leave a stale entry.
        let persist = proto::Persist {
            seq_no,
            ack_intents: ack_intents.clone(),
            connector_patches_json: connector_patches,
            delete_ack_intents: true,
            ..Default::default()
        };

        // Acknowledge all transaction Checkpoints sent by the connector.
        let checkpoints = extents
            .checkpoints
            .saturating_sub(u32::from(extents.synthetic_checkpoint));

        // Persist -> Recover (a no-op hop) -> Acknowledge.
        let recover_state = TailRecover {
            checkpoints,
            ack_intents,
        };
        let persist_state = TailPersist {
            next_action: Action::PollAgain,
            next_state: Box::new(Tail::Recover(recover_state)),
        };

        (Action::Persist { persist }, Tail::Persist(persist_state))
    }
}

/// TailPersist awaits the in-flight Persist to complete, then chains the
/// contained action and state. A generic "persist, then resume" trampoline,
/// mirroring the materialize TailFSM.
#[derive(Debug)]
pub struct TailPersist {
    pub next_action: Action,
    pub next_state: Box<Tail>,
}

impl TailPersist {
    pub fn step(self, persist_done: bool) -> (Action, Tail) {
        if !persist_done {
            return (Action::Idle, Tail::Persist(self));
        }
        let Self {
            next_action,
            next_state,
        } = self;

        (next_action, *next_state)
    }
}

/// Recover is the post-commit handoff. It follows Persist each transaction,
/// and is also the Tail's initial state after recovery.
#[derive(Debug)]
pub struct TailRecover {
    /// Connector checkpoints to acknowledge — zero at session start.
    pub checkpoints: u32,
    /// ACK intents to publish: recovered from RocksDB at session start, or
    /// the just-committed transaction's own intents after a Persist.
    pub ack_intents: BTreeMap<String, bytes::Bytes>,
}

impl TailRecover {
    pub fn step(self, task: &Task) -> (Action, Tail) {
        let Self {
            checkpoints,
            ack_intents,
        } = self;

        // Acknowledge only when the connector requested explicit ACKs, and
        // only when there are actually committed checkpoints to acknowledge.
        if task.explicit_acknowledgements && checkpoints != 0 {
            (
                Action::Acknowledge { checkpoints },
                Tail::Acknowledge(TailAcknowledge { ack_intents }),
            )
        } else {
            (
                Action::WriteIntents { ack_intents },
                Tail::WriteIntents(TailWriteIntents {}),
            )
        }
    }
}

#[derive(Debug)]
pub struct TailAcknowledge {
    pub ack_intents: BTreeMap<String, bytes::Bytes>,
}

impl TailAcknowledge {
    pub fn step(self, acknowledge_done: bool) -> (Action, Tail) {
        if !acknowledge_done {
            return (Action::Idle, Tail::Acknowledge(self));
        }

        let Self { ack_intents } = self;

        // Append ACK intents to journals, making this transaction's documents
        // visible. The commit already durably recorded them, so this also
        // re-runs (harmlessly) on recovery if the process crashed here.
        (
            Action::WriteIntents { ack_intents },
            Tail::WriteIntents(TailWriteIntents {}),
        )
    }
}

#[derive(Debug)]
pub struct TailWriteIntents {}

impl TailWriteIntents {
    pub fn step(self, intents_write_idle: bool) -> (Action, Tail) {
        if !intents_write_idle {
            return (Action::Idle, Tail::WriteIntents(self));
        }
        // ACK intents are appended and committed. Unlike materialize, no
        // follow-up Persist clears them from RocksDB: the next transaction's
        // commit Persist overwrites them, and an idle capture simply re-writes
        // the same idempotent intents on its next recovery.
        (Action::Idle, Tail::Done(TailDone {}))
    }
}

#[derive(Debug, Default)]
pub struct TailDone {}

/// Build an `ops::Stats` document snapshotting this transaction's extents.
fn build_stats_doc(task: &Task, extents: &Extents) -> ops::proto::Stats {
    let mut capture = BTreeMap::<String, ops::proto::stats::CaptureBinding>::new();
    let last_published_at = extents.close.to_pb_json_timestamp();

    for (binding_index, extents) in &extents.bindings {
        let Some(binding) = task.bindings.get(*binding_index as usize) else {
            continue;
        };
        let entry = capture.entry(binding.collection_name.clone()).or_default();
        entry.last_published_at = last_published_at;

        ops::merge_docs_and_bytes(&extents.captured, &mut entry.right);
        ops::merge_docs_and_bytes(&extents.drained, &mut entry.out);
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
        capture,
        derive: None,                    // N/A.
        interval: None,                  // N/A.
        materialize: Default::default(), // N/A.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::leader::capture::task::Binding;
    use bytes::Bytes;

    /// Aggregates the capture Actor's per-iteration locals so `step_head` /
    /// `step_tail` can be driven without recreating the actor's IO scaffolding.
    ///
    /// Each boolean mirrors an actor field that is `true`/idle when no IO future
    /// is in flight — e.g. the actor passes `persist_done = self.persist_fut
    /// .is_none()`. Tests flip the flag to `false` to model an operation that's
    /// been dispatched but hasn't completed, then back to `true` on completion.
    /// `drain_finished` and `pending_ack_intents` carry the actual hand-off
    /// payloads the actor stages between FSM steps.
    struct Ctx {
        acknowledge_done: bool,
        close_requested: bool,
        combiner_bytes: u64,
        drain_finished: Option<DrainedCapture>,
        intents_idle: bool,
        now: uuid::Clock,
        pending_ack_intents: BTreeMap<String, Bytes>,
        persist_done: bool,
        ready: ConnectorRx,
        stats_idle: bool,
        stopping: bool,
        task: Task,
    }

    impl Ctx {
        fn step_head(&mut self, head: Head, tail: &Tail) -> (Action, Head) {
            self.now.tick();
            head.step(
                self.now,
                &mut self.close_requested,
                self.combiner_bytes,
                &mut self.ready,
                self.stopping,
                tail,
                &self.task,
            )
        }

        fn step_tail(&mut self, tail: Tail) -> (Action, Tail) {
            self.now.tick();
            tail.step(
                self.acknowledge_done,
                &mut self.drain_finished,
                self.intents_idle,
                self.now,
                self.persist_done,
                &self.task,
                self.stats_idle.then_some(&mut self.pending_ack_intents),
            )
        }
    }

    /// A Ctx with no IO in flight and a Pending connector, at a fixed wall clock.
    fn mk_ctx(task: Task) -> Ctx {
        Ctx {
            acknowledge_done: true,
            close_requested: false,
            combiner_bytes: 0,
            drain_finished: None,
            intents_idle: true,
            now: uuid::Clock::from_unix(1_700_000_000, 0),
            pending_ack_intents: BTreeMap::new(),
            persist_done: true,
            ready: ConnectorRx::Pending,
            stats_idle: true,
            stopping: false,
            task,
        }
    }

    fn mk_task(explicit_acknowledgements: bool) -> Task {
        Task {
            bindings: vec![
                mk_binding("test/collectionA", "stateA"),
                mk_binding("test/collectionB", "stateB"),
            ],
            // Wide thresholds: `policy_extend` is always true and `policy_close`
            // is always satisfiable, so a close is driven only by
            // `close_requested` / `stopping` or by `ready` going Pending — which
            // keeps the test free of policy-driven close timing.
            close_policy: close_policy::Policy::new(Duration::ZERO, Duration::MAX),
            explicit_acknowledgements,
            max_transactions: 0,
            redact_salt: Bytes::new(),
            // In the past, so an EOF in HeadIdle resolves to an immediate Stop.
            restart: uuid::Clock::zero(),
            sequence_bytes_limit: 1024,
            shard_ref: ops::ShardRef::default(),
        }
    }

    fn mk_binding(collection_name: &str, state_key: &str) -> Binding {
        Binding {
            collection_name: collection_name.to_string(),
            collection_generation_id: models::Id::zero(),
            document_uuid_ptr: json::Pointer::empty(),
            key_extractors: Vec::new(),
            partition_template_name: collection_name.to_string(),
            state_key: state_key.to_string(),
            write_schema_json: Bytes::from_static(b"{}"),
            write_shape: doc::Shape::nothing(),
        }
    }

    fn captured(binding: u32, doc_json: &'static [u8]) -> ConnectorRx {
        ConnectorRx::Captured(capture::response::Captured {
            binding,
            doc_json: Bytes::from_static(doc_json),
        })
    }

    fn checkpoint() -> ConnectorRx {
        ConnectorRx::Checkpoint(capture::response::Checkpoint { state: None })
    }

    /// Walks Head and Tail through a recovery replay, two pipelined
    /// transactions, and a graceful stop. No IO; each step mutates Ctx fields
    /// and reads back the (Action, State) tuple.
    ///
    /// Phase 0: the Tail starts in Recover seeded with ACK intents recovered
    ///          from RocksDB. With no committed checkpoints it skips Acknowledge
    ///          and replays the intents through WriteIntents to Done.
    /// Phase 1: txn 1 opens on a SourcedSchema, accrues two Captured documents
    ///          across two bindings and a Checkpoint, then a second sequence of
    ///          one Captured and a Checkpoint, and closes on `close_requested`.
    /// Phase 2: rotation hands the extents to Tail::Begin. Head opens txn 2
    ///          (pipelined) while Tail's full commit sequence runs interleaved:
    ///          Drain → WriteStats → Persist → Recover → Acknowledge (explicit
    ///          ACKs, two checkpoints) → WriteIntents → Done. While Tail is
    ///          mid-commit, Head is shown unable to rotate txn 2.
    /// Phase 3: `stopping` is set; Head rotates txn 2, Tail commits it, and Head
    ///          then steps to Stop rather than opening a third transaction.
    #[test]
    fn happy_path_recovery_two_transactions_then_stop() {
        let mut ctx = mk_ctx(mk_task(true));
        let mut head = Head::Idle(HeadIdle::default());

        // ===== Phase 0: replay recovered ACK intents at session start =====

        let recovered: BTreeMap<String, Bytes> =
            BTreeMap::from([("ops/recovered".to_string(), Bytes::from_static(b"replay"))]);
        let mut tail = Tail::Recover(TailRecover {
            checkpoints: 0,
            ack_intents: recovered.clone(),
        });

        // Recover with zero committed checkpoints skips Acknowledge (despite
        // explicit_acknowledgements) and writes the recovered intents directly.
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        match action {
            Action::WriteIntents { ack_intents } => assert_eq!(ack_intents, recovered),
            other => panic!("expected WriteIntents, got {other:?}"),
        }
        assert!(matches!(tail, Tail::WriteIntents(_)));

        // The intents write is in flight, then completes → Done.
        ctx.intents_idle = false;
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::Idle));
        assert!(matches!(tail, Tail::WriteIntents(_)));

        ctx.intents_idle = true;
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::Idle));
        assert!(matches!(tail, Tail::Done(_)));

        // ===== Phase 1: txn 1 accumulates two checkpoint sequences =====

        // A SourcedSchema opens the transaction.
        ctx.ready = ConnectorRx::SourcedSchema {
            binding: 0,
            shape: doc::Shape::nothing(),
        };
        let (action, h) = ctx.step_head(head, &tail);
        head = h;
        assert!(matches!(action, Action::PollAgain));
        assert!(matches!(head, Head::Extend(_)));

        // HeadExtend folds the SourcedSchema and rests (no combiner write).
        let (action, h) = ctx.step_head(head, &tail);
        head = h;
        assert!(matches!(action, Action::Idle));
        assert!(matches!(head, Head::Extend(_)));

        // Two Captured documents into distinct bindings.
        ctx.ready = captured(0, b"{\"id\":\"a0\"}");
        let (action, h) = ctx.step_head(head, &tail);
        head = h;
        assert!(matches!(action, Action::Captured { .. }));

        ctx.ready = captured(1, b"{\"id\":\"b0\"}");
        let (action, h) = ctx.step_head(head, &tail);
        head = h;
        assert!(matches!(action, Action::Captured { .. }));

        // First Checkpoint completes the sequence; Head returns to Idle.
        ctx.ready = checkpoint();
        let (action, h) = ctx.step_head(head, &tail);
        head = h;
        assert!(matches!(action, Action::Checkpoint { .. }));
        assert!(matches!(head, Head::Idle(_)));

        // A second sequence: one more Captured into binding 0, then a Checkpoint.
        ctx.ready = captured(0, b"{\"id\":\"a1\"}");
        let (action, h) = ctx.step_head(head, &tail);
        head = h;
        assert!(matches!(action, Action::PollAgain)); // Idle → Extend.
        let (action, h) = ctx.step_head(head, &tail);
        head = h;
        assert!(matches!(action, Action::Captured { .. }));

        ctx.ready = checkpoint();
        let (action, h) = ctx.step_head(head, &tail);
        head = h;
        assert!(matches!(action, Action::Checkpoint { .. }));
        assert!(matches!(head, Head::Idle(_)));

        // Close on request. `ready` is Pending and Tail is Done, so Head rotates.
        // The now-stale close request is cleared when the next idle Head is evaluated.
        ctx.close_requested = true;
        let (action, h) = ctx.step_head(head, &tail);
        head = h;
        let extents = match action {
            Action::Rotate { extents } => extents,
            other => panic!("expected Rotate, got {other:?}"),
        };
        assert!(matches!(head, Head::Idle(_)));
        tail = Tail::Begin(TailBegin { extents });

        let (action, h) = ctx.step_head(head, &tail);
        head = h;
        assert!(matches!(action, Action::Idle));
        assert!(matches!(head, Head::Idle(_)));
        assert!(!ctx.close_requested);

        // ===== Phase 2: commit txn 1 while Head pipelines txn 2 =====

        // Begin lifts the folded SourcedSchema (binding 0) into the Drain action.
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        match action {
            Action::Drain { sourced_schemas } => assert!(sourced_schemas.contains_key(&0)),
            other => panic!("expected Drain, got {other:?}"),
        }
        assert!(matches!(tail, Tail::Drain(_)));

        // Drain holds until the actor stages its output.
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::Idle));
        assert!(matches!(tail, Tail::Drain(_)));

        // Meanwhile Head opens txn 2 (Captured + Checkpoint) — pipelined with the
        // still-committing Tail.
        ctx.ready = captured(0, b"{\"id\":\"a2\"}");
        let (action, h) = ctx.step_head(head, &tail);
        head = h;
        assert!(matches!(action, Action::PollAgain));
        let (action, h) = ctx.step_head(head, &tail);
        head = h;
        assert!(matches!(action, Action::Captured { .. }));
        ctx.ready = checkpoint();
        let (action, h) = ctx.step_head(head, &tail);
        head = h;
        assert!(matches!(action, Action::Checkpoint { .. }));
        assert!(matches!(head, Head::Idle(_)));

        // Head wants to close txn 2, but Tail is mid-commit (not Done): `may_close`
        // is false, so Head sleeps and `close_requested` is NOT cleared.
        ctx.close_requested = true;
        let (action, h) = ctx.step_head(head, &tail);
        head = h;
        assert!(matches!(action, Action::Sleep { .. }));
        assert!(matches!(head, Head::Idle(_)));
        assert!(ctx.close_requested);

        // Resume txn 1's Tail. The staged drain output drives WriteStats.
        ctx.drain_finished = Some(DrainedCapture {
            connector_patches: Bytes::from_static(b"[{\"cursor\":\"lsn-1\"}\n]"),
            bindings: BTreeMap::from([
                (
                    0,
                    ops::proto::stats::DocsAndBytes {
                        docs_total: 2,
                        bytes_total: 50,
                    },
                ),
                (
                    1,
                    ops::proto::stats::DocsAndBytes {
                        docs_total: 1,
                        bytes_total: 25,
                    },
                ),
            ]),
        });
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        let stats = match action {
            Action::WriteStats { stats } => stats,
            other => panic!("expected WriteStats, got {other:?}"),
        };
        assert!(matches!(tail, Tail::WriteStats(_)));
        insta::assert_json_snapshot!(stats, @r#"
        {
          "_meta": {},
          "shard": {},
          "ts": "2023-11-14T22:13:20.000000016+00:00",
          "openSecondsTotal": 0.000000032,
          "txnCount": 1,
          "capture": {
            "test/collectionA": {
              "right": {
                "docsTotal": 2,
                "bytesTotal": 22
              },
              "out": {
                "docsTotal": 2,
                "bytesTotal": 50
              },
              "lastPublishedAt": "2023-11-14T22:13:20.000000048+00:00"
            },
            "test/collectionB": {
              "right": {
                "docsTotal": 1,
                "bytesTotal": 11
              },
              "out": {
                "docsTotal": 1,
                "bytesTotal": 25
              },
              "lastPublishedAt": "2023-11-14T22:13:20.000000048+00:00"
            }
          }
        }
        "#);

        // The stats write is in flight, then completes and yields ACK intents.
        ctx.stats_idle = false;
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::Idle));
        assert!(matches!(tail, Tail::WriteStats(_)));

        ctx.pending_ack_intents = BTreeMap::from([(
            "ops/journal".to_string(),
            Bytes::from_static(b"intent-txn1"),
        )]);
        ctx.stats_idle = true;
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        let persist = match action {
            Action::Persist { persist } => persist,
            other => panic!("expected Persist, got {other:?}"),
        };
        assert!(matches!(tail, Tail::Persist(_)));
        insta::assert_debug_snapshot!(
            (
                &persist.connector_patches_json,
                persist.delete_ack_intents,
                &persist.ack_intents,
            ),
            @r#"
        (
            b"[{\"cursor\":\"lsn-1\"}\n]",
            true,
            {
                "ops/journal": b"intent-txn1",
            },
        )
        "#
        );

        // Persist is in flight, then completes → Recover.
        ctx.persist_done = false;
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::Idle));
        assert!(matches!(tail, Tail::Persist(_)));

        ctx.persist_done = true;
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::PollAgain));
        assert!(matches!(tail, Tail::Recover(_)));

        // Recover acknowledges the two committed checkpoints (explicit ACKs).
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        match action {
            Action::Acknowledge { checkpoints } => assert_eq!(checkpoints, 2),
            other => panic!("expected Acknowledge, got {other:?}"),
        }
        assert!(matches!(tail, Tail::Acknowledge(_)));

        // Acknowledge is in flight, then completes → WriteIntents carrying txn 1's
        // own intents.
        ctx.acknowledge_done = false;
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::Idle));
        assert!(matches!(tail, Tail::Acknowledge(_)));

        ctx.acknowledge_done = true;
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        match action {
            Action::WriteIntents { ack_intents } => {
                assert_eq!(
                    ack_intents.get("ops/journal").map(Bytes::as_ref),
                    Some(b"intent-txn1".as_slice())
                );
            }
            other => panic!("expected WriteIntents, got {other:?}"),
        }

        // Intents write is in flight, then completes → Done.
        ctx.intents_idle = false;
        let (_action, t) = ctx.step_tail(tail);
        tail = t;
        ctx.intents_idle = true;
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::Idle));
        assert!(matches!(tail, Tail::Done(_)));

        // ===== Phase 3: stop; commit txn 2, then Head stops =====

        // With Tail now Done and `stopping` set, Head rotates txn 2 promptly.
        ctx.stopping = true;
        let (action, h) = ctx.step_head(head, &tail);
        head = h;
        let extents = match action {
            Action::Rotate { extents } => extents,
            other => panic!("expected Rotate, got {other:?}"),
        };
        assert!(matches!(head, Head::Idle(_)));
        tail = Tail::Begin(TailBegin { extents });

        // Commit txn 2 with all IO completing immediately (holds were covered
        // above). It still acknowledges its single checkpoint.
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::Drain { .. }));
        ctx.drain_finished = Some(DrainedCapture::default());
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::WriteStats { .. }));
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::Persist { .. }));
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::PollAgain)); // Persist → Recover.
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        match action {
            Action::Acknowledge { checkpoints } => assert_eq!(checkpoints, 1),
            other => panic!("expected Acknowledge, got {other:?}"),
        }
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::WriteIntents { .. }));
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::Idle));
        assert!(matches!(tail, Tail::Done(_)));

        // Head is Idle with no open transaction; `stopping` and a Done Tail step
        // it to Stop instead of opening txn 3.
        let (action, h) = ctx.step_head(head, &tail);
        head = h;
        assert!(matches!(action, Action::PollAgain));
        assert!(matches!(head, Head::Stop));
    }

    /// An oversized connector checkpoint sequence forces a synthetic checkpoint,
    /// which commits the buffered documents but does NOT advance connector state.
    /// The transaction therefore degrades to at-least-once: with its lone
    /// checkpoint synthetic, the count nets out so Recover skips Acknowledge
    /// entirely (despite explicit acknowledgements).
    #[test]
    fn synthetic_checkpoint_is_not_acknowledged() {
        let mut ctx = mk_ctx(mk_task(true));
        ctx.task.sequence_bytes_limit = 8; // Tiny, to trip the hard bound at once.
        let mut head = Head::Idle(HeadIdle::default());
        let tail = Tail::Done(TailDone::default());

        // Open the transaction with one oversized Captured document.
        ctx.ready = captured(0, b"{\"big\":\"xxxxxxxxxxxxxxxx\"}");
        let (action, h) = ctx.step_head(head, &tail);
        head = h;
        assert!(matches!(action, Action::PollAgain)); // Idle → Extend.

        let (action, h) = ctx.step_head(head, &tail);
        head = h;
        assert!(matches!(action, Action::Captured { .. }));

        // `sequence_bytes` now exceeds the limit: HeadExtend injects a synthetic
        // checkpoint and returns to Idle without awaiting a connector Checkpoint.
        let (action, h) = ctx.step_head(head, &tail);
        head = h;
        assert!(matches!(action, Action::Idle));
        assert!(matches!(head, Head::Idle(_)));

        // The connector offers a second oversized document, but a transaction
        // that already injected a synthetic checkpoint must NOT extend: Head
        // closes instead, leaving the document queued for the next transaction.
        ctx.ready = captured(0, b"{\"big\":\"zzzzzzzzzzzzzzzz\"}");
        let (action, _h) = ctx.step_head(head, &tail);
        let extents = match action {
            Action::Rotate { extents } => extents,
            other => panic!("expected Rotate, got {other:?}"),
        };
        assert_eq!(extents.checkpoints, 1);
        assert!(extents.synthetic_checkpoint);
        assert_eq!(extents.captured_docs, 1); // The second document was not folded in.
        assert!(matches!(ctx.ready, ConnectorRx::Captured(_))); // It waits for txn 2.

        // Drive the Tail commit far enough to observe the Recover decision.
        let mut tail = Tail::Begin(TailBegin { extents });
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::Drain { .. }));
        ctx.drain_finished = Some(DrainedCapture::default());
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::WriteStats { .. }));
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::Persist { .. }));
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::PollAgain)); // Persist → Recover.

        // The lone checkpoint was synthetic, so the effective count is zero:
        // Recover goes straight to WriteIntents, not Acknowledge.
        let (action, t) = ctx.step_tail(tail);
        tail = t;
        assert!(matches!(action, Action::WriteIntents { .. }));
        assert!(matches!(tail, Tail::WriteIntents(_)));
    }

    /// Fuzz Head and Tail by perturbing every Ctx field at each step. Random
    /// connector messages, IO completions, and idle/stopping flags drive
    /// arbitrary transitions; the test asserts no panics. The FSMs are expected
    /// to handle out-of-order or unexpected inputs gracefully (hold their
    /// current state, ignore stray completions) rather than crashing — so most
    /// random sequences never reach commit, but none trips an `unwrap` /
    /// `unreachable!` / index / overflow panic.
    #[test]
    fn fuzz_head_tail_no_panics() {
        use rand::{Rng, SeedableRng, rngs::SmallRng};

        // A random connector message. Never `Pending` (callers gate injection on
        // the connector already being Pending) and only rarely `Eof`, so traces
        // spend their time accumulating and committing rather than stopping early.
        fn random_connector_rx(rng: &mut SmallRng) -> ConnectorRx {
            match rng.random_range(0..12) {
                0..=4 => captured(rng.random_range(0..3), b"{\"v\":1}"),
                5..=8 => checkpoint(),
                9..=10 => ConnectorRx::SourcedSchema {
                    binding: rng.random_range(0..3),
                    shape: doc::Shape::nothing(),
                },
                _ => ConnectorRx::Eof,
            }
        }

        fn perturb(ctx: &mut Ctx, rng: &mut SmallRng) {
            ctx.now.tick();

            // Flip Boolean knobs with low probability so a run spans many
            // distinct combinations. `stopping` is stickier so traces can settle
            // into Stop rather than toggling straight back out.
            if rng.random_bool(0.20) {
                ctx.close_requested = !ctx.close_requested;
            }
            if rng.random_bool(0.05) {
                ctx.stopping = !ctx.stopping;
            }
            if rng.random_bool(0.30) {
                ctx.acknowledge_done = rng.random_bool(0.5);
            }
            if rng.random_bool(0.30) {
                ctx.intents_idle = rng.random_bool(0.5);
            }
            if rng.random_bool(0.30) {
                ctx.persist_done = rng.random_bool(0.5);
            }
            if rng.random_bool(0.30) {
                ctx.stats_idle = rng.random_bool(0.5);
            }
            ctx.combiner_bytes = rng.random_range(0..1_000_000);

            // Inject a connector message only when the connector is Pending,
            // mirroring the actor which reads a new message only then. HeadIdle
            // leaves a queued message in place across its Idle → Extend hop, so
            // forcing one here would race that contract.
            if matches!(ctx.ready, ConnectorRx::Pending) && rng.random_bool(0.6) {
                ctx.ready = random_connector_rx(rng);
            }

            // Stage a drain completion for the Tail.
            if rng.random_bool(0.30) {
                ctx.drain_finished = Some(DrainedCapture {
                    connector_patches: Bytes::from_static(b"[{\"c\":1}\n]"),
                    bindings: BTreeMap::from([(
                        rng.random_range(0..3),
                        ops::proto::stats::DocsAndBytes {
                            docs_total: 1,
                            bytes_total: 10,
                        },
                    )]),
                });
            }

            // Occasionally add an ACK intent; WriteStats drains them into Persist.
            if rng.random_bool(0.10) {
                ctx.pending_ack_intents.insert(
                    format!("ops/journal-{}", rng.random_range(0..4)),
                    Bytes::from_static(b"intent"),
                );
            }
        }

        fn prop(seed: u64) -> bool {
            let mut rng = SmallRng::seed_from_u64(seed);

            // Narrow the close-policy thresholds (vs `mk_task`'s wide ranges) so
            // `policy_extend` flips false after a few Captured documents, letting
            // `policy_close` trip frequently and driving traces through Rotate and
            // the full Tail commit. Without this, Head spends the trace in Extend.
            let mut task = mk_task(rng.random_bool(0.5));
            task.close_policy.combiner_usage_bytes = 0..10_000;
            task.close_policy.read_bytes = 0..50;
            task.close_policy.read_docs = 0..5;
            task.sequence_bytes_limit = 256; // Trip synthetic checkpoints too.

            let mut ctx = mk_ctx(task);
            let mut head = Head::Idle(HeadIdle::default());
            // Start where a real session does: Tail replaying recovered intents.
            let mut tail = Tail::Recover(TailRecover {
                checkpoints: 0,
                ack_intents: BTreeMap::new(),
            });

            for _ in 0..256 {
                perturb(&mut ctx, &mut rng);

                if rng.random_bool(0.5) {
                    // Head::Stop panics at the step boundary by contract, so skip
                    // stepping it — the Actor likewise stops dispatching there.
                    if !matches!(head, Head::Stop) {
                        let (action, h) = ctx.step_head(head, &tail);
                        head = h;
                        // Mirror the Actor's Rotate dispatch so traces exercise
                        // Tail's Drain / Persist / Acknowledge / WriteIntents path
                        // instead of leaving Tail wedged in Done.
                        if let Action::Rotate { extents } = action {
                            tail = Tail::Begin(TailBegin { extents });
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
