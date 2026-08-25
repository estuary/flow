use super::drain;
use crate::leader::capture::{Task, fsm};
use crate::proto;
use anyhow::Context;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use proto_flow::capture::{Request, Response, request, response};
use proto_flow::flow;
use proto_gazette::uuid;
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::sync::mpsc;

type PersistFut = BoxFuture<
    'static,
    anyhow::Result<(
        (crate::shard::RocksDB, Vec<String>),
        Option<proto::persist::ActiveBackfillChange>,
    )>,
>;
type StatsWriteFut<P> = BoxFuture<
    'static,
    tonic::Result<(
        P,
        BTreeMap<String, Bytes>,
        Option<proto::persist::ActiveBackfillChange>,
    )>,
>;
type LabelsApplyFut<P> = BoxFuture<'static, (P, BTreeMap<u32, u64>, tonic::Result<()>)>;

/// Shard-side capture transaction loop for one connector session.
///
/// The actor drives the [`fsm::Head`] and [`fsm::Tail`] state machines: it polls
/// the connector and local IO completions, hands them to the FSMs, and maps the
/// FSMs' emitted `Action`s back into combiner / publisher / persistence / and
/// connector operations.
///
/// In-flight IO futures and the resources they borrow are held as fields. A
/// resource (`db`, `publisher`, ...) is `None` exactly while its future runs,
/// and is restored when that future completes — the "parking" pattern shared
/// with the materialize leader actor.
pub(super) struct Actor<P: crate::Publisher, L: crate::Logger> {
    // --- Task and IO endpoints, fixed for the session. ---
    // `task` is shared (Arc) so the drain future can hold its own handle.
    task: std::sync::Arc<Task>,
    connector_tx: mpsc::Sender<Request>,
    // Per-session metrics counters.
    metrics: super::Metrics,
    // When Some, a deadline at which we begin a graceful session stop.
    // Consumed once at `serve` entry into a hoisted timer.
    token_restart_at: Option<tokio::time::Instant>,
    // Logger of task-centric state changes and events.
    logger: L,
    // True only for shard zero (origin of the key and r-clock ranges). Gates
    // backfill truncation: only shard zero emits backfill messages and manages
    // truncated-at labels, since it alone sees each backfill's full lifecycle.
    is_shard_zero: bool,

    // --- Parked resources: `Some` unless borrowed by an in-flight future. ---
    // RocksDB is parked with its per-binding state keys.
    db: Option<(crate::shard::RocksDB, Vec<String>)>,
    publisher: Option<P>,
    // Inferred per-target write-shapes. Seeded from prior sessions at
    // construction, parked into the drain future, handed back at session end.
    shapes: Option<Vec<doc::Shape>>,
    // Long-lived per-journal throttle policy, fed once per transaction once the
    // collection appends have flushed.
    split_policy: crate::shard::split_policy::SplitPolicy,
    // Drain inputs staged by a Rotate, consumed by the Drain dispatch.
    drain_input: Option<DrainInput>,

    // --- In-flight IO futures; `None` when idle. ---
    acknowledge_fut: Option<BoxFuture<'static, ()>>,
    drain_fut: Option<BoxFuture<'static, anyhow::Result<drain::Output<P>>>>,
    intents_write_fut: Option<BoxFuture<'static, tonic::Result<P>>>,
    labels_apply_fut: Option<LabelsApplyFut<P>>,
    persist_fut: Option<PersistFut>,
    split_fut: Option<crate::shard::SplitFuture>,
    stats_write_fut: Option<StatsWriteFut<P>>,

    // `truncated_at` clock of each binding with an in-progress backfill —
    // present from its BackfillBegin until its BackfillComplete commits.
    active_backfills: BTreeMap<u32, u64>,
    // True when `active_backfills` differs from what's reflected in journal
    // labels.
    labels_dirty: bool,

    // --- Hand-offs staged between FSM steps. ---
    // Drain output, staged for `TailDrain`.
    drain_finished: Option<fsm::DrainedCapture>,
    // ACK intents from a completed stats write, staged for `TailWriteStats`.
    pending_ack_intents: BTreeMap<String, Bytes>,
    // Active-backfill change resolved by the stats write (needs the marker's
    // commit clock), staged for `TailWriteStats` to fold into its Persist.
    pending_active_backfill_change: Option<proto::persist::ActiveBackfillChange>,
}

/// Drain inputs staged by a Rotate, handed to [`drain::drain_and_publish`]
/// when the Tail reaches its Drain step.
struct DrainInput {
    drainer: doc::combine::Drainer,
    parser: simd_doc::Parser,
}

impl<P: crate::Publisher, L: crate::Logger> Actor<P, L> {
    pub fn new(
        active_backfills: BTreeMap<u32, u64>,
        binding_state_keys: Vec<String>,
        connector_tx: mpsc::Sender<Request>,
        db: crate::shard::RocksDB,
        is_shard_zero: bool,
        metrics: super::Metrics,
        logger: L,
        publisher: P,
        shapes: Vec<doc::Shape>,
        task: std::sync::Arc<Task>,
        token_restart_at: Option<std::time::SystemTime>,
    ) -> Self {
        // Map the wall-clock deadline onto the monotonic clock driving `serve`.
        let token_restart_at = token_restart_at.map(|at| {
            let delay = at
                .duration_since(std::time::SystemTime::now())
                .unwrap_or_default();
            tokio::time::Instant::now() + delay
        });

        let labels_dirty = !active_backfills.is_empty();
        Self {
            task,
            connector_tx,
            metrics,
            token_restart_at,
            logger,
            is_shard_zero,
            db: Some((db, binding_state_keys)),
            publisher: Some(publisher),
            shapes: Some(shapes),
            split_policy: crate::shard::split_policy::SplitPolicy::new(),
            drain_input: None,
            acknowledge_fut: None,
            drain_fut: None,
            intents_write_fut: None,
            labels_apply_fut: None,
            persist_fut: None,
            split_fut: None,
            stats_write_fut: None,
            active_backfills,
            labels_dirty,
            drain_finished: None,
            pending_ack_intents: BTreeMap::new(),
            pending_active_backfill_change: None,
        }
    }

    #[tracing::instrument(level = "debug", err(Debug, level = "warn"), skip_all)]
    pub async fn serve<Ctrl, Conn>(
        mut self,
        connector_rx: Conn,
        controller_rx: &mut Ctrl,
        mut head: fsm::Head,
        mut tail: fsm::Tail,
    ) -> anyhow::Result<(crate::shard::RocksDB, Vec<doc::Shape>)>
    where
        Ctrl: futures::Stream<Item = tonic::Result<proto::Capture>> + Send + Unpin + 'static,
        Conn: futures::Stream<Item = tonic::Result<Response>> + Send + Unpin + 'static,
    {
        let mut connector_rx = std::pin::pin!(connector_rx);

        // Double-buffered combiners: one drains while the other accumulates.
        let mut accumulator =
            crate::Accumulator::new(self.task.combine_spec()?).context("creating combiner")?;
        let mut accumulator_idle =
            Some(crate::Accumulator::new(self.task.combine_spec()?).context("creating combiner")?);
        // When true, Head should close its current open transaction ASAP.
        let mut close_requested = false;
        // Iteration counter for the per-loop trace event.
        let mut loop_count = 0u64;
        // Monotonic Clock which is ticked on loop iterations, and updated on IO.
        let mut now = now_clock();
        // When !Pending, a message from the connector that's ready to consume.
        let mut ready_connector_rx = fsm::ConnectorRx::Pending;
        // When true, the capture should gracefully exit.
        let mut stopping = false;
        // Transactions completed in this task session, for preview harness limits.
        let mut transactions_completed = 0usize;
        // Timer for the lowest-priority arm; `sleep_unless_zero` resets it
        // before each await, so this initial deadline is never observed.
        let mut wake_sleep = std::pin::pin!(tokio::time::sleep(Duration::ZERO));
        // IAM token-restart deadline, consumed once at loop entry and hoisted out
        // of the `select!` (which rebuilds its arms every iteration).
        // `far_future` stands in for "no injected credentials, never restart".
        let mut token_restart = std::pin::pin!(tokio::time::sleep_until(
            self.token_restart_at.unwrap_or_else(crate::far_future)
        ));

        while !matches!(head, fsm::Head::Stop) {
            loop_count += 1;
            now.tick(); // Strictly increasing iteration values.

            tracing::trace!(
                loop_count,
                close_requested,
                drain_in_flight = self.drain_fut.is_some(),
                head = ?head,
                persist_in_flight = self.persist_fut.is_some(),
                ready_connector_rx = ready_connector_rx.kind(),
                split_in_flight = self.split_fut.is_some(),
                stats_in_flight = self.stats_write_fut.is_some(),
                stopping,
                tail = ?tail,
                "shard capture Actor::serve iteration"
            );

            let action: fsm::Action;
            let prev_kind = tail.kind();
            (action, tail) = tail.step(
                self.acknowledge_fut.is_none(),
                &mut self.drain_finished,
                self.intents_write_fut.is_none(),
                self.labels_apply_fut.is_none(),
                now,
                self.persist_fut.is_none(),
                &self.task,
                self.stats_write_fut
                    .is_none()
                    .then_some(&mut self.pending_ack_intents),
                &mut self.pending_active_backfill_change,
            );
            if prev_kind != tail.kind() {
                service_kit::event!(
                    tracing::Level::DEBUG,
                    "tail",
                    prev = prev_kind,
                    action = action.kind(),
                    next = tail.kind(),
                    "transition",
                );
            }
            let tail_wake_after = self.dispatch(action, &mut accumulator)?;

            let action: fsm::Action;
            let prev_kind = head.kind();
            (action, head) = head.step(
                now,
                &mut close_requested,
                accumulator.combiner_byte_usage(),
                &mut ready_connector_rx,
                stopping,
                &tail,
                &self.task,
            );
            if prev_kind != head.kind() {
                service_kit::event!(
                    tracing::Level::DEBUG,
                    "head",
                    prev = prev_kind,
                    action = action.kind(),
                    next = head.kind(),
                    "transition",
                );
            }
            let head_wake_after = match action {
                fsm::Action::Rotate { extents } => {
                    assert!(matches!(tail, fsm::Tail::Done(_)));
                    self.metrics.transactions.increment(1);
                    transactions_completed += 1;

                    if self.task.max_transactions == 0 || stopping {
                        // Pass
                    } else if transactions_completed >= self.task.max_transactions as usize {
                        service_kit::event!(
                            tracing::Level::INFO,
                            "head",
                            transactions_completed,
                            max_transactions = self.task.max_transactions,
                            "capture transaction limit reached; stopping gracefully",
                        );
                        stopping = true;
                    }
                    tail = fsm::Tail::Begin(fsm::TailBegin { extents });

                    let recycled = accumulator_idle.take().expect("tail is done");
                    let active = std::mem::replace(&mut accumulator, recycled);
                    let (drainer, parser) = active
                        .into_drainer()
                        .context("preparing to drain combiner")?;
                    self.drain_input = Some(DrainInput { drainer, parser });

                    Duration::ZERO
                }
                action => self.dispatch(action, &mut accumulator)?,
            };
            let wake_after = std::cmp::min(head_wake_after, tail_wake_after);

            tokio::select! {
                biased;

                // Prioritize completions of Tail IO first.
                Some(result) = maybe_fut(&mut self.drain_fut) => {
                    let output: drain::Output<P> = result?;
                    accumulator_idle = Some(output.accumulator);
                    self.publisher = Some(output.publisher);
                    self.shapes = Some(output.shapes);
                    self.drain_finished = Some(output.drained);
                    self.drain_fut = None;
                }
                Some(result) = maybe_fut(&mut self.stats_write_fut) => {
                    let (publisher, ack_intents, change) = result.map_err(crate::status_to_anyhow)
                        .context("writing capture ops stats document")?;
                    self.publisher = Some(publisher);
                    self.pending_ack_intents = ack_intents;
                    self.pending_active_backfill_change = change;
                    self.stats_write_fut = None;

                    // WriteStats flushed this transaction's collection appends, so
                    // the publisher's per-journal throttle samples are now complete
                    self.observe_throttle();
                }
                Some(result) = maybe_fut(&mut self.persist_fut) => {
                    let (db, change) = result?;
                    self.db = Some(db);
                    self.persist_fut = None;
                    match change {
                        Some(proto::persist::ActiveBackfillChange::Begin(begin)) => {
                            self.active_backfills.insert(begin.binding, begin.truncated_at);
                            self.labels_dirty = true;
                        }
                        Some(proto::persist::ActiveBackfillChange::CompleteBinding(binding)) => {
                            self.active_backfills.remove(&binding);
                            // Re-apply remaining backfills' labels; if this was the
                            // last one, an empty map has nothing to apply, so don't
                            // strand `labels_dirty` at true.
                            self.labels_dirty = !self.active_backfills.is_empty();
                        }
                        None => {}
                    }
                }
                Some(()) = maybe_fut(&mut self.acknowledge_fut) => {
                    self.acknowledge_fut = None;
                }
                Some(result) = maybe_fut(&mut self.intents_write_fut) => {
                    let publisher = result.map_err(crate::status_to_anyhow)
                        .context("writing capture ACK intents")?;
                    self.publisher = Some(publisher);
                    self.intents_write_fut = None;
                }
                Some((journal, outcome)) = maybe_fut(&mut self.split_fut) => {
                    crate::shard::finish_split(
                        &mut self.split_policy,
                        &journal,
                        outcome,
                        std::time::Instant::now(),
                    );
                    self.split_fut = None;
                }
                Some((publisher, active_backfills, result)) = maybe_fut(&mut self.labels_apply_fut) => {
                    result.context("applying truncated-at journal labels")?;
                    self.publisher = Some(publisher);
                    self.active_backfills = active_backfills;
                    self.labels_apply_fut = None;
                    self.labels_dirty = false;
                }
                // Process controller messages next.
                msg = controller_rx.next() => {
                    Self::on_controller_rx(msg, &mut close_requested, &mut stopping)?;
                },
                // Process new connector messages last.
                msg = connector_rx.next(), if matches!(ready_connector_rx, fsm::ConnectorRx::Pending) => {
                    self.on_connector_rx(&mut ready_connector_rx, msg)?;
                }
                // Next, a graceful session restart ahead of IAM token expiry
                _ = token_restart.as_mut() => {
                    service_kit::event!(
                        tracing::Level::INFO,
                        "shard",
                        "injected IAM credentials expire soon; stopping session gracefully",
                    );
                    stopping = true;
                    // A fired `Sleep` stays Ready, so disarm to keep this arm
                    // from winning every later iteration.
                    token_restart.as_mut().reset(crate::far_future());
                }

                // Lowest priority.
                _ = crate::sleep_unless_zero(wake_sleep.as_mut(), wake_after) => {}
            }

            if !wake_after.is_zero() {
                now.update(now_clock()); // Resync after blocking IO.
            }
        }

        let (db, _binding_state_keys) = self.db.take().context("missing RocksDB")?;
        // Hand inferred shapes back so the next session continues inference
        // rather than re-widening every binding from `nothing()`.
        let shapes = self.shapes.take().context("missing capture shapes")?;

        Ok((db, shapes))
    }

    /// Drain this transaction's per-journal throttle samples from the publisher
    /// and feed them into the long-lived split policy, then start a split of
    /// at most one persistently-throttled journal — off the hot path, parked
    /// as `split_fut`.
    fn observe_throttle(&mut self) {
        // Callers ensure the publisher is Some whenever this is called, so unwrap here.
        let publisher = self
            .publisher
            .as_mut()
            .expect("publisher is Some whenever observe_throttle is called");
        let now = std::time::Instant::now();
        crate::shard::observe_throttle_samples(
            &mut self.split_policy,
            publisher.take_throttle_samples(),
            now,
        );
        if self.split_fut.is_none() {
            self.split_fut = crate::shard::start_due_split(&mut self.split_policy, publisher, now);
        }
    }

    /// Execute the outgoing-IO primitive for an Action.
    #[tracing::instrument(level = "trace", fields(action = ?action), skip_all)]
    fn dispatch(
        &mut self,
        action: fsm::Action,
        accumulator: &mut crate::Accumulator,
    ) -> anyhow::Result<Duration> {
        let blocking = match action {
            fsm::Action::Rotate { .. } => unreachable!("never dispatched"),

            fsm::Action::Idle => true,
            fsm::Action::PollAgain => false,
            fsm::Action::Sleep { wake_after } => return Ok(wake_after),

            fsm::Action::Captured { captured } => {
                let response::Captured { binding, doc_json } = captured;
                let binding_spec = self
                    .task
                    .bindings
                    .get(binding as usize)
                    .with_context(|| format!("invalid captured binding {binding}"))?;
                let target = &self.task.targets[binding_spec.target as usize];

                let (memtable, alloc, mut doc) =
                    accumulator.parse_json_doc(&doc_json).with_context(|| {
                        format!(
                            "couldn't parse captured document as JSON (target {})",
                            target.collection_name
                        )
                    })?;

                let uuid_ptr = &target.document_uuid_ptr;
                if !uuid_ptr.0.is_empty() {
                    let Ok(_) = doc.try_set(
                        uuid_ptr,
                        doc::HeapNode::String(doc::BumpStr::from_str(
                            crate::UUID_PLACEHOLDER,
                            alloc,
                        )),
                        alloc,
                    ) else {
                        anyhow::bail!("unable to create document UUID placeholder");
                    };
                }
                memtable.add(binding as u16, doc, false)?;
                true
            }

            fsm::Action::Checkpoint { checkpoint } => {
                // A checkpoint is not obligated to carry a connector state update.
                if let Some(flow::ConnectorState {
                    updated_json,
                    merge_patch,
                }) = checkpoint.state
                {
                    let (memtable, _alloc, doc) = accumulator
                        .parse_json_doc(&updated_json)
                        .context("couldn't parse connector state as JSON")?;

                    // Non-merge-patch uses a `null` followed by the new state.
                    if !merge_patch {
                        memtable.add(
                            self.task.bindings.len() as u16,
                            doc::HeapNode::Null,
                            false,
                        )?;
                    }
                    memtable.add(self.task.bindings.len() as u16, doc, false)?;
                }
                false // Re-poll to allow for close on connector idle-ness.
            }

            fsm::Action::Drain { sourced_schemas } => {
                let DrainInput { drainer, parser } = self
                    .drain_input
                    .take()
                    .context("missing capture drain input")?;
                let publisher = self.publisher.take().context("missing capture publisher")?;
                let shapes = self.shapes.take().context("missing capture shape state")?;
                let task = std::sync::Arc::clone(&self.task);
                let metrics = self.metrics.clone();
                let logger = self.logger.clone();
                self.drain_fut = Some(
                    async move {
                        drain::drain_and_publish(
                            drainer,
                            parser,
                            publisher,
                            task,
                            sourced_schemas,
                            shapes,
                            metrics,
                            logger,
                        )
                        .await
                    }
                    .boxed(),
                );
                true
            }

            fsm::Action::WriteStats { stats, backfill } => {
                let mut publisher = self.publisher.take().context("missing capture publisher")?;
                // A BackfillComplete truncates to its matching begin's clock,
                // recovered from the shard's active-backfill state; snapshot it
                // before the future moves `publisher`.
                let active_backfill_begin = match &backfill {
                    Some(fsm::BackfillMessage::BackfillComplete { binding }) => {
                        self.active_backfills.get(binding).copied()
                    }
                    _ => None,
                };
                self.stats_write_fut = Some(
                    async move {
                        if !stats.capture.is_empty() {
                            publisher.publish_stats(stats).await?;
                        }
                        publisher.flush().await?;

                        let (intents, change) =
                            build_write_intents(&mut publisher, backfill, active_backfill_begin)
                                .await?;

                        Ok((publisher, intents, change))
                    }
                    .boxed(),
                );
                true
            }

            fsm::Action::Persist { persist } => {
                self.logger
                    .event(crate::LogEvent::Persist { persist: &persist });

                let (db, binding_state_keys) =
                    self.db.take().context("Persist while RocksDB is busy")?;
                self.persist_fut = Some(
                    async move {
                        let db = db
                            .persist(&persist, &binding_state_keys)
                            .await
                            .context("Persisting capture state")?;
                        Ok(((db, binding_state_keys), persist.active_backfill_change))
                    }
                    .boxed(),
                );
                true
            }

            fsm::Action::Acknowledge { checkpoints } => {
                let connector_tx = self.connector_tx.clone();
                self.acknowledge_fut = Some(
                    async move {
                        // Sends to the connector are best-effort: a connector
                        // which exited has closed its channel, and the
                        // acknowledgement is moot once it has -- the commit is
                        // durable, and it recovers from persisted state on its
                        // next session. Connector failures surface on the
                        // receive side, never here.
                        _ = connector_tx
                            .send(Request {
                                acknowledge: Some(request::Acknowledge { checkpoints }),
                                ..Default::default()
                            })
                            .await;
                    }
                    .boxed(),
                );
                true
            }

            fsm::Action::WriteIntents { ack_intents } => {
                let mut publisher = self.publisher.take().context("missing capture publisher")?;
                self.intents_write_fut = Some(
                    async move {
                        publisher.write_intents(ack_intents).await?;
                        Ok(publisher)
                    }
                    .boxed(),
                );
                true
            }

            fsm::Action::ApplyTruncatedLabels => {
                // Only shard zero manages truncated-at labels: a non-zero shard
                // that inherited `active_backfills` (e.g. a mid-backfill split)
                // can't clear them — BackfillComplete reaches only shard zero.
                if !self.is_shard_zero || !self.labels_dirty || self.active_backfills.is_empty() {
                    false
                } else {
                    let mut publisher =
                        self.publisher.take().context("missing capture publisher")?;
                    let active_backfills = std::mem::take(&mut self.active_backfills);
                    self.labels_apply_fut = Some(
                        async move {
                            let result =
                                publisher.apply_truncated_at_labels(&active_backfills).await;
                            (publisher, active_backfills, result)
                        }
                        .boxed(),
                    );
                    true
                }
            }

            fsm::Action::Error(error) => return Err(error),
        };

        Ok(if blocking {
            crate::ACTOR_TICK_INTERVAL
        } else {
            Duration::ZERO
        })
    }

    fn on_controller_rx(
        result: Option<tonic::Result<proto::Capture>>,
        close_requested: &mut bool,
        stopping: &mut bool,
    ) -> anyhow::Result<()> {
        let verify = crate::verify("Capture", "Stop or CloseNow", "controller");
        let msg = verify.not_eof(result)?;

        let kind: &str;
        if matches!(msg.stop, Some(proto::Stop {})) {
            *stopping = true;
            kind = "Stopping";
        } else if matches!(msg.close_now, Some(proto::CloseNow {})) {
            *close_requested = true;
            kind = "CloseNow";
        } else {
            return Err(verify.fail_msg(msg));
        }

        service_kit::event!(
            tracing::Level::DEBUG,
            "controller",
            kind,
            "received from controller",
        );
        Ok(())
    }

    fn on_connector_rx(
        &self,
        ready: &mut fsm::ConnectorRx,
        msg: Option<tonic::Result<Response>>,
    ) -> anyhow::Result<()> {
        let verify = crate::verify(
            "Capture",
            "Captured, SourcedSchema, Checkpoint, BackfillBegin, or BackfillComplete",
            "connector",
        );
        let Some(response) = msg else {
            *ready = fsm::ConnectorRx::Eof;
            return Ok(());
        };
        let response = verify.ok(response)?;

        *ready = if let Some(captured) = response.captured {
            fsm::ConnectorRx::Captured(captured)
        } else if let Some(sourced) = response.sourced_schema {
            let (binding, shape) = parse_sourced_schema(sourced, &self.task)?;

            service_kit::event!(
                tracing::Level::DEBUG,
                "connector",
                binding,
                "received SourcedSchema from connector",
            );
            fsm::ConnectorRx::SourcedSchema { binding, shape }
        } else if let Some(checkpoint) = response.checkpoint {
            service_kit::event!(
                tracing::Level::TRACE,
                "connector",
                "received Checkpoint from connector",
            );
            fsm::ConnectorRx::Checkpoint(checkpoint)
        } else if let Some(response::BackfillBegin { binding }) = response.backfill_begin {
            if !self.is_shard_zero {
                anyhow::bail!(
                    "connector emitted BackfillBegin for binding {binding}, but \
                     only shard zero manages backfill truncation"
                );
            }
            if binding as usize >= self.task.bindings.len() {
                anyhow::bail!("connector emitted BackfillBegin for out-of-range binding {binding}");
            }
            service_kit::event!(
                tracing::Level::INFO,
                "connector",
                binding,
                "received BackfillBegin from connector",
            );
            fsm::ConnectorRx::Backfill(fsm::BackfillMessage::BackfillBegin { binding })
        } else if let Some(response::BackfillComplete { binding }) = response.backfill_complete {
            if !self.is_shard_zero {
                anyhow::bail!(
                    "connector emitted BackfillComplete for binding {binding}, but \
                     only shard zero manages backfill truncation"
                );
            }
            if binding as usize >= self.task.bindings.len() {
                anyhow::bail!(
                    "connector emitted BackfillComplete for out-of-range binding {binding}"
                );
            }
            service_kit::event!(
                tracing::Level::INFO,
                "connector",
                binding,
                "received BackfillComplete from connector",
            );
            fsm::ConnectorRx::Backfill(fsm::BackfillMessage::BackfillComplete { binding })
        } else {
            return Err(verify.fail_msg(response));
        };
        Ok(())
    }
}

/// Snapshot this transaction's ACK intents, plus its resolved
/// [`proto::persist::ActiveBackfillChange`] for a marker transaction. An ordinary
/// transaction (`backfill` is `None`) ACKs only journals it appended; a marker
/// broadcasts across *every* partition via [`crate::Publisher::marker_commit`].
async fn build_write_intents(
    publisher: &mut impl crate::Publisher,
    backfill: Option<fsm::BackfillMessage>,
    active_backfill_begin: Option<u64>,
) -> tonic::Result<(
    BTreeMap<String, Bytes>,
    Option<proto::persist::ActiveBackfillChange>,
)> {
    match backfill {
        None => {
            let intents = match publisher.commit_intents() {
                Some(commit) => publisher::intents::build_transaction_intents(&[commit], None),
                None => BTreeMap::new(),
            };
            Ok((intents, None))
        }
        Some(fsm::BackfillMessage::BackfillBegin { binding }) => {
            let Some((producer, clock, journals)) =
                publisher.marker_commit(binding as usize).await?
            else {
                // Preview only: no journal IO, so no broadcast clock. Stage the
                // Begin with a zero (inert) boundary so preview state transitions
                // like a real run — a `truncated_at` of 0 suppresses nothing,
                // since real document clocks are always > 0.
                return Ok((
                    BTreeMap::new(),
                    Some(proto::persist::ActiveBackfillChange::Begin(
                        proto::ActiveBackfillBegin {
                            binding,
                            truncated_at: 0,
                        },
                    )),
                ));
            };
            // The marker's single broadcast clock is the authoritative boundary.
            let truncated_at = clock.as_u64();
            let intents = publisher::intents::build_transaction_intents(
                &[(producer, clock, journals)],
                Some(&publisher::intents::BackfillMarker::Begin),
            );
            Ok((
                intents,
                Some(proto::persist::ActiveBackfillChange::Begin(
                    proto::ActiveBackfillBegin {
                        binding,
                        truncated_at,
                    },
                )),
            ))
        }
        Some(fsm::BackfillMessage::BackfillComplete { binding }) => {
            let Some(truncated_at) = active_backfill_begin else {
                // Orphaned complete (no active backfill, e.g. a begin was never
                // observed): publish nothing, change nothing. Unexpected — a
                // connector shouldn't complete a backfill it never began — so
                // surface it rather than swallowing it silently.
                service_kit::event!(
                    tracing::Level::WARN,
                    "connector",
                    binding,
                    "ignoring a BackfillComplete with no matching active backfill (orphaned complete)",
                );
                return Ok((BTreeMap::new(), None));
            };
            let change = Some(proto::persist::ActiveBackfillChange::CompleteBinding(
                binding,
            ));
            let Some((producer, clock, journals)) =
                publisher.marker_commit(binding as usize).await?
            else {
                return Ok((BTreeMap::new(), change));
            };
            let intents = publisher::intents::build_transaction_intents(
                &[(producer, clock, journals)],
                Some(&publisher::intents::BackfillMarker::Complete { truncated_at }),
            );
            Ok((intents, change))
        }
    }
}

/// Parse and validate a connector `SourcedSchema` into its target binding
/// index and inferred write-shape. All schema parsing and error checking lives
/// here so the HeadFSM's per-binding shape fold stays infallible.
fn parse_sourced_schema(
    sourced: response::SourcedSchema,
    task: &Task,
) -> anyhow::Result<(u32, doc::Shape)> {
    let response::SourcedSchema {
        binding,
        schema_json,
    } = sourced;

    let binding_spec = task
        .bindings
        .get(binding as usize)
        .with_context(|| format!("invalid sourced schema binding {binding}"))?;
    let collection_name = &task.targets[binding_spec.target as usize].collection_name;

    let built_schema = doc::validation::build_bundle(&schema_json).with_context(|| {
        format!("couldn't parse sourced schema as JSON Schema (target {collection_name})")
    })?;
    let validator = doc::Validator::new(built_schema).with_context(|| {
        format!("couldn't build a sourced schema validator (target {collection_name})")
    })?;
    let shape = doc::Shape::infer(validator.schema(), validator.schema_index());

    let errors = shape.inspect_closed();
    if !errors.is_empty() {
        anyhow::bail!(
            "connector implementation error: binding {binding} (target {collection_name}) SourcedSchema has errors: {errors:?}"
        );
    }
    Ok((binding, shape))
}

fn now_clock() -> uuid::Clock {
    let now = tokens::now();
    uuid::Clock::from_unix(now.timestamp() as u64, now.timestamp_subsec_nanos())
}

/// Resolve to the future's output, or park forever when there's no future.
/// The caller clears the `Option` in the corresponding `select!` arm.
async fn maybe_fut<T>(opt: &mut Option<BoxFuture<'static, T>>) -> Option<T> {
    match opt.as_mut() {
        Some(fut) => Some(fut.await),
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::leader::capture::task::fixture;
    use crate::logger::RecordingLogger;
    use crate::publish::RecordingPublisher;
    use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};

    // --- Harness: task fixtures, actor and session drivers, responses. ---

    /// The default Task: two bindings on distinct collections, of which binding
    /// 0 carries a UUID pointer (exercising placeholder injection).
    fn mk_task(explicit_acknowledgements: bool) -> Task {
        fixture::task(
            &[
                ("test/collectionA", "stateA", "/_meta/uuid"),
                ("test/collectionB", "stateB", ""),
            ],
            b"{}",
            explicit_acknowledgements,
        )
    }

    /// State keys of `task`'s bindings, under which a shard scans and persists.
    fn state_keys(task: &Task) -> Vec<String> {
        task.bindings
            .iter()
            .map(|binding| binding.state_key.clone())
            .collect()
    }

    /// An [`Actor`] over `task`, with an mpsc channel standing in for the
    /// connector. Its returned receiver is the mock connector's end: hold it
    /// even when unread, since dropping it closes the actor's request channel.
    fn mk_actor<P: crate::Publisher, L: crate::Logger>(
        task: &std::sync::Arc<Task>,
        active_backfills: BTreeMap<u32, u64>,
        db: crate::shard::RocksDB,
        is_shard_zero: bool,
        logger: L,
        publisher: P,
    ) -> (Actor<P, L>, mpsc::Receiver<Request>) {
        let (connector_tx, connector_rx) = mpsc::channel::<Request>(crate::CHANNEL_BUFFER);

        let actor = Actor::new(
            active_backfills,
            state_keys(task),
            connector_tx,
            db,
            is_shard_zero,
            super::super::Metrics::new("test/shard"),
            logger,
            publisher,
            task.shapes_by_target(Default::default()),
            task.clone(),
            None, // token_restart_at
        );
        (actor, connector_rx)
    }

    /// What one driven capture session left behind.
    struct Session {
        db: crate::shard::RocksDB,
        /// Inferred write-shapes handed back at session end, one per Target.
        shapes: Vec<doc::Shape>,
        acks: Vec<request::Acknowledge>,
        publisher: RecordingPublisher,
        task: std::sync::Arc<Task>,
    }

    impl Session {
        /// Scan this session's RocksDB, as a next session's recovery does.
        async fn recover(self) -> (crate::shard::RocksDB, proto::Recover) {
            self.db.scan(state_keys(&self.task)).await.unwrap()
        }
    }

    /// Drive `Actor::serve` end to end over mpsc channels standing in for the
    /// connector and controller, with a real RocksDB: feed `responses`, drain
    /// `expect_acks` Acknowledges (one per committed transaction, so each
    /// commits before Stop), then Stop.
    ///
    /// The actor owns its publisher and logger for the session's duration, so
    /// both record through `Arc` handles: `logger` is the caller's to read back
    /// once this returns, as is [`Session::publisher`].
    async fn run_capture_session(
        task: Task,
        db: crate::shard::RocksDB,
        active_backfills: BTreeMap<u32, u64>,
        logger: impl crate::Logger,
        responses: Vec<tonic::Result<Response>>,
        expect_acks: usize,
    ) -> Session {
        let (conn_resp_tx, conn_resp_rx) =
            mpsc::channel::<tonic::Result<Response>>(crate::CHANNEL_BUFFER);
        let (controller_tx, controller_rx) =
            mpsc::unbounded_channel::<tonic::Result<proto::Capture>>();

        let task = std::sync::Arc::new(task);
        let publisher = RecordingPublisher::default();
        let (actor, mut actor_to_conn_rx) = mk_actor(
            &task,
            active_backfills,
            db,
            true, // is_shard_zero
            logger,
            publisher.clone(),
        );

        let serve = tokio::spawn(async move {
            let mut controller_rx = UnboundedReceiverStream::new(controller_rx);
            actor
                .serve(
                    ReceiverStream::new(conn_resp_rx),
                    &mut controller_rx,
                    fsm::Head::Idle(fsm::HeadIdle::default()),
                    fsm::Tail::Recover(fsm::TailRecover {
                        checkpoints: 0,
                        ack_intents: BTreeMap::new(),
                    }),
                )
                .await
        });

        // An actor which errors drops its channels, so unwrapping a send or
        // receive here would report a closed channel in place of the failure that
        // actually happened. Break out instead and let the join below re-raise
        // the actor's own error.
        for response in responses {
            if conn_resp_tx.send(response).await.is_err() {
                break;
            }
        }
        let mut acks = Vec::new();
        while acks.len() < expect_acks {
            let Some(request) = actor_to_conn_rx.recv().await else {
                break;
            };
            acks.push(request.acknowledge.expect("actor sent a non-Acknowledge"));
        }
        _ = controller_tx.send(Ok(proto::Capture {
            stop: Some(proto::Stop {}),
            ..Default::default()
        }));

        let (db, shapes) = serve
            .await
            .unwrap()
            .unwrap_or_else(|err| panic!("capture session failed: {err:?}"));
        assert_eq!(
            acks.len(),
            expect_acks,
            "session stopped before acknowledging"
        );

        Session {
            db,
            shapes,
            acks,
            publisher,
            task,
        }
    }

    fn captured(binding: u32, doc_json: &'static [u8]) -> tonic::Result<Response> {
        Ok(Response {
            captured: Some(response::Captured {
                binding,
                doc_json: Bytes::from_static(doc_json),
            }),
            ..Default::default()
        })
    }

    fn checkpoint(state_json: &'static [u8]) -> tonic::Result<Response> {
        Ok(Response {
            checkpoint: Some(response::Checkpoint {
                state: Some(flow::ConnectorState {
                    updated_json: Bytes::from_static(state_json),
                    merge_patch: true,
                }),
            }),
            ..Default::default()
        })
    }

    fn backfill_begin(binding: u32) -> tonic::Result<Response> {
        Ok(Response {
            backfill_begin: Some(response::BackfillBegin { binding }),
            ..Default::default()
        })
    }

    fn backfill_complete(binding: u32) -> tonic::Result<Response> {
        Ok(Response {
            backfill_complete: Some(response::BackfillComplete { binding }),
            ..Default::default()
        })
    }

    /// Distinct target collections of the fan-in fixture.
    const M: usize = 3;
    /// Bindings of the fan-in fixture, fanning evenly onto the M collections:
    /// binding `i` writes collection `i % M`, so each collection has `N / M`.
    const N: usize = 12;
    /// Documents the mock connector captures under one key, per collection.
    /// Well above the memtable's 32-queued-document compaction threshold, so the
    /// documents genuinely reduce in memory rather than only at drain.
    const RUN: i64 = 64;

    /// Everything one fan-in session published and inferred, rendered as strings
    /// so a mismatch reads as a diff rather than as two proto Debug dumps.
    type Recording = (
        Vec<(usize, String)>,
        Vec<String>,
        Vec<(String, Option<usize>, String)>,
    );

    /// Drive one capture session over `spec` with the shared connector script,
    /// and return what it published and inferred.
    ///
    /// The script exercises both hazards of per-collection derived state:
    /// several bindings of one collection publish documents under the *same*
    /// key (so a shared validator or a mis-mapped target would cross-contaminate
    /// rather than merely miscount), repeats within one binding reduce, and a
    /// `SourcedSchema` folds into the collection's inference alongside the
    /// documents' widening.
    async fn run_fan_in_session(spec: flow::CaptureSpec) -> Recording {
        let (open, opened) = fixture::open(spec);
        let task = Task::new(&open, &opened, 0).unwrap();

        // Every document uses key "shared", so each collection's documents
        // collide with the others' on key as well as within a binding.
        let capture_doc = |binding: usize, value: i64| {
            Ok(Response {
                captured: Some(response::Captured {
                    binding: binding as u32,
                    doc_json: Bytes::from(format!(
                        r#"{{"id":"shared","from_collection_{}":true,"value":{value}}}"#,
                        binding % M,
                    )),
                }),
                ..Default::default()
            })
        };

        // One `RUN` per collection, through bindings 0, 1 and 2, so each shared
        // validator is exercised for its *reduce* annotations and not only for
        // validation. Then single documents on bindings 3, 6 and 9 -- collection-0
        // again -- whose outputs must stay separate from binding 0's however much
        // machinery the four of them share.
        let responses = (0..RUN)
            .flat_map(|_| [capture_doc(0, 1), capture_doc(1, 2), capture_doc(2, 3)])
            .chain([capture_doc(3, 10), capture_doc(6, 100), capture_doc(9, 1000)])
            .chain([
                Ok(Response {
                    sourced_schema: Some(response::SourcedSchema {
                        binding: 5, // Collection-2, which binding 2 also writes.
                        schema_json: Bytes::from_static(
                            br#"{"type":"object","additionalProperties":false,"properties":{"id":{"type":"string"},"from_collection_2":{"const":true},"sourced_only":{"type":"boolean"}},"required":["id","from_collection_2"]}"#,
                        ),
                    }),
                    ..Default::default()
                }),
                checkpoint(br#"{"cursor":"lsn-1"}"#),
            ])
            .collect();

        let logger = RecordingLogger::default();
        let session = run_capture_session(
            task,
            crate::shard::RocksDB::open(None).await.unwrap(),
            BTreeMap::new(),
            logger.clone(),
            responses,
            1,
        )
        .await;

        let stats = session
            .publisher
            .take_stats()
            .into_iter()
            .map(normalize_stats)
            .collect();

        (
            session.publisher.take_docs(),
            stats,
            logger.take_inferences(),
        )
    }

    /// Render `stats` for comparison, dropping the fields which are a function of
    /// when the transaction ran rather than of what it did.
    fn normalize_stats(mut stats: ops::proto::Stats) -> String {
        stats.meta = None; // UUID is stamped by the publisher.
        stats.timestamp = None;
        stats.open_seconds_total = 0.0;

        for binding in stats.capture.values_mut() {
            binding.last_published_at = None;
        }
        serde_json::to_string_pretty(&stats).unwrap()
    }

    // --- Tests. ---

    /// One checkpoint sequence end to end: the connector emits two Captured
    /// documents (into distinct bindings) and a Checkpoint carrying connector
    /// state. The actor accumulates them, closes the transaction once the
    /// connector idles, and runs the full Tail commit: drain+publish, stats, the
    /// committing Persist, Acknowledge, and WriteIntents. Receiving the connector
    /// Acknowledge proves the commit reached its post-Persist handoff; a
    /// controller Stop then drains the Tail and steps Head to Stop.
    #[tokio::test]
    async fn serve_transaction_then_stop() {
        let session = run_capture_session(
            mk_task(true),
            crate::shard::RocksDB::open(None).await.unwrap(),
            BTreeMap::new(),
            crate::TracingLogger,
            vec![
                captured(0, br#"{"id":"a0"}"#),
                captured(1, br#"{"id":"b0"}"#),
                checkpoint(br#"{"cursor":"lsn-9"}"#),
            ],
            1,
        )
        .await;

        assert_eq!(session.acks[0].checkpoints, 1);
        assert_eq!(session.shapes.len(), 2); // One inferred shape per Target.

        // The drain published each document to the binding which captured it.
        assert_eq!(
            session
                .publisher
                .take_docs()
                .into_iter()
                .map(|(binding, _doc)| binding)
                .collect::<Vec<_>>(),
            [0, 1],
        );

        // The committing Persist durably recorded the connector state.
        let (_db, recover) = session.recover().await;
        assert_eq!(
            recover.connector_state_json.as_ref(),
            br#"{"cursor":"lsn-9"}"#
        );
    }

    /// A connector which exits before its committed transaction is acknowledged
    /// must not fail the session: sends to the connector are best-effort, and
    /// the acknowledgement is moot once it has exited -- the commit is durable,
    /// and the connector recovers from persisted state on its next session.
    /// This is routine for polling captures, which emit a final Checkpoint and
    /// exit without awaiting its Acknowledge.
    #[tokio::test]
    async fn connector_exit_before_acknowledge_is_benign() {
        let (conn_resp_tx, conn_resp_rx) =
            mpsc::channel::<tonic::Result<Response>>(crate::CHANNEL_BUFFER);
        let (_controller_tx, controller_rx) =
            mpsc::unbounded_channel::<tonic::Result<proto::Capture>>();

        let task = std::sync::Arc::new(mk_task(true));
        let (actor, actor_to_conn_rx) = mk_actor(
            &task,
            BTreeMap::new(),
            crate::shard::RocksDB::open(None).await.unwrap(),
            true, // is_shard_zero
            crate::TracingLogger,
            RecordingPublisher::default(),
        );
        // The connector has exited: its request channel is closed before the
        // actor can send the post-commit Acknowledge.
        std::mem::drop(actor_to_conn_rx);

        for response in [
            captured(0, br#"{"id":"a0"}"#),
            checkpoint(br#"{"cursor":"lsn-9"}"#),
        ] {
            conn_resp_tx.send(response).await.unwrap();
        }
        // Response stream EOF. Once the Tail drains, the Head steps to Stop on
        // its own (the fixture's `restart` Clock is zero, and thus elapsed).
        std::mem::drop(conn_resp_tx);

        let mut controller_rx = UnboundedReceiverStream::new(controller_rx);
        let (db, _shapes) = actor
            .serve(
                ReceiverStream::new(conn_resp_rx),
                &mut controller_rx,
                fsm::Head::Idle(fsm::HeadIdle::default()),
                fsm::Tail::Recover(fsm::TailRecover {
                    checkpoints: 0,
                    ack_intents: BTreeMap::new(),
                }),
            )
            .await
            .expect("session must stop cleanly despite the closed connector channel");

        // The transaction nonetheless committed durably.
        let (_db, recover) = db.scan(state_keys(&task)).await.unwrap();
        assert_eq!(
            recover.connector_state_json.as_ref(),
            br#"{"cursor":"lsn-9"}"#
        );
    }

    /// A capture fanning N bindings onto M collections publishes each binding's
    /// own combined documents and infers one schema per collection -- and does so
    /// identically whether the spec names its collections indirectly or inline.
    ///
    /// Bindings which share a collection share its derived state: one combiner
    /// validator, one publisher target, one inferred shape. That sharing is the
    /// point (a task pays per collection, not per binding), and this is the
    /// strongest statement it can make from outside. If it leaked -- a document
    /// reduced against the wrong binding's accumulation, a target pointed at the
    /// wrong collection, a widened shape attributed to the wrong target -- either
    /// the documents below would be wrong, or the two spec forms would diverge.
    #[tokio::test]
    async fn fan_in_capture_publishes_per_binding_and_infers_per_collection() {
        let indirect = fixture::capture_spec(M, &(0..N).map(|index| index % M).collect::<Vec<_>>());
        let mut inline = indirect.clone();
        fixture::into_inline(&mut inline);

        // Targets key on the collection's journal identity, which both forms
        // carry, so the forms are indistinguishable from outside.
        let indirect = run_fan_in_session(indirect).await;
        assert_eq!(indirect, run_fan_in_session(inline).await);

        let (docs, stats, inferences) = indirect;

        // Guard against the recordings agreeing because both are empty, and pin
        // what each binding published as `(documents, summed value)`.
        let mut published = BTreeMap::<usize, (usize, i64)>::new();

        for (binding, doc) in &docs {
            let doc: serde_json::Value = serde_json::from_str(doc).unwrap();
            let entry = published.entry(*binding).or_default();

            entry.0 += 1;
            entry.1 += doc["value"].as_i64().unwrap();

            assert_eq!(
                doc[format!("from_collection_{}", binding % M)],
                serde_json::json!(true),
                "binding {binding} published a document of another collection: {doc}",
            );
        }
        assert_eq!(
            published,
            BTreeMap::from([
                // Bindings 0, 1 and 2 each captured `RUN` documents of one key,
                // valued 1, 2 and 3. A capture combines *associatively*
                // (`doc::combine`'s `is_full == false`), so a key group drains as
                // its first document plus the reduction of the rest: two out for
                // `RUN` in, summing to the binding's own value times the run.
                (0, (2, RUN)),
                (1, (2, 2 * RUN)),
                (2, (2, 3 * RUN)),
                // Collection-0's other bindings each captured one document, which
                // stays its own however much machinery the four of them share.
                (3, (1, 10)),
                (6, (1, 100)),
                (9, (1, 1000)),
            ]),
        );
        assert_eq!(stats.len(), 1);

        // One inference event per collection, each naming the last binding to
        // update it -- including collection-2, whose shape merges binding 5's
        // sourced schema with binding 2's document, applied in that order.
        assert_eq!(
            inferences
                .iter()
                .map(|(collection, binding, _schema)| (collection.as_str(), *binding))
                .collect::<Vec<_>>(),
            [
                ("acmeCo/collection-0", Some(9)),
                ("acmeCo/collection-1", Some(1)),
                ("acmeCo/collection-2", Some(2)),
            ],
        );
        assert!(
            inferences[2].2.contains("sourced_only"),
            "collection-2's inference merged the sourced schema: {}",
            inferences[2].2,
        );
    }

    /// Backfill lifecycle across a restart, for a target written by one binding
    /// and for one written by two:
    ///
    /// - A Begin persists the active backfill -- unless its target is fan-in,
    ///   where the Begin is suppressed and absence from `active_backfills` *is*
    ///   the suppression decision, so recovery replays nothing and no
    ///   `truncated-at` label or marker intent is ever built.
    /// - A fresh session recovers what was persisted, a Complete removes it, and
    ///   a Complete for a never-begun binding is an orphaned no-op. A suppressed
    ///   backfill converges by that same orphaned path.
    ///
    /// `truncated_at` is 0 -- the recording publisher's no-op marker clock -- and
    /// each backfill message is sealed by its own terminating Checkpoint.
    #[tokio::test]
    async fn serve_backfill_lifecycle() {
        // The fan-in task's two bindings target one collection; the default
        // task's target two, so neither of its targets is fan-in.
        let mk_fan_in_task = || {
            fixture::task(
                &[
                    ("test/collectionA", "stateA", "/_meta/uuid"),
                    ("test/collectionA", "stateB", ""),
                ],
                b"{}",
                true,
            )
        };
        let cases: [(&str, fn() -> Task, BTreeMap<u32, u64>); 2] = [
            (
                "distinct targets",
                || mk_task(true),
                BTreeMap::from([(0, 0)]),
            ),
            ("fan-in target", mk_fan_in_task, BTreeMap::new()),
        ];

        for (case, mk_task, after_begin) in cases {
            // A Begin persists nothing exactly when its target is fan-in.
            assert_eq!(
                mk_task().targets.iter().any(|target| target.fan_in),
                after_begin.is_empty(),
                "{case}: fixture",
            );

            let (db, recover) = run_capture_session(
                mk_task(),
                crate::shard::RocksDB::open(None).await.unwrap(),
                BTreeMap::new(),
                crate::TracingLogger,
                vec![backfill_begin(0), checkpoint(br#"{"cursor":"1"}"#)],
                1,
            )
            .await
            .recover()
            .await;

            assert_eq!(recover.active_backfills, after_begin, "{case}: after begin");

            // Binding 0's Complete removes what its Begin persisted (or takes the
            // orphaned path, when the Begin was suppressed). Binding 1 never began
            // either way, so its Complete is always orphaned.
            let (_db, recover) = run_capture_session(
                mk_task(),
                db,
                recover.active_backfills,
                crate::TracingLogger,
                vec![
                    backfill_complete(0),
                    checkpoint(br#"{"cursor":"2"}"#),
                    backfill_complete(1),
                    checkpoint(br#"{"cursor":"3"}"#),
                ],
                2,
            )
            .await
            .recover()
            .await;

            assert_eq!(
                recover.active_backfills,
                BTreeMap::new(),
                "{case}: after complete",
            );
            assert_eq!(recover.connector_state_json.as_ref(), br#"{"cursor":"3"}"#);
        }
    }

    /// Truncated-at labels belong to shard zero alone, and the two shards which
    /// start a session already holding `active_backfills` part ways on that:
    ///
    /// - Shard zero recovered mid-backfill, and must re-apply its labels on the
    ///   first `ApplyTruncatedLabels` rather than skip -- the restart case a
    ///   false `labels_dirty` seed would silently break.
    /// - A non-zero shard inherited the same backfills through a mid-backfill
    ///   split, and must not apply them at all: it never sees the
    ///   BackfillComplete which would clear them.
    #[tokio::test]
    async fn recovered_backfills_apply_labels_on_shard_zero_only() {
        for is_shard_zero in [true, false] {
            let task = std::sync::Arc::new(mk_task(true));
            let (mut actor, _connector_rx) = mk_actor(
                &task,
                BTreeMap::from([(0u32, 5u64)]), // recovered, or split-inherited
                crate::shard::RocksDB::open(None).await.unwrap(),
                is_shard_zero,
                crate::TracingLogger,
                RecordingPublisher::default(),
            );

            let mut accumulator = crate::Accumulator::new(task.combine_spec().unwrap()).unwrap();
            actor
                .dispatch(fsm::Action::ApplyTruncatedLabels, &mut accumulator)
                .unwrap();

            assert_eq!(
                actor.labels_apply_fut.is_some(),
                is_shard_zero,
                "labels applied by a shard with is_shard_zero={is_shard_zero}",
            );
        }
    }

    /// `observe_throttle` parks at most one split for a due journal, never
    /// replaces an in-flight split, and is suppressed by cooldown and by the
    /// terminal `ignore` set.
    #[tokio::test]
    async fn observe_throttle_split_dispatch() {
        let spec = flow::CollectionSpec {
            name: "test/collectionA".to_string(),
            partition_template: Some(proto_gazette::broker::JournalSpec {
                name: "test/collectionA/v1".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let (mut actor, _connector_rx) = mk_actor(
            &std::sync::Arc::new(mk_task(true)),
            BTreeMap::new(),
            crate::shard::RocksDB::open(None).await.unwrap(),
            true, // is_shard_zero
            crate::TracingLogger,
            crate::JournalPublisher::new_test_real([&spec]),
        );

        // Seed a policy under which the observed journal is immediately due.
        const J: &str = "test/collectionA/v1/pivot=00";
        actor.split_policy = crate::shard::split_policy::SplitPolicy::with_config(
            crate::shard::split_policy::Config {
                threshold: -1.0,
                min_observation_span: Duration::ZERO,
                ..Default::default()
            },
        );
        actor
            .split_policy
            .observe(J, true, std::time::Instant::now());

        // Exactly one split is dispatched and parked for the due journal.
        actor.observe_throttle();
        assert!(actor.split_fut.is_some());

        // An in-flight split is never replaced: park a sentinel, re-evaluate
        // (J is still due), and observe the sentinel itself resolve.
        actor.split_fut = Some(
            async {
                (
                    "sentinel".to_string(),
                    Ok(publisher::SplitOutcome::Transient),
                )
            }
            .boxed(),
        );
        actor.observe_throttle();
        let (journal, _outcome) = actor.split_fut.take().unwrap().await;
        assert_eq!(journal, "sentinel");

        // A completed split puts J in cooldown: nothing re-dispatches.
        crate::shard::finish_split(
            &mut actor.split_policy,
            J,
            Ok(publisher::SplitOutcome::Split),
            std::time::Instant::now(),
        );
        actor.observe_throttle();
        assert!(actor.split_fut.is_none());

        // An ignored journal never re-triggers, even under fresh pressure.
        actor.split_policy.ignore(J);
        actor
            .split_policy
            .observe(J, true, std::time::Instant::now());
        actor.observe_throttle();
        assert!(actor.split_fut.is_none());
    }

    #[tokio::test]
    async fn backfill_message_rejects_out_of_range_binding() {
        // `mk_task(true)` has two bindings (indices 0 and 1); index 2 is out of
        // range. An out-of-range binding from the connector must surface as a
        // clean error rather than panicking downstream in publisher indexing.
        let (actor, _connector_rx) = mk_actor(
            &std::sync::Arc::new(mk_task(true)),
            BTreeMap::new(),
            crate::shard::RocksDB::open(None).await.unwrap(),
            true, // is_shard_zero
            crate::TracingLogger,
            RecordingPublisher::default(),
        );

        let mut ready = fsm::ConnectorRx::Eof;
        for response in [backfill_begin(2), backfill_complete(2)] {
            let err = actor
                .on_connector_rx(&mut ready, Some(response))
                .unwrap_err();
            assert!(
                err.to_string().contains("out-of-range binding 2"),
                "unexpected error: {err}",
            );
        }
    }

    /// `parse_sourced_schema` resolves a valid closed schema to its binding and
    /// inferred shape, and rejects an out-of-range binding index.
    #[test]
    fn parse_sourced_schema_validates() {
        let task = mk_task(false);

        let (binding, _shape) = parse_sourced_schema(
            response::SourcedSchema {
                binding: 1,
                schema_json: Bytes::from_static(
                    br#"{"type":"object","additionalProperties":false,"properties":{"id":{"type":"string"}},"required":["id"]}"#,
                ),
            },
            &task,
        )
        .unwrap();
        assert_eq!(binding, 1);

        let err = parse_sourced_schema(
            response::SourcedSchema {
                binding: 5,
                schema_json: Bytes::new(),
            },
            &task,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("invalid sourced schema binding 5"), "{err}");
    }
}
