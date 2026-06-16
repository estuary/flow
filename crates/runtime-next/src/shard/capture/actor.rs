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
pub(super) struct Actor {
    // --- Task and IO endpoints, fixed for the session. ---
    // `task` is shared (Arc) so the drain future can hold its own handle.
    task: std::sync::Arc<Task>,
    connector_tx: mpsc::Sender<Request>,
    // Per-session metrics counters.
    metrics: super::Metrics,

    // --- Parked resources: `Some` unless borrowed by an in-flight future. ---
    // RocksDB is parked with its per-binding state keys.
    db: Option<(crate::shard::RocksDB, Vec<String>)>,
    publisher: Option<crate::Publisher>,
    // Inferred per-binding write-shapes. Seeded from prior sessions at
    // construction, parked into the drain future, handed back at session end.
    shapes: Option<Vec<doc::Shape>>,
    // Long-lived per-journal throttle policy, fed once per transaction once the
    // collection appends have flushed.
    split_policy: crate::shard::split_policy::SplitPolicy,
    // Drain inputs staged by a Rotate, consumed by the Drain dispatch.
    drain_input: Option<DrainInput>,

    // --- In-flight IO futures; `None` when idle. ---
    acknowledge_fut: Option<BoxFuture<'static, anyhow::Result<()>>>,
    drain_fut: Option<BoxFuture<'static, anyhow::Result<drain::Output>>>,
    intents_write_fut: Option<BoxFuture<'static, tonic::Result<crate::Publisher>>>,
    persist_fut: Option<BoxFuture<'static, anyhow::Result<(crate::shard::RocksDB, Vec<String>)>>>,
    split_fut: Option<crate::shard::SplitFuture>,
    stats_write_fut:
        Option<BoxFuture<'static, tonic::Result<(crate::Publisher, BTreeMap<String, Bytes>)>>>,

    // --- Hand-offs staged between FSM steps. ---
    // Drain output, staged for `TailDrain`.
    drain_finished: Option<fsm::DrainedCapture>,
    // ACK intents from a completed stats write, staged for `TailWriteStats`.
    pending_ack_intents: BTreeMap<String, Bytes>,
}

/// Drain inputs staged by a Rotate, handed to [`drain::drain_and_publish`]
/// when the Tail reaches its Drain step.
struct DrainInput {
    drainer: doc::combine::Drainer,
    parser: simd_doc::Parser,
}

impl Actor {
    pub fn new(
        binding_state_keys: Vec<String>,
        connector_tx: mpsc::Sender<Request>,
        db: crate::shard::RocksDB,
        metrics: super::Metrics,
        publisher: crate::Publisher,
        shapes: Vec<doc::Shape>,
        task: std::sync::Arc<Task>,
    ) -> Self {
        Self {
            task,
            connector_tx,
            metrics,
            db: Some((db, binding_state_keys)),
            publisher: Some(publisher),
            shapes: Some(shapes),
            split_policy: crate::shard::split_policy::SplitPolicy::new(),
            drain_input: None,
            acknowledge_fut: None,
            drain_fut: None,
            intents_write_fut: None,
            persist_fut: None,
            split_fut: None,
            stats_write_fut: None,
            drain_finished: None,
            pending_ack_intents: BTreeMap::new(),
        }
    }

    #[tracing::instrument(level = "debug", err(Debug, level = "warn"), skip_all)]
    pub async fn serve<R, C>(
        mut self,
        connector_rx: C,
        controller_rx: &mut R,
        mut head: fsm::Head,
        mut tail: fsm::Tail,
    ) -> anyhow::Result<(crate::shard::RocksDB, Vec<doc::Shape>)>
    where
        R: futures::Stream<Item = tonic::Result<proto::Capture>> + Send + Unpin + 'static,
        C: futures::Stream<Item = tonic::Result<Response>> + Send + Unpin + 'static,
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
                now,
                self.persist_fut.is_none(),
                &self.task,
                self.stats_write_fut
                    .is_none()
                    .then_some(&mut self.pending_ack_intents),
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
                    let output : drain::Output = result?;
                    accumulator_idle = Some(output.accumulator);
                    self.publisher = Some(output.publisher);
                    self.shapes = Some(output.shapes);
                    self.drain_finished = Some(output.drained);
                    self.drain_fut = None;
                }
                Some(result) = maybe_fut(&mut self.stats_write_fut) => {
                    let (publisher, ack_intents) = result.map_err(crate::status_to_anyhow)
                        .context("writing capture ops stats document")?;
                    self.publisher = Some(publisher);
                    self.pending_ack_intents = ack_intents;
                    self.stats_write_fut = None;

                    // WriteStats flushed this transaction's collection appends, so
                    // the publisher's per-journal throttle samples are now complete
                    self.observe_throttle();
                }
                Some(result) = maybe_fut(&mut self.persist_fut) => {
                    self.db = Some(result?);
                    self.persist_fut = None;
                }
                Some(result) = maybe_fut(&mut self.acknowledge_fut) => {
                    result?;
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
                // Process controller messages next.
                msg = controller_rx.next() => {
                    Self::on_controller_rx(msg, &mut close_requested, &mut stopping)?;
                },
                // Process new connector messages last.
                msg = connector_rx.next(), if matches!(ready_connector_rx, fsm::ConnectorRx::Pending) => {
                    self.on_connector_rx(&mut ready_connector_rx, msg)?;
                }

                // Lowest priority.
                _ = tokio::time::sleep(wake_after) => {}
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
        let Some(publisher) = self.publisher.as_mut() else {
            return;
        };
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

                let (memtable, alloc, mut doc) =
                    accumulator.parse_json_doc(&doc_json).with_context(|| {
                        format!(
                            "couldn't parse captured document as JSON (target {})",
                            binding_spec.collection_name
                        )
                    })?;

                let uuid_ptr = &binding_spec.document_uuid_ptr;
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
                        )
                        .await
                    }
                    .boxed(),
                );
                true
            }

            fsm::Action::WriteStats { stats } => {
                let mut publisher = self.publisher.take().context("missing capture publisher")?;
                self.stats_write_fut = Some(
                    async move {
                        if !stats.capture.is_empty() {
                            publisher.publish_stats(stats).await?;
                        }
                        publisher.flush().await?;

                        let intents = match publisher.commit_intents() {
                            Some(commit) => {
                                publisher::intents::build_transaction_intents(&[commit])
                            }
                            None => BTreeMap::new(),
                        };

                        Ok((publisher, intents))
                    }
                    .boxed(),
                );
                true
            }

            fsm::Action::Persist { persist } => {
                let (db, binding_state_keys) =
                    self.db.take().context("Persist while RocksDB is busy")?;
                self.persist_fut = Some(
                    async move {
                        let db = db
                            .persist(&persist, &binding_state_keys)
                            .await
                            .context("Persisting capture state")?;
                        Ok((db, binding_state_keys))
                    }
                    .boxed(),
                );
                true
            }

            fsm::Action::Acknowledge { checkpoints } => {
                let connector_tx = self.connector_tx.clone();
                self.acknowledge_fut = Some(
                    async move {
                        connector_tx
                            .send(Request {
                                acknowledge: Some(request::Acknowledge { checkpoints }),
                                ..Default::default()
                            })
                            .await
                            .context("sending connector Acknowledge")
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
            "Captured, SourcedSchema, or Checkpoint",
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
        } else {
            return Err(verify.fail_msg(response));
        };
        Ok(())
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

    let collection_name = &task
        .bindings
        .get(binding as usize)
        .with_context(|| format!("invalid sourced schema binding {binding}"))?
        .collection_name;

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
    use crate::leader::capture::task::Binding;
    use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};

    fn mk_binding(collection_name: &str, state_key: &str, uuid_ptr: &str) -> Binding {
        Binding {
            collection_name: collection_name.to_string(),
            collection_generation_id: models::Id::zero(),
            document_uuid_ptr: json::Pointer::from(uuid_ptr),
            key_extractors: Vec::new(),
            partition_template_name: collection_name.to_string(),
            state_key: state_key.to_string(),
            write_schema_json: Bytes::from_static(b"{}"),
            write_shape: doc::Shape::nothing(),
        }
    }

    fn mk_task(explicit_acknowledgements: bool) -> Task {
        Task {
            // Binding 0 carries a UUID pointer (exercising placeholder injection),
            // binding 1 does not.
            bindings: vec![
                mk_binding("test/collectionA", "stateA", "/_meta/uuid"),
                mk_binding("test/collectionB", "stateB", ""),
            ],
            // Wide thresholds: a transaction closes as soon as the connector
            // idles (its checkpoint sequence completes and no further input is
            // ready), free of policy-driven close timing.
            close_policy: crate::leader::close_policy::Policy::new(Duration::ZERO, Duration::MAX),
            explicit_acknowledgements,
            max_transactions: 0,
            redact_salt: Bytes::new(),
            restart: uuid::Clock::zero(),
            sequence_bytes_limit: 1 << 20,
            shard_ref: ops::ShardRef::default(),
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

    /// Drive `Actor::serve` end-to-end over mpsc channels standing in for the
    /// connector and controller, with a real RocksDB and a preview Publisher.
    ///
    /// The connector emits two Captured documents (into distinct bindings) and a
    /// Checkpoint carrying connector state. The actor accumulates them, closes
    /// the transaction once the connector idles, and runs the full Tail commit:
    /// drain+publish, stats, the committing Persist, Acknowledge, and
    /// WriteIntents. Receiving the connector Acknowledge proves the commit
    /// reached its post-Persist handoff; a controller Stop then drains the Tail
    /// and steps Head to Stop. Asserts the connector saw one acknowledged
    /// checkpoint and that the persisted connector state round-trips from RocksDB.
    #[tokio::test]
    async fn serve_transaction_then_stop() {
        // Actor → connector requests; the test reads as the mock connector.
        let (connector_tx, mut actor_to_conn_rx) = mpsc::channel::<Request>(crate::CHANNEL_BUFFER);
        // Mock connector → actor responses.
        let (conn_resp_tx, conn_resp_rx) =
            mpsc::channel::<tonic::Result<Response>>(crate::CHANNEL_BUFFER);
        // Controller → actor signals.
        let (controller_tx, controller_rx) =
            mpsc::unbounded_channel::<tonic::Result<proto::Capture>>();

        let task = std::sync::Arc::new(mk_task(true));
        // Preview only reads each spec's `name`; a minimal spec per binding suffices.
        let collection_specs: Vec<flow::CollectionSpec> = task
            .bindings
            .iter()
            .map(|b| flow::CollectionSpec {
                name: b.collection_name.clone(),
                ..Default::default()
            })
            .collect();
        let publisher = crate::Publisher::new_preview(collection_specs.iter());
        let shapes = task.binding_shapes_by_index(Default::default());

        let actor = Actor::new(
            vec!["stateA".to_string(), "stateB".to_string()],
            connector_tx,
            crate::shard::RocksDB::open(None).await.unwrap(),
            super::super::Metrics::new("test/shard"),
            publisher,
            shapes,
            task,
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

        // One checkpoint sequence: two documents, then a connector-state checkpoint.
        conn_resp_tx
            .send(captured(0, br#"{"id":"a0"}"#))
            .await
            .unwrap();
        conn_resp_tx
            .send(captured(1, br#"{"id":"b0"}"#))
            .await
            .unwrap();
        conn_resp_tx
            .send(checkpoint(br#"{"cursor":"lsn-9"}"#))
            .await
            .unwrap();

        // The Acknowledge follows Drain → WriteStats → Persist, so its receipt
        // proves the transaction committed.
        let ack = actor_to_conn_rx.recv().await.unwrap();
        assert_eq!(ack.acknowledge.unwrap().checkpoints, 1);

        // Gracefully stop: the Tail finishes and Head steps to Stop. The connector
        // response channel stays open (`conn_resp_tx` is held) so the connector
        // never EOFs out from under the still-running session.
        controller_tx
            .send(Ok(proto::Capture {
                stop: Some(proto::Stop {}),
                ..Default::default()
            }))
            .unwrap();

        let (db, shapes) = serve.await.unwrap().unwrap();
        assert_eq!(shapes.len(), 2); // One inferred shape handed back per binding.

        // The committing Persist durably recorded the connector state.
        let (_db, recover) = db.scan(Vec::new()).await.unwrap();
        assert_eq!(
            recover.connector_state_json.as_ref(),
            br#"{"cursor":"lsn-9"}"#
        );
    }

    /// `observe_throttle` parks at most one split for a due journal, never
    /// replaces an in-flight split, and is suppressed by cooldown and by the
    /// terminal `ignore` set.
    #[tokio::test]
    async fn observe_throttle_split_dispatch() {
        let (connector_tx, _actor_to_conn_rx) = mpsc::channel::<Request>(crate::CHANNEL_BUFFER);
        let task = std::sync::Arc::new(mk_task(true));

        let spec = flow::CollectionSpec {
            name: "test/collectionA".to_string(),
            partition_template: Some(proto_gazette::broker::JournalSpec {
                name: "test/collectionA/v1".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let publisher = crate::Publisher::new_test_real([&spec]);
        let shapes = task.binding_shapes_by_index(Default::default());

        let mut actor = Actor::new(
            vec!["stateA".to_string()],
            connector_tx,
            crate::shard::RocksDB::open(None).await.unwrap(),
            super::super::Metrics::new("test/shard"),
            publisher,
            shapes,
            task,
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
