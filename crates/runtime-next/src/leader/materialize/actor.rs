use super::{Task, fsm};
use crate::proto;
use anyhow::Context;
use bytes::Bytes;
use futures::stream::{BoxStream, FuturesUnordered};
use futures::{FutureExt, StreamExt, future::BoxFuture};
use proto_gazette::{consumer, uuid};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::sync::mpsc;

/// Actor leads transactions of an established materialization task session.
pub struct Actor {
    // Client used for trigger dispatch.
    http_client: reqwest::Client,
    // Future for an in-flight ACK intents write, if any.
    intents_write_fut: Option<BoxFuture<'static, tonic::Result<crate::Publisher>>>,
    // Optional full Frontier and Checkpoint, used for V1 rollback support.
    legacy_checkpoint: Option<(shuffle::Frontier, consumer::Checkpoint)>,
    // Per-task metrics counters and gauges.
    metrics: super::Metrics,
    // Publisher for stats and ACK intents, parked while no async operation is in-flight.
    parked_publisher: Option<crate::Publisher>,
    // ACK intents to persist and append at later transaction stages.
    pending_ack_intents: BTreeMap<String, Bytes>,
    // One channel to each shard for synchronously sending it messages.
    // We use UnboundedSender because Actor never "pumps" messages to shards:
    // it follows a strict request/response pattern.
    shard_tx: Vec<mpsc::UnboundedSender<tonic::Result<proto::Materialize>>>,
    // Future for an in-flight stats flush, if any, yielding ACK intents.
    stats_write_fut:
        Option<BoxFuture<'static, tonic::Result<(crate::Publisher, BTreeMap<String, Bytes>)>>>,
    // Task being executed by this actor.
    task: Task,
    // Future for an in-flight trigger dispatch, if any.
    trigger_fut: Option<BoxFuture<'static, anyhow::Result<()>>>,
}

impl Actor {
    pub fn new(
        http_client: reqwest::Client,
        legacy_checkpoint: Option<shuffle::Frontier>,
        metrics: super::Metrics,
        publisher: crate::Publisher,
        shard_tx: Vec<mpsc::UnboundedSender<tonic::Result<proto::Materialize>>>,
        task: Task,
    ) -> Self {
        Self {
            http_client,
            intents_write_fut: None,
            legacy_checkpoint: legacy_checkpoint.map(|f| (f, consumer::Checkpoint::default())),
            metrics,
            parked_publisher: Some(publisher),
            pending_ack_intents: BTreeMap::new(),
            shard_tx,
            stats_write_fut: None,
            task,
            trigger_fut: None,
        }
    }

    #[tracing::instrument(level = "debug", err(Debug, level = "warn"), skip_all)]
    pub async fn serve(
        &mut self,
        mut head: fsm::Head,
        mut tail: fsm::Tail,
        mut session: shuffle::SessionClient,
        shard_rx: Vec<BoxStream<'static, tonic::Result<proto::Materialize>>>,
    ) -> anyhow::Result<()> {
        service_kit::event!(
            tracing::Level::INFO,
            "leader",
            n_shards = self.task.n_shards,
            "materialize Actor::serve started",
        );
        assert_eq!(self.task.n_shards, shard_rx.len());
        assert_eq!(self.task.n_shards, self.shard_tx.len());

        // Build a stream of receive futures for each shard.
        let mut shard_rx: FuturesUnordered<_> = shard_rx
            .into_iter()
            .enumerate()
            .map(next_shard_rx)
            .collect();

        // Per-binding absolute measure, into which deltas are reduced.
        let mut binding_bytes_behind = vec![0; self.task.binding_collection_names.len()];
        // We keep exactly one NextCheckpoint request in flight while idle.
        let mut checkpoint_requested = false;
        // When true, Head should close its current open transaction ASAP.
        let mut close_requested = false;
        // Iteration counter for the per-loop trace event.
        let mut loop_count: u64 = 0;
        // Monotonic Clock which is ticked on loop iterations, and updated on IO.
        let mut now = now_clock();
        // When Some, a Frontier that's ready to extend a transaction.
        let mut ready_frontier: Option<shuffle::Frontier> = None;
        // When Some, a message from a shard that's ready to consume.
        let mut ready_shard_rx = None;
        // When true, the topology should gracefully exit.
        let mut stopping = false;
        // Transactions completed in this task session, for preview harness limits.
        let mut transactions_completed = 0usize;

        while !matches!(head, fsm::Head::Stop) {
            loop_count += 1;
            now.tick(); // Strictly increasing iteration values.

            tracing::trace!(
                loop_count,
                close_requested,
                head = ?head,
                intents_in_flight = self.intents_write_fut.is_some(),
                pending_ack_intents = self.pending_ack_intents.len(),
                ready_frontier = ready_frontier.is_some(),
                ready_shard_rx = ready_shard_rx.is_some(),
                stats_in_flight = self.stats_write_fut.is_some(),
                stopping,
                tail = ?tail,
                trigger_in_flight = self.trigger_fut.is_some(),
                "leader materialize Actor::serve iteration"
            );

            let action: fsm::Action;
            let prev_kind = tail.kind();
            (action, tail) = tail.step(
                self.intents_write_fut.is_none(),
                now,
                &mut ready_shard_rx,
                stopping,
                &self.task,
                self.trigger_fut.is_some(),
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
            let tail_wake_after = self.dispatch(action)?;

            let action: fsm::Action;
            let prev_kind = head.kind();
            (action, head) = head.step(
                &mut binding_bytes_behind,
                &mut close_requested,
                &mut self.legacy_checkpoint,
                now,
                &mut ready_frontier,
                &mut ready_shard_rx,
                self.stats_write_fut
                    .is_none()
                    .then_some(&mut self.pending_ack_intents),
                stopping,
                &mut tail,
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
                fsm::Action::Rotate { pending } => {
                    assert!(matches!(tail, fsm::Tail::Done(_)));
                    self.metrics.transactions.increment(1);
                    transactions_completed += 1;

                    if self.task.max_transactions == 0 || stopping {
                        // Pass
                    } else if transactions_completed >= self.task.max_transactions as usize {
                        service_kit::event!(
                            tracing::Level::INFO,
                            "head",
                            transactions_completed = transactions_completed,
                            max_transactions = self.task.max_transactions,
                            "materialize transaction limit reached; stopping gracefully",
                        );
                        stopping = true;
                    }
                    tail = fsm::Tail::Begin(fsm::TailBegin { pending });

                    Duration::ZERO
                }
                action => self.dispatch(action)?,
            };
            let wake_after = std::cmp::min(head_wake_after, tail_wake_after);

            // If `head` and `tail` are awaiting IO and `ready_shard_rx` was not
            // consumed by either, then it was unexpected and is a protocol error.
            if let Some((shard_index, msg)) = ready_shard_rx.as_ref()
                && !wake_after.is_zero()
            {
                anyhow::bail!(
                    "unexpected message {msg:?} from {} (index {shard_index})",
                    self.task.peers[*shard_index],
                );
            }

            // Keep one NextCheckpoint in flight whenever we can accept a frontier.
            if ready_frontier.is_none() && !checkpoint_requested {
                session.request_checkpoint();
                checkpoint_requested = true;
            }

            tokio::select! {
                biased;

                // Prioritize completions of leader IO first.
                Some(result) = maybe_fut(&mut self.stats_write_fut) => {
                    let (publisher, intents) = result.map_err(crate::status_to_anyhow)
                        .context("writing ops stats document")?;

                    self.parked_publisher = Some(publisher);
                    self.pending_ack_intents = intents;
                    self.stats_write_fut = None;

                    service_kit::event!(
                        tracing::Level::DEBUG,
                        "leader",
                        "completed ops stats publish",
                    );
                    // Having just written stats, we know this measure is fresh.
                    let total: i64 = binding_bytes_behind.iter().copied().sum();
                    self.metrics.bytes_behind.set(total as f64);
                }
                Some(result) = maybe_fut(&mut self.intents_write_fut) => {
                    let publisher = result.map_err(crate::status_to_anyhow)
                        .context("writing ACK intents")?;

                    self.parked_publisher = Some(publisher);
                    self.intents_write_fut = None;

                    service_kit::event!(
                        tracing::Level::DEBUG,
                        "leader",
                        "completed ACK intents write",
                    );
                }
                Some(result) = maybe_fut(&mut self.trigger_fut) => {
                    () = result?;
                    self.trigger_fut = None;

                    service_kit::event!(
                        tracing::Level::DEBUG,
                        "leader",
                        "completed trigger dispatch",
                    );
                }
                // Process shard messages next.
                Some((shard_index, msg, rx)) = shard_rx.next() => {
                    if let Some(msg) = self.on_shard_rx(
                        &mut close_requested,
                        &mut stopping,
                        shard_index,
                        msg,
                    )? {
                        ready_shard_rx = Some((shard_index, msg));
                    }
                    shard_rx.push(next_shard_rx((shard_index, rx)));
                }
                // Receive a requested NextCheckpoint frontier.
                result = session.recv_checkpoint(), if checkpoint_requested => {
                    let frontier = result?;
                    let (journals, journal_producers, bytes_read_delta, bytes_behind_delta) = frontier.measures();
                    let unresolved_hints = frontier.unresolved_hints;

                    ready_frontier = Some(frontier);
                    checkpoint_requested = false;

                    service_kit::event!(
                        tracing::Level::DEBUG,
                        "leader",
                        bytes_behind_delta,
                        bytes_read_delta,
                        journal_producers,
                        journals,
                        unresolved_hints,
                        "received Frontier from shuffle Session",
                    );
                }

                // Lowest priority.
                _ = tokio::time::sleep(wake_after) => {}
            }

            if !wake_after.is_zero() {
                now.update(now_clock()); // Resync after blocking IO.
            }
        }

        service_kit::event!(
            tracing::Level::INFO,
            "leader",
            "materialize Actor::serve exiting; broadcasting Stopped",
        );

        // Broadcast L:Stopped. Each shard, upon observing it, removes its shuffle
        // log segment files — releasing any disk back-pressure held by the
        // co-located shuffle Log RPC so the Session topology can drain.
        for tx in &self.shard_tx {
            let _ = tx.send(Ok(proto::Materialize {
                stopped: Some(proto::Stopped {}),
                ..Default::default()
            }));
        }

        // Close the shuffle Session, blocking until the entire
        // Session→Slice→Log topology has drained to EOF and exited. This depends
        // on the shard segment removals above to release disk back-pressure.
        () = session
            .close()
            .await
            .context("closing shuffle Session on Stop")?;

        Ok(())
    }

    /// Execute the outgoing-IO primitive for an Action.
    #[tracing::instrument(level = "trace", fields(action = ?action), skip_all)]
    fn dispatch(&mut self, action: fsm::Action) -> anyhow::Result<Duration> {
        match action {
            fsm::Action::Rotate { .. } => unreachable!("never dispatched"),
            fsm::Action::Error { error } => return Err(error),

            fsm::Action::Idle => (),
            fsm::Action::PollAgain => return Ok(Duration::ZERO),
            fsm::Action::Sleep { wake_after } => return Ok(wake_after),

            fsm::Action::Load { frontier } => {
                service_kit::event!(tracing::Level::DEBUG, "shard", "broadcasting L:Load");
                self.broadcast(proto::Materialize {
                    load: Some(proto::materialize::Load {
                        frontier: Some(frontier.encode()),
                    }),
                    ..Default::default()
                });
            }

            fsm::Action::Flush { connector_patches } => {
                service_kit::event!(tracing::Level::DEBUG, "shard", "broadcasting L:Flush");
                self.broadcast(proto::Materialize {
                    flush: Some(proto::materialize::Flush {
                        connector_patches_json: connector_patches,
                    }),
                    ..Default::default()
                });
            }

            fsm::Action::Store => {
                service_kit::event!(tracing::Level::DEBUG, "shard", "broadcasting L:Store");
                self.broadcast(proto::Materialize {
                    store: Some(proto::materialize::Store {}),
                    ..Default::default()
                });
            }

            fsm::Action::StartCommit {
                connector_checkpoint,
                connector_patches,
            } => {
                service_kit::event!(tracing::Level::DEBUG, "shard", "broadcasting L:StartCommit");
                self.broadcast(proto::Materialize {
                    start_commit: Some(proto::materialize::StartCommit {
                        connector_checkpoint: Some(connector_checkpoint),
                        connector_patches_json: connector_patches,
                    }),
                    ..Default::default()
                });
            }

            fsm::Action::Acknowledge { connector_patches } => {
                service_kit::event!(tracing::Level::DEBUG, "shard", "broadcasting L:Acknowledge");
                self.broadcast(proto::Materialize {
                    acknowledge: Some(proto::materialize::Acknowledge {
                        connector_patches_json: connector_patches,
                    }),
                    ..Default::default()
                });
            }

            fsm::Action::Persist { persist } => {
                service_kit::event!(tracing::Level::DEBUG, "shard", "sending L:Persist");
                let _ = self.shard_tx[0].send(Ok(proto::Materialize {
                    persist: Some(persist),
                    ..Default::default()
                }));
            }

            fsm::Action::WriteStats { stats } => {
                service_kit::event!(
                    tracing::Level::DEBUG,
                    "leader",
                    "starting ops stats publish"
                );
                let mut publisher = self
                    .parked_publisher
                    .take()
                    .expect("publisher owned at WriteOpsStats dispatch");

                self.stats_write_fut = Some(
                    async move {
                        // Resync the leader publisher clock to wall-clock at the
                        // start of this transaction's stats + ACK publish stream.
                        publisher.update_clock();

                        () = publisher.publish_stats(stats).await?;

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
            }

            fsm::Action::WriteIntents { ack_intents } => {
                service_kit::event!(
                    tracing::Level::DEBUG,
                    "leader",
                    "starting ACK intents write"
                );
                let mut publisher = self
                    .parked_publisher
                    .take()
                    .expect("publisher owned at WriteIntents dispatch");

                self.intents_write_fut = Some(
                    async move {
                        () = publisher.write_intents(ack_intents).await?;
                        Ok(publisher)
                    }
                    .boxed(),
                );
            }

            fsm::Action::CallTrigger {
                triggers,
                trigger_params,
            } => {
                service_kit::event!(
                    tracing::Level::DEBUG,
                    "leader",
                    "starting trigger execution"
                );
                let variables: models::TriggerVariables =
                    serde_json::from_slice(&trigger_params)
                        .context("decoding trigger_variables JSON")?;
                let client = self.http_client.clone();

                self.trigger_fut = Some(
                    async move {
                        // TODO(johnny): Periodic writes into task ops logs if it takes a while.
                        super::triggers::fire_pending_triggers(&triggers, &variables, &client).await
                    }
                    .boxed(),
                );
            }
        }

        // All actions except for Sleep are blocking (they start IO we must
        // await before usefully re-polling the FSMs).
        Ok(crate::ACTOR_TICK_INTERVAL)
    }

    /// Receive a message from a shard. Returns the message for the
    /// FSM to consume, or `None` if this was a control message (Stop)
    /// the Actor handled itself.
    fn on_shard_rx(
        &self,
        close_requested: &mut bool,
        stopping: &mut bool,
        shard_index: usize,
        result: Option<tonic::Result<proto::Materialize>>,
    ) -> anyhow::Result<Option<proto::Materialize>> {
        let verify = crate::verify(
            "Materialize",
            "actor message",
            &self.task.peers[shard_index],
        );
        let msg = verify.not_eof(result)?;

        if matches!(msg.stop, Some(proto::Stop {})) {
            *stopping = true;
            return Ok(None);
        } else if matches!(msg.close_now, Some(proto::CloseNow {})) {
            *close_requested = true;
            return Ok(None);
        }

        let kind = if msg.loaded.is_some() {
            "L:Loaded"
        } else if msg.flushed.is_some() {
            "L:Flushed"
        } else if msg.stored.is_some() {
            "L:Stored"
        } else if msg.started_commit.is_some() {
            "L:StartedCommit"
        } else if msg.acknowledged.is_some() {
            "L:Acknowledged"
        } else if msg.persisted.is_some() {
            "L:Persisted"
        } else if msg.opened.is_some() {
            "L:Opened"
        } else if msg.recover.is_some() {
            "L:Recover"
        } else if msg.stopped.is_some() {
            "L:Stopped"
        } else {
            "(other)"
        };
        service_kit::event!(
            tracing::Level::DEBUG,
            "shard",
            shard_index,
            kind,
            "received from shard",
        );

        Ok(Some(msg))
    }

    /// Synchronously fan out a single leader message to every shard.
    fn broadcast(&self, msg: proto::Materialize) {
        let (head, tail) = self.shard_tx.split_first().unwrap();

        for tx in tail {
            let _ = tx.send(Ok(msg.clone()));
        }
        let _ = head.send(Ok(msg)); // Avoid a clone (single-shard common case).
    }
}

fn now_clock() -> uuid::Clock {
    let now = tokens::now();
    uuid::Clock::from_unix(now.timestamp() as u64, now.timestamp_subsec_nanos())
}

async fn maybe_fut<T>(opt: &mut Option<BoxFuture<'static, T>>) -> Option<T> {
    match opt.as_mut() {
        Some(fut) => Some(fut.await),
        None => std::future::pending().await,
    }
}

async fn next_shard_rx(
    (shard_index, mut rx): (usize, BoxStream<'static, tonic::Result<proto::Materialize>>),
) -> (
    usize,
    Option<tonic::Result<proto::Materialize>>,
    BoxStream<'static, tonic::Result<proto::Materialize>>,
) {
    let msg = rx.next().await;
    (shard_index, msg, rx)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_actor(
        n_shards: usize,
    ) -> (
        Actor,
        Vec<mpsc::UnboundedReceiver<tonic::Result<proto::Materialize>>>,
    ) {
        let mut shard_tx = Vec::with_capacity(n_shards);
        let mut rxs = Vec::with_capacity(n_shards);
        for _ in 0..n_shards {
            let (tx, rx) = mpsc::unbounded_channel();
            shard_tx.push(tx);
            rxs.push(rx);
        }
        let task = Task {
            binding_collection_names: vec!["test/collection".to_string()],
            binding_journal_read_suffixes: vec!["pivot=00".to_string()],
            close_policy: super::super::close_policy::Policy::new(Duration::ZERO, Duration::MAX),
            connector_image: String::new(),
            max_transactions: 0,
            n_shards,
            peers: (0..n_shards).map(|i| format!("shard-{i}")).collect(),
            shard_ref: ops::ShardRef::default(),
            triggers: None,
        };
        let actor = Actor::new(
            reqwest::Client::new(),
            None,
            super::super::Metrics::new("test/task/shard"),
            crate::Publisher::new_preview([]),
            shard_tx,
            task,
        );
        (actor, rxs)
    }

    #[test]
    fn broadcast_fans_out_to_every_shard() {
        let (actor, mut rxs) = mk_actor(3);
        let msg = proto::Materialize {
            store: Some(proto::materialize::Store {}),
            ..Default::default()
        };
        actor.broadcast(msg);

        for (i, rx) in rxs.iter_mut().enumerate() {
            let got = rx
                .try_recv()
                .unwrap_or_else(|_| panic!("shard {i} should have received broadcast"))
                .unwrap();
            assert!(got.store.is_some(), "shard {i}");
            assert!(rx.try_recv().is_err(), "shard {i} should only get one msg");
        }
    }

    #[test]
    fn broadcast_single_shard_works() {
        // Single-shard path uses split_first head/tail; verify nothing odd at N=1.
        let (actor, mut rxs) = mk_actor(1);
        actor.broadcast(proto::Materialize {
            store: Some(proto::materialize::Store {}),
            ..Default::default()
        });
        rxs[0].try_recv().unwrap().unwrap();
    }

    #[test]
    fn on_shard_rx_dispatches_by_message_kind() {
        // Outcome: Ok(None) consumed control flag, Ok(Some) data, Err message.
        enum Want {
            Consumed { stopping: bool, close: bool },
            Passthrough,
            Err(&'static str),
        }
        use Want::*;

        let cases: &[(usize, _, Want)] = &[
            (
                1,
                Some(Ok(proto::Materialize {
                    stop: Some(proto::Stop {}),
                    ..Default::default()
                })),
                Consumed {
                    stopping: true,
                    close: false,
                },
            ),
            (
                0,
                Some(Ok(proto::Materialize {
                    close_now: Some(proto::CloseNow {}),
                    ..Default::default()
                })),
                Consumed {
                    stopping: false,
                    close: true,
                },
            ),
            (
                0,
                Some(Ok(proto::Materialize {
                    loaded: Some(proto::materialize::Loaded::default()),
                    ..Default::default()
                })),
                Passthrough,
            ),
            // EOF mentions the shard name to localize the failure.
            (2, None, Err("unexpected EOF")),
            (2, None, Err("shard-2")),
            (
                1,
                Some(Result::<_, _>::Err(tonic::Status::aborted("peer aborted"))),
                Err("peer aborted"),
            ),
        ];

        for (shard_index, input, want) in cases {
            let (actor, _rxs) = mk_actor(3);
            let mut close_requested = false;
            let mut stopping = false;
            let result = actor.on_shard_rx(
                &mut close_requested,
                &mut stopping,
                *shard_index,
                input.clone(),
            );
            match want {
                Consumed { stopping: s, close } => {
                    assert!(result.as_ref().unwrap().is_none());
                    assert_eq!(stopping, *s);
                    assert_eq!(close_requested, *close);
                }
                Passthrough => {
                    let msg = result.unwrap().expect("passthrough");
                    assert!(msg.loaded.is_some());
                    assert!(!stopping && !close_requested);
                }
                Err(needle) => {
                    let err = result.unwrap_err();
                    let s = format!("{err:?}");
                    assert!(s.contains(needle), "missing {needle:?} in {s}");
                }
            }
        }
    }
}
