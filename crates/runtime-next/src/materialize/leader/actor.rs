use super::{Task, fsm};
use crate::proto;
use anyhow::Context;
use bytes::Bytes;
use futures::stream::{BoxStream, FuturesUnordered};
use futures::{FutureExt, StreamExt, future::BoxFuture};
use proto_gazette::{consumer, uuid};
use std::collections::BTreeMap;
use tokio::sync::mpsc;

/// Actor leads transactions of an established materialization task session.
pub struct Actor {
    // Client used for trigger dispatch.
    http_client: reqwest::Client,
    // Future for an in-flight ACK intents write, if any.
    intents_write_fut: Option<BoxFuture<'static, tonic::Result<crate::Publisher>>>,
    // Optional full Frontier and Checkpoint, used for V1 rollback support.
    legacy_checkpoint: Option<(shuffle::Frontier, consumer::Checkpoint)>,
    // Publisher for stats and ACK intents, parked while no async operation is in-flight.
    parked_publisher: Option<crate::Publisher>,
    // ACK intents to persist and append at later transaction stages.
    pending_ack_intents: BTreeMap<String, Bytes>,
    // One channel to each shard, for sending messages to the shard.
    // We use UnboundedSender because Actor never "pumps" messages to shards:
    // it follows a strict request-response pattern, where requests may be emitted
    // as a run of gRPC messages but have a bounded scope. UnboundedSender lets us
    // ignore details of waiting for send capacity and model sends as synchronous.
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
        publisher: crate::Publisher,
        shard_tx: Vec<mpsc::UnboundedSender<tonic::Result<proto::Materialize>>>,
        task: Task,
    ) -> Self {
        Self {
            http_client,
            intents_write_fut: None,
            legacy_checkpoint: legacy_checkpoint.map(|f| (f, consumer::Checkpoint::default())),
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
        session: shuffle::SessionClient,
        shard_rx: Vec<BoxStream<'static, tonic::Result<proto::Materialize>>>,
    ) -> anyhow::Result<()> {
        tracing::info!(self.task.n_shards, "materialize actor started");
        assert_eq!(self.task.n_shards, shard_rx.len());
        assert_eq!(self.task.n_shards, self.shard_tx.len());

        // Build a stream of receive futures for each shard.
        let mut shard_rx: FuturesUnordered<_> = shard_rx
            .into_iter()
            .enumerate()
            .map(next_shard_rx)
            .collect();

        // Build a stream of frontier checkpoints from the shuffle Session.
        let frontier_rx = futures::stream::unfold(session, |mut session| async {
            let result = session.next_checkpoint().await;
            Some((result, session))
        });
        let mut frontier_rx = std::pin::pin!(frontier_rx);

        let mut binding_bytes_behind = vec![0; self.task.binding_collection_names.len()];
        let mut now = now_clock();
        let mut ready_frontier = None;
        let mut ready_shard_rx = None;
        let mut stopping = false;

        loop {
            let mut wake_after = crate::ACTOR_TICK_INTERVAL;
            now.tick();

            // Drive `tail` to idle.
            let action: fsm::Action;
            (action, tail) = tail.step(
                self.intents_write_fut.is_none(),
                now,
                &mut ready_shard_rx,
                &self.task,
                self.trigger_fut.is_some(),
            );

            match action {
                fsm::Action::Idle => (),
                fsm::Action::Sleep { .. } => unreachable!("Tail does not emit Sleep"),
                fsm::Action::Rotate { .. } => unreachable!("Tail does not emit Rotate"),
                action => {
                    self.dispatch(action)?;
                    continue;
                }
            }

            // Drive `head` to idle or stop.
            let action: fsm::Action;
            (action, head) = head.step(
                &mut binding_bytes_behind,
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

            match action {
                fsm::Action::Idle => (),
                fsm::Action::Sleep { wake_after: w } => wake_after = w,
                fsm::Action::Rotate { pending } => {
                    assert!(matches!(tail, fsm::Tail::Done(_)));
                    tail = fsm::Tail::Begin(fsm::TailBegin { pending });
                    continue;
                }
                action => {
                    self.dispatch(action)?;
                    continue;
                }
            }

            if matches!(head, fsm::Head::Stop) {
                break;
            }

            // If `ready_shard_rx` was not consumed by either `head` or `tail`,
            // then it was unexpected and is a protocol error.
            if let Some((shard_index, msg)) = ready_shard_rx.take() {
                anyhow::bail!(
                    "unexpected message {msg:?} from {} (index {shard_index})",
                    self.task.peers[shard_index],
                );
            }

            tokio::select! {
                biased;

                // Prioritize RX completions of the leader first, and then shard RX.
                Some(result) = frontier_rx.next(), if ready_frontier.is_none() => {
                    ready_frontier = Some(result?);
                }
                Some(result) = maybe_fut(&mut self.stats_write_fut) => {
                    let (publisher, intents) = result.map_err(crate::status_to_anyhow)
                        .context("writing ops stats document")?;

                    self.parked_publisher = Some(publisher);
                    self.pending_ack_intents = intents;
                    self.stats_write_fut = None;
                }
                Some(result) = maybe_fut(&mut self.intents_write_fut) => {
                    let publisher = result.map_err(crate::status_to_anyhow)
                        .context("writing ACK intents")?;

                    self.parked_publisher = Some(publisher);
                    self.intents_write_fut = None;
                }
                Some(result) = maybe_fut(&mut self.trigger_fut) => {
                    () = result?;
                    self.trigger_fut = None;
                }
                // Finally, process messages from shards.
                Some((shard_index, msg, rx)) = shard_rx.next() => {
                    if let Some(msg) = self.on_shard_rx(&mut stopping, shard_index, msg)? {
                        ready_shard_rx = Some((shard_index, msg));
                    }
                    shard_rx.push(next_shard_rx((shard_index, rx)));
                }

                // Lowest priority.
                _ = tokio::time::sleep(wake_after) => {}
            }

            now.update(now_clock());
        }

        tracing::info!("materialize actor exiting");

        for tx in &self.shard_tx {
            let _ = tx.send(Ok(proto::Materialize {
                stopped: Some(proto::Stopped {}),
                ..Default::default()
            }));
        }

        Ok(())
    }

    /// Execute the outgoing-IO primitive for an Action.
    #[tracing::instrument(level = "trace", fields(action = ?action), skip_all)]
    fn dispatch(&mut self, action: fsm::Action) -> anyhow::Result<()> {
        match action {
            fsm::Action::Idle | fsm::Action::Sleep { .. } | fsm::Action::Rotate { .. } => {
                unreachable!("never reach dispatch");
            }

            fsm::Action::Load { frontier } => {
                tracing::debug!(journals = frontier.journals.len(), "broadcasting L:Load");
                let mut drain = shuffle::frontier::Drain::new();
                drain.start(frontier);

                while let Some(chunk) = drain.next_chunk() {
                    self.broadcast(proto::Materialize {
                        load: Some(proto::materialize::Load {
                            frontier: Some(chunk),
                        }),
                        ..Default::default()
                    });
                }
            }

            fsm::Action::Flush { connector_patches } => {
                tracing::debug!(
                    patches_bytes = connector_patches.len(),
                    "broadcasting L:Flush"
                );
                self.broadcast(proto::Materialize {
                    flush: Some(proto::materialize::Flush {
                        connector_patches_json: connector_patches,
                    }),
                    ..Default::default()
                });
            }

            fsm::Action::Store => {
                tracing::debug!("broadcasting L:Store");
                self.broadcast(proto::Materialize {
                    store: Some(proto::materialize::Store {}),
                    ..Default::default()
                });
            }

            fsm::Action::StartCommit {
                connector_checkpoint,
                connector_patches,
            } => {
                tracing::debug!(
                    patches_bytes = connector_patches.len(),
                    "broadcasting L:StartCommit"
                );

                self.broadcast(proto::Materialize {
                    start_commit: Some(proto::materialize::StartCommit {
                        connector_checkpoint: Some(connector_checkpoint),
                        connector_patches_json: connector_patches,
                    }),
                    ..Default::default()
                });
            }

            fsm::Action::Acknowledge { connector_patches } => {
                tracing::debug!(
                    patches_bytes = connector_patches.len(),
                    "broadcasting L:Acknowledge"
                );
                self.broadcast(proto::Materialize {
                    acknowledge: Some(proto::materialize::Acknowledge {
                        connector_patches_json: connector_patches,
                    }),
                    ..Default::default()
                });
            }

            fsm::Action::Persist { persist } => {
                tracing::debug!("dispatching L:Persist to shard zero");

                let _ = self.shard_tx[0].send(Ok(proto::Materialize {
                    persist: Some(persist),
                    ..Default::default()
                }));
            }

            fsm::Action::WriteStats { stats } => {
                let mut publisher = self
                    .parked_publisher
                    .take()
                    .expect("publisher owned at WriteOpsStats dispatch");

                self.stats_write_fut = Some(
                    async move {
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

            fsm::Action::CallTrigger { trigger_params } => {
                let variables: models::TriggerVariables =
                    serde_json::from_slice(&trigger_params)
                        .context("decoding trigger_variables JSON")?;
                let compiled = self
                    .task
                    .triggers
                    .clone()
                    .expect("CallTrigger fired without compiled_triggers");
                let client = self.http_client.clone();
                self.trigger_fut = Some(
                    async move {
                        super::triggers::fire_pending_triggers(&compiled, &variables, &client).await
                    }
                    .boxed(),
                );
            }
        }

        Ok(())
    }

    /// Receive a message from a shard. Returns the message for the
    /// FSM to consume, or `None` if this was a control message (Stop)
    /// the Actor handled itself.
    fn on_shard_rx(
        &self,
        stopping: &mut bool,
        shard_index: usize,
        result: Option<tonic::Result<proto::Materialize>>,
    ) -> anyhow::Result<Option<proto::Materialize>> {
        let verify = crate::verify(
            "Materialize",
            "message",
            &self.task.peers[shard_index],
            shard_index,
        );
        let msg = verify.not_eof(result)?;

        if matches!(msg.stop, Some(proto::Stop {})) {
            *stopping = true;
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
        tracing::debug!(shard_index, kind, "received from shard");

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
