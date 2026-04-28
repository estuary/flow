use super::fsm;
use super::state::Task;
use crate::proto;
use anyhow::Context;
use bytes::Bytes;
use futures::stream::{BoxStream, FuturesUnordered};
use futures::{FutureExt, StreamExt, future::BoxFuture};
use proto_gazette::uuid;
use std::collections::BTreeMap;
use tokio::sync::mpsc;

/// Actor leads transactions of an established materialization task session.
// We use UnboundedSender because Actor never "pumps" messages to shards:
// it follows a strict request-response pattern, where requests may be emitted
// as a run of gRPC messages but have a bounded scope. UnboundedSender lets us
// ignore details of waiting for send capacity and model sends as synchronous.
pub struct Actor {
    pub ack_intents_fut: Option<BoxFuture<'static, (crate::Publisher, tonic::Result<()>)>>,
    pub http_client: reqwest::Client,
    pub parked_publisher: Option<crate::Publisher>,
    pub peers: Vec<String>,
    pub pending_ack_intents: BTreeMap<String, Bytes>,
    pub shard_tx: Vec<mpsc::UnboundedSender<tonic::Result<proto::Materialize>>>,
    pub stats_flush_fut: Option<BoxFuture<'static, (crate::Publisher, tonic::Result<()>)>>,
    pub task: Task,
    pub trigger_fut: Option<BoxFuture<'static, anyhow::Result<()>>>,
}

impl Actor {
    #[tracing::instrument(level = "debug", err(Debug, level = "warn"), skip_all)]
    pub async fn serve(
        &mut self,
        mut head: fsm::Head,
        mut tail: fsm::Tail,
        session: shuffle::SessionClient,
        shard_rx: Vec<BoxStream<'static, tonic::Result<proto::Materialize>>>,
    ) -> anyhow::Result<()> {
        let n_shards = self.peers.len();
        assert_eq!(n_shards, shard_rx.len());
        assert_eq!(n_shards, self.shard_tx.len());

        tracing::info!(n_shards, "materialize actor started");

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
                self.ack_intents_fut.is_none(),
                now,
                &mut ready_shard_rx,
                &self.task,
                self.trigger_fut.is_none(),
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
                now,
                &mut ready_frontier,
                &mut ready_shard_rx,
                self.stats_flush_fut.is_none(),
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
            // then it's unexpected and a protocol error.
            if let Some((shard_index, msg)) = ready_shard_rx.take() {
                anyhow::bail!(
                    "unexpected message {msg:?} from {}@{shard_index}",
                    self.peers[shard_index],
                );
            }

            tokio::select! {
                biased;

                // First receive shard IO.
                Some((shard_index, msg, rx)) = shard_rx.next() => {
                    if let Some(msg) = self.on_shard_rx(&mut stopping, shard_index, msg)? {
                        ready_shard_rx = Some((shard_index, msg));
                    }
                    shard_rx.push(next_shard_rx((shard_index, rx)));
                }
                Some((publisher, result)) = maybe_fut(&mut self.stats_flush_fut) => {
                    () = result.map_err(crate::status_to_anyhow)
                        .context("flushing stats")?;

                    self.parked_publisher = Some(publisher);
                    self.stats_flush_fut = None;
                }
                Some((publisher, result)) = maybe_fut(&mut self.ack_intents_fut) => {
                    () = result.map_err(crate::status_to_anyhow)
                        .context("writing ACK intents")?;

                    self.parked_publisher = Some(publisher);
                    self.ack_intents_fut = None;
                }
                Some(result) = maybe_fut(&mut self.trigger_fut) => {
                    () = result.context("trigger delivery")?;
                    self.trigger_fut = None;
                }
                Some(result) = frontier_rx.next(), if ready_frontier.is_none() => {
                    ready_frontier = Some(result?);
                }
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
                unreachable!("terminators never reach dispatch");
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

            fsm::Action::StartCommit { connector_patches } => {
                tracing::debug!(
                    patches_bytes = connector_patches.len(),
                    "broadcasting L:StartCommit"
                );
                self.broadcast(proto::Materialize {
                    start_commit: Some(proto::materialize::StartCommit {
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

            fsm::Action::Persist {
                stack,
                snapshot_ack_intents,
            } => {
                tracing::debug!(
                    stack_len = stack.len(),
                    snapshot_ack_intents,
                    "dispatching L:Persist to shard zero"
                );
                let prelude = if snapshot_ack_intents {
                    let publisher = self
                        .parked_publisher
                        .as_mut()
                        .expect("publisher owned at Persist dispatch with ACK snapshot");

                    // Retain to write later, post-commit. In
                    // `Publisher::Preview` there are no producers and
                    // `commit_intents` returns None, leaving
                    // `pending_ack_intents` empty.
                    self.pending_ack_intents = match publisher.commit_intents() {
                        Some(commit) => publisher::intents::build_transaction_intents(&[commit]),
                        None => BTreeMap::new(),
                    };

                    Some(proto::Persist {
                        delete_ack_intents: true,
                        ack_intents: self.pending_ack_intents.clone(),
                        ..Default::default()
                    })
                } else {
                    None
                };

                for persist in prelude.into_iter().chain(stack) {
                    let _ = self.shard_tx[0].send(Ok(proto::Materialize {
                        persist: Some(persist),
                        ..Default::default()
                    }));
                }
            }

            fsm::Action::PublishStats { stats } => {
                let mut publisher = self
                    .parked_publisher
                    .take()
                    .expect("publisher owned at PublishStats dispatch");

                self.stats_flush_fut = Some(
                    async move {
                        let result = publisher.publish_stats(stats).await;
                        (publisher, result)
                    }
                    .boxed(),
                );
            }

            fsm::Action::WriteAckIntents {} => {
                let intents = std::mem::take(&mut self.pending_ack_intents);

                let mut publisher = self
                    .parked_publisher
                    .take()
                    .expect("publisher owned at WriteAckIntents dispatch");

                self.ack_intents_fut = Some(
                    async move {
                        let result = publisher.write_intents(intents).await;
                        (publisher, result)
                    }
                    .boxed(),
                );
            }

            fsm::Action::CallTrigger { trigger_variables } => {
                let variables: models::TriggerVariables =
                    serde_json::from_slice(&trigger_variables)
                        .context("decoding trigger_variables JSON")?;
                let compiled = self
                    .task
                    .compiled_triggers
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
            &self.peers[shard_index],
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
        for tx in &self.shard_tx {
            let _ = tx.send(Ok(msg.clone()));
        }
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
