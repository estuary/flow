use super::{Binding, LoadKeys, drain, scan};
use crate::{patches, proto};
use anyhow::Context;
use bytes::Bytes;
use futures::{FutureExt, StreamExt, future, future::BoxFuture};
use proto_flow::materialize;
use std::collections::{HashMap, VecDeque};
use tokio::sync::mpsc;

use crate::proto::materialize::flushed::Binding as FlushedBinding;

/// Phase of the shard transaction which are incrementally driven by Actor.
pub(super) enum Phase {
    Idle {
        accumulator: crate::Accumulator,
        shuffle_reader: shuffle::log::Reader,
        shuffle_remainders: VecDeque<shuffle::log::Remainder>,
    },
    Scanning(scan::Scanner),
    Draining(drain::Drainer),
}

/// Shard-side materialization reactor for one joined leader session.
pub(super) struct Actor {
    // Task binding specifications.
    bindings: Vec<Binding>,
    // FIFO of outbound connector requests, drained head-first into
    // `connector_tx` as channel capacity permits.
    connector_pending: Vec<materialize::Request>,
    // Bounded channel out to the connector subprocess.
    connector_tx: mpsc::Sender<materialize::Request>,
    // RocksDB and binding state keys, when a Persist is not in flight.
    db: Option<(crate::shard::RocksDB, Vec<String>)>,
    // RocksDB future when a Persist is in flight.
    db_persist_fut: Option<
        BoxFuture<
            'static,
            anyhow::Result<((crate::shard::RocksDB, Vec<String>), proto::Persisted)>,
        >,
    >,
    // When true, don't suppress C:Load for keys less-than `max_keys`.
    disable_load_optimization: bool,
    // Aggregate active bindings of a pending Flushed response.
    flushed: HashMap<u32, FlushedBinding>,
    // Channel for sending to the leader.
    leader_tx: mpsc::UnboundedSender<proto::Materialize>,
    // Keys for which we've sent C:Load in the current transaction.
    load_keys: LoadKeys,
    // Running maximum observed key (committed, current), one entry per binding.
    // The current key ratchets up during frontier scans, and is rotated into
    // committed at transaction close (and not before; this lets us filter keys
    // above "committed" but below "current" during scans).
    max_keys: Vec<(Bytes, Bytes)>,
    // Per-session metrics counters.
    metrics: super::Metrics,
}

impl Actor {
    pub fn new(
        bindings: Vec<Binding>,
        binding_state_keys: Vec<String>,
        connector_tx: mpsc::Sender<materialize::Request>,
        db: crate::shard::RocksDB,
        disable_load_optimization: bool,
        leader_tx: mpsc::UnboundedSender<proto::Materialize>,
        max_keys: Vec<(Bytes, Bytes)>,
        metrics: super::Metrics,
    ) -> Self {
        Self {
            bindings,
            connector_pending: Vec::new(),
            connector_tx,
            db: Some((db, binding_state_keys)),
            db_persist_fut: None,
            disable_load_optimization,
            flushed: HashMap::new(),
            leader_tx,
            load_keys: Default::default(),
            max_keys,
            metrics,
        }
    }

    #[tracing::instrument(level = "debug", err(Debug, level = "warn"), skip_all)]
    pub async fn serve<R, C, L>(
        mut self,
        accumulator: crate::Accumulator,
        connector_rx: &mut C,
        controller_rx: &mut R,
        leader_rx: &mut L,
        shuffle_reader: shuffle::log::Reader,
    ) -> anyhow::Result<crate::shard::RocksDB>
    where
        R: futures::Stream<Item = tonic::Result<proto::Materialize>> + Send + Unpin + 'static,
        C: futures::Stream<Item = tonic::Result<materialize::Response>> + Send + Unpin + 'static,
        L: futures::Stream<Item = tonic::Result<proto::Materialize>> + Send + Unpin + 'static,
    {
        let mut phase = Phase::Idle {
            accumulator,
            shuffle_reader,
            shuffle_remainders: VecDeque::new(),
        };
        let mut loop_count: u64 = 0;

        let mut ticker = tokio::time::interval(crate::ACTOR_TICK_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            loop_count += 1;

            let phase_kind = match &phase {
                Phase::Idle { .. } => "idle",
                Phase::Scanning(_) => "scanning",
                Phase::Draining(_) => "draining",
            };
            tracing::trace!(
                loop_count,
                connector_pending_len = self.connector_pending.len(),
                loaded_bindings = self.flushed.len(),
                persist_in_flight = self.db_persist_fut.is_some(),
                phase = phase_kind,
                "shard materialize Actor::serve iteration"
            );

            // Perform non-blocking sends of pending connector requests.
            let wake_connector_tx = self.try_connector_tx();

            // Attempt to drive `phase` forward. On success we loop to
            // immediately start IO, falling through to the select below only
            // if we can't immediately send to `connector_tx`.
            if !self.connector_pending.is_empty() {
                // Channel is stuffed -- don't allow further requests to queue.
            } else if let Phase::Scanning(mut scanner) = phase {
                if scanner.step(
                    &self.bindings,
                    &mut self.load_keys,
                    &mut self.max_keys,
                    self.disable_load_optimization,
                    &mut self.connector_pending,
                )? {
                    phase = Phase::Scanning(scanner);
                } else {
                    let (accumulator, shuffle_reader, shuffle_remainders, active) =
                        scanner.into_parts();

                    let combiner_bytes = accumulator.combiner_byte_usage();
                    service_kit::event!(
                        tracing::Level::DEBUG,
                        "leader",
                        active_bindings = active.len(),
                        combiner_bytes,
                        "sending L:Loaded after frontier scan",
                    );
                    self.metrics.scans_completed.increment(1);

                    _ = self.leader_tx.send(proto::Materialize {
                        loaded: Some(proto::materialize::Loaded {
                            bindings: active.into_values().collect(),
                            combiner_usage_bytes: combiner_bytes,
                        }),
                        ..Default::default()
                    });
                    phase = Phase::Idle {
                        accumulator,
                        shuffle_reader,
                        shuffle_remainders,
                    };
                }
                continue;
            } else if let Phase::Draining(mut drainer) = phase {
                if let Some(request) = drainer.step(&self.bindings)? {
                    self.connector_pending.push(request);
                    phase = Phase::Draining(drainer);
                } else {
                    let (accumulator, shuffle_reader, shuffle_remainders, active) =
                        drainer.into_parts()?;

                    service_kit::event!(
                        tracing::Level::DEBUG,
                        "leader",
                        active_bindings = active.len(),
                        "sending L:Stored after memtable drain",
                    );
                    self.metrics.drains_completed.increment(1);

                    _ = self.leader_tx.send(proto::Materialize {
                        stored: Some(proto::materialize::Stored { bindings: active }),
                        ..Default::default()
                    });
                    phase = Phase::Idle {
                        accumulator,
                        shuffle_reader,
                        shuffle_remainders,
                    };
                }
                continue;
            }

            tokio::select! {
                biased;

                // Prioritize moving connector messages (high volume).
                msg = connector_rx.next() => {
                    self.on_connector_response(&mut phase, msg)?;
                }
                // Next, a leader message.
                msg = leader_rx.next() => {
                    let (next, stopped) = self.on_leader_message(phase, msg)?;
                    phase = next;

                    if stopped {
                        break;
                    }
                }
                // Next, a controller message.
                msg = controller_rx.next() => {
                    self.on_controller_request(msg)?;
                }
                // Next, a Persist completion.
                result = maybe_fut(&mut self.db_persist_fut) => {
                    let (db, persisted) = result?;
                    let seq_no = persisted.seq_no;
                    self.db = Some(db);

                    service_kit::event!(
                        tracing::Level::DEBUG,
                        "leader",
                        seq_no,
                        "RocksDB persist completed; sending L:Persisted",
                    );
                    self.metrics.persists.increment(1);

                    _ = self.leader_tx.send(proto::Materialize {
                        persisted: Some(persisted),
                        ..Default::default()
                    });
                }
                // Next, wait for capacity to send to the connector.
                true = wake_connector_tx => {}
                // Periodic tick ensures the iteration trace fires even when otherwise idle.
                _ = ticker.tick() => {}
            }
        }

        // After Stopped, the leader's stream must EOF.
        let verify = crate::verify("Materialize", "leader EOF after Stopped", "leader");
        verify.eof(leader_rx.next().await)?;

        // Leader-protocol invariant: the leader does not send L:Stopped while a
        // Persist is outstanding — every L:Persist is acknowledged with
        // L:Persisted before L:Stopped.
        let Some((db, _)) = self.db.take() else {
            anyhow::bail!("leader Stopped while a Persist is in flight");
        };

        Ok(db)
    }

    fn try_connector_tx(&mut self) -> impl Future<Output = bool> + 'static {
        // Closure for mapping an OwnedPermit Result to Ok (our "poll again" signal).
        let ok = |result: Result<_, _>| result.is_ok();
        // Future which represents an absence of an awake signal.
        let idle = future::Either::Right(std::future::ready(false));

        if self.connector_pending.is_empty() {
            return idle;
        } else if let Ok(permits) = self
            .connector_tx
            .try_reserve_many(self.connector_pending.len())
        {
            for (request, permit) in self.connector_pending.drain(..).zip(permits) {
                permit.send(request);
            }
            return idle;
        }

        // We have only partial capacity to send. In this uncommon case, we pay
        // for relocating the tail of `connector_pending`. Note the sender may
        // race sends, so account for capacity > connector_pending having opened up.
        let n = self
            .connector_tx
            .capacity()
            .min(self.connector_pending.len());
        let permits = self
            .connector_tx
            .try_reserve_many(n)
            .expect("capacity was just observed and we are the sole sender");

        for (request, permit) in self.connector_pending.drain(..n).zip(permits) {
            permit.send(request);
        }
        // Wake when more capacity becomes available.
        future::Either::Left(self.connector_tx.clone().reserve_owned().map(ok))
    }

    fn on_leader_message(
        &mut self,
        phase: Phase,
        msg: Option<tonic::Result<proto::Materialize>>,
    ) -> anyhow::Result<(Phase, bool)> {
        let verify = crate::verify("Materialize", "leader message", "leader");
        let msg = verify.not_eof(msg)?;

        if let Some(proto::Stopped {}) = msg.stopped {
            return Ok((phase, true));
        } else if let Some(proto::materialize::Load {
            frontier: Some(proto),
        }) = msg.load
        {
            let frontier =
                shuffle::Frontier::decode(proto).context("invalid Frontier on L:Load")?;

            let Phase::Idle {
                accumulator,
                shuffle_reader,
                shuffle_remainders,
            } = phase
            else {
                anyhow::bail!("L:Load received while actor is not idle");
            };

            let scanner =
                scan::Scanner::new(accumulator, frontier, shuffle_reader, shuffle_remainders)?;
            return Ok((Phase::Scanning(scanner), false));
        } else if let Some(proto::materialize::Flush {
            connector_patches_json,
        }) = msg.flush
        {
            self.connector_pending.push(materialize::Request {
                flush: Some(materialize::request::Flush {
                    state_patches_json: connector_patches_json,
                }),
                ..Default::default()
            });
        } else if let Some(proto::materialize::Store {}) = msg.store {
            let Phase::Idle {
                accumulator,
                shuffle_reader,
                shuffle_remainders,
            } = phase
            else {
                anyhow::bail!("L:Store received while actor is not idle");
            };

            // Clear and partially release `load_keys` memory before the next scan.
            let load_keys_len = self.load_keys.len();
            self.load_keys.clear();
            self.load_keys.shrink_to(load_keys_len / 2);

            // Rotate a changed `next_max` into `prev_max` for each of `max_keys`.
            for (prev_max, next_max) in self.max_keys.iter_mut() {
                if !next_max.is_empty() {
                    *prev_max = std::mem::take(next_max);
                }
            }

            let drainer = drain::Drainer::new(accumulator, shuffle_reader, shuffle_remainders)?;
            return Ok((Phase::Draining(drainer), false));
        } else if let Some(proto::materialize::StartCommit {
            connector_checkpoint,
            connector_patches_json,
        }) = msg.start_commit
        {
            self.connector_pending.push(materialize::Request {
                start_commit: Some(materialize::request::StartCommit {
                    runtime_checkpoint: connector_checkpoint,
                    state_patches_json: connector_patches_json,
                }),
                ..Default::default()
            });
        } else if let Some(proto::materialize::Acknowledge {
            connector_patches_json,
        }) = msg.acknowledge
        {
            self.connector_pending.push(materialize::Request {
                acknowledge: Some(materialize::request::Acknowledge {
                    state_patches_json: connector_patches_json,
                }),
                ..Default::default()
            });
        } else if let Some(persist) = msg.persist {
            let seq_no = persist.seq_no;

            let (db, binding_state_keys) = self
                .db
                .take()
                .context("received L:Persist while a Persist is already in flight")?;

            self.db_persist_fut = Some(
                async move {
                    let db = db.persist(&persist, &binding_state_keys).await?;
                    Ok(((db, binding_state_keys), proto::Persisted { seq_no }))
                }
                .boxed(),
            );
        } else {
            return Err(verify.fail_msg(msg));
        }

        Ok((phase, false))
    }

    fn on_connector_response(
        &mut self,
        phase: &mut Phase,
        resp: Option<tonic::Result<materialize::Response>>,
    ) -> anyhow::Result<()> {
        let verify = crate::verify("Materialize", "connector response", "connector");
        let resp = verify.not_eof(resp)?;

        if let Some(materialize::response::Loaded { binding, doc_json }) = resp.loaded {
            let active = self
                .flushed
                .entry(binding)
                .or_insert_with(|| FlushedBinding {
                    index: binding,
                    ..Default::default()
                });
            active.loaded_docs_total += 1;
            active.loaded_bytes_total += doc_json.len() as u64;

            self.metrics.loaded_docs.increment(1);
            self.metrics.loaded_bytes.increment(doc_json.len() as u64);

            // C:Loaded responses arrive at the connector's pace, which may
            // straddle phase transitions:
            // * Phase::Scanning — a common case, mid-scan response.
            // * Phase::Idle — the connector is still draining responses for
            //   prior C:Load requests after the Scanner finished and we sent
            //   L:Loaded back to the leader (often triggered by C:Flush).
            //   The Idle and Scanning phases share the same accumulator,
            //   so we add the doc to it identically.
            // * Phase::Draining — must not happen: the leader sends L:Store
            //   only after every shard's L:Flushed, which we send only after
            //   C:Flushed, which ends the connector's Loaded phase.
            let accumulator = match phase {
                Phase::Idle { accumulator, .. } => accumulator,
                Phase::Scanning(scanner) => scanner.accumulator(),
                Phase::Draining(_) => {
                    anyhow::bail!("C:Loaded received while actor is draining")
                }
            };
            let binding_index = binding as usize;
            let binding_spec = self
                .bindings
                .get(binding_index)
                .ok_or_else(|| anyhow::anyhow!("Loaded binding {binding_index} out of range"))?;

            let (memtable, _alloc, doc) =
                accumulator.parse_json_doc(&doc_json).with_context(|| {
                    format!("parsing loaded doc for {}", binding_spec.collection_name)
                })?;
            memtable.add(binding_index as u16, doc, true)?;
        } else if let Some(materialize::response::Flushed { state }) = resp.flushed {
            let bindings = std::mem::take(&mut self.flushed).into_values().collect();
            _ = self.leader_tx.send(proto::Materialize {
                flushed: Some(proto::materialize::Flushed {
                    bindings,
                    connector_patches_json: patches::encode_connector_state(state),
                }),
                ..Default::default()
            });
        } else if let Some(materialize::response::StartedCommit { state }) = resp.started_commit {
            _ = self.leader_tx.send(proto::Materialize {
                started_commit: Some(proto::materialize::StartedCommit {
                    connector_patches_json: patches::encode_connector_state(state),
                }),
                ..Default::default()
            });
        } else if let Some(materialize::response::Acknowledged { state }) = resp.acknowledged {
            _ = self.leader_tx.send(proto::Materialize {
                acknowledged: Some(proto::materialize::Acknowledged {
                    connector_patches_json: patches::encode_connector_state(state),
                }),
                ..Default::default()
            });
        } else {
            return Err(verify.fail_msg(resp));
        }

        Ok(())
    }

    fn on_controller_request(
        &mut self,
        msg: Option<tonic::Result<proto::Materialize>>,
    ) -> anyhow::Result<()> {
        let verify = crate::verify("Materialize", "Stop or CloseNow", "controller");
        let msg = verify.not_eof(msg)?;

        if matches!(msg.stop, Some(proto::Stop {})) {
            _ = self.leader_tx.send(proto::Materialize {
                stop: Some(proto::Stop {}),
                ..Default::default()
            });
        } else if matches!(msg.close_now, Some(proto::CloseNow {})) {
            _ = self.leader_tx.send(proto::Materialize {
                close_now: Some(proto::CloseNow {}),
                ..Default::default()
            });
        } else {
            return Err(verify.fail_msg(msg));
        }
        Ok(())
    }
}

async fn maybe_fut<T>(opt: &mut Option<BoxFuture<'static, T>>) -> T {
    match opt.as_mut() {
        Some(fut) => {
            let result = fut.await;
            *opt = None;
            result
        }
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto_flow::flow;
    use proto_flow::materialize::response;
    use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};

    fn make_idle_phase() -> Phase {
        let accumulator =
            crate::Accumulator::new(super::super::task::combine_spec(&[]).unwrap()).unwrap();
        let shuffle_reader = shuffle::log::Reader::new(std::path::Path::new("/dev/null"), 0);
        Phase::Idle {
            accumulator,
            shuffle_reader,
            shuffle_remainders: VecDeque::new(),
        }
    }

    fn make_actor() -> (
        Actor,
        mpsc::UnboundedReceiver<proto::Materialize>,
        mpsc::Receiver<materialize::Request>,
    ) {
        let (leader_tx, leader_rx) = mpsc::unbounded_channel();
        let (connector_tx, connector_rx) = mpsc::channel(8);

        (
            Actor {
                bindings: Vec::new(),
                connector_pending: Vec::new(),
                connector_tx,
                db: None,
                db_persist_fut: None,
                disable_load_optimization: false,
                leader_tx,
                load_keys: Default::default(),
                flushed: HashMap::new(),
                max_keys: Vec::new(),
                metrics: super::super::Metrics::new("test/shard"),
            },
            leader_rx,
            connector_rx,
        )
    }

    #[tokio::test]
    async fn acknowledge_round_trip_forwards_patches() {
        let (mut actor, mut leader_rx, mut connector_rx) = make_actor();
        let patches = Bytes::from_static(br#"[{"ok":true}]"#);

        let (_phase, _stop) = actor
            .on_leader_message(
                make_idle_phase(),
                Some(Ok(proto::Materialize {
                    acknowledge: Some(proto::materialize::Acknowledge {
                        connector_patches_json: patches.clone(),
                    }),
                    ..Default::default()
                })),
            )
            .unwrap();

        // Drive the non-blocking send of the pending connector request.
        _ = actor.try_connector_tx();

        let request = connector_rx.recv().await.unwrap();
        assert_eq!(request.acknowledge.unwrap().state_patches_json, patches);

        let mut phase = make_idle_phase();
        actor
            .on_connector_response(
                &mut phase,
                Some(Ok(materialize::Response {
                    acknowledged: Some(response::Acknowledged {
                        state: Some(flow::ConnectorState {
                            updated_json: Bytes::from_static(br#"{"done":true}"#),
                            merge_patch: true,
                        }),
                    }),
                    ..Default::default()
                })),
            )
            .unwrap();

        let response = leader_rx.recv().await.unwrap();
        assert_eq!(
            response.acknowledged.unwrap().connector_patches_json,
            Bytes::from_static(
                br#"[{"done":true}
]"#
            )
        );
    }

    /// Drive `Actor::serve` end-to-end with mpsc channels acting as the
    /// connector, leader, and controller, walking a transaction lifecycle:
    /// Acknowledge → Flush → Store (empty drain) → StartCommit → Persist →
    /// controller Stop → controller CloseNow → leader Stopped. Each step
    /// asserts that the actor correctly translates leader commands into
    /// connector requests, fans connector responses back to the leader,
    /// forwards controller signals to the leader, and persists state to
    /// RocksDB.
    #[tokio::test]
    async fn full_lifecycle_round_trip() {
        // Actor → connector requests; the test reads as a mock connector.
        let (actor_to_conn_tx, mut actor_to_conn_rx) = mpsc::channel::<materialize::Request>(8);
        // Mock connector → actor responses.
        let (conn_to_actor_tx, conn_to_actor_rx) =
            mpsc::channel::<tonic::Result<materialize::Response>>(8);
        // Actor → leader; the test reads as a mock leader.
        let (actor_to_leader_tx, mut actor_to_leader_rx) =
            mpsc::unbounded_channel::<proto::Materialize>();
        // Mock leader → actor commands.
        let (leader_to_actor_tx, leader_to_actor_rx) =
            mpsc::unbounded_channel::<tonic::Result<proto::Materialize>>();
        // Controller → actor; used to drive Stop + CloseNow forwarding below.
        let (controller_to_actor_tx, controller_to_actor_rx) =
            mpsc::unbounded_channel::<tonic::Result<proto::Materialize>>();

        let conn_stream = ReceiverStream::new(conn_to_actor_rx);
        let leader_stream = UnboundedReceiverStream::new(leader_to_actor_rx);
        let controller_stream = UnboundedReceiverStream::new(controller_to_actor_rx);

        // A real RocksDB lets the Persist step exercise the full code path.
        let db = crate::shard::RocksDB::open(None).await.unwrap();

        let actor = Actor {
            bindings: Vec::new(),
            connector_pending: Vec::new(),
            connector_tx: actor_to_conn_tx,
            db: Some((db, Vec::new())),
            db_persist_fut: None,
            disable_load_optimization: false,
            leader_tx: actor_to_leader_tx,
            load_keys: Default::default(),
            flushed: HashMap::new(),
            max_keys: Vec::new(),
            metrics: super::super::Metrics::new("test/shard"),
        };

        let accumulator =
            crate::Accumulator::new(super::super::task::combine_spec(&[]).unwrap()).unwrap();
        let shuffle_reader = shuffle::log::Reader::new(std::path::Path::new("/dev/null"), 0);

        let serve_handle = tokio::spawn(async move {
            let mut conn_stream = conn_stream;
            let mut leader_stream = leader_stream;
            let mut controller_stream = controller_stream;
            actor
                .serve(
                    accumulator,
                    &mut conn_stream,
                    &mut controller_stream,
                    &mut leader_stream,
                    shuffle_reader,
                )
                .await
        });

        // 1) L:Acknowledge → C:Acknowledge → C:Acknowledged → L:Acknowledged.
        leader_to_actor_tx
            .send(Ok(proto::Materialize {
                acknowledge: Some(proto::materialize::Acknowledge {
                    connector_patches_json: Bytes::from_static(br#"[{"ack":1}]"#),
                }),
                ..Default::default()
            }))
            .unwrap();

        let req = actor_to_conn_rx.recv().await.unwrap();
        assert_eq!(
            req.acknowledge.unwrap().state_patches_json,
            Bytes::from_static(br#"[{"ack":1}]"#),
        );

        conn_to_actor_tx
            .send(Ok(materialize::Response {
                acknowledged: Some(response::Acknowledged {
                    state: Some(flow::ConnectorState {
                        updated_json: Bytes::from_static(br#"{"ack_done":true}"#),
                        merge_patch: false,
                    }),
                }),
                ..Default::default()
            }))
            .await
            .unwrap();

        let resp = actor_to_leader_rx.recv().await.unwrap();
        assert!(resp.acknowledged.is_some());

        // 2) L:Flush → C:Flush → C:Flushed → L:Flushed.
        leader_to_actor_tx
            .send(Ok(proto::Materialize {
                flush: Some(proto::materialize::Flush {
                    connector_patches_json: Bytes::from_static(br#"[{"f":1}]"#),
                }),
                ..Default::default()
            }))
            .unwrap();

        let req = actor_to_conn_rx.recv().await.unwrap();
        assert_eq!(
            req.flush.unwrap().state_patches_json,
            Bytes::from_static(br#"[{"f":1}]"#),
        );

        conn_to_actor_tx
            .send(Ok(materialize::Response {
                flushed: Some(response::Flushed {
                    state: Some(flow::ConnectorState {
                        updated_json: Bytes::from_static(br#"{"flushed":true}"#),
                        merge_patch: false,
                    }),
                }),
                ..Default::default()
            }))
            .await
            .unwrap();

        let resp = actor_to_leader_rx.recv().await.unwrap();
        let flushed = resp.flushed.unwrap();
        assert!(flushed.bindings.is_empty());

        // 3) L:Store → empty drain → L:Stored.
        // With no bindings and an empty memtable, the drainer immediately
        // completes and the actor reports zero stored bindings.
        leader_to_actor_tx
            .send(Ok(proto::Materialize {
                store: Some(proto::materialize::Store {}),
                ..Default::default()
            }))
            .unwrap();

        let resp = actor_to_leader_rx.recv().await.unwrap();
        let stored = resp.stored.unwrap();
        assert!(stored.bindings.is_empty());

        // 4) L:StartCommit → C:StartCommit → C:StartedCommit → L:StartedCommit.
        leader_to_actor_tx
            .send(Ok(proto::Materialize {
                start_commit: Some(proto::materialize::StartCommit {
                    connector_checkpoint: Some(proto_gazette::consumer::Checkpoint::default()),
                    connector_patches_json: Bytes::from_static(br#"[{"sc":1}]"#),
                }),
                ..Default::default()
            }))
            .unwrap();

        let req = actor_to_conn_rx.recv().await.unwrap();
        let sc = req.start_commit.unwrap();
        assert_eq!(sc.state_patches_json, Bytes::from_static(br#"[{"sc":1}]"#),);

        conn_to_actor_tx
            .send(Ok(materialize::Response {
                started_commit: Some(response::StartedCommit {
                    state: Some(flow::ConnectorState {
                        updated_json: Bytes::from_static(br#"{"sc_done":true}"#),
                        merge_patch: false,
                    }),
                }),
                ..Default::default()
            }))
            .await
            .unwrap();

        let resp = actor_to_leader_rx.recv().await.unwrap();
        assert!(resp.started_commit.is_some());

        // 5) L:Persist → RocksDB write → L:Persisted echoes seq_no.
        leader_to_actor_tx
            .send(Ok(proto::Materialize {
                persist: Some(proto::Persist {
                    seq_no: 42,
                    last_applied: Bytes::from_static(b"persisted-spec-bytes"),
                    ..Default::default()
                }),
                ..Default::default()
            }))
            .unwrap();

        let resp = actor_to_leader_rx.recv().await.unwrap();
        assert_eq!(resp.persisted.unwrap().seq_no, 42);

        // 6) Controller Stop + CloseNow → forwarded to the leader.
        controller_to_actor_tx
            .send(Ok(proto::Materialize {
                stop: Some(proto::Stop {}),
                ..Default::default()
            }))
            .unwrap();
        let resp = actor_to_leader_rx.recv().await.unwrap();
        assert!(resp.stop.is_some());

        controller_to_actor_tx
            .send(Ok(proto::Materialize {
                close_now: Some(proto::CloseNow {}),
                ..Default::default()
            }))
            .unwrap();
        let resp = actor_to_leader_rx.recv().await.unwrap();
        assert!(resp.close_now.is_some());

        // 7) L:Stopped + leader EOF → serve completes, returning the DB.
        leader_to_actor_tx
            .send(Ok(proto::Materialize {
                stopped: Some(proto::Stopped {}),
                ..Default::default()
            }))
            .unwrap();
        std::mem::drop(leader_to_actor_tx);

        let db = serve_handle.await.unwrap().unwrap();

        // Confirm the Persist round-tripped: scan back the last_applied bytes.
        let (_db, recover) = db.scan(Vec::new()).await.unwrap();
        assert_eq!(recover.last_applied.as_ref(), b"persisted-spec-bytes");
    }
}
