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
    // Cumulative backfill-begin clock per binding.
    backfill_begin: Vec<u64>,
    // Truncation boundary (begin clock) of the latest completed backfill per
    // binding.
    backfill_complete: Vec<u64>,
    // Backfill-begin clock the connector has been notified of per binding.
    notified_backfill_begin: Vec<u64>,
    // Backfill-complete clock the connector has been notified of per binding.
    notified_backfill_complete: Vec<u64>,
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
    // Wire codec negotiated with the connector.
    codec: connector_init::Codec,
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
        codec: connector_init::Codec,
        leader_tx: mpsc::UnboundedSender<proto::Materialize>,
        max_keys: Vec<(Bytes, Bytes)>,
        notified_backfill_begin: Vec<u64>,
        notified_backfill_complete: Vec<u64>,
        metrics: super::Metrics,
    ) -> Self {
        let l = bindings.len();
        Self {
            bindings,
            backfill_begin: vec![0; l],
            backfill_complete: vec![0; l],
            notified_backfill_begin,
            notified_backfill_complete,
            connector_pending: Vec::new(),
            connector_tx,
            db: Some((db, binding_state_keys)),
            db_persist_fut: None,
            disable_load_optimization,
            codec,
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
                    self.codec,
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
                if let Some(request) = drainer.step(&self.bindings, self.codec)? {
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

        // We observed L:Stopped, which the leader sends only at a transaction
        // boundary — so we're idle and own the shuffle Reader. Remove our shuffle
        // log segment files now, before blocking on the leader's EOF below. The
        // leader is concurrently closing its shuffle SessionClient; deleting these
        // segments releases any disk back-pressure held by the co-located Log RPC,
        // letting the shuffle topology drain to EOF so the leader's close() can
        // complete. Only then does the leader drop our channel, delivering the EOF
        // we await next.
        let Phase::Idle { shuffle_reader, .. } = &phase else {
            anyhow::bail!("leader sent Stopped while shard was not idle");
        };
        shuffle::log::remove_shard_segments(
            shuffle_reader.directory(),
            shuffle_reader.shard_index(),
        )
        .context("removing shuffle log segments on Stop")?;

        // After Stopped and shuffle session drain, the leader's stream must EOF.
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

            // Resolve cumulative per-binding backfill-begin (truncated_at) clocks
            // from the Frontier, keyed by each binding's journal_read_suffix.
            for (begin, binding) in self.backfill_begin.iter_mut().zip(self.bindings.iter()) {
                *begin = frontier
                    .latest_backfill_begin
                    .get(&binding.journal_read_suffix)
                    .map_or(0, |clock| clock.as_u64());
            }
            for (complete, binding) in self.backfill_complete.iter_mut().zip(self.bindings.iter()) {
                *complete = frontier
                    .latest_backfill_complete
                    .get(&binding.journal_read_suffix)
                    .map_or(0, |clock| clock.as_u64());
            }

            let Phase::Idle {
                accumulator,
                shuffle_reader,
                shuffle_remainders,
            } = phase
            else {
                anyhow::bail!("L:Load received while actor is not idle");
            };

            let scanner = scan::Scanner::new(
                accumulator,
                frontier,
                shuffle_reader,
                shuffle_remainders,
                self.backfill_begin.clone(),
            )?;
            return Ok((Phase::Scanning(scanner), false));
        } else if let Some(proto::materialize::Flush {
            connector_patches_json,
        }) = msg.flush
        {
            let (backfill_begins, backfill_completes) = self.backfill_flush_notifications();
            self.connector_pending.push(materialize::Request {
                flush: Some(materialize::request::Flush {
                    state_patches_json: connector_patches_json,
                    backfill_begins,
                    backfill_completes,
                    ..Default::default()
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

            if self.notified_backfill_begin != self.backfill_begin
                || self.notified_backfill_complete != self.backfill_complete
            {
                self.notified_backfill_begin = self.backfill_begin.clone();
                self.notified_backfill_complete = self.backfill_complete.clone();
            }

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

    /// Compute connector backfill notifications for bindings whose in-flight
    /// begin/complete clock is ahead of the notified baseline. The `timestamp`
    /// is always the begin clock — the truncation boundary — as a wall-clock
    /// Timestamp. The baseline advances at commit, so each notification fires
    /// once and is not re-sent across a clean restart.
    fn backfill_flush_notifications(
        &self,
    ) -> (
        Vec<materialize::request::flush::BackfillBegin>,
        Vec<materialize::request::flush::BackfillComplete>,
    ) {
        let mut begins = Vec::new();
        let mut completes = Vec::new();

        for binding in 0..self.backfill_begin.len() {
            let begin = self.backfill_begin[binding];
            if begin > self.notified_backfill_begin[binding] {
                begins.push(materialize::request::flush::BackfillBegin {
                    binding: binding as u32,
                    timestamp: proto_gazette::uuid::Clock::from_u64(begin).to_pb_json_timestamp(),
                });
            }

            let complete = self.backfill_complete[binding];
            if complete > self.notified_backfill_complete[binding] {
                completes.push(materialize::request::flush::BackfillComplete {
                    binding: binding as u32,
                    timestamp: proto_gazette::uuid::Clock::from_u64(complete)
                        .to_pb_json_timestamp(),
                });
            }
        }
        (begins, completes)
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

            // A loaded row is prior-generation when it predates the binding's
            // backfill `truncated_at`: the combiner then won't reduce it with
            // same-key current-generation source documents, and the drain path
            // discards it. Skip the lookup entirely when no backfill is active.
            let begin = self.backfill_begin[binding_index];
            let prior_gen = if begin == 0 {
                false
            } else if let Some(doc::HeapNode::String(uuid)) =
                binding_spec.document_uuid_ptr.query(&doc)
            {
                let (_, clock, _) = proto_gazette::uuid::parse_str(uuid).with_context(|| {
                    format!(
                        "loaded doc for {} has an unparseable document UUID {uuid:?}",
                        binding_spec.collection_name,
                    )
                })?;
                clock.as_u64() < begin
            } else {
                false
            };

            if prior_gen {
                memtable.add_prior_gen(binding_index as u16, doc)?;
            } else {
                memtable.add(binding_index as u16, doc, true)?;
            }
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
                backfill_begin: Vec::new(),
                backfill_complete: Vec::new(),
                notified_backfill_begin: Vec::new(),
                notified_backfill_complete: Vec::new(),
                connector_pending: Vec::new(),
                connector_tx,
                db: None,
                db_persist_fut: None,
                disable_load_optimization: false,
                codec: connector_init::Codec::Proto,
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
            Bytes::from_static(b"[{\"done\":true}\t]")
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
            backfill_begin: Vec::new(),
            backfill_complete: Vec::new(),
            notified_backfill_begin: Vec::new(),
            notified_backfill_complete: Vec::new(),
            connector_pending: Vec::new(),
            connector_tx: actor_to_conn_tx,
            db: Some((db, Vec::new())),
            db_persist_fut: None,
            disable_load_optimization: false,
            codec: connector_init::Codec::Proto,
            leader_tx: actor_to_leader_tx,
            load_keys: Default::default(),
            flushed: HashMap::new(),
            max_keys: Vec::new(),
            metrics: super::super::Metrics::new("test/shard"),
        };

        let accumulator =
            crate::Accumulator::new(super::super::task::combine_spec(&[]).unwrap()).unwrap();
        let shuffle_dir = tempfile::tempdir().unwrap();
        let shuffle_reader = shuffle::log::Reader::new(shuffle_dir.path(), 0);

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

    #[tokio::test]
    async fn backfill_notifications_fire_once_and_survive_restart() {
        let (mut actor, _leader_rx, mut connector_rx) = make_actor();
        actor.backfill_begin = vec![0];
        actor.backfill_complete = vec![0];
        actor.notified_backfill_begin = vec![0];
        actor.notified_backfill_complete = vec![0];

        // Backfill 1 begins. The real L:Flush handler stages a C:Flush carrying
        // the begin notification to the connector.
        actor.backfill_begin[0] = 10;
        let (phase, _stop) = actor
            .on_leader_message(
                make_idle_phase(),
                Some(Ok(proto::Materialize {
                    flush: Some(proto::materialize::Flush {
                        connector_patches_json: Bytes::new(),
                    }),
                    ..Default::default()
                })),
            )
            .unwrap();
        _ = actor.try_connector_tx();
        let flush = connector_rx.recv().await.unwrap().flush.unwrap();
        assert_eq!(
            flush.backfill_begins.len(),
            1,
            "begin1 forwarded to connector"
        );
        assert_eq!(flush.backfill_begins[0].binding, 0);
        assert!(flush.backfill_completes.is_empty());

        // Flush forwards but does not commit; the baseline holds until Persist.
        assert_eq!(actor.notified_backfill_begin, vec![0]);

        // The real L:Persist handler commits, advancing the durable baseline to
        // the in-flight clocks so begin1 isn't re-sent.
        let db = crate::shard::RocksDB::open(None).await.unwrap();
        actor.db = Some((db, Vec::new()));
        _ = actor
            .on_leader_message(
                phase,
                Some(Ok(proto::Materialize {
                    persist: Some(proto::Persist {
                        seq_no: 1,
                        ..Default::default()
                    }),
                    ..Default::default()
                })),
            )
            .unwrap();
        assert_eq!(actor.notified_backfill_begin, vec![10], "begin1 committed");

        // The remaining rounds poke the trackers directly to exercise the edge
        // detection compactly. `backfill_flush_notifications` is exactly what the
        // L:Flush handler above calls; committing is `notified_* := backfill_*`.
        actor.backfill_complete[0] = 20; // complete1 > begin1
        let (begins, completes) = actor.backfill_flush_notifications();
        assert_eq!((begins.len(), completes.len()), (0, 1), "complete1 fires");
        actor.notified_backfill_complete[0] = 20; // commit

        // Backfill 2 begins (begin2 > complete1); its completion hasn't arrived,
        // so the only complete clock is still complete1 (< begin2) — no completion.
        actor.backfill_begin[0] = 30;
        let (begins, completes) = actor.backfill_flush_notifications();
        assert_eq!(
            (begins.len(), completes.len()),
            (1, 0),
            "begin2 surfaces; no completion while it's in flight",
        );
        actor.notified_backfill_begin[0] = 30; // commit

        // Restart mid-backfill-2: the notified baseline reseeds from the DURABLE
        // committed frontier (begin2=30, complete1=20), NOT to 0; the in-flight
        // clocks reload to the same committed values from the first Load.
        actor.notified_backfill_begin = vec![30];
        actor.notified_backfill_complete = vec![20];
        actor.backfill_begin = vec![30];
        actor.backfill_complete = vec![20];
        let (begins, completes) = actor.backfill_flush_notifications();
        assert!(
            begins.is_empty() && completes.is_empty(),
            "a clean restart re-fires nothing: the notified baseline already \
             covers begin2 and complete1",
        );

        // Backfill 2's own completion (complete2 > begin2) finally fires once.
        actor.backfill_complete[0] = 40;
        let (_begins, completes) = actor.backfill_flush_notifications();
        assert_eq!(completes.len(), 1, "begin2's real completion fires");
    }

    // A full-reduction binding storing the root document, keyed on /key, whose
    // `v` array reduces by append. `document_uuid_ptr` lets the shard read each
    // loaded row's UUID to classify it against the backfill boundary.
    fn backfill_binding() -> Binding {
        Binding {
            collection_name: "test/collection".to_string(),
            delta_updates: false,
            document_uuid_ptr: json::Pointer::from("/_meta/uuid"),
            journal_read_suffix: "test/collection/pivot=00".to_string(),
            key_extractors: vec![doc::Extractor::with_default(
                "/key",
                &doc::SerPolicy::noop(),
                serde_json::json!(""),
            )],
            read_schema_json: bytes::Bytes::from_static(
                br#"{
                    "type": "object",
                    "properties": {
                        "key": { "type": "string" },
                        "v": { "type": "array", "reduce": { "strategy": "append" } }
                    },
                    "reduce": { "strategy": "merge" }
                }"#,
            ),
            ser_policy: doc::SerPolicy::noop(),
            state_key: "test/collection".to_string(),
            store_document: true,
            value_plan: doc::ExtractorPlan::new(&[]),
        }
    }

    #[tokio::test]
    async fn backfill_load_classifies_loaded_docs_through_drain() {
        let producer = proto_gazette::uuid::Producer::from_bytes([0x01, 0, 0, 0, 0, 0]);
        let flags = proto_gazette::uuid::Flags(0);
        let mk_uuid = |clock| proto_gazette::uuid::build(producer, clock, flags).to_string();
        // The boundary, plus a row clock below it (stale) and above it (fresh).
        let truncated_at = proto_gazette::uuid::Clock::from_unix(1_700_000_000, 0);
        let stale = mk_uuid(proto_gazette::uuid::Clock::from_unix(1_699_999_999, 0));
        let fresh = mk_uuid(proto_gazette::uuid::Clock::from_unix(1_700_000_001, 0));

        let (mut actor, _leader_rx, _connector_rx) = make_actor();
        actor.bindings = vec![backfill_binding()];
        actor.backfill_begin = vec![0];
        actor.backfill_complete = vec![0];
        actor.notified_backfill_begin = vec![0];
        actor.notified_backfill_complete = vec![0];

        let accumulator = crate::Accumulator::new(
            super::super::task::combine_spec(&[backfill_binding()]).unwrap(),
        )
        .unwrap();
        let shuffle_dir = tempfile::tempdir().unwrap();
        let shuffle_reader = shuffle::log::Reader::new(shuffle_dir.path(), 0);
        let idle = Phase::Idle {
            accumulator,
            shuffle_reader,
            shuffle_remainders: VecDeque::new(),
        };

        // L:Load — the Frontier carries the binding's `truncated_at`, which the
        // handler densifies into `backfill_begin` before entering the scan.
        let mut frontier = shuffle::Frontier::new(Vec::new(), vec![0u64]).unwrap();
        frontier
            .latest_backfill_begin
            .insert(actor.bindings[0].journal_read_suffix.clone(), truncated_at);

        let (mut phase, _stop) = actor
            .on_leader_message(
                idle,
                Some(Ok(proto::Materialize {
                    load: Some(proto::materialize::Load {
                        frontier: Some(frontier.encode()),
                    }),
                    ..Default::default()
                })),
            )
            .unwrap();

        assert_eq!(
            actor.backfill_begin,
            vec![truncated_at.as_u64()],
            "begin clock densified from Frontier"
        );
        assert_eq!(
            actor.backfill_complete,
            vec![0],
            "no completion present in Frontier"
        );
        assert!(
            matches!(phase, Phase::Scanning(_)),
            "L:Load enters the scan"
        );

        // Three C:Loaded rows, classified against the boundary as they arrive.
        let loaded = |key: &str, v: &str, uuid: Option<&str>| {
            let doc = match uuid {
                Some(u) => serde_json::json!({"key": key, "v": [v], "_meta": {"uuid": u}}),
                None => serde_json::json!({"key": key, "v": [v]}),
            };
            materialize::Response {
                loaded: Some(materialize::response::Loaded {
                    binding: 0,
                    doc_json: Bytes::from(serde_json::to_vec(&doc).unwrap()),
                }),
                ..Default::default()
            }
        };
        for resp in [
            loaded("straddle", "stale", Some(&stale)), // older than the boundary
            loaded("normal", "loaded", Some(&fresh)),  // newer than the boundary
            loaded("nouuid", "kept", None),            // no UUID to compare
        ] {
            actor
                .on_connector_response(&mut phase, Some(Ok(resp)))
                .unwrap();
        }

        // Inject the current-generation source documents the scan would surface,
        // pairing one against each loaded row.
        let Phase::Scanning(mut scanner) = phase else {
            panic!("expected Scanning phase after L:Load");
        };
        {
            let memtable = scanner.accumulator().memtable().unwrap();
            for (key, v) in [("straddle", "fresh"), ("normal", "src"), ("nouuid", "add")] {
                let doc = serde_json::json!({"key": key, "v": [v]});
                let node = doc::HeapNode::from_node(&doc, memtable.alloc());
                memtable.add(0, node, false).unwrap();
            }
        }

        // Drain and collect each stored (key, v, exists).
        let (accumulator, shuffle_reader, shuffle_remainders, _active) = scanner.into_parts();
        let mut drainer =
            drain::Drainer::new(accumulator, shuffle_reader, shuffle_remainders).unwrap();

        let mut stores = Vec::new();
        while let Some(req) = drainer
            .step(&actor.bindings, connector_init::Codec::Json)
            .unwrap()
        {
            let store = req.store.expect("drained request is a Store");
            let doc: serde_json::Value = serde_json::from_slice(&store.doc_json).unwrap();
            stores.push((
                doc.get("key").and_then(|k| k.as_str()).unwrap().to_string(),
                doc.get("v").cloned().unwrap(),
                store.exists,
            ));
        }

        // Drained in key order. "straddle" is prior-generation: its stale ["stale"]
        // is dropped (NOT reduced) and only the current-gen source ["fresh"] stores,
        // with exists=true. "normal" (newer than the boundary) and "nouuid" (no UUID)
        // load normally, reducing their loaded value forward.
        assert_eq!(
            stores,
            vec![
                (
                    "normal".to_string(),
                    serde_json::json!(["loaded", "src"]),
                    true
                ),
                (
                    "nouuid".to_string(),
                    serde_json::json!(["kept", "add"]),
                    true
                ),
                ("straddle".to_string(), serde_json::json!(["fresh"]), true),
            ],
        );
    }

    #[tokio::test]
    async fn loaded_doc_with_corrupt_uuid_errors() {
        let (mut actor, _leader_rx, _connector_rx) = make_actor();
        actor.bindings = vec![backfill_binding()];
        actor.backfill_begin = vec![10]; // active backfill
        actor.backfill_complete = vec![0];
        actor.notified_backfill_begin = vec![0];
        actor.notified_backfill_complete = vec![0];

        // /_meta/uuid is present and a string, but not a valid v1 UUID.
        let doc = serde_json::json!({"key": "k", "v": ["x"], "_meta": {"uuid": "not-a-uuid"}});
        let mut phase = make_idle_phase();
        let result = actor.on_connector_response(
            &mut phase,
            Some(Ok(materialize::Response {
                loaded: Some(materialize::response::Loaded {
                    binding: 0,
                    doc_json: Bytes::from(serde_json::to_vec(&doc).unwrap()),
                }),
                ..Default::default()
            })),
        );
        assert!(
            result.is_err(),
            "a corrupt document UUID fails the transaction"
        );
    }
}
