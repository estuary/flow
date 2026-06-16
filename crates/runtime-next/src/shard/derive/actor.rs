use super::{drain, scan, task::Task};
use crate::{patches, proto};
use anyhow::Context;
use futures::{FutureExt, StreamExt, future, future::BoxFuture};
use proto_flow::derive;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Read-side phase of the shard transaction, driven by leader `L:Load`s.
/// The output-publishing drain is orthogonal: it runs as `drain_fut` while
/// the read side proceeds (the leader pipelines the next transaction's reads
/// against the current transaction's commit).
enum Phase {
    Idle {
        shuffle_reader: shuffle::log::Reader,
        shuffle_remainders: VecDeque<shuffle::log::Remainder>,
    },
    Scanning(scan::Scanner),
}

/// Shard-side derivation reactor for one joined leader session.
pub(super) struct Actor {
    // FIFO of outbound connector requests, drained head-first into
    // `connector_tx` as channel capacity permits.
    connector_pending: Vec<derive::Request>,
    // Bounded channel out to the connector.
    connector_tx: mpsc::Sender<derive::Request>,
    // Wire codec negotiated with the connector.
    codec: connector_init::Codec,
    // RocksDB, when a Persist is not in flight (shard zero only persists).
    db: Option<crate::shard::RocksDB>,
    // RocksDB future when a Persist is in flight.
    db_persist_fut:
        Option<BoxFuture<'static, anyhow::Result<(crate::shard::RocksDB, proto::Persisted)>>>,
    // Output-combiner drain + publish future, when in flight.
    drain_fut: Option<BoxFuture<'static, anyhow::Result<drain::Output>>>,
    // Channel for sending to the leader.
    leader_tx: mpsc::UnboundedSender<proto::Derive>,
    // Per-session metrics counters.
    metrics: super::Metrics,
    // Publisher for derived documents; parked while a drain borrows it.
    publisher: Option<crate::Publisher>,
    // C:Published measures of the open transaction (reset at each L:Store).
    published_docs: u64,
    published_bytes: u64,
    // C:Published measures snapshotted at L:Store for the in-flight drain's L:Stored.
    staged_published_docs: u64,
    staged_published_bytes: u64,
    // Long-lived per-journal throttle policy, fed once per transaction
    split_policy: crate::shard::split_policy::SplitPolicy,
    split_fut: Option<crate::shard::SplitFuture>,
    // Task being executed.
    task: Arc<Task>,
    // Inferred write shape; parked while a drain borrows it.
    write_shape: Option<doc::Shape>,
}

impl Actor {
    pub fn new(
        codec: connector_init::Codec,
        connector_tx: mpsc::Sender<derive::Request>,
        db: crate::shard::RocksDB,
        leader_tx: mpsc::UnboundedSender<proto::Derive>,
        metrics: super::Metrics,
        publisher: crate::Publisher,
        task: Arc<Task>,
        write_shape: doc::Shape,
    ) -> Self {
        Self {
            connector_pending: Vec::new(),
            connector_tx,
            codec,
            db: Some(db),
            db_persist_fut: None,
            drain_fut: None,
            leader_tx,
            metrics,
            publisher: Some(publisher),
            published_docs: 0,
            published_bytes: 0,
            staged_published_docs: 0,
            staged_published_bytes: 0,
            split_policy: crate::shard::split_policy::SplitPolicy::new(),
            split_fut: None,
            task,
            write_shape: Some(write_shape),
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
        R: futures::Stream<Item = tonic::Result<proto::Derive>> + Send + Unpin + 'static,
        C: futures::Stream<Item = tonic::Result<derive::Response>> + Send + Unpin + 'static,
        L: futures::Stream<Item = tonic::Result<proto::Derive>> + Send + Unpin + 'static,
    {
        // Source-document validators, indexed by transform. Built once and lent
        // to each `Scanner::step` to re-validate documents that the shuffle read
        // pipeline flagged as schema-invalid.
        let mut source_validators: Vec<doc::Validator> = self.task.source_validators()?;

        // Double-buffered output combiners: one drains while the other accumulates.
        let mut accumulator = accumulator;
        let mut accumulator_idle = Some(
            crate::Accumulator::new(self.task.combine_spec()?)
                .context("building derive output combiner")?,
        );
        let mut phase = Phase::Idle {
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
            };
            tracing::trace!(
                loop_count,
                connector_pending_len = self.connector_pending.len(),
                drain_in_flight = self.drain_fut.is_some(),
                persist_in_flight = self.db_persist_fut.is_some(),
                phase = phase_kind,
                split_in_flight = self.split_fut.is_some(),
                "shard derive Actor::serve iteration"
            );

            let wake_connector_tx = self.try_connector_tx();

            // Drive the scan forward when able. On progress we loop to start IO
            // immediately, falling through to the select below only when we
            // can't send more to `connector_tx`.
            if !self.connector_pending.is_empty() {
                // Channel is stuffed -- don't queue further requests.
            } else if let Phase::Scanning(mut scanner) = phase {
                if scanner.step(
                    &self.task.transforms,
                    &mut source_validators,
                    self.codec,
                    &mut self.connector_pending,
                )? {
                    phase = Phase::Scanning(scanner);
                } else {
                    let (shuffle_reader, shuffle_remainders, active) = scanner.into_parts();
                    let combiner_usage_bytes = accumulator.combiner_byte_usage();

                    service_kit::event!(
                        tracing::Level::DEBUG,
                        "leader",
                        active_bindings = active.len(),
                        combiner_usage_bytes,
                        "sending L:Loaded after frontier scan",
                    );
                    self.metrics.scans_completed.increment(1);

                    _ = self.leader_tx.send(proto::Derive {
                        loaded: Some(proto::derive::Loaded {
                            bindings: active.into_values().collect(),
                            combiner_usage_bytes,
                        }),
                        ..Default::default()
                    });
                    phase = Phase::Idle {
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
                    self.on_connector_response(&mut accumulator, msg)?;
                }
                // Next, a leader message.
                msg = leader_rx.next() => {
                    let (next, stopped) =
                        self.on_leader_message(phase, &mut accumulator, &mut accumulator_idle, msg)?;
                    phase = next;

                    if stopped {
                        break;
                    }
                }
                // Next, a controller message.
                msg = controller_rx.next() => {
                    self.on_controller_request(msg)?;
                }
                // A Persist completion (shard zero).
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

                    _ = self.leader_tx.send(proto::Derive {
                        persisted: Some(persisted),
                        ..Default::default()
                    });
                }
                // A drain + publish completion.
                result = maybe_fut(&mut self.drain_fut) => {
                    let output = result?;
                    accumulator_idle = Some(output.accumulator);
                    self.publisher = Some(output.publisher);
                    self.write_shape = Some(output.write_shape);
                    self.drain_fut = None;

                    // The drain just flushed this transaction's collection appends,
                    // check the throttling stats
                    self.observe_throttle();

                    service_kit::event!(
                        tracing::Level::DEBUG,
                        "leader",
                        drained_docs = output.drained_docs,
                        "drain completed; sending L:Stored",
                    );
                    self.metrics.drains_completed.increment(1);

                    _ = self.leader_tx.send(proto::Derive {
                        stored: Some(proto::derive::Stored {
                            published_docs_total: self.staged_published_docs,
                            published_bytes_total: self.staged_published_bytes,
                            drained_docs_total: output.drained_docs,
                            drained_bytes_total: output.drained_bytes,
                            publisher_commit: output.publisher_commit,
                        }),
                        ..Default::default()
                    });
                }
                // An automatic journal-split completion.
                (journal, outcome) = maybe_fut(&mut self.split_fut) => {
                    crate::shard::finish_split(
                        &mut self.split_policy,
                        &journal,
                        outcome,
                        std::time::Instant::now(),
                    );
                }
                // Wait for capacity to send to the connector.
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
        let verify = crate::verify("Derive", "leader EOF after Stopped", "leader");
        verify.eof(leader_rx.next().await)?;

        let Some(db) = self.db.take() else {
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
        accumulator: &mut crate::Accumulator,
        accumulator_idle: &mut Option<crate::Accumulator>,
        msg: Option<tonic::Result<proto::Derive>>,
    ) -> anyhow::Result<(Phase, bool)> {
        let verify = crate::verify("Derive", "leader message", "leader");
        let msg = verify.not_eof(msg)?;

        if let Some(proto::Stopped {}) = msg.stopped {
            return Ok((phase, true));
        } else if let Some(proto::derive::Load {
            frontier: Some(frontier),
        }) = msg.load
        {
            let frontier =
                shuffle::Frontier::decode(frontier).context("invalid Frontier on L:Load")?;

            let Phase::Idle {
                shuffle_reader,
                shuffle_remainders,
            } = phase
            else {
                anyhow::bail!("L:Load received while actor is not idle");
            };

            let scanner = scan::Scanner::new(frontier, shuffle_reader, shuffle_remainders)?;
            return Ok((Phase::Scanning(scanner), false));
        } else if let Some(proto::derive::Flush {
            connector_patches_json,
        }) = msg.flush
        {
            self.connector_pending.push(derive::Request {
                flush: Some(derive::request::Flush {
                    state_patches_json: connector_patches_json,
                }),
                ..Default::default()
            });
        } else if let Some(proto::derive::Store {}) = msg.store {
            // Rotate the active combiner into a drain; accumulate the next
            // transaction into the recycled idle combiner.
            let recycled = accumulator_idle
                .take()
                .context("L:Store while a drain is still in flight")?;
            let active = std::mem::replace(accumulator, recycled);
            let (drainer, parser) = active.into_drainer().context("preparing combiner drain")?;

            let publisher = self
                .publisher
                .take()
                .context("L:Store while publisher is busy")?;
            let write_shape = self
                .write_shape
                .take()
                .context("L:Store while write shape is busy")?;

            self.staged_published_docs = std::mem::take(&mut self.published_docs);
            self.staged_published_bytes = std::mem::take(&mut self.published_bytes);

            let task = Arc::clone(&self.task);
            let metrics = self.metrics.clone();
            self.drain_fut = Some(
                async move {
                    drain::drain_and_publish(drainer, parser, publisher, task, write_shape, metrics)
                        .await
                }
                .boxed(),
            );
        } else if let Some(proto::derive::StartCommit {
            connector_checkpoint,
        }) = msg.start_commit
        {
            self.connector_pending.push(derive::Request {
                start_commit: Some(derive::request::StartCommit {
                    runtime_checkpoint: connector_checkpoint,
                }),
                ..Default::default()
            });
        } else if let Some(persist) = msg.persist {
            let seq_no = persist.seq_no;
            let db = self
                .db
                .take()
                .context("received L:Persist while a Persist is already in flight")?;
            let task = Arc::clone(&self.task);

            self.db_persist_fut = Some(
                async move {
                    let db = db.persist(&persist, &task.binding_state_keys).await?;
                    Ok((db, proto::Persisted { seq_no }))
                }
                .boxed(),
            );
        } else {
            return Err(verify.fail_msg(msg));
        }

        Ok((phase, false))
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

    fn on_connector_response(
        &mut self,
        accumulator: &mut crate::Accumulator,
        resp: Option<tonic::Result<derive::Response>>,
    ) -> anyhow::Result<()> {
        let verify = crate::verify("Derive", "connector response", "connector");
        let resp = verify.not_eof(resp)?;

        if let Some(derive::response::Published { doc_json }) = resp.published {
            let (memtable, alloc, mut doc) = accumulator
                .parse_json_doc(&doc_json)
                .context("couldn't parse derived document as JSON")?;

            let uuid_ptr = &self.task.document_uuid_ptr;
            if !uuid_ptr.0.is_empty() {
                let Ok(_) = doc.try_set(
                    uuid_ptr,
                    doc::HeapNode::String(doc::BumpStr::from_str(crate::UUID_PLACEHOLDER, alloc)),
                    alloc,
                ) else {
                    anyhow::bail!("unable to create document UUID placeholder");
                };
            }
            memtable.add(0, doc, false)?;

            self.published_docs += 1;
            self.published_bytes += doc_json.len() as u64;
            self.metrics.published_docs.increment(1);
            self.metrics
                .published_bytes
                .increment(doc_json.len() as u64);
        } else if let Some(derive::response::Flushed { state, more }) = resp.flushed {
            _ = self.leader_tx.send(proto::Derive {
                flushed: Some(proto::derive::Flushed {
                    connector_patches_json: patches::encode_connector_state(state),
                    more,
                }),
                ..Default::default()
            });
        } else if let Some(derive::response::StartedCommit { state }) = resp.started_commit {
            // V2 connectors report state via Flushed; StartedCommit.state is the
            // deprecated V1 path and must be empty here.
            if let Some(state) = &state {
                anyhow::ensure!(
                    state.updated_json.is_empty(),
                    "connector C:StartedCommit carried a state update, which the V2 runtime does not accept",
                );
            }
            _ = self.leader_tx.send(proto::Derive {
                started_commit: Some(proto::derive::StartedCommit {}),
                ..Default::default()
            });
        } else {
            return Err(verify.fail_msg(resp));
        }

        Ok(())
    }

    fn on_controller_request(
        &mut self,
        msg: Option<tonic::Result<proto::Derive>>,
    ) -> anyhow::Result<()> {
        let verify = crate::verify("Derive", "Stop or CloseNow", "controller");
        let msg = verify.not_eof(msg)?;

        if matches!(msg.stop, Some(proto::Stop {})) {
            _ = self.leader_tx.send(proto::Derive {
                stop: Some(proto::Stop {}),
                ..Default::default()
            });
        } else if matches!(msg.close_now, Some(proto::CloseNow {})) {
            _ = self.leader_tx.send(proto::Derive {
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
    use super::super::task::Transform;
    use super::*;
    use proto_flow::derive::response;
    use proto_flow::flow;
    use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};

    fn test_task() -> Task {
        Task {
            collection_name: "test/derived".to_string(),
            document_uuid_ptr: json::Pointer::from("/_meta/uuid"),
            key_extractors: Vec::new(),
            redact_salt: bytes::Bytes::new(),
            transforms: vec![Transform {
                transform: "fromSrc".to_string(),
                collection: "test/source".to_string(),
                schema_json: bytes::Bytes::from_static(b"{}"),
                shuffle_key_extractors: Vec::new(),
            }],
            binding_state_keys: vec!["fromSrc".to_string()],
            write_schema_json: bytes::Bytes::from_static(b"{}"),
            write_shape: doc::Shape::nothing(),
        }
    }

    /// `observe_throttle` parks at most one split for a due journal, never
    /// replaces an in-flight split, and is suppressed by cooldown and by the
    /// terminal `ignore` set.
    #[tokio::test]
    async fn observe_throttle_split_dispatch() {
        let (actor_to_conn_tx, _conn_rx) = mpsc::channel::<derive::Request>(crate::CHANNEL_BUFFER);
        let (actor_to_leader_tx, _leader_rx) = mpsc::unbounded_channel::<proto::Derive>();
        let task = Arc::new(test_task());

        let spec = flow::CollectionSpec {
            name: task.collection_name.clone(),
            partition_template: Some(proto_gazette::broker::JournalSpec {
                name: "test/derived/v1".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let publisher = crate::Publisher::new_test_real([&spec]);
        let write_shape = task.write_shape.clone();

        let mut actor = Actor::new(
            connector_init::Codec::Proto,
            actor_to_conn_tx,
            crate::shard::RocksDB::open(None).await.unwrap(),
            actor_to_leader_tx,
            super::super::Metrics::new("test/shard"),
            publisher,
            task,
            write_shape,
        );

        // Seed a policy under which the observed journal is immediately due.
        const J: &str = "test/derived/v1/pivot=00";
        actor.split_policy = crate::shard::split_policy::SplitPolicy::with_config(
            crate::shard::split_policy::Config {
                threshold: -1.0,
                min_observation_span: std::time::Duration::ZERO,
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

    /// Drive `Actor::serve` end-to-end over mpsc channels standing in for the
    /// connector, leader, and controller. Walks a derive transaction:
    /// C:Published → L:Flush/C:Flush/C:Flushed/L:Flushed → L:Store/drain/L:Stored
    /// → L:StartCommit/C:StartCommit/C:StartedCommit/L:StartedCommit →
    /// L:Persist/L:Persisted → controller Stop/CloseNow → L:Stopped. Asserts the
    /// actor translates leader commands into connector requests, accumulates and
    /// publishes a derived document, persists state, and forwards controller
    /// signals.
    #[tokio::test]
    async fn full_lifecycle_round_trip() {
        let (actor_to_conn_tx, mut actor_to_conn_rx) =
            mpsc::channel::<derive::Request>(crate::CHANNEL_BUFFER);
        let (conn_to_actor_tx, conn_to_actor_rx) =
            mpsc::channel::<tonic::Result<derive::Response>>(crate::CHANNEL_BUFFER);
        let (actor_to_leader_tx, mut actor_to_leader_rx) =
            mpsc::unbounded_channel::<proto::Derive>();
        let (leader_to_actor_tx, leader_to_actor_rx) =
            mpsc::unbounded_channel::<tonic::Result<proto::Derive>>();
        let (controller_to_actor_tx, controller_to_actor_rx) =
            mpsc::unbounded_channel::<tonic::Result<proto::Derive>>();

        let task = Arc::new(test_task());
        let accumulator = crate::Accumulator::new(task.combine_spec().unwrap()).unwrap();
        let publisher = crate::Publisher::new_preview([&flow::CollectionSpec {
            name: task.collection_name.clone(),
            ..Default::default()
        }]);
        let write_shape = task.write_shape.clone();
        let db = crate::shard::RocksDB::open(None).await.unwrap();
        let shuffle_dir = tempfile::tempdir().unwrap();
        let shuffle_reader = shuffle::log::Reader::new(shuffle_dir.path(), 0);

        let actor = Actor::new(
            connector_init::Codec::Proto,
            actor_to_conn_tx,
            db,
            actor_to_leader_tx,
            super::super::Metrics::new("test/shard"),
            publisher,
            task,
            write_shape,
        );

        let serve_handle = tokio::spawn(async move {
            let mut conn_stream = ReceiverStream::new(conn_to_actor_rx);
            let mut leader_stream = UnboundedReceiverStream::new(leader_to_actor_rx);
            let mut controller_stream = UnboundedReceiverStream::new(controller_to_actor_rx);
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

        // 1) The connector publishes one derived document into the combiner.
        conn_to_actor_tx
            .send(Ok(derive::Response {
                published: Some(response::Published {
                    doc_json: bytes::Bytes::from_static(br#"{"id":"a","_meta":{"uuid":""}}"#),
                }),
                ..Default::default()
            }))
            .await
            .unwrap();

        // 2) L:Flush → C:Flush → C:Flushed → L:Flushed.
        leader_to_actor_tx
            .send(Ok(proto::Derive {
                flush: Some(proto::derive::Flush {
                    connector_patches_json: bytes::Bytes::from_static(br#"[{"f":1}]"#),
                }),
                ..Default::default()
            }))
            .unwrap();

        let req = actor_to_conn_rx.recv().await.unwrap();
        assert_eq!(
            req.flush.unwrap().state_patches_json,
            bytes::Bytes::from_static(br#"[{"f":1}]"#),
        );

        conn_to_actor_tx
            .send(Ok(derive::Response {
                flushed: Some(response::Flushed {
                    state: None,
                    more: false,
                }),
                ..Default::default()
            }))
            .await
            .unwrap();

        let resp = actor_to_leader_rx.recv().await.unwrap();
        let flushed = resp.flushed.unwrap();
        assert!(!flushed.more);

        // 3) L:Store → drain + publish → L:Stored (published=1, drained=1).
        leader_to_actor_tx
            .send(Ok(proto::Derive {
                store: Some(proto::derive::Store {}),
                ..Default::default()
            }))
            .unwrap();

        let resp = actor_to_leader_rx.recv().await.unwrap();
        let stored = resp.stored.unwrap();
        assert_eq!(stored.published_docs_total, 1);
        assert_eq!(stored.drained_docs_total, 1);
        // Preview publisher reports no commit.
        assert!(stored.publisher_commit.is_none());

        // 4) L:StartCommit → C:StartCommit → C:StartedCommit → L:StartedCommit.
        leader_to_actor_tx
            .send(Ok(proto::Derive {
                start_commit: Some(proto::derive::StartCommit {
                    connector_checkpoint: Some(proto_gazette::consumer::Checkpoint::default()),
                }),
                ..Default::default()
            }))
            .unwrap();

        let req = actor_to_conn_rx.recv().await.unwrap();
        assert!(req.start_commit.unwrap().runtime_checkpoint.is_some());

        conn_to_actor_tx
            .send(Ok(derive::Response {
                started_commit: Some(response::StartedCommit { state: None }),
                ..Default::default()
            }))
            .await
            .unwrap();

        let resp = actor_to_leader_rx.recv().await.unwrap();
        assert!(resp.started_commit.is_some());

        // 5) L:Persist → RocksDB write → L:Persisted echoes seq_no.
        leader_to_actor_tx
            .send(Ok(proto::Derive {
                persist: Some(proto::Persist {
                    seq_no: 42,
                    last_applied: bytes::Bytes::from_static(b"persisted-spec-bytes"),
                    ..Default::default()
                }),
                ..Default::default()
            }))
            .unwrap();

        let resp = actor_to_leader_rx.recv().await.unwrap();
        assert_eq!(resp.persisted.unwrap().seq_no, 42);

        // 6) Controller Stop + CloseNow → forwarded to the leader.
        controller_to_actor_tx
            .send(Ok(proto::Derive {
                stop: Some(proto::Stop {}),
                ..Default::default()
            }))
            .unwrap();
        assert!(actor_to_leader_rx.recv().await.unwrap().stop.is_some());

        controller_to_actor_tx
            .send(Ok(proto::Derive {
                close_now: Some(proto::CloseNow {}),
                ..Default::default()
            }))
            .unwrap();
        assert!(actor_to_leader_rx.recv().await.unwrap().close_now.is_some());

        // 7) L:Stopped + leader EOF → serve completes, returning the DB.
        leader_to_actor_tx
            .send(Ok(proto::Derive {
                stopped: Some(proto::Stopped {}),
                ..Default::default()
            }))
            .unwrap();
        std::mem::drop(leader_to_actor_tx);

        let db = serve_handle.await.unwrap().unwrap();

        // Confirm the Persist round-tripped.
        let (_db, recover) = db.scan(Vec::new()).await.unwrap();
        assert_eq!(recover.last_applied.as_ref(), b"persisted-spec-bytes");
    }
}
