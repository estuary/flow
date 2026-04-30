//! Per-session shard-side actor.
//!
//! Owns all per-session IO and reacts to leader and connector events. The
//! leader's FSM drives transaction pipelining; the shard's job is to react
//! to leader-driven actions (L:Load, L:Flush, L:StartCommit, L:Acknowledge,
//! L:Store, L:Persist) and connector responses, updating per-session state and
//! emitting the corresponding outbound messages. There is intentionally no
//! shard-side FSM: per-session bookkeeping lives in fields on `Actor`.
//!
//! Shape:
//!  - Three IO endpoints: leader_tx/rx, connector_tx/rx, controller_tx
//!    (the controller is mostly write-only from the actor; reads are
//!    limited to the `Stop` handshake).
//!  - The actor owns the shuffle log Reader and Remainder state and runs
//!    `FrontierScan`s inline. Each scan yields between segment blocks via
//!    `tokio::task::yield_now()`. Source documents are added to the
//!    combiner as `ArchivedNode` directly (`MemTable::add_embedded`),
//!    skipping any per-document JSON parse.
//!  - The RocksDB is held as `Option<RocksDB>` (single ownership).
//!    Dispatching a Persist takes the DB into a short-lived
//!    `spawn_blocking` future; the future returns it back when the
//!    WriteBatch has been durably synced. The slot is `None` only while a
//!    Persist is in flight — the leader never sends another L:Persist
//!    until the matching L:Persisted has been observed.
//!
//! Persist#1 fence on C:Store: the combiner drain begins only after L:Store
//! is received from the leader. The leader sends L:Store only after Persist#1
//! is durable on shard zero, so this gate is the runtime-side enforcement
//! that no C:Store runs before max-keys are durable.

#![allow(dead_code)]

use super::state::{DrainStoresComplete, LoadDeltas, ScanComplete};
use super::{LoadKeySet, Task};
use crate::Accumulator;
use crate::proto;
use crate::rocksdb::RocksDB;
use crate::verify_send;
use anyhow::Context;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{FutureExt, SinkExt, StreamExt, future::BoxFuture};
use proto_flow::materialize;
use shuffle::log::{FrontierScan, Reader, Remainder};
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use xxhash_rust::xxh3::xxh3_128;

/// Per-session resources and state owned by the actor for the duration of
/// one leader session.
pub struct Actor {
    /// Stream to the controller (downstream); receives final L:Stopped +
    /// runtime_checkpoint forwards as well as protocol errors.
    pub controller_tx: tokio::sync::mpsc::Sender<tonic::Result<proto::Materialize>>,

    /// Outbound to the leader sidecar.
    pub leader_tx: tokio::sync::mpsc::Sender<proto::Materialize>,
    /// Inbound from the leader sidecar.
    pub leader_rx: BoxStream<'static, tonic::Result<proto::Materialize>>,

    /// Outbound to the connector RPC.
    pub connector_tx: futures::channel::mpsc::Sender<materialize::Request>,
    /// Inbound from the connector RPC.
    pub connector_rx: BoxStream<'static, tonic::Result<materialize::Response>>,

    /// Owned RocksDB. Shard zero only; non-zero shards have None. While a
    /// Persist is in flight the DB is moved into `persist_fut` and this
    /// slot is `None`; it is restored when `Persisted` is observed.
    pub rocksdb: Option<RocksDB>,
    /// Stable binding state_keys, passed with every Persist.
    pub binding_state_keys: Arc<Vec<String>>,

    /// Combiner accumulator: source docs and C:Loaded responses fold into here.
    /// `None` only between drain start and recycle.
    pub(crate) accumulator: Option<Accumulator>,
    /// Per-session Task built from the C:Open request. Used by the combiner
    /// drain path to extract keys/values when emitting C:Store.
    pub task: Option<Task>,
    /// Durably-persisted maximum packed key per binding `state_key`,
    /// loaded from `L:Recovered`. A source document's key larger than this
    /// committed value cannot exist in the materialized endpoint, so the
    /// connector `Load` is skipped.
    pub committed_max_keys: BTreeMap<u32, Bytes>,
    /// New high-water packed keys observed during Load. Reduces with
    /// `committed_max_keys` and is durably committed in Persist #1.
    pub next_max_keys: BTreeMap<u32, Bytes>,
    /// Per-transaction set of source-doc keys for which a `C:Load` has
    /// already been emitted; used to dedupe redundant loads. Cleared at
    /// the start of each combiner drain.
    pub load_keys: LoadKeySet,

    /// Shuffle log Reader, owned for the session. `None` only briefly while a
    /// `FrontierScan` is being constructed inside `scan_frontier`. Re-installed
    /// after the scan returns its parts.
    pub shuffle_reader: Option<Reader>,
    /// Block remainders carried across scans of this session.
    pub shuffle_remainders: VecDeque<Remainder>,

    /// Pending Persist future. Resolves to the returned RocksDB and the
    /// `Persisted` reply; both are restored to the actor on completion.
    /// Also serves as the "Persist in flight?" flag — `Some` iff a
    /// WriteBatch is pending durable sync.
    pub persist_fut:
        Option<BoxFuture<'static, Result<(RocksDB, proto::Persisted), rocksdb::Error>>>,

    /// In-progress combiner drain. Holds the live drainer, its parser
    /// (recycled into a fresh Accumulator on completion), the Task, and
    /// the running per-binding summary. Each loop iteration advances the
    /// drain by some bounded chunk before yielding back to the main
    /// `select!` so `leader_rx` (Persist#1 messages) and `persist_fut`
    /// keep getting serviced. Without this chunking a slow connector
    /// would block the actor's main task for the entire drain — observed
    /// as a ~30s gap between L:Flushed and L:Persisted on transactions
    /// with many C:Stores.
    pub drain_state: Option<DrainState>,

    /// Per-transaction stats accumulator. Drained piece-by-piece into
    /// L:Loaded, L:Flushed, and L:StartedCommit.
    pub deltas: LoadDeltas,

    /// Accumulating `JournalFrontier` entries from streamed L:Load chunks.
    /// On the terminator chunk a single `FrontierScan` covers the full
    /// delta and this is cleared.
    pub frontier_journals: Vec<shuffle::frontier::JournalFrontier>,

    /// True after C:Flushed has been observed for the current transaction.
    /// L:Store is only valid after this point.
    pub c_flushed_received: bool,

    /// True after L:StartCommit has been received for the current transaction.
    pub received_l_start_commit: bool,

    /// Aggregated peer state patches stashed on L:StartCommit; forwarded
    /// verbatim with C:StartCommit after the drain completes. Includes
    /// this shard's own contribution for symmetry with the multi-shard
    /// case.
    pub peer_patches_for_start_commit: Bytes,

    /// In-progress `WriteBatch` accumulating effects of streamed L:Persist
    /// messages since the last terminator (a Persist with `nonce != 0`).
    /// Shard zero only; non-zero shards never see L:Persist messages, so
    /// the batch stays empty.
    pub persist_batch: rocksdb::WriteBatch,
}

/// Live combiner drain in progress. Owned by the actor between the
/// transition into the Stores drain and its completion.
pub struct DrainState {
    pub drainer: doc::combine::Drainer,
    pub parser: simd_doc::Parser,
    pub task: Task,
    pub summary: DrainStoresComplete,
}

/// Maximum number of C:Store messages emitted per `drive_drain` chunk.
/// One per chunk so the main `select!` polls `persist_fut` and
/// `leader_rx` between every store — Persist#1 application interleaves
/// with the Stores drain rather than serializing after it.
const DRAIN_CHUNK_STORES: usize = 1;

impl Actor {
    /// Drive IO and reactor handlers until the session ends (L:Stopped or
    /// error).
    ///
    /// `controller_rx` is borrowed for the session's lifetime so the actor
    /// can react to controller-side `Stop` messages (or stream EOF) by
    /// forwarding `L:Stop` to the leader. The controller-facing stream
    /// itself is owned by the handler and survives across leader sessions,
    /// so it's lent by `&mut` rather than moved. Generic over the stream
    /// type so unit tests can pass any stream of `proto::Materialize`.
    #[tracing::instrument(level = "debug", err(Debug, level = "warn"), skip_all)]
    pub async fn serve<S>(&mut self, controller_rx: &mut S) -> anyhow::Result<()>
    where
        S: futures::Stream<Item = tonic::Result<proto::Materialize>> + Unpin,
    {
        // Once we've forwarded a Stop to the leader, suppress further
        // controller reads — controller EOF or repeat Stops are no-ops.
        let mut stop_forwarded = false;
        // Set when we've observed L:Stopped from the leader and forwarded
        // it on to the controller. Last loop iteration flushes the select!
        // arms and returns.
        let mut session_done = false;

        while !session_done {
            // Biased ordering: Persisted, then leader_rx (notably
            // Persist#1 messages), drain ahead of connector_rx and the
            // drain-step fallback. This keeps Persist application
            // progressing concurrently with an in-flight Stores drain.
            tokio::select! {
                biased;

                Some(persisted_result) = maybe_fut(&mut self.persist_fut) => {
                    let (db, persisted) = persisted_result.context("RocksDB Persist")?;
                    self.rocksdb = Some(db);
                    self.handle_persisted(persisted.nonce).await?;
                }
                Some(result) = self.leader_rx.next() => {
                    let msg: proto::Materialize = result.map_err(crate::status_to_anyhow)
                        .context("leader stream")?;
                    if self.handle_leader_msg(msg).await? {
                        session_done = true;
                    }
                }
                Some(result) = self.connector_rx.next() => {
                    let resp: materialize::Response = result.context("connector stream")?;
                    self.handle_connector_resp(resp).await?;
                }
                // Drain the combiner by one chunk. `std::future::ready(())`
                // is always-ready, so this arm always wins when no higher-
                // priority event is. Biased ordering above guarantees
                // Persisted/leader_rx pre-empt drain on every iteration —
                // Persist#1 application happens concurrently with the
                // long Stores drain rather than serialized after it.
                _ = std::future::ready(()), if self.drain_state.is_some() => {
                    self.drive_drain().await?;
                }
                controller_msg = controller_rx.next(), if !stop_forwarded => {
                    // A message (or EOF) from the controller during an active
                    // session. The only protocol-legal message is Stop; EOF
                    // is treated as an implicit Stop so the leader can drive
                    // a clean shutdown via L:Stopped.
                    match controller_msg {
                        None => {
                            self.forward_stop_to_leader().await?;
                            stop_forwarded = true;
                        }
                        Some(Err(status)) => {
                            return Err(crate::status_to_anyhow(status)
                                .context("controller stream error during session"));
                        }
                        Some(Ok(msg)) if msg.stop.is_some() => {
                            self.forward_stop_to_leader().await?;
                            stop_forwarded = true;
                        }
                        Some(Ok(msg)) => {
                            anyhow::bail!(
                                "unexpected controller message during session (only Stop is legal): {msg:?}"
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Forward `L:Stop` to the leader. Idempotent: callers must guard with
    /// `stop_forwarded` so we send exactly once per session.
    async fn forward_stop_to_leader(&mut self) -> anyhow::Result<()> {
        self.leader_tx
            .send(proto::Materialize {
                stop: Some(proto::Stop {}),
                ..Default::default()
            })
            .await
            .context("forwarding Stop to leader")?;
        Ok(())
    }

    /// Dispatch a single inbound leader message to the appropriate
    /// handler. Returns `true` if the message ended the session
    /// (L:Stopped).
    async fn handle_leader_msg(&mut self, msg: proto::Materialize) -> anyhow::Result<bool> {
        if msg.stopped.is_some() {
            // Forward to the controller and end the session. Per the plan,
            // post-commit work after Persist #2 is intentionally deferred
            // to the next leader session via recovery, so it's safe to
            // finalize any in-flight transaction state here. (Persist
            // replies for in-flight WriteBatches still drain via
            // persist_fut.)
            verify_send(
                &self.controller_tx,
                Ok(proto::Materialize {
                    stopped: Some(proto::Stopped {}),
                    ..Default::default()
                }),
            )
            .context("forwarding Stopped to controller")?;
            return Ok(true);
        }
        if let Some(load) = msg.load {
            self.on_l_load(load).await?;
        } else if let Some(flush) = msg.flush {
            self.on_l_flush(flush).await?;
        } else if let Some(store) = msg.store {
            self.on_l_store(store).await?;
        } else if let Some(start_commit) = msg.start_commit {
            self.on_l_start_commit(start_commit).await?;
        } else if let Some(ack) = msg.acknowledge {
            self.on_l_acknowledge(ack).await?;
        } else if let Some(persist) = msg.persist {
            self.on_l_persist(persist).await?;
        } else {
            anyhow::bail!("unexpected leader message: {msg:?}");
        }
        Ok(false)
    }

    /// Dispatch a single inbound connector response to the appropriate
    /// handler.
    async fn handle_connector_resp(&mut self, resp: materialize::Response) -> anyhow::Result<()> {
        if resp.loaded.is_some() {
            self.on_c_loaded(resp).await
        } else if resp.flushed.is_some() {
            self.on_c_flushed(resp).await
        } else if resp.started_commit.is_some() {
            self.on_c_started_commit(resp).await
        } else if resp.acknowledged.is_some() {
            self.on_c_acknowledged(resp).await
        } else {
            anyhow::bail!("unexpected connector response: {resp:?}");
        }
    }

    /// L:Load: append the chunk's `JournalFrontier` entries into the
    /// per-transaction accumulator. On the terminator chunk (empty
    /// journals + populated `flushed_lsn`), run a single `FrontierScan`
    /// covering the full delta, fold the result into deltas, and emit
    /// L:Loaded for this round.
    async fn on_l_load(&mut self, load: proto::materialize::Load) -> anyhow::Result<()> {
        let Some(chunk) = load.frontier else {
            return Ok(());
        };
        let is_terminator = chunk.journals.is_empty() && !chunk.flushed_lsn.is_empty();
        let flushed_lsn = chunk.flushed_lsn.clone();
        for jf in shuffle::frontier::JournalFrontier::decode(chunk) {
            self.frontier_journals.push(jf);
        }

        if !is_terminator {
            return Ok(());
        }

        let journals = std::mem::take(&mut self.frontier_journals);
        tracing::debug!(
            journal_count = journals.len(),
            flushed_lsn_count = flushed_lsn.len(),
            "L:Load terminator received; running ScanFrontier"
        );
        let delta = shuffle::Frontier::new(journals, flushed_lsn)
            .context("invalid Frontier delta on L:Load")?;
        let summary = self.scan_frontier(delta).await?;
        tracing::debug!(
            binding_read = ?summary.binding_read,
            combiner_usage_bytes = summary.combiner_usage_bytes,
            max_keys = summary.max_key_deltas.len(),
            "ScanFrontier complete"
        );
        self.fold_scan_complete(summary);
        self.send_l_loaded().await?;
        Ok(())
    }

    fn fold_scan_complete(&mut self, scan: ScanComplete) {
        for (binding, dab) in scan.binding_read {
            let entry = self.deltas.binding_read.entry(binding).or_default();
            entry.docs_total += dab.docs_total;
            entry.bytes_total += dab.bytes_total;
        }
        for (key, value) in scan.max_key_deltas {
            self.deltas.max_key_deltas.insert(key, value);
        }
        self.deltas.combiner_usage_bytes = scan.combiner_usage_bytes;

        for (binding, clock) in scan.first_source_clock {
            self.deltas
                .first_source_clock
                .entry(binding)
                .and_modify(|prev| *prev = (*prev).min(clock))
                .or_insert(clock);
        }
        for (binding, clock) in scan.last_source_clock {
            self.deltas
                .last_source_clock
                .entry(binding)
                .and_modify(|prev| *prev = (*prev).max(clock))
                .or_insert(clock);
        }
    }

    /// Emit L:Loaded for the current Load round. Per-round counters
    /// (`binding_read`, `binding_loaded`, `max_key_deltas`,
    /// `combiner_usage_bytes`) are drained; cross-transaction counters
    /// remain on `self.deltas` for L:StartedCommit.
    async fn send_l_loaded(&mut self) -> anyhow::Result<()> {
        let loaded = proto::materialize::Loaded {
            combiner_usage_bytes: self.deltas.combiner_usage_bytes,
            max_key_deltas: std::mem::take(&mut self.deltas.max_key_deltas),
            binding_read: std::mem::take(&mut self.deltas.binding_read),
            binding_loaded: std::mem::take(&mut self.deltas.binding_loaded),
        };
        self.leader_tx
            .send(proto::Materialize {
                loaded: Some(loaded),
                ..Default::default()
            })
            .await
            .context("send L:Loaded")?;
        Ok(())
    }

    /// L:Flush: forward as C:Flush to the connector. The leader sends
    /// L:Flush only after we've sent L:Acknowledged for the prior
    /// transaction (which we do exactly when C:Acknowledged arrives), so
    /// the connector-protocol requirement that C:Flush follow
    /// C:Acknowledge is structurally satisfied — no runtime gate needed.
    /// The patches are forwarded verbatim, including this shard's own
    /// contribution (symmetric with multi-shard).
    async fn on_l_flush(&mut self, flush: proto::materialize::Flush) -> anyhow::Result<()> {
        self.connector_tx
            .send(materialize::Request {
                flush: Some(materialize::request::Flush {
                    connector_state_patches_json: flush.connector_patches_json,
                }),
                ..Default::default()
            })
            .await
            .context("send C:Flush")?;
        Ok(())
    }

    /// L:Store: begin draining the combiner into C:Store requests. This is
    /// sent by the leader only after the idempotency Persist is durable.
    async fn on_l_store(&mut self, _store: proto::materialize::Store) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.c_flushed_received,
            "received L:Store before C:Flushed completed"
        );
        self.start_drain()?;
        Ok(())
    }

    /// L:StartCommit: forward as C:StartCommit after the leader has observed
    /// L:Stored from every shard and assembled the transaction checkpoint.
    async fn on_l_start_commit(
        &mut self,
        sc: proto::materialize::StartCommit,
    ) -> anyhow::Result<()> {
        self.received_l_start_commit = true;
        self.peer_patches_for_start_commit = sc.connector_patches_json;
        let connector_state_patches_json = std::mem::take(&mut self.peer_patches_for_start_commit);
        self.connector_tx
            .send(materialize::Request {
                start_commit: Some(materialize::request::StartCommit {
                    runtime_checkpoint: sc.connector_checkpoint,
                    connector_state_patches_json,
                }),
                ..Default::default()
            })
            .await
            .context("send C:StartCommit")?;
        Ok(())
    }

    /// L:Acknowledge: forward as C:Acknowledge to the connector,
    /// carrying the all-shards-aggregated C:StartedCommit patches
    /// verbatim (including this shard's own contribution, for symmetry
    /// with multi-shard). Also notifies the controller for observability.
    async fn on_l_acknowledge(
        &mut self,
        ack: proto::materialize::Acknowledge,
    ) -> anyhow::Result<()> {
        // Observability fan-out: in-process controllers (e.g. `flowctl
        // preview`) pace sessions by counting transactions; the production
        // Go controller is free to ignore the envelope.
        verify_send(
            &self.controller_tx,
            Ok(proto::Materialize {
                acknowledge: Some(proto::materialize::Acknowledge::default()),
                ..Default::default()
            }),
        )
        .context("forwarding Acknowledge envelope to controller")?;

        self.connector_tx
            .send(materialize::Request {
                acknowledge: Some(materialize::request::Acknowledge {
                    connector_state_patches_json: ack.connector_patches_json,
                }),
                ..Default::default()
            })
            .await
            .context("send C:Acknowledge")?;
        Ok(())
    }

    /// L:Persist: shard zero accumulates the message onto a stack until
    /// a terminator (`nonce != 0`) is seen, at which point the stack is
    /// handed to RocksDB. Non-zero shards never see L:Persist; arriving
    /// here on a non-zero shard is a protocol error caught in the
    /// dispatch.
    async fn on_l_persist(&mut self, persist: proto::Persist) -> anyhow::Result<()> {
        let nonce = persist.nonce;
        crate::rocksdb::extend_write_batch(
            &mut self.persist_batch,
            &persist,
            self.binding_state_keys.as_slice(),
        )
        .context("encoding L:Persist into WriteBatch")?;
        if nonce == 0 {
            return Ok(());
        }

        // Terminator: dispatch the accumulated WriteBatch.
        let wb = std::mem::take(&mut self.persist_batch);
        let db = self
            .rocksdb
            .take()
            .context("L:Persist on shard with no RocksDB (or one already in flight)")?;

        self.persist_fut = Some(
            async move {
                let mut wo = rocksdb::WriteOptions::new();
                wo.set_sync(true);
                let db = db.write_opt(wb, wo).await?;
                Ok((db, proto::Persisted { nonce }))
            }
            .boxed(),
        );

        Ok(())
    }

    /// Persist completion from RocksDB: emit L:Persisted carrying the
    /// nonce returned by RocksDB (which echoes the terminator we sent
    /// down on the persist_stack).
    async fn handle_persisted(&mut self, nonce: u64) -> anyhow::Result<()> {
        self.leader_tx
            .send(proto::Materialize {
                persisted: Some(proto::Persisted { nonce }),
                ..Default::default()
            })
            .await
            .context("send L:Persisted")?;
        Ok(())
    }

    /// C:Loaded: fold the loaded doc into the combiner with `front=true`
    /// so it reduces with subsequent input, and accumulate per-binding
    /// stats.
    async fn on_c_loaded(&mut self, resp: materialize::Response) -> anyhow::Result<()> {
        let loaded = resp
            .loaded
            .as_ref()
            .expect("dispatched on resp.loaded.is_some()");
        let entry = self
            .deltas
            .binding_loaded
            .entry(loaded.binding)
            .or_default();
        entry.docs_total += 1;
        entry.bytes_total += loaded.doc_json.len() as u64;

        self.process_loaded(&resp).context("processing C:Loaded")?;
        Ok(())
    }

    /// C:Flushed: emit L:Flushed and wait for L:Store before draining
    /// the combiner. L:Store is the leader's durable Persist#1 fence.
    async fn on_c_flushed(&mut self, resp: materialize::Response) -> anyhow::Result<()> {
        let flushed = resp
            .flushed
            .as_ref()
            .expect("dispatched on resp.flushed.is_some()");
        let connector_patches_json = flushed
            .state
            .as_ref()
            .map(|s| Bytes::copy_from_slice(s.updated_json.as_ref()))
            .unwrap_or_default();

        let l_flushed = proto::materialize::Flushed {
            connector_patches_json,
            binding_loaded: std::mem::take(&mut self.deltas.binding_loaded),
        };
        self.leader_tx
            .send(proto::Materialize {
                flushed: Some(l_flushed),
                ..Default::default()
            })
            .await
            .context("send L:Flushed")?;

        self.c_flushed_received = true;
        Ok(())
    }

    /// Begin the combiner drain. The Persist#1 fence on C:Store is enforced
    /// by the L:Store gate.
    fn start_drain(&mut self) -> anyhow::Result<()> {
        debug_assert!(self.c_flushed_received);
        self.load_keys.clear();
        let task = self.task.take().context("start_drain: no Task")?;
        let accumulator = self
            .accumulator
            .take()
            .context("start_drain: accumulator already drained")?;
        let (drainer, parser) = accumulator
            .into_drainer()
            .context("preparing combiner drain")?;
        self.drain_state = Some(DrainState {
            drainer,
            parser,
            task,
            summary: DrainStoresComplete::default(),
        });
        Ok(())
    }

    /// C:StartedCommit: emit L:StartedCommit with cross-transaction
    /// counters and reset per-transaction flags for the next round.
    async fn on_c_started_commit(&mut self, resp: materialize::Response) -> anyhow::Result<()> {
        let started = resp
            .started_commit
            .as_ref()
            .expect("dispatched on resp.started_commit.is_some()");
        let connector_patches_json = started
            .state
            .as_ref()
            .map(|s| Bytes::copy_from_slice(s.updated_json.as_ref()))
            .unwrap_or_default();
        let l_started = proto::materialize::StartedCommit {
            connector_patches_json,
        };
        self.leader_tx
            .send(proto::Materialize {
                started_commit: Some(l_started),
                ..Default::default()
            })
            .await
            .context("send L:StartedCommit")?;

        self.c_flushed_received = false;
        self.received_l_start_commit = false;
        Ok(())
    }

    /// C:Acknowledged: emit L:Acknowledged.
    async fn on_c_acknowledged(&mut self, resp: materialize::Response) -> anyhow::Result<()> {
        let acknowledged = resp
            .acknowledged
            .as_ref()
            .expect("dispatched on resp.acknowledged.is_some()");
        let connector_patches_json = acknowledged
            .state
            .as_ref()
            .map(|s| Bytes::copy_from_slice(s.updated_json.as_ref()))
            .unwrap_or_default();

        self.leader_tx
            .send(proto::Materialize {
                acknowledged: Some(proto::materialize::Acknowledged {
                    connector_patches_json,
                }),
                ..Default::default()
            })
            .await
            .context("send L:Acknowledged")?;
        Ok(())
    }

    /// Drive a `FrontierScan` to completion against the actor-owned Reader,
    /// folding source documents into the combiner as `ArchivedNode` directly
    /// (no JSON parse), emitting `C:Load`s as needed, and yielding between
    /// segment blocks so the actor stays responsive.
    async fn scan_frontier(&mut self, delta: shuffle::Frontier) -> anyhow::Result<ScanComplete> {
        let task = self
            .task
            .take()
            .context("scan_frontier: actor has no Task")?;
        let mut accumulator = self
            .accumulator
            .take()
            .context("scan_frontier: accumulator already drained")?;
        let reader = self
            .shuffle_reader
            .take()
            .context("scan_frontier: shuffle Reader missing")?;
        let remainders = std::mem::take(&mut self.shuffle_remainders);

        let result = self
            .scan_inner(&task, &mut accumulator, reader, remainders, delta)
            .await;

        self.task = Some(task);
        self.accumulator = Some(accumulator);
        result
    }

    async fn scan_inner(
        &mut self,
        task: &Task,
        accumulator: &mut Accumulator,
        reader: Reader,
        remainders: VecDeque<Remainder>,
        delta: shuffle::Frontier,
    ) -> anyhow::Result<ScanComplete> {
        let mut scan =
            FrontierScan::new(delta, reader, remainders).context("constructing FrontierScan")?;
        let mut summary = ScanComplete::default();
        let mut c_loads: Vec<materialize::Request> = Vec::new();

        while scan.advance_block().context("advancing FrontierScan")? {
            // Borrow memtable + iterate the block in a tight scope so the
            // non-Send `&MemTable` / `&Bump` references drop before any
            // `.await`. C:Load requests are buffered into `c_loads` and
            // sent after the borrow is released.
            {
                let memtable = accumulator.memtable().context("acquiring memtable")?;
                let alloc = memtable.alloc();

                for entry in scan.block_iter() {
                    let binding_index = entry.meta.binding.to_native() as usize;
                    let binding = task.bindings.get(binding_index).with_context(|| {
                        format!("scanned binding index {binding_index} out of range")
                    })?;

                    let archived = entry.doc.doc.get();

                    // Extract the FULL packed key from the ArchivedNode for
                    // max-key tracking and C:Load. (The block carries only a
                    // 16-byte prefix used by the combiner's pre-sort.)
                    let mut key_buf = bytes::BytesMut::new();
                    doc::Extractor::extract_all(archived, &binding.key_extractors, &mut key_buf);
                    let key_packed = key_buf.split().freeze();

                    // Max-key load-skip: if this key is larger than the
                    // largest key durably stored, the connector cannot have
                    // it loaded. Track the new high-water for Persist #1.
                    let binding_idx = binding_index as u32;
                    let prev_max = self
                        .committed_max_keys
                        .get(&binding_idx)
                        .cloned()
                        .unwrap_or_default();
                    let cannot_exist = !prev_max.is_empty() && key_packed > prev_max;
                    if cannot_exist {
                        let next_max = self.next_max_keys.entry(binding_idx).or_default();
                        if &key_packed > next_max {
                            *next_max = key_packed.clone();
                            summary
                                .max_key_deltas
                                .insert(binding_idx, key_packed.clone());
                        }
                    }

                    // C:Load decision: skip if delta-updates, guaranteed-absent,
                    // or already pending this transaction.
                    let key_hash = xxh3_128(&key_packed);
                    if !binding.delta_updates
                        && !cannot_exist
                        && !self.load_keys.contains(&key_hash)
                    {
                        self.load_keys.insert(key_hash);
                        c_loads.push(materialize::Request {
                            load: Some(materialize::request::Load {
                                binding: binding_index as u32,
                                key_packed: key_packed.clone(),
                                key_json: Bytes::new(),
                            }),
                            ..Default::default()
                        });
                    }

                    // Add the document to the combiner as ArchivedNode —
                    // no JSON parse.
                    let schema_valid =
                        entry.meta.flags.to_native() & shuffle::FLAGS_SCHEMA_VALID != 0;
                    memtable
                        .add_embedded(
                            entry.meta.binding.to_native(),
                            &entry.doc.packed_key_prefix,
                            entry.doc.doc.to_heap(alloc),
                            false,
                            schema_valid,
                        )
                        .context("MemTable::add_embedded")?;

                    let dab = summary
                        .binding_read
                        .entry(binding_index as u32)
                        .or_default();
                    dab.docs_total += 1;
                    dab.bytes_total += entry.doc.source_byte_length.to_native() as u64;

                    // Track per-binding min/max source clocks for the leader's
                    // trigger-variables reduction. Producer clocks are
                    // monotonic per-producer but interleave across producers,
                    // so we apply min/max within each binding bucket.
                    let clock = entry.meta.clock.to_native();
                    summary
                        .first_source_clock
                        .entry(binding_index as u32)
                        .and_modify(|prev| *prev = (*prev).min(clock))
                        .or_insert(clock);
                    summary
                        .last_source_clock
                        .entry(binding_index as u32)
                        .and_modify(|prev| *prev = (*prev).max(clock))
                        .or_insert(clock);
                }
            }

            // Send buffered C:Loads. Doing this once per block (rather than
            // once per scan) bounds in-actor backlog without thrashing.
            for req in c_loads.drain(..) {
                self.connector_tx.send(req).await.context("send C:Load")?;
            }

            // Yield so other tasks (and other actor IO when this returns)
            // get a turn between blocks.
            tokio::task::yield_now().await;
        }

        let (_, returned_reader, returned_remainders) = scan.into_parts();
        self.shuffle_reader = Some(returned_reader);
        self.shuffle_remainders = returned_remainders;

        // combiner_usage_bytes mirrors the spilled file size; drainer/spill
        // accounting is internal. We report 0 for now; a future pass can
        // surface the real figure once exposed by `doc::combine`.
        summary.combiner_usage_bytes = 0;

        Ok(summary)
    }

    /// Fold a `C:Loaded` doc into the combiner with `front=true` so it
    /// reduces with subsequent source documents.
    fn process_loaded(&mut self, response: &materialize::Response) -> anyhow::Result<()> {
        let materialize::Response {
            loaded: Some(materialize::response::Loaded { binding, doc_json }),
            ..
        } = response
        else {
            return Ok(());
        };

        let task = self
            .task
            .as_ref()
            .context("process_loaded: actor has no Task")?;
        let accumulator = self
            .accumulator
            .as_mut()
            .context("process_loaded: accumulator already drained")?;

        let binding_index = *binding as usize;
        let binding_spec = task
            .bindings
            .get(binding_index)
            .ok_or_else(|| anyhow::anyhow!("Loaded binding {binding_index} out of range"))?;

        let (memtable, _alloc, doc) = accumulator
            .parse_json_doc(doc_json)
            .with_context(|| format!("parsing loaded doc for {}", binding_spec.collection_name))?;

        memtable.add(binding_index as u16, doc, true)?;
        Ok(())
    }

    /// Advance an in-progress combiner drain by up to
    /// `DRAIN_CHUNK_STORES` C:Store messages. When the drainer is
    /// exhausted, recycle the Accumulator/Task back into the actor,
    /// and emit L:Stored. While drain is in progress, the main `select!`
    /// keeps servicing `leader_rx` (Persist#1) and `persist_fut` between
    /// chunks so a slow connector doesn't starve Persist application.
    async fn drive_drain(&mut self) -> anyhow::Result<()> {
        let mut state = self
            .drain_state
            .take()
            .expect("drive_drain called with no drain_state");
        let mut buf = bytes::BytesMut::new();
        for _ in 0..DRAIN_CHUNK_STORES {
            let Some(drained) = state.drainer.drain_next().context("drain_next")? else {
                // Drainer exhausted: recycle and emit L:Stored.
                let recycled = Accumulator::from_drainer(state.drainer, state.parser)?;
                self.accumulator = Some(recycled);
                self.task = Some(state.task);
                tracing::debug!(
                    binding_stored = ?state.summary.binding_stored,
                    "drain complete"
                );
                self.leader_tx
                    .send(proto::Materialize {
                        stored: Some(proto::materialize::Stored {
                            binding_stored: state.summary.binding_stored,
                            first_source_clock: std::mem::take(&mut self.deltas.first_source_clock),
                            last_source_clock: std::mem::take(&mut self.deltas.last_source_clock),
                        }),
                        ..Default::default()
                    })
                    .await
                    .context("send L:Stored")?;
                return Ok(());
            };

            let binding_index = drained.meta.binding() as u32;
            let store = build_store(&mut buf, drained, &state.task);
            let bytes_total = match &store.store {
                Some(s) => {
                    s.doc_json.len() as u64
                        + s.key_packed.len() as u64
                        + s.values_packed.len() as u64
                }
                None => 0,
            };
            let entry = state
                .summary
                .binding_stored
                .entry(binding_index)
                .or_default();
            entry.docs_total += 1;
            entry.bytes_total += bytes_total;

            self.connector_tx
                .send(store)
                .await
                .context("send C:Store")?;
        }
        // Chunk done but drainer still has more — re-park drain_state
        // and yield to the main loop.
        self.drain_state = Some(state);
        Ok(())
    }
}

/// Build a C:Store from a single drained combiner document. Mirrors
/// `runtime/src/materialize/protocol::send_connector_store`, simplified.
fn build_store(
    buf: &mut bytes::BytesMut,
    drained: doc::combine::DrainedDoc,
    task: &Task,
) -> materialize::Request {
    let doc::combine::DrainedDoc { meta, root } = drained;
    let binding_index = meta.binding();
    let binding = &task.bindings[binding_index];

    let truncation_indicator = AtomicBool::new(false);
    doc::Extractor::extract_all_owned_indicate_truncation(
        &root,
        &binding.key_extractors,
        buf,
        &truncation_indicator,
    );
    let key_packed = buf.split().freeze();

    use bytes::BufMut;
    serde_json::to_writer(
        buf.writer(),
        &binding
            .ser_policy
            .on_owned_with_truncation_indicator(&root, &truncation_indicator),
    )
    .expect("document serialization cannot fail");
    let mut doc_json = buf.split().freeze();

    doc::Extractor::extract_all_owned_indicate_truncation(
        &root,
        &binding.value_extractors,
        buf,
        &truncation_indicator,
    );
    let values_packed = buf.split().freeze();

    if !binding.store_document {
        doc_json.clear();
    }

    materialize::Request {
        store: Some(materialize::request::Store {
            binding: binding_index as u32,
            delete: meta.deleted(),
            doc_json,
            exists: meta.front(),
            key_json: bytes::Bytes::new(),
            key_packed,
            values_json: bytes::Bytes::new(),
            values_packed,
        }),
        ..Default::default()
    }
}

async fn maybe_fut<T>(opt: &mut Option<BoxFuture<'static, T>>) -> Option<T> {
    match opt.as_mut() {
        Some(fut) => {
            let r = fut.await;
            *opt = None;
            Some(r)
        }
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use proto_flow::materialize::response;
    use tokio::sync::mpsc;

    fn make_actor(
        leader_tx: mpsc::Sender<proto::Materialize>,
        leader_rx: BoxStream<'static, tonic::Result<proto::Materialize>>,
        connector_tx: futures::channel::mpsc::Sender<materialize::Request>,
        connector_rx: BoxStream<'static, tonic::Result<materialize::Response>>,
    ) -> (Actor, mpsc::Receiver<tonic::Result<proto::Materialize>>) {
        let (controller_tx, controller_rx) = crate::new_channel();
        (
            Actor {
                controller_tx,
                leader_tx,
                leader_rx,
                connector_tx,
                connector_rx,
                rocksdb: None,
                binding_state_keys: Arc::new(Vec::new()),
                accumulator: None,
                task: None,
                committed_max_keys: BTreeMap::new(),
                next_max_keys: BTreeMap::new(),
                load_keys: super::super::LoadKeySet::default(),
                shuffle_reader: None,
                shuffle_remainders: VecDeque::new(),
                persist_fut: None,
                drain_state: None,
                deltas: LoadDeltas::default(),
                frontier_journals: Vec::new(),
                c_flushed_received: false,
                received_l_start_commit: false,
                peer_patches_for_start_commit: Bytes::new(),
                persist_batch: rocksdb::WriteBatch::default(),
            },
            controller_rx,
        )
    }

    /// Bare-bones round trip: leader sends L:Acknowledge, the actor sends
    /// C:Acknowledge to the connector, the connector replies
    /// C:Acknowledged, the actor sends L:Acknowledged back. Exercises the
    /// reactor's leader/connector dispatch.
    #[tokio::test]
    async fn round_trip_acknowledge() {
        let (leader_tx, mut leader_rx_capture) = mpsc::channel(8);
        let (connector_tx, mut connector_rx_capture) = futures::channel::mpsc::channel(8);

        let inbound_leader = stream::iter(vec![Ok(proto::Materialize {
            acknowledge: Some(proto::materialize::Acknowledge {
                connector_patches_json: Bytes::new(),
            }),
            ..Default::default()
        })])
        .boxed();

        let inbound_connector = stream::iter(vec![Ok(materialize::Response {
            acknowledged: Some(response::Acknowledged { state: None }),
            ..Default::default()
        })])
        .boxed();

        let (mut actor, _controller_rx) =
            make_actor(leader_tx, inbound_leader, connector_tx, inbound_connector);

        let (_controller_in_tx, controller_in_rx) =
            mpsc::channel::<tonic::Result<proto::Materialize>>(1);
        let mut controller_stream = tokio_stream::wrappers::ReceiverStream::new(controller_in_rx);
        let serve = tokio::spawn(async move {
            let _ = actor.serve(&mut controller_stream).await;
        });

        let next = connector_rx_capture.next().await.unwrap();
        assert!(next.acknowledge.is_some(), "actor sent C:Acknowledge");

        let next = leader_rx_capture.recv().await.unwrap();
        assert!(next.acknowledged.is_some(), "actor sent L:Acknowledged");

        serve.abort();
    }

    /// Persist#1 fence: when C:Flushed has been observed but L:Store has
    /// not, the drain MUST NOT begin and no C:Store must be sent to the
    /// connector. This is the runtime-side enforcement that no C:Store runs
    /// before max-keys are durable on shard zero.
    #[tokio::test]
    async fn drain_does_not_start_before_l_store() {
        let (leader_tx, mut leader_rx_capture) = mpsc::channel(8);
        let (connector_tx, mut connector_rx_capture) = futures::channel::mpsc::channel(8);

        let inbound_leader: BoxStream<'static, tonic::Result<proto::Materialize>> =
            stream::pending().boxed();
        let inbound_connector: BoxStream<'static, tonic::Result<materialize::Response>> =
            stream::pending().boxed();

        let (mut actor, _controller_rx) =
            make_actor(leader_tx, inbound_leader, connector_tx, inbound_connector);

        // We're at end of Loading; deliver C:Flushed without prior L:StartCommit.
        actor
            .on_c_flushed(materialize::Response {
                flushed: Some(response::Flushed { state: None }),
                ..Default::default()
            })
            .await
            .unwrap();

        // L:Flushed went out, but the drain must NOT have begun.
        let l_flushed = leader_rx_capture.recv().await.unwrap();
        assert!(l_flushed.flushed.is_some(), "L:Flushed forwarded");
        assert!(actor.c_flushed_received);
        assert!(
            actor.drain_state.is_none(),
            "drain must not begin before L:Store"
        );
        assert!(
            connector_rx_capture.try_next().is_err(),
            "no C:Store sent before L:Store"
        );

        // Now deliver L:Store. With Task/Accumulator absent in this
        // bare test fixture, start_drain returns an error — but we're
        // verifying the *gate*: the handler MUST attempt start_drain
        // exactly when both gates flip true. Confirm by observing that
        // the call path is exercised.
        let err = actor
            .on_l_store(proto::materialize::Store {})
            .await
            .expect_err("start_drain errors without Task/Accumulator");
        assert!(
            err.to_string().contains("start_drain"),
            "expected start_drain error, got: {err}"
        );
    }
}
