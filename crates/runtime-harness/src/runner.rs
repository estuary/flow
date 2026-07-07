//! `DerivationRunner`: one derivation resident as a runtime-next session for the
//! whole test run, driven Stat-by-Stat by the harness.
//!
//! It is a generalization of preview's `derive_driver`: it hosts the leader +
//! N synthetic shards over one long-lived SessionLoop (so the connector
//! container / in-process derive-sqlite starts at most once and stays warm), but
//! instead of running to a fixed transaction budget it keeps the session open
//! and runs exactly one transaction per [`stat`](DerivationRunner::stat).
//!
//! Executing a Stat:
//! 1. From the pending stat's `read_through` clock, feed the source documents
//!    each transform must now read — the store window each source collection has
//!    grown by since the last feed — into the per-shard shuffle logs (key-routed
//!    by [`segments::write_transaction`]), producing one checkpoint frontier.
//! 2. Push the frontier to the channel-fed shuffle session; the leader consumes
//!    it, runs one transaction (shards derive and publish through
//!    [`TestPublisher`](crate::publish::TestPublisher), which appends derived
//!    documents to the store), and emits a committing `Persist`.
//! 3. Await that commit signal, then report the task's cumulative read-through
//!    and its output collection's new write clock back to the graph.
//!
//! Read delays need no wall clock here: the graph gates a stat's `read_through`
//! by synthetic time before calling [`stat`](DerivationRunner::stat), so the
//! runner simply feeds up to whatever `read_through` it is given.
//!
//! Feeding is per *binding* (transform), not per source collection: a stat's
//! `read_through` entries carry the transform's checkpoint suffix, which the
//! runner maps to a specific binding and feeds only that binding. So a source
//! read by two transforms with *different* read delays feeds each independently
//! — the delayed transform sees a document only when its own (later) stat fires,
//! exactly as a live shuffle read would.

use crate::clock::Clock;
use crate::drive::{segments, services::Run};
use crate::graph::PendingStat;
use crate::logger::TestLoggerFactory;
use crate::publish::TestPublisherFactory;
use crate::store::CollectionStore;
use anyhow::Context;
use prost::Message;
use proto_flow::{flow, flow::collection_spec::derivation::ConnectorType, runtime as cruntime};
use proto_gazette::uuid;
use runtime_next::proto;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

/// A single derivation, resident as a runtime-next session for the run.
pub struct DerivationRunner {
    task_name: String,
    /// The derivation's own (output) collection name.
    output_collection: String,
    /// Number of synthetic shards hosted for this derivation.
    n_shards: u32,

    // --- Live session (dropped last: aborts the server after shard streams). ---
    /// Per-shard request streams; kept open for the run. Dropping them EOFs each
    /// shard's SessionLoop, ending the session.
    request_txs: Vec<mpsc::UnboundedSender<tonic::Result<proto::Derive>>>,
    /// Per-shard response drainers; joined on shutdown to surface late errors.
    shard_handles: Vec<tokio::task::JoinHandle<anyhow::Result<()>>>,
    _run: Run,

    /// Push checkpoint frontiers to the leader's channel-fed shuffle session.
    frontier_tx: mpsc::UnboundedSender<segments::FixtureItem>,
    /// One `()` per committed transaction (from the leader's `Persist`).
    commit_rx: mpsc::UnboundedReceiver<()>,
    /// One `()` per shard `ResetDone` forwarded to its controller during a
    /// [`reset`](Self::reset) (the leader broadcasts `ResetDone` to all shards).
    reset_rx: mpsc::UnboundedReceiver<()>,

    // --- Feed state (segment writer inputs, advanced across the run). ---
    bindings: Vec<shuffle::Binding>,
    validators: Vec<doc::Validator>,
    /// Read-checkpoint suffix (`derive/{derivation}/{transform}`) → binding
    /// index. A stat's `read_through` journals carry this suffix, letting the
    /// runner feed the exact binding a stat advances — so a source read by
    /// several transforms with *different* read delays feeds each independently,
    /// honoring the delay rather than delivering to every binding at once.
    suffix_to_binding: HashMap<String, usize>,
    shards: Vec<shuffle::proto::Shard>,
    writers: Vec<segments::ShardWriter>,
    sealed: Vec<shuffle::log::writer::SealedSegment>,
    clock: uuid::Clock,
    journal_offsets: HashMap<(String, u16), i64>,
    round_robin: HashMap<u16, usize>,

    /// Store doc-offset fed so far, per (source partition journal, binding).
    fed: HashMap<(String, usize), i64>,
    /// Cumulative read-through clock reported to the graph (all transforms).
    cumulative_read: Clock,

    store: Arc<Mutex<CollectionStore>>,
}

impl DerivationRunner {
    /// Start a derivation session: host the leader + N shards, open the
    /// SessionLoop / Join / Task, and block until every shard has Opened.
    #[allow(clippy::too_many_arguments)]
    pub async fn start(
        spec: &flow::CollectionSpec,
        n_shards: u32,
        network: String,
        registry: service_kit::Registry,
        store: Arc<Mutex<CollectionStore>>,
        publish_clock: Arc<std::sync::atomic::AtomicU64>,
        log_handler: crate::logger::LogHandler,
        remote_connectors: Option<Arc<dyn runtime_next::RemoteConnectors>>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(n_shards >= 1, "a derivation needs at least one shard");
        let task_name = spec.name.clone();
        let derivation = spec
            .derivation
            .as_ref()
            .context("collection spec is not a derivation")?;
        let is_sqlite = derivation.connector_type == ConnectorType::Sqlite as i32;

        // Build the shuffle bindings for key-routing / feeding the source docs.
        let shuffle_task = shuffle::proto::Task {
            task: Some(shuffle::proto::task::Task::Derivation(spec.clone())),
        };
        let (bindings, validators, _collection_bindings) = segments::task_bindings(&shuffle_task)?;

        // Map each transform's read-checkpoint suffix to its binding index, so a
        // stat feeds exactly the binding it advances.
        let suffix_to_binding: HashMap<String, usize> = bindings
            .iter()
            .enumerate()
            .map(|(i, b)| (b.journal_read_suffix.clone(), i))
            .collect();

        // Host the leader + fixture shuffle. The publisher appends derived docs
        // to the store (all shards + leader share one clock, so per-key publish
        // order is total); the logger signals each transaction commit — only the
        // leader emits a committing `Persist`, so shard loggers never fire it.
        let (opener, frontier_tx) = segments::fixture_opener();
        let (commit_tx, commit_rx) = mpsc::unbounded_channel::<()>();
        let (reset_done_tx, reset_rx) = mpsc::unbounded_channel::<()>();
        let publisher_factory = TestPublisherFactory::new(store.clone(), publish_clock);
        let logger_factory = TestLoggerFactory::new(commit_tx, log_handler);

        let run = Run::start_with_shuffle_leader(
            network.clone(),
            n_shards,
            None,
            registry.clone(),
            publisher_factory.clone(),
            logger_factory.clone(),
            move |_peer_endpoint| Ok((opener, None)),
        )
        .await?;

        // Per-shard shuffle-log directories and their segment writers.
        let mut shard_dirs = Vec::with_capacity(n_shards as usize);
        let mut writers = Vec::with_capacity(n_shards as usize);
        for i in 0..n_shards {
            let dir = std::path::Path::new(&run.shuffle_log_dir).join(format!("shard-{i:03}"));
            std::fs::create_dir(&dir)
                .with_context(|| format!("creating shard shuffle directory {dir:?}"))?;
            writers.push(segments::ShardWriter::new(&dir, i)?);
            shard_dirs.push(dir.to_string_lossy().into_owned());
        }

        // One-transaction-per-checkpoint: collapse the transaction-duration
        // window so each pushed frontier commits as exactly one transaction.
        let mut spec = spec.clone();
        if let Some(t) = spec
            .derivation
            .as_mut()
            .and_then(|d| d.shard_template.as_mut())
        {
            force_single_transaction(t);
        }
        let spec_bytes: bytes::Bytes = spec.encode_to_vec().into();
        let join_shards = crate::drive::shards::build_derive_join_shards(n_shards, &spec)?;

        // Open each shard's SessionLoop / Join / Task and drain until Opened.
        let mut request_txs = Vec::with_capacity(n_shards as usize);
        let mut shard_handles = Vec::with_capacity(n_shards as usize);
        let mut ready_rxs = Vec::with_capacity(n_shards as usize);

        for i in 0..n_shards {
            let (request_tx, request_rx) =
                mpsc::unbounded_channel::<tonic::Result<proto::Derive>>();
            let shard_svc = runtime_next::shard::Service::new(
                cruntime::Plane::Local,
                network.clone(),
                None,
                format!("test-derive-{i:03}"),
                publisher_factory.clone(),
                logger_factory.clone(),
                run.registry.clone(),
                None,
            )
            .with_remote_connectors(remote_connectors.clone());
            let response_rx = shard_svc.spawn_derive(UnboundedReceiverStream::new(request_rx));

            let sqlite_vfs_uri = if is_sqlite {
                format!("{}/derive-sqlite-{i:03}.db", run.rocksdb_path)
            } else {
                String::new()
            };
            let rocksdb_descriptor = (i == 0).then(|| cruntime::RocksDbDescriptor {
                rocksdb_path: run.rocksdb_path.clone(),
                rocksdb_env_memptr: 0,
            });

            request_tx
                .send(Ok(proto::Derive {
                    session_loop: Some(proto::SessionLoop { rocksdb_descriptor }),
                    ..Default::default()
                }))
                .map_err(|_| anyhow::anyhow!("serve task closed before SessionLoop"))?;
            request_tx
                .send(Ok(proto::Derive {
                    join: Some(proto::Join {
                        etcd_mod_revision: 1,
                        shards: join_shards.clone(),
                        shard_index: i,
                        shuffle_directory: shard_dirs[i as usize].clone(),
                        shuffle_endpoint: run.peer_endpoint.clone(),
                        leader_endpoint: run.peer_endpoint.clone(),
                    }),
                    ..Default::default()
                }))
                .map_err(|_| anyhow::anyhow!("serve task closed before Join"))?;
            request_tx
                .send(Ok(proto::Derive {
                    task: Some(proto::Task {
                        spec: spec_bytes.clone(),
                        max_transactions: 0, // Unbounded: the session is resident.
                        sqlite_vfs_uri,
                        publisher_id: Default::default(),
                    }),
                    ..Default::default()
                }))
                .map_err(|_| anyhow::anyhow!("serve task closed before Task"))?;

            let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();
            shard_handles.push(tokio::spawn(drain_shard(
                response_rx,
                i,
                ready_tx,
                reset_done_tx.clone(),
            )));
            request_txs.push(request_tx);
            ready_rxs.push(ready_rx);
        }

        // Block until every shard has joined and opened its connector.
        for (i, ready_rx) in ready_rxs.into_iter().enumerate() {
            ready_rx
                .await
                .map_err(|_| anyhow::anyhow!("shard {i} exited before opening its session"))?;
        }

        Ok(Self {
            task_name,
            output_collection: spec.name.clone(),
            n_shards,
            request_txs,
            shard_handles,
            _run: run,
            frontier_tx,
            commit_rx,
            reset_rx,
            bindings,
            validators,
            suffix_to_binding,
            shards: segments::full_range_shards(n_shards),
            writers,
            sealed: Vec::new(),
            clock: uuid::Clock::from_unix(1, 0),
            journal_offsets: HashMap::new(),
            round_robin: HashMap::new(),
            fed: HashMap::new(),
            cumulative_read: Clock::new(),
            store,
        })
    }

    /// The task (derivation) name this runner drives.
    pub fn task_name(&self) -> &str {
        &self.task_name
    }

    /// Execute one Stat: feed the newly-readable source documents, run one
    /// transaction, and return the task's `(read_through, write_at)` progress.
    pub async fn stat(&mut self, pending: &PendingStat) -> anyhow::Result<(Clock, Clock)> {
        // Map each read-through entry to the (source partition journal, binding)
        // it advances and the doc offset to read through. The binding is resolved
        // by the entry's transform suffix, so a source read by transforms with
        // different read delays feeds each binding independently.
        let mut targets: HashMap<(String, usize), i64> = HashMap::new();
        for (journal_with_suffix, &offset) in &pending.read_through {
            let Some((store_journal, suffix)) = journal_with_suffix.split_once(';') else {
                continue; // read-through journals always carry a transform suffix.
            };
            let Some(&bi) = self.suffix_to_binding.get(suffix) else {
                continue; // Not one of this derivation's transforms.
            };
            let entry = targets.entry((store_journal.to_string(), bi)).or_insert(0);
            *entry = (*entry).max(offset);
        }

        // Assemble binding-indexed feed items: each (journal, binding)'s newly
        // readable documents, in (journal, binding) order.
        let mut docs: Vec<(usize, serde_json::Value)> = Vec::new();
        {
            let store = self.store.lock().unwrap();
            let mut keys: Vec<&(String, usize)> = targets.keys().collect();
            keys.sort();
            for key in keys {
                let (journal, bi) = key;
                let target = targets[key];
                let from = self.fed.get(key).copied().unwrap_or(0);
                for stored in store.read_window(journal, from, target) {
                    let doc: serde_json::Value = serde_json::from_slice(&stored.doc)
                        .context("parsing stored source document to feed")?;
                    docs.push((*bi, doc));
                }
            }
        }

        // Write the segments and push the resulting checkpoint frontier.
        let items: Vec<(usize, &serde_json::Value)> =
            docs.iter().map(|(bi, doc)| (*bi, doc)).collect();
        let frontier = segments::write_transaction_for_bindings(
            &items,
            &self.bindings,
            &mut self.validators,
            &self.shards,
            &mut self.writers,
            &mut self.sealed,
            &mut self.clock,
            &mut self.journal_offsets,
            &mut self.round_robin,
            &mut bytes::BytesMut::new(),
        )?;
        self.frontier_tx
            .send(segments::FixtureItem::Frontier(frontier))
            .map_err(|_| anyhow::anyhow!("{}: shuffle session closed", self.task_name))?;

        // Advance fed offsets and await the transaction's commit.
        for (key, target) in targets {
            self.fed.insert(key, target);
        }
        self.commit_rx
            .recv()
            .await
            .with_context(|| format!("{}: session ended before commit", self.task_name))?;

        // The task's cumulative read-through, and its output write clock.
        self.cumulative_read =
            crate::clock::max_clock(&self.cumulative_read, &pending.read_through);
        let write_at = self
            .store
            .lock()
            .unwrap()
            .write_clock(&self.output_collection);
        Ok((self.cumulative_read.clone(), write_at))
    }

    /// Reset the derivation's connector state between test cases, driving the
    /// runtime-next Reset flow (Controller → Shard → Leader; the leader clears and
    /// durably persists empty connector state, then broadcasts `ResetDone` back to
    /// every shard's controller). Read frontiers and collection data are
    /// untouched — only connector-internal state (e.g. derive-sqlite registers) is
    /// cleared. The runner's feed cursors persist, matching V1, where read
    /// checkpoints survive a reset.
    pub async fn reset(&mut self) -> anyhow::Result<()> {
        // A single controller Reset request drives the whole topology; the leader
        // broadcasts ResetDone to all shards, so we await one confirmation per
        // shard (drained and forwarded by each shard's `drain_shard`).
        self.request_txs[0]
            .send(Ok(proto::Derive {
                reset: Some(proto::Reset {}),
                ..Default::default()
            }))
            .map_err(|_| anyhow::anyhow!("{}: session closed before Reset", self.task_name))?;

        for _ in 0..self.n_shards {
            self.reset_rx.recv().await.with_context(|| {
                format!("{}: session ended before Reset completed", self.task_name)
            })?;
        }
        Ok(())
    }

    /// Gracefully stop the session: Stop every shard, await each drainer's
    /// Stopped confirmation, then drop the request streams (EOF). Consumes the
    /// runner. Dropping the senders before Stopped would EOF the stream mid
    /// handshake, which the shard rejects as an unexpected controller EOF.
    pub async fn shutdown(mut self) -> anyhow::Result<()> {
        for request_tx in &self.request_txs {
            let _ = request_tx.send(Ok(proto::Derive {
                stop: Some(proto::Stop {}),
                ..Default::default()
            }));
        }

        // Drainers return once they observe Stopped; the request streams stay
        // open through the handshake because we still hold `request_txs`.
        let mut first_err = None;
        for handle in std::mem::take(&mut self.shard_handles) {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) if first_err.is_none() => first_err = Some(e),
                Ok(Err(e)) => tracing::warn!(error = ?e, "secondary shard drainer error"),
                Err(panic) if first_err.is_none() => {
                    first_err = Some(anyhow::anyhow!("shard drainer panic: {panic}"))
                }
                Err(panic) => tracing::warn!(?panic, "secondary shard drainer panic"),
            }
        }
        // Now EOF each request stream, letting each shard's serve loop finish.
        self.request_txs.clear();

        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

/// Drain a shard's response stream: signal readiness once Opened, forward each
/// `ResetDone` to `reset_done_tx`, then keep draining (surfacing errors) until the
/// request stream closes and the shard EOFs. `ready_tx` fires exactly once, on
/// the first Opened.
async fn drain_shard(
    mut response_rx: mpsc::UnboundedReceiver<tonic::Result<proto::Derive>>,
    shard_index: u32,
    ready_tx: tokio::sync::oneshot::Sender<()>,
    reset_done_tx: mpsc::UnboundedSender<()>,
) -> anyhow::Result<()> {
    let mut ready_tx = Some(ready_tx);
    while let Some(msg) = response_rx.recv().await {
        let msg = msg.map_err(runtime_next::status_to_anyhow)?;
        if msg.opened.is_some() {
            if let Some(tx) = ready_tx.take() {
                let _ = tx.send(());
            }
        } else if msg.joined.is_some() {
            tracing::debug!(shard_index, "runner shard joined");
        } else if msg.reset_done.is_some() {
            // The leader's Reset completion reached this shard's controller.
            tracing::debug!(shard_index, "runner shard reset done");
            let _ = reset_done_tx.send(());
        } else if msg.stopped.is_some() {
            // Graceful shutdown reached: the shard has confirmed Stopped. Return
            // so `shutdown` can drop the request stream and end the serve loop.
            tracing::debug!(shard_index, "runner shard stopped");
            return Ok(());
        }
    }
    Ok(())
}

/// Collapse the transaction-duration window so each pushed checkpoint frontier
/// commits as exactly one transaction (as preview's fixture path does). A
/// literal zero max would deadlock the leader's first-checkpoint gate, so the
/// smallest positive duration is used.
fn force_single_transaction(shard_template: &mut proto_gazette::consumer::ShardSpec) {
    shard_template.min_txn_duration = Some(pbjson_types::Duration {
        seconds: 0,
        nanos: 0,
    });
    shard_template.max_txn_duration = Some(pbjson_types::Duration {
        seconds: 0,
        nanos: 1,
    });
}
