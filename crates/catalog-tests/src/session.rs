//! `DerivationSession`: one derivation resident as a runtime-next session for the
//! whole test run, driven read-by-read by the scheduler.
//!
//! It is a generalization of preview's `derive_driver`: it hosts the leader plus
//! N synthetic shards over one long-lived SessionLoop — so a connector container
//! (or in-process derive-sqlite) starts at most once per derivation per run and
//! stays warm — but instead of running to a fixed transaction budget it keeps the
//! session open and runs exactly one transaction per
//! [`read`](DerivationSession::read), awaiting its commit.
//!
//! Read delays cost no wall-clock time: the graph gates a read's `read_through`
//! by synthetic time *before* calling `read`, so the session simply feeds up to
//! whatever `read_through` it is handed.
//!
//! Feeding is per *binding* (transform), not per source collection. A read's
//! `read_through` journals carry the transform's checkpoint suffix, which the
//! session maps back to a binding and feeds only that binding — so a collection
//! read by two transforms with *different* read delays feeds each independently,
//! the delayed one seeing a document only when its own later read fires.
//!
//! A binding's partition selector is applied here, against the store journal's
//! labels, as the production shuffle applies it when listing journals: a
//! non-matching journal's documents are never fed, though the binding's read
//! progress over that journal still advances. Documents of one read are fed in
//! store transaction order (see [`crate::store`]), not journal order.

use crate::clock::Clock;
use crate::publish::TestPublisherFactory;
use crate::store::CollectionStore;
use anyhow::Context;
use prost::Message;
use proto_flow::{flow, flow::collection_spec::derivation::ConnectorType, runtime as cruntime};
use proto_gazette::uuid;
use runtime_local::segments;
use runtime_next::proto;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

/// A single derivation, resident as a runtime-next session for the run.
pub struct DerivationSession {
    derivation: String,
    /// Partition-template name of the derivation's own (output) collection:
    /// the prefix which selects its journals from the store.
    output_template: String,

    // --- Live session (dropped last: aborts the server after shard streams). ---
    /// Per-shard request streams, kept open for the run. Dropping them EOFs each
    /// shard's SessionLoop, ending the session.
    request_txs: Vec<mpsc::UnboundedSender<tonic::Result<proto::Derive>>>,
    /// Per-shard response drainers, joined on shutdown to surface late errors.
    shard_handles: Vec<tokio::task::JoinHandle<anyhow::Result<()>>>,
    _run: runtime_local::services::Run,

    /// Push checkpoint frontiers to the leader's channel-fed shuffle session.
    frontier_tx: mpsc::UnboundedSender<segments::FixtureItem>,
    /// One `()` per committed transaction, from the leader publisher's
    /// `write_intents`.
    commit_rx: mpsc::UnboundedReceiver<()>,
    /// One `()` per shard `ResetDone`. Reset is shard-local, so a
    /// [`reset`](Self::reset) sends one Reset per shard and awaits one reply each.
    reset_rx: mpsc::UnboundedReceiver<()>,
    /// The error of the first shard drainer to end without stopping — neither
    /// signal channel above can observe a dead session on its own, see
    /// [`await_signal`].
    failure_rx: mpsc::UnboundedReceiver<anyhow::Error>,

    // --- Feed state (segment writer inputs, advanced across the run). ---
    bindings: Vec<shuffle::Binding>,
    sources: Vec<shuffle::Source>,
    validators: Vec<doc::Validator>,
    /// Read-checkpoint suffix (`derive/{derivation}/{transform}`) → binding index.
    suffix_to_binding: HashMap<String, usize>,
    shards: Vec<shuffle::proto::Shard>,
    writers: Vec<segments::ShardWriter>,
    sealed: Vec<shuffle::log::writer::SealedSegment>,
    /// Monotonic document clock. Must increase globally across the run, or a
    /// recovered frontier would re-admit documents already read.
    clock: uuid::Clock,
    journal_offsets: HashMap<(String, u16), i64>,

    /// Store document offset fed so far, per (source partition journal, binding).
    fed: HashMap<(String, usize), i64>,
    /// Cumulative read-through clock reported to the graph, over all transforms.
    cumulative_read: Clock,

    store: Arc<Mutex<CollectionStore>>,
}

impl DerivationSession {
    /// Start a derivation session: host the leader plus `n_shards` shards, open
    /// the SessionLoop / Join / Task, and block until every shard has Opened.
    ///
    /// `logger_factory` is the run's shared logger seam (see
    /// [`crate::run::Options`]), opened once by the leader and once per shard.
    pub async fn start<L: runtime_next::LoggerFactory>(
        spec: &flow::CollectionSpec,
        n_shards: u32,
        network: String,
        registry: service_kit::Registry,
        store: Arc<Mutex<CollectionStore>>,
        logger_factory: L,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(n_shards >= 1, "a derivation needs at least one shard");
        let derivation = spec.name.clone();
        let derivation_spec = spec
            .derivation
            .as_ref()
            .context("collection spec is not a derivation")?;
        let is_sqlite = derivation_spec.connector_type == ConnectorType::Sqlite as i32;

        // Shuffle bindings, for key-routing the source documents we feed.
        let shuffle_task = shuffle::proto::Task {
            task: Some(shuffle::proto::task::Task::Derivation(spec.clone())),
        };
        let (mut bindings, sources, validators, _collection_bindings) =
            segments::task_bindings(&shuffle_task)?;

        // A transform's `notBefore` / `notAfter` are not modeled by catalog
        // tests: document clocks are synthetic (see the crate README), so the
        // sequencer's bounds check would compare a real timestamp against 1970.
        for binding in &mut bindings {
            binding.not_before = uuid::Clock::UNIX_EPOCH;
            binding.not_after = uuid::Clock::from_u64(u64::MAX);
        }

        // Map each transform's read-checkpoint suffix to its binding index.
        let suffix_to_binding: HashMap<String, usize> = bindings
            .iter()
            .enumerate()
            .map(|(i, b)| (b.journal_read_suffix.clone(), i))
            .collect();

        let (opener, frontier_tx) = segments::fixture_opener();
        let (commit_tx, mut commit_rx) = mpsc::unbounded_channel::<()>();
        let (reset_done_tx, reset_rx) = mpsc::unbounded_channel::<()>();
        let (failure_tx, mut failure_rx) = mpsc::unbounded_channel::<anyhow::Error>();
        let publisher_factory = TestPublisherFactory::new(store.clone(), commit_tx);

        let run = runtime_local::services::Run::start_with_shuffle_leader(
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

        // One transaction per pushed checkpoint, so a read maps 1:1 onto a
        // runtime transaction.
        let mut spec = spec.clone();
        runtime_local::force_single_transaction(
            spec.derivation
                .as_mut()
                .and_then(|d| d.shard_template.as_mut()),
        );
        // Catalog tests redact under an empty salt everywhere, including through
        // derivations, so an expected digest is `sha256:` plus the SHA-256 of the
        // raw value. The platform salt is derived from the task's shard-ID
        // prefix and rotates whenever a task is deleted and re-published, which
        // would make an expectation unwritable by hand. See the crate README.
        if let Some(derivation) = spec.derivation.as_mut() {
            derivation.redact_salt.clear();
        }
        let spec_bytes: bytes::Bytes = spec.encode_to_vec().into();
        let join_shards = runtime_local::shards::build_derive_join_shards(n_shards, &spec)?;

        // Open each shard's SessionLoop / Join / Task and drain until Opened,
        // then consume the leader's session-startup commit signal (see the
        // `publish` module docs), which the first read would otherwise take for
        // its own.
        //
        // Every fallible step from the first spawn onward lives inside this one
        // block, so that a failure anywhere in it — including partway through
        // spawning — falls through to the single cleanup below.
        let mut request_txs = Vec::with_capacity(n_shards as usize);
        let mut shard_handles = Vec::with_capacity(n_shards as usize);
        // The derivation's ops logger, for drainers to report session failures
        // into the run's log stream (a publication's job logs).
        let ops_logger = logger_factory.open(&derivation);

        let started = async {
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
                    None, // No AuthN+AuthZ signer (local loopback).
                );
                let response_rx = shard_svc.spawn_derive(UnboundedReceiverStream::new(request_rx));

                // The drainer is spawned before any request is sent, so a
                // shard whose serve task is live is always joined by cleanup.
                let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();
                shard_handles.push(tokio::spawn(drain_shard(
                    response_rx,
                    i,
                    ready_tx,
                    reset_done_tx.clone(),
                    failure_tx.clone(),
                    ops_logger.clone(),
                )));
                request_txs.push(request_tx);
                ready_rxs.push(ready_rx);
                let request_tx = request_txs.last().unwrap();

                // A tempfile under the run's RocksDB tempdir, persistent across
                // the run so the connector's checkpoint recovers if it ever
                // restarts.
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
            }
            // Hold no sender ourselves, so `failure_rx` closes once every
            // drainer has exited.
            std::mem::drop(failure_tx);

            for (i, ready_rx) in ready_rxs.into_iter().enumerate() {
                ready_rx
                    .await
                    .map_err(|_| anyhow::anyhow!("shard {i} exited before opening its session"))?;
            }
            await_signal(
                &mut commit_rx,
                &mut failure_rx,
                &derivation,
                "startup completed",
            )
            .await
        }
        .await;

        // Startup failed with shard sessions already live. Stop them before
        // `run` drops: its tempdirs are removed on drop, and a shard still
        // holding RocksDB open inside one crashes the process.
        if let Err(err) = started {
            _ = stop_shards(&request_txs, shard_handles).await;
            return Err(err);
        }

        Ok(Self {
            derivation,
            output_template: crate::partitions::template_name(&spec)?.to_string(),
            request_txs,
            shard_handles,
            _run: run,
            frontier_tx,
            commit_rx,
            reset_rx,
            failure_rx,
            bindings,
            sources,
            validators,
            suffix_to_binding,
            shards: segments::full_range_shards(n_shards),
            writers,
            sealed: Vec::new(),
            clock: uuid::Clock::from_unix(1, 0),
            journal_offsets: HashMap::new(),
            fed: HashMap::new(),
            cumulative_read: Clock::new(),
            store,
        })
    }

    /// Execute one read: feed the newly-readable source documents, run one
    /// transaction, and return the derivation's `(read_through, write_at)`
    /// progress.
    pub async fn read(
        &mut self,
        pending: &crate::graph::PendingRead,
    ) -> anyhow::Result<(Clock, Clock)> {
        // Map each read-through entry to the (source journal, binding) it
        // advances, and the document offset to read through.
        let mut targets: HashMap<(String, usize), i64> = HashMap::new();
        for (journal_with_suffix, &offset) in &pending.read_through {
            let Some((store_journal, suffix)) = journal_with_suffix.split_once(';') else {
                continue; // Read-through journals always carry a transform suffix.
            };
            let Some(&bi) = self.suffix_to_binding.get(suffix) else {
                continue; // Not one of this derivation's transforms.
            };
            let entry = targets.entry((store_journal.to_string(), bi)).or_insert(0);
            *entry = (*entry).max(offset);
        }

        // Collect each (journal, binding)'s newly-readable documents which the
        // binding's partition selector admits, then order them by store
        // transaction so an earlier transaction's documents are always fed —
        // and clocked — ahead of a later one's, whichever journals they're in.
        // The sort is stable, so within a transaction the incidental
        // (journal, append) order stands; nothing may depend on it.
        let mut docs: Vec<(u64, usize, serde_json::Value)> = Vec::new();
        {
            let store = self.store.lock().unwrap();
            let mut keys: Vec<&(String, usize)> = targets.keys().collect();
            keys.sort();

            for key in keys {
                let (journal, bi) = key;
                let selector = &self.bindings[*bi].partition_selector;
                let admitted = labels::matches(selector, store.partition_labels_of(journal))
                    .with_context(|| format!("matching partition selector against {journal}"))?;
                if !admitted {
                    continue;
                }
                let target = targets[key];
                let from = self.fed.get(key).copied().unwrap_or(0);

                for stored in store.read_window(journal, from, target) {
                    let doc: serde_json::Value = serde_json::from_slice(&stored.body)
                        .context("parsing stored source document to feed")?;
                    docs.push((stored.txn, *bi, doc));
                }
            }
        }
        docs.sort_by_key(|(txn, _, _)| *txn);

        let docs: Vec<(usize, uuid::Clock, serde_json::Value)> = docs
            .into_iter()
            .map(|(_, bi, doc)| {
                self.clock = self.clock.tick();
                (bi, self.clock, doc)
            })
            .collect();

        let items: Vec<(usize, uuid::Clock, &serde_json::Value)> = docs
            .iter()
            .map(|(bi, clock, doc)| (*bi, *clock, doc))
            .collect();

        let frontier = segments::write_transaction_for_bindings(
            &items,
            &self.bindings,
            &self.sources,
            &mut self.validators,
            &self.shards,
            &mut self.writers,
            &mut self.sealed,
            &mut self.journal_offsets,
            &mut bytes::BytesMut::new(),
        )?;
        self.frontier_tx
            .send(segments::FixtureItem::Frontier(frontier))
            .map_err(|_| anyhow::anyhow!("{}: shuffle session closed", self.derivation))?;

        // Advance fed offsets and await this transaction's commit.
        for (key, target) in targets {
            self.fed.insert(key, target);
        }
        await_signal(
            &mut self.commit_rx,
            &mut self.failure_rx,
            &self.derivation,
            "commit",
        )
        .await?;

        // We fed exactly through `read_through` and awaited the transaction's
        // commit, so reporting `read_through` as the derivation's read progress
        // is exact: there is no partially-read remainder to min-reduce over.
        self.cumulative_read =
            crate::clock::max_clock(&self.cumulative_read, &pending.read_through);
        let write_at = self
            .store
            .lock()
            .unwrap()
            .write_clock(&self.output_template);

        Ok((self.cumulative_read.clone(), write_at))
    }

    /// Reset the derivation's connector state between test cases: only
    /// connector-internal state — derive-sqlite registers, a TypeScript module's
    /// state — is cleared, while read frontiers, feed cursors, and collection
    /// data persist. Cases are therefore isolated in what a connector remembers,
    /// but share one monotonic store, which Verify's `(from, to]` window slices
    /// per case.
    ///
    /// Reset is shard-local (the leader is not in its path), so this sends one
    /// Reset per shard and awaits one `ResetDone` each. It is safe to send here
    /// because the session is quiescent between reads, with no transaction open.
    pub async fn reset(&mut self) -> anyhow::Result<()> {
        for request_tx in &self.request_txs {
            request_tx
                .send(Ok(proto::Derive {
                    reset: Some(proto::Reset {}),
                    ..Default::default()
                }))
                .map_err(|_| anyhow::anyhow!("{}: session closed before Reset", self.derivation))?;
        }

        for _ in 0..self.request_txs.len() {
            await_signal(
                &mut self.reset_rx,
                &mut self.failure_rx,
                &self.derivation,
                "Reset completed",
            )
            .await?;
        }
        Ok(())
    }

    /// Gracefully stop the session, then drop the request streams (EOF).
    pub async fn shutdown(mut self) -> anyhow::Result<()> {
        let result = stop_shards(&self.request_txs, std::mem::take(&mut self.shard_handles)).await;
        // Now EOF each request stream, letting each shard's serve loop finish.
        self.request_txs.clear();
        result
    }
}

/// Stop every shard and await each drainer's Stopped confirmation, reporting the
/// first failure but always awaiting all of them.
///
/// The caller retains `request_txs` and drops them only once this returns:
/// EOF-ing them mid-handshake is rejected by the shard as an unexpected
/// controller EOF.
async fn stop_shards(
    request_txs: &[mpsc::UnboundedSender<tonic::Result<proto::Derive>>],
    shard_handles: Vec<tokio::task::JoinHandle<anyhow::Result<()>>>,
) -> anyhow::Result<()> {
    for request_tx in request_txs {
        let _ = request_tx.send(Ok(proto::Derive {
            stop: Some(proto::Stop {}),
            ..Default::default()
        }));
    }

    let mut first_err = None;
    for handle in shard_handles {
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

    match first_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Await one signal of a running session — a transaction commit, or one shard's
/// `ResetDone` — failing with the session's own error if it died first.
///
/// Neither signal channel closes when a session dies — the run's tonic server
/// outlives every session and holds `commit_rx`'s sender, and `reset_rx`'s
/// senders are per-shard — so waiting on one alone can only ever hang.
/// `failure_rx` closes the gap: each drainer reports there as it ends, and the
/// channel itself closes once none are left. A *closed* `failure_rx` is itself
/// terminal: every drainer has exited and its one failure (if any) was consumed
/// by an earlier await, so no signal can ever arrive.
async fn await_signal(
    signal_rx: &mut mpsc::UnboundedReceiver<()>,
    failure_rx: &mut mpsc::UnboundedReceiver<anyhow::Error>,
    derivation: &str,
    what: &str,
) -> anyhow::Result<()> {
    tokio::select! {
        biased;

        failure = failure_rx.recv() => match failure {
            Some(err) => Err(err.context(format!("{derivation}: session failed awaiting {what}"))),
            None => Err(anyhow::anyhow!("{derivation}: session ended before {what}")),
        },
        Some(()) = signal_rx.recv() => Ok(()),
    }
}

/// Drain a shard's response stream: signal readiness on the first Opened, forward
/// each `ResetDone`, then keep draining (surfacing errors) until the request
/// stream closes and the shard EOFs.
///
/// Ending without having seen Stopped means no further commit or `ResetDone` can
/// arrive, so the reason is published to `failure_tx` for whichever
/// [`await_signal`] is waiting on one — and also logged through the task's ops
/// `logger`, because the runtime reports a session's death as a stream status
/// rather than an ops log, and nothing else lands it in the task's log stream.
async fn drain_shard<L: runtime_next::Logger>(
    mut response_rx: mpsc::UnboundedReceiver<tonic::Result<proto::Derive>>,
    shard_index: u32,
    ready_tx: tokio::sync::oneshot::Sender<()>,
    reset_done_tx: mpsc::UnboundedSender<()>,
    failure_tx: mpsc::UnboundedSender<anyhow::Error>,
    logger: L,
) -> anyhow::Result<()> {
    let mut ready_tx = Some(ready_tx);

    let stopped = async {
        while let Some(msg) = response_rx.recv().await {
            let msg = msg.map_err(runtime_next::status_to_anyhow)?;

            if msg.opened.is_some() {
                if let Some(tx) = ready_tx.take() {
                    let _ = tx.send(());
                }
            } else if msg.joined.is_some() {
                tracing::debug!(shard_index, "runner shard joined");
            } else if msg.reset_done.is_some() {
                tracing::debug!(shard_index, "runner shard reset done");
                let _ = reset_done_tx.send(());
            } else if msg.stopped.is_some() {
                // Graceful shutdown: return so `shutdown` can drop the request
                // stream and end the serve loop.
                tracing::debug!(shard_index, "runner shard stopped");
                return anyhow::Ok(true);
            }
        }
        anyhow::Ok(false)
    }
    .await;

    let failure = match &stopped {
        Ok(true) => None,
        Ok(false) => Some(format!(
            "shard {shard_index} ended its session without stopping"
        )),
        Err(err) => Some(format!("shard {shard_index} failed: {err:#}")),
    };
    if let Some(failure) = failure {
        logger.log(&ops::Log {
            timestamp: Some(proto_flow::as_timestamp(std::time::SystemTime::now())),
            level: ops::LogLevel::Error as i32,
            message: failure.clone(),
            ..Default::default()
        });
        let _ = failure_tx.send(anyhow::anyhow!(failure));
    }
    stopped.map(|_| ())
}
