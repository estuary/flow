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
use proto_flow::flow;
use proto_gazette::uuid;
use runtime_local::segments;
use runtime_next::proto;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

/// One shard's controller-facing stream pair.
struct Shard {
    /// `{derivation} shard {index}`, used with [`runtime_next::verify`] expectations.
    peer: String,
    /// The shard's request stream, until [`stop_shards`] drops it to end the
    /// shard's session loop. `None` only over the last step of a shutdown.
    request_tx: Option<mpsc::UnboundedSender<tonic::Result<proto::Derive>>>,
    response_rx: mpsc::UnboundedReceiver<tonic::Result<proto::Derive>>,
}

/// A single derivation, resident as a runtime-next session for the run.
pub struct DerivationSession {
    derivation: String,
    /// Partition-template name of the derivation's own (output) collection:
    /// the prefix which selects its journals from the store.
    output_template: String,

    // --- Live session (dropped last: aborts the server after shard streams). ---
    /// Per-shard controller streams.
    shards: Vec<Shard>,
    _run: runtime_local::services::Run,

    /// Push checkpoint frontiers to the leader's channel-fed shuffle session.
    frontier_tx: mpsc::UnboundedSender<segments::FixtureItem>,
    /// One `()` per committed transaction, from the leader publisher's
    /// `write_intents` — the one signal of a live session that doesn't arrive on
    /// a shard's stream. See [`await_commit`].
    commit_rx: mpsc::UnboundedReceiver<()>,

    // --- Feed state (segment writer inputs, advanced across the run). ---
    bindings: Vec<shuffle::Binding>,
    sources: Vec<shuffle::Source>,
    validators: Vec<doc::Validator>,
    /// Read-checkpoint suffix (`derive/{derivation}/{transform}`) → binding index.
    suffix_to_binding: HashMap<String, usize>,
    /// Shuffle's view of shards: the key ranges fed documents are routed over.
    shard_shuffles: Vec<shuffle::proto::Shard>,
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

        // Spawn each shard and drive SessionLoop / Join / Task.
        let mut shards = Vec::with_capacity(n_shards as usize);

        for i in 0..n_shards {
            let (request_tx, request_rx) =
                mpsc::unbounded_channel::<tonic::Result<proto::Derive>>();
            let shard_svc = runtime_next::shard::Service::new(
                run.connector_router.clone(),
                None,
                derivation.clone(),
                publisher_factory.clone(),
                logger_factory.clone(),
                run.registry.clone(),
                None, // No AuthN+AuthZ signer (local loopback).
            );
            let response_rx = shard_svc.spawn_derive(UnboundedReceiverStream::new(request_rx));

            for msg in [
                proto::Derive {
                    // No path: `RocksDB` then makes and owns a tempdir. Tests
                    // keep no state across a run, so there's nothing to name a
                    // path for, and no final state to report.
                    session_loop: Some(proto::SessionLoop {
                        rocksdb_descriptor: None,
                        initial_connector_state_json: bytes::Bytes::new(),
                        report_final_state: false,
                    }),
                    ..Default::default()
                },
                proto::Derive {
                    join: Some(proto::Join {
                        etcd_mod_revision: 1,
                        shards: join_shards.clone(),
                        shard_index: i,
                        shuffle_directory: shard_dirs[i as usize].clone(),
                        shuffle_endpoint: run.peer_endpoint.clone(),
                        leader_endpoint: run.peer_endpoint.clone(),
                    }),
                    ..Default::default()
                },
                proto::Derive {
                    task: Some(proto::Task {
                        spec: spec_bytes.clone(),
                        max_transactions: 0, // Unbounded: the session is resident.
                        // Empty → derive-sqlite runs `:memory:` and reports no
                        // checkpoint, which is what permits multi-shard
                        // sessions; see crate README.
                        sqlite_vfs_uri: String::new(),
                        publisher_id: Default::default(),
                    }),
                    ..Default::default()
                },
            ] {
                // Sends are best-effort. Errors surface on read.
                _ = request_tx.send(Ok(msg));
            }

            shards.push(Shard {
                peer: format!("{derivation} shard {i}"),
                request_tx: Some(request_tx),
                response_rx,
            });
        }

        // Await each shard's Joined and Opened, then consume the leader's
        // session-startup commit signal (see the `publish` module docs), which
        // the first read would otherwise take for its own.
        let started = async {
            () = recv_opened(&mut shards).await?;
            await_commit(&mut shards, &mut commit_rx, "startup").await
        }
        .await;

        if let Err(err) = started {
            _ = stop_shards(&mut shards).await; // Best-effort stop-EOF-drain.
            return Err(err);
        }

        Ok(Self {
            derivation,
            output_template: crate::partitions::template_name(&spec)?.to_string(),
            shards,
            _run: run,
            frontier_tx,
            commit_rx,
            bindings,
            sources,
            validators,
            suffix_to_binding,
            shard_shuffles: segments::full_range_shards(n_shards),
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
            &self.shard_shuffles,
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
        () = await_commit(&mut self.shards, &mut self.commit_rx, "transaction").await?;

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
    /// Reset per shard and awaits each shard's `ResetDone`. It is safe to send
    /// here because the session is quiescent between reads, with no transaction
    /// open.
    pub async fn reset(&mut self) -> anyhow::Result<()> {
        broadcast(
            &self.shards,
            proto::Derive {
                reset: Some(proto::Reset {}),
                ..Default::default()
            },
        );

        _ = futures::future::try_join_all(self.shards.iter_mut().map(|shard| async move {
            let verify = runtime_next::verify("Derive", "ResetDone", &shard.peer);
            match verify.not_eof(shard.response_rx.recv().await)? {
                proto::Derive {
                    reset_done: Some(_),
                    ..
                } => Ok(()),
                other => Err(verify.fail_msg(other)),
            }
        }))
        .await?;

        Ok(())
    }

    /// Gracefully stop the session, drop the request streams (EOF), and read
    /// every shard's response stream through to termination.
    pub async fn shutdown(mut self) -> anyhow::Result<()> {
        stop_shards(&mut self.shards).await
    }
}

/// Send one message to every shard, best-effort: a shard whose serve task has
/// ended surfaces on its next response, with the expectation it failed to meet.
fn broadcast(shards: &[Shard], msg: proto::Derive) {
    for shard in shards {
        if let Some(request_tx) = &shard.request_tx {
            let _ = request_tx.send(Ok(msg.clone()));
        }
    }
}

/// Gracefully stop every shard, then EOF its request stream and read its
/// response stream through to termination.
async fn stop_shards(shards: &mut [Shard]) -> anyhow::Result<()> {
    broadcast(
        shards,
        proto::Derive {
            stop: Some(proto::Stop {}),
            ..Default::default()
        },
    );

    // Unlike a starting session's fan-ins, both fan-ins here await *every*
    // shard rather than short-circuiting on the first failure.
    let stopped = first_error(
        futures::future::join_all(shards.iter_mut().map(|shard| async move {
            let verify = runtime_next::verify("Derive", "Stopped", &shard.peer);
            match verify.not_eof(shard.response_rx.recv().await)? {
                proto::Derive {
                    stopped: Some(_), ..
                } => Ok(()),
                other => Err(verify.fail_msg(other)),
            }
        }))
        .await,
    );

    // Every session loop is back at its `Join` await, so EOF is now graceful.
    for shard in shards.iter_mut() {
        shard.request_tx = None;
    }

    let drained = first_error(
        futures::future::join_all(shards.iter_mut().map(|shard| async move {
            let verify = runtime_next::verify("Derive", "EOF after Stopped", &shard.peer);
            verify.eof(shard.response_rx.recv().await)
        }))
        .await,
    );

    stopped.and(drained)
}

/// Await each shard's `Joined` and then its `Opened`.
async fn recv_opened(shards: &mut [Shard]) -> anyhow::Result<()> {
    _ = futures::future::try_join_all(shards.iter_mut().map(|shard| async move {
        let verify = runtime_next::verify("Derive", "Joined", &shard.peer);
        match verify.not_eof(shard.response_rx.recv().await)? {
            proto::Derive {
                joined: Some(_), ..
            } => (),
            other => return Err(verify.fail_msg(other)),
        }

        let verify = runtime_next::verify("Derive", "Opened", &shard.peer);
        match verify.not_eof(shard.response_rx.recv().await)? {
            proto::Derive {
                opened: Some(_), ..
            } => Ok(()),
            other => Err(verify.fail_msg(other)),
        }
    }))
    .await?;

    Ok(())
}

/// Reduce a fan-in's per-shard results to its first error, tracing the rest.
fn first_error(results: Vec<anyhow::Result<()>>) -> anyhow::Result<()> {
    let mut first = None;

    for result in results {
        match result {
            Ok(()) => {}
            Err(err) if first.is_none() => first = Some(err),
            Err(err) => tracing::warn!(?err, "secondary shard error"),
        }
    }

    match first {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

/// Await the leader's next transaction commit, failing with a shard's own error
/// if the session died first.
///
/// The commit arrives through the `Publisher` seam (see [`crate::publish`]) and
/// never on a shard's stream, so waiting on `commit_rx` alone could only hang:
/// nothing closes it when a session dies, as the run's tonic server outlives
/// every session and holds its sender. [`shard_failure`] closes the gap.
async fn await_commit(
    shards: &mut [Shard],
    commit_rx: &mut mpsc::UnboundedReceiver<()>,
    what: &str,
) -> anyhow::Result<()> {
    tokio::select! {
        biased;

        err = shard_failure(shards) => Err(err.context(format!("session failed awaiting {what} commit"))),

        commit = commit_rx.recv() => match commit {
            Some(()) => Ok(()),
            None => Err(anyhow::anyhow!("commit channel closed awaiting {what} commit")),
        },
    }
}

/// Resolve with the error of the first shard to say *anything*.
///
/// A shard sends its controller nothing between its `Opened` and its `Stopped`,
/// so during a transaction every outcome of a response stream — an unexpected
/// message, an `Err`, or EOF — is the session coming apart, and this future
/// resolves only when one has. Each `recv` is cancel-safe, so losing the race
/// drops the other shards' polls without dropping their messages.
async fn shard_failure(shards: &mut [Shard]) -> anyhow::Error {
    let (err, _index, _remaining) = futures::future::select_all(shards.iter_mut().map(|shard| {
        let Shard {
            peer, response_rx, ..
        } = shard;

        Box::pin(async move {
            let verify = runtime_next::verify("Derive", "no message until commit", peer);

            match response_rx.recv().await {
                None => verify.fail_err(anyhow::anyhow!("unexpected EOF")),
                Some(Err(status)) => verify.fail_status(status),
                Some(Ok(msg)) => verify.fail_msg(msg),
            }
        })
    }))
    .await;

    err
}
