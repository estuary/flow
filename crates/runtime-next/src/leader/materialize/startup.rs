use super::super::frontier_mapping;
use super::Task;
use crate::proto;
use anyhow::Context;
use bytes::Bytes;
use futures::{StreamExt, stream::BoxStream};
use prost::Message;
use proto_flow::flow;
use proto_gazette::{consumer, uuid};
use std::collections::BTreeMap;
use tokio::sync::mpsc;

/// Outcomes of the leader protocol startup phase.
pub(super) struct Startup<P: crate::Publisher, S: crate::leader::ShuffleSession, L: crate::Logger> {
    // Clock at which the last-committed transaction closed.
    pub committed_close: uuid::Clock,
    // Fully committed Frontier.
    pub committed_frontier: shuffle::Frontier,
    // Is the first transaction an idempotent replay of a recovered hinted Frontier?
    pub idempotent_replay: bool,
    // Logger of task-centric state changes and events.
    pub logger: L,
    // Recovered ACK intents of the last transaction.
    pub pending_ack_intents: BTreeMap<String, Bytes>,
    // Recovered variables for the task.
    pub pending_trigger_params: Bytes,
    // Publisher for writing stats and ACK intents.
    pub publisher: P,
    // Initiated shuffle session for the task and topology.
    pub session: S,
    // Task definition.
    pub task: Task,
}

#[tracing::instrument(
    level = "debug",
    err(Debug, level = "warn"),
    skip_all,
    fields(shard_zero = %shard_ids[0], shards = shard_ids.len())
)]
pub(super) async fn run<
    S: crate::ShuffleSessionFactory,
    P: crate::PublisherFactory,
    L: crate::LoggerFactory,
>(
    build: String,
    drop_v1_rollback: bool,
    ops_stats_journal: String,
    reactors: Vec<String>,
    shard_rx: &mut Vec<BoxStream<'static, tonic::Result<proto::Materialize>>>,
    shard_tx: &Vec<mpsc::UnboundedSender<tonic::Result<proto::Materialize>>>,
    service: &crate::Service<S, P, L>,
    shard_ids: Vec<String>,
    shard_shuffles: Vec<shuffle::proto::Shard>,
) -> anyhow::Result<Startup<P::Publisher, S::Session, L::Logger>> {
    let n_shards = reactors.len();
    assert_eq!(n_shards, shard_rx.len());
    assert_eq!(n_shards, shard_tx.len());
    assert_eq!(n_shards, shard_ids.len());
    assert_eq!(n_shards, shard_shuffles.len());

    let peers: Vec<String> = shard_ids
        .iter()
        .zip(reactors.iter())
        .map(|(shard_id, reactor)| format!("{shard_id}@{reactor}"))
        .collect();

    // Send L:Joined response to all shards.
    for tx in shard_tx {
        let _ = tx.send(Ok(proto::Materialize {
            joined: Some(proto::Joined {
                max_etcd_revision: 0, // Success.
            }),
            ..Default::default()
        }));
    }

    // Receive L:Task from shard zero.
    let verify = crate::verify("Materialize", "Open", &peers[0]);
    let task = match verify.not_eof(shard_rx[0].next().await)? {
        proto::Materialize {
            task: Some(task), ..
        } => task,
        other => return Err(verify.fail_msg(other)),
    };

    // Build task definition.
    let proto::Task {
        max_transactions,
        spec: spec_bytes,
        sqlite_vfs_uri: _,
        publisher_id,
    } = task;

    let spec = flow::MaterializationSpec::decode(spec_bytes.as_ref())
        .context("invalid Task materialization")?;
    let task = Task::new(build, &spec, max_transactions, peers)
        .await
        .context("building task definition")?;

    // Open a Logger for runtime events, bound to the task.
    let logger = service.logger_factory.open(&task.shard_ref.name);

    // Open a publisher for stats and ACK intents (no collection bindings).
    let publisher = service
        .publisher_factory
        .open(
            shard_ids[0].clone(), // Shard zero is AuthZ subject.
            crate::publish::producer_from_bytes(&publisher_id)?,
            &ops_stats_journal,
            &[],
        )
        .context("opening publisher")?;

    // Receive Recover fan-in.
    let proto::Recover {
        ack_intents: pending_ack_intents,
        committed_close_clock: committed_close,
        committed_frontier,
        mut connector_state_json,
        hinted_close_clock: hinted_close,
        hinted_frontier,
        last_applied,
        legacy_checkpoint,
        max_keys,
        trigger_params_json: pending_trigger_params,
    } = recv_recovers(shard_rx, &task.peers)
        .await
        .context("receiving Recover fan-in")?;

    let committed_close = uuid::Clock::from_u64(committed_close);
    let hinted_close = uuid::Clock::from_u64(hinted_close);

    let hinted_frontier = shuffle::Frontier::decode(hinted_frontier.unwrap_or_default())
        .context("validating hinted Frontier")?;
    let committed_frontier = shuffle::Frontier::decode(committed_frontier.unwrap_or_default())
        .context("validating committed Frontier")?;

    tracing::debug!(
        ?committed_close,
        ?committed_frontier,
        connector_state_bytes = connector_state_json.len(),
        ?hinted_close,
        ?hinted_frontier,
        last_applied = !last_applied.is_empty(),
        ?legacy_checkpoint,
        ?max_keys,
        ?pending_trigger_params,
        "collected Recover from all shards",
    );

    // Run Apply on shard zero until convergence.
    apply_loop(
        &mut shard_rx[0],
        &shard_tx[0],
        &task.peers[0],
        &last_applied,
        &spec_bytes,
        &task.shard_ref.build,
        &mut connector_state_json,
        &logger,
    )
    .await?;

    // Open connectors across all shards.
    for (tx, shard) in shard_tx.iter().zip(shard_shuffles.iter()) {
        let _ = tx.send(Ok(proto::Materialize {
            open: Some(proto::Open {
                spec: spec_bytes.clone(),
                version: task.shard_ref.build.clone(),
                range: shard.range.clone(),
                connector_state_json: connector_state_json.clone(),
                max_keys: max_keys.clone(),
            }),
            ..Default::default()
        }));
    }

    // Receive Opened fan-in.
    let proto::materialize::Opened {
        container: _, // Not sent to leader.
        connector_checkpoint,
    } = recv_opened(shard_rx, &task.peers)
        .await
        .context("receiving Opened fan-in")?;
    let connector_checkpoint = connector_checkpoint.unwrap_or_default();

    // Build sorted index on journal_read_suffix => binding index, for frontier mapping.
    let mut journal_read_suffix_index: Vec<(&str, usize)> = task
        .binding_journal_read_suffixes
        .iter()
        .enumerate()
        .map(|(i, b)| (b.as_str(), i))
        .collect();
    journal_read_suffix_index.sort();

    // Reconcile recovered RocksDB state against any authoritative checkpoint,
    // yielding the session resume Frontier and an optional RocksDB cleanup.
    let Reconciled {
        committed_close,
        committed_frontier,
        pending_ack_intents,
        resume_frontier,
        idempotent_replay,
        cleanup_persist,
    } = reconcile_recovered(
        drop_v1_rollback,
        committed_close,
        hinted_close,
        committed_frontier,
        hinted_frontier,
        legacy_checkpoint,
        connector_checkpoint,
        pending_ack_intents,
        &journal_read_suffix_index,
    )?;

    // Durably apply the cleanup so future recoveries observe the reconciled
    // baseline (this is the only IO the reconciliation implies).
    if let Some(cleanup_persist) = cleanup_persist {
        send_persist(
            &mut shard_rx[0],
            &shard_tx[0],
            &task.peers[0],
            cleanup_persist,
        )
        .await
        .context("sending startup cleanup Persist")?;
    }

    // Open the shuffle Session with the recovered resume Frontier.
    let shuffle_task = shuffle::proto::Task {
        task: Some(shuffle::proto::task::Task::Materialization(spec)),
    };
    let session = service
        .shuffle_factory
        .open(shuffle_task, shard_shuffles, resume_frontier)
        .await
        .context("opening shuffle Session")?;

    Ok(Startup {
        committed_close,
        committed_frontier,
        idempotent_replay,
        logger,
        pending_ack_intents,
        pending_trigger_params,
        publisher,
        session,
        task,
    })
}

/// Outcome of [`reconcile_recovered`]: the resume Frontier plus any RocksDB
/// cleanup that `run` must durably apply before opening the shuffle Session.
struct Reconciled {
    /// Final committed close Clock. Advances to the recovered `hinted_close`
    /// when a remote-authoritative connector confirms the hinted txn committed.
    committed_close: uuid::Clock,
    /// Final committed Frontier, possibly rebuilt from an authoritative checkpoint.
    committed_frontier: shuffle::Frontier,
    /// Final ACK intents; replaced by an authoritative checkpoint's when present.
    pending_ack_intents: BTreeMap<String, Bytes>,
    /// Session resume Frontier: `project_hinted(hinted).reduce(committed)`.
    resume_frontier: shuffle::Frontier,
    /// Whether the first transaction must idempotently replay a hinted commit.
    idempotent_replay: bool,
    /// Startup cleanup `Persist` to durably reconcile RocksDB, or `None` when
    /// nothing needs reconciling.
    cleanup_persist: Option<proto::Persist>,
}

/// Reconcile the RocksDB-recovered frontiers (`committed_frontier`,
/// `hinted_frontier`, `committed_close`) against any authoritative checkpoint —
/// a `legacy_checkpoint` (V1 rollback migration) or a remote-authoritative
/// `connector_checkpoint` recovered from Open. Returns the composed session
/// resume Frontier and the RocksDB cleanup its caller must persist.
///
/// Pure (no IO), so the reconciliation policy is unit-testable in isolation
/// from the leader's shard-protocol plumbing. `journal_read_suffix_index` maps
/// `journal_read_suffix` => binding index and must be sorted on the suffix.
fn reconcile_recovered(
    drop_v1_rollback: bool,
    mut committed_close: uuid::Clock,
    hinted_close: uuid::Clock,
    mut committed_frontier: shuffle::Frontier,
    mut hinted_frontier: shuffle::Frontier,
    legacy_checkpoint: Option<consumer::Checkpoint>,
    connector_checkpoint: consumer::Checkpoint,
    mut pending_ack_intents: BTreeMap<String, Bytes>,
    journal_read_suffix_index: &[(&str, usize)],
) -> anyhow::Result<Reconciled> {
    // Set when a recovered checkpoint (legacy V1 or connector) is authoritative
    // and its mapped Frontier replaces `committed_frontier`.
    //
    // Rebuilding also forces us to discard the recovered `hinted_frontier`. The
    // hinted frontier is a read-ahead of `committed_frontier` that's replayed at
    // startup: `project_hinted` zeroes each hinted producer's read offset and
    // relies on that producer's entry in `committed_frontier` to restore it.
    // A rebuilt committed frontier is a fresh mapping of the authoritative
    // checkpoint, and won't carry producers the checkpoint has dropped — e.g. the
    // V1 runtime prunes a producer that's been silent for >24h. Any hinted
    // producer without a committed counterpart then resolves to offset 0, forcing
    // a full re-read of the journal (from the beginning) to replay a hint whose
    // producer has long gone silent and whose data the checkpoint already
    // reflects. Discarding the hinted frontier avoids this: the authoritative
    // checkpoint is a complete `committed_frontier`, so there's nothing to replay.
    let mut committed_frontier_rebuilt = false;

    // Handle migration from `legacy_checkpoint`.
    let legacy_checkpoint_present = legacy_checkpoint.is_some();
    if let Some(legacy_checkpoint) = legacy_checkpoint {
        let clock = frontier_mapping::extract_committed_close(&legacy_checkpoint);

        if clock == Some(committed_close) {
            service_kit::event!(
                tracing::Level::DEBUG,
                "leader",
                committed_close,
                "legacy_checkpoint present but matches Recover::committed_close (ignoring)",
            );
        } else if let Some(clock) = clock {
            // Implementation error: these update together and should always sync.
            anyhow::bail!(
                "legacy_checkpoint has clock {clock:?} that doesn't match Recover's committed_close ({committed_close:?})"
            );
        } else {
            service_kit::event!(
                tracing::Level::DEBUG,
                "leader",
                committed_close,
                "legacy_checkpoint doesn't contain committed-close-clock; treating as authoritative",
            );
            committed_frontier = frontier_mapping::checkpoint_to_frontier(
                &legacy_checkpoint.sources,
                journal_read_suffix_index,
            )
            .context("mapping recovered legacy checkpoint into Frontier")?;

            hinted_frontier = Default::default(); // Discard; see `committed_frontier_rebuilt`.
            committed_frontier_rebuilt = true;

            pending_ack_intents = legacy_checkpoint.ack_intents;
        }
    } else {
        service_kit::event!(
            tracing::Level::DEBUG,
            "leader",
            "no legacy_checkpoint present",
        );
    }

    // Handle a `connector_checkpoint` from remote-authoritative connectors.
    // It may be *ahead* of `committed_frontier`, which is detected as its embedded
    // committed-close Clock matching our recovered `hinted_close`.
    if !connector_checkpoint.sources.is_empty() {
        let clock = frontier_mapping::extract_committed_close(&connector_checkpoint);

        if clock == Some(committed_close) {
            service_kit::event!(
                tracing::Level::DEBUG,
                "leader",
                committed_close,
                "connector_checkpoint present but matches Recover::committed_close (ignoring)",
            );
        } else if clock == Some(hinted_close) {
            // Connector declares that the hinted txn did in fact commit.
            service_kit::event!(
                tracing::Level::DEBUG,
                "leader",
                committed_close,
                hinted_close,
                "connector_checkpoint present and matches Recover::hinted_close; applying delta",
            );
            committed_close = hinted_close;
            committed_frontier = committed_frontier.reduce(std::mem::take(&mut hinted_frontier));

            pending_ack_intents = connector_checkpoint.ack_intents;
        } else if let Some(clock) = clock {
            // Implementation error: these update together and should always sync.
            anyhow::bail!(
                "connector_checkpoint has clock {clock:?} which doesn't match Recover's \
                 committed_close ({committed_close:?}) or hinted_close ({hinted_close:?})"
            );
        } else {
            service_kit::event!(
                tracing::Level::DEBUG,
                "leader",
                committed_close,
                "connector_checkpoint doesn't contain committed-close-clock; treating as authoritative",
            );

            committed_frontier = frontier_mapping::checkpoint_to_frontier(
                &connector_checkpoint.sources,
                journal_read_suffix_index,
            )
            .context("mapping recovered connector checkpoint into Frontier")?;

            hinted_frontier = Default::default(); // Discard; see `committed_frontier_rebuilt`.
            committed_frontier_rebuilt = true;

            pending_ack_intents = connector_checkpoint.ack_intents;
        }
    } else {
        service_kit::event!(
            tracing::Level::DEBUG,
            "leader",
            "no connector_checkpoint present",
        );
    }

    // Reconcile RocksDB now that the final status of the recovered V1 and
    // connector checkpoints is known. If `committed_frontier_rebuilt`, then
    // `committed_frontier` is not natively represented in RocksDB and must be
    // persisted (clearing stale state). This establishes a baseline for future
    // recoveries. Go-forward commits are deltas that apply atop this base.
    let delete_legacy_checkpoint = drop_v1_rollback && legacy_checkpoint_present;
    let cleanup_persist = (committed_frontier_rebuilt || delete_legacy_checkpoint).then(|| {
        service_kit::event!(
            tracing::Level::INFO,
            "leader",
            committed_frontier_rebuilt,
            delete_legacy_checkpoint,
            "reconciling recovered checkpoint state",
        );
        proto::Persist {
            seq_no: 0,
            delete_committed_frontier: committed_frontier_rebuilt,
            delete_hinted_frontier: committed_frontier_rebuilt,
            committed_frontier: committed_frontier_rebuilt
                .then(|| shuffle::JournalFrontier::encode(&committed_frontier.journals)),
            delete_legacy_checkpoint,
            ..Default::default()
        }
    });

    // Compose the session resume Frontier: project the recovered hinted
    // Frontier into hinted form (last_commit -> hinted_commit, zero
    // last_commit/offset) and reduce with the committed Frontier.
    let resume_frontier =
        frontier_mapping::project_hinted(hinted_frontier).reduce(committed_frontier.clone());

    // If we recovered a producer frontier with an unapplied hinted commit,
    // then the first transaction must be an idempotent replay of the hinted frontier.
    let idempotent_replay = resume_frontier.unresolved_hints != 0;

    Ok(Reconciled {
        committed_close,
        committed_frontier,
        pending_ack_intents,
        resume_frontier,
        idempotent_replay,
        cleanup_persist,
    })
}

async fn recv_recovers(
    request_rxs: &mut [BoxStream<'static, tonic::Result<proto::Materialize>>],
    peers: &[String],
) -> anyhow::Result<proto::Recover> {
    let mut recovers = futures::future::try_join_all(request_rxs.into_iter().enumerate().map(
        |(shard_index, rx)| async move {
            let verify = crate::verify("Materialize", "Recover", &peers[shard_index]);
            match verify.not_eof(rx.next().await)? {
                proto::Materialize {
                    recover: Some(recover),
                    ..
                } if shard_index == 0 || recover == proto::Recover::default() => {
                    Ok::<_, anyhow::Error>(recover)
                }
                other => Err(verify.fail_msg(other)),
            }
        },
    ))
    .await?;

    Ok(recovers.swap_remove(0))
}

/// Send a `Persist` to a shard and await the matching `Persisted` echo.
async fn send_persist(
    rx: &mut BoxStream<'static, tonic::Result<proto::Materialize>>,
    tx: &mpsc::UnboundedSender<tonic::Result<proto::Materialize>>,
    peer: &str,
    persist: proto::Persist,
) -> anyhow::Result<()> {
    let verify = crate::verify("Materialize", "Persisted", peer);
    let seq_no = persist.seq_no;

    // Sends are best-effort: a closed peer surfaces on the next `rx`.
    let _ = tx.send(Ok(proto::Materialize {
        persist: Some(persist),
        ..Default::default()
    }));

    match verify.not_eof(rx.next().await)? {
        proto::Materialize {
            persisted: Some(proto::Persisted { seq_no: got }),
            ..
        } if got == seq_no => Ok(()),
        other => Err(verify.fail_msg(other)),
    }
}

// The apply loop's persistent state machine is `(last_applied,
// connector_state_json)`. Each iteration may persist new connector state
// patches; `last_applied` is bumped only on the FINAL iteration once the
// connector returns no further patches. A crash mid-loop therefore resumes
// with the OLD `last_applied` against the partially-advanced state,
// requiring the connector's Apply to be idempotent across repeated
// invocations of the same target spec.
async fn apply_loop<L: crate::Logger>(
    rx: &mut BoxStream<'static, tonic::Result<proto::Materialize>>,
    tx: &mpsc::UnboundedSender<tonic::Result<proto::Materialize>>,
    peer: &str,
    last_applied: &Bytes,
    next_applied: &Bytes,
    next_version: &str,
    connector_state_json: &mut Bytes,
    logger: &L,
) -> anyhow::Result<()> {
    let verify_applied = crate::verify("Materialize", "Applied", peer);
    let last_version = if last_applied.is_empty() {
        String::new()
    } else {
        let last_spec = flow::MaterializationSpec::decode(last_applied.as_ref())
            .context("invalid recovered last-applied spec")?;
        labels_build_for(&last_spec)
    };

    const MAX_APPLY_ITERATIONS: u64 = 3;

    for iteration in 1..=MAX_APPLY_ITERATIONS {
        // Send Apply carrying the current reduced connector state.
        // Sends are best-effort: a closed peer will surface on the next rx.
        let _ = tx.send(Ok(proto::Materialize {
            apply: Some(proto::Apply {
                spec: next_applied.clone(),
                version: next_version.to_string(),
                last_spec: last_applied.clone(),
                last_version: last_version.clone(),
                connector_state_json: connector_state_json.clone(),
            }),
            ..Default::default()
        }));

        // Receive Applied.
        let applied_patches_json = match verify_applied.not_eof(rx.next().await)? {
            proto::Materialize {
                applied:
                    Some(proto::Applied {
                        action_description,
                        connector_patches_json,
                    }),
                ..
            } => {
                logger.event(crate::LogEvent::Applied {
                    action_description: &action_description,
                });

                service_kit::event!(
                    tracing::Level::INFO,
                    "leader",
                    iteration,
                    action_description,
                    patches = service_kit::event::debug(connector_patches_json.clone()),
                    "connector Apply completed",
                );
                connector_patches_json
            }
            other => return Err(verify_applied.fail_msg(other)),
        };

        if applied_patches_json.is_empty() {
            service_kit::event!(
                tracing::Level::DEBUG,
                "leader",
                iteration,
                "apply loop complete",
            );

            if last_applied == next_applied {
                return Ok(());
            }

            send_persist(
                rx,
                tx,
                peer,
                proto::Persist {
                    seq_no: iteration,
                    last_applied: next_applied.clone(),
                    ..Default::default()
                },
            )
            .await?;

            return Ok(());
        }

        // Fold the iteration's patches into the running reduced state so
        // subsequent Apply iterations and Open observe the newly-applied state.
        *connector_state_json =
            crate::patches::apply_state_patches(connector_state_json, &applied_patches_json)?;

        // Persist the iteration's patches to shard zero, observing the delta.
        let persist = proto::Persist {
            seq_no: iteration, // End-of-sequence.
            connector_patches_json: applied_patches_json,
            ..Default::default()
        };
        logger.event(crate::LogEvent::Persist { persist: &persist });
        send_persist(rx, tx, peer, persist).await?;
    }

    anyhow::bail!(
        "apply loop did not converge after {MAX_APPLY_ITERATIONS} iterations; \
         connector continues to return state patches"
    );
}

fn labels_build_for(spec: &flow::MaterializationSpec) -> String {
    let Some(template) = spec.shard_template.as_ref() else {
        return String::new();
    };
    let Some(set) = template.labels.as_ref() else {
        return String::new();
    };

    labels::expect_one(set, labels::BUILD)
        .unwrap_or_default()
        .to_string()
}

async fn recv_opened(
    request_rxs: &mut [BoxStream<'static, tonic::Result<proto::Materialize>>],
    peers: &[String],
) -> anyhow::Result<proto::materialize::Opened> {
    let mut openeds = futures::future::try_join_all(request_rxs.iter_mut().enumerate().map(
        |(shard_index, rx)| async move {
            let verify = crate::verify("Materialize", "Opened", &peers[shard_index]);
            match verify.not_eof(rx.next().await)? {
                proto::Materialize {
                    opened: Some(opened),
                    ..
                } if shard_index == 0 || opened == proto::materialize::Opened::default() => {
                    Ok::<_, anyhow::Error>(opened)
                }
                other => Err(verify.fail_msg(other)),
            }
        },
    ))
    .await?;

    Ok(openeds.swap_remove(0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_stream::wrappers::UnboundedReceiverStream;

    // Build a peer/leader pair of channels, returning the BoxStream
    // the apply_loop would read from and the receiver of leader-side
    // sends, plus a peer-side sender used to inject responses.
    fn channel_pair() -> (
        BoxStream<'static, tonic::Result<proto::Materialize>>,
        mpsc::UnboundedSender<tonic::Result<proto::Materialize>>,
        mpsc::UnboundedSender<tonic::Result<proto::Materialize>>,
        mpsc::UnboundedReceiver<tonic::Result<proto::Materialize>>,
    ) {
        let (peer_tx, peer_rx) = mpsc::unbounded_channel();
        let (leader_tx, leader_rx) = mpsc::unbounded_channel();
        let stream = UnboundedReceiverStream::new(peer_rx).boxed();
        (stream, peer_tx, leader_tx, leader_rx)
    }

    fn applied(patches: &'static [u8]) -> proto::Materialize {
        proto::Materialize {
            applied: Some(proto::Applied {
                action_description: String::new(),
                connector_patches_json: Bytes::from_static(patches),
            }),
            ..Default::default()
        }
    }

    fn persisted(seq_no: u64) -> proto::Materialize {
        proto::Materialize {
            persisted: Some(proto::Persisted { seq_no }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn apply_loop_no_op_when_last_eq_next_and_no_patches() {
        // When last_applied == next_applied and the connector returns no
        // patches, we issue a single Apply and return without Persist.
        let (mut rx, peer_tx, leader_tx, mut leader_rx) = channel_pair();
        peer_tx.send(Ok(applied(b""))).unwrap();

        let same = Bytes::new();
        let mut state = Bytes::from_static(b"{\"k\":1}");
        apply_loop(
            &mut rx,
            &leader_tx,
            "p",
            &same,
            &same,
            "v1",
            &mut state,
            &crate::TracingLogger,
        )
        .await
        .unwrap();

        let m = leader_rx.try_recv().unwrap().unwrap();
        let apply = m.apply.expect("Apply was sent");
        assert!(apply.last_spec.is_empty());
        assert!(apply.spec.is_empty());
        assert_eq!(apply.version, "v1");
        // No Persist since spec is unchanged.
        assert!(leader_rx.try_recv().is_err());
        // State is unchanged.
        assert_eq!(state.as_ref(), b"{\"k\":1}");
    }

    #[tokio::test]
    async fn apply_loop_persists_last_applied_when_no_patches_but_spec_changed() {
        // No patches but next != last: loop sends Apply, then Persist
        // marking next_applied as the new last_applied with matching seq_no.
        let (mut rx, peer_tx, leader_tx, mut leader_rx) = channel_pair();
        peer_tx.send(Ok(applied(b""))).unwrap();
        peer_tx.send(Ok(persisted(1))).unwrap();

        let last = Bytes::new();
        let next = Bytes::from_static(b"new-spec-bytes");
        let mut state = Bytes::from_static(b"{}");
        apply_loop(
            &mut rx,
            &leader_tx,
            "p",
            &last,
            &next,
            "v2",
            &mut state,
            &crate::TracingLogger,
        )
        .await
        .unwrap();

        let m1 = leader_rx.try_recv().unwrap().unwrap();
        let apply = m1.apply.unwrap();
        assert_eq!(apply.spec, next);
        assert!(apply.last_spec.is_empty());
        assert_eq!(apply.last_version, "");

        let m2 = leader_rx.try_recv().unwrap().unwrap();
        let p = m2.persist.unwrap();
        assert_eq!(p.seq_no, 1);
        assert_eq!(p.last_applied, next);
        assert!(p.connector_patches_json.is_empty());

        assert!(leader_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn apply_loop_accumulates_patches_across_iterations() {
        // Initial state has nested objects, a key the patches will overwrite,
        // and a key the patches will delete (RFC 7396 null sentinel). Across
        // two patching iterations we should observe: deep merge of `nested`,
        // replacement of `keep`, deletion of `drop`, and addition of `added`.
        let (mut rx, peer_tx, leader_tx, mut leader_rx) = channel_pair();

        let patch1 = b"[{\"nested\":{\"a\":1},\"keep\":\"v1\"}\t]";
        let patch2 = b"[{\"nested\":{\"b\":2},\"keep\":\"v2\",\"drop\":null,\"added\":true}\t]";
        peer_tx.send(Ok(applied(patch1))).unwrap();
        peer_tx.send(Ok(persisted(1))).unwrap();
        peer_tx.send(Ok(applied(patch2))).unwrap();
        peer_tx.send(Ok(persisted(2))).unwrap();
        peer_tx.send(Ok(applied(b""))).unwrap();
        peer_tx.send(Ok(persisted(3))).unwrap();

        let last = Bytes::new();
        let next = Bytes::from_static(b"spec");
        let mut state = Bytes::from_static(br#"{"nested":{"a":0},"keep":"v0","drop":"x"}"#);
        apply_loop(
            &mut rx,
            &leader_tx,
            "p",
            &last,
            &next,
            "v2",
            &mut state,
            &crate::TracingLogger,
        )
        .await
        .unwrap();

        // Apply (iter 1) — connector observes the original state.
        let apply1 = leader_rx.try_recv().unwrap().unwrap().apply.unwrap();
        let s1: serde_json::Value = serde_json::from_slice(&apply1.connector_state_json).unwrap();
        assert_eq!(
            s1,
            serde_json::json!({"nested":{"a":0},"keep":"v0","drop":"x"})
        );
        // Persist iter 1 carries the connector's patches but no last_applied.
        let p1 = leader_rx.try_recv().unwrap().unwrap().persist.unwrap();
        assert_eq!(p1.seq_no, 1);
        assert!(p1.last_applied.is_empty());
        assert_eq!(p1.connector_patches_json.as_ref(), patch1);

        // Apply (iter 2) — `keep` replaced, `nested.a` retained from iter 1.
        let apply2 = leader_rx.try_recv().unwrap().unwrap().apply.unwrap();
        let s2: serde_json::Value = serde_json::from_slice(&apply2.connector_state_json).unwrap();
        assert_eq!(
            s2,
            serde_json::json!({"nested":{"a":1},"keep":"v1","drop":"x"}),
        );
        let p2 = leader_rx.try_recv().unwrap().unwrap().persist.unwrap();
        assert_eq!(p2.seq_no, 2);
        assert!(p2.last_applied.is_empty());
        assert_eq!(p2.connector_patches_json.as_ref(), patch2);

        // Apply (iter 3) — `drop` removed via null, `nested` deep-merged, `added` added.
        let apply3 = leader_rx.try_recv().unwrap().unwrap().apply.unwrap();
        let s3: serde_json::Value = serde_json::from_slice(&apply3.connector_state_json).unwrap();
        assert_eq!(
            s3,
            serde_json::json!({"nested":{"a":1,"b":2},"keep":"v2","added":true}),
        );
        // Final Persist promotes spec and carries no patches.
        let p3 = leader_rx.try_recv().unwrap().unwrap().persist.unwrap();
        assert_eq!(p3.seq_no, 3);
        assert_eq!(p3.last_applied, next);
        assert!(p3.connector_patches_json.is_empty());

        // Final reduced state escapes via &mut, observable to caller.
        let final_state: serde_json::Value = serde_json::from_slice(&state).unwrap();
        assert_eq!(
            final_state,
            serde_json::json!({"nested":{"a":1,"b":2},"keep":"v2","added":true}),
        );
    }

    #[tokio::test]
    async fn apply_loop_error_paths() {
        struct Case {
            name: &'static str,
            // Build the peer-side responses; closure receives the peer tx.
            seed: fn(&mpsc::UnboundedSender<tonic::Result<proto::Materialize>>),
            expect: &'static str,
        }
        let cases = [
            Case {
                // Connector returns patches forever; we cap at MAX_APPLY_ITERATIONS.
                name: "no_convergence",
                seed: |tx| {
                    for seq_no in 1..=4 {
                        tx.send(Ok(applied(b"[{\"x\":1}\t]"))).unwrap();
                        tx.send(Ok(persisted(seq_no))).unwrap();
                    }
                },
                expect: "did not converge",
            },
            Case {
                // Peer returns Persisted with a wrong seq_no — protocol error.
                name: "persisted_seq_no_mismatch",
                seed: |tx| {
                    tx.send(Ok(applied(b"[{\"x\":1}\t]"))).unwrap();
                    tx.send(Ok(persisted(99))).unwrap();
                },
                expect: "expected Persisted",
            },
            Case {
                // Peer sends a non-Applied message in response to Apply.
                name: "unexpected_message_kind",
                seed: |tx| {
                    tx.send(Ok(proto::Materialize {
                        opened: Some(proto::materialize::Opened::default()),
                        ..Default::default()
                    }))
                    .unwrap();
                },
                expect: "expected Applied",
            },
            Case {
                // Peer closes the stream without sending Applied — surfaces as EOF.
                name: "eof",
                seed: |_tx| {},
                expect: "unexpected EOF",
            },
        ];

        for case in cases {
            let (mut rx, peer_tx, leader_tx, _leader_rx) = channel_pair();
            (case.seed)(&peer_tx);
            drop(peer_tx);

            let last = Bytes::new();
            let next = Bytes::from_static(b"spec");
            let mut state = Bytes::from_static(b"{}");
            let err = apply_loop(
                &mut rx,
                &leader_tx,
                "p",
                &last,
                &next,
                "v2",
                &mut state,
                &crate::TracingLogger,
            )
            .await
            .unwrap_err();
            let s = format!("{err:?}");
            assert!(
                s.contains(case.expect),
                "{}: missing {:?} in {s}",
                case.name,
                case.expect,
            );
        }
    }

    fn make_streams(
        per_shard: Vec<Vec<tonic::Result<proto::Materialize>>>,
    ) -> Vec<BoxStream<'static, tonic::Result<proto::Materialize>>> {
        per_shard
            .into_iter()
            .map(|msgs| {
                let (tx, rx) = mpsc::unbounded_channel();
                for m in msgs {
                    tx.send(m).unwrap();
                }
                drop(tx);
                UnboundedReceiverStream::new(rx).boxed()
            })
            .collect()
    }

    fn recover_msg(recover: proto::Recover) -> tonic::Result<proto::Materialize> {
        Ok(proto::Materialize {
            recover: Some(recover),
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn recv_recovers_returns_shard_zero_value() {
        let zero = proto::Recover {
            committed_close_clock: 42,
            ..Default::default()
        };
        let mut streams = make_streams(vec![
            vec![recover_msg(zero.clone())],
            vec![recover_msg(proto::Recover::default())],
            vec![recover_msg(proto::Recover::default())],
        ]);
        let peers = vec!["s0".into(), "s1".into(), "s2".into()];
        let got = recv_recovers(&mut streams, &peers).await.unwrap();
        assert_eq!(got, zero);
    }

    #[tokio::test]
    async fn recv_recovers_error_paths() {
        let cases = [
            (
                "non_default_from_non_zero_shard",
                vec![
                    vec![recover_msg(proto::Recover::default())],
                    vec![recover_msg(proto::Recover {
                        committed_close_clock: 1,
                        ..Default::default()
                    })],
                ],
                vec!["expected Recover", "from s1"],
            ),
            (
                "wrong_message_kind",
                vec![vec![Ok(proto::Materialize {
                    opened: Some(proto::materialize::Opened::default()),
                    ..Default::default()
                })]],
                vec!["expected Recover"],
            ),
            ("eof", vec![vec![]], vec!["unexpected EOF"]),
        ];

        for (name, per_shard, needles) in cases {
            let mut streams = make_streams(per_shard);
            let peers: Vec<String> = (0..streams.len()).map(|i| format!("s{i}")).collect();
            let err = recv_recovers(&mut streams, &peers).await.unwrap_err();
            let s = format!("{err:?}");
            for n in needles {
                assert!(s.contains(n), "{name}: missing {n:?} in {s}");
            }
        }
    }

    #[tokio::test]
    async fn recv_opened_returns_shard_zero_value_and_rejects_others() {
        let zero = proto::materialize::Opened {
            container: None,
            connector_checkpoint: Some(proto_gazette::consumer::Checkpoint::default()),
        };
        let mut streams = make_streams(vec![
            vec![Ok(proto::Materialize {
                opened: Some(zero.clone()),
                ..Default::default()
            })],
            vec![Ok(proto::Materialize {
                opened: Some(proto::materialize::Opened::default()),
                ..Default::default()
            })],
        ]);
        let peers = vec!["s0".into(), "s1".into()];
        let got = recv_opened(&mut streams, &peers).await.unwrap();
        assert_eq!(got, zero);

        // Now a non-zero shard sends a populated Opened — error.
        let mut streams = make_streams(vec![
            vec![Ok(proto::Materialize {
                opened: Some(proto::materialize::Opened::default()),
                ..Default::default()
            })],
            vec![Ok(proto::Materialize {
                opened: Some(proto::materialize::Opened {
                    container: None,
                    connector_checkpoint: Some(proto_gazette::consumer::Checkpoint::default()),
                }),
                ..Default::default()
            })],
        ]);
        let err = recv_opened(&mut streams, &peers).await.unwrap_err();
        let s = format!("{err:?}");
        assert!(s.contains("expected Opened"));
        assert!(s.contains("from s1"));
    }

    // ---- reconcile_recovered ----

    fn clk(secs: u64) -> uuid::Clock {
        uuid::Clock::from_unix(secs, 0)
    }

    fn prod(tag: u8) -> uuid::Producer {
        uuid::Producer::from_bytes([0x01, tag, 0, 0, 0, 0])
    }

    fn pf(
        tag: u8,
        last_commit: uuid::Clock,
        hinted_commit: uuid::Clock,
        offset: i64,
    ) -> shuffle::ProducerFrontier {
        shuffle::ProducerFrontier {
            producer: prod(tag),
            last_commit,
            hinted_commit,
            offset,
        }
    }

    // A single-journal, single-binding crate `shuffle::Frontier`.
    fn frontier(
        journal: &str,
        binding: u16,
        producers: Vec<shuffle::ProducerFrontier>,
    ) -> shuffle::Frontier {
        shuffle::Frontier::new(
            vec![shuffle::JournalFrontier {
                journal: journal.into(),
                binding,
                producers,
                bytes_read_delta: 0,
                bytes_behind_delta: 0,
            }],
            vec![],
        )
        .unwrap()
    }

    // An authoritative checkpoint (no committed-close key) mapping
    // "{journal};{suffix}" to a single producer at committed end offset
    // `read_through` (begin=-1) and `last_ack`.
    fn authoritative_checkpoint(
        journal: &str,
        suffix: &str,
        tag: u8,
        last_ack: uuid::Clock,
        read_through: i64,
    ) -> consumer::Checkpoint {
        let mut sources = BTreeMap::new();
        sources.insert(
            format!("{journal};{suffix}"),
            consumer::checkpoint::Source {
                read_through,
                producers: vec![consumer::checkpoint::source::ProducerEntry {
                    id: Bytes::copy_from_slice(prod(tag).as_bytes()),
                    state: Some(consumer::checkpoint::ProducerState {
                        last_ack: last_ack.as_u64(),
                        begin: -1,
                    }),
                }],
            },
        );
        consumer::Checkpoint {
            sources,
            ack_intents: [("ack/j".to_string(), Bytes::from_static(b"ACK"))].into(),
        }
    }

    // A checkpoint carrying only a committed-close Clock (no data sources), used
    // for the in-sync and hinted-close-delta paths that never map it to a Frontier.
    fn close_only_checkpoint(close: uuid::Clock, ack_key: &str) -> consumer::Checkpoint {
        let (k, v) = frontier_mapping::encode_committed_close(close);
        consumer::Checkpoint {
            sources: [(k, v)].into(),
            ack_intents: [(ack_key.to_string(), Bytes::from_static(b"C"))].into(),
        }
    }

    fn producer_tags(f: &shuffle::Frontier) -> Vec<u8> {
        f.journals
            .iter()
            .flat_map(|jf| jf.producers.iter().map(|p| p.producer.as_bytes()[1]))
            .collect()
    }

    /// The fix: rebuilding the committed frontier from an authoritative legacy V1
    /// checkpoint discards the stale hinted frontier, so an orphaned hint (its
    /// producer absent from the rebuilt committed frontier — e.g. V1-pruned after
    /// >24h idle) does NOT resume at offset 0.
    #[test]
    fn reconcile_rebuild_from_legacy_discards_orphan_hints() {
        let index = [("s-a", 0usize)];
        // Recovered RocksDB committed frontier — replaced by the rebuild.
        let committed = frontier(
            "j/one",
            0,
            vec![pf(0xaa, clk(50), uuid::Clock::zero(), -100)],
        );
        // Stale hinted frontier references old producer 0xaa (hint in last_commit).
        // Its stored offset is irrelevant — project_hinted zeroes it; the offset-0
        // re-read comes solely from 0xaa's absence in the rebuilt committed frontier.
        let hinted = frontier(
            "j/one",
            0,
            vec![pf(0xaa, clk(80), uuid::Clock::zero(), -99_999)],
        );
        // Legacy V1 checkpoint (no committed-close key => authoritative), carrying
        // the current producer 0xbb committed to end offset 5000.
        let legacy = authoritative_checkpoint("j/one", "s-a", 0xbb, clk(1000), 5000);

        let r = reconcile_recovered(
            true, // drop_v1_rollback
            clk(50),
            clk(80),
            committed,
            hinted,
            Some(legacy),
            consumer::Checkpoint::default(),
            BTreeMap::new(),
            &index,
        )
        .unwrap();

        // Hinted frontier discarded: no unresolved hints, no idempotent replay.
        assert!(!r.idempotent_replay);
        assert_eq!(r.resume_frontier.unresolved_hints, 0);
        // Resume == committed rebuilt from the checkpoint: only producer 0xbb, at
        // the checkpoint's committed end offset (NOT the orphan at offset 0).
        assert_eq!(producer_tags(&r.resume_frontier), vec![0xbb]);
        assert_eq!(r.resume_frontier.journals[0].producers[0].offset, -5000);
        // committed_close is unchanged by the legacy path.
        assert_eq!(r.committed_close, clk(50));
        // Cleanup clears BOTH frontiers on disk and drops the legacy checkpoint.
        let p = r.cleanup_persist.expect("cleanup persist");
        assert!(p.delete_committed_frontier);
        assert!(p.delete_hinted_frontier);
        assert!(p.committed_frontier.is_some());
        assert!(p.delete_legacy_checkpoint);
        // ACK intents adopted from the authoritative checkpoint.
        assert!(r.pending_ack_intents.contains_key("ack/j"));
    }

    /// Contrast: with no rebuild, a hinted producer that HAS a committed baseline
    /// is preserved and replayed from its committed offset — never offset 0.
    #[test]
    fn reconcile_no_rebuild_preserves_hints() {
        let index = [("s-a", 0usize)];
        let committed = frontier(
            "j/one",
            0,
            vec![pf(0xbb, clk(50), uuid::Clock::zero(), -500)],
        );
        let hinted = frontier(
            "j/one",
            0,
            vec![pf(0xbb, clk(200), uuid::Clock::zero(), -500)],
        );

        let r = reconcile_recovered(
            false,
            clk(50),
            clk(60),
            committed,
            hinted,
            None,
            consumer::Checkpoint::default(),
            BTreeMap::new(),
            &index,
        )
        .unwrap();

        // Hint preserved -> unresolved -> replayed, but from the committed offset.
        assert!(r.idempotent_replay);
        assert_eq!(r.resume_frontier.unresolved_hints, 1);
        let p0 = &r.resume_frontier.journals[0].producers[0];
        assert_eq!(p0.offset, -500);
        assert!(p0.hinted_commit > p0.last_commit);
        // Nothing to reconcile on disk.
        assert!(r.cleanup_persist.is_none());
    }

    /// A remote-authoritative connector confirming the hinted txn committed folds
    /// the hinted frontier into committed and advances committed_close — no rebuild,
    /// no cleanup.
    #[test]
    fn reconcile_connector_hinted_close_folds_delta() {
        let index = [("s-a", 0usize)];
        let committed = frontier(
            "j/one",
            0,
            vec![pf(0xbb, clk(50), uuid::Clock::zero(), -500)],
        );
        let hinted = frontier(
            "j/one",
            0,
            vec![pf(0xbb, clk(200), uuid::Clock::zero(), -800)],
        );
        let connector = close_only_checkpoint(clk(200), "ack/c");

        let r = reconcile_recovered(
            false,
            clk(50),
            clk(200), // hinted_close matches the connector's close clock
            committed,
            hinted,
            None,
            connector,
            BTreeMap::new(),
            &index,
        )
        .unwrap();

        assert_eq!(r.committed_close, clk(200));
        assert!(r.cleanup_persist.is_none());
        // Hinted folded into committed: 0xbb at the further offset, last_commit=200.
        let p0 = &r.committed_frontier.journals[0].producers[0];
        assert_eq!(p0.offset, -800);
        assert_eq!(p0.last_commit, clk(200));
        assert_eq!(r.resume_frontier.unresolved_hints, 0);
        assert!(!r.idempotent_replay);
        assert!(r.pending_ack_intents.contains_key("ack/c"));
    }

    /// Dropping V1 rollback capability when the legacy checkpoint is in sync (not
    /// authoritative) deletes only the legacy checkpoint — the hinted and committed
    /// frontiers are untouched, so hints are still honored.
    #[test]
    fn reconcile_drop_rollback_deletes_only_legacy_checkpoint() {
        let index = [("s-a", 0usize)];
        let committed = frontier(
            "j/one",
            0,
            vec![pf(0xbb, clk(50), uuid::Clock::zero(), -500)],
        );
        let hinted = frontier(
            "j/one",
            0,
            vec![pf(0xbb, clk(200), uuid::Clock::zero(), -500)],
        );
        // In-sync legacy checkpoint: committed-close key matches committed_close.
        let legacy = close_only_checkpoint(clk(50), "ack/legacy");

        let r = reconcile_recovered(
            true, // drop_v1_rollback
            clk(50),
            clk(60),
            committed,
            hinted,
            Some(legacy),
            consumer::Checkpoint::default(),
            BTreeMap::new(),
            &index,
        )
        .unwrap();

        // Not rebuilt: the hint survives and is replayed from its committed offset.
        assert!(r.idempotent_replay);
        assert_eq!(r.resume_frontier.journals[0].producers[0].offset, -500);
        // Cleanup drops only the legacy checkpoint; frontiers untouched on disk.
        let p = r
            .cleanup_persist
            .expect("cleanup persist for drop-rollback");
        assert!(p.delete_legacy_checkpoint);
        assert!(!p.delete_committed_frontier);
        assert!(!p.delete_hinted_frontier);
        assert!(p.committed_frontier.is_none());
    }

    /// The same fix on the connector-authoritative rebuild branch (no legacy
    /// checkpoint): a remote-authoritative connector checkpoint rebuilds the
    /// committed frontier and discards the stale hinted frontier, so the orphan
    /// hint does not resume at offset 0. Guards the connector branch's discard
    /// independently of the legacy branch's.
    #[test]
    fn reconcile_rebuild_from_connector_discards_orphan_hints() {
        let index = [("s-a", 0usize)];
        let committed = frontier(
            "j/one",
            0,
            vec![pf(0xaa, clk(50), uuid::Clock::zero(), -100)],
        );
        let hinted = frontier(
            "j/one",
            0,
            vec![pf(0xaa, clk(80), uuid::Clock::zero(), -99_999)],
        );
        // Remote-authoritative connector checkpoint (no committed-close key =>
        // authoritative), carrying the current producer 0xbb at end offset 5000.
        let connector = authoritative_checkpoint("j/one", "s-a", 0xbb, clk(1000), 5000);

        let r = reconcile_recovered(
            false, // no V1 rollback in play
            clk(50),
            clk(80),
            committed,
            hinted,
            None, // no legacy checkpoint
            connector,
            BTreeMap::new(),
            &index,
        )
        .unwrap();

        // Hinted frontier discarded: no orphan, no idempotent replay.
        assert!(!r.idempotent_replay);
        assert_eq!(r.resume_frontier.unresolved_hints, 0);
        assert_eq!(producer_tags(&r.resume_frontier), vec![0xbb]);
        assert_eq!(r.resume_frontier.journals[0].producers[0].offset, -5000);
        // Cleanup clears both frontiers; no legacy checkpoint to drop.
        let p = r.cleanup_persist.expect("cleanup persist");
        assert!(p.delete_committed_frontier);
        assert!(p.delete_hinted_frontier);
        assert!(p.committed_frontier.is_some());
        assert!(!p.delete_legacy_checkpoint);
        assert!(r.pending_ack_intents.contains_key("ack/j"));
    }
}
