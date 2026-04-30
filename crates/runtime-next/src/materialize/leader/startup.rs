use super::{Task, frontier_mapping};
use crate::proto;
use anyhow::Context;
use bytes::Bytes;
use futures::{StreamExt, stream::BoxStream};
use prost::Message;
use proto_flow::flow;
use proto_gazette::uuid;
use std::collections::BTreeMap;
use tokio::sync::mpsc;

/// Outcomes of the leader protocol startup phase.
pub(super) struct Startup {
    // Clock at which the last-committed transaction closed.
    pub committed_close: uuid::Clock,
    // Fully committed Frontier.
    pub committed_frontier: shuffle::Frontier,
    // Is the first transaction an idempotent replay of a recovered hinted Frontier?
    pub idempotent_replay: bool,
    // Recovered ACK intents of the last transaction.
    pub pending_ack_intents: BTreeMap<String, Bytes>,
    // Recovered variables for the task.
    pub pending_trigger_params: Bytes,
    // Publisher for writing stats and ACK intents.
    pub publisher: crate::Publisher,
    // Initiated shuffle session for the task and topology.
    pub session: shuffle::SessionClient,
    // Task definition.
    pub task: Task,
}

#[tracing::instrument(
    level = "debug",
    err(Debug, level = "warn"),
    skip_all,
    fields(shard_zero = %shard_ids[0], shards = shard_ids.len())
)]
pub(super) async fn run(
    build: String,
    reactors: Vec<String>,
    shard_rx: &mut Vec<BoxStream<'static, tonic::Result<proto::Materialize>>>,
    shard_tx: &Vec<mpsc::UnboundedSender<tonic::Result<proto::Materialize>>>,
    service: &crate::Service,
    shard_ids: Vec<String>,
    shard_shuffles: Vec<shuffle::proto::Shard>,
) -> anyhow::Result<Startup> {
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
    let verify = crate::verify("Materialize", "Open", &peers[0], 0);
    let task = match verify.not_eof(shard_rx[0].next().await)? {
        proto::Materialize {
            task: Some(task), ..
        } => task,
        other => return Err(verify.fail_msg(other)),
    };

    // Build task definition.
    let proto::Task {
        ops_stats_journal,
        ops_stats_spec,
        preview,
        spec: spec_bytes,
    } = task;

    let spec = flow::MaterializationSpec::decode(spec_bytes.as_ref())
        .context("invalid Task materialization")?;
    let task = Task::new(build, &spec, peers)
        .await
        .context("building task definition")?;

    // Initialize publisher.
    let publisher = if preview {
        crate::Publisher::new_preview()
    } else {
        let ops_stats_spec = ops_stats_spec.as_ref().context("missing ops stats spec")?;

        crate::Publisher::new_real(
            shard_ids[0].clone(), // Shard zero is AuthZ subject.
            &service.publisher_factory,
            &ops_stats_journal,
            ops_stats_spec,
            [], // No additional bindings.
        )
        .context("creating publisher")?
    };

    // Receive Recover fan-in.
    let (recovers, hinted_frontiers, committed_frontiers) = recv_recovers(shard_rx, &task.peers)
        .await
        .context("receiving Recover fan-in")?;

    let proto::Recover {
        ack_intents: mut pending_ack_intents,
        committed_close_clock: committed_close,
        committed_frontier: _,
        mut connector_state_json,
        hinted_close_clock: hinted_close,
        hinted_frontier: _,
        last_applied,
        legacy_checkpoint,
        max_keys,
        trigger_params_json: pending_trigger_params,
    } = reduce_recovers(&task.peers, recovers).context("reducing Recover fan-in")?;

    let mut committed_close = uuid::Clock::from_u64(committed_close);
    let hinted_close = uuid::Clock::from_u64(hinted_close);
    let legacy_checkpoint = legacy_checkpoint.unwrap_or_default();

    // Reduce per-shard hinted/committed Frontiers across all shards.
    let mut hinted_frontier = hinted_frontiers
        .into_iter()
        .fold(shuffle::Frontier::default(), shuffle::Frontier::reduce);
    let mut committed_frontier = committed_frontiers
        .into_iter()
        .fold(shuffle::Frontier::default(), shuffle::Frontier::reduce);

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
    )
    .await?;

    // Open connectors across all shards.
    () = broadcast_open(
        &shard_tx,
        &shard_shuffles,
        &spec_bytes,
        &task.shard_ref.build,
        &connector_state_json,
        &max_keys,
    )
    .await;

    // Receive Opened fan-in.
    let openeds = recv_opened(shard_rx, &task.peers)
        .await
        .context("receiving Opened fan-in")?;

    let proto::materialize::Opened {
        container: _, // Not sent to leader.
        connector_checkpoint,
    } = reduce_opened(&task.peers, openeds).context("reducing Opened fan-in")?;
    let connector_checkpoint = connector_checkpoint.unwrap_or_default();

    // Build sorted index on journal_read_suffix => binding index, for frontier mapping.
    let mut journal_read_suffix_index: Vec<(&str, usize)> = task
        .binding_journal_read_suffixes
        .iter()
        .enumerate()
        .map(|(i, b)| (b.as_str(), i))
        .collect();
    journal_read_suffix_index.sort();

    // Handle migration from `legacy_checkpoint`.
    if !legacy_checkpoint.sources.is_empty() {
        let clock = frontier_mapping::extract_committed_close(&legacy_checkpoint);

        if clock == Some(committed_close) {
            tracing::debug!(
                ?committed_close,
                "legacy_checkpoint present but matches Recover::committed_close (ignoring)"
            );
        } else if let Some(clock) = clock {
            // Implementation error: these update together and should always sync.
            anyhow::bail!(
                "legacy_checkpoint has clock {clock:?} that doesn't match Recover's committed_close ({committed_close:?})"
            );
        } else {
            tracing::debug!(
                ?committed_close,
                ?legacy_checkpoint,
                "legacy_checkpoint doesn't contain committed-close-clock; treating as authoritative"
            );
            committed_frontier = frontier_mapping::checkpoint_to_frontier(
                &legacy_checkpoint.sources,
                &journal_read_suffix_index,
            )
            .context("mapping recovered legacy checkpoint into Frontier")?;

            pending_ack_intents = legacy_checkpoint.ack_intents;
        }
    } else {
        tracing::debug!("no legacy_checkpoint present");
    }

    // Handle a `connector_checkpoint` from remote-authoritative connectors.
    // It may be *ahead* of `committed_frontier`, which is detect as its embedded
    // committed-close Clock matching our recovered `hinted_close`.
    if !connector_checkpoint.sources.is_empty() {
        let clock = frontier_mapping::extract_committed_close(&connector_checkpoint);

        if clock == Some(committed_close) {
            tracing::debug!(
                ?committed_close,
                "connector_checkpoint present but matches Recover::committed_close (ignoring)"
            );
        } else if clock == Some(hinted_close) {
            // Connector declares that the hinted txn did in fact commit.
            tracing::debug!(
                ?committed_close,
                ?hinted_close,
                ?hinted_frontier,
                "connector_checkpoint present and matches Recover::hinted_close; applying delta"
            );
            committed_close = hinted_close;
            committed_frontier = committed_frontier.reduce(std::mem::take(&mut hinted_frontier));

            pending_ack_intents = connector_checkpoint.ack_intents;
        } else if let Some(clock) = clock {
            // Implementation error: these update together and should always sync.
            anyhow::bail!(
                "connector_checkpoint has clock {clock:?} which doesn't match Recover's\
                 committed_close ({committed_close:?}) or hinted_close ({hinted_close:?})"
            );
        } else {
            tracing::debug!(
                ?committed_close,
                ?connector_checkpoint,
                "connector_checkpoint doesn't contain committed-close-clock; treating as authoritative"
            );

            committed_frontier = frontier_mapping::checkpoint_to_frontier(
                &connector_checkpoint.sources,
                &journal_read_suffix_index,
            )
            .context("mapping recovered connector checkpoint into Frontier")?;

            pending_ack_intents = connector_checkpoint.ack_intents;
        }
    } else {
        tracing::debug!("no connector_checkpoint present");
    }

    // Compose the session resume Frontier: project the recovered hinted
    // Frontier into hinted form (last_commit -> hinted_commit, zero
    // last_commit/offset) and reduce with the committed Frontier.
    let resume_frontier =
        frontier_mapping::project_hinted(hinted_frontier).reduce(committed_frontier.clone());

    // If we recovered a producer frontier with an unapplied hinted commit,
    // then the first transaction must be an idempotent replay of the hinted frontier.
    let idempotent_replay = resume_frontier.unresolved_hints != 0;

    // Open the shuffle Session with the recovered resume Frontier.
    let shuffle_task = shuffle::proto::Task {
        task: Some(shuffle::proto::task::Task::Materialization(spec)),
    };
    let session = shuffle::SessionClient::open(
        &service.shuffle_service,
        shuffle_task,
        shard_shuffles,
        resume_frontier,
    )
    .await
    .context("opening shuffle Session")?;

    Ok(Startup {
        committed_close,
        committed_frontier,
        idempotent_replay,
        pending_ack_intents,
        pending_trigger_params,
        publisher,
        session,
        task,
    })
}

async fn recv_recovers(
    request_rxs: &mut [BoxStream<'static, tonic::Result<proto::Materialize>>],
    peers: &[String],
) -> anyhow::Result<(
    Vec<(usize, proto::Recover)>,
    Vec<shuffle::Frontier>,
    Vec<shuffle::Frontier>,
)> {
    let per_shard: Vec<(Vec<proto::Recover>, shuffle::Frontier, shuffle::Frontier)> =
        futures::future::try_join_all(request_rxs.into_iter().enumerate().map(
            |(shard_index, rx)| async move {
                let verify =
                    crate::verify("Materialize", "Recover", &peers[shard_index], shard_index);
                let mut recover = match verify.not_eof(rx.next().await)? {
                    proto::Materialize {
                        recover: Some(recover),
                        ..
                    } => recover,
                    other => return Err(verify.fail_msg(other)),
                };
                if shard_index != 0 && recover != proto::Recover::default() {
                    anyhow::bail!(
                        "non-zero shard {} (index {shard_index}) sent non-empty Recover: {recover:?}",
                        peers[shard_index],
                    );
                }
                let hinted_journals = recover
                    .hinted_frontier
                    .take()
                    .into_iter()
                    .flat_map(shuffle::JournalFrontier::decode)
                    .collect();
                let committed_journals = recover
                    .committed_frontier
                    .take()
                    .into_iter()
                    .flat_map(shuffle::JournalFrontier::decode)
                    .collect();
                let hinted = shuffle::Frontier::new(hinted_journals, vec![]).with_context(|| {
                    format!(
                        "validating hinted Frontier from {}@{shard_index}",
                        peers[shard_index],
                    )
                })?;
                let committed =
                    shuffle::Frontier::new(committed_journals, vec![]).with_context(|| {
                        format!(
                            "validating committed Frontier from {}@{shard_index}",
                            peers[shard_index],
                        )
                    })?;
                let recovers = if recover == proto::Recover::default() {
                    Vec::new()
                } else {
                    vec![recover]
                };

                Ok((recovers, hinted, committed))
            },
        ))
        .await?;

    let mut flattened: Vec<(usize, proto::Recover)> = Vec::new();
    let mut hinted_per_shard: Vec<shuffle::Frontier> = Vec::with_capacity(per_shard.len());
    let mut committed_per_shard: Vec<shuffle::Frontier> = Vec::with_capacity(per_shard.len());

    for (index, (recovers, hinted, committed)) in per_shard.into_iter().enumerate() {
        flattened.extend(recovers.into_iter().map(|r| (index, r)));
        hinted_per_shard.push(hinted);
        committed_per_shard.push(committed);
    }

    Ok((flattened, hinted_per_shard, committed_per_shard))
}

fn reduce_recovers(
    peers: &[String],
    recovers: impl IntoIterator<Item = (usize, proto::Recover)>,
) -> anyhow::Result<proto::Recover> {
    let mut reduced = proto::Recover::default();

    for (
        shard_index,
        proto::Recover {
            ack_intents,
            committed_close_clock,
            committed_frontier: _, // Handled in recv_recovers
            connector_state_json,
            hinted_close_clock,
            hinted_frontier: _, // Handled in recv_recovers
            last_applied,
            legacy_checkpoint,
            max_keys,
            trigger_params_json,
        },
    ) in recovers
    {
        ack_intents.into_iter().for_each(|(k, v)| {
            reduced
                .ack_intents
                .entry(k)
                .and_modify(|cur| {
                    let mut b = cur.to_vec();
                    b.extend_from_slice(&v);
                    *cur = b.into();
                })
                .or_insert(v);
        });
        if committed_close_clock > reduced.committed_close_clock {
            reduced.committed_close_clock = committed_close_clock;
        }
        if !connector_state_json.is_empty() {
            anyhow::ensure!(
                reduced.connector_state_json.is_empty()
                    || reduced.connector_state_json == connector_state_json,
                "conflicting connector_state_json from {} (index {shard_index})",
                peers[shard_index],
            );
            reduced.connector_state_json = connector_state_json;
        }
        if hinted_close_clock > reduced.hinted_close_clock {
            reduced.hinted_close_clock = hinted_close_clock;
        }
        if !last_applied.is_empty() {
            anyhow::ensure!(
                reduced.last_applied.is_empty() || reduced.last_applied == last_applied,
                "conflicting last_applied from {} (index {shard_index})",
                peers[shard_index],
            );
            reduced.last_applied = last_applied;
        }
        if legacy_checkpoint.is_some() {
            reduced.legacy_checkpoint = legacy_checkpoint;
        }
        max_keys.into_iter().for_each(|(k, v)| {
            reduced
                .max_keys
                .entry(k)
                .and_modify(|cur| {
                    if v > *cur {
                        *cur = v.clone();
                    }
                })
                .or_insert(v);
        });
        if !trigger_params_json.is_empty() {
            anyhow::ensure!(
                reduced.trigger_params_json.is_empty()
                    || reduced.trigger_params_json == trigger_params_json,
                "conflicting trigger_params_json from {} (index {shard_index})",
                peers[shard_index],
            );
            reduced.trigger_params_json = trigger_params_json;
        }
    }

    Ok(reduced)
}

async fn apply_loop(
    rx: &mut BoxStream<'static, tonic::Result<proto::Materialize>>,
    tx: &mpsc::UnboundedSender<tonic::Result<proto::Materialize>>,
    peer: &str,
    last_applied: &Bytes,
    next_applied: &Bytes,
    next_version: &str,
    connector_state_json: &mut Bytes,
) -> anyhow::Result<()> {
    let verify_applied = crate::verify("Materialize", "Applied", peer, 0);
    let verify_persisted = crate::verify("Materialize", "Persisted", peer, 0);
    let last_version = if last_applied.is_empty() {
        String::new()
    } else {
        let last_spec = flow::MaterializationSpec::decode(last_applied.as_ref())
            .context("invalid recovered last-applied spec")?;
        labels_build_for(&last_spec)
    };

    for iteration in 1.. {
        // Send Apply carrying the current reduced connector state.
        // Sends are best-effort: a closed peer will surface on the next rx.
        let _ = tx.send(Ok(proto::Materialize {
            apply: Some(proto::Apply {
                spec: next_applied.clone(),
                version: next_version.to_string(),
                last_spec: last_applied.clone(),
                last_version: last_version.clone(),
                state_json: connector_state_json.clone(),
            }),
            ..Default::default()
        }));

        // Receive Applied.
        let applied_patches_json = match verify_applied.not_eof(rx.next().await)? {
            proto::Materialize {
                applied:
                    Some(proto::Applied {
                        action_description: _,
                        connector_patches_json,
                    }),
                ..
            } => connector_patches_json,
            other => return Err(verify_applied.fail_msg(other)),
        };

        if applied_patches_json.is_empty() {
            tracing::debug!(iteration, "apply loop complete");

            if last_applied == next_applied {
                return Ok(());
            }

            let _ = tx.send(Ok(proto::Materialize {
                persist: Some(proto::Persist {
                    nonce: iteration,
                    last_applied: next_applied.clone(),
                    ..Default::default()
                }),
                ..Default::default()
            }));

            match verify_persisted.not_eof(rx.next().await)? {
                proto::Materialize {
                    persisted: Some(proto::Persisted { nonce }),
                    ..
                } if nonce == iteration => {}
                other => return Err(verify_persisted.fail_msg(other)),
            }

            return Ok(());
        }

        // Fold the iteration's patches into the running reduced state so
        // subsequent Apply iterations and Open observe the newly-applied state.
        *connector_state_json = apply_state_patches(connector_state_json, &applied_patches_json)?;

        // Persist the iteration's patches to shard zero.
        let _ = tx.send(Ok(proto::Materialize {
            persist: Some(proto::Persist {
                nonce: iteration, // End-of-sequence.
                connector_patches_json: applied_patches_json,
                ..Default::default()
            }),
            ..Default::default()
        }));

        // Receive Persisted.
        match verify_persisted.not_eof(rx.next().await)? {
            proto::Materialize {
                persisted: Some(proto::Persisted { nonce }),
                ..
            } if nonce == iteration => {}
            other => return Err(verify_persisted.fail_msg(other)),
        }
    }

    unreachable!();
}

async fn broadcast_open(
    response_txs: &[mpsc::UnboundedSender<tonic::Result<proto::Materialize>>],
    shard_shuffles: &[shuffle::proto::Shard],
    spec: &Bytes,
    version: &str,
    connector_state_json: &Bytes,
    max_keys: &BTreeMap<u32, Bytes>,
) {
    for (tx, shard) in response_txs.iter().zip(shard_shuffles) {
        let _ = tx.send(Ok(proto::Materialize {
            open: Some(proto::Open {
                spec: spec.clone(),
                version: version.to_string(),
                range: shard.range.clone(),
                connector_state_json: connector_state_json.clone(),
                max_keys: max_keys.clone(),
            }),
            ..Default::default()
        }));
    }
}

fn apply_state_patches(state_json: &Bytes, patches_json: &Bytes) -> anyhow::Result<Bytes> {
    let mut doc = if state_json.is_empty() {
        serde_json::Value::Object(Default::default())
    } else {
        serde_json::from_slice(state_json).context("parsing connector state JSON")?
    };

    for patch in crate::recovery::codec::split_state_patches(patches_json)? {
        let patch = serde_json::from_slice(&patch).context("parsing connector state patch")?;
        json_patch::merge(&mut doc, &patch);
    }

    Ok(Bytes::from(serde_json::to_vec(&doc)?))
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
) -> anyhow::Result<Vec<(usize, proto::materialize::Opened)>> {
    futures::future::try_join_all(request_rxs.iter_mut().enumerate().map(
        |(shard_index, rx)| async move {
            let verify = crate::verify("Materialize", "Opened", &peers[shard_index], shard_index);
            match verify.not_eof(rx.next().await)? {
                proto::Materialize {
                    opened: Some(opened),
                    ..
                } => Ok::<_, anyhow::Error>((shard_index, opened)),
                other => Err(verify.fail_msg(other)),
            }
        },
    ))
    .await
}

fn reduce_opened(
    peers: &[String],
    opened: impl IntoIterator<Item = (usize, proto::materialize::Opened)>,
) -> anyhow::Result<proto::materialize::Opened> {
    let mut opened = opened.into_iter();
    let (_, reduced) = opened.next().unwrap();

    for (
        shard_index,
        proto::materialize::Opened {
            container: _, // Not sent to leader.
            connector_checkpoint,
        },
    ) in opened
    {
        anyhow::ensure!(
            connector_checkpoint.is_none(),
            "shard {} (index {shard_index}) returned a non-empty connector_checkpoint, but only shard zero should",
            peers[shard_index],
        );
    }

    Ok(reduced)
}
