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
    let verify = crate::verify("Materialize", "Open", &peers[0]);
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
    let proto::Recover {
        ack_intents: mut pending_ack_intents,
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

    let mut committed_close = uuid::Clock::from_u64(committed_close);
    let hinted_close = uuid::Clock::from_u64(hinted_close);
    let legacy_checkpoint = legacy_checkpoint.unwrap_or_default();

    let mut hinted_frontier = shuffle::Frontier::decode(hinted_frontier.unwrap_or_default())
        .context("validating hinted Frontier")?;
    let mut committed_frontier = shuffle::Frontier::decode(committed_frontier.unwrap_or_default())
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

async fn apply_loop(
    rx: &mut BoxStream<'static, tonic::Result<proto::Materialize>>,
    tx: &mpsc::UnboundedSender<tonic::Result<proto::Materialize>>,
    peer: &str,
    last_applied: &Bytes,
    next_applied: &Bytes,
    next_version: &str,
    connector_state_json: &mut Bytes,
) -> anyhow::Result<()> {
    let verify_applied = crate::verify("Materialize", "Applied", peer);
    let verify_persisted = crate::verify("Materialize", "Persisted", peer);
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
        *connector_state_json =
            crate::patches::apply_state_patches(connector_state_json, &applied_patches_json)?;

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
