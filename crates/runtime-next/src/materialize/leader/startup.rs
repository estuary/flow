use super::state;
use crate::proto;
use anyhow::Context;
use bytes::Bytes;
use futures::{StreamExt, stream::BoxStream};
use prost::Message;
use proto_flow::flow;
use proto_gazette::uuid;
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::sync::mpsc;

/// Outcomes of the leader protocol startup phase.
pub(super) struct Startup {
    // Is the first transaction an idempotent replay of a recovered hinted Frontier?
    pub idempotent_replay: bool,
    // The last commit timestamp.
    pub last_commit: uuid::Clock,
    // Peer addresses, in shard index order, suitable for error messages.
    pub peers: Vec<String>,
    // Recovered ACK intents of the last transaction.
    pub pending_ack_intents: BTreeMap<String, Bytes>,
    // Recovered variables for the task.
    pub pending_trigger_params: Bytes,
    // Publisher for writing stats and ACK intents.
    pub publisher: crate::Publisher,
    // Initiated shuffle session for the task and topology.
    pub session: shuffle::SessionClient,
    // Task definition -- to be unified with current shard::Task.
    pub task: state::Task,
}

#[tracing::instrument(
    level = "debug",
    err(Debug, level = "warn"),
    skip_all,
    fields(shard_zero = %shard_ids[0], shards = shard_ids.len())
)]
pub(super) async fn run(
    reactors: Vec<String>,
    shard_rx: &mut Vec<BoxStream<'static, tonic::Result<proto::Materialize>>>,
    shard_tx: &Vec<mpsc::UnboundedSender<tonic::Result<proto::Materialize>>>,
    service: &crate::Service,
    shard_ids: Vec<String>,
    shard_labelings: Vec<ops::ShardLabeling>,
    shard_shuffles: Vec<shuffle::proto::Shard>,
) -> anyhow::Result<Startup> {
    let n_shards = reactors.len();
    assert_eq!(n_shards, shard_rx.len());
    assert_eq!(n_shards, shard_tx.len());
    assert_eq!(n_shards, shard_ids.len());
    assert_eq!(n_shards, shard_labelings.len());
    assert_eq!(n_shards, shard_shuffles.len());

    let peers: Vec<String> = shard_ids
        .iter()
        .zip(reactors.iter())
        .map(|(shard_id, reactor)| format!("{shard_id}@{reactor}"))
        .collect();

    // Send Joined response to all shards.
    for tx in shard_tx {
        let _ = tx.send(Ok(proto::Materialize {
            joined: Some(proto::Joined {
                max_etcd_revision: 0, // Success.
            }),
            ..Default::default()
        }));
    }

    // Receive Recover fan-in.
    let (recovers, hinted_frontiers, committed_frontiers) = recv_recovers(shard_rx, &peers)
        .await
        .context("receiving Recover fan-in")?;

    let proto::Recover {
        ack_intents: pending_ack_intents,
        mut connector_patches_json,
        last_applied,
        last_commit,
        max_keys,
        trigger_params_json: pending_trigger_params,
        hinted_frontier: _,
        committed_frontier: _,
    } = reduce_recovers(&peers, recovers).context("reducing Recover fan-in")?;
    let last_commit = uuid::Clock::from_u64(last_commit);

    // Reduce per-shard hinted/committed Frontiers across all shards.
    let hinted_frontier = hinted_frontiers
        .into_iter()
        .fold(shuffle::Frontier::default(), shuffle::Frontier::reduce);
    let committed_frontier = committed_frontiers
        .into_iter()
        .fold(shuffle::Frontier::default(), shuffle::Frontier::reduce);

    tracing::debug!(
        connector_patches_bytes = connector_patches_json.len(),
        max_keys = max_keys.len(),
        last_applied_bytes = last_applied.len(),
        trigger_params_bytes = pending_trigger_params.len(),
        ?last_commit,
        "collected Recover from all shards",
    );

    // Receive Open from shard zero. ops_logs / ops_stats specs and journals
    // are optional: when both specs are absent the leader runs in
    // `Publisher::Preview` (no journal IO; stats logged via tracing). When at
    // least one spec is present, both must be — they're a unit.
    let verify = crate::verify("Materialize", "Open", &peers[0], 0);
    let (spec, ops_logs, ops_stats) = match verify.not_eof(shard_rx[0].next().await)? {
        proto::Materialize {
            open:
                Some(proto::materialize::Open {
                    materialization: Some(task),
                    ops_logs_spec,
                    ops_stats_spec,
                    ops_logs_journal,
                    ops_stats_journal,
                }),
            ..
        } => {
            let ops = match (ops_logs_spec, ops_stats_spec) {
                (None, None) => None,
                (Some(ls), Some(ss)) => Some(((ls, ops_logs_journal), (ss, ops_stats_journal))),
                _ => {
                    anyhow::bail!(
                        "Open ops_logs_spec / ops_stats_spec must both be set or both absent",
                    );
                }
            };
            let (logs, stats) = ops.map_or((None, None), |(l, s)| (Some(l), Some(s)));
            (task, logs, stats)
        }
        other => return Err(verify.fail_msg(other)),
    };

    let publisher = match (ops_logs.as_ref(), ops_stats.as_ref()) {
        (Some((logs_spec, logs_journal)), Some((stats_spec, stats_journal))) => {
            crate::Publisher::new_real(
                shard_ids[0].clone(), // Shard zero is AuthZ subject.
                &service.publisher_factory,
                logs_journal,
                logs_spec,
                stats_journal,
                stats_spec,
                [], // No additional bindings.
            )
            .context("creating publisher")?
        }
        _ => crate::Publisher::new_preview(),
    };

    let next_applied = Bytes::from(spec.encode_to_vec());

    // Apply loop on shard zero.
    apply_loop(
        &mut shard_rx[0],
        &shard_tx[0],
        &peers[0],
        &last_applied,
        &next_applied,
        &mut connector_patches_json,
    )
    .await?;

    // Broadcast Recovered to all shards.
    () = broadcast_recovered(&shard_tx, &connector_patches_json, &max_keys).await;

    // Receive Opened fan-in.
    let openeds = recv_opened(shard_rx, &peers)
        .await
        .context("receiving Opened fan-in")?;

    let proto::materialize::Opened {
        skip_replay_determinism,
        legacy_checkpoint,
        container: _, // Not sent to leader.
        connector_image,
    } = reduce_opened(&peers, openeds).context("reducing Opened fan-in")?;

    // For remote-authoritative connectors, C:Opened may return a
    // `runtime_checkpoint` which shard zero forwards as L:Opened.legacy_checkpoint.
    // When present, it **supersedes** the recovered committed Frontier — the
    // remote endpoint is the source of truth for committed progress.
    //
    // `reduce_opened` enforces that only shard zero may report a
    // legacy_checkpoint. Since legacy_checkpoint is owned by shard zero,
    // and committed_frontier here is reduced across all shards, we replace
    // it wholesale (production materialize tasks are single-shard today;
    // multi-shard remote-authoritative tasks are not yet supported).
    let committed_frontier = if let Some(cp) = legacy_checkpoint {
        let binding_suffixes: Vec<&str> = spec
            .bindings
            .iter()
            .map(|b| b.journal_read_suffix.as_str())
            .collect();
        crate::recovery::frontier_mapping::checkpoint_to_frontier(&cp.sources, &binding_suffixes)
            .context("mapping legacy_checkpoint into Frontier")?
    } else {
        committed_frontier
    };

    // Compose the session resume Frontier: project the recovered hinted
    // Frontier into hinted form (last_commit -> hinted_commit, zero
    // last_commit/offset) and reduce with the (possibly superseded)
    // committed Frontier.
    let resume_frontier = shuffle::Frontier::reduce(
        committed_frontier,
        crate::recovery::frontier_mapping::project_hinted(hinted_frontier),
    );

    let task = build_task(
        &spec,
        n_shards,
        skip_replay_determinism,
        &shard_labelings[0].build,
        connector_image,
    )
    .await?;

    // If we recovered a producer frontier with an unapplied hinted commit,
    // then the first transaction must be an idempotent replay of the hinted frontier.
    let idempotent_replay = resume_frontier.journals.iter().any(|jf| {
        jf.producers
            .iter()
            .any(|pf| pf.hinted_commit > pf.last_commit)
    });

    // Open the shuffle Session, streaming the resume Frontier.
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
        idempotent_replay,
        last_commit,
        peers,
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
                let mut recovers: Vec<proto::Recover> = Vec::new();
                let mut hinted_journals: Vec<shuffle::JournalFrontier> = Vec::new();
                let mut committed_journals: Vec<shuffle::JournalFrontier> = Vec::new();

                loop {
                    let mut recover = match verify.not_eof(rx.next().await)? {
                        proto::Materialize {
                            recover: Some(recover),
                            ..
                        } => recover,
                        other => return Err(verify.fail_msg(other)),
                    };
                    if recover == proto::Recover::default() {
                        // Empty terminator: end-of-sequence.
                        let hinted =
                            shuffle::Frontier::new(hinted_journals, vec![]).with_context(|| {
                                format!(
                                    "validating hinted Frontier from {}@{shard_index}",
                                    peers[shard_index],
                                )
                            })?;
                        let committed = shuffle::Frontier::new(committed_journals, vec![])
                            .with_context(|| {
                                format!(
                                    "validating committed Frontier from {}@{shard_index}",
                                    peers[shard_index],
                                )
                            })?;
                        return Ok((recovers, hinted, committed));
                    }
                    if let Some(chunk) = recover.hinted_frontier.take() {
                        hinted_journals.extend(shuffle::JournalFrontier::decode(chunk));
                    }
                    if let Some(chunk) = recover.committed_frontier.take() {
                        committed_journals.extend(shuffle::JournalFrontier::decode(chunk));
                    }
                    if recover != proto::Recover::default() {
                        recovers.push(recover);
                    }
                }
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
            ack_intents: this_ack_intents,
            connector_patches_json: this_connector_patches,
            last_applied: this_last_applied,
            last_commit: this_last_commit,
            max_keys: this_max_keys,
            trigger_params_json: this_trigger_params,
            // Frontier chunks are drained out of Recovers in recv_recovers.
            hinted_frontier: _,
            committed_frontier: _,
        },
    ) in recovers
    {
        this_ack_intents.into_iter().for_each(|(k, v)| {
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
        if !this_connector_patches.is_empty() {
            anyhow::ensure!(
                reduced.connector_patches_json.is_empty()
                    || reduced.connector_patches_json == this_connector_patches,
                "conflicting connector_patches_json from {}@{shard_index}",
                peers[shard_index],
            );
            reduced.connector_patches_json = this_connector_patches;
        }
        if !this_last_applied.is_empty() {
            anyhow::ensure!(
                reduced.last_applied.is_empty() || reduced.last_applied == this_last_applied,
                "conflicting last_applied from {}@{shard_index}",
                peers[shard_index],
            );
            reduced.last_applied = this_last_applied;
        }
        if this_last_commit > reduced.last_commit {
            reduced.last_commit = this_last_commit;
        }
        this_max_keys.into_iter().for_each(|(k, v)| {
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
        if !this_trigger_params.is_empty() {
            anyhow::ensure!(
                reduced.trigger_params_json.is_empty()
                    || reduced.trigger_params_json == this_trigger_params,
                "conflicting trigger_params_json from {}@{shard_index}",
                peers[shard_index],
            );
            reduced.trigger_params_json = this_trigger_params;
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
    connector_patches_json: &mut Bytes,
) -> anyhow::Result<()> {
    let verify_applied = crate::verify("Materialize", "Applied", peer, 0);
    let verify_persisted = crate::verify("Materialize", "Persisted", peer, 0);

    for iteration in 1.. {
        // Send Apply carrying the running accumulated patches.
        // Sends are best-effort: a closed peer will surface on the next rx.
        let _ = tx.send(Ok(proto::Materialize {
            apply: Some(proto::materialize::Apply {
                last_applied: last_applied.clone(),
                connector_patches_json: connector_patches_json.clone(),
            }),
            ..Default::default()
        }));

        // Receive Applied.
        let applied_patches_json = match verify_applied.not_eof(rx.next().await)? {
            proto::Materialize {
                applied:
                    Some(proto::materialize::Applied {
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

        // Fold the iteration's patches into the running accumulator so
        // subsequent Apply iterations (and the later Recovered broadcast)
        // observe the newly-applied state.
        append_patches(connector_patches_json, &applied_patches_json);

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

fn append_patches(connector_patches_json: &mut Bytes, applied_patches_json: &Bytes) {
    if connector_patches_json.is_empty() {
        *connector_patches_json = applied_patches_json.clone();
        return;
    }

    let mut b = Vec::with_capacity(connector_patches_json.len() + applied_patches_json.len());
    b.extend_from_slice(connector_patches_json);
    b.truncate(b.len() - 1); // Remove trailing ']'.
    b.push(b',');
    b.extend_from_slice(&applied_patches_json[1..]); // Extend, removing leading '['.
    *connector_patches_json = Bytes::from(b);
}

async fn broadcast_recovered(
    response_txs: &[mpsc::UnboundedSender<tonic::Result<proto::Materialize>>],
    connector_patches_json: &Bytes,
    max_keys: &BTreeMap<u32, Bytes>,
) {
    for tx in response_txs {
        if !connector_patches_json.is_empty() {
            let _ = tx.send(Ok(proto::Materialize {
                recovered: Some(proto::Recovered {
                    connector_patches_json: connector_patches_json.clone(),
                    ..Default::default()
                }),
                ..Default::default()
            }));
        }
        if !max_keys.is_empty() {
            let _ = tx.send(Ok(proto::Materialize {
                recovered: Some(proto::Recovered {
                    max_keys: max_keys.clone(),
                    ..Default::default()
                }),
                ..Default::default()
            }));
        }
        let _ = tx.send(Ok(proto::Materialize {
            recovered: Some(proto::Recovered::default()),
            ..Default::default()
        }));
    }
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
            skip_replay_determinism: this_skip,
            legacy_checkpoint,
            container: _, // Not sent to leader.
            connector_image,
        },
    ) in opened
    {
        // skip_replay_determinism is a per-task invariant: all shards' connectors
        // are the same image and must report the same value.
        anyhow::ensure!(
            this_skip == reduced.skip_replay_determinism,
            "shard {}@{shard_index} reported skip_replay_determinism={this_skip} but shard zero reported {}",
            peers[shard_index],
            reduced.skip_replay_determinism,
        );
        anyhow::ensure!(
            legacy_checkpoint.is_none(),
            "shard {}@{shard_index} returned a non-empty legacy_checkpoint, but only shard zero should",
            peers[shard_index],
        );
        anyhow::ensure!(
            connector_image.is_empty(),
            "shard {}@{shard_index} reported connector_image={connector_image:?}, but only shard zero should",
            peers[shard_index],
        );
    }

    Ok(reduced)
}

async fn build_task(
    spec: &proto_flow::flow::MaterializationSpec,
    n_shards: usize,
    skip_replay_determinism: bool,
    build: &str,
    connector_image: String,
) -> anyhow::Result<super::state::Task> {
    let flow::MaterializationSpec {
        name,
        shard_template,
        bindings,
        triggers_json,
        ..
    } = spec;

    let compiled_triggers = super::triggers::decrypt_and_compile(triggers_json)
        .await
        .context("decrypting and compiling triggers")?
        .map(std::sync::Arc::new);

    let shard_template = shard_template.as_ref().context("missing shard template")?;

    let collection_names = bindings
        .iter()
        .map(|b| {
            b.collection
                .as_ref()
                .map(|c| c.name.clone())
                .unwrap_or_default()
        })
        .collect();

    let shard_ref = ops::ShardRef {
        kind: ops::TaskType::Materialization as i32,
        name: name.clone(),
        key_begin: labels::KEY_BEGIN_MIN.to_string(),
        r_clock_begin: labels::RCLOCK_BEGIN_MIN.to_string(),
        build: build.to_string(),
    };

    let min_txn_duration = shard_template
        .min_txn_duration
        .context("missing min_txn_duration")?
        .try_into()?;
    let max_txn_duration = shard_template
        .max_txn_duration
        .context("missing max_txn_duration")?
        .try_into()?;

    // Close-policy thresholds, many with placeholder defaults.
    // TODO: thread these through from the spec once they're supported there.
    let open_duration: std::ops::Range<std::time::Duration> = min_txn_duration..max_txn_duration;
    let last_commit_age = Duration::from_secs(0)..Duration::from_secs(300);
    let combiner_usage_bytes = (4 * 1024 * 1024)..(256 * 1024 * 1024);
    let read_docs = 1_000..1_000_000;
    let read_bytes = (1 << 20)..(1 << 30);

    Ok(super::state::Task {
        n_shards,
        skip_replay_determinism,
        collection_names,
        shard_ref,
        connector_image,
        compiled_triggers,
        open_duration,
        last_commit_age,
        combiner_usage_bytes,
        read_docs,
        read_bytes,
    })
}
