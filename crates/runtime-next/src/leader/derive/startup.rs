use super::super::frontier_mapping;
use super::Task;
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
pub(super) struct Startup<Pub: crate::Publisher, Shuffle: crate::leader::ShuffleSession> {
    // Clock at which the last-committed transaction closed.
    pub committed_close: uuid::Clock,
    // Fully committed Frontier.
    pub committed_frontier: shuffle::Frontier,
    // Recovered ACK intents of the last transaction.
    pub pending_ack_intents: BTreeMap<String, Bytes>,
    // Publisher for writing stats and ACK intents.
    pub publisher: Pub,
    // Initiated shuffle session for the task and topology.
    pub session: Shuffle,
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
    Shuffle: crate::ShuffleSessionFactory,
    Pub: crate::PublisherFactory,
    Obs: crate::ObserverFactory,
>(
    build: String,
    drop_v1_rollback: bool,
    ops_stats_journal: String,
    reactors: Vec<String>,
    shard_rx: &mut Vec<BoxStream<'static, tonic::Result<proto::Derive>>>,
    shard_tx: &Vec<mpsc::UnboundedSender<tonic::Result<proto::Derive>>>,
    service: &crate::Service<Shuffle, Pub, Obs>,
    shard_ids: Vec<String>,
    shard_shuffles: Vec<shuffle::proto::Shard>,
) -> anyhow::Result<Startup<Pub::Publisher, Shuffle::Session>> {
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
        let _ = tx.send(Ok(proto::Derive {
            joined: Some(proto::Joined {
                max_etcd_revision: 0, // Success.
            }),
            ..Default::default()
        }));
    }

    // Receive L:Task from shard zero.
    let verify = crate::verify("Derive", "Open", &peers[0]);
    let task = match verify.not_eof(shard_rx[0].next().await)? {
        proto::Derive {
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

    let spec = flow::CollectionSpec::decode(spec_bytes.as_ref())
        .context("invalid Task collection (derivation)")?;
    let mut task = Task::new(build, &spec, max_transactions, peers)
        .await
        .context("building task definition")?;

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
        ack_intents: mut pending_ack_intents,
        committed_close_clock: committed_close,
        committed_frontier,
        connector_state_json,
        hinted_close_clock: hinted_close,
        hinted_frontier,
        last_applied: _,
        legacy_checkpoint,
        max_keys,
        trigger_params_json: _,
    } = recv_recovers(shard_rx, &task.peers)
        .await
        .context("receiving Recover fan-in")?;

    // Derivations never track max-keys; a non-empty set is stale per-shard state.
    anyhow::ensure!(
        max_keys.is_empty(),
        "derive Recover.max_keys must be empty, but recovered {} entries",
        max_keys.len(),
    );

    let committed_close = uuid::Clock::from_u64(committed_close);

    // Derivations never write a hinted Frontier (`FH:`) or hinted-close clock.
    anyhow::ensure!(
        hinted_frontier.is_none() && hinted_close == 0,
        "derive Recover carried hinted state (hinted_close_clock={hinted_close}, \
         hinted_frontier {}), but derivations never write one",
        if hinted_frontier.is_some() {
            "present"
        } else {
            "absent"
        },
    );

    let mut committed_frontier = shuffle::Frontier::decode(committed_frontier.unwrap_or_default())
        .context("validating committed Frontier")?;

    tracing::debug!(
        ?committed_close,
        ?committed_frontier,
        connector_state_bytes = connector_state_json.len(),
        ?legacy_checkpoint,
        "collected Recover from all shards",
    );

    // Derive has no Apply/Applied phase: open connectors across all shards directly.
    for (tx, shard) in shard_tx.iter().zip(shard_shuffles.iter()) {
        let _ = tx.send(Ok(proto::Derive {
            open: Some(proto::Open {
                spec: spec_bytes.clone(),
                version: task.shard_ref.build.clone(),
                range: shard.range.clone(),
                connector_state_json: connector_state_json.clone(),
                max_keys: BTreeMap::new(),
            }),
            ..Default::default()
        }));
    }

    // Receive Opened fan-in.
    let proto::derive::Opened {
        container: _, // Not sent to leader.
        connector_checkpoint,
    } = recv_opened(shard_rx, &task.peers)
        .await
        .context("receiving Opened fan-in")?;

    // A connector that returns a checkpoint at Opened is remote-authoritative
    // (only derive-sqlite, today).
    task.remote_authoritative = connector_checkpoint.is_some();

    // Remote-authoritative derivations (derive-sqlite) commit their checkpoint
    // to the connector at StartCommit and cannot drop V1 rollback support: the
    // leader must keep maintaining the legacy checkpoint to feed StartCommit.
    // Fail loudly at startup rather than panicking mid-transaction.
    anyhow::ensure!(
        !(task.remote_authoritative && drop_v1_rollback),
        "remote-authoritative derivation (e.g. derive-sqlite) cannot drop V1 rollback support; \
         the {} flag must not be set on this task",
        super::super::DROP_V1_ROLLBACK_FLAG,
    );

    // Build sorted index on journal_read_suffix => binding index, for frontier mapping.
    let mut journal_read_suffix_index: Vec<(&str, usize)> = task
        .binding_journal_read_suffixes
        .iter()
        .enumerate()
        .map(|(i, b)| (b.as_str(), i))
        .collect();
    journal_read_suffix_index.sort();

    // Set when a recovered checkpoint (legacy V1 or connector) is authoritative
    // and its mapped Frontier replaces `committed_frontier`.
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
                &journal_read_suffix_index,
            )
            .context("mapping recovered legacy checkpoint into Frontier")?;
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

    // Handle a `connector_checkpoint` from a remote-authoritative connector (only
    // derive-sqlite, today). The connector is the sole authority for its checkpoint.
    if let Some(mut connector_checkpoint) = connector_checkpoint {
        // The connector round-trips our synthetic committed-close source key
        // (stamped by `encode_committed_close` and stored verbatim by, e.g.,
        // derive-sqlite). It isn't a real journal source, so drop it before
        // mapping — `checkpoint_to_frontier` requires a ';' suffix separator.
        connector_checkpoint
            .sources
            .remove(std::str::from_utf8(crate::shard::recovery::KEY_COMMITTED_CLOSE).unwrap());

        committed_frontier = frontier_mapping::checkpoint_to_frontier(
            &connector_checkpoint.sources,
            &journal_read_suffix_index,
        )
        .context("mapping recovered connector checkpoint into Frontier")?;
        committed_frontier_rebuilt = true;

        pending_ack_intents = connector_checkpoint.ack_intents;
    } else {
        service_kit::event!(
            tracing::Level::DEBUG,
            "leader",
            "no connector_checkpoint present",
        );
    }

    let delete_legacy_checkpoint = drop_v1_rollback && legacy_checkpoint_present;
    if committed_frontier_rebuilt || delete_legacy_checkpoint {
        service_kit::event!(
            tracing::Level::INFO,
            "leader",
            committed_frontier_rebuilt,
            delete_legacy_checkpoint,
            "reconciling recovered checkpoint state",
        );
        send_persist(
            &mut shard_rx[0],
            &shard_tx[0],
            &task.peers[0],
            proto::Persist {
                seq_no: 0,
                delete_committed_frontier: committed_frontier_rebuilt,
                committed_frontier: committed_frontier_rebuilt
                    .then(|| shuffle::JournalFrontier::encode(&committed_frontier.journals)),
                delete_legacy_checkpoint,
                ..Default::default()
            },
        )
        .await
        .context("sending startup cleanup Persist")?;
    }

    // No hints are possible (asserted above), so the resume Frontier is exactly
    // the committed Frontier.
    let resume_frontier = committed_frontier.clone();

    let shuffle_task = shuffle::proto::Task {
        task: Some(shuffle::proto::task::Task::Derivation(spec)),
    };
    let session = service
        .shuffle_factory
        .open(shuffle_task, shard_shuffles, resume_frontier)
        .await
        .context("opening shuffle session")?;

    Ok(Startup {
        committed_close,
        committed_frontier,
        pending_ack_intents,
        publisher,
        session,
        task,
    })
}

async fn recv_recovers(
    request_rxs: &mut [BoxStream<'static, tonic::Result<proto::Derive>>],
    peers: &[String],
) -> anyhow::Result<proto::Recover> {
    let mut recovers = futures::future::try_join_all(request_rxs.into_iter().enumerate().map(
        |(shard_index, rx)| async move {
            let verify = crate::verify("Derive", "Recover", &peers[shard_index]);
            match verify.not_eof(rx.next().await)? {
                proto::Derive {
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
    rx: &mut BoxStream<'static, tonic::Result<proto::Derive>>,
    tx: &mpsc::UnboundedSender<tonic::Result<proto::Derive>>,
    peer: &str,
    persist: proto::Persist,
) -> anyhow::Result<()> {
    let verify = crate::verify("Derive", "Persisted", peer);
    let seq_no = persist.seq_no;

    let _ = tx.send(Ok(proto::Derive {
        persist: Some(persist),
        ..Default::default()
    }));

    match verify.not_eof(rx.next().await)? {
        proto::Derive {
            persisted: Some(proto::Persisted { seq_no: got }),
            ..
        } if got == seq_no => Ok(()),
        other => Err(verify.fail_msg(other)),
    }
}

async fn recv_opened(
    request_rxs: &mut [BoxStream<'static, tonic::Result<proto::Derive>>],
    peers: &[String],
) -> anyhow::Result<proto::derive::Opened> {
    let mut openeds = futures::future::try_join_all(request_rxs.iter_mut().enumerate().map(
        |(shard_index, rx)| async move {
            let verify = crate::verify("Derive", "Opened", &peers[shard_index]);
            match verify.not_eof(rx.next().await)? {
                proto::Derive {
                    opened: Some(opened),
                    ..
                } if shard_index == 0 || opened == proto::derive::Opened::default() => {
                    Ok::<_, anyhow::Error>(opened)
                }
                proto::Derive {
                    opened: Some(_), ..
                } => Err(anyhow::anyhow!(
                    "non-zero shard {} reported connector checkpoint state at Opened; \
                     remote-authoritative derivations (e.g. derive-sqlite) must be single-shard",
                    peers[shard_index],
                )),
                other => Err(verify.fail_msg(other)),
            }
        },
    ))
    .await?;

    Ok(openeds.swap_remove(0))
}
