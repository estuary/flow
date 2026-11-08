//! Run-scoped driver: spawns N shard tasks via
//! `runtime_next::shard::Service::spawn_derive`, synthesizing the
//! SessionLoop/Join/Task envelopes the controller (Go in production) would
//! normally send. For SQLite derivations it threads a per-shard tempfile path
//! as the `Task.sqlite_vfs_uri` (production supplies a recorded recovery-log
//! VFS instead), hosted in a run-scoped tempdir this module owns: SQLite
//! tolerates an unlinked-but-open file, so nothing has to sequence its removal.

use crate::Controls;
use crate::services::Run;
use anyhow::Context;
use prost::Message;
use proto_flow::{flow, flow::collection_spec::derivation::ConnectorType};
use runtime_next::proto;
use runtime_next::{LoggerFactory, PublisherFactory};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;

/// Returns shard zero's final reduced connector state when
/// [`Controls::report_final_state`] asked for it, else `None`.
pub async fn run_sessions<P: PublisherFactory, L: LoggerFactory>(
    run: &Run,
    spec: &flow::CollectionSpec,
    session_targets: Vec<u32>,
    fixture_dirs: Vec<String>,
    controls: Controls<P, L>,
    stop_token: CancellationToken,
) -> anyhow::Result<Option<bytes::Bytes>> {
    let join_shards = crate::shards::build_derive_join_shards(run.n_shards, spec)?;

    // SQLite derivations require a VFS URI; preview supplies a plain tempfile
    // path (the connector opens it with SQLite's default file VFS).
    let is_sqlite = spec
        .derivation
        .as_ref()
        .map(|d| d.connector_type == ConnectorType::Sqlite as i32)
        .unwrap_or(false);

    // Home for the per-shard SQLite files, held for the life of this call so
    // each connector's checkpoint survives across the run's sessions. Only a
    // SQLite derivation has one.
    let sqlite_tmp = is_sqlite
        .then(tempfile::tempdir)
        .transpose()
        .context("creating derive-sqlite tempdir")?;
    let sqlite_dir = sqlite_tmp
        .as_ref()
        .map(|dir| dir.path().to_string_lossy().into_owned())
        .unwrap_or_default();

    // Encode the spec once; each shard's Task carries a cheap refcount clone of
    // these bytes rather than deep-cloning and re-encoding the spec per shard.
    let spec_bytes: bytes::Bytes = spec.encode_to_vec().into();

    let mut handles = Vec::with_capacity(run.n_shards as usize);
    for i in 0..run.n_shards {
        let run_handle = RunHandle {
            peer_endpoint: run.peer_endpoint.clone(),
            shuffle_log_dir: run.shuffle_log_dir.clone(),
            sqlite_dir: sqlite_dir.clone(),
            connector_router: run.connector_router.clone(),
            registry: run.registry.clone(),
        };
        let task_name = spec.name.clone();
        let spec_bytes = spec_bytes.clone();
        let join_shards = join_shards.clone();
        let session_targets = session_targets.clone();
        let fixture_dirs = fixture_dirs.clone();
        let controls = controls.clone();
        let stop_token = stop_token.clone();

        handles.push(tokio::spawn(async move {
            drive_one_shard(
                run_handle,
                task_name,
                spec_bytes,
                i,
                is_sqlite,
                join_shards,
                session_targets,
                fixture_dirs,
                controls,
                stop_token,
            )
            .await
        }));
    }

    crate::join_shard_drivers(handles, "derive").await
}

struct RunHandle {
    peer_endpoint: String,
    shuffle_log_dir: String,
    sqlite_dir: String, // Empty unless derive-sqlite.
    connector_router: std::sync::Arc<dyn proto_grpc::connector::Router>,
    registry: service_kit::Registry,
}

async fn drive_one_shard<P: PublisherFactory, L: LoggerFactory>(
    run: RunHandle,
    task_name: String,
    spec_bytes: bytes::Bytes,
    shard_index: u32,
    is_sqlite: bool,
    join_shards: Vec<proto::join::Shard>,
    session_targets: Vec<u32>,
    fixture_dirs: Vec<String>,
    controls: Controls<P, L>,
    stop_token: CancellationToken,
) -> anyhow::Result<Option<bytes::Bytes>> {
    let (request_tx, request_rx) = mpsc::unbounded_channel::<tonic::Result<proto::Derive>>();

    let shard_svc = runtime_next::shard::Service::new(
        run.connector_router,
        None,
        task_name,
        controls.publisher_factory.clone(),
        controls.logger_factory.clone(),
        run.registry,
        None, // No AuthN+AuthZ signer (local loopback).
    );

    let mut response_rx = shard_svc.spawn_derive(UnboundedReceiverStream::new(request_rx));

    let session_loop = controls.session_loop(shard_index);
    let report_final_state = session_loop.report_final_state;
    let mut final_state = report_final_state.then(|| controls.initial_state_json.clone());

    // Every fallible step from here on lives inside this one block, so that a
    // failure anywhere in it — including partway through the session loop —
    // falls through to the single teardown below.
    let result = async {
        // A tempfile in the run's SQLite tempdir, persistent across the run's
        // sessions so the connector's checkpoint recovers across them.
        let sqlite_vfs_uri = if is_sqlite {
            format!("{}/derive-sqlite-{shard_index:03}.db", run.sqlite_dir)
        } else {
            String::new()
        };

        request_tx
            .send(Ok(proto::Derive {
                session_loop: Some(session_loop),
                ..Default::default()
            }))
            .map_err(|_| anyhow::anyhow!("serve task closed before SessionLoop"))?;

        for (idx, target_txns) in session_targets.into_iter().enumerate() {
            if stop_token.is_cancelled() {
                break;
            }
            let session_index = idx + 1;

            // A fixture preview reads each session from its own directory (fresh
            // segments from segment one); live preview shares the run's directory.
            let shuffle_directory = fixture_dirs
                .get(idx)
                .cloned()
                .unwrap_or_else(|| run.shuffle_log_dir.clone());

            request_tx
                .send(Ok(proto::Derive {
                    join: Some(proto::Join {
                        etcd_mod_revision: session_index as i64,
                        shards: join_shards.clone(),
                        shard_index,
                        shuffle_directory,
                        shuffle_endpoint: run.peer_endpoint.clone(),
                        leader_endpoint: run.peer_endpoint.clone(),
                    }),
                    ..Default::default()
                }))
                .map_err(|_| anyhow::anyhow!("serve task closed before Join"))?;

            // All shards receive Task (each carries its own VFS URI); shard zero
            // forwards it to the leader.
            tracing::info!(
                session = session_index,
                shard_index,
                target_txns,
                "starting preview derive session",
            );
            request_tx
                .send(Ok(proto::Derive {
                    task: Some(proto::Task {
                        spec: spec_bytes.clone(),
                        max_transactions: target_txns,
                        sqlite_vfs_uri: sqlite_vfs_uri.clone(),
                        publisher_id: Default::default(), // The harness forwards no leader producer.
                    }),
                    ..Default::default()
                }))
                .map_err(|_| anyhow::anyhow!("serve task closed before Task"))?;

            let stopped_state =
                drive_session_responses(&request_tx, &mut response_rx, session_index, &stop_token)
                    .await?;

            if report_final_state {
                final_state = Some(stopped_state);
            }
        }
        anyhow::Ok(())
    }
    .await;

    crate::teardown_shard_stream(request_tx, response_rx, result, final_state).await
}

/// Drive one session to its `Stopped`, returning that message's connector state
/// (empty unless this stream's SessionLoop set `report_final_state`).
async fn drive_session_responses(
    request_tx: &mpsc::UnboundedSender<tonic::Result<proto::Derive>>,
    response_rx: &mut mpsc::UnboundedReceiver<tonic::Result<proto::Derive>>,
    session_index: usize,
    stop_token: &CancellationToken,
) -> anyhow::Result<bytes::Bytes> {
    let verify = runtime_next::verify("Derive", "Joined, Opened, or Stopped", "shard");

    let mut requested_stop = false;
    loop {
        tokio::select! {
            biased;

            _ = stop_token.cancelled(), if !requested_stop => {
                requested_stop = true;
                _ = request_tx
                    .send(Ok(proto::Derive {
                        stop: Some(proto::Stop {}),
                        ..Default::default()
                    }));
            }
            msg = response_rx.recv() => {
                let msg = verify.not_eof(msg)?;

                if let Some(proto::Joined { max_etcd_revision }) = msg.joined {
                    tracing::debug!(session_index, max_etcd_revision, "session joined");
                } else if let Some(proto::derive::Opened { container, .. }) = &msg.opened {
                    tracing::debug!(session_index, ?container, "session opened");
                } else if let Some(stopped) = msg.stopped {
                    tracing::debug!(session_index, "session stopped");
                    return Ok(stopped.connector_state_json);
                } else {
                    return Err(verify.fail_msg(msg));
                }
            }
        }
    }
}
