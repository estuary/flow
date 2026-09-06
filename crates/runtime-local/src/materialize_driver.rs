//! Run-scoped materialize driver: spawns N shard tasks via
//! `runtime_next::shard::Service::spawn_materialize`, synthesizing the
//! SessionLoop/Join/Task envelopes the controller (Go in production) would
//! normally send.

use crate::Controls;
use crate::services::Run;
use prost::Message;
use proto_flow::flow;
use runtime_next::proto;
use runtime_next::{LoggerFactory, PublisherFactory};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;

/// Run preview sessions against the prepared topology. Sessions are
/// numbered `1..` for log context, and all run over the same per-shard
/// SessionLoop streams.
///
/// Returns shard zero's final reduced connector state when
/// [`Controls::report_final_state`] asked for it, else `None`.
pub async fn run_sessions<P: PublisherFactory, L: LoggerFactory>(
    run: &Run,
    spec: &flow::MaterializationSpec,
    session_targets: Vec<u32>,
    fixture_dirs: Vec<String>,
    controls: Controls<P, L>,
    stop_token: CancellationToken,
) -> anyhow::Result<Option<bytes::Bytes>> {
    let join_shards = crate::shards::build_materialize_join_shards(run.n_shards, spec)?;
    // Encode the spec once; each shard's Task carries a cheap refcount clone of
    // these bytes rather than deep-cloning and re-encoding the spec per shard.
    let spec_bytes: bytes::Bytes = spec.encode_to_vec().into();

    let mut handles = Vec::with_capacity(run.n_shards as usize);
    for i in 0..run.n_shards {
        let run_handle = RunHandle {
            peer_endpoint: run.peer_endpoint.clone(),
            shuffle_log_dir: run.shuffle_log_dir.clone(),
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
                join_shards,
                session_targets,
                fixture_dirs,
                controls,
                stop_token,
            )
            .await
        }));
    }

    crate::join_shard_drivers(handles, "materialize").await
}

/// `Run` fields a single shard driver needs. Cheaper to clone than `&Run`
/// so we can hand it into a spawned task without lifetime gymnastics.
struct RunHandle {
    peer_endpoint: String,
    shuffle_log_dir: String,
    connector_router: std::sync::Arc<dyn proto_grpc::connector::Router>,
    registry: service_kit::Registry,
}

async fn drive_one_shard<P: PublisherFactory, L: LoggerFactory>(
    run: RunHandle,
    task_name: String,
    spec_bytes: bytes::Bytes,
    shard_index: u32,
    join_shards: Vec<proto::join::Shard>,
    session_targets: Vec<u32>,
    fixture_dirs: Vec<String>,
    controls: Controls<P, L>,
    stop_token: CancellationToken,
) -> anyhow::Result<Option<bytes::Bytes>> {
    let (request_tx, request_rx) = mpsc::unbounded_channel::<tonic::Result<proto::Materialize>>();

    let shard_svc = runtime_next::shard::Service::new(
        run.connector_router,
        None,
        task_name,
        controls.publisher_factory.clone(),
        controls.logger_factory.clone(),
        run.registry,
        None, // No AuthN+AuthZ signer (local loopback).
    );

    let mut response_rx = shard_svc.spawn_materialize(UnboundedReceiverStream::new(request_rx));

    let session_loop = controls.session_loop(shard_index);
    let report_final_state = session_loop.report_final_state;
    let mut final_state = report_final_state.then(|| controls.initial_state_json.clone());

    // Every fallible step from here on lives inside this one block, so that a
    // failure anywhere in it — including partway through the session loop —
    // falls through to the single teardown below.
    let result = async {
        request_tx
            .send(Ok(proto::Materialize {
                session_loop: Some(session_loop),
                ..Default::default()
            }))
            .map_err(|_| anyhow::anyhow!("serve task closed before SessionLoop"))?;

        // Every run ends with one additional empty "drain" session (represented
        // as `None`): the runtime halts a session after its final commit without
        // running its post-commit work, so the drain session's startup recovery
        // performs the last transaction's Acknowledge before the preview exits.
        // A run aborted by timeout or Ctrl-C skips it, like any other session.
        let sessions = session_targets
            .into_iter()
            .map(Some)
            .chain(std::iter::once(None));

        for (idx, target_txns) in sessions.enumerate() {
            if stop_token.is_cancelled() {
                break;
            }
            let session_index = idx + 1;
            let drain = target_txns.is_none();
            let target_txns = target_txns.unwrap_or(0);

            // A fixture preview reads each session from its own directory (fresh
            // segments from segment one); live preview shares the run's directory.
            let shuffle_directory = fixture_dirs
                .get(idx)
                .cloned()
                .unwrap_or_else(|| run.shuffle_log_dir.clone());

            request_tx
                .send(Ok(proto::Materialize {
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

            // All shards receive Task. Shard zero alone forwards to the leader.
            tracing::info!(
                session = session_index,
                shard_index,
                target_txns,
                drain,
                "starting preview session",
            );

            request_tx
                .send(Ok(proto::Materialize {
                    task: Some(proto::Task {
                        spec: spec_bytes.clone(),
                        max_transactions: target_txns,
                        sqlite_vfs_uri: String::new(),
                        publisher_id: Default::default(), // The harness forwards no leader producer.
                    }),
                    ..Default::default()
                }))
                .map_err(|_| anyhow::anyhow!("serve task closed before Task"))?;

            let stopped_state = drive_session_responses(
                &request_tx,
                &mut response_rx,
                session_index,
                &stop_token,
                drain,
            )
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
    request_tx: &mpsc::UnboundedSender<tonic::Result<proto::Materialize>>,
    response_rx: &mut mpsc::UnboundedReceiver<tonic::Result<proto::Materialize>>,
    session_index: usize,
    stop_token: &CancellationToken,
    drain: bool,
) -> anyhow::Result<bytes::Bytes> {
    let verify = runtime_next::verify("Materialize", "Joined, Opened, or Stopped", "shard");

    let mut requested_stop = false;
    loop {
        tokio::select! {
            biased;

            _ = stop_token.cancelled(), if !requested_stop => {
                requested_stop = true;
                _ = request_tx
                    .send(Ok(proto::Materialize {
                        stop: Some(proto::Stop {}),
                        ..Default::default()
                    }));
            }
            msg = response_rx.recv() => {
                let msg = verify.not_eof(msg)?;

                if let Some(proto::Joined { max_etcd_revision }) = msg.joined {
                    tracing::debug!(session_index, max_etcd_revision, "session joined");
                } else if let Some(proto::materialize::Opened { container, .. }) = &msg.opened {
                    tracing::debug!(session_index, ?container, "session opened");

                    // A drain session runs no transactions: request a graceful
                    // stop as soon as it opens. The leader completes its
                    // startup tail — which acknowledges the prior session's
                    // final committed transaction — before honoring the stop.
                    if drain && !requested_stop {
                        requested_stop = true;
                        _ = request_tx.send(Ok(proto::Materialize {
                            stop: Some(proto::Stop {}),
                            ..Default::default()
                        }));
                    }
                } else if msg.synced.is_some() {
                    // Sync-now progress, which preview has no caller to serve.
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
