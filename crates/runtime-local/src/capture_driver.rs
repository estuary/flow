//! Capture driver for `flowctl preview-next`.
//!
//! Captures are leaderless: each shard runs its own connector container,
//! RocksDB, publish loop, and transaction loop with no cross-shard coordination.
//! The preview driver mirrors that — N synthetic shards each driving one long-lived
//! SessionLoop via `runtime_next::shard::Service::spawn_capture`, fanned out as
//! independent `tokio::spawn` tasks.
//!
//! Real-world note: many connectors expect N=1 today, and a connector that
//! ignores `Open.range.key_begin/key_end` will duplicate work across shards.
//! Same caveat as the materialize driver — N>1 exercises the runtime, but
//! interpreting the output as a workload signal requires a range-partitioning
//! connector.

use crate::Controls;
use crate::services::Run;
use prost::Message;
use proto_flow::{flow, runtime as cruntime};
use runtime_next::proto;
use runtime_next::{LoggerFactory, PublisherFactory};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;

/// Returns shard zero's final reduced connector state when
/// [`Controls::report_final_state`] asked for it, else `None`.
pub async fn run_sessions<P: PublisherFactory, L: LoggerFactory>(
    run: &Run,
    spec: &flow::CaptureSpec,
    session_targets: Vec<u32>,
    controls: Controls<P, L>,
    stop_token: CancellationToken,
) -> anyhow::Result<Option<bytes::Bytes>> {
    let join_shards = crate::shards::build_capture_join_shards(run.n_shards, spec)?;
    // Encode the spec once; each shard's Task carries a cheap refcount clone of
    // these bytes rather than deep-cloning and re-encoding the spec per shard.
    let spec_bytes: bytes::Bytes = spec.encode_to_vec().into();

    let mut handles = Vec::with_capacity(run.n_shards as usize);
    for i in 0..run.n_shards {
        let run_handle = RunHandle {
            network: run.network.clone(),
            registry: run.registry.clone(),
        };
        let task_name = spec.name.clone();
        let spec_bytes = spec_bytes.clone();
        let join_shard = join_shards[i as usize].clone();
        let session_targets = session_targets.clone();
        let controls = controls.clone();
        let stop_token = stop_token.clone();

        handles.push(tokio::spawn(async move {
            drive_one_shard(
                run_handle,
                task_name,
                spec_bytes,
                i,
                join_shard,
                session_targets,
                controls,
                stop_token,
            )
            .await
        }));
    }

    crate::join_shard_drivers(handles, "capture").await
}

/// `Run` fields a single capture shard driver needs. Cheaper to clone than
/// `&Run` so we can move it into a spawned task without lifetime gymnastics.
struct RunHandle {
    network: String,
    registry: service_kit::Registry,
}

async fn drive_one_shard<P: PublisherFactory, L: LoggerFactory>(
    run: RunHandle,
    task_name: String,
    spec_bytes: bytes::Bytes,
    shard_index: u32,
    join_shard: proto::join::Shard,
    session_targets: Vec<u32>,
    controls: Controls<P, L>,
    stop_token: CancellationToken,
) -> anyhow::Result<Option<bytes::Bytes>> {
    let shard_svc = runtime_next::shard::Service::new(
        cruntime::Plane::Local,
        run.network,
        None,
        task_name,
        controls.publisher_factory.clone(),
        controls.logger_factory.clone(),
        run.registry,
        None, // No AuthN+AuthZ signer (local loopback).
    );

    let (request_tx, request_rx) = mpsc::unbounded_channel::<tonic::Result<proto::Capture>>();
    let mut response_rx = shard_svc.spawn_capture(UnboundedReceiverStream::new(request_rx));

    let session_loop = controls.session_loop(shard_index);
    let report_final_state = session_loop.report_final_state;
    let mut final_state = report_final_state.then(|| controls.initial_state_json.clone());

    // Every fallible step from here on lives inside this one block, so that a
    // failure anywhere in it — including partway through the session loop —
    // falls through to the single teardown below.
    let result = async {
        request_tx
            .send(Ok(proto::Capture {
                session_loop: Some(session_loop),
                ..Default::default()
            }))
            .map_err(|_| anyhow::anyhow!("serve task closed before SessionLoop"))?;

        for (idx, target_txns) in session_targets.into_iter().enumerate() {
            if stop_token.is_cancelled() {
                break;
            }
            let session_index = idx + 1;

            request_tx
                .send(Ok(proto::Capture {
                    join: Some(proto::Join {
                        etcd_mod_revision: session_index as i64,
                        shards: vec![join_shard.clone()],
                        shard_index: 0,
                        // Captures have no shuffle or leader; the handler ignores these.
                        shuffle_directory: String::new(),
                        shuffle_endpoint: String::new(),
                        leader_endpoint: String::new(),
                    }),
                    ..Default::default()
                }))
                .map_err(|_| anyhow::anyhow!("serve task closed before Join"))?;

            tracing::info!(
                shard_index,
                session = session_index,
                target_txns,
                "starting preview capture session",
            );

            request_tx
                .send(Ok(proto::Capture {
                    task: Some(proto::Task {
                        spec: spec_bytes.clone(),
                        max_transactions: target_txns,
                        sqlite_vfs_uri: String::new(),
                        publisher_id: Default::default(),
                    }),
                    ..Default::default()
                }))
                .map_err(|_| anyhow::anyhow!("serve task closed before Task"))?;

            let stopped_state = drive_session_responses(
                &request_tx,
                &mut response_rx,
                shard_index,
                session_index,
                &stop_token,
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
    request_tx: &mpsc::UnboundedSender<tonic::Result<proto::Capture>>,
    response_rx: &mut mpsc::UnboundedReceiver<tonic::Result<proto::Capture>>,
    shard_index: u32,
    session_index: usize,
    stop_token: &CancellationToken,
) -> anyhow::Result<bytes::Bytes> {
    let verify = runtime_next::verify("Capture", "Joined, Opened, or Stopped", "shard");

    let mut requested_stop = false;
    loop {
        tokio::select! {
            biased;

            _ = stop_token.cancelled(), if !requested_stop => {
                requested_stop = true;
                _ = request_tx
                    .send(Ok(proto::Capture {
                        stop: Some(proto::Stop {}),
                        ..Default::default()
                    }));
            }
            msg = response_rx.recv() => {
                let msg = verify.not_eof(msg)?;

                if let Some(proto::Joined { max_etcd_revision }) = msg.joined {
                    if max_etcd_revision != 0 {
                        anyhow::bail!(
                            "capture preview expected Joined.max_etcd_revision = 0, got {max_etcd_revision}",
                        );
                    }
                    tracing::debug!(shard_index, session_index, "capture session joined");
                } else if let Some(proto::capture::Opened { container }) = &msg.opened {
                    tracing::debug!(shard_index, session_index, ?container, "capture session opened");
                } else if let Some(stopped) = msg.stopped {
                    tracing::debug!(shard_index, session_index, "capture session stopped");
                    return Ok(stopped.connector_state_json);
                } else {
                    return Err(verify.fail_msg(msg));
                }
            }
        }
    }
}
