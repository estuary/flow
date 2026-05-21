//! Run-scoped driver: spawns N shard tasks via
//! `runtime_next::shard::Service::spawn_materialize`, synthesizing the
//! SessionLoop/Join/Task envelopes the controller (Go in production) would
//! normally send.

use crate::raw::preview_next::services::Run;
use prost::Message;
use proto_flow::{flow, runtime as cruntime};
use runtime_next::proto;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;

/// Run preview sessions against the prepared topology. Sessions are
/// numbered `1..` for log context, and all run over the same per-shard
/// SessionLoop streams.
pub async fn run_sessions(
    run: &Run,
    spec: &flow::MaterializationSpec,
    session_targets: Vec<u32>,
    stop_token: CancellationToken,
) -> anyhow::Result<()> {
    let join_shards =
        crate::raw::preview_next::shards::build_materialize_join_shards(run.n_shards, spec)?;

    let mut handles = Vec::with_capacity(run.n_shards as usize);
    for i in 0..run.n_shards {
        let run_handle = RunHandle {
            peer_endpoint: run.peer_endpoint.clone(),
            shuffle_log_dir: run.shuffle_log_dir.clone(),
            rocksdb_path: run.rocksdb_path.clone(),
            network: run.network.clone(),
            log_handler: run.log_handler,
            registry: run.registry.clone(),
        };
        let spec = spec.clone();
        let join_shards = join_shards.clone();
        let session_targets = session_targets.clone();
        let stop_token = stop_token.clone();

        handles.push(tokio::spawn(async move {
            drive_one_shard(
                run_handle,
                spec,
                i,
                join_shards,
                session_targets,
                stop_token,
            )
            .await
        }));
    }

    // Await all shard drivers. The first error surfaces; remaining drivers
    // observe their request channel dropping (when their handle drops) and
    // tear down naturally.
    let mut first_err: Option<anyhow::Error> = None;
    for handle in handles {
        match handle.await {
            Ok(Ok(())) => (),
            Ok(Err(e)) if first_err.is_none() => first_err = Some(e),
            Ok(Err(e)) => tracing::warn!(error = ?e, "secondary shard driver error"),
            Err(panic) => {
                if first_err.is_none() {
                    first_err = Some(anyhow::anyhow!("driver panic: {panic}"));
                } else {
                    tracing::warn!(?panic, "secondary driver panic");
                }
            }
        }
    }
    if let Some(e) = first_err {
        return Err(e);
    }
    Ok(())
}

/// `Run` fields a single shard driver needs. Cheaper to clone than `&Run`
/// so we can hand it into a spawned task without lifetime gymnastics.
struct RunHandle {
    peer_endpoint: String,
    shuffle_log_dir: String,
    rocksdb_path: String,
    network: String,
    log_handler: fn(&::ops::Log),
    registry: service_kit::Registry,
}

async fn drive_one_shard(
    run: RunHandle,
    spec: flow::MaterializationSpec,
    shard_index: u32,
    join_shards: Vec<proto::join::Shard>,
    session_targets: Vec<u32>,
    stop_token: CancellationToken,
) -> anyhow::Result<()> {
    let (request_tx, request_rx) = mpsc::unbounded_channel::<tonic::Result<proto::Materialize>>();

    let task_name = format!("preview-shard-{shard_index:03}");

    let publisher_factory: gazette::journal::ClientFactory = std::sync::Arc::new({
        move |_authz_sub: String, _authz_obj: String| -> gazette::journal::Client {
            unreachable!("live Publisher is not used by preview ({_authz_sub}, {_authz_obj})")
        }
    });

    let shard_svc = runtime_next::shard::Service::new(
        cruntime::Plane::Local,
        run.network.clone(),
        run.log_handler,
        None,
        task_name,
        publisher_factory,
        run.registry,
    );

    let mut response_rx = shard_svc.spawn_materialize(UnboundedReceiverStream::new(request_rx));
    let spec_bytes: bytes::Bytes = spec.encode_to_vec().into();

    // Open the SessionLoop once. `runtime-next` opens RocksDB here and keeps
    // the handle live across the repeated Join/Task sessions below.
    let rocksdb_descriptor = if shard_index == 0 {
        Some(cruntime::RocksDbDescriptor {
            rocksdb_path: run.rocksdb_path.clone(),
            rocksdb_env_memptr: 0,
        })
    } else {
        None
    };
    request_tx
        .send(Ok(proto::Materialize {
            session_loop: Some(proto::SessionLoop { rocksdb_descriptor }),
            ..Default::default()
        }))
        .map_err(|_| anyhow::anyhow!("serve task closed before SessionLoop"))?;

    for (idx, target_txns) in session_targets.into_iter().enumerate() {
        if stop_token.is_cancelled() {
            break;
        }
        let session_index = idx + 1;

        request_tx
            .send(Ok(proto::Materialize {
                join: Some(proto::Join {
                    etcd_mod_revision: session_index as i64,
                    shards: join_shards.clone(),
                    shard_index,
                    shuffle_directory: run.shuffle_log_dir.clone(),
                    shuffle_endpoint: run.peer_endpoint.clone(),
                    leader_endpoint: run.peer_endpoint.clone(),
                }),
                ..Default::default()
            }))
            .map_err(|_| anyhow::anyhow!("serve task closed before Join"))?;

        if shard_index == 0 {
            tracing::info!(
                session = session_index,
                target_txns,
                "starting preview session",
            );

            request_tx
                .send(Ok(proto::Materialize {
                    task: Some(proto::Task {
                        spec: spec_bytes.clone(),
                        preview: true,
                        max_transactions: target_txns,
                    }),
                    ..Default::default()
                }))
                .map_err(|_| anyhow::anyhow!("serve task closed before Task"))?;
        }

        drive_session_responses(&request_tx, &mut response_rx, session_index, &stop_token).await?;
    }

    drop(request_tx);
    while let Some(msg) = response_rx.recv().await {
        let _msg = msg.map_err(runtime_next::status_to_anyhow)?;
    }

    Ok(())
}

async fn drive_session_responses(
    request_tx: &mpsc::UnboundedSender<tonic::Result<proto::Materialize>>,
    response_rx: &mut mpsc::UnboundedReceiver<tonic::Result<proto::Materialize>>,
    session_index: usize,
    stop_token: &CancellationToken,
) -> anyhow::Result<()> {
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
                } else if let Some(proto::Stopped {}) = msg.stopped {
                    tracing::debug!(session_index, "session stopped");
                    return Ok(());
                } else {
                    return Err(verify.fail_msg(msg));
                }
            }
        }
    }
}
