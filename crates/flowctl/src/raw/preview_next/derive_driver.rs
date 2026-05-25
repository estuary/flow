//! Run-scoped driver: spawns N shard tasks via
//! `runtime_next::shard::Service::spawn_derive`, synthesizing the
//! SessionLoop/Join/Task envelopes the controller (Go in production) would
//! normally send. For SQLite derivations it threads a per-shard tempfile path
//! as the `Task.sqlite_vfs_uri` (production supplies a recorded recovery-log
//! VFS instead).

use crate::raw::preview_next::services::Run;
use prost::Message;
use proto_flow::{flow, flow::collection_spec::derivation::ConnectorType, runtime as cruntime};
use runtime_next::proto;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;

pub async fn run_sessions(
    run: &Run,
    spec: &flow::CollectionSpec,
    session_targets: Vec<u32>,
    stop_token: CancellationToken,
) -> anyhow::Result<()> {
    let join_shards =
        crate::raw::preview_next::shards::build_derive_join_shards(run.n_shards, spec)?;

    // SQLite derivations require a VFS URI; preview supplies a plain tempfile
    // path (the connector opens it with SQLite's default file VFS).
    let is_sqlite = spec
        .derivation
        .as_ref()
        .map(|d| d.connector_type == ConnectorType::Sqlite as i32)
        .unwrap_or(false);

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
                is_sqlite,
                join_shards,
                session_targets,
                stop_token,
            )
            .await
        }));
    }

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
    spec: flow::CollectionSpec,
    shard_index: u32,
    is_sqlite: bool,
    join_shards: Vec<proto::join::Shard>,
    session_targets: Vec<u32>,
    stop_token: CancellationToken,
) -> anyhow::Result<()> {
    let (request_tx, request_rx) = mpsc::unbounded_channel::<tonic::Result<proto::Derive>>();

    let task_name = format!("preview-derive-{shard_index:03}");

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

    let mut response_rx = shard_svc.spawn_derive(UnboundedReceiverStream::new(request_rx));
    let spec_bytes: bytes::Bytes = spec.encode_to_vec().into();

    // A tempfile under the run RocksDB tempdir, persistent across the run's
    // sessions so the connector's checkpoint recovers across them.
    let sqlite_vfs_uri = if is_sqlite {
        format!("{}/derive-sqlite-{shard_index:03}.db", run.rocksdb_path)
    } else {
        String::new()
    };

    let rocksdb_descriptor = if shard_index == 0 {
        Some(cruntime::RocksDbDescriptor {
            rocksdb_path: run.rocksdb_path.clone(),
            rocksdb_env_memptr: 0,
        })
    } else {
        None
    };
    request_tx
        .send(Ok(proto::Derive {
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
            .send(Ok(proto::Derive {
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
                    preview: true,
                    max_transactions: target_txns,
                    sqlite_vfs_uri: sqlite_vfs_uri.clone(),
                    publisher_id: Default::default(), // Unused when `preview`.
                }),
                ..Default::default()
            }))
            .map_err(|_| anyhow::anyhow!("serve task closed before Task"))?;

        drive_session_responses(&request_tx, &mut response_rx, session_index, &stop_token).await?;
    }

    drop(request_tx);
    while let Some(msg) = response_rx.recv().await {
        let _msg = msg.map_err(runtime_next::status_to_anyhow)?;
    }

    Ok(())
}

async fn drive_session_responses(
    request_tx: &mpsc::UnboundedSender<tonic::Result<proto::Derive>>,
    response_rx: &mut mpsc::UnboundedReceiver<tonic::Result<proto::Derive>>,
    session_index: usize,
    stop_token: &CancellationToken,
) -> anyhow::Result<()> {
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
