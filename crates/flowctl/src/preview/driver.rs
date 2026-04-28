//! Per-session driver: spawns N shard tasks via
//! `runtime_next::Runtime::spawn_materialize`, synthesizing the
//! Start/Join/Open envelopes the controller (Go in production) would
//! normally send.
//!
//! Transactions are counted by observing `Acknowledge` messages on a
//! single driver; on the configured target, that driver sends `Stop` —
//! which the leader fans out to every shard via `Stopped`, dropping each
//! request channel and surfacing a clean Go EOF to `serve`.

use crate::preview::services::Run;
use proto_flow::{flow, runtime as cruntime};
use runtime_next::proto;
use std::io::Write;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

/// Run one preview session against the prepared topology. Sessions are
/// numbered `1..` for log context.
pub async fn run_session(
    run: &Run,
    spec: &flow::MaterializationSpec,
    session_index: usize,
    target_txns: usize,
    output_state: bool,
    output_apply: bool,
    stdout: std::sync::Arc<std::sync::Mutex<std::io::Stdout>>,
) -> anyhow::Result<()> {
    if session_index > 1 {
        tracing::warn!(
            session_index,
            "Apply will run against an already-applied endpoint; connector must be idempotent",
        );
    }

    let join_shards = crate::preview::shards::build_join_shards(run.n_shards, &spec.name);
    // shuffle topology is consumed by leader-side; passed in Join via
    // `proto::join::Shard` whose `labeling.range` matches indices. The
    // shuffle service learns ranges from the leader-opened Session, not
    // from Join — but we still build the shuffle topology to validate
    // consistent ranges (and for future use if we expose it).
    let _shuffle_shards = crate::preview::shards::build_shuffle_topology(
        run.n_shards,
        &run.peer_endpoint,
        &run.shuffle_log_dir,
    );

    let mut handles = Vec::with_capacity(run.n_shards as usize);
    for i in 0..run.n_shards {
        let run_handle = RunHandle {
            peer_endpoint: run.peer_endpoint.clone(),
            shuffle_log_dir: run.shuffle_log_dir.clone(),
            rocksdb_path: run.rocksdb_path.clone(),
            network: run.network.clone(),
            log_handler: run.log_handler,
        };
        let spec = spec.clone();
        let join_shards = join_shards.clone();
        let stdout = stdout.clone();

        handles.push(tokio::spawn(async move {
            drive_one_shard(
                run_handle,
                spec,
                i,
                join_shards,
                target_txns,
                output_state,
                output_apply,
                stdout,
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
}

async fn drive_one_shard(
    run: RunHandle,
    spec: flow::MaterializationSpec,
    shard_index: u32,
    join_shards: Vec<proto::join::Shard>,
    target_txns: usize,
    output_state: bool,
    output_apply: bool,
    stdout: std::sync::Arc<std::sync::Mutex<std::io::Stdout>>,
) -> anyhow::Result<()> {
    let (request_tx, request_rx) = mpsc::unbounded_channel::<tonic::Result<proto::Materialize>>();

    let task_name = format!("preview-shard-{shard_index:03}");

    let publisher_factory: gazette::journal::ClientFactory = std::sync::Arc::new({
        move |_authz_sub: String, _authz_obj: String| -> gazette::journal::Client {
            unreachable!("live Publisher is not used by preview ({_authz_sub}, {_authz_obj})")
        }
    });

    let runtime = runtime_next::Runtime::new(
        cruntime::Plane::Local,
        run.network.clone(),
        run.log_handler,
        None,
        task_name,
        publisher_factory,
    );

    let mut response_rx = runtime.spawn_materialize(UnboundedReceiverStream::new(request_rx));

    // 1. Start.
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
            start: Some(proto::Start {
                log_level: ::ops::LogLevel::Info as i32,
                rocksdb_descriptor,
            }),
            ..Default::default()
        }))
        .map_err(|_| anyhow::anyhow!("serve task closed before Start"))?;

    // 2. Join.
    request_tx
        .send(Ok(proto::Materialize {
            join: Some(proto::Join {
                etcd_mod_revision: 1,
                shards: join_shards,
                shard_index,
                shuffle_directory: run.shuffle_log_dir.clone(),
                shuffle_endpoint: run.peer_endpoint.clone(),
                leader_endpoint: run.peer_endpoint.clone(),
            }),
            ..Default::default()
        }))
        .map_err(|_| anyhow::anyhow!("serve task closed before Join"))?;

    // 3. Open. ops_logs/ops_stats omitted ⇒ Publisher::Preview on both sides.
    request_tx
        .send(Ok(proto::Materialize {
            open: Some(proto::materialize::Open {
                materialization: Some(spec),
                ops_logs_spec: None,
                ops_stats_spec: None,
                ops_logs_journal: String::new(),
                ops_stats_journal: String::new(),
            }),
            ..Default::default()
        }))
        .map_err(|_| anyhow::anyhow!("serve task closed before Open"))?;

    // 4. Response loop.
    //
    // Transaction counting: the leader's TailBegin always broadcasts an
    // opening L:Acknowledge at session start (acknowledging the prior
    // session's last transaction for crash-recovery purposes). On a fresh
    // session this is a no-op, but the envelope still flows and our
    // observability fanout (runtime-next/materialize/actor.rs) surfaces it
    // to Go. We discount that first ack so `target_txns` reflects actual
    // committed transactions in the current session.
    let mut acks = 0usize;
    let mut requested_stop = false;
    let mut saw_session_open_ack = false;
    while let Some(msg) = response_rx.recv().await {
        let msg = msg.map_err(runtime_next::status_to_anyhow)?;
        emit_to_stdout(&msg, output_state, output_apply, &stdout)?;

        if msg.acknowledge.is_some() {
            if !saw_session_open_ack {
                saw_session_open_ack = true;
            } else {
                acks += 1;
                if !requested_stop && target_txns != usize::MAX && acks >= target_txns {
                    requested_stop = true;
                    let _ = request_tx.send(Ok(proto::Materialize {
                        stop: Some(proto::Stop {}),
                        ..Default::default()
                    }));
                }
            }
        }
        if msg.stopped.is_some() {
            // Simulate controller EOF; spawn_materialize's task will
            // observe it and exit cleanly, dropping its response sender.
            drop(request_tx);
            break;
        }
    }

    Ok(())
}

fn emit_to_stdout(
    msg: &proto::Materialize,
    output_state: bool,
    output_apply: bool,
    stdout: &std::sync::Arc<std::sync::Mutex<std::io::Stdout>>,
) -> anyhow::Result<()> {
    if output_apply {
        if let Some(opened) = &msg.opened {
            // The driver sees only the leader's L:Opened. Apply runs
            // entirely inside startup; we surface the resolved
            // connector image as a one-shot line.
            let mut stdout = stdout.lock().unwrap();
            writeln!(
                stdout,
                "[\"applied\",{{\"image\":\"{}\"}}]",
                opened.connector_image
            )?;
        }
    }

    if msg.flushed.is_some() {
        tracing::debug!("flushed");
    }

    if output_state {
        if let Some(sc) = &msg.started_commit {
            // Connector state is carried on the leader protocol as the
            // raw State Update Wire Format. For preview, surface the
            // bytes verbatim — they are already valid JSON.
            let payload = if sc.connector_patches_json.is_empty() {
                "[]".to_string()
            } else {
                String::from_utf8_lossy(&sc.connector_patches_json).into_owned()
            };
            let mut stdout = stdout.lock().unwrap();
            writeln!(stdout, "[\"connectorState\",{payload}]")?;
        }
    }

    Ok(())
}
