//! Materialize service entry point.
//!
//! Reads the first controller message and dispatches:
//!  - `spec` / `validate` → unary mode. Forwards to a transient connector
//!    and streams back the response. A Validate may follow on the same
//!    stream. Unary mode never sees `Start`; the leader sidecar is not
//!    involved — Spec/Validate stay between the controller and runtime-next.
//!  - `start` → session mode. Opens the per-shard `RocksDB` (when a path is
//!    provided) and loops over leader sessions until controller EOF. The DB
//!    is held as an `Option<RocksDB>` and threaded by-value through each
//!    leader session: `startup::run` performs the one Scan per session,
//!    the actor performs Persists, and the DB is returned at session end.

use crate::{Runtime, proto, rocksdb::RocksDB, verify_send};
use anyhow::Context;
use futures::{SinkExt, Stream, StreamExt};
use tokio::sync::mpsc;

/// Top-level Materialize service entry point. Reads the first controller
/// message to choose between unary and session mode.
///
/// `controller_rx` is generic so that in-process callers (e.g. `flowctl
/// preview`, which drives synthetic shards over mpsc) can supply an
/// `UnboundedReceiverStream` in lieu of a real `tonic::Streaming`.
pub async fn serve<L: crate::LogHandler, S>(
    runtime: Runtime<L>,
    mut controller_rx: S,
    controller_tx: mpsc::Sender<tonic::Result<proto::Materialize>>,
) -> anyhow::Result<()>
where
    S: Stream<Item = tonic::Result<proto::Materialize>> + Send + Unpin + 'static,
{
    let first = controller_rx
        .next()
        .await
        .context("controller stream EOF before first message")?
        .map_err(crate::status_to_anyhow)?;

    if first.spec.is_some() || first.validate.is_some() {
        return serve_unary(runtime, first, controller_rx, controller_tx).await;
    }
    if first.start.is_some() {
        return serve_session(runtime, first, controller_rx, controller_tx).await;
    }
    anyhow::bail!("first controller message must be `spec`, `validate`, or `start`; got {first:?}");
}

/// Unary-mode: forward the first message (and any subsequent Spec/Validate
/// on the same stream) to a transient materialize connector. The connector
/// container is started once and reused across messages on this stream.
async fn serve_unary<L: crate::LogHandler, S>(
    runtime: Runtime<L>,
    first: proto::Materialize,
    mut controller_rx: S,
    controller_tx: mpsc::Sender<tonic::Result<proto::Materialize>>,
) -> anyhow::Result<()>
where
    S: Stream<Item = tonic::Result<proto::Materialize>> + Send + Unpin + 'static,
{
    let initial = build_connector_request(first)?;

    let (mut connector_tx, mut connector_rx, _open_extras) =
        super::connector::start(&runtime, initial)
            .await
            .context("starting unary-mode connector")?;

    forward_one_response(&mut connector_rx, &controller_tx).await?;

    while let Some(msg) = controller_rx.next().await {
        let next_req = build_connector_request(msg.map_err(crate::status_to_anyhow)?)?;
        connector_tx
            .send(next_req)
            .await
            .context("forwarding follow-up unary request to connector")?;
        forward_one_response(&mut connector_rx, &controller_tx).await?;
    }
    Ok(())
}

fn build_connector_request(
    msg: proto::Materialize,
) -> anyhow::Result<proto_flow::materialize::Request> {
    use proto_flow::materialize::Request;
    if let Some(spec) = msg.spec {
        Ok(Request {
            spec: Some(spec),
            ..Default::default()
        })
    } else if let Some(validate) = msg.validate {
        Ok(Request {
            validate: Some(validate),
            ..Default::default()
        })
    } else {
        anyhow::bail!("expected `spec` or `validate` in unary-mode message; got {msg:?}");
    }
}

async fn forward_one_response<S>(
    connector_rx: &mut S,
    controller_tx: &mpsc::Sender<tonic::Result<proto::Materialize>>,
) -> anyhow::Result<()>
where
    S: futures::Stream<Item = tonic::Result<proto_flow::materialize::Response>> + Unpin,
{
    let response = connector_rx
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("connector EOF before unary response"))??;

    let outgoing = if let Some(spec) = response.spec {
        proto::Materialize {
            spec_response: Some(spec),
            ..Default::default()
        }
    } else if let Some(validated) = response.validated {
        proto::Materialize {
            validated: Some(validated),
            ..Default::default()
        }
    } else {
        anyhow::bail!("connector response was neither Spec nor Validated: {response:?}");
    };

    verify_send(controller_tx, Ok(outgoing)).context("forwarding unary response to controller")?;
    Ok(())
}

/// Session mode: open the per-shard RocksDB (shard zero only) and loop
/// over leader sessions until controller EOF. The DB is threaded by-value
/// through each session so single ownership encodes the at-most-one-Persist
/// invariant. Per-session resources are torn down on Stopped while the
/// controller-facing stream stays open for the next session.
async fn serve_session<L: crate::LogHandler, S>(
    runtime: Runtime<L>,
    first: proto::Materialize,
    mut controller_rx: S,
    controller_tx: mpsc::Sender<tonic::Result<proto::Materialize>>,
) -> anyhow::Result<()>
where
    S: Stream<Item = tonic::Result<proto::Materialize>> + Send + Unpin + 'static,
{
    let start = first
        .start
        .context("session-mode dispatch missing required Start message")?;

    runtime.set_log_level(start.log_level());

    let mut rocksdb: Option<RocksDB> = match start.rocksdb_descriptor {
        Some(desc) if !desc.rocksdb_path.is_empty() => {
            Some(RocksDB::open(Some(desc)).await.context("opening RocksDB")?)
        }
        _ => None,
    };

    loop {
        let join = match controller_rx.next().await {
            None => return Ok(()), // Clean controller EOF — drop the RocksDB and exit.
            Some(Err(status)) => return Err(crate::status_to_anyhow(status)),
            Some(Ok(msg)) => msg.join.context("expected Join after Start")?,
        };

        match run_session(
            &runtime,
            &mut rocksdb,
            join,
            &mut controller_rx,
            &controller_tx,
        )
        .await
        {
            Ok(()) => continue,
            Err(err) => {
                let _ = verify_send(&controller_tx, Err(crate::anyhow_to_status(err)));
                return Ok(());
            }
        }
    }
}

/// Run one leader session: drive `startup::run` to bring the leader stream
/// and connector through C:Opened, then hand off to `actor::serve`. The
/// `rocksdb` slot is `take`n into the session and stored back when the
/// session ends, ready for the next leader session.
async fn run_session<L: crate::LogHandler, S>(
    runtime: &Runtime<L>,
    rocksdb: &mut Option<RocksDB>,
    join: proto::Join,
    controller_rx: &mut S,
    controller_tx: &mpsc::Sender<tonic::Result<proto::Materialize>>,
) -> anyhow::Result<()>
where
    S: Stream<Item = tonic::Result<proto::Materialize>> + Send + Unpin + 'static,
{
    let shard_index = join.shard_index as usize;
    let shard_id = join
        .shards
        .get(shard_index)
        .map(|s| s.id.clone())
        .unwrap_or_default();

    let session_db = rocksdb.take();

    let (started, db_after_startup) = match super::startup::run(
        super::startup::RunInputs {
            runtime,
            rocksdb: session_db,
            join,
            shard_id,
        },
        controller_rx,
        controller_tx,
    )
    .await?
    {
        Some(out) => (Some(out.startup), out.rocksdb),
        None => (None, None),
    };

    let Some(startup) = started else {
        // Session ended cleanly during startup (topology disagreement or controller EOF).
        *rocksdb = db_after_startup;
        return Ok(());
    };

    let mut actor = startup.into_actor(controller_tx.clone(), db_after_startup);
    let result = actor.serve(controller_rx).await;
    *rocksdb = actor.rocksdb.take();
    result
}
