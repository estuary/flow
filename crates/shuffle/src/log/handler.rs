use super::{LogJoin, state, writer::Writer};
use anyhow::Context;
use futures::StreamExt;
use proto_flow::shuffle;
use tokio::sync::mpsc;

pub(crate) async fn serve_log<R>(
    service: crate::Service,
    mut request_rx: R,
    response_tx: mpsc::Sender<tonic::Result<shuffle::LogResponse>>,
) -> anyhow::Result<()>
where
    R: futures::Stream<Item = tonic::Result<shuffle::LogRequest>> + Send + Unpin + 'static,
{
    // Read the Open request.
    let open = request_rx
        .next()
        .await
        .context("expected Open request")?
        .map_err(crate::status_to_anyhow)?;

    let shuffle::log_request::Open {
        session_id,
        shards,
        slice_shard_index,
        log_shard_index,
        disk_backlog_threshold,
    } = open.open.context("first message must be Open")?;

    let directory = shards
        .get(log_shard_index as usize)
        .map(|m| m.directory.as_str())
        .unwrap_or_default();

    tracing::info!(
        session_id,
        shards = shards.len(),
        slice_shard_index,
        log_shard_index,
        %directory,
        disk_backlog_threshold,
        "Log received Open"
    );
    let join_key = (directory.to_string(), log_shard_index);

    // Scope `guard` to prove it's not held across await points.
    let connections = {
        let mut guard = service.log_joins.lock().unwrap();

        let join = guard.entry(join_key.clone()).or_insert_with(|| LogJoin {
            shards: std::iter::repeat_with(|| None).take(shards.len()).collect(),
        });
        if join.shards.len() != shards.len() {
            anyhow::bail!(
                "Log shard_index {log_shard_index} directory {directory} in session {session_id} expected shard_count {} but got {}",
                join.shards.len(),
                shards.len(),
            );
        }
        if slice_shard_index as usize >= join.shards.len() {
            anyhow::bail!(
                "Log shard_index {log_shard_index} directory {directory} in session {session_id}: slice_shard_index {slice_shard_index} out of range (shard_count {})",
                join.shards.len(),
            );
        }
        if join.shards[slice_shard_index as usize].is_some() {
            anyhow::bail!(
                "Log shard_index {log_shard_index} directory {directory} in session {session_id} received duplicate Slice connection from {slice_shard_index}",
            );
        }
        join.shards[slice_shard_index as usize] = Some((request_rx.boxed(), response_tx));

        let connected = join.shards.iter().filter(|s| s.is_some()).count();

        tracing::debug!(
            session_id,
            log_shard_index,
            slice_shard_index,
            connected,
            shards = shards.len(),
            "registered Slice connection with LogJoin"
        );

        // Are there still more Slices that need to connect?
        if connected != shards.len() as usize {
            return Ok(());
        }
        // All Slices have connected to this Log.
        let LogJoin { shards } = guard.remove(&join_key).unwrap();
        shards
    };

    // Walk `connections` and partition into Senders and receiver Streams.
    let mut log_response_tx = Vec::with_capacity(shards.len());
    let mut log_request_rx = Vec::with_capacity(shards.len());

    for connection in connections {
        let (rx, tx) = connection.unwrap();
        log_response_tx.push(tx);
        log_request_rx.push(rx);
    }

    // Send Opened response to all Slices.
    // Safety: this is the first message on a new channel.
    for tx in &log_response_tx {
        crate::verify_send(
            tx,
            Ok(shuffle::LogResponse {
                opened: Some(shuffle::log_response::Opened {}),
                ..Default::default()
            }),
        )?;
    }

    let shard_count = shards.len();
    let writer = Writer::new(std::path::Path::new(directory), log_shard_index)?;

    super::actor::LogActor {
        topology: super::state::Topology {
            session_id,
            shards,
            log_shard_index,
            disk_backlog_threshold,
        },
        append_heap: super::heap::AppendHeap::new(),
        slice_prev_journal: vec![String::new(); shard_count],
        slice_appends: std::iter::repeat_with(|| None).take(shard_count).collect(),
        writer: Some(writer),
        block: state::BlockState::new(),
        flush: state::FlushState::new(),
        log_response_tx,
    }
    .serve(log_request_rx)
    .await
}
