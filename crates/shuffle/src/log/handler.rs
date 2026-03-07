use super::LogJoin;
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
        members,
        slice_member_index,
        log_member_index,
    } = open.open.context("first message must be Open")?;

    tracing::info!(
        session_id,
        members = members.len(),
        slice_member_index,
        log_member_index,
        "Log received Open"
    );
    let join_key = (session_id, log_member_index);

    // Scope `guard` to prove it's not held across await points.
    let connections = {
        let mut guard = service.log_joins.lock().unwrap();

        let join = guard.entry(join_key).or_insert_with(|| LogJoin {
            members: std::iter::repeat_with(|| None)
                .take(members.len())
                .collect(),
        });
        if join.members.len() != members.len() {
            anyhow::bail!(
                "Log member_index {log_member_index} in session {session_id} expected member_count {} but got {}",
                join.members.len(),
                members.len(),
            );
        }
        if slice_member_index as usize >= join.members.len() {
            anyhow::bail!(
                "Log member_index {log_member_index} in session {session_id}: slice_member_index {slice_member_index} out of range (member_count {})",
                join.members.len(),
            );
        }
        if join.members[slice_member_index as usize].is_some() {
            anyhow::bail!(
                "Log member_index {log_member_index} in session {session_id} received duplicate Slice connection from {slice_member_index}",
            );
        }
        join.members[slice_member_index as usize] = Some((request_rx.boxed(), response_tx));

        let connected = join.members.iter().filter(|s| s.is_some()).count();

        tracing::debug!(
            session_id,
            log_member_index,
            slice_member_index,
            connected,
            members = members.len(),
            "registered Slice connection with LogJoin"
        );

        // Are there still more Slices that need to connect?
        if connected != members.len() as usize {
            return Ok(());
        }
        // All Slices have connected to this Log.
        let LogJoin { members } = guard.remove(&join_key).unwrap();
        members
    };

    // Walk `connections` and partition into Senders and receiver Streams.
    let mut log_response_tx = Vec::with_capacity(members.len());
    let mut log_request_rx = Vec::with_capacity(members.len());

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

    let member_count = members.len();

    super::actor::LogActor {
        topology: super::state::Topology {
            session_id,
            members,
            log_member_index,
        },
        append_heap: super::heap::AppendHeap::new(),
        slice_prev_journal: vec![String::new(); member_count],
        slice_appends: std::iter::repeat_with(|| None).take(member_count).collect(),
        pending_flushed: Vec::new(),
        write_head: 0,
        log_response_tx,
    }
    .serve(log_request_rx)
    .await
}
