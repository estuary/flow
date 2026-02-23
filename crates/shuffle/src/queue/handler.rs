use super::QueueJoin;
use anyhow::Context;
use futures::StreamExt;
use proto_flow::shuffle;
use tokio::sync::mpsc;

pub(crate) async fn serve_queue<R>(
    service: crate::Service,
    mut request_rx: R,
    response_tx: mpsc::Sender<tonic::Result<shuffle::QueueResponse>>,
) -> anyhow::Result<()>
where
    R: futures::Stream<Item = tonic::Result<shuffle::QueueRequest>> + Send + Unpin + 'static,
{
    // Read the Open request.
    let open = request_rx
        .next()
        .await
        .context("expected Open request")?
        .map_err(crate::status_to_anyhow)?;

    let shuffle::queue_request::Open {
        session_id,
        members,
        slice_member_index,
        queue_member_index,
    } = open.open.context("first message must be Open")?;

    tracing::info!(
        session_id,
        members = members.len(),
        slice_member_index,
        queue_member_index,
        "queue received Open"
    );
    let join_key = (session_id, queue_member_index);

    // Scope `guard` to prove it's not held across await points.
    let connections = {
        let mut guard = service.queue_joins.lock().unwrap();

        let join = guard.entry(join_key).or_insert_with(|| QueueJoin {
            members: std::iter::repeat_with(|| None)
                .take(members.len())
                .collect(),
        });
        if join.members.len() != members.len() {
            anyhow::bail!(
                "Queue member_index {queue_member_index} in session {session_id} expected member_count {} but got {}",
                join.members.len(),
                members.len(),
            );
        }
        if slice_member_index as usize >= join.members.len() {
            anyhow::bail!(
                "Queue member_index {queue_member_index} in session {session_id}: slice_member_index {slice_member_index} out of range (member_count {})",
                join.members.len(),
            );
        }
        if join.members[slice_member_index as usize].is_some() {
            anyhow::bail!(
                "Queue member_index {queue_member_index} in session {session_id} received duplicate Slice connection from {slice_member_index}",
            );
        }
        join.members[slice_member_index as usize] = Some((request_rx.boxed(), response_tx));

        let connected = join.members.iter().filter(|s| s.is_some()).count();

        tracing::debug!(
            session_id,
            queue_member_index,
            slice_member_index,
            connected,
            members = members.len(),
            "registered Slice connection with QueueJoin"
        );

        // Are there still more Slices that need to connect?
        if connected != members.len() as usize {
            return Ok(());
        }
        // All Slices have connected to this Queue.
        let QueueJoin { members } = guard.remove(&join_key).unwrap();
        members
    };

    // Walk `connections` and partition into Senders and receiver Streams.
    let mut queue_response_tx = Vec::with_capacity(members.len());
    let mut queue_request_rx = Vec::with_capacity(members.len());

    for connection in connections {
        let (rx, tx) = connection.unwrap();
        queue_response_tx.push(tx);
        queue_request_rx.push(rx);
    }

    // Send Opened response to all Slices.
    // Safety: this is the first message on a new channel.
    for tx in &queue_response_tx {
        crate::verify_send(
            tx,
            Ok(shuffle::QueueResponse {
                opened: Some(shuffle::queue_response::Opened {}),
                ..Default::default()
            }),
        )?;
    }

    let member_count = members.len();

    super::actor::QueueActor {
        topology: super::state::Topology {
            session_id,
            members,
            queue_member_index,
        },
        enqueue_heap: super::heap::EnqueueHeap::new(),
        slice_prev_journal: vec![String::new(); member_count],
        slice_enqueues: std::iter::repeat_with(|| None).take(member_count).collect(),
        pending_flushed: Vec::new(),
        queue_response_tx,
    }
    .serve(queue_request_rx)
    .await
}
