use anyhow::Context;
use futures::{StreamExt, stream};
use proto_flow::shuffle;
use tokio::sync::mpsc;

pub(crate) async fn serve_slice<R>(
    service: crate::Service,
    mut slice_request_rx: R,
    slice_response_tx: mpsc::Sender<tonic::Result<shuffle::SliceResponse>>,
) -> anyhow::Result<()>
where
    R: futures::Stream<Item = tonic::Result<shuffle::SliceRequest>> + Send + Unpin + 'static,
{
    // Read the Open request.
    let open = slice_request_rx
        .next()
        .await
        .context("expected Open request")?
        .map_err(crate::status_to_anyhow)?;

    let shuffle::slice_request::Open {
        session_id,
        task,
        members,
        member_index: slice_member_index,
    } = open.open.context("first message must be Open")?;

    if members
        .get(slice_member_index as usize)
        .map(|m| &m.endpoint)
        != Some(&service.peer_endpoint)
    {
        anyhow::bail!(
            "this endpoint ({}) is not member_index {slice_member_index} of the session: {members:?}",
            service.peer_endpoint,
        );
    }
    let task = task.context("Open must include task")?;
    let (task_name, bindings) = crate::Binding::from_task(&task)?;
    let clients = std::iter::repeat(None).take(bindings.len()).collect();

    tracing::info!(
        session_id,
        slice_member_index,
        members = members.len(),
        "Slice received Open"
    );

    // Concurrently Open a Queue RPC with every member.
    let open_results =
        futures::future::join_all((0..members.len()).into_iter().map(|queue_member_index| {
            open_queue_rpc(
                &service,
                session_id,
                slice_member_index as u32,
                &members,
                queue_member_index as u32,
            )
        }))
        .await;

    // Walk results and partition into Senders and receiver Streams.
    let mut queue_request_tx = Vec::with_capacity(members.len());
    let mut queue_response_rx = Vec::with_capacity(members.len());

    for result in open_results {
        let (tx, rx) = result?;
        queue_request_tx.push(tx);
        queue_response_rx.push(rx);
    }

    tracing::info!(
        session_id,
        queue_count = queue_request_tx.len(),
        "Slice opened all Queue RPCs"
    );

    // Send Opened response to Slice client (the Session).
    let _ignored = slice_response_tx
        .send(Ok(shuffle::SliceResponse {
            opened: Some(shuffle::slice_response::Opened {}),
            ..Default::default()
        }))
        .await;

    super::actor::SliceActor {
        cancel: tokens::CancellationToken::new(),
        service,
        session_id,
        members,
        slice_member_index,
        task_name,
        bindings,
        clients,
        queue_request_tx,
        slice_response_tx,
        pending_reads: stream::FuturesUnordered::new(),
        parser: simd_doc::SimdParser::new(1_000_000),
    }
    .rx_loop(slice_request_rx, queue_response_rx)
    .await
}

/// Open Queue RPCs to all members and wait for Opened responses.
#[tracing::instrument(level = "debug", skip(service, members), err)]
async fn open_queue_rpc(
    service: &crate::Service,
    session_id: u64,
    slice_member_index: u32,
    members: &[shuffle::Member],
    queue_member_index: u32,
) -> anyhow::Result<(
    mpsc::Sender<shuffle::QueueRequest>,
    stream::BoxStream<'static, tonic::Result<shuffle::QueueResponse>>,
)> {
    let verify = crate::verify(
        "QueueResponse",
        "Opened",
        &members[queue_member_index as usize].endpoint,
        queue_member_index as usize,
    );
    let (request_tx, request_rx) = crate::new_channel::<shuffle::QueueRequest>();

    // Spawn or dial RPC, yielding a boxed response stream.
    let request_rx = tokio_stream::wrappers::ReceiverStream::new(request_rx);

    let mut response_rx = if queue_member_index == slice_member_index {
        tracing::debug!("spawning in-process Queue RPC");
        tokio_stream::wrappers::ReceiverStream::new(service.spawn_queue(request_rx.map(Ok))).boxed()
    } else {
        let endpoint = &members[queue_member_index as usize].endpoint;
        tracing::debug!(queue_member_index, endpoint=%endpoint, "dialing remote Queue RPC");
        let channel = verify.ok(service.dial_channel(&endpoint))?;
        let mut client = proto_grpc::shuffle::shuffle_client::ShuffleClient::new(channel);

        verify
            .ok(client.queue(request_rx).await)?
            .into_inner()
            .boxed()
    };

    // Send Open request.
    let _ignored = request_tx
        .send(shuffle::QueueRequest {
            open: Some(shuffle::queue_request::Open {
                session_id,
                members: members.to_vec(),
                slice_member_index,
                queue_member_index,
            }),
            enqueue: None,
            flush: None,
        })
        .await;

    // Wait for Opened response.
    match verify.not_eof(response_rx.next().await)? {
        shuffle::QueueResponse {
            opened: Some(shuffle::queue_response::Opened {}),
            ..
        } => Ok((request_tx, response_rx)),

        response => Err(verify.fail(response)),
    }
}
