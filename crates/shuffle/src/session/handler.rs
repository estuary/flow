use anyhow::Context;
use futures::StreamExt;
use proto_flow::shuffle;
use tokio::sync::mpsc;

pub(crate) async fn serve_session<R>(
    service: crate::Service,
    mut request_rx: R,
    response_tx: mpsc::Sender<tonic::Result<shuffle::SessionResponse>>,
) -> anyhow::Result<()>
where
    R: futures::Stream<Item = tonic::Result<shuffle::SessionRequest>> + Send + Unpin + 'static,
{
    // Read the Open request.
    let open = request_rx
        .next()
        .await
        .context("expected Open request")?
        .map_err(crate::status_to_anyhow)?;

    let shuffle::session_request::Open {
        session_id,
        task,
        members,
    } = open.open.context("first message must be Open")?;

    if members.first().map(|m| &m.endpoint) != Some(&service.peer_endpoint) {
        anyhow::bail!(
            "this endpoint ({}) is not the first member of the session: {members:?}",
            service.peer_endpoint,
        );
    }
    let task = task.context("Open must include task")?;
    let (task_name, bindings) = crate::Binding::from_task(&task)?;

    tracing::info!(
        session_id,
        member_count = members.len(),
        "Session received Open"
    );

    // Concurrently Open a Slice RPC with every member.
    let open_results =
        futures::future::join_all((0..members.len()).into_iter().map(|member_index| {
            open_slice_rpc(&service, session_id, &task, &members, member_index as u32)
        }))
        .await;

    // Walk results and partition into Senders and receiver Streams.
    let mut request_tx = Vec::with_capacity(members.len());
    let mut response_rx = Vec::with_capacity(members.len());

    for result in open_results {
        let (tx, rx) = result?;
        request_tx.push(tx);
        response_rx.push(rx);
    }

    tracing::info!(
        session_id,
        slice_count = request_tx.len(),
        "Session opened all Slice RPCs"
    );

    // Send Opened response to Session client.
    let _ignored = response_tx
        .send(Ok(shuffle::SessionResponse {
            opened: Some(shuffle::session_response::Opened {}),
            ..Default::default()
        }))
        .await;

    // Read last-commit chunks.
    let mut last_commit = Vec::new();
    let verify = crate::verify("SessionRequest", "last-commit chunk", "coordinator", 0);
    loop {
        match verify.not_eof(request_rx.next().await)? {
            shuffle::SessionRequest {
                last_commit_chunk: Some(shuffle::JournalProducerChunk { chunk }),
                ..
            } => {
                if chunk.is_empty() {
                    break;
                }
                last_commit.extend(chunk);
            }
            request => return Err(verify.fail(request)),
        };
    }

    // Read read-through chunks.
    let mut read_through = Vec::new();
    let verify = crate::verify("SessionRequest", "read-through chunk", "coordinator", 0);
    loop {
        match verify.not_eof(request_rx.next().await)? {
            shuffle::SessionRequest {
                read_through_chunk: Some(shuffle::JournalProducerChunk { chunk }),
                ..
            } => {
                if chunk.is_empty() {
                    break;
                }
                read_through.extend(chunk);
            }
            request => return Err(verify.fail(request)),
        };
    }

    // Send Start request to all Slices.
    for tx in &request_tx {
        let _ignored = tx
            .send(shuffle::SliceRequest {
                start: Some(shuffle::slice_request::Start {}),
                ..Default::default()
            })
            .await;
    }

    super::actor::SessionActor {
        service,
        session_id,
        members,
        task_name,
        bindings,
        last_commit,
        read_through,
        session_response_tx: response_tx.clone(),
        slice_request_tx: request_tx,
    }
    .rx_loop(request_rx, response_rx)
    .await
}

#[tracing::instrument(level = "debug", skip(service, task, members), err)]
pub async fn open_slice_rpc(
    service: &crate::Service,
    session_id: u64,
    task: &shuffle::Task,
    members: &[shuffle::Member],
    slice_member_index: u32,
) -> anyhow::Result<(
    mpsc::Sender<shuffle::SliceRequest>,
    futures::stream::BoxStream<'static, tonic::Result<shuffle::SliceResponse>>,
)> {
    let verify = crate::verify(
        "SliceResponse",
        "Opened",
        &members[slice_member_index as usize].endpoint,
        slice_member_index as usize,
    );
    let (request_tx, request_rx) = crate::new_channel::<shuffle::SliceRequest>();

    // Spawn or dial RPC, yielding a boxed response stream.
    let request_rx = tokio_stream::wrappers::ReceiverStream::new(request_rx);

    let mut response_rx = if slice_member_index == 0 {
        tracing::debug!("spawning in-process Slice RPC");
        tokio_stream::wrappers::ReceiverStream::new(service.spawn_slice(request_rx.map(Ok))).boxed()
    } else {
        let endpoint = &members[slice_member_index as usize].endpoint;
        tracing::debug!(slice_member_index, endpoint=%endpoint, "dialing remote Slice RPC");
        let channel = verify.ok(service.dial_channel(&endpoint))?;
        let mut client = proto_grpc::shuffle::shuffle_client::ShuffleClient::new(channel);

        verify
            .ok(client.slice(request_rx).await)?
            .into_inner()
            .boxed()
    };

    // Send Open request.
    let _ignored = request_tx
        .send(shuffle::SliceRequest {
            open: Some(shuffle::slice_request::Open {
                session_id,
                task: Some(task.clone()),
                members: members.to_vec(),
                member_index: slice_member_index,
            }),
            ..Default::default()
        })
        .await;

    // Wait for Opened response.
    match verify.not_eof(response_rx.next().await)? {
        shuffle::SliceResponse {
            opened: Some(shuffle::slice_response::Opened {}),
            ..
        } => Ok((request_tx, response_rx)),

        response => Err(verify.fail(response)),
    }
}
