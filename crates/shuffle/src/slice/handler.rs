use super::{LazyJournalClient, SliceActor, heap::ReadyReadHeap, state};
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
    let (task_name, bindings, validators, disk_backlog_threshold) =
        crate::Binding::from_task(&task)?;

    tracing::info!(
        session_id,
        slice_member_index,
        members = members.len(),
        "Slice received Open"
    );

    // Concurrently Open a Log RPC with every member.
    let open_results =
        futures::future::join_all((0..members.len()).into_iter().map(|log_member_index| {
            open_log_rpc(
                &service,
                session_id,
                slice_member_index as u32,
                &members,
                log_member_index as u32,
                disk_backlog_threshold,
            )
        }))
        .await;

    // Walk results and partition into Senders and receiver Streams.
    let mut log_request_tx = Vec::with_capacity(members.len());
    let mut log_response_rx = Vec::with_capacity(members.len());

    for result in open_results {
        let (tx, rx) = result?;
        log_request_tx.push(tx);
        log_response_rx.push(rx);
    }

    tracing::info!(
        session_id,
        log_count = log_request_tx.len(),
        "Slice opened all Log RPCs"
    );

    // Send Opened response to Slice client (the Session).
    // Capacity: fresh channel, this is the first message.
    crate::verify_send(
        &slice_response_tx,
        Ok(shuffle::SliceResponse {
            opened: Some(shuffle::slice_response::Opened {}),
            ..Default::default()
        }),
    )?;

    let journal_clients = bindings
        .iter()
        .map(|binding| {
            let service = service.clone();
            let collection = binding.collection.clone();
            let task_name = task_name.clone();

            LazyJournalClient::new(Box::new(move || {
                (service.journal_client_factory)(collection, task_name)
            }))
        })
        .collect();

    let hint_index = state::HintIndex::from_bindings(&bindings);

    let topology = state::Topology {
        session_id,
        members,
        slice_member_index,
        task_name,
        bindings,
        journal_clients,
        hint_index,
    };

    SliceActor {
        topology,
        validators,
        reads: Vec::new(),
        causal_hints: Default::default(),
        flush: state::FlushState::new(),
        progress: state::ProgressState::new(),
        slice_response_tx,
        log_prev_journal: vec![String::new(); log_request_tx.len()],
        log_request_tx,
        pending_probes: stream::FuturesUnordered::new(),
        pending_reads: stream::FuturesUnordered::new(),
        parser: simd_doc::SimdParser::new(1_000_000),
        ready_read_heap: ReadyReadHeap::new(),
        tailing_reads: 0,
        progressed_drain: crate::frontier::Drain::new(),
    }
    .serve(slice_request_rx, log_response_rx)
    .await
}

/// Open Log RPCs to all members and wait for Opened responses.
#[tracing::instrument(level = "debug", skip(service, members), err(Debug, level = "warn"))]
async fn open_log_rpc(
    service: &crate::Service,
    session_id: u32,
    slice_member_index: u32,
    members: &[shuffle::Member],
    log_member_index: u32,
    disk_backlog_threshold: u64,
) -> anyhow::Result<(
    mpsc::Sender<shuffle::LogRequest>,
    stream::BoxStream<'static, tonic::Result<shuffle::LogResponse>>,
)> {
    let verify = crate::verify(
        "LogResponse",
        "Opened",
        &members[log_member_index as usize].endpoint,
        log_member_index as usize,
    );
    let (request_tx, request_rx) = crate::new_channel::<shuffle::LogRequest>();

    // Spawn or dial RPC, yielding a boxed response stream.
    let request_rx = tokio_stream::wrappers::ReceiverStream::new(request_rx);

    let mut response_rx = if log_member_index == slice_member_index {
        tracing::debug!("spawning in-process Log RPC");
        tokio_stream::wrappers::ReceiverStream::new(service.spawn_log(request_rx.map(Ok))).boxed()
    } else {
        let endpoint = &members[log_member_index as usize].endpoint;
        tracing::debug!(log_member_index, endpoint=%endpoint, "dialing remote Log RPC");
        let channel = verify.ok(service.dial_channel(&endpoint))?;
        let mut client = proto_grpc::shuffle::shuffle_client::ShuffleClient::new(channel);

        verify
            .ok(client.log(request_rx).await)?
            .into_inner()
            .boxed()
    };

    // Send Open request.
    // Capacity: fresh channel, this is the first message.
    crate::verify_send(
        &request_tx,
        shuffle::LogRequest {
            open: Some(shuffle::log_request::Open {
                session_id,
                members: members.to_vec(),
                slice_member_index,
                log_member_index,
                disk_backlog_threshold,
            }),
            append: None,
            flush: None,
        },
    )?;

    // Wait for Opened response.
    match verify.not_eof(response_rx.next().await)? {
        shuffle::LogResponse {
            opened: Some(shuffle::log_response::Opened {}),
            ..
        } => Ok((request_tx, response_rx)),

        response => Err(verify.fail(response)),
    }
}
