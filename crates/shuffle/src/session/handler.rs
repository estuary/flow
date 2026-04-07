use anyhow::Context;
use futures::StreamExt;
use proto_flow::shuffle;
use tokio::sync::mpsc;

pub(crate) async fn serve_session<R>(
    service: crate::Service,
    mut request_rx: R,
    session_response_tx: mpsc::Sender<tonic::Result<shuffle::SessionResponse>>,
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

    let shuffle::session_request::Open { task, shards } =
        open.open.context("first message must be Open")?;

    let session_id: u32 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u32;

    if shards.first().map(|m| &m.endpoint) != Some(&service.peer_endpoint) {
        anyhow::bail!(
            "this endpoint ({}) is not the first shard of the session: {shards:?}",
            service.peer_endpoint,
        );
    }
    super::state::validate_shard_ranges(&shards)?;

    let task = task.context("Open must include task")?;
    let (task_name, bindings, _validators, _disk_backlog_threshold) =
        crate::Binding::from_task(&task)?;

    tracing::info!(
        session_id,
        shard_count = shards.len(),
        "Session received Open"
    );

    // Concurrently Open a Slice RPC with every shard.
    let open_results =
        futures::future::join_all((0..shards.len()).into_iter().map(|shard_index| {
            open_slice_rpc(&service, session_id, &task, &shards, shard_index as u32)
        }))
        .await;

    // Walk results and partition into Senders and receiver Streams.
    let mut slice_request_tx = Vec::with_capacity(shards.len());
    let mut response_rx = Vec::with_capacity(shards.len());

    for result in open_results {
        let (tx, rx) = result?;
        slice_request_tx.push(tx);
        response_rx.push(rx);
    }

    tracing::info!(
        session_id,
        slice_count = slice_request_tx.len(),
        "Session opened all Slice RPCs"
    );

    // Send Opened response to Session client.
    // Non-blocking capacity: first message of `session_response_tx`.
    crate::verify_send(
        &session_response_tx,
        Ok(shuffle::SessionResponse {
            opened: Some(shuffle::session_response::Opened {}),
            ..Default::default()
        }),
    )?;

    // Read resume-checkpoint frontier chunks.
    let mut resume_checkpoint: Vec<crate::JournalFrontier> = Vec::new();
    let verify = crate::verify(
        "SessionRequest",
        "resume-checkpoint chunk",
        "coordinator",
        0,
    );
    loop {
        match verify.not_eof(request_rx.next().await)? {
            shuffle::SessionRequest {
                resume_checkpoint_chunk: Some(chunk),
                ..
            } => {
                if chunk.journals.is_empty() {
                    break;
                }
                resume_checkpoint.extend(crate::JournalFrontier::decode(chunk));
            }
            request => return Err(verify.fail(request)),
        };
    }
    let resume_checkpoint = crate::Frontier::new(resume_checkpoint, Vec::new())
        .context("validating resume_checkpoint frontier")?;

    tracing::debug!(session_id, ?resume_checkpoint, "Session resume checkpoint");

    // Send Start to all Slices.
    // Non-blocking capacity: first message of `slice_request_tx`.
    for tx in &slice_request_tx {
        crate::verify_send(
            tx,
            shuffle::SliceRequest {
                start: Some(shuffle::slice_request::Start {}),
                ..Default::default()
            },
        )?;
    }

    let shard_count = shards.len();

    let topology = super::state::Topology {
        session_id,
        shards,
        task_name,
        bindings,
        resume_checkpoint,
    };
    let checkpoint =
        super::state::CheckpointPipeline::new(&topology.resume_checkpoint, shard_count);

    super::actor::SessionActor {
        topology,
        checkpoint,
        progress_ready: vec![true; shard_count],
        session_response_tx: session_response_tx.clone(),
        slice_request_tx,
        start_reads: std::collections::VecDeque::new(),
        checkpoint_drain: crate::frontier::Drain::new(),
    }
    .serve(request_rx, response_rx)
    .await
}

#[tracing::instrument(
    level = "debug",
    skip(service, task, shards),
    err(Debug, level = "warn")
)]
pub async fn open_slice_rpc(
    service: &crate::Service,
    session_id: u32,
    task: &shuffle::Task,
    shards: &[shuffle::Shard],
    slice_shard_index: u32,
) -> anyhow::Result<(
    mpsc::Sender<shuffle::SliceRequest>,
    futures::stream::BoxStream<'static, tonic::Result<shuffle::SliceResponse>>,
)> {
    let verify = crate::verify(
        "SliceResponse",
        "Opened",
        &shards[slice_shard_index as usize].endpoint,
        slice_shard_index as usize,
    );
    let (request_tx, request_rx) = crate::new_channel::<shuffle::SliceRequest>();

    // Spawn or dial RPC, yielding a boxed response stream.
    let request_rx = tokio_stream::wrappers::ReceiverStream::new(request_rx);

    let mut response_rx = if slice_shard_index == 0 {
        tracing::debug!("spawning in-process Slice RPC");
        tokio_stream::wrappers::ReceiverStream::new(service.spawn_slice(request_rx.map(Ok))).boxed()
    } else {
        let endpoint = &shards[slice_shard_index as usize].endpoint;
        tracing::debug!(slice_shard_index, endpoint=%endpoint, "dialing remote Slice RPC");
        let channel = verify.ok(service.dial_channel(&endpoint))?;
        let mut client = proto_grpc::shuffle::shuffle_client::ShuffleClient::new(channel);

        verify
            .ok(client.slice(request_rx).await)?
            .into_inner()
            .boxed()
    };

    // Send Open request.
    // Capacity: fresh channel (cap 32), this is the first message.
    crate::verify_send(
        &request_tx,
        shuffle::SliceRequest {
            open: Some(shuffle::slice_request::Open {
                session_id,
                task: Some(task.clone()),
                shards: shards.to_vec(),
                shard_index: slice_shard_index,
            }),
            ..Default::default()
        },
    )?;

    // Wait for Opened response.
    match verify.not_eof(response_rx.next().await)? {
        shuffle::SliceResponse {
            opened: Some(shuffle::slice_response::Opened {}),
            ..
        } => Ok((request_tx, response_rx)),

        response => Err(verify.fail(response)),
    }
}
