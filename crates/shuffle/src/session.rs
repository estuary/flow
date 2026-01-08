use anyhow::Context;
use futures::TryStreamExt;
use proto_flow::shuffle::{
    Member, SessionRequest, SessionResponse, SliceRequest, SliceResponse, Task, session_request,
    session_response, slice_request,
};

pub(crate) async fn serve_session(
    service: crate::Service,
    mut request_rx: impl futures::Stream<Item = anyhow::Result<SessionRequest>> + Send + Unpin + 'static,
    response_tx: tokio::sync::mpsc::Sender<tonic::Result<SessionResponse>>,
) -> anyhow::Result<()> {
    // Read the Open request.
    let open = request_rx
        .try_next()
        .await?
        .context("expected Open request")?;

    let session_request::Open {
        session_id,
        task,
        members,
        resume_tags,
        last_commit,
        read_through,
    } = open.open.context("first message must be Open")?;

    let task = task.context("Open must include task")?;

    tracing::info!(
        session_id,
        member_count = members.len(),
        resume_tag_count = resume_tags.len(),
        last_commit_count = last_commit.len(),
        read_through_count = read_through.len(),
        "session received Open"
    );

    // Open Slice RPCs to all members.
    let slice_streams = open_slice_rpcs(&service, session_id, &task, &members).await?;

    tracing::info!(
        session_id,
        slice_count = slice_streams.len(),
        "session opened all Slice RPCs, sending Opened"
    );

    // Send Opened response to client.
    response_tx
        .send(Ok(SessionResponse {
            opened: Some(session_response::Opened {}),
            next_checkpoint: None,
        }))
        .await
        .ok();

    tracing::info!(session_id, "session sent Opened to client");

    // TODO: Start journal watch, broadcast JournalTags, send StartRead/StopRead.

    // Main loop: handle NextCheckpoint requests from client.
    while let Some(request) = request_rx.try_next().await? {
        if request.next_checkpoint.is_some() {
            // TODO: Aggregate progress deltas and return checkpoint.
            tracing::debug!("session received NextCheckpoint request");

            // For now, return an empty checkpoint delta.
            response_tx
                .send(Ok(SessionResponse {
                    opened: None,
                    next_checkpoint: Some(session_response::NextCheckpoint {
                        delta_checkpoint: Vec::new(),
                    }),
                }))
                .await
                .ok();
        }
    }

    tracing::info!(session_id, "session stream ended");
    Ok(())
}

/// Open Slice RPCs to all members and wait for Opened responses.
async fn open_slice_rpcs(
    service: &crate::Service,
    session_id: u64,
    task: &Task,
    members: &[Member],
) -> anyhow::Result<Vec<SliceStream>> {
    let futures: Vec<_> = members
        .iter()
        .enumerate()
        .map(|(member_index, member)| async move {
            tracing::debug!(session_id, member_index, address=%member.address, "opening Slice RPC");

            let (request_tx, request_rx) = super::new_channel::<SliceRequest>();
            let request_rx = tokio_stream::wrappers::ReceiverStream::new(request_rx);
            let mut response_rx = service.dial_slice(&member.address, request_rx).await?;

            // Send Open request.
            request_tx
                .send(SliceRequest {
                    open: Some(slice_request::Open {
                        session_id,
                        task: Some(task.clone()),
                        members: members.to_vec(),
                        member_index: member_index as u32,
                    }),
                    journal_tags: None,
                    start_read: None,
                    stop_read: None,
                })
                .await
                .context("sending Slice Open")?;

            // Wait for Opened response.
            let opened = response_rx
                .try_next()
                .await
                .context("Slice closed without Opened")?
                .context("waiting for Slice Opened")?;

            anyhow::ensure!(
                opened.opened.is_some(),
                "expected Opened response from Slice"
            );

            tracing::debug!(session_id, member_index, "received Opened from Slice");

            Ok(SliceStream {
                request_tx,
                response_rx,
            })
        })
        .collect();

    futures::future::try_join_all(futures).await
}

/// A connected Slice RPC stream.
#[allow(dead_code)]
pub struct SliceStream {
    pub request_tx: tokio::sync::mpsc::Sender<SliceRequest>,
    pub response_rx: futures::stream::BoxStream<'static, anyhow::Result<SliceResponse>>,
}
