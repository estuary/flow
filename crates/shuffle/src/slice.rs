use anyhow::Context;
use futures::TryStreamExt;
use proto_flow::shuffle::{
    Member, QueueRequest, QueueResponse, SliceRequest, SliceResponse, queue_request, slice_request,
    slice_response,
};

pub(crate) async fn serve_slice(
    service: crate::Service,
    mut request_rx: impl futures::Stream<Item = anyhow::Result<SliceRequest>> + Send + Unpin + 'static,
    response_tx: tokio::sync::mpsc::Sender<tonic::Result<SliceResponse>>,
) -> anyhow::Result<()> {
    // Read the Open request.
    let open = request_rx
        .try_next()
        .await?
        .context("expected Open request")?;

    let slice_request::Open {
        session_id,
        task: _task,
        members,
        member_index,
    } = open.open.context("first message must be Open")?;

    tracing::info!(
        session_id,
        member_index,
        member_count = members.len(),
        "slice received Open"
    );

    // Open Queue RPCs to all members.
    let queue_streams = open_queue_rpcs(&service, session_id, member_index, &members).await?;

    tracing::info!(
        session_id,
        member_index,
        queue_count = queue_streams.len(),
        "slice opened all Queue RPCs"
    );

    // Send Opened response to Session.
    response_tx
        .send(Ok(SliceResponse {
            opened: Some(slice_response::Opened {}),
            progress_delta: None,
        }))
        .await
        .context("sending Slice Opened")?;

    tracing::info!(session_id, member_index, "slice sent Opened to Session");

    // Main loop: handle JournalTags, StartRead, StopRead from Session.
    while let Some(request) = request_rx.try_next().await? {
        if let Some(journal_tags) = request.journal_tags {
            // TODO: Store journal tag mappings.
            tracing::debug!(
                tag_count = journal_tags.tags.len(),
                "slice received JournalTags"
            );
        }

        if let Some(start_read) = request.start_read {
            // TODO: Start reading from the specified journal.
            tracing::debug!(
                journal_tag = start_read.journal_tag,
                binding = start_read.binding,
                "slice received StartRead"
            );
        }

        if let Some(stop_read) = request.stop_read {
            // TODO: Stop reading from the specified journal.
            tracing::debug!(
                journal_tag = stop_read.journal_tag,
                "slice received StopRead"
            );
        }
    }

    tracing::info!(session_id, member_index, "slice stream ended");
    Ok(())
}

/// Open Queue RPCs to all members and wait for Opened responses.
async fn open_queue_rpcs(
    service: &crate::Service,
    session_id: u64,
    slice_member_index: u32,
    members: &[Member],
) -> anyhow::Result<Vec<QueueStream>> {
    let member_count = members.len() as u32;

    let futures: Vec<_> = members
        .iter()
        .enumerate()
        .map(|(queue_member_index, member)| async move {
            tracing::debug!(
                session_id,
                slice_member_index,
                queue_member_index,
                address=%member.address,
                "opening Queue RPC"
            );

            let (request_tx, request_rx) = super::new_channel::<QueueRequest>();
            let request_rx = tokio_stream::wrappers::ReceiverStream::new(request_rx);
            let mut response_rx = service.dial_queue(&member.address, request_rx).await?;

            // Send Open request.
            request_tx
                .send(QueueRequest {
                    open: Some(queue_request::Open {
                        session_id,
                        member_count,
                        slice_member_index,
                        queue_member_index: queue_member_index as u32,
                    }),
                    enqueue: None,
                    flush: None,
                })
                .await
                .context("sending Queue Open")?;

            // Wait for Opened response.
            let opened = response_rx
                .try_next()
                .await
                .context("waiting for Queue Opened")?
                .context("Queue closed without Opened")?;

            anyhow::ensure!(
                opened.opened.is_some(),
                "expected Opened response from Queue"
            );

            tracing::debug!(
                session_id,
                slice_member_index,
                queue_member_index,
                "received Opened from Queue"
            );

            Ok(QueueStream {
                request_tx,
                response_rx,
            })
        })
        .collect();

    futures::future::try_join_all(futures).await
}

/// A connected Queue RPC stream.
#[allow(dead_code)]
pub struct QueueStream {
    pub request_tx: tokio::sync::mpsc::Sender<QueueRequest>,
    pub response_rx: futures::stream::BoxStream<'static, anyhow::Result<QueueResponse>>,
}
