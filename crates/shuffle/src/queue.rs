use anyhow::Context;
use futures::TryStreamExt;
use proto_flow::shuffle::{QueueRequest, QueueResponse, queue_request, queue_response};
use tokio::sync::oneshot;

/// QueueJoin coordinates multiple Slice streams connecting to the same Queue.
/// Each Queue member receives connections from all Slices (M connections total).
pub(crate) struct QueueJoin {
    /// Expected number of Slice connections.
    member_count: u32,
    /// Sender to notify when all Slices have connected.
    ready_tx: Option<oneshot::Sender<()>>,
    /// Number of Slices that have connected so far.
    connected: u32,
}

impl QueueJoin {
    pub fn new(member_count: u32, ready_tx: oneshot::Sender<()>) -> Self {
        Self {
            member_count,
            ready_tx: Some(ready_tx),
            connected: 0,
        }
    }

    /// Record a new Slice connection. Returns true if all Slices are now connected.
    pub fn add_connection(&mut self) -> bool {
        self.connected += 1;
        if self.connected == self.member_count {
            if let Some(tx) = self.ready_tx.take() {
                let _ = tx.send(());
            }
            true
        } else {
            false
        }
    }
}

pub(crate) async fn serve_queue(
    service: crate::Service,
    mut request_rx: impl futures::Stream<Item = anyhow::Result<QueueRequest>> + Send + Unpin + 'static,
    response_tx: tokio::sync::mpsc::Sender<tonic::Result<QueueResponse>>,
) -> anyhow::Result<()> {
    // Read the Open request.
    let open = request_rx
        .try_next()
        .await?
        .context("expected Open request")?;

    let queue_request::Open {
        session_id,
        member_count,
        slice_member_index,
        queue_member_index,
    } = open.open.context("first message must be Open")?;

    tracing::info!(
        session_id,
        member_count,
        slice_member_index,
        queue_member_index,
        "queue received Open"
    );

    // Register this connection with the QueueJoin coordinator.
    let ready_rx = {
        let mut joins = service.0.queue_joins.lock().await;
        let key = (session_id, queue_member_index);

        let join = joins.entry(key).or_insert_with(|| {
            let (ready_tx, _) = oneshot::channel();
            QueueJoin::new(member_count, ready_tx)
        });

        // For now, we create a new ready channel each time.
        // In a real implementation, we'd coordinate properly.
        let (ready_tx, ready_rx) = oneshot::channel();

        if join.add_connection() {
            // All Slices connected - we're the last one.
            let _ = ready_tx.send(());
        } else {
            // Still waiting for more Slices.
            // In a full implementation, we'd store this and signal later.
            // For now, just proceed since we're running in single-member mode.
            let _ = ready_tx.send(());
        }

        ready_rx
    };

    // Wait for all Slices to connect (in full implementation).
    // For now this returns immediately.
    let _ = ready_rx.await;

    // Send Opened response.
    response_tx
        .send(Ok(QueueResponse {
            opened: Some(queue_response::Opened {}),
            flushed: None,
        }))
        .await
        .ok();

    tracing::info!(
        session_id,
        queue_member_index,
        slice_member_index,
        "queue sent Opened"
    );

    // Main loop: process Enqueue and Flush requests.
    while let Some(request) = request_rx.try_next().await? {
        if let Some(enqueue) = request.enqueue {
            // TODO: Write document to disk queue.
            tracing::trace!(
                journal_tag = enqueue.journal_tag,
                binding = enqueue.binding,
                "queue received Enqueue"
            );
        }

        if let Some(flush) = request.flush {
            // TODO: Ensure all documents are durable on disk.
            tracing::debug!(seq = flush.seq, "queue received Flush, sending Flushed");

            response_tx
                .send(Ok(QueueResponse {
                    opened: None,
                    flushed: Some(queue_response::Flushed { seq: flush.seq }),
                }))
                .await
                .ok();
        }
    }

    tracing::info!(session_id, queue_member_index, "queue stream ended");
    Ok(())
}
