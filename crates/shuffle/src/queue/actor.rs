use futures::{StreamExt, stream::BoxStream};
use proto_flow::shuffle;
use tokio::sync::mpsc;

pub struct QueueActor {
    pub members: Vec<shuffle::Member>,
    pub queue_member_index: u32,
    pub queue_response_tx: Vec<mpsc::Sender<tonic::Result<shuffle::QueueResponse>>>,
    pub service: crate::Service,
    pub session_id: u64,
}

impl QueueActor {
    #[tracing::instrument(
        level = "debug",
        err(level = "warn"),
        skip_all,
        fields(
            session = self.session_id,
            member = self.queue_member_index,
        )
    )]
    pub async fn rx_loop(
        mut self,
        queue_request_rx: Vec<BoxStream<'static, tonic::Result<shuffle::QueueRequest>>>,
    ) -> anyhow::Result<()> {
        _ = &self.service.gazette_factory;
        _ = &self.session_id;
        _ = &self.queue_member_index;
        _ = &self.queue_response_tx;

        // Use FuturesUnordered as a Stream over receive Futures for every Queue RPC.
        let mut queue_request_rx: futures::stream::FuturesUnordered<_> = queue_request_rx
            .into_iter()
            .enumerate()
            .map(next_queue_rx)
            .collect();

        loop {
            tokio::select! {
                queue_request = queue_request_rx.next() => {
                    let (member_index, queue_request, rx) = match queue_request {
                        Some(v) => v,
                        None => break Ok(()), // All Queue RPCs done: shutdown.
                    };
                    let Some(queue_request) = queue_request else {
                        continue; // Clean EOF from this member's Queue RPC.
                    };
                    self.on_queue_request(member_index, queue_request).await?;
                    queue_request_rx.push(next_queue_rx((member_index, rx)));
                }
            }
        }
    }

    async fn on_queue_request(
        &mut self,
        member_index: usize,
        queue_request: tonic::Result<shuffle::QueueRequest>,
    ) -> anyhow::Result<()> {
        let verify = crate::verify(
            "QueueRequest",
            "TODO",
            &self.members[member_index].endpoint,
            member_index,
        );
        let queue_request = verify.ok(queue_request)?;

        match queue_request {
            request => Err(verify.fail(request)),
        }
    }
}

// Helper which builds a future that yields the next request from a member's Queue RPC.
async fn next_queue_rx(
    (member_index, mut rx): (
        usize,
        BoxStream<'static, tonic::Result<shuffle::QueueRequest>>,
    ),
) -> (
    usize,                                                    // Member index.
    Option<tonic::Result<shuffle::QueueRequest>>,             // Request.
    BoxStream<'static, tonic::Result<shuffle::QueueRequest>>, // Stream.
) {
    (member_index, rx.next().await, rx)
}
