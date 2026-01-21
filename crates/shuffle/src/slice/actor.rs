use futures::{StreamExt, stream::BoxStream};
use proto_flow::shuffle;
use tokio::sync::mpsc;

pub struct SliceActor {
    pub members: Vec<shuffle::Member>,
    pub queue_request_tx: Vec<mpsc::Sender<shuffle::QueueRequest>>,
    pub service: crate::Service,
    pub session_id: u64,
    pub slice_member_index: u32,
    pub slice_response_tx: mpsc::Sender<tonic::Result<shuffle::SliceResponse>>,
    pub task: shuffle::Task,
}

impl SliceActor {
    #[tracing::instrument(
        level = "debug",
        err(level = "warn"),
        skip_all,
        fields(
            session = self.session_id,
            member = self.slice_member_index,
        )
    )]
    pub async fn rx_loop<R>(
        mut self,
        mut slice_request_rx: R,
        queue_response_rx: Vec<BoxStream<'static, tonic::Result<shuffle::QueueResponse>>>,
    ) -> anyhow::Result<()>
    where
        R: futures::Stream<Item = tonic::Result<shuffle::SliceRequest>> + Send + Unpin + 'static,
    {
        _ = &self.service.gazette_factory;
        _ = &self.session_id;
        _ = &self.slice_member_index;
        _ = &self.members;
        _ = &self.task;
        _ = &self.slice_response_tx;
        _ = &self.queue_request_tx;

        // Use FuturesUnordered as a Stream over receive Futures for every Queue RPC.
        let mut queue_response_rx: futures::stream::FuturesUnordered<_> = queue_response_rx
            .into_iter()
            .enumerate()
            .map(next_queue_rx)
            .collect();

        loop {
            tokio::select! {
                slice_request = slice_request_rx.next() => {
                    match slice_request {
                        Some(result) => self.on_slice_request(result).await?,
                        None => break Ok(()), // Clean EOF: shutdown.
                    }
                }
                queue_response = queue_response_rx.next() => {
                    let (member_index, queue_response, rx) = queue_response.expect("queue_response_rx not empty");
                    self.on_queue_response(member_index, queue_response).await?;
                    queue_response_rx.push(next_queue_rx((member_index, rx)));
                }
            }
        }
    }

    async fn on_slice_request(
        &mut self,
        slice_request: tonic::Result<shuffle::SliceRequest>,
    ) -> anyhow::Result<()> {
        match slice_request.map_err(crate::status_to_anyhow)? {
            shuffle::SliceRequest {
                start: Some(start), ..
            } => self.on_start(start).await,

            request => {
                anyhow::bail!("unexpected SliceRequest: {request:?}");
            }
        }
    }

    async fn on_queue_response(
        &mut self,
        member_index: usize,
        queue_response: Option<tonic::Result<shuffle::QueueResponse>>,
    ) -> anyhow::Result<()> {
        let Some(queue_response) = queue_response else {
            anyhow::bail!(
                "unexpected QueueResponse EOF from {member_index}@{}",
                self.members[member_index].endpoint
            );
        };
        match queue_response.map_err(crate::status_to_anyhow)? {
            response => {
                anyhow::bail!("unexpected QueueResponse: {response:?}");
            }
        }
    }

    async fn on_start(
        &mut self,
        shuffle::slice_request::Start {}: shuffle::slice_request::Start,
    ) -> anyhow::Result<()> {
        tracing::info!("on_start");
        Ok(())
    }
}

// Helper which builds a future that yields the next response from a member's Slice RPC.
async fn next_queue_rx(
    (member_index, mut rx): (
        usize,
        BoxStream<'static, tonic::Result<shuffle::QueueResponse>>,
    ),
) -> (
    usize,                                                     // Member index.
    Option<tonic::Result<shuffle::QueueResponse>>,             // Response.
    BoxStream<'static, tonic::Result<shuffle::QueueResponse>>, // Stream.
) {
    (member_index, rx.next().await, rx)
}
