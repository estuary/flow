use futures::{StreamExt, stream::BoxStream};
use proto_flow::shuffle;
use tokio::sync::mpsc;

pub struct SessionActor {
    pub last_commit: Vec<shuffle::JournalProducer>,
    pub members: Vec<shuffle::Member>,
    pub read_through: Vec<shuffle::JournalProducer>,
    pub service: crate::Service,
    pub session_id: u64,
    pub session_response_tx: mpsc::Sender<tonic::Result<shuffle::SessionResponse>>,
    pub slice_request_tx: Vec<mpsc::Sender<shuffle::SliceRequest>>,
}

impl SessionActor {
    #[tracing::instrument(
        level = "debug",
        err(level = "warn"),
        skip_all,
        fields(
            session = self.session_id,
        )
    )]
    pub async fn rx_loop<R>(
        mut self,
        mut session_request_rx: R,
        slice_response_rx: Vec<BoxStream<'static, tonic::Result<shuffle::SliceResponse>>>,
    ) -> anyhow::Result<()>
    where
        R: futures::Stream<Item = tonic::Result<shuffle::SessionRequest>> + Send + Unpin + 'static,
    {
        _ = &self.service.gazette_factory;
        _ = &self.session_id;
        _ = &self.members;
        _ = &self.last_commit;
        _ = &self.read_through;
        _ = &self.session_response_tx;
        _ = &self.slice_request_tx;

        // Use FuturesUnordered as a Stream over receive Futures for every Slice RPC.
        let mut slice_response_rx: futures::stream::FuturesUnordered<_> = slice_response_rx
            .into_iter()
            .enumerate()
            .map(next_slice_rx)
            .collect();

        loop {
            tokio::select! {
                session_request = session_request_rx.next() => {
                    match session_request {
                        Some(result) => self.on_session_request(result).await?,
                        None => break Ok(()), // Clean EOF: shutdown.
                    }
                }
                slice_response = slice_response_rx.next() => {
                    let (member_index, slice_response, rx) = slice_response.expect("slice_response_rx not empty");
                    self.on_slice_response(member_index, slice_response).await?;
                    slice_response_rx.push(next_slice_rx((member_index, rx)));
                }
            }
        }
    }

    async fn on_session_request(
        &mut self,
        session_request: tonic::Result<shuffle::SessionRequest>,
    ) -> anyhow::Result<()> {
        match session_request.map_err(crate::status_to_anyhow)? {
            request => {
                anyhow::bail!("unexpected SessionRequest: {request:?}");
            }
        }
    }

    async fn on_slice_response(
        &mut self,
        member_index: usize,
        slice_response: Option<tonic::Result<shuffle::SliceResponse>>,
    ) -> anyhow::Result<()> {
        let Some(slice_response) = slice_response else {
            anyhow::bail!(
                "unexpected SliceResponse EOF from {member_index}@{}",
                self.members[member_index].endpoint
            );
        };
        match slice_response.map_err(crate::status_to_anyhow)? {
            response => {
                anyhow::bail!(
                    "unexpected SliceResponse from {member_index}@{}: {response:?}",
                    self.members[member_index].endpoint
                );
            }
        }
    }
}

// Helper which builds a future that yields the next response from a member's Slice RPC.
async fn next_slice_rx(
    (member_index, mut rx): (
        usize,
        BoxStream<'static, tonic::Result<shuffle::SliceResponse>>,
    ),
) -> (
    usize,                                                     // Member index.
    Option<tonic::Result<shuffle::SliceResponse>>,             // Response.
    BoxStream<'static, tonic::Result<shuffle::SliceResponse>>, // Stream.
) {
    (member_index, rx.next().await, rx)
}
