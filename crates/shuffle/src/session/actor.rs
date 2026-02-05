use futures::{StreamExt, stream::BoxStream};
use proto_flow::shuffle;
use tokio::sync::mpsc;

pub struct SessionActor {
    pub service: crate::Service,
    pub session_id: u64,
    pub members: Vec<shuffle::Member>,

    pub task_name: models::Name,
    pub bindings: Vec<crate::Binding>,
    pub last_commit: Vec<shuffle::JournalProducer>,
    pub read_through: Vec<shuffle::JournalProducer>,

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
        _ = &self.task_name;
        _ = &self.bindings;
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
                Some((member_index, slice_response, rx)) = slice_response_rx.next() => {
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
        let verify = crate::verify("SessionRequest", "TODO", "coordinator", 0);

        match verify.ok(session_request)? {
            request => Err(verify.fail(request)),
        }
    }

    async fn on_slice_response(
        &mut self,
        member_index: usize,
        slice_response: Option<tonic::Result<shuffle::SliceResponse>>,
    ) -> anyhow::Result<()> {
        let verify = crate::verify(
            "SliceResponse",
            "ListingAdded, ListingRemoved",
            &self.members[member_index].endpoint,
            member_index,
        );
        let slice_response = verify.not_eof(slice_response)?;

        match slice_response {
            shuffle::SliceResponse {
                listing_added: Some(added),
                ..
            } => self.on_listing_added(added).await,

            response => Err(verify.fail(response)),
        }
    }

    async fn on_listing_added(
        &mut self,
        listing_added: shuffle::slice_response::ListingAdded,
    ) -> anyhow::Result<()> {
        let shuffle::slice_response::ListingAdded {
            binding,
            spec,
            create_revision,
            mod_revision,
            route,
        } = listing_added;

        // TODO: pick actual member to send to based on overlap.
        let member_index = 0;

        self.slice_request_tx[member_index]
            .send(shuffle::SliceRequest {
                start_read: Some(shuffle::slice_request::StartRead {
                    binding,
                    spec,
                    create_revision,
                    mod_revision,
                    route,
                    checkpoint: Vec::new(), // TODO
                }),
                ..Default::default()
            })
            .await
            .map_err(|e| anyhow::anyhow!("failed to send Listing to Slice RPC: {e}"))?;

        todo!()
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
