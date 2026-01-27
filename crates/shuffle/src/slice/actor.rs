use futures::{StreamExt, stream::BoxStream};
use proto_flow::shuffle;
use tokio::sync::mpsc;

pub struct SliceActor {
    pub cancel: tokens::CancellationToken,
    pub service: crate::Service,
    pub session_id: u64,
    pub members: Vec<shuffle::Member>,
    pub slice_member_index: u32,

    pub task_name: models::Name,
    pub bindings: Vec<crate::Binding>,
    pub clients: Vec<Option<gazette::journal::Client>>,

    pub queue_request_tx: Vec<mpsc::Sender<shuffle::QueueRequest>>,
    pub slice_response_tx: mpsc::Sender<tonic::Result<shuffle::SliceResponse>>,
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
        let _ = &self.queue_request_tx; // TODO

        let _drop_guard = self.cancel.clone().drop_guard();

        // Await Start from the Session RPC.
        let verify = crate::verify("SliceRequest", "Start", &self.members[0].endpoint, 0);
        match verify.not_eof(slice_request_rx.next().await)? {
            shuffle::SliceRequest {
                start: Some(shuffle::slice_request::Start {}),
                ..
            } => (),
            request => return Err(verify.fail(request)),
        };

        // Start tasks that watch journal listings of assigned bindings.
        let mut listing_tasks: futures::stream::FuturesUnordered<_> = self
            .bindings
            .iter()
            .filter(|binding| {
                binding.index % self.members.len() == self.slice_member_index as usize
            })
            .map(|binding| {
                super::listing::spawn_listing(
                    binding,
                    journal_client(&self.service, &mut self.clients, binding, &self.task_name),
                    self.slice_response_tx.clone(),
                    self.cancel.clone(),
                )
            })
            .collect();

        // Build a Stream over receive Futures for every Queue RPC.
        let mut queue_response_rx: futures::stream::FuturesUnordered<_> = queue_response_rx
            .into_iter()
            .enumerate()
            .map(next_queue_rx)
            .collect();

        // REMEMBER: we cannot send SliceResponse from this loop without risking deadlock.
        // We also cannot send QueueRequest from this loop.

        loop {
            tokio::select! {
                slice_request = slice_request_rx.next() => {
                    match slice_request {
                        Some(result) => self.on_slice_request(result).await?,
                        None => break Ok(()), // Clean EOF: shutdown.
                    }
                }
                Some((member_index, queue_response, rx)) = queue_response_rx.next() => {
                    self.on_queue_response(member_index, queue_response).await?;
                    queue_response_rx.push(next_queue_rx((member_index, rx)));
                }
                Some(listing_task) = listing_tasks.next() => {
                    self.on_listing_task(listing_task)?;
                }
            }
        }
    }

    async fn on_slice_request(
        &mut self,
        slice_request: tonic::Result<shuffle::SliceRequest>,
    ) -> anyhow::Result<()> {
        let verify = crate::verify("SliceRequest", "TODO", &self.members[0].endpoint, 0);

        match verify.ok(slice_request)? {
            request => Err(verify.fail(request)),
        }
    }

    async fn on_queue_response(
        &mut self,
        member_index: usize,
        queue_response: Option<tonic::Result<shuffle::QueueResponse>>,
    ) -> anyhow::Result<()> {
        let verify = crate::verify(
            "QueueResponse",
            "TODO",
            &self.members[member_index].endpoint,
            member_index,
        );
        let queue_response = verify.not_eof(queue_response)?;

        match queue_response {
            response => Err(verify.fail(response)),
        }
    }

    fn on_listing_task(
        &mut self,
        listing_task: Result<Option<anyhow::Error>, tokio::task::JoinError>,
    ) -> anyhow::Result<()> {
        match listing_task {
            Err(err) => Err(anyhow::Error::new(err).context("listing task panicked")),
            Ok(None) => anyhow::bail!("listing task canceled before SliceActor::rx_loop exited"),
            Ok(Some(err)) => Err(err),
        }
    }
}

fn journal_client(
    service: &crate::Service,
    clients: &mut Vec<Option<gazette::journal::Client>>,
    binding: &crate::Binding,
    task_name: &models::Name,
) -> gazette::journal::Client {
    let cell = &mut clients[binding.index];

    match cell {
        Some(client) => client.clone(),
        None => {
            let client = (service.gazette_factory)(binding.collection.clone(), task_name.clone());

            *cell = Some(client.clone());
            client
        }
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
