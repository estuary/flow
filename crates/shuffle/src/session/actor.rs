use futures::{FutureExt, StreamExt, future, stream::BoxStream};
use proto_flow::shuffle;
use tokio::sync::mpsc;

pub struct SessionActor {
    /// Immutable session configuration: topology, bindings, resume checkpoint.
    pub topology: super::state::Topology,
    /// Four-stage checkpoint pipeline state machine.
    pub checkpoint: super::state::CheckpointPipeline,
    /// Bits by-member indicating whether to send a ProgressRequest.
    pub progress_ready: Vec<bool>,
    /// Channel for sending SessionResponse messages back to the coordinator.
    pub session_response_tx: mpsc::Sender<tonic::Result<shuffle::SessionResponse>>,
    /// Per-member channels for sending SliceRequest messages.
    pub slice_request_tx: Vec<mpsc::Sender<shuffle::SliceRequest>>,
    /// Buffered StartReads to be transmitted to their target Slice channel.
    /// Each entry is (member_index, StartRead). Drained in FIFO order.
    pub start_reads: std::collections::VecDeque<(usize, shuffle::slice_request::StartRead)>,
    /// Drain of the checkpoint frontier being transmitted as chunked responses.
    pub checkpoint_drain: crate::frontier::Drain,
}

impl SessionActor {
    #[tracing::instrument(
        level = "debug",
        err(Debug, level = "warn"),
        skip_all,
        fields(
            session = self.topology.session_id,
            task = %self.topology.task_name,
        )
    )]
    pub async fn serve<R>(
        mut self,
        mut session_request_rx: R,
        slice_response_rx: Vec<BoxStream<'static, tonic::Result<shuffle::SliceResponse>>>,
    ) -> anyhow::Result<()>
    where
        R: futures::Stream<Item = tonic::Result<shuffle::SessionRequest>> + Send + Unpin + 'static,
    {
        // Use FuturesUnordered as a Stream over receive Futures for every Slice RPC.
        let mut slice_response_rx: futures::stream::FuturesUnordered<_> = slice_response_rx
            .into_iter()
            .enumerate()
            .map(next_slice_rx)
            .collect();

        loop {
            // First, attempt non-blocking sends.
            let wake_slice_request_tx = self.try_slice_request_tx()?;
            let wake_session_response_tx = self.try_session_response_tx()?;

            // Then, wait for a blocking future to resolve.
            tokio::select! {
                biased;

                // First priority is receiving messages.
                session_request = session_request_rx.next() => {
                    match session_request {
                        Some(result) => self.on_session_request(result)?,
                        None => break Ok(()), // Clean EOF: shutdown.
                    }
                }
                Some((member_index, slice_response, rx)) = slice_response_rx.next() => {
                    self.on_slice_response(member_index, slice_response)?;
                    slice_response_rx.push(next_slice_rx((member_index, rx)));
                }

                // Next priority is draining ready-to-send messages.
                true = wake_slice_request_tx => {}
                true = wake_session_response_tx => {}
            }
        }
    }

    fn try_slice_request_tx(&mut self) -> anyhow::Result<impl Future<Output = bool> + 'static> {
        // Closure for mapping an OwnedPermit Result to Ok (our "poll again" signal).
        // On Err (channel closed), we don't wake and rely on rx of a causal error / fail-fast teardown.
        let ok = |result: Result<_, _>| result.is_ok();
        // Future which represent an absence of an awake signal.
        let idle = future::Either::Right(std::future::ready(false));

        // Try to drain Progress requests. This loop may head-of-line block if
        // we're unable to send to a FIFO member. We accept this property for
        // implementation simplicity.
        for (member_index, pending) in self.progress_ready.iter_mut().enumerate() {
            if !*pending {
                continue;
            }
            let tx = &self.slice_request_tx[member_index];

            let Ok(permit) = tx.try_reserve() else {
                return Ok(future::Either::Left(tx.clone().reserve_owned().map(ok)));
            };
            *pending = false;

            permit.send(shuffle::SliceRequest {
                progress: Some(shuffle::slice_request::Progress {}),
                ..Default::default()
            });
        }

        // Try to drain StartRead requests in FIFO order.
        while let Some((member_index, _start_read)) = self.start_reads.front() {
            let tx = &self.slice_request_tx[*member_index];

            let Ok(permit) = tx.try_reserve() else {
                return Ok(future::Either::Left(tx.clone().reserve_owned().map(ok)));
            };
            let (_member_index, start_read) = self.start_reads.pop_front().unwrap();

            permit.send(shuffle::SliceRequest {
                start_read: Some(start_read),
                ..Default::default()
            });
        }

        Ok(idle)
    }

    fn try_session_response_tx(&mut self) -> anyhow::Result<impl Future<Output = bool> + 'static> {
        // Closure for mapping an OwnedPermit Result to Ok (our "poll again" signal).
        // On Err (channel closed), we don't wake and rely on rx of a causal error / fail-fast teardown.
        let ok = |result: Result<_, _>| result.is_ok();
        // Future which represent an absence of an awake signal.
        let idle = future::Either::Right(std::future::ready(false));

        if self.checkpoint_drain.is_empty() {
            if let Some(frontier) = self.checkpoint.take_ready() {
                self.checkpoint_drain.start(frontier);
            }
        }

        // Try to drain NextCheckpoint response chunks.
        // Ensure channel capacity *before* next_chunk() to not lose it.
        while !self.checkpoint_drain.is_empty() {
            let Ok(permit) = self.session_response_tx.try_reserve() else {
                return Ok(future::Either::Left(
                    self.session_response_tx.clone().reserve_owned().map(ok),
                ));
            };
            let chunk = self.checkpoint_drain.next_chunk().unwrap();

            permit.send(Ok(shuffle::SessionResponse {
                next_checkpoint_chunk: Some(chunk),
                ..Default::default()
            }));
        }

        Ok(idle)
    }

    fn on_session_request(
        &mut self,
        session_request: tonic::Result<shuffle::SessionRequest>,
    ) -> anyhow::Result<()> {
        let verify = crate::verify("SessionRequest", "NextCheckpoint", "coordinator", 0);

        match verify.ok(session_request)? {
            shuffle::SessionRequest {
                next_checkpoint: Some(shuffle::session_request::NextCheckpoint {}),
                ..
            } => self.checkpoint.request(),
            request => Err(verify.fail(request)),
        }
    }

    fn on_slice_response(
        &mut self,
        member_index: usize,
        slice_response: Option<tonic::Result<shuffle::SliceResponse>>,
    ) -> anyhow::Result<()> {
        let verify = crate::verify(
            "SliceResponse",
            "ListingAdded or ProgressDelta",
            &self.topology.members[member_index].endpoint,
            member_index,
        );
        let slice_response = verify.not_eof(slice_response)?;

        match slice_response {
            shuffle::SliceResponse {
                listing_added: Some(added),
                ..
            } => {
                let routed = self.topology.route_read(&added)?;
                let (member_index, start_read) = self.topology.build_start_read(&routed, added);
                self.start_reads.push_back((member_index, start_read));
                Ok(())
            }

            shuffle::SliceResponse {
                progressed: Some(chunk),
                ..
            } => {
                if self.checkpoint.on_progressed_chunk(member_index, chunk)? {
                    self.progress_ready[member_index] = true;
                }
                Ok(())
            }

            response => Err(verify.fail(response)),
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
