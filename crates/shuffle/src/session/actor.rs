use futures::{FutureExt, StreamExt, future, stream::BoxStream};
use proto_flow::shuffle;
use tokio::sync::mpsc;

/// SessionActor implements the main event loop of a shuffle Session RPC.
pub struct SessionActor {
    /// Immutable session configuration: topology, bindings, resume checkpoint.
    pub topology: super::state::Topology,
    /// Four-stage checkpoint pipeline state machine.
    pub checkpoint: super::state::CheckpointPipeline,
    /// Bits by-shard indicating whether to send a ProgressRequest.
    pub progress_ready: Vec<bool>,
    /// Channel for sending SessionResponse messages back to the coordinator.
    pub session_response_tx: mpsc::Sender<tonic::Result<shuffle::SessionResponse>>,
    /// Per-shard channels for sending SliceRequest messages.
    pub slice_request_tx: Vec<mpsc::Sender<shuffle::SliceRequest>>,
    /// Buffered StartReads to be transmitted to their target Slice channel.
    /// Each entry is (shard_index, StartRead). Drained in FIFO order.
    pub start_reads: std::collections::VecDeque<(usize, shuffle::slice_request::StartRead)>,
    /// Drain of the checkpoint Frontier to be transmitted.
    pub checkpoint_drain: Option<shuffle::Frontier>,
}

impl SessionActor {
    #[tracing::instrument(
        level = "debug",
        ret,
        err(Debug, level = "warn"),
        skip_all,
        fields(
            session = self.topology.session_id,
            shard_prefix = %self.topology.shard_prefix,
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

        let mut ticker = tokio::time::interval(crate::ACTOR_TICKER_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let mut loop_count: u64 = 0;
        loop {
            loop_count += 1;
            tracing::debug!(
                loop_count,
                checkpoint = ?self.checkpoint,
                drain_pending = self.checkpoint_drain.is_some(),
                progress_ready = ?self.progress_ready,
                start_reads = self.start_reads.len(),
                "SessionActor::serve iteration"
            ); // debug, not trace, because we don't loop on documents.

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
                        None => break,
                    }
                }
                Some((shard_index, slice_response, rx)) = slice_response_rx.next() => {
                    self.on_slice_response(shard_index, slice_response)?;
                    slice_response_rx.push(next_slice_rx((shard_index, rx)));
                }

                // Next priority is draining ready-to-send messages.
                true = wake_slice_request_tx => {}
                true = wake_session_response_tx => {}

                // Periodic tick ensures tracing fires even when idle,
                // and detects stalled causal hint resolution.
                _ = ticker.tick() => {
                    self.checkpoint.on_tick()?;
                }
            }
        }

        tracing::debug!(loop_count, "SessionActor::serve exiting on coordinator EOF");
        self.slice_request_tx.clear(); // Drop all tx handles to close.

        // Read clean EOF from all Slice RPCs.
        while let Some((shard_index, slice_response, rx)) = slice_response_rx.next().await {
            let verify = crate::verify(
                "SliceResponse",
                "EOF",
                &self.topology.shards[shard_index].endpoint,
                shard_index,
            );
            match slice_response {
                None => (), // Clean EOF.
                Some(Ok(_ignored)) => slice_response_rx.push(next_slice_rx((shard_index, rx))),
                Some(Err(status)) => return Err(verify.fail_status(status)),
            }
        }

        Ok(())
    }

    fn try_slice_request_tx(&mut self) -> anyhow::Result<impl Future<Output = bool> + 'static> {
        // Closure for mapping an OwnedPermit Result to Ok (our "poll again" signal).
        // On Err (channel closed), we don't wake and rely on rx of a causal error / fail-fast teardown.
        let ok = |result: Result<_, _>| result.is_ok();
        // Future which represent an absence of an awake signal.
        let idle = future::Either::Right(std::future::ready(false));

        // Try to drain Progress requests. This loop may head-of-line block if
        // we're unable to send to a FIFO shard. We accept this property for
        // implementation simplicity.
        for (shard_index, pending) in self.progress_ready.iter_mut().enumerate() {
            if !*pending {
                continue;
            }
            let tx = &self.slice_request_tx[shard_index];

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
        while let Some((shard_index, _start_read)) = self.start_reads.front() {
            let tx = &self.slice_request_tx[*shard_index];

            let Ok(permit) = tx.try_reserve() else {
                return Ok(future::Either::Left(tx.clone().reserve_owned().map(ok)));
            };
            let (_shard_index, start_read) = self.start_reads.pop_front().unwrap();

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

        if self.checkpoint_drain.is_none() {
            if let Some(frontier) = self.checkpoint.take_ready() {
                tracing::debug!(?frontier, "sending NextCheckpoint to client");
                self.checkpoint_drain = Some(frontier.encode());
            }
        }

        // Try to drain a NextCheckpoint response.
        // Ensure channel capacity *before* take() to not lose it.
        if self.checkpoint_drain.is_some() {
            let Ok(permit) = self.session_response_tx.try_reserve() else {
                return Ok(future::Either::Left(
                    self.session_response_tx.clone().reserve_owned().map(ok),
                ));
            };

            permit.send(Ok(shuffle::SessionResponse {
                next_checkpoint: self.checkpoint_drain.take(),
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
            } => {
                tracing::debug!("received NextCheckpoint request from coordinator");
                self.checkpoint.request()
            }
            request => Err(verify.fail(request)),
        }
    }

    fn on_slice_response(
        &mut self,
        shard_index: usize,
        slice_response: Option<tonic::Result<shuffle::SliceResponse>>,
    ) -> anyhow::Result<()> {
        let verify = crate::verify(
            "SliceResponse",
            "ListingAdded or ProgressDelta",
            &self.topology.shards[shard_index].endpoint,
            shard_index,
        );
        let slice_response = verify.not_eof(slice_response)?;

        match slice_response {
            shuffle::SliceResponse {
                listing_added: Some(added),
                ..
            } => {
                let routed = self.topology.route_read(&added)?;
                tracing::debug!(
                    shard_index,
                    binding = added.binding,
                    journal = added.spec.as_ref().map(|s| s.name.as_str()).unwrap_or(""),
                    target_shard = routed.shard_index,
                    candidates = routed.shard_stop - routed.shard_start,
                    "received ListingAdded, assigning read"
                );
                let (shard_index, start_read) = self.topology.build_start_read(&routed, added);
                self.start_reads.push_back((shard_index, start_read));
                Ok(())
            }

            shuffle::SliceResponse {
                progressed: Some(proto),
                ..
            } => {
                self.checkpoint.on_progressed(shard_index, proto)?;
                tracing::debug!(
                    shard_index,
                    "Progressed received, sending next ProgressRequest"
                );
                self.progress_ready[shard_index] = true;
                Ok(())
            }

            response => Err(verify.fail(response)),
        }
    }
}

// Helper which builds a future that yields the next response from a shard's Slice RPC.
async fn next_slice_rx(
    (shard_index, mut rx): (
        usize,
        BoxStream<'static, tonic::Result<shuffle::SliceResponse>>,
    ),
) -> (
    usize,                                                     // Shard index.
    Option<tonic::Result<shuffle::SliceResponse>>,             // Response.
    BoxStream<'static, tonic::Result<shuffle::SliceResponse>>, // Stream.
) {
    (shard_index, rx.next().await, rx)
}
