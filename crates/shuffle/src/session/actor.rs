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
    /// Unbounded because the coordinator drives req/resp pairs (≤1 in flight),
    /// so the queue depth is bounded by protocol — no back-pressure needed.
    pub session_response_tx: mpsc::UnboundedSender<tonic::Result<shuffle::SessionResponse>>,
    /// Per-shard channels for sending SliceRequest messages.
    pub slice_request_tx: Vec<mpsc::Sender<shuffle::SliceRequest>>,
    /// Buffered StartReads to be transmitted to their target Slice channel.
    /// Each entry is (shard_index, StartRead). Drained in FIFO order.
    pub start_reads: std::collections::VecDeque<(usize, shuffle::slice_request::StartRead)>,
    /// Per-task metrics counters.
    pub metrics: super::Metrics,
}

impl SessionActor {
    #[tracing::instrument(
        level = "debug",
        ret,
        err(Debug, level = "warn"),
        skip_all,
        fields(
            session = self.topology.session_id,
            shard_id = %self.topology.shards[0].id,
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
                progress_ready = ?self.progress_ready,
                start_reads = self.start_reads.len(),
                "SessionActor::serve iteration"
            ); // debug, not trace, because we don't loop on documents.

            // First, attempt non-blocking sends.
            let wake_slice_request_tx = self.try_slice_request_tx()?;
            self.try_session_response_tx();

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

            service_kit::event!(
                tracing::Level::DEBUG,
                "slice",
                shard_index,
                "sent Progress request",
            );
        }

        // Try to drain StartRead requests in FIFO order.
        while let Some((shard_index, _start_read)) = self.start_reads.front() {
            let tx = &self.slice_request_tx[*shard_index];

            let Ok(permit) = tx.try_reserve() else {
                return Ok(future::Either::Left(tx.clone().reserve_owned().map(ok)));
            };
            let (shard_index, start_read) = self.start_reads.pop_front().unwrap();

            permit.send(shuffle::SliceRequest {
                start_read: Some(start_read),
                ..Default::default()
            });

            service_kit::event!(
                tracing::Level::DEBUG,
                "slice",
                shard_index,
                "sent StartRead request",
            );
        }

        Ok(idle)
    }

    /// Drain a ready checkpoint Frontier (if any) to the coordinator.
    fn try_session_response_tx(&mut self) {
        let Some(frontier) = self.checkpoint.take_ready() else {
            return;
        };
        let (journals, journal_producers, bytes_read_delta, bytes_behind_delta) =
            frontier.measures();

        let _ = self.session_response_tx.send(Ok(shuffle::SessionResponse {
            next_checkpoint: Some(frontier.encode()),
            ..Default::default()
        }));

        service_kit::event!(
            tracing::Level::DEBUG,
            "coordinator",
            bytes_behind_delta,
            bytes_read_delta,
            journal_producers,
            journals,
            "sent NextCheckpoint response",
        );
        self.metrics.checkpoints.increment(1);
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
                service_kit::event!(
                    tracing::Level::DEBUG,
                    "coordinator",
                    "received NextCheckpoint request"
                );
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
                service_kit::event!(
                    tracing::Level::DEBUG,
                    "slice",
                    shard_index,
                    "received ListingAdded",
                );

                let routed = self.topology.route_read(&added)?;
                let (routed_shard, start_read) = self.topology.build_start_read(&routed, added);
                self.start_reads.push_back((routed_shard, start_read));

                Ok(())
            }

            shuffle::SliceResponse {
                progressed: Some(proto),
                ..
            } => {
                self.checkpoint.on_progressed(shard_index, proto)?; // Handles event! diagnostic.
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
