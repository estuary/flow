use super::heap::{self, AppendHeap};
use futures::{FutureExt, StreamExt, future, stream::BoxStream};
use proto_flow::shuffle;
use proto_gazette::uuid;
use std::future::Future;
use tokio::sync::mpsc;

type SliceRx = BoxStream<'static, tonic::Result<shuffle::LogRequest>>;

/// LogActor implements the main event loop of a shuffle Log RPC.
///
/// Notes on back-pressure: when a Slice sends an Append, its rx stream is
/// parked in `slice_appends` until the heap pops that entry (lowest priority
/// in the select loop). Given a parked rx stream, and knowing that a Log RPC
/// is constrained by the HTTP/2 stream flow-control window (64KB), this means
/// that the Slice is itself back-pressured, and will in-turn back-pressure to
/// its journal reads (the journal Read RPC sits idle in a SliceActor's ReadyRead).
///
/// This creates the conditions for system-wide priority enforcement:
/// LogActors drain high-priority, earlier-clock documents first, which back-pressures
/// to Slices and journals that are producing lower-priority or higher-clock documents.
/// The overall rate of progress is bounded by the write throughput of the slowest Log.
pub struct LogActor {
    /// Immutable session topology: identity and member configuration.
    pub topology: super::state::Topology,
    /// Per-Slice response channel for sending Opened and Flushed responses.
    pub log_response_tx: Vec<mpsc::Sender<tonic::Result<shuffle::LogResponse>>>,
    /// Ready Append and receive stream for each Slice, set when an Append is
    /// received and consumed when the corresponding heap entry is popped.
    /// None while the slice's next read is pending in `pending_slices`.
    pub slice_appends: Vec<Option<(shuffle::log_request::Append, SliceRx)>>,
    /// Previous journal name received from each Slice member, for delta decoding.
    pub slice_prev_journal: Vec<String>,
    /// Ordered heap of references to Some `slice_appends` items.
    pub append_heap: AppendHeap,
    /// Completed flush IOs awaiting send of their Flushed response.
    pub pending_flushed: Vec<(usize, u64)>,
}

impl LogActor {
    #[tracing::instrument(
        level = "debug",
        err(Debug, level = "warn"),
        skip_all,
        fields(
            session = self.topology.session_id,
            member = self.topology.log_member_index,
        )
    )]
    pub async fn serve(
        mut self,
        log_request_rx: Vec<BoxStream<'static, tonic::Result<shuffle::LogRequest>>>,
    ) -> anyhow::Result<()> {
        // Build rx futures for the next LogRequest from each Slice.
        let mut pending_slices: futures::stream::FuturesUnordered<_> = log_request_rx
            .into_iter()
            .enumerate()
            .map(next_log_rx)
            .collect();

        // Pending IO futures (e.g. disk writes) that must complete before
        // we send the corresponding Flushed response.
        let mut pending_io = futures::stream::FuturesUnordered::new();

        let mut loop_count: u64 = 0;
        loop {
            loop_count += 1;
            tracing::debug!(
                loop_count,
                pending_slices = pending_slices.len(),
                pending_io = pending_io.len(),
                pending_flushed = self.pending_flushed.len(),
                heap_size = self.append_heap.len(),
                "LogActor::serve iteration"
            );

            // First, attempt non-blocking sends of completed Flushed responses.
            let wake_log_response_tx = self.try_log_response_tx()?;

            tokio::select! {
                biased;

                // First priority: read a ready LogRequest from pending slices.
                Some((member_index, log_request, rx)) = pending_slices.next() => {
                    if let Some((rx, flush_fut)) = self.on_log_request(member_index, log_request, rx)? {
                        pending_slices.push(next_log_rx((member_index, rx)));
                        pending_io.push(flush_fut);
                    }
                }

                // Second priority: complete a pending IO, queuing the Flushed for send.
                Some(result) = pending_io.next() => {
                    let (member_index, cycle) = result?;
                    tracing::debug!(member_index, cycle, "flush IO completed");
                    self.pending_flushed.push((member_index, cycle));
                }

                // Third priority: wake when a blocked log_response_tx has capacity.
                true = wake_log_response_tx => {}

                // Fourth priority: drain one entry from the heap.
                _ = std::future::ready(()), if !self.append_heap.is_empty() => {
                    let pending = self.on_append_pop();
                    pending_slices.push(next_log_rx(pending));
                }

                // All slices EOF'd, heap drained, IO complete, and flushes sent.
                else => {
                    tracing::debug!(loop_count, "LogActor::serve exiting, all slices EOF");
                    break Ok(());
                }
            }
        }
    }

    /// Try to send completed Flushed responses. If a channel is full, return
    /// a wake future that resolves when capacity is available.
    fn try_log_response_tx(&mut self) -> anyhow::Result<impl Future<Output = bool> + 'static> {
        // Closure for mapping an OwnedPermit Result to Ok (our "poll again" signal).
        // On Err (channel closed), we don't wake and rely on rx of a causal error / fail-fast teardown.
        let ok = |result: Result<_, _>| result.is_ok();
        // Future which represent an absence of an awake signal.
        let idle = future::Either::Right(std::future::ready(false));

        // This loop may head-of-line block if we're unable to send a LIFO Flushed.
        // We accept this property for implementation simplicity.
        while let Some(&(member_index, cycle)) = self.pending_flushed.last() {
            let tx = &self.log_response_tx[member_index];

            let Ok(permit) = tx.try_reserve() else {
                return Ok(future::Either::Left(tx.clone().reserve_owned().map(ok)));
            };
            self.pending_flushed.pop();

            permit.send(Ok(shuffle::LogResponse {
                flushed: Some(shuffle::log_response::Flushed { cycle }),
                ..Default::default()
            }));
            tracing::debug!(member_index, cycle, "sent Flushed response to Slice");
        }

        Ok(idle)
    }

    /// Handle a ready slice: verify the request and dispatch to on_append or on_flush.
    ///
    /// Returns `None` on clean EOF or when the Append and rx are parked in
    /// `slice_ready`. Returns `Some` with the stream to re-push and the
    /// `(member_index, cycle)` flush IO to start.
    fn on_log_request(
        &mut self,
        member_index: usize,
        log_request: Option<tonic::Result<shuffle::LogRequest>>,
        rx: SliceRx,
    ) -> anyhow::Result<
        Option<(
            SliceRx,
            impl Future<Output = anyhow::Result<(usize, u64)>> + 'static,
        )>,
    > {
        let Some(log_request) = log_request else {
            return Ok(None); // Clean EOF of this member Slice's Log RPC.
        };

        let verify = crate::verify(
            "LogRequest",
            "Append or Flush",
            &self.topology.members[member_index].endpoint,
            member_index,
        );
        let log_request = verify.ok(log_request)?;

        match log_request {
            shuffle::LogRequest {
                append: Some(append),
                ..
            } => {
                self.on_append_rx(append, member_index, rx);
                Ok(None)
            }

            shuffle::LogRequest {
                flush: Some(flush), ..
            } => Ok(Some((rx, self.on_flush(flush, member_index)))),

            request => Err(verify.fail(request)),
        }
    }

    fn on_append_rx(
        &mut self,
        append: shuffle::log_request::Append,
        member_index: usize,
        rx: SliceRx,
    ) {
        let priority = append.priority;
        let clock = uuid::Clock::from_u64(append.clock);
        let adjusted_clock = clock + uuid::Clock::from_u64(append.read_delay);

        tracing::trace!(
            member_index,
            priority,
            ?adjusted_clock,
            doc_bytes = append.doc_archived.len(),
            "received Append from Slice"
        );

        debug_assert!(self.slice_appends[member_index].is_none());
        self.slice_appends[member_index] = Some((append, rx));

        self.append_heap.push(heap::AppendEntry {
            priority,
            adjusted_clock,
            member_index,
        });
    }

    fn on_flush(
        &mut self,
        flush: shuffle::log_request::Flush,
        member_index: usize,
    ) -> impl Future<Output = anyhow::Result<(usize, u64)>> + 'static {
        let shuffle::log_request::Flush { cycle } = flush;

        tracing::debug!(member_index, cycle, "received Flush from Slice");

        // Emulate disk IO latency.
        async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            Ok((member_index, cycle))
        }
    }

    fn on_append_pop(&mut self) -> (usize, SliceRx) {
        let heap::AppendEntry {
            priority,
            adjusted_clock,
            member_index,
        } = self.append_heap.pop().unwrap();

        let (append, rx) = self.slice_appends[member_index]
            .take()
            .expect("slice_appends must be Some for a heap entry");

        gazette::delta::decode(
            &mut self.slice_prev_journal[member_index],
            append.journal_name_truncate_delta,
            &append.journal_name_suffix,
        );

        tracing::trace!(
            member_index,
            journal = self.slice_prev_journal[member_index],
            priority,
            ?adjusted_clock,
            doc_bytes = append.doc_archived.len(),
            "drained Append from heap"
        );

        (member_index, rx)
    }
}

// Helper which builds a future that yields the next request from a member's Log RPC.
async fn next_log_rx(
    (member_index, mut rx): (usize, SliceRx),
) -> (
    usize,                                      // Member index.
    Option<tonic::Result<shuffle::LogRequest>>, // Request.
    SliceRx,                                    // Stream.
) {
    (member_index, rx.next().await, rx)
}
