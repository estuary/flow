use super::heap::{self, EnqueueHeap};
use futures::{FutureExt, StreamExt, future, stream::BoxStream};
use proto_flow::shuffle;
use proto_gazette::uuid;
use std::future::Future;
use tokio::sync::mpsc;

type SliceRx = BoxStream<'static, tonic::Result<shuffle::QueueRequest>>;

/// QueueActor implements the main event loop of a shuffle Queue RPC.
///
/// Notes on back-pressure: when a Slice sends an Enqueue, its rx stream is
/// parked in `slice_enqueues` until the heap pops that entry (lowest priority
/// in the select loop). Given a parked rx stream, and knowing that a Queue RPC
/// is constrained by the HTTP/2 stream flow-control window (64KB), this means
/// that the Slice is itself back-pressured, and will in-turn back-pressure to
/// its journal reads (the journal Read RPC sits idle in a SliceActor's ReadyRead).
///
/// This creates the conditions for system-wide priority enforcement:
/// QueueActors drain high-priority, earlier-clock documents first, which back-pressures
/// to Slices and journals that are producing lower-priority or higher-clock documents.
/// The overall rate of progress is bounded by the write throughput of the slowest Queue.
pub struct QueueActor {
    /// Immutable session topology: identity and member configuration.
    pub topology: super::state::Topology,
    /// Per-Slice response channel for sending Opened and Flushed responses.
    pub queue_response_tx: Vec<mpsc::Sender<tonic::Result<shuffle::QueueResponse>>>,
    /// Ready Enqueue and receive stream for each Slice, set when an Enqueue is
    /// received and consumed when the corresponding heap entry is popped.
    /// None while the slice's next read is pending in `pending_slices`.
    pub slice_enqueues: Vec<Option<(shuffle::queue_request::Enqueue, SliceRx)>>,
    /// Previous journal name received from each Slice member, for delta decoding.
    pub slice_prev_journal: Vec<String>,
    /// Ordered heap of references to Some `slice_enqueues` items.
    pub enqueue_heap: EnqueueHeap,
    /// Completed flush IOs awaiting send of their Flushed response.
    pub pending_flushed: Vec<(usize, u64)>,
}

impl QueueActor {
    #[tracing::instrument(
        level = "debug",
        err(Debug, level = "warn"),
        skip_all,
        fields(
            session = self.topology.session_id,
            member = self.topology.queue_member_index,
        )
    )]
    pub async fn serve(
        mut self,
        queue_request_rx: Vec<BoxStream<'static, tonic::Result<shuffle::QueueRequest>>>,
    ) -> anyhow::Result<()> {
        // Build rx futures for the next QueueRequest from each Slice.
        let mut pending_slices: futures::stream::FuturesUnordered<_> = queue_request_rx
            .into_iter()
            .enumerate()
            .map(next_queue_rx)
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
                heap_size = self.enqueue_heap.len(),
                "QueueActor::serve iteration"
            );

            // First, attempt non-blocking sends of completed Flushed responses.
            let wake_queue_response_tx = self.try_queue_response_tx()?;

            tokio::select! {
                biased;

                // First priority: read a ready QueueRequest from pending slices.
                Some((member_index, queue_request, rx)) = pending_slices.next() => {
                    if let Some((rx, flush_fut)) = self.on_queue_request(member_index, queue_request, rx)? {
                        pending_slices.push(next_queue_rx((member_index, rx)));
                        pending_io.push(flush_fut);
                    }
                }

                // Second priority: complete a pending IO, queuing the Flushed for send.
                Some(result) = pending_io.next() => {
                    let (member_index, seq) = result?;
                    tracing::debug!(member_index, seq, "flush IO completed");
                    self.pending_flushed.push((member_index, seq));
                }

                // Third priority: wake when a blocked queue_response_tx has capacity.
                true = wake_queue_response_tx => {}

                // Fourth priority: drain one entry from the heap.
                _ = std::future::ready(()), if !self.enqueue_heap.is_empty() => {
                    let pending = self.on_enqueue_pop();
                    pending_slices.push(next_queue_rx(pending));
                }

                // All slices EOF'd, heap drained, IO complete, and flushes sent.
                else => {
                    tracing::debug!(loop_count, "QueueActor::serve exiting, all slices EOF");
                    break Ok(());
                }
            }
        }
    }

    /// Try to send completed Flushed responses. If a channel is full, return
    /// a wake future that resolves when capacity is available.
    fn try_queue_response_tx(&mut self) -> anyhow::Result<impl Future<Output = bool> + 'static> {
        // Closure for mapping an OwnedPermit Result to Ok (our "poll again" signal).
        // On Err (channel closed), we don't wake and rely on rx of a causal error / fail-fast teardown.
        let ok = |result: Result<_, _>| result.is_ok();
        // Future which represent an absence of an awake signal.
        let idle = future::Either::Right(std::future::ready(false));

        // This loop may head-of-line block if we're unable to send a LIFO Flushed.
        // We accept this property for implementation simplicity.
        while let Some(&(member_index, seq)) = self.pending_flushed.last() {
            let tx = &self.queue_response_tx[member_index];

            let Ok(permit) = tx.try_reserve() else {
                return Ok(future::Either::Left(tx.clone().reserve_owned().map(ok)));
            };
            self.pending_flushed.pop();

            permit.send(Ok(shuffle::QueueResponse {
                flushed: Some(shuffle::queue_response::Flushed { seq }),
                ..Default::default()
            }));
            tracing::debug!(member_index, seq, "sent Flushed response to Slice");
        }

        Ok(idle)
    }

    /// Handle a ready slice: verify the request and dispatch to on_enqueue or on_flush.
    ///
    /// Returns `None` on clean EOF or when the Enqueue and rx are parked in
    /// `slice_ready`. Returns `Some` with the stream to re-push and the
    /// `(member_index, seq)` flush IO to start.
    fn on_queue_request(
        &mut self,
        member_index: usize,
        queue_request: Option<tonic::Result<shuffle::QueueRequest>>,
        rx: SliceRx,
    ) -> anyhow::Result<
        Option<(
            SliceRx,
            impl Future<Output = anyhow::Result<(usize, u64)>> + 'static,
        )>,
    > {
        let Some(queue_request) = queue_request else {
            return Ok(None); // Clean EOF of this member Slice's Queue RPC.
        };

        let verify = crate::verify(
            "QueueRequest",
            "Enqueue or Flush",
            &self.topology.members[member_index].endpoint,
            member_index,
        );
        let queue_request = verify.ok(queue_request)?;

        match queue_request {
            shuffle::QueueRequest {
                enqueue: Some(enqueue),
                ..
            } => {
                self.on_enqueue_rx(enqueue, member_index, rx);
                Ok(None)
            }

            shuffle::QueueRequest {
                flush: Some(flush), ..
            } => Ok(Some((rx, self.on_flush(flush, member_index)))),

            request => Err(verify.fail(request)),
        }
    }

    fn on_enqueue_rx(
        &mut self,
        enqueue: shuffle::queue_request::Enqueue,
        member_index: usize,
        rx: SliceRx,
    ) {
        let priority = enqueue.priority;
        let clock = uuid::Clock::from_u64(enqueue.clock);
        let adjusted_clock = clock + uuid::Clock::from_u64(enqueue.read_delay);

        tracing::trace!(
            member_index,
            priority,
            ?adjusted_clock,
            doc_bytes = enqueue.doc_archived.len(),
            "received Enqueue from Slice"
        );

        debug_assert!(self.slice_enqueues[member_index].is_none());
        self.slice_enqueues[member_index] = Some((enqueue, rx));

        self.enqueue_heap.push(heap::EnqueueEntry {
            priority,
            adjusted_clock,
            member_index,
        });
    }

    fn on_flush(
        &mut self,
        flush: shuffle::queue_request::Flush,
        member_index: usize,
    ) -> impl Future<Output = anyhow::Result<(usize, u64)>> + 'static {
        let shuffle::queue_request::Flush { seq } = flush;

        tracing::debug!(member_index, seq, "received Flush from Slice");

        // Emulate disk IO latency.
        async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            Ok((member_index, seq))
        }
    }

    fn on_enqueue_pop(&mut self) -> (usize, SliceRx) {
        let heap::EnqueueEntry {
            priority,
            adjusted_clock,
            member_index,
        } = self.enqueue_heap.pop().unwrap();

        let (enqueue, rx) = self.slice_enqueues[member_index]
            .take()
            .expect("slice_enqueues must be Some for a heap entry");

        gazette::delta::decode(
            &mut self.slice_prev_journal[member_index],
            enqueue.journal_name_truncate_delta,
            &enqueue.journal_name_suffix,
        );

        tracing::trace!(
            member_index,
            journal = self.slice_prev_journal[member_index],
            priority,
            ?adjusted_clock,
            doc_bytes = enqueue.doc_archived.len(),
            "drained Enqueue from heap"
        );

        (member_index, rx)
    }
}

// Helper which builds a future that yields the next request from a member's Queue RPC.
async fn next_queue_rx(
    (member_index, mut rx): (usize, SliceRx),
) -> (
    usize,                                        // Member index.
    Option<tonic::Result<shuffle::QueueRequest>>, // Request.
    SliceRx,                                      // Stream.
) {
    (member_index, rx.next().await, rx)
}
