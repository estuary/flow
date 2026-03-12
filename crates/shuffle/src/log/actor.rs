use super::Lsn;
use super::heap::{self, AppendHeap};
use super::state::{BlockState, FlushState};
use super::writer::{SealedSegment, Writer};
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
///
/// Additionally, the total on-disk backlog of sealed segments is tracked. When it
/// exceeds `disk_backlog_threshold`, heap draining is paused, propagating back-pressure
/// all the way to Slice journal reads.
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
    /// Log segment writer. `None` while a background flush is in-flight
    /// (the Writer has been moved into a `spawn_blocking` task).
    pub writer: Option<Writer>,
    /// Block accumulation state: journals, producers, entries, byte tracking.
    pub block: BlockState,
    /// Flush lifecycle state: pending requests, in-flight tracking, completed responses.
    pub flush: FlushState,
}

impl LogActor {
    #[tracing::instrument(
        level = "debug",
        ret,
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
        let mut pending_slice_rx: futures::stream::FuturesUnordered<_> = log_request_rx
            .into_iter()
            .enumerate()
            .map(next_log_rx)
            .collect();

        // Handle for a single in-flight background flush.
        let mut flush_handle: Option<
            tokio::task::JoinHandle<anyhow::Result<(Writer, Lsn, Option<SealedSegment>)>>,
        > = None;
        // Per-sealed-segment streams that drive compression and track unlink.
        // Each stream yields negative size deltas as disk space is freed.
        let mut sealed_segments = futures::stream::SelectAll::new();

        // Aggregate on-disk bytes across all living sealed segments.
        let mut disk_backlog_bytes: u64 = 0;
        // Threshold at which we'll stop draining Append requests.
        let disk_backlog_threshold = self.topology.disk_backlog_threshold;
        // Hysteresis flag: engaged at disk_backlog_threshold, released at 50%.
        let mut disk_back_pressure = false;

        let mut loop_count: u64 = 0;
        loop {
            loop_count += 1;

            // We may buffer a min-heap Append into the next block if we're
            // under cap and disk backlog back-pressure is not active.
            let may_buffer =
                !self.append_heap.is_empty() && !self.block.is_full() && !disk_back_pressure;

            // We may begin a non-empty block flush if one isn't underway, and either:
            // - We've been asked to flush by a slice, OR
            // - The block has reached capacity.
            // Invariant: pending flushes always have a non-empty block to flush,
            // because on_flush immediately resolves when the block is empty.
            let may_flush = !self.flush.flush_in_flight()
                && !self.block.is_empty()
                && (self.flush.has_pending_flush() || self.block.is_full());

            tracing::debug!(
                loop_count,
                append_heap = self.append_heap.len(),
                block = ?self.block,
                disk_backlog_mib = disk_backlog_bytes / (1024 * 1024),
                flush = ?self.flush,
                flushing = flush_handle.is_some(),
                may_buffer,
                may_flush,
                pending_slice_rx = pending_slice_rx.len(),
                "LogActor::serve iteration"
            );

            // First, attempt non-blocking sends of completed Flushed responses.
            let wake_log_response_tx = self.try_log_response_tx()?;

            tokio::select! {
                // Arms have a deliberate ordering designed to service IO first
                // (reads, then writes), and to encourage larger block aggregation.
                biased;

                // Read a ready LogRequest from pending slices.
                Some((member_index, log_request, rx)) = pending_slice_rx.next() => {
                    match self.on_log_request(member_index, log_request, rx)? {
                        Ok(rx) => pending_slice_rx.push(next_log_rx((member_index, rx))),
                        Err(true) => {}, // `rx` is parked in self.slice_appends
                        Err(false) => {
                            let remaining = pending_slice_rx.len() + self.append_heap.len();
                            tracing::debug!(member_index, remaining, "Slice LogRequest stream EOF");

                            if remaining != 0 {
                                continue;
                            }
                            // Drop SealedSegments: the Stream now yields None, which
                            // allows the select! `else` arm to fire and break the loop.
                            sealed_segments.clear();
                        }
                    }
                }

                // Read the completion of an in-flight flush.
                Some(result) = futures::future::OptionFuture::from(flush_handle.as_mut()) => {
                    flush_handle = None;

                    let result = match result {
                        Ok(r) => r,
                        Err(err) if err.is_cancelled() => continue,
                        Err(err) => std::panic::resume_unwind(err.into_panic()),
                    };

                    let (writer, flushed_lsn, sealed) = result?;
                    self.writer = Some(writer);
                    self.flush.on_flushed(flushed_lsn);

                    // Did the flush seal its segment (the writer rolled to the next)?
                    let Some(sealed) = sealed else {
                        continue;
                    };
                    disk_backlog_bytes += sealed.size;

                    if disk_backlog_bytes >= disk_backlog_threshold {
                        tracing::debug!(
                            disk_backlog_mib = disk_backlog_bytes / (1024 * 1024),
                            "disk back-pressure engaged"
                        );
                        disk_back_pressure = true;
                    }
                    sealed_segments.push(Box::pin(sealed.serve()));
                }

                // Read an update of a sealed segment, reclaiming disk from compression or unlink.
                Some(reclaimed) = sealed_segments.next() => {
                    disk_backlog_bytes = disk_backlog_bytes
                        .checked_sub(reclaimed?)
                        .expect("disk_backlog_bytes underflow");

                    if disk_back_pressure
                        && disk_backlog_bytes < disk_backlog_threshold / 2
                    {
                        tracing::debug!(
                            disk_backlog_mib = disk_backlog_bytes / (1024 * 1024),
                            "disk back-pressure released"
                        );
                        disk_back_pressure = false;
                    }
                }

                // Wake when a blocked log_response_tx has capacity.
                true = wake_log_response_tx => {}

                // Drain a ready entry from the heap into the buffering block.
                true = std::future::ready(may_buffer) => {
                    let pending = self.on_append_pop();
                    pending_slice_rx.push(next_log_rx(pending));
                }

                // Start a flush of the buffering block.
                true = std::future::ready(may_flush) => {
                    self.start_flush(&mut flush_handle);
                }

                // All slices EOF'd, heap drained, IO complete, and flushes sent.
                // No pending flushes can remain: they imply a non-empty block,
                // which would have triggered may_flush above.
                else => break,
            }
        }

        tracing::debug!(loop_count, "LogActor::serve exiting, all slices EOF");
        Ok(())
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
        while let Some(&(member_index, cycle, flushed_lsn)) = self.flush.peek_pending_flushed() {
            let tx = &self.log_response_tx[member_index];

            let Ok(permit) = tx.try_reserve() else {
                return Ok(future::Either::Left(tx.clone().reserve_owned().map(ok)));
            };
            self.flush.pop_pending_flushed();

            permit.send(Ok(shuffle::LogResponse {
                flushed: Some(shuffle::log_response::Flushed {
                    cycle,
                    flushed_lsn: flushed_lsn.as_u64(),
                }),
                ..Default::default()
            }));
            tracing::debug!(
                member_index,
                cycle,
                ?flushed_lsn,
                "sent Flushed response to Slice"
            );
        }

        Ok(idle)
    }

    /// Handle a ready slice: verify the request and dispatch to on_append or on_flush.
    ///
    /// Returns:
    ///  - Some(rx) on Flush (`rx` is re-pushed immediately)
    ///  - Err(true) when `rx` (and a read Append) are parked in `slice_appends`.
    ///  - Err(false) on clean EOF from the Slice RPC.
    fn on_log_request(
        &mut self,
        member_index: usize,
        log_request: Option<tonic::Result<shuffle::LogRequest>>,
        rx: SliceRx,
    ) -> anyhow::Result<Result<SliceRx, bool>> {
        let Some(log_request) = log_request else {
            return Ok(Err(false)); // Clean EOF of this member Slice's Log RPC.
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
                Ok(Err(true))
            }

            shuffle::LogRequest {
                flush: Some(flush), ..
            } => {
                let shuffle::log_request::Flush { cycle } = flush;
                let empty = self.block.is_empty();
                tracing::debug!(member_index, cycle, empty, "received Flush from Slice");
                self.flush.on_flush(member_index, cycle, empty);
                Ok(Ok(rx))
            }

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

    /// Pop the top append from the heap and accumulate it into the current block.
    fn on_append_pop(&mut self) -> (usize, SliceRx) {
        let heap::AppendEntry {
            priority,
            adjusted_clock,
            member_index,
        } = self.append_heap.pop().unwrap();

        let (append, rx) = self.slice_appends[member_index]
            .take()
            .expect("slice_appends must be Some for a heap entry");

        // Delta-decode the journal name.
        gazette::delta::decode(
            &mut self.slice_prev_journal[member_index],
            append.journal_name_truncate_delta,
            &append.journal_name_suffix,
        );

        let journal = &self.slice_prev_journal[member_index];
        let producer = uuid::Producer::from_i64(append.producer);
        self.block.accumulate(journal, producer, &append);

        tracing::trace!(
            member_index,
            journal,
            priority,
            ?producer,
            ?adjusted_clock,
            doc_bytes = append.doc_archived.len(),
            "drained Append from heap"
        );

        (member_index, rx)
    }

    /// Move the writer and accumulated block state into a background blocking
    /// task that encodes and writes the block.
    fn start_flush(
        &mut self,
        flush_handle: &mut Option<
            tokio::task::JoinHandle<anyhow::Result<(Writer, super::Lsn, Option<SealedSegment>)>>,
        >,
    ) {
        self.flush.start_flush();

        let mut writer = self
            .writer
            .take()
            .expect("writer must be present when no flush is in-flight");

        let (journals, producers, entries) = self.block.take();

        *flush_handle = Some(tokio::task::spawn_blocking(move || {
            let (flushed_lsn, sealed) = writer.append_block(journals, producers, entries)?;
            Ok((writer, flushed_lsn, sealed))
        }));
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
