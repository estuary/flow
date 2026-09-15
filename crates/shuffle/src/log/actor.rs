use super::Lsn;
use super::heap::{self, AppendHeap};
use super::state::{BackPressureState, BlockState, FlushState};
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
/// exceeds `shuffle_disk_limit_bytes`, heap draining is paused, propagating back-pressure
/// all the way to Slice journal reads.
pub struct LogActor {
    /// Immutable session topology: identity and shard configuration.
    pub topology: super::state::Topology,
    /// Per-Slice response channel for sending Opened, Flushed, and DiskBackPressure responses.
    pub log_response_tx: Vec<mpsc::Sender<tonic::Result<shuffle::LogResponse>>>,
    /// Ready Append and receive stream for each Slice, set when an Append is
    /// received and consumed when the corresponding heap entry is popped.
    /// None while the slice's next read is pending in `pending_slices`.
    pub slice_appends: Vec<Option<(shuffle::log_request::Append, SliceRx)>>,
    /// Previous journal name received from each Slice shard, for delta decoding.
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
    /// Disk back-pressure state: backlog measure, engaged flag, per-Slice notifications.
    pub back_pressure: BackPressureState,
    /// Per-task metrics counters and gauges.
    pub metrics: super::Metrics,
}

impl LogActor {
    #[tracing::instrument(
        level = "debug",
        ret,
        err(Debug, level = "warn"),
        skip_all,
        fields(
            session = self.topology.session_id,
            shard_id = %self.topology.shards[self.topology.log_shard_index as usize].id,
        )
    )]
    pub async fn serve(
        mut self,
        log_request_rx: Vec<BoxStream<'static, tonic::Result<shuffle::LogRequest>>>,
    ) -> anyhow::Result<()> {
        // Number of still-connected Slice RPC shards.
        let mut connected = log_request_rx.len();
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

        let mut ticker = tokio::time::interval(crate::ACTOR_TICKER_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let mut loop_count: u64 = 0;
        loop {
            loop_count += 1;

            // We may buffer a min-heap Append into the next block if we're
            // under cap and disk backlog back-pressure is not active.
            let may_buffer = !self.append_heap.is_empty()
                && !self.block.is_full()
                && !self.back_pressure.engaged();

            // We may begin a non-empty block flush if one isn't underway, and either:
            // - We've been asked to flush by a slice, OR
            // - The block has reached capacity.
            // Note that we immediately reply to a Flush request if our current
            // block is empty (using the LSN of the last-completed flush).
            let may_flush = !self.flush.flush_in_flight()
                && !self.block.is_empty()
                && (self.flush.has_pending_request() || self.block.is_full());

            tracing::trace!(
                loop_count,
                append_heap = self.append_heap.len(),
                block = ?self.block,
                connected,
                disk_backlog_mib = self.back_pressure.backlog_bytes() / (1024 * 1024),
                flush = ?self.flush,
                flushing = flush_handle.is_some(),
                may_buffer,
                may_flush,
                pending_slice_rx = pending_slice_rx.len(),
                "LogActor::serve iteration"
            );

            // First, attempt non-blocking sends of pending responses.
            let wake_log_response_tx = self.try_log_response_tx()?;

            tokio::select! {
                // Arms have a deliberate ordering designed to service IO first
                // (reads, then writes), and to encourage larger block aggregation.
                biased;

                // Read a ready LogRequest from pending slices.
                Some((shard_index, log_request, rx)) = pending_slice_rx.next() => {
                    match self.on_log_request(shard_index, log_request, rx)? {
                        Ok(rx) => pending_slice_rx.push(next_log_rx((shard_index, rx))),
                        Err(true) => {}, // `rx` is parked in self.slice_appends
                        Err(false) => {
                            connected -= 1;

                            service_kit::event!(
                                tracing::Level::DEBUG,
                                "slice",
                                shard_index,
                                connected,
                                "received EOF from Slice"
                            );
                        }
                    }
                }

                // Read the completion of an in-flight flush.
                Some(result) = futures::future::OptionFuture::from(flush_handle.as_mut()) => {
                    flush_handle = None;

                    let (writer, flushed_lsn, sealed) = match result {
                        Ok(r) => r?,
                        Err(err) if err.is_cancelled() => continue,
                        Err(err) => std::panic::resume_unwind(err.into_panic()),
                    };

                    self.on_flushed(writer, flushed_lsn, sealed.as_ref());
                    if let Some(sealed) = sealed {
                        sealed_segments.push(Box::pin(sealed.serve()));
                    }
                }

                // Read an update reclaiming disk from compression or unlink.
                // This arm is deactivated if no `connected` shards remain,
                // to allow the `else` arm below to fire and exit.
                Some(reclaimed) = sealed_segments.next(), if connected != 0 => {
                    self.on_reclaimed(reclaimed?);
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

                // Periodic tick ensures tracing fires even when idle.
                // Guarded like sealed_segments to allow the `else` arm to fire
                // when all slices have disconnected.
                _ = ticker.tick(), if connected != 0 => {
                    // The exporter evicts metrics idle for 10m, and a Log wedged
                    // by back-pressure neither seals nor reclaims, so without
                    // this the series vanishes on a stalled task.
                    self.metrics.disk_backlog_bytes.set(self.back_pressure.backlog_bytes() as f64);
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

    /// Try to send pending responses. If a channel is full, return
    /// a wake future that resolves when capacity is available.
    fn try_log_response_tx(&mut self) -> anyhow::Result<impl Future<Output = bool> + 'static> {
        // Closure for mapping an OwnedPermit Result to Ok (our "poll again" signal).
        // On Err (channel closed), we don't wake and rely on rx of a causal error / fail-fast teardown.
        let ok = |result: Result<_, _>| result.is_ok();
        // Future which represent an absence of an awake signal.
        let idle = future::Either::Right(std::future::ready(false));

        // Either loop may head-of-line block on a full channel.
        while let Some(shard_index) = self.back_pressure.pending_slice() {
            let tx = &self.log_response_tx[shard_index];

            let Ok(permit) = tx.try_reserve() else {
                return Ok(future::Either::Left(tx.clone().reserve_owned().map(ok)));
            };
            let engaged = self.back_pressure.on_sent(shard_index);

            permit.send(Ok(shuffle::LogResponse {
                disk_back_pressure: Some(shuffle::log_response::DiskBackPressure { engaged }),
                ..Default::default()
            }));

            service_kit::event!(
                tracing::Level::DEBUG,
                "slice",
                shard_index,
                engaged,
                "sent DiskBackPressure response to Slice",
            );
        }

        while let Some(&(shard_index, cycle, flushed_lsn)) = self.flush.peek_pending_response() {
            let tx = &self.log_response_tx[shard_index];

            let Ok(permit) = tx.try_reserve() else {
                return Ok(future::Either::Left(tx.clone().reserve_owned().map(ok)));
            };
            self.flush.pop_pending_response();

            permit.send(Ok(shuffle::LogResponse {
                flushed: Some(shuffle::log_response::Flushed {
                    cycle,
                    flushed_lsn: flushed_lsn.as_u64(),
                }),
                ..Default::default()
            }));

            service_kit::event!(
                tracing::Level::DEBUG,
                "slice",
                shard_index,
                cycle,
                flushed_lsn = flushed_lsn.as_u64(),
                "sent Flushed response to Slice",
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
        shard_index: usize,
        log_request: Option<tonic::Result<shuffle::LogRequest>>,
        rx: SliceRx,
    ) -> anyhow::Result<Result<SliceRx, bool>> {
        let Some(log_request) = log_request else {
            return Ok(Err(false)); // Clean EOF of this shard's Slice Log RPC.
        };

        let verify = proto_grpc::verify(
            "LogRequest",
            "Append or Flush",
            &self.topology.shards[shard_index].endpoint,
        );
        let log_request = verify.ok(log_request)?;

        match log_request {
            shuffle::LogRequest {
                append: Some(append),
                ..
            } => {
                self.on_append_rx(append, shard_index, rx);
                Ok(Err(true))
            }

            shuffle::LogRequest {
                flush: Some(flush), ..
            } => {
                let shuffle::log_request::Flush { cycle } = flush;
                let empty = self.block.is_empty();
                self.flush.on_flush(shard_index, cycle, empty);

                service_kit::event!(
                    tracing::Level::DEBUG,
                    "slice",
                    shard_index,
                    cycle,
                    empty,
                    "received Flush from Slice",
                );
                Ok(Ok(rx))
            }

            request => Err(verify.fail_msg(request)),
        }
    }

    fn on_append_rx(
        &mut self,
        append: shuffle::log_request::Append,
        shard_index: usize,
        rx: SliceRx,
    ) {
        let priority = append.priority;
        let clock = uuid::Clock::from_u64(append.clock);
        let adjusted_clock = clock + uuid::Clock::from_u64(append.read_delay);

        tracing::trace!(
            shard_index,
            priority,
            ?adjusted_clock,
            doc_bytes = append.doc_archived.len(),
            "received Append from Slice"
        );

        debug_assert!(self.slice_appends[shard_index].is_none());
        self.slice_appends[shard_index] = Some((append, rx));

        self.append_heap.push(heap::AppendEntry {
            priority,
            adjusted_clock,
            shard_index,
        });
    }

    /// Pop the top append from the heap and accumulate it into the current block.
    fn on_append_pop(&mut self) -> (usize, SliceRx) {
        let heap::AppendEntry {
            priority,
            adjusted_clock,
            shard_index,
        } = self.append_heap.pop().unwrap();

        let (append, rx) = self.slice_appends[shard_index]
            .take()
            .expect("slice_appends must be Some for a heap entry");

        // Delta-decode the journal name.
        gazette::delta::decode(
            &mut self.slice_prev_journal[shard_index],
            append.journal_name_truncate_delta,
            &append.journal_name_suffix,
        );

        let journal = &self.slice_prev_journal[shard_index];
        let producer = uuid::Producer::from_i64(append.producer);
        self.block.accumulate(journal, producer, &append);

        tracing::trace!(
            shard_index,
            journal,
            priority,
            ?producer,
            ?adjusted_clock,
            doc_bytes = append.doc_archived.len(),
            "drained Append from heap"
        );
        self.metrics.appends.increment(1);
        self.metrics
            .bytes_appended
            .increment(append.source_byte_length as u64);

        (shard_index, rx)
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

        service_kit::event!(
            tracing::Level::DEBUG,
            "writer",
            journals = journals.len(),
            producers = producers.len(),
            entries = entries.len(),
            "starting block flush"
        );
        self.metrics.flushes.increment(1);

        *flush_handle = Some(tokio::task::spawn_blocking(move || {
            let (flushed_lsn, sealed) = writer.append_block(journals, producers, entries)?;
            Ok((writer, flushed_lsn, sealed))
        }));
    }

    /// Handle the completion of a background block flush: restore the writer,
    /// advance flush state, and bookkeep the disk-backlog measure (engaging
    /// back-pressure if a new segment was sealed).
    fn on_flushed(&mut self, writer: Writer, flushed_lsn: Lsn, sealed: Option<&SealedSegment>) {
        self.writer = Some(writer);
        self.flush.on_flushed(flushed_lsn);

        // Did the flush seal its segment (the writer rolled to the next)?
        let Some(sealed) = sealed else {
            service_kit::event!(
                tracing::Level::TRACE,
                "writer",
                disk_back_pressure = self.back_pressure.engaged(),
                disk_backlog_mib = self.back_pressure.backlog_bytes() / (1024 * 1024),
                next_pending = self.flush.has_pending_request(),
                "log segment flushed (partial segment)"
            );
            return;
        };

        self.back_pressure
            .on_sealed(sealed.size, self.topology.shuffle_disk_limit_bytes);

        service_kit::event!(
            tracing::Level::DEBUG,
            "writer",
            disk_back_pressure = self.back_pressure.engaged(),
            disk_backlog_mib = self.back_pressure.backlog_bytes() / (1024 * 1024),
            last_segment = service_kit::event::debug(sealed.path.to_owned()),
            next_pending = self.flush.has_pending_request(),
            sealed_mib = sealed.size / (1024 * 1024),
            "log segment flushed (segment sealed)"
        );
        self.metrics.segments_sealed.increment(1);
        self.metrics
            .disk_backlog_bytes
            .set(self.back_pressure.backlog_bytes() as f64);
    }

    /// Handle a disk-space reclaim from a sealed segment's compress / unlink
    /// stream: subtract from the backlog measure and release back-pressure
    /// at the hysteresis threshold (half of `shuffle_disk_limit_bytes`).
    fn on_reclaimed(&mut self, reclaimed: u64) {
        self.back_pressure
            .on_reclaimed(reclaimed, self.topology.shuffle_disk_limit_bytes);

        service_kit::event!(
            tracing::Level::DEBUG,
            "writer",
            disk_back_pressure = self.back_pressure.engaged(),
            disk_backlog_mib = self.back_pressure.backlog_bytes() / (1024 * 1024),
            reclaimed_mib = reclaimed / (1024 * 1024),
            "log segment reclaimed",
        );
        self.metrics
            .disk_backlog_bytes
            .set(self.back_pressure.backlog_bytes() as f64);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    const LIMIT: u64 = 1024;

    fn test_actor(
        dir: &std::path::Path,
    ) -> (
        LogActor,
        Vec<mpsc::Receiver<tonic::Result<shuffle::LogResponse>>>,
    ) {
        let shards = crate::testing::test_shards_3();
        let shard_count = shards.len();

        let (log_response_tx, log_response_rx): (Vec<_>, Vec<_>) = (0..shard_count)
            .map(|_| crate::new_channel::<tonic::Result<shuffle::LogResponse>>())
            .unzip();

        let metrics = super::super::Metrics::new(&shards[0].id);

        let actor = LogActor {
            topology: super::super::state::Topology {
                session_id: 1,
                shards,
                log_shard_index: 0,
                shuffle_disk_limit_bytes: LIMIT,
            },
            append_heap: super::super::heap::AppendHeap::new(),
            slice_prev_journal: vec![String::new(); shard_count],
            slice_appends: std::iter::repeat_with(|| None).take(shard_count).collect(),
            writer: Some(Writer::new(dir, 0).unwrap()),
            block: BlockState::new(),
            flush: FlushState::new(),
            back_pressure: BackPressureState::new(shard_count),
            log_response_tx,
            metrics,
        };
        (actor, log_response_rx)
    }

    fn seal(actor: &mut LogActor, dir: &std::path::Path, size: u64) {
        let writer = actor.writer.take().unwrap();
        actor.flush.start_flush();

        actor.on_flushed(
            writer,
            Lsn::new(1, 0),
            Some(&SealedSegment::new(dir.join("sealed-segment"), size)),
        );
    }

    fn drain(
        actor: &mut LogActor,
        rx: &mut [mpsc::Receiver<tonic::Result<shuffle::LogResponse>>],
    ) -> Vec<(usize, bool)> {
        _ = actor.try_log_response_tx().unwrap();

        let mut out = Vec::new();
        for (shard_index, rx) in rx.iter_mut().enumerate() {
            while let Ok(response) = rx.try_recv() {
                match response.unwrap() {
                    shuffle::LogResponse {
                        disk_back_pressure:
                            Some(shuffle::log_response::DiskBackPressure { engaged }),
                        ..
                    } => out.push((shard_index, engaged)),
                    response => panic!("unexpected LogResponse: {response:?}"),
                }
            }
        }
        out
    }

    #[test]
    fn test_disk_back_pressure_broadcast() {
        enum Step {
            Seal(u64),
            Reclaim(u64),
        }
        use Step::*;

        let cases: &[(&str, &[Step])] = &[
            ("sealed under the limit", &[Seal(LIMIT / 2)]),
            ("sealed at the limit", &[Seal(LIMIT / 2)]),
            ("sealed while engaged", &[Seal(LIMIT)]),
            ("reclaimed to the limit", &[Reclaim(LIMIT)]),
            ("reclaimed to the hysteresis floor", &[Reclaim(LIMIT / 2)]),
            (
                "reclaimed below the hysteresis floor",
                &[Reclaim(LIMIT / 2)],
            ),
            ("reclaimed while released", &[Reclaim(0)]),
            ("sealed at the limit anew", &[Seal(LIMIT)]),
            (
                "released and re-engaged before a drain",
                &[Reclaim(LIMIT), Seal(LIMIT)],
            ),
            (
                "reclaimed below the hysteresis floor anew",
                &[Reclaim(LIMIT)],
            ),
            (
                "engaged and released before a drain",
                &[Seal(LIMIT), Reclaim(LIMIT)],
            ),
        ];

        let dir = tempfile::tempdir().unwrap();
        let (mut actor, mut rx) = test_actor(dir.path());

        let phases: Vec<_> = cases
            .iter()
            .map(|(label, steps)| {
                for step in *steps {
                    match step {
                        Seal(size) => seal(&mut actor, dir.path(), *size),
                        Reclaim(size) => actor.on_reclaimed(*size),
                    }
                }
                (
                    *label,
                    actor.back_pressure.engaged(),
                    drain(&mut actor, &mut rx),
                )
            })
            .collect();

        insta::assert_debug_snapshot!("disk_back_pressure_flips", phases);
    }
}

// Helper which builds a future that yields the next request from a shard's Log RPC.
async fn next_log_rx(
    (shard_index, mut rx): (usize, SliceRx),
) -> (
    usize,                                      // Shard index.
    Option<tonic::Result<shuffle::LogRequest>>, // Request.
    SliceRx,                                    // Stream.
) {
    (shard_index, rx.next().await, rx)
}
