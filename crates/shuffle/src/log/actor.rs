use super::Lsn;
use super::buffer::{self, NextMerge, SliceBuffer};
use super::state::{BlockState, DiskState, FlushState};
use super::writer::{SealedSegment, Writer};
use anyhow::Context;
use futures::{StreamExt, stream::BoxStream};
use proto_flow::shuffle;
use proto_gazette::uuid;
use tokio::sync::{mpsc, watch};

type SliceRx = BoxStream<'static, tonic::Result<shuffle::LogRequest>>;
type FlushHandle = tokio::task::JoinHandle<anyhow::Result<(Writer, Lsn, Option<SealedSegment>)>>;

/// LogActor is the event loop of a shard's Log RPC, which every Slice joins.
/// It merges the Slices' Appends into blocks of the shard's on-disk log.
///
/// Each Slice's rounds are read as they arrive, and their Appends and closing
/// Barriers are buffered into its `SliceBuffer`. The merge takes the least next
/// entry of any Slice, provided no Slice's gate orders before it
/// (`buffer::next_merge`), in a tight loop (`merge_block`) until it must
/// wait, or until it yields after `MAX_MERGES`.
///
/// Blocks are written by a background flush, one at a time, while the merge
/// continues into the next block. A Slice's Barrier may request a flush at a
/// cycle number, answered by Flushed once the Barrier has merged and a flush
/// covers every Append which preceded it. A Slice has at most one flush request
/// outstanding, so its Flushed response never awaits channel capacity.
///
/// Back-pressure runs from the disk back to the Slices' journal reads.
/// The merge pauses while a full block awaits the flush already in flight,
/// or while the backlog of sealed segments on disk is over its limit
/// (`DiskState`). A paused merge merges no Barriers, so it releases no quota
/// (`acked_tx`). A Slice whose window of unreleased quota is full stops
/// reading its journals.
///
/// A running merge also releases quota in merge order: high-priority,
/// earlier-clock documents first. So back-pressure tends to fall on Slices
/// and journals with lower-priority or later documents. Every Slice sends
/// the same quota to every Log, and stops reading once any one Log is a full
/// window behind. So one slow Log holds back every Slice.
///
/// The merge is live: it can't deadlock. Apart from back-pressure, a Log's
/// merge waits only on a Slice which gates it: one with nothing queued here,
/// whose gate is strictly below every queued entry (`buffer::next_merge`).
/// That Slice sends its next round once it reads its next document, or finds
/// it has none due. Only a full window can hold it back.
///
/// If its window is full, another Log holds unmerged Barriers of the Slice.
/// Those Barriers and their Appends are at or below the Slice's gate, which
/// is the position of its last Barrier (`SliceBuffer::on_round`). The other
/// Log merges them, unless it too waits on a Slice which gates it. That
/// Slice's gate is strictly below them, and so strictly below the first gate.
///
/// Following these waits from Log to Slice to Log, gates strictly descend.
/// No Slice can appear twice, so the chain ends: at a Log which merges and
/// releases a window, or at a Slice whose window isn't full. Strict descent
/// is why an entry merges ahead of a gate at an equal position.
/// See "Log Merge and Output" of the crate README.
pub struct LogActor {
    /// Immutable session topology: identity and shard configuration.
    pub topology: super::state::Topology,
    /// Per-Slice response channel for sending Opened and Flushed responses.
    pub log_response_tx: Vec<mpsc::Sender<tonic::Result<shuffle::LogResponse>>>,
    /// Per-Slice cumulative quota of merged Barriers, which releases the
    /// Slice's window as `LogResponse.Acked` (see `crate::Service::spawn_log`).
    pub acked_tx: Vec<watch::Sender<u64>>,
    /// Buffer of received and unmerged rounds from each Slice.
    pub slices: Vec<SliceBuffer>,
    /// Log segment writer. `None` while a background flush is in-flight
    /// (the Writer has been moved into `flush_handle`).
    pub writer: Option<Writer>,
    /// Background task of an in-flight flush, which returns the Writer.
    pub flush_handle: Option<FlushHandle>,
    /// Block accumulation state: journals, producers, entries, byte tracking.
    pub block: BlockState,
    /// Flush accounting: started and completed blocks, and waiting requests.
    pub flush: FlushState,
    /// On-disk backlog of sealed segments, and its back-pressure of the merge.
    pub disk: DiskState,
    /// Per-task metrics counters and gauges.
    pub metrics: super::Metrics,
}

/// What the Log's merge must await before it may continue.
#[derive(Debug)]
enum Wait {
    /// Nothing is queued: await a next round from any Slice.
    Idle,
    /// `slice` gates the merge. Await its next round.
    Gated {
        #[allow(dead_code)]
        slice: usize, // Read by std::fmt::Debug.
    },
    /// The block is full and a flush is in flight: await its completion.
    Flushing,
    /// The disk backlog is over its limit: await reclaim of sealed segments.
    DiskBackPressure,
    /// Await nothing: resume merging after servicing actor events.
    Yield,
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
        // Number of still-connected Slice RPCs.
        let mut connected = log_request_rx.len();
        // Build rx futures for the next LogRequest from each Slice.
        let mut pending_slice_rx: futures::stream::FuturesUnordered<_> = log_request_rx
            .into_iter()
            .enumerate()
            .map(next_log_rx)
            .collect();
        // Per-sealed-segment streams that drive compression and track unlink.
        // Each stream yields negative size deltas as disk space is freed.
        let mut sealed_segments = futures::stream::SelectAll::new();

        let mut ticker = tokio::time::interval(crate::ACTOR_TICKER_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let mut loop_count: u64 = 0;
        loop {
            loop_count += 1;

            // First, merge into the block until we must wait.
            let wait = self.merge_block()?;

            // Release windows of Barriers merged by `merge_block`.
            for (slice, acked_tx) in self.slices.iter().zip(&self.acked_tx) {
                let released = slice.released();
                acked_tx.send_if_modified(|acked| std::mem::replace(acked, released) != released);
            }

            // merge_block() starts a flush only when its block is full,
            // to encourage larger blocks. Now that we've stalled,
            // also flush if requested to by a Slice.
            if self.flush.should_start(&self.block) {
                self.start_flush();
            }

            tracing::trace!(
                loop_count,
                ?wait,
                block = ?self.block,
                connected,
                disk = ?self.disk,
                flush = ?self.flush,
                pending_slice_rx = pending_slice_rx.len(),
                "LogActor::serve iteration"
            );

            // Build a select-arm Future that yields if Wait::Yield.
            let wake_yield = async move {
                match &wait {
                    Wait::Yield => {
                        tokio::task::yield_now().await;
                        true
                    }
                    _ => false,
                }
            };

            tokio::select! {
                biased;

                // Read a ready LogRequest from pending slices.
                Some((slice, log_request, rx)) = pending_slice_rx.next() => {
                    let Some(log_request) = log_request else {
                        // Clean EOF of this Slice's Log RPC.
                        connected -= 1;

                        service_kit::event!(
                            tracing::Level::DEBUG,
                            "slice",
                            slice,
                            connected,
                            "received EOF from Slice"
                        );
                        continue;
                    };
                    self.on_log_request(slice, log_request)?;
                    pending_slice_rx.push(next_log_rx((slice, rx)));
                }

                // Read the completion of an in-flight flush.
                Some(result) = futures::future::OptionFuture::from(self.flush_handle.as_mut()) => {
                    self.flush_handle = None;

                    let (writer, flushed_lsn, sealed) = match result {
                        Ok(r) => r?,
                        Err(err) if err.is_cancelled() => continue,
                        Err(err) => std::panic::resume_unwind(err.into_panic()),
                    };

                    self.on_flushed(writer, flushed_lsn, sealed.as_ref())?;
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

                // Periodic tick ensures tracing fires even when idle.
                // Guarded like sealed_segments to allow the `else` arm to fire
                // when all slices have disconnected.
                _ = ticker.tick(), if connected != 0 => {
                    // The exporter evicts metrics idle for 10m, and a Log wedged
                    // by back-pressure neither seals nor reclaims, so without
                    // this the series vanishes on a stalled task.
                    self.metrics.disk_backlog_bytes.set(self.disk.bytes as f64);
                }

                // Wake when a Wait::Yield has completed.
                true = wake_yield => {}

                // All Slices EOF'd and no flush is in flight.
                else => break,
            }
        }

        tracing::debug!(loop_count, "LogActor::serve exiting, all slices EOF");
        Ok(())
    }

    /// Merge into the block until the merge must wait, or has run for
    /// `MAX_MERGES`. A full block is flushed in-line,
    /// if a flush isn't already underway, and the merge continues.
    fn merge_block(&mut self) -> anyhow::Result<Wait> {
        let mut merges = 0;

        loop {
            // `slice` is the next Slice to merge, and `through` is the least
            // next Position of its peers, through which `slice` Appends may merge.
            let (slice, through) = match buffer::next_merge(&self.slices) {
                NextMerge::Idle => return Ok(Wait::Idle),
                NextMerge::Gated { slice } => return Ok(Wait::Gated { slice }),
                NextMerge::Ready { slice, through } => (slice, through),
            };

            // Merge a run of `slice` without re-evaluating `next_merge`.
            while self.slices[slice]
                .peek_position()
                .is_some_and(|position| position <= through)
            {
                if self.disk.back_pressure {
                    return Ok(Wait::DiskBackPressure);
                }
                if self.block.is_full() {
                    if self.flush.in_flight() {
                        return Ok(Wait::Flushing);
                    }
                    self.start_flush();
                }
                if merges == MAX_MERGES {
                    return Ok(Wait::Yield);
                }
                self.on_next_merge(slice)?;
                merges += 1;
            }
        }
    }

    /// Verify and apply a Slice's round to its SliceBuffer.
    fn on_log_request(
        &mut self,
        slice: usize,
        log_request: tonic::Result<shuffle::LogRequest>,
    ) -> anyhow::Result<()> {
        let (shard, priority) = self.topology.slice(slice);
        let verify = proto_grpc::verify(
            "LogRequest",
            "round of Appends and a Barrier",
            &shard.endpoint,
        );
        let log_request = verify.ok(log_request)?;

        let shuffle::LogRequest {
            open: None,
            appends,
            barrier: Some(barrier),
        } = log_request
        else {
            return Err(verify.fail_msg(log_request));
        };

        tracing::trace!(
            slice,
            appends = appends.len(),
            ?barrier,
            "received round from Slice"
        );
        self.metrics.rounds.increment(1);

        self.slices[slice]
            .on_round(appends, barrier)
            .with_context(|| format!("round of Slice {} with priority {priority}", shard.endpoint))
    }

    /// A Slice's Barrier requesting a flush at `cycle` has merged, along with
    /// all preceding Appends. Retain the request or, if the block is already
    /// flushed, send a Flushed response now.
    fn on_flush(&mut self, slice: usize, cycle: u64) -> anyhow::Result<()> {
        let empty = self.block.is_empty();
        let flushed_lsn = self.flush.on_request(slice, cycle, empty);

        service_kit::event!(
            tracing::Level::DEBUG,
            "slice",
            slice,
            cycle,
            empty,
            satisfied = flushed_lsn.is_some(),
            "Slice requested a flush",
        );

        let Some(flushed_lsn) = flushed_lsn else {
            return Ok(());
        };
        send_flushed(&self.log_response_tx[slice], slice, cycle, flushed_lsn)
    }

    /// Pop the next entry of a Slice: accumulate an Append into the current
    /// block, or take a Barrier's flush request, if any.
    fn on_next_merge(&mut self, slice: usize) -> anyhow::Result<()> {
        let (append, journal) = match self.slices[slice].pop() {
            buffer::Popped::Append { append, journal } => (append, journal),
            buffer::Popped::Barrier { flush_cycle: None } => return Ok(()),
            buffer::Popped::Barrier {
                flush_cycle: Some(cycle),
            } => return self.on_flush(slice, cycle),
        };

        let priority = self.topology.slice(slice).1;
        let producer = uuid::Producer::from_i64(append.producer);
        let position = crate::merge::Position::from_append(priority, &append);
        self.block.accumulate(journal, producer, &append);

        tracing::trace!(
            slice,
            journal,
            ?position,
            ?producer,
            doc_bytes = append.doc_archived.len(),
            "merged Append"
        );
        self.metrics.appends.increment(1);
        self.metrics
            .bytes_appended
            .increment(append.source_byte_length as u64);

        Ok(())
    }

    /// Move the writer and accumulated block state into a background blocking
    /// task that encodes and writes the block.
    fn start_flush(&mut self) {
        assert!(!self.block.is_empty());
        self.flush.on_started();

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

        self.flush_handle = Some(tokio::task::spawn_blocking(move || {
            let (flushed_lsn, sealed) = writer.append_block(journals, producers, entries)?;
            Ok((writer, flushed_lsn, sealed))
        }));
    }

    /// Handle the completion of a background block flush: restore the writer,
    /// answer the flush requests it satisfies, and bookkeep the disk backlog
    /// (engaging back-pressure if a new segment was sealed).
    fn on_flushed(
        &mut self,
        writer: Writer,
        flushed_lsn: Lsn,
        sealed: Option<&SealedSegment>,
    ) -> anyhow::Result<()> {
        self.writer = Some(writer);

        for (slice, cycle) in self.flush.on_completed(flushed_lsn) {
            send_flushed(&self.log_response_tx[slice], slice, cycle, flushed_lsn)?;
        }

        // Did the flush seal its segment (the writer rolled to the next)?
        let Some(sealed) = sealed else {
            service_kit::event!(
                tracing::Level::TRACE,
                "writer",
                disk_back_pressure = self.disk.back_pressure,
                disk_backlog_mib = self.disk.bytes / (1024 * 1024),
                next_requested = self.flush.is_requested(),
                "log segment flushed (partial segment)"
            );
            return Ok(());
        };

        self.disk.on_sealed(sealed.size);

        service_kit::event!(
            tracing::Level::DEBUG,
            "writer",
            disk_back_pressure = self.disk.back_pressure,
            disk_backlog_mib = self.disk.bytes / (1024 * 1024),
            last_segment = service_kit::event::debug(sealed.path.to_owned()),
            next_requested = self.flush.is_requested(),
            sealed_mib = sealed.size / (1024 * 1024),
            "log segment flushed (segment sealed)"
        );
        self.metrics.segments_sealed.increment(1);
        self.metrics.disk_backlog_bytes.set(self.disk.bytes as f64);
        Ok(())
    }

    /// A sealed segment was compressed or unlinked, reclaiming disk space.
    fn on_reclaimed(&mut self, reclaimed: u64) {
        self.disk.on_reclaimed(reclaimed);

        service_kit::event!(
            tracing::Level::DEBUG,
            "writer",
            disk_back_pressure = self.disk.back_pressure,
            disk_backlog_mib = self.disk.bytes / (1024 * 1024),
            reclaimed_mib = reclaimed / (1024 * 1024),
            "log segment reclaimed",
        );
        self.metrics.disk_backlog_bytes.set(self.disk.bytes as f64);
    }
}

// Send a Flushed response to a Slice. A Slice has at most one flush request
// outstanding, so its channel always has capacity.
fn send_flushed(
    tx: &mpsc::Sender<tonic::Result<shuffle::LogResponse>>,
    slice: usize,
    cycle: u64,
    flushed_lsn: Lsn,
) -> anyhow::Result<()> {
    crate::verify_send(
        tx,
        Ok(shuffle::LogResponse {
            flushed: Some(shuffle::log_response::Flushed {
                cycle,
                flushed_lsn: flushed_lsn.as_u64(),
            }),
            ..Default::default()
        }),
    )?;

    service_kit::event!(
        tracing::Level::DEBUG,
        "slice",
        slice,
        cycle,
        flushed_lsn = flushed_lsn.as_u64(),
        "sent Flushed response to Slice",
    );
    Ok(())
}

// Helper which builds a future that yields the next request from a Slice's Log RPC.
async fn next_log_rx(
    (slice, mut rx): (usize, SliceRx),
) -> (
    usize,                                      // Slice index.
    Option<tonic::Result<shuffle::LogRequest>>, // Request.
    SliceRx,                                    // Stream.
) {
    (slice, rx.next().await, rx)
}

/// Maximum entries merged before the Log yields to service its other events,
/// and then to the tokio runtime: to read Slices' rounds, release their
/// windows, and send Flushed responses while a long merge runs. It bounds how
/// long a ready flush awaits.
const MAX_MERGES: usize = 1024;

#[cfg(test)]
mod test {
    use super::*;

    // A Log's merge, paused by disk back-pressure, still reads each Slice's
    // rounds through to EOF, and exits, discarding rounds it never merged
    // without releasing their quota.
    #[tokio::test]
    async fn test_exit_under_disk_back_pressure() {
        let dir = tempfile::tempdir().unwrap();
        let mut request_tx = Vec::new();
        let mut request_rx = Vec::new();
        let mut response_tx = Vec::new();
        let mut response_rx = Vec::new();
        let mut acked_tx = Vec::new();
        let mut acked_rx = Vec::new();

        for _ in 0..2 {
            let (tx, rx) = mpsc::channel(MAX_MERGES);
            request_tx.push(tx);
            request_rx.push(tokio_stream::wrappers::ReceiverStream::new(rx).boxed());
            let (tx, rx) = mpsc::channel(proto_grpc::CHANNEL_BUFFER);
            response_tx.push(tx);
            response_rx.push(rx);
            let (tx, rx) = watch::channel(0);
            acked_tx.push(tx);
            acked_rx.push(rx);
        }

        let actor = LogActor {
            topology: super::super::state::Topology {
                session_id: 1,
                shards: vec![shuffle::Shard::default(), shuffle::Shard::default()],
                priorities: vec![0],
                log_shard_index: 0,
            },
            log_response_tx: response_tx,
            acked_tx,
            slices: (0..2).map(|_| SliceBuffer::new(0)).collect(),
            // Each block seals its segment, which engages back-pressure.
            writer: Some(Writer::with_thresholds(dir.path(), 0, usize::MAX, 1).unwrap()),
            flush_handle: None,
            block: BlockState::new(),
            flush: FlushState::new(2),
            disk: DiskState::new(1),
            metrics: super::super::Metrics::new("acmeCo/shard-000"),
        };
        let serve = tokio::spawn(actor.serve(request_rx));

        let alloc = doc::HeapNode::new_allocator();
        let doc = doc::HeapNode::from_serde(&serde_json::json!({"key": "val"}), &alloc).unwrap();
        let doc = bytes::Bytes::from(doc.to_archive().to_vec());

        let round = |clock: u64, flush_cycle: u64| {
            Ok(shuffle::LogRequest {
                open: None,
                appends: vec![shuffle::log_request::Append {
                    journal_name_suffix: "acmeCo/journal".to_string(),
                    producer: uuid::Producer::from_bytes([1, 0, 0, 0, 0, 1]).as_i64(),
                    clock,
                    doc_archived: doc.clone(),
                    ..Default::default()
                }],
                barrier: Some(shuffle::log_request::Barrier {
                    adjusted_clock: clock,
                    quota: 10 * clock,
                    caught_up: false,
                    flush_cycle,
                }),
            })
        };

        // Slice 1 is caught up, and Slice 0's first round merges and flushes,
        // which seals a segment.
        request_tx[1]
            .send(Ok(shuffle::LogRequest {
                barrier: Some(shuffle::log_request::Barrier {
                    caught_up: true,
                    ..Default::default()
                }),
                ..Default::default()
            }))
            .await
            .unwrap();
        request_tx[0].send(round(1, 1)).await.unwrap();
        let flushed = response_rx[0].recv().await.unwrap().unwrap();

        // Further rounds are read, but their Appends aren't merged.
        request_tx[0].send(round(2, 2)).await.unwrap();
        request_tx[1].send(round(3, 0)).await.unwrap();
        request_tx.clear();

        tokio::time::timeout(std::time::Duration::from_secs(10), serve)
            .await
            .expect("LogActor must exit under back-pressure")
            .unwrap()
            .unwrap();

        let remaining: Vec<_> = response_rx
            .iter_mut()
            .map(|rx| std::iter::from_fn(|| rx.try_recv().ok()).count())
            .collect();
        let acked: Vec<_> = acked_rx.iter().map(|rx| *rx.borrow()).collect();

        insta::assert_debug_snapshot!((flushed, remaining, acked), @r"
        (
            LogResponse {
                opened: None,
                flushed: Some(
                    Flushed {
                        cycle: 1,
                        flushed_lsn: 65536,
                    },
                ),
                acked: None,
            },
            [
                0,
                0,
            ],
            [
                10,
                0,
            ],
        )
        ");
    }
}
