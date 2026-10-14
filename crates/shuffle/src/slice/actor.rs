use super::{
    heap::{ReadEvent, ReadHeap, ReadyReadEntry, ResolvedTop},
    read::{
        Meta, ReadFailure, ReadState, ReadyRead, classify_read_failure, map_read_error,
        parse_lines_batch,
    },
    replay::Replay,
    rounds::Rounds,
    state::{self, FlushState, ProgressState, Topology},
};
use crate::log;
use anyhow::Context;
use futures::{FutureExt, StreamExt, future, stream};
use proto_flow::shuffle;
use proto_gazette::{broker, uuid};
use tokio::sync::mpsc;

/// SliceActor implements the main event loop of a shuffle Slice RPC.
#[allow(dead_code)]
pub struct SliceActor {
    /// Immutable slice configuration: topology, bindings, journal clients.
    pub topology: Topology,
    /// Schema validators, indexed by Source.
    pub validators: Vec<doc::Validator>,
    /// Per-read producer tracking
    pub reads: Vec<ReadState>,
    /// Causal hints accumulated from consumed ACK documents. Drained during flush.
    pub causal_hints: super::CausalHints,
    /// State machine for tracking flush cycles with Log shards.
    pub flush: FlushState,
    /// State machine for tracking progress reporting with the Session.
    pub progress: ProgressState,
    /// Channel for sends to parent Session.
    pub slice_response_tx: mpsc::Sender<tonic::Result<shuffle::SliceResponse>>,
    /// Rounds of sends to shard Log RPCs.
    pub rounds: Rounds,
    /// Shard parser for transcoding documents from LinesBatch.
    pub parser: simd_doc::SimdParser,
    /// Journal reads, and the heap of their ready documents.
    pub heap: ReadHeap,
    /// A replay of a gapped producer's open span, if any.
    /// A Slice has at most one replay, which stalls all other reads.
    pub replay: Option<Replay>,
    /// Per-task metrics counters and gauges.
    pub metrics: super::Metrics,
}

impl Drop for SliceActor {
    fn drop(&mut self) {
        // Ensure that `*_started` / `*_stopped` counters are balanced.
        self.metrics
            .reads_stopped
            .increment(self.heap.tracked_count() as u64);
        self.metrics
            .replays_stopped
            .increment(self.replay.is_some() as u64);
    }
}

/// Why a round ended, and what the Slice's send loop must await before its next.
pub(super) enum Wait {
    /// Await a next actor event.
    Idle,
    /// Await nothing: resume with a next round after servicing actor events,
    /// and yielding to the runtime.
    Yield,
    /// Await the adjusted clock of the heap top.
    Sleep(std::time::Duration),
    /// Await an Ack which releases quota of the window, which is currently full.
    Quota,
}

impl SliceActor {
    #[tracing::instrument(
        level = "debug",
        ret,
        err(Debug, level = "warn"),
        skip_all,
        fields(
            session = self.topology.session_id,
            shard_id = %self.topology.shards[self.topology.slice_shard_index as usize].id,
        )
    )]
    pub async fn serve<R>(
        mut self,
        mut slice_request_rx: R,
        log_response_rx: Vec<stream::BoxStream<'static, tonic::Result<shuffle::LogResponse>>>,
    ) -> anyhow::Result<()>
    where
        R: futures::Stream<Item = tonic::Result<shuffle::SliceRequest>> + Send + Unpin + 'static,
    {
        let cancel = tokens::CancellationToken::new();
        let _drop_guard = cancel.clone().drop_guard();

        // Build a Stream over receive Futures for every Log RPC.
        let mut log_response_rx: stream::FuturesUnordered<_> = log_response_rx
            .into_iter()
            .enumerate()
            .map(next_log_rx)
            .collect();

        // Await Start from the Session RPC.
        let verify = proto_grpc::verify("SliceRequest", "Start", &self.topology.shards[0].endpoint);
        match verify.not_eof(slice_request_rx.next().await)? {
            shuffle::SliceRequest {
                start: Some(shuffle::slice_request::Start {}),
                ..
            } => (),
            request => return Err(verify.fail_msg(request)),
        };

        // Spawn tasks that watch journal listings of assigned bindings.
        let mut listing_tasks: stream::FuturesUnordered<
            tokio::task::JoinHandle<Option<anyhow::Error>>,
        > = self.spawn_listings(&cancel);

        // Measure of wall-clock time, used to gate delayed reads.
        let mut now = uuid::Clock::zero();

        let mut ticker = tokio::time::interval(crate::ACTOR_TICKER_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        // Warning mechanism which is armed on tick and disarmed on read.
        // This is a coarse mechanism as it's disarmed by any read, including
        // completions of tailing bindings that don't unblock the heap,
        // but it's cheap and gives us noisy optics into truly wedged tasks.
        let mut stall_armed = false;

        let mut loop_count: u64 = 0;
        loop {
            loop_count += 1;
            tracing::trace!(
                loop_count,
                total_reads = self.reads.len(),
                heap = ?self.heap,
                flush = ?self.flush,
                progress = ?self.progress,
                "SliceActor::serve iteration"
            );
            // First, attempt non-blocking sends.
            let wake_log_request_tx = self.try_log_request_tx(&mut now)?;
            let wake_slice_response_tx = self.try_slice_response_tx()?;

            // Then, wait for a blocking future to resolve.
            tokio::select! {
                biased;

                // First priority is receiving messages.
                slice_request = slice_request_rx.next() => {
                    match slice_request {
                        Some(result) => self.on_slice_request(result)?,
                        None => break,
                    }
                }
                Some((shard_index, log_response, rx)) = log_response_rx.next() => {
                    self.on_log_response(shard_index, log_response)?;
                    log_response_rx.push(next_log_rx((shard_index, rx)));
                }

                // Next priority is draining ready-to-send messages.
                true = wake_log_request_tx => {}
                true = wake_slice_response_tx => {}

                // Lowest priority is processing journal listings and reads.
                Some(listing_result) = listing_tasks.next() => {
                    self.on_listing_task_done(listing_result)?;
                }
                event = self.heap.next_read_event() => match event {
                    ReadEvent::Probed(probed) => {
                        let (resume_offset, probed_offset, read) = match probed {
                            Ok(ok) => ok,
                            Err(err) => {
                                self.metrics.reads_stopped.increment(1);
                                return Err(err);
                            }
                        };
                        let read_state = &mut self.reads[read.id() as usize];
                        read_state.pending_gap = probed_offset > resume_offset;
                        // Seed the ReadState's offsets at the probed offset before
                        // its first batch, so byte deltas exclude the filtered range
                        // the read skipped.
                        read_state.start_at(probed_offset);
                        self.process_or_await(read)?;
                    }
                    ReadEvent::Resolved { result, read, was_stalled } => {
                        self.on_pending_read_resolved(result, read, was_stalled)?;
                        stall_armed = false;
                    }
                },
                (replay, result) = Replay::next_batch(&mut self.replay) => {
                    self.on_replay_read_resolved(replay, result)?;
                }

                // Periodic tick ensures tracing fires even when idle.
                _ = ticker.tick() => {
                    if stall_armed {
                        self.emit_stall_warnings();
                    }
                    stall_armed = true;
                }
            }
        }

        service_kit::event!(
            tracing::Level::DEBUG,
            "session",
            loop_count,
            total_reads = self.reads.len(),
            flush_cycle = self.flush.cycle,
            "SliceActor::serve exiting on Session EOF"
        );
        self.rounds.logs.clear(); // Drop all tx handles to close.

        // Read clean EOF from all Log RPCs.
        while let Some((shard_index, slice_response, rx)) = log_response_rx.next().await {
            let verify = proto_grpc::verify(
                "LogResponse",
                "EOF",
                &self.topology.shards[shard_index].endpoint,
            );
            match slice_response {
                None => (), // Clean EOF.
                Some(Ok(_ignored)) => log_response_rx.push(next_log_rx((shard_index, rx))),
                Some(Err(status)) => return Err(verify.fail_status(status)),
            }
        }

        Ok(())
    }

    // Start tasks that watch journal listings of assigned bindings.
    fn spawn_listings(
        &self,
        cancel: &tokens::CancellationToken,
    ) -> stream::FuturesUnordered<tokio::task::JoinHandle<Option<anyhow::Error>>> {
        let out = stream::FuturesUnordered::new();

        for binding in &self.topology.bindings {
            // Use modulo round-robin to assign the bindings of our priority to
            // its Slices across shards.
            if binding.priority != self.topology.priority
                || binding.index % self.topology.shards.len() as u16
                    != self.topology.slice_shard_index as u16
            {
                continue;
            }
            let join_handle = super::listing::spawn_listing(
                binding,
                &self.topology.sources[binding.source as usize],
                (*self.topology.journal_clients[binding.source as usize]).clone(),
                self.slice_response_tx.clone(),
                cancel.clone(),
            );
            out.push(join_handle);
        }
        out
    }

    fn on_listing_task_done(
        &mut self,
        listing_result: Result<Option<anyhow::Error>, tokio::task::JoinError>,
    ) -> anyhow::Result<()> {
        match listing_result {
            Err(err) => Err(anyhow::Error::new(err).context("listing task panicked")),
            Ok(None) => anyhow::bail!("listing task canceled before SliceActor::serve exited"),
            Ok(Some(err)) => Err(err),
        }
    }

    fn on_slice_request(
        &mut self,
        slice_request: tonic::Result<shuffle::SliceRequest>,
    ) -> anyhow::Result<()> {
        let verify = proto_grpc::verify(
            "SliceRequest",
            "InitialReadsStarted, Progress, or StartRead",
            &self.topology.shards[0].endpoint,
        );

        match verify.ok(slice_request)? {
            shuffle::SliceRequest {
                progress: Some(shuffle::slice_request::Progress {}),
                ..
            } => {
                service_kit::event!(
                    tracing::Level::DEBUG,
                    "session",
                    "received Progress request"
                );
                self.progress.request()
            }

            shuffle::SliceRequest {
                initial_reads_started: Some(shuffle::slice_request::InitialReadsStarted {}),
                ..
            } => {
                service_kit::event!(
                    tracing::Level::DEBUG,
                    "session",
                    "received InitialReadsStarted request"
                );
                self.heap.on_initial_reads_started();
                Ok(())
            }

            shuffle::SliceRequest {
                start_read: Some(start_read),
                ..
            } => self.on_start_read(start_read),

            request => Err(verify.fail_msg(request)),
        }
    }

    pub fn on_start_read(
        &mut self,
        start_read: shuffle::slice_request::StartRead,
    ) -> anyhow::Result<()> {
        let shuffle::slice_request::StartRead {
            binding: binding_index,
            spec,
            create_revision,
            mod_revision: _,
            route,
            checkpoint,
        } = start_read;

        let binding = self
            .topology
            .bindings
            .get(binding_index as usize)
            .context("StartRead invalid binding")?;

        if binding.priority != self.topology.priority {
            anyhow::bail!(
                "StartRead of binding {binding_index} with priority {} is not of this Slice's priority {}",
                binding.priority,
                self.topology.priority,
            );
        }

        let binding_state_key = binding.state_key().to_string();
        let client = (*self.topology.journal_clients[binding.source as usize]).clone();
        let spec = spec.context("StartRead missing spec")?;

        let truncated_at = match spec
            .labels
            .as_ref()
            .map_or(Ok(""), |set| labels::maybe_one(set, labels::TRUNCATED_AT))?
        {
            "" => uuid::Clock::default(),
            value => uuid::Clock::from_u64(labels::parse_truncated_at(value)?),
        };
        let effective_not_before = binding.not_before.max(truncated_at);

        let journal = spec.name.into_boxed_str();
        let read_id = self.reads.len() as u32;

        // Resolve the checkpoint into producer state and start offset.
        let state::ResolvedCheckpoint {
            offset,
            producers,
            hinted,
        } = state::resolve_checkpoint(checkpoint);

        let request = broker::ReadRequest {
            // Add `journal_read_suffix` as a metadata component to the journal name.
            // This helps identify the sources of reads from the perspective of a gazette broker.
            journal: format!("{journal};{}", binding.journal_read_suffix),

            begin_mod_time: effective_not_before.to_unix().0 as i64,
            block: true,
            do_not_proxy: true,
            end_offset: 0, // No end offset.
            metadata_only: false,
            offset,
            min_etcd_revision: create_revision,

            // `route` is a hint which directs us to the right broker.
            // This is an optimization and isn't required for correctness.
            header: route.map(|r| broker::Header {
                route: Some(r),
                ..Default::default()
            }),
        };

        service_kit::event!(
            tracing::Level::DEBUG,
            "read",
            read_id,
            binding = binding.index,
            journal = journal.to_string(),
            begin_mod_time = request.begin_mod_time,
            n_producers = producers.len(),
            offset,
            "starting journal read",
        );
        self.metrics.reads_started.increment(1);

        self.reads.push(ReadState::recovered(
            binding_index as u16,
            journal,
            truncated_at,
            producers,
            hinted,
        ));

        self.heap.start_probe(Box::pin(super::read::start_read(
            client,
            request,
            read_id,
            binding_state_key,
        )));

        Ok(())
    }

    /// If `read` is ready to yield its next batch, process it immediately.
    /// Otherwise push it into the heap's reads awaiting broker I/O,
    /// emitting a `stall` event if it's stalled.
    ///
    /// Pre-condition: `read` is *not* already awaiting I/O in the heap.
    fn process_or_await(&mut self, mut read: super::ReadLines) -> anyhow::Result<()> {
        if let Some(result) = read.next().now_or_never() {
            return self.process_read_result(result, read);
        }

        let read_id = read.id();
        if self.heap.push_pending(read) {
            self.emit_stall_event(
                read_id,
                "read has stalled heap drain (pending and !tailing)",
            );
        }
        self.update_read_gauges();
        Ok(())
    }

    /// Handle a resolution of a read which was awaiting broker I/O.
    fn on_pending_read_resolved(
        &mut self,
        result: Option<gazette::RetryResult<gazette::journal::read::LinesBatch>>,
        read: super::ReadLines,
        was_stalled: bool,
    ) -> anyhow::Result<()> {
        if was_stalled {
            self.emit_stall_event(read.id(), "read is no longer stalled");
        }
        self.update_read_gauges();
        self.process_read_result(result, read)
    }

    fn update_read_gauges(&self) {
        self.metrics
            .tailing_reads
            .set(self.heap.tailing_count() as f64);
        self.metrics
            .stalled_reads
            .set(self.heap.stalled_count() as f64);
    }

    fn emit_stall_event(&self, read_id: u32, message: &'static str) {
        let read_state = &self.reads[read_id as usize];
        service_kit::event!(
            tracing::Level::DEBUG,
            "stall",
            read_id,
            binding = read_state.binding_index,
            journal = read_state.journal.to_string(),
            read_offset = read_state.read_offset,
            write_head = read_state.write_head,
            "{}",
            message,
        );
    }

    fn emit_stall_warnings(&self) {
        for read in self.heap.stalled_reads() {
            let read_state = &self.reads[read.id() as usize];
            let fragment = read.fragment();
            let mod_time = if fragment.mod_time != 0 {
                tokens::DateTime::from_timestamp_secs(fragment.mod_time)
            } else {
                None
            };

            service_kit::event!(
                tracing::Level::WARN,
                "stall",
                read_id = read.id(),
                binding = read_state.binding_index,
                journal = read_state.journal.to_string(),
                read_offset = read_state.read_offset,
                write_head = read.write_head(),
                fragment_mod_time = service_kit::event::debug(mod_time),
                fragment_begin = fragment.begin,
                fragment_end = fragment.end,
                "read is stalled awaiting broker I/O",
            );
        }
    }

    /// Parse a LinesBatch into documents and push a ReadyRead onto the heap, or
    /// handle a terminal/transient status from the underlying ReadLines stream.
    fn process_read_result(
        &mut self,
        result: Option<gazette::RetryResult<gazette::journal::read::LinesBatch>>,
        read: super::ReadLines,
    ) -> anyhow::Result<()> {
        let read_state = &mut self.reads[read.id() as usize];
        let binding = &self.topology.bindings[read_state.binding_index as usize];
        let journal = read_state.journal.to_string();

        let Some(result) = result else {
            service_kit::event!(
                tracing::Level::INFO,
                "read",
                read_id = read.id(),
                binding = binding.index,
                journal,
                "stopped journal read (EOF)",
            );
            self.metrics.reads_stopped.increment(1);
            return Ok(());
        };

        let lines_batch = match result {
            Err(err) => match classify_read_failure(err) {
                ReadFailure::JournalRemoved(status) => {
                    service_kit::event!(
                        tracing::Level::DEBUG,
                        "read",
                        read_id = read.id(),
                        binding = binding.index,
                        journal,
                        status = status.as_str_name(),
                        "stopped journal read due to removal",
                    );
                    self.metrics.reads_stopped.increment(1);
                    return Ok(());
                }
                ReadFailure::Transient(err, attempt) => {
                    let level = if attempt == 0 {
                        tracing::Level::TRACE
                    } else {
                        tracing::Level::WARN
                    };
                    service_kit::event!(
                        level,
                        "read",
                        read_id = read.id(),
                        binding = binding.index,
                        journal,
                        attempt,
                        err = service_kit::event::debug(err),
                        "transient error reading from journal (will retry)",
                    );
                    return self.process_or_await(read);
                }
                ReadFailure::Terminal(err) => {
                    self.metrics.reads_stopped.increment(1);
                    return Err(map_read_error(
                        err,
                        &read_state.journal,
                        binding.state_key(),
                        "reading next lines",
                    ));
                }
            },
            Ok(lines_batch) => lines_batch,
        };

        read_state.write_head = read.write_head();

        service_kit::event!(
            tracing::Level::TRACE,
            "read",
            read_id = read.id(),
            binding = binding.index,
            journal,
            offset = lines_batch.offset,
            length = lines_batch.content.len(),
            tailing = lines_batch.tailing,
            n_tailing = self.heap.tailing_count(),
            "received LinesBatch",
        );

        let ready_read = match parse_lines_batch(
            &mut self.parser,
            &mut self.validators[binding.source as usize],
            binding,
            &self.topology.sources[binding.source as usize],
            &read_state.journal,
            read,
            lines_batch,
            "transcoding documents",
        ) {
            Ok(ready_read) => ready_read,
            Err(err) => {
                self.metrics.reads_stopped.increment(1);
                return Err(err);
            }
        };

        self.heap.push_ready(ReadyReadEntry {
            adjusted_clock: ready_read.meta.clock + binding.read_delay,
            inner: Some(Box::new(ready_read)),
        });

        Ok(())
    }

    fn on_log_response(
        &mut self,
        shard_index: usize,
        log_response: Option<tonic::Result<shuffle::LogResponse>>,
    ) -> anyhow::Result<()> {
        let verify = proto_grpc::verify(
            "LogResponse",
            "Acked or Flushed",
            &self.topology.shards[shard_index].endpoint,
        );
        let log_response = verify.not_eof(log_response)?;

        match log_response {
            shuffle::LogResponse {
                acked: Some(shuffle::log_response::Acked { quota }),
                ..
            } => self.rounds.on_acked(shard_index, quota).with_context(|| {
                format!(
                    "Acked from Log {}",
                    self.topology.shards[shard_index].endpoint
                )
            }),
            shuffle::LogResponse {
                flushed: Some(shuffle::log_response::Flushed { cycle, flushed_lsn }),
                ..
            } if cycle == self.flush.cycle => {
                let flushed_lsn = log::Lsn::from_u64(flushed_lsn);

                if let Some(completed) = self.flush.on_flushed(shard_index, flushed_lsn)? {
                    self.progress.on_flush_completed(completed);
                }
                Ok(())
            }

            response => Err(verify.fail_msg(response)),
        }
    }

    /// Attempt to send a round to Logs with Appends of ready documents and/or a
    /// flush request, if ready. Returns a future of what the Actor must next await.
    fn try_log_request_tx(
        &mut self,
        now: &mut uuid::Clock,
    ) -> anyhow::Result<impl Future<Output = bool> + 'static> {
        // Ensure all Log channels have capacity for a next round.
        if let Some(log) = (self.rounds.logs)
            .iter()
            .position(|log| log.tx.capacity() == 0)
        {
            let tx = self.rounds.logs[log].tx.clone();

            return Ok(future::Either::Left(
                // On Err (channel closed), we don't wake and rely on rx of a
                // causal error / fail-fast teardown.
                async move { tx.reserve().await.is_ok() },
            ));
        }

        // If we just received Flushed then request the next flush immediately,
        // instead of delaying it behind a full round.
        let wait = if self.flush.should_flush() {
            Wait::Yield
        } else {
            self.fill_round(now)?
        };

        let flush = if self.flush.should_flush() {
            let frontier = super::producer::build_flush_frontier(
                &mut self.reads,
                self.causal_hints.drain(),
                self.topology.shards.len(),
            );
            let cycle = self.flush.start(self.topology.shards.len(), frontier);

            service_kit::event!(
                tracing::Level::DEBUG,
                "log",
                cycle,
                "broadcast flush request",
            );
            self.metrics.flushes.increment(1);

            Some(cycle)
        } else {
            None
        };

        // A Slice is caught up if, post-round, it has no document due.
        // An active replay always has documents due.
        let caught_up = self.replay.is_none()
            && matches!(
                self.heap.resolved_top(now),
                ResolvedTop::Idle | ResolvedTop::Delayed(_)
            );

        if self.rounds.send(caught_up, flush)? {
            self.metrics.rounds.increment(1);
        }

        Ok(future::Either::Right(async move {
            match wait {
                Wait::Idle | Wait::Quota => false,
                Wait::Yield => {
                    tokio::task::yield_now().await;
                    true
                }
                Wait::Sleep(wait) => {
                    tokio::time::sleep(wait).await;
                    true
                }
            }
        }))
    }

    /// Fill the round by consuming ready documents until the round must
    /// end, returning what the actor must await next.
    fn fill_round(&mut self, now: &mut uuid::Clock) -> anyhow::Result<Wait> {
        let wait = loop {
            // End a round having consumed its quota, which also yields
            // periodically to service Actor events.
            if self.rounds.is_round_full() {
                break Wait::Yield;
            }

            // If there's an active replay, sequence and drain its historical
            // documents. They append before all heap documents, including the
            // replay trigger (still in the heap).
            if let Some(replay) = self.replay.take() {
                break self.try_drain_replay(replay).inspect_err(|_| {
                    self.metrics.replays_stopped.increment(1);
                })?;
            }

            match self.heap.resolved_top(now) {
                ResolvedTop::Unknown | ResolvedTop::Idle => break Wait::Idle,
                ResolvedTop::Delayed(delay) => break Wait::Sleep(delay),
                ResolvedTop::Due => (),
            }

            if let Some(wait) = self.extend_round()? {
                break wait;
            }
        };

        Ok(wait)
    }

    /// Extend the round by the due top of the heap.
    /// Returns the Wait which ends the round, or None if it may be extended further.
    fn extend_round(&mut self) -> anyhow::Result<Option<Wait>> {
        let ReadyReadEntry {
            adjusted_clock,
            inner: ready_read,
        } = self.heap.peek_ready().unwrap();
        let ready_read = ready_read.as_deref().unwrap();

        let ReadyRead {
            doc,
            meta,
            inner: read,
            ..
        } = ready_read;

        let read_id = read.id() as usize;
        let read_state = &mut self.reads[read_id];
        let binding = &self.topology.bindings[read_state.binding_index as usize];

        let producer_state = read_state.producer_state(meta.producer);
        let sequenced = state::sequence_producer(
            producer_state,
            &read_state.journal,
            read_state.truncated_at,
            binding,
            meta,
        )?;

        // If this document awakens a gapped producer, then we must leave
        // this document in the heap while we replay the gapped span.
        // We'll re-sequence this document when it's done.
        if sequenced.replay {
            self.start_replay(read_id, producer_state, *meta)?;
            return Ok(None);
        }

        if self
            .rounds
            .try_charge(doc.bytes().len() as u64, *adjusted_clock)
            .is_err()
        {
            return Ok(Some(Wait::Quota));
        }

        // Buffer an Append for the appropriate shard(s).
        if sequenced.is_append {
            self.rounds.buffer_append(
                binding,
                &read_state.journal,
                &self.topology.shards,
                ready_read,
            );
        }

        // Pop the heap entry now that its quota is charged.
        // Crucially: we now cannot fail to consume this document.
        let ReadyReadEntry {
            inner: ready_read, ..
        } = self.heap.pop_ready().unwrap();
        let ready_read = ready_read.unwrap();

        let Meta {
            end_offset,
            producer,
            clock,
            flags,
            ..
        } = ready_read.meta;

        // Track maximum forward progress of the read.
        read_state.read_offset = end_offset;

        // Fold any committed backfill control clock into this journal's
        // per-flush backfill state, then step producer state forward.
        read_state.backfill_begin = read_state.backfill_begin.max(sequenced.backfill_begin);
        read_state.backfill_complete = read_state
            .backfill_complete
            .max(sequenced.backfill_complete);

        read_state.sample_gap_floor(clock);

        _ = read_state
            .unreported
            .insert(producer, sequenced.producer_state);

        // Copy so the `binding` borrow can end below, freeing &mut self for re-borrow.
        let (cohort, read_delay) = (binding.cohort, binding.read_delay);

        let (clock_seconds, clock_nanos) = clock.to_unix();
        self.metrics.last_source_published_at[cohort as usize]
            .set(clock_seconds as f64 + clock_nanos as f64 / 1_000_000_000.0);

        if sequenced.is_commit {
            if flags.is_ack() {
                // This ACK is (binding, journal)-scoped: it commits only
                // this producer's documents in this binding's read of this journal.
                // But, it may contain causal hints of *other* journals which
                // committed with this one. Extract and project so we can propagate
                // to the Session, which is tasked with gating checkpoints for
                // atomic cross-journal visibility.
                state::extract_causal_hints(
                    &self.topology.hint_index,
                    &read_state.journal,
                    cohort,
                    read_state.binding_index,
                    producer,
                    clock,
                    ready_read.doc.get(),
                    &mut self.causal_hints,
                )?;
            }
            self.flush.set_ready();

            // If we resolved our last recovery resumption hint, then drop.
            // Everything after is post-crash docs this session can't consume.
            if read_state.resolve_hint(producer, &sequenced.producer_state) {
                debug_assert!(self.replay.is_none());

                service_kit::event!(
                    tracing::Level::INFO,
                    "read",
                    read_id,
                    binding = read_state.binding_index,
                    journal = read_state.journal.to_string(),
                    read_offset = read_state.read_offset,
                    write_head = read_state.write_head,
                    "parked read at its hinted frontier",
                );
                // The read is no longer held by `heap`.
                self.metrics.reads_stopped.increment(1);

                return Ok(None); // Drops `ready_read`, and its read.
            }
        }

        match ready_read.advance() {
            Ok(ready_read) => self.heap.push_ready(ReadyReadEntry {
                adjusted_clock: ready_read.meta.clock + read_delay,
                inner: Some(ready_read),
            }),
            // This read's batch is fully drained, and we must await I/O.
            Err(read) => self.process_or_await(read)?,
        }

        Ok(None)
    }

    fn try_slice_response_tx(&mut self) -> anyhow::Result<impl Future<Output = bool> + 'static> {
        // Future which represent an absence of an awake signal.
        let idle = future::Either::Right(std::future::ready(false));

        if !self.progress.has_progressed() {
            return Ok(idle);
        }
        // Reserve capacity *before* taking the Frontier — otherwise an absent
        // permit would discard progress that hasn't been emitted yet.
        let Ok(permit) = self.slice_response_tx.try_reserve() else {
            return Ok(future::Either::Left(
                self.slice_response_tx.clone().reserve_owned().map(|_| true),
            ));
        };

        let frontier = self.progress.take_progressed();
        let (journals, journal_producers, bytes_read_delta, bytes_behind_delta) =
            frontier.measures();

        permit.send(Ok(shuffle::SliceResponse {
            progressed: Some(frontier.encode()),
            ..Default::default()
        }));

        service_kit::event!(
            tracing::Level::DEBUG,
            "session",
            bytes_behind_delta,
            bytes_read_delta,
            journal_producers,
            journals,
            "sent Progressed",
        );
        self.metrics
            .bytes_read
            .increment(bytes_read_delta.max(0) as u64);

        Ok(idle)
    }
}

// Helper which builds a future that yields the next response from a shard's Log RPC.
async fn next_log_rx(
    (shard_index, mut rx): (
        usize,
        stream::BoxStream<'static, tonic::Result<shuffle::LogResponse>>,
    ),
) -> (
    usize,                                                           // Shard index.
    Option<tonic::Result<shuffle::LogResponse>>,                     // Response.
    stream::BoxStream<'static, tonic::Result<shuffle::LogResponse>>, // Stream.
) {
    (shard_index, rx.next().await, rx)
}
