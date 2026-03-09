use super::{
    heap::{ReadyReadEntry, ReadyReadHeap},
    read::{Meta, ReadState, ReadyRead, map_read_error, probe_write_head},
    routing,
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
    /// Per-binding schema validators, indexed by binding index.
    pub validators: Vec<doc::Validator>,
    /// Per-read producer tracking
    pub reads: Vec<ReadState>,
    /// Causal hints accumulated from consumed ACK documents. Drained during flush.
    pub causal_hints: super::CausalHints,
    /// State machine for tracking flush cycles with Log members.
    pub flush: FlushState,
    /// State machine for tracking progress reporting with the Session.
    pub progress: ProgressState,
    /// Channel for sends to parent Session.
    pub slice_response_tx: mpsc::Sender<tonic::Result<shuffle::SliceResponse>>,
    /// Channels for sends to member Log RPCs, indexed by member index.
    pub log_request_tx: Vec<mpsc::Sender<shuffle::LogRequest>>,
    /// Previous journal name sent to each Log member, for delta encoding.
    pub log_prev_journal: Vec<String>,
    /// Pending Journal write-head probes for newly started reads.
    pub pending_probes:
        stream::FuturesUnordered<future::BoxFuture<'static, anyhow::Result<super::ReadLines>>>,
    /// Reads that are awaiting more data from Gazette brokers.
    pub pending_reads: stream::FuturesUnordered<stream::StreamFuture<super::ReadLines>>,
    /// Number of pending reads that are caught up to their journal write head.
    /// We defer sending Append requests until all pending reads are tailing,
    /// ensuring no pending read has content that could preempt the current heap top.
    pub tailing_reads: usize,
    /// Shard parser for transcoding documents from LinesBatch.
    pub parser: simd_doc::SimdParser,
    /// Ordered heap of reads with ready documents.
    pub ready_read_heap: ReadyReadHeap,
    /// Drain of the Progressed frontier being transmitted as chunked responses.
    pub progressed_drain: crate::frontier::Drain,
}

struct Buffers {
    packed_key: bytes::BytesMut,
    targets: Vec<usize>,
    permits: Vec<mpsc::Permit<'static, shuffle::LogRequest>>,
}

impl SliceActor {
    #[tracing::instrument(
        level = "debug",
        err(Debug, level = "warn"),
        skip_all,
        fields(
            session = self.topology.session_id,
            member = self.topology.slice_member_index,
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
        let verify = crate::verify(
            "SliceRequest",
            "Start",
            &self.topology.members[0].endpoint,
            0,
        );
        match verify.not_eof(slice_request_rx.next().await)? {
            shuffle::SliceRequest {
                start: Some(shuffle::slice_request::Start {}),
                ..
            } => (),
            request => return Err(verify.fail(request)),
        };

        // Spawn tasks that watch journal listings of assigned bindings.
        let mut listing_tasks: stream::FuturesUnordered<
            tokio::task::JoinHandle<Option<anyhow::Error>>,
        > = self.spawn_listings(&cancel);

        // Re-usable scratch buffers.
        let mut buffers = Buffers {
            packed_key: bytes::BytesMut::new(),
            targets: Vec::new(),
            permits: Vec::new(),
        };

        // Measure of wall-clock time, used to gate delayed reads.
        let mut now = uuid::Clock::zero();

        let mut loop_count: u64 = 0;
        loop {
            loop_count += 1;
            tracing::debug!(
                loop_count,
                total_reads = self.reads.len(),
                tailing_reads = self.tailing_reads,
                pending_probes = self.pending_probes.len(),
                pending_reads = self.pending_reads.len(),
                ready_heap = self.ready_read_heap.len(),
                flush = ?self.flush,
                progress = ?self.progress,
                progressed_drain = ?self.progressed_drain,
                "SliceActor::serve iteration"
            );
            // First, attempt non-blocking sends.
            let wake_log_request_tx = self.try_log_request_tx(&mut buffers, &mut now)?;
            let wake_slice_response_tx = self.try_slice_response_tx()?;

            // Then, wait for a blocking future to resolve.
            tokio::select! {
                biased;

                // First priority is receiving messages.
                slice_request = slice_request_rx.next() => {
                    match slice_request {
                        Some(result) => self.on_slice_request(result)?,
                        None => {
                            tracing::debug!(
                                loop_count,
                                total_reads = self.reads.len(),
                                flush_cycle = self.flush.cycle,
                                "SliceActor::serve exiting on Session EOF"
                            );
                            break Ok(());
                        }
                    }
                }
                Some((member_index, log_response, rx)) = log_response_rx.next() => {
                    self.on_log_response(member_index, log_response)?;
                    log_response_rx.push(next_log_rx((member_index, rx)));
                }

                // Next priority is draining ready-to-send messages.
                true = wake_log_request_tx => {}
                true = wake_slice_response_tx => {}

                // Lowest priority is processing journal listings and reads.
                Some(probe_result) = self.pending_probes.next() => {
                    self.on_probe_result(probe_result)?;
                }
                Some(listing_result) = listing_tasks.next() => {
                    self.on_listing_task_done(listing_result)?;
                }
                Some((result, read)) = self.pending_reads.next() => {
                    self.on_read_result(result, read)?;
                }
            }
        }
    }

    // Start tasks that watch journal listings of assigned bindings.
    fn spawn_listings(
        &self,
        cancel: &tokens::CancellationToken,
    ) -> stream::FuturesUnordered<tokio::task::JoinHandle<Option<anyhow::Error>>> {
        let out = stream::FuturesUnordered::new();

        for binding in &self.topology.bindings {
            // Use modulo round-robin to assign bindings to slice members.
            if binding.index % self.topology.members.len() as u16
                != self.topology.slice_member_index as u16
            {
                continue;
            }
            let join_handle = super::listing::spawn_listing(
                binding,
                (*self.topology.journal_clients[binding.index as usize]).clone(),
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
        let verify = crate::verify(
            "SliceRequest",
            "Progress or StartRead",
            &self.topology.members[0].endpoint,
            0,
        );

        match verify.ok(slice_request)? {
            shuffle::SliceRequest {
                progress: Some(shuffle::slice_request::Progress {}),
                ..
            } => {
                tracing::debug!("received Progress request from Session");
                self.progress.request()
            }

            shuffle::SliceRequest {
                start_read: Some(start_read),
                ..
            } => self.on_start_read(start_read),

            request => Err(verify.fail(request)),
        }
    }

    pub fn on_start_read(
        &mut self,
        start_read: shuffle::slice_request::StartRead,
    ) -> anyhow::Result<()> {
        let shuffle::slice_request::StartRead {
            binding: binding_index,
            spec,
            create_revision: _,
            mod_revision: _,
            route,
            checkpoint,
        } = start_read;

        let binding = self
            .topology
            .bindings
            .get(binding_index as usize)
            .context("StartRead invalid binding")?;

        let spec = spec.context("StartRead missing spec")?;
        let journal = spec.name.into_boxed_str();
        let client = (*self.topology.journal_clients[binding.index as usize]).clone();

        // Resolve the checkpoint into producer state and start offset.
        let (offset, producers) = state::resolve_checkpoint(checkpoint);

        tracing::debug!(
            binding = binding.state_key(),
            %journal,
            offset,
            ?producers,
            "starting journal read"
        );

        let mut request = broker::ReadRequest {
            // Add `journal_read_suffix` as a metadata component to the journal name.
            // This helps identify the sources of reads from the perspective of a gazette broker.
            journal: format!("{journal};{}", binding.journal_read_suffix),

            begin_mod_time: binding.not_before.to_unix().0 as i64,
            block: true,
            do_not_proxy: true,
            end_offset: 0, // No end offset.
            metadata_only: false,
            offset,

            // `route` is a hint which directs us to the right broker.
            // This is an optimization and isn't required for correctness.
            header: route.map(|r| broker::Header {
                route: Some(r),
                ..Default::default()
            }),
        };

        let binding_state_key = binding.state_key().to_string();
        let read_id = self.reads.len() as u32;
        self.reads.push(ReadState {
            binding_index: binding_index as u16,
            journal,
            settled: producers,
            pending: Default::default(),
        });

        self.pending_probes.push(Box::pin(async move {
            // Probe the journal for its write head.
            let (write_head, probe_header) = probe_write_head(
                client.clone(),
                &request.journal,
                &binding_state_key,
                request.header.take(),
            )
            .await?;

            let tailing = offset >= write_head;
            request.header = probe_header;

            tracing::debug!(
                journal = %request.journal,
                offset,
                write_head,
                tailing,
                "probed journal write head for started read"
            );

            Ok(Box::pin(gazette::journal::read::ReadLines::new(
                client.read(request).boxed(),
                read_id,
                tailing,
            )))
        }));

        Ok(())
    }

    pub fn on_probe_result(
        &mut self,
        probe_result: anyhow::Result<super::ReadLines>,
    ) -> anyhow::Result<()> {
        let read = probe_result?;
        if read.tailing() {
            self.tailing_reads += 1;
        }
        self.pending_reads.push(read.into_future());
        Ok(())
    }

    /// Parse a LinesBatch into documents and push a ReadyRead onto the heap,
    /// or handle errors from the underlying ReadLines stream.
    pub fn on_read_result(
        &mut self,
        result: Option<gazette::RetryResult<gazette::journal::read::LinesBatch>>,
        mut read: super::ReadLines,
    ) -> anyhow::Result<()> {
        let read_state = &self.reads[read.id() as usize];
        let binding = &self.topology.bindings[read_state.binding_index as usize];

        let Some(result) = result else {
            tracing::info!(
                binding = binding.state_key(),
                journal = %read.fragment().journal,
                "stopping journal read due to EOF"
            );
            if read.tailing() {
                self.tailing_reads = self.tailing_reads.strict_sub(1);
            }
            return Ok(());
        };

        let mut lines_batch = match result {
            Err(gazette::RetryError {
                attempt,
                inner: err,
            }) => match err {
                gazette::Error::BrokerStatus(broker::Status::JournalNotFound) => {
                    tracing::info!(
                        binding = binding.state_key(),
                        journal = %read.fragment().journal,
                        "stopping journal read due to its deletion"
                    );
                    if read.tailing() {
                        self.tailing_reads = self.tailing_reads.strict_sub(1);
                    }
                    return Ok(());
                }
                err if err.is_transient() => {
                    tracing::warn!(
                        binding = %binding.state_key(),
                        journal = %read_state.journal,
                        attempt = attempt,
                        %err,
                        "transient error reading from journal (will retry)"
                    );
                    self.pending_reads.push(read.into_future());
                    return Ok(());
                }
                err => {
                    return Err(map_read_error(
                        err,
                        &read_state.journal,
                        binding.state_key(),
                    ));
                }
            },
            Ok(lines_batch) => lines_batch,
        };

        // Pending read has now resolved. Update tailing aggregate.
        if lines_batch.tailing {
            self.tailing_reads = self.tailing_reads.strict_sub(1);
        }

        let transcoded = match simd_doc::transcode_many(
            &mut self.parser,
            &mut lines_batch.content,
            &mut lines_batch.offset,
            Default::default(),
        ) {
            Err((err, location)) => {
                return Err(map_read_error(
                    gazette::Error::Parsing { err, location },
                    &read_state.journal,
                    binding.state_key(),
                ));
            }
            Ok(transcoded) => transcoded,
        };

        // There may be a remainder if we failed to parse partway through.
        // Put it back to handle it next time.
        if !lines_batch.content.is_empty() {
            read.as_mut().put_back(lines_batch.content.into());
        }

        let metas = super::read::extract_metas(
            &transcoded,
            &binding.source_uuid_ptr,
            &mut self.validators[read_state.binding_index as usize],
            &read_state.journal,
        )?;

        // Consume into owned documents and pair with pre-extracted metadata.
        let mut doc_tail = transcoded.into_iter();
        let mut meta_tail = metas.into_iter();

        let (doc, _) = doc_tail.next().expect("non-empty transcoded");
        let meta = meta_tail.next().expect("non-empty metas");

        let ready_read = ReadyRead {
            doc,
            meta,
            doc_tail,
            meta_tail,
            inner: read,
        };

        self.ready_read_heap.push(ReadyReadEntry {
            priority: binding.priority,
            adjusted_clock: ready_read.meta.clock + binding.read_delay,
            inner: Some(Box::new(ready_read)),
        });

        Ok(())
    }

    fn on_log_response(
        &mut self,
        member_index: usize,
        log_response: Option<tonic::Result<shuffle::LogResponse>>,
    ) -> anyhow::Result<()> {
        let verify = crate::verify(
            "LogResponse",
            "Flushed",
            &self.topology.members[member_index].endpoint,
            member_index,
        );
        let log_response = verify.not_eof(log_response)?;

        match log_response {
            shuffle::LogResponse {
                flushed: Some(shuffle::log_response::Flushed { cycle, flushed_lsn }),
                ..
            } if cycle == self.flush.cycle => {
                let flushed_lsn = log::Lsn::from_u64(flushed_lsn);

                if let Some(completed) = self.flush.on_flushed(member_index, flushed_lsn) {
                    self.progress.on_flush_completed(completed);
                }
                Ok(())
            }

            response => Err(verify.fail(response)),
        }
    }

    fn try_log_request_tx(
        &mut self,
        buffers: &mut Buffers,
        now: &mut uuid::Clock,
    ) -> anyhow::Result<impl Future<Output = bool> + 'static> {
        // Closure for mapping an OwnedPermit Result to Ok (our "poll again" signal).
        // On Err (channel closed), we don't wake and rely on rx of a causal error / fail-fast teardown.
        let ok = |result: Result<_, _>| result.is_ok();
        // Future which represent an absence of an awake signal.
        let idle = future::Either::Right(future::Either::Right(std::future::ready(false)));

        loop {
            // A flush cycle takes priority over sending Append requests.
            // We'll await capacity for Flushes even if the next Append member has capacity.
            if self.flush.should_flush() {
                if let Err(tx) = self.try_log_request_flush_tx(buffers) {
                    return Ok(future::Either::Left(tx.reserve_owned().map(ok)));
                }
            }

            // Defer draining if any pending reads aren't tailing, because a
            // non-tailing read (once resolved) could order first in the heap.
            if self.tailing_reads != self.pending_reads.len() {
                return Ok(idle);
            }

            // Do we have a document ready for append?
            let Some(ReadyReadEntry {
                adjusted_clock,
                inner: ready_read,
                ..
            }) = self.ready_read_heap.peek()
            else {
                return Ok(idle);
            };
            let ready_read = ready_read.as_deref().unwrap();

            let ReadyRead {
                meta, inner: read, ..
            } = ready_read;

            let read_id = read.id() as usize;
            let read_state = &mut self.reads[read_id];
            let binding = &self.topology.bindings[read_state.binding_index as usize];

            // Gate on the adjusted clock: sleep until wall-clock time catches up.
            if let Some(wait) = state::clock_delay(adjusted_clock, now, crate::now_clock) {
                return Ok(future::Either::Right(future::Either::Left(
                    tokio::time::sleep(wait).map(|()| true),
                )));
            }

            let sequenced = state::sequence_document(read_state, binding, meta)?;

            // If this is an Append, attempt to send it to the appropriate member(s).
            if sequenced.is_append {
                if let Err(tx) = Self::try_log_request_append_tx(
                    binding,
                    buffers,
                    &read_state.journal,
                    &self.topology.members,
                    &mut self.log_prev_journal,
                    &self.log_request_tx,
                    ready_read,
                ) {
                    return Ok(future::Either::Left(tx.reserve_owned().map(ok)));
                }
            }

            // Pop the heap entry now that any Append requests have been sent.
            // Crucially: we now cannot fail to consume this document.
            let ReadyReadEntry {
                priority,
                inner: ready_read,
                ..
            } = self.ready_read_heap.pop().unwrap();
            let mut ready_read = ready_read.unwrap();

            let ReadyRead {
                inner: read,
                meta:
                    Meta {
                        producer,
                        clock,
                        flags,
                        ..
                    },
                doc,
                mut doc_tail,
                mut meta_tail,
            } = *ready_read;

            if sequenced.is_commit {
                if flags == uuid::Flags::ACK_TXN {
                    // This ACK is (binding, journal)-scoped: it commits only
                    // this producer's documents in this binding's read of this journal.
                    // But, it may contain causal hints of *other* journals which
                    // committed with this one. Extract and project so we can propagate
                    // to the Session, which is tasked with gating checkpoints for
                    // atomic cross-journal visibility.
                    state::extract_causal_hints(
                        &self.topology.hint_index,
                        &read_state.journal,
                        binding.cohort,
                        producer,
                        clock,
                        doc.get(),
                        &mut self.causal_hints,
                    )?;
                }
                self.flush.set_ready();
            }

            // Step producer state forward to reflect the append.
            _ = read_state
                .pending
                .insert(producer, sequenced.producer_state);

            // Advance doc_tail and meta_tail in lock-step (guaranteed equal length).
            match (doc_tail.next(), meta_tail.next()) {
                (Some((doc, _)), Some(meta)) => {
                    // Re-structure into the existing Box to re-use it.
                    *ready_read = ReadyRead {
                        doc,
                        meta,
                        doc_tail,
                        meta_tail,
                        inner: read,
                    };
                    self.ready_read_heap.push(ReadyReadEntry {
                        priority,
                        adjusted_clock: ready_read.meta.clock + binding.read_delay,
                        inner: Some(ready_read),
                    })
                }
                (None, None) => {
                    if read.tailing() {
                        self.tailing_reads += 1;
                    }
                    self.pending_reads.push(read.into_future());
                }
                _ => unreachable!("doc_tail and meta_tail have equal length"),
            }
        }
    }

    /// Try to send Flush requests to all log channels (all-or-nothing).
    /// Returns `Err(tx)` with the sender that lacked capacity.
    fn try_log_request_flush_tx(
        &mut self,
        buffers: &mut Buffers,
    ) -> Result<(), mpsc::Sender<shuffle::LogRequest>> {
        let Buffers { permits, .. } = buffers;

        // Safety: `permits` is always empty on return (retaining only capacity).
        let permits: &mut Vec<_> =
            unsafe { std::mem::transmute::<&mut Vec<_>, &mut Vec<_>>(permits) };

        // Collect permits to send to all log channels (all-or-nothing).
        for tx in &self.log_request_tx {
            let Ok(permit) = tx.try_reserve() else {
                permits.clear();
                return Err(tx.clone());
            };
            permits.push(permit);
        }

        // Build the frontier from pending producers and causal hints,
        // and then drain pending→settled.
        let frontier = super::producer::build_flush_frontier(
            &self.reads,
            self.causal_hints.drain(),
            self.topology.members.len(),
        );

        for read in self.reads.iter_mut() {
            read.settled.extend(read.pending.drain());
        }
        let flush_cycle = self.flush.start(self.log_request_tx.len(), frontier);

        for permit in permits.drain(..) {
            permit.send(shuffle::LogRequest {
                flush: Some(shuffle::log_request::Flush { cycle: flush_cycle }),
                ..Default::default()
            });
        }

        tracing::debug!(
            members = self.log_request_tx.len(),
            cycle = flush_cycle,
            "sent Flush to all logs"
        );

        Ok(())
    }

    /// Try to send Append requests to target log channels (all-or-nothing).
    /// Returns `Err(tx)` with the sender that lacked capacity.
    fn try_log_request_append_tx(
        binding: &crate::Binding,
        buffers: &mut Buffers,
        journal: &str,
        members: &[shuffle::Member],
        log_prev_journal: &mut [String],
        log_request_tx: &[mpsc::Sender<shuffle::LogRequest>],
        ready_read: &ReadyRead,
    ) -> Result<(), mpsc::Sender<shuffle::LogRequest>> {
        let Buffers {
            packed_key,
            permits,
            targets,
        } = buffers;

        let ReadyRead {
            doc,
            meta:
                Meta {
                    begin_offset,
                    clock,
                    producer,
                    ..
                },
            ..
        } = ready_read;

        // Extract into `packed_key` and hash to route the document.
        // Compute member index `targets` to receive an Append of this document.
        packed_key.clear();
        doc::Extractor::extract_all(doc.get(), &binding.key_extractors, packed_key);

        let key_hash = doc::Extractor::packed_hash(packed_key);
        let r_clock = routing::rotate_clock(*clock);

        targets.clear();
        targets.extend(routing::route_to_members(
            key_hash,
            r_clock,
            binding.filter_r_clocks,
            members,
        ));

        tracing::trace!(
            %journal,
            binding = binding.state_key(),
            ?producer,
            ?clock,
            begin_offset,
            key_hash,
            valid = ready_read.meta.is_schema_valid(),
            r_clock,
            ?targets,
            "appending document to members"
        );

        // Safety: `permits` is always cleared prior to return (retaining only capacity).
        let permits: &mut Vec<_> =
            unsafe { std::mem::transmute::<&mut Vec<_>, &mut Vec<_>>(permits) };

        // All-or-nothing: reserve permits for every target channel.
        for &target in targets.iter() {
            let Ok(permit) = log_request_tx[target].try_reserve() else {
                permits.clear();
                return Err(log_request_tx[target].clone());
            };
            permits.push(permit);
        }
        // All channels reserved. At this point, a send is infallible.

        let packed_key = packed_key.split().freeze();

        for (&target, permit) in targets.iter().zip(permits.drain(..)) {
            let prev_journal = &mut log_prev_journal[target];

            let (journal_name_truncate_delta, journal_name_suffix) =
                gazette::delta::encode(prev_journal, journal);
            let journal_name_suffix = journal_name_suffix.to_string();

            // Update `prev_journal` for next iteration.
            gazette::delta::decode(
                &mut log_prev_journal[target],
                journal_name_truncate_delta,
                &journal_name_suffix,
            );

            permit.send(shuffle::LogRequest {
                append: Some(shuffle::log_request::Append {
                    journal_name_truncate_delta,
                    journal_name_suffix,
                    binding: binding.index as u32,
                    priority: binding.priority,
                    read_delay: binding.read_delay.as_u64(),
                    begin_offset: *begin_offset,
                    producer: producer.as_i64(),
                    clock: clock.as_u64(),
                    packed_key: packed_key.clone(),
                    doc_archived: doc.bytes().clone(),
                    valid: ready_read.meta.is_schema_valid(),
                }),
                ..Default::default()
            });
        }

        Ok(())
    }

    fn try_slice_response_tx(&mut self) -> anyhow::Result<impl Future<Output = bool> + 'static> {
        // Future which represent an absence of an awake signal.
        let idle = future::Either::Right(std::future::ready(false));

        // If no drain is in progress, check whether we should start one.
        if self.progressed_drain.is_empty() {
            let Some(frontier) = self.progress.take_progressed() else {
                return Ok(idle);
            };
            tracing::debug!(?frontier, "sending Progressed to Session");
            self.progressed_drain.start(frontier);
        }

        // Drain chunked Progressed responses.
        // Ensure channel capacity *before* next_chunk() to not lose it.
        while !self.progressed_drain.is_empty() {
            let Ok(permit) = self.slice_response_tx.try_reserve() else {
                return Ok(future::Either::Left(
                    self.slice_response_tx.clone().reserve_owned().map(|_| true),
                ));
            };
            let chunk = self.progressed_drain.next_chunk().unwrap();

            permit.send(Ok(shuffle::SliceResponse {
                progressed: Some(chunk),
                ..Default::default()
            }));
        }

        Ok(idle)
    }
}

// Helper which builds a future that yields the next response from a member's Log RPC.
async fn next_log_rx(
    (member_index, mut rx): (
        usize,
        stream::BoxStream<'static, tonic::Result<shuffle::LogResponse>>,
    ),
) -> (
    usize,                                                           // Member index.
    Option<tonic::Result<shuffle::LogResponse>>,                     // Response.
    stream::BoxStream<'static, tonic::Result<shuffle::LogResponse>>, // Stream.
) {
    (member_index, rx.next().await, rx)
}
