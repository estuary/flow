use super::producer::{ProducerMap, ProducerState};
use super::read::ReadState;
use crate::binding::PartitionFilter;
use anyhow::Context;
use proto_flow::shuffle;
use proto_gazette::uuid;

/// Immutable slice configuration: topology, bindings, and lazily-initialized
/// journal clients. Set once during Slice startup and never modified.
#[allow(dead_code)]
pub struct Topology {
    /// Unique identifier for this session, assigned by the coordinator.
    pub session_id: u64,
    /// Ordered member topology: each member owns a disjoint key range.
    pub members: Vec<shuffle::Member>,
    /// Index of this Slice RPC within `members`.
    pub slice_member_index: u32,
    /// Name of the task that owns this session.
    pub task_name: models::Name,
    /// Per-binding shuffle configuration extracted from the task spec.
    pub bindings: Vec<crate::Binding>,
    /// Lazily-initialized Gazette clients for listing and reading journals, indexed by binding.
    pub journal_clients: Vec<super::LazyJournalClient>,
    /// Sorted index for projecting hinted journal names to bindings.
    pub hint_index: HintIndex,
}

/// Flush cycle state machine, tracking in-flight flushes to Queue members.
///
/// The caller is responsible for building the frontier (from reads + causal hints)
/// and passing it to `start`. When a cycle completes, `on_flushed` returns the
/// completed frontier for the caller to reduce into accumulated progress.
///
/// Flush and progress reporting (see `ProgressState`) are deliberately decoupled
/// for latency pipelining: Slices flush autonomously after each commit without
/// waiting for a Session progress request. Multiple flush cycles can complete
/// while the Session processes the previous progress delta. When the Session sends
/// a Progress request, the accumulated flushed frontiers are often already
/// available, reducing end-to-end latency from flush_time + round_trip to
/// approximately max(flush_time, round_trip).
pub struct FlushState {
    /// Monotonically increasing sequence number for flush cycles.
    pub seq: u64,
    /// Whether a commit has been observed since the last flush cycle started.
    ready: bool,
    /// Per-member in-flight tracking. Non-empty while a cycle is active.
    in_flight: Vec<bool>,
    /// The frontier being flushed in the current cycle.
    flushing: crate::Frontier,
}

impl FlushState {
    pub fn new() -> Self {
        Self {
            seq: 1,
            ready: false,
            in_flight: Vec::new(),
            flushing: Default::default(),
        }
    }

    /// Whether we're ready to begin a Flush cycle.
    pub fn should_flush(&self) -> bool {
        self.ready && self.in_flight.is_empty()
    }

    /// Record that a commit has been observed (ACK_TXN or OUTSIDE_TXN).
    pub fn set_ready(&mut self) {
        self.ready = true;
    }

    /// Begin a flush cycle with the given pre-built frontier.
    /// Returns the new flush sequence number.
    pub fn start(&mut self, member_count: usize, frontier: crate::Frontier) -> u64 {
        assert!(
            self.in_flight.is_empty(),
            "cannot start flush while one is in-flight"
        );

        self.ready = false;
        self.in_flight.resize(member_count, true);
        self.flushing = frontier;
        self.seq
    }

    /// Record a Flushed response from a queue member.
    /// Returns the completed frontier when all members have flushed.
    pub fn on_flushed(&mut self, member_index: usize) -> Option<crate::Frontier> {
        let Some(in_flight) = self.in_flight.get_mut(member_index) else {
            return None;
        };
        *in_flight = false;

        if self.in_flight.iter().any(|pending| *pending) {
            return None;
        }
        tracing::debug!(seq = self.seq, "all members Flushed");

        // We increment `seq` now to ensure we'll clearly reject a duplicate
        // Flushed from a Queue (which would be a protocol violation).
        self.in_flight.clear();
        self.seq += 1;

        Some(std::mem::take(&mut self.flushing))
    }
}

/// Progress reporting state, tracking the Session's outstanding progress
/// request and accumulating flushed frontiers until the request is fulfilled.
pub struct ProgressState {
    /// Whether the Session has an outstanding progress request.
    requested: bool,
    /// Accumulated frontier from completed flush cycles, awaiting reporting.
    flushed: crate::Frontier,
}

impl ProgressState {
    pub fn new() -> Self {
        Self {
            requested: false,
            flushed: Default::default(),
        }
    }

    /// Record a Progress request from the Session.
    pub fn request(&mut self) -> anyhow::Result<()> {
        if self.requested {
            anyhow::bail!("received Progress request while one is already pending");
        }
        self.requested = true;
        Ok(())
    }

    /// Reduce a completed flush frontier into accumulated progress.
    pub fn on_flush_completed(&mut self, frontier: crate::Frontier) {
        self.flushed = std::mem::take(&mut self.flushed).reduce(frontier);
    }

    /// If progress was requested and we have flushed progress to report,
    /// take the flushed frontier and clear both flags.
    pub fn take_progressed(&mut self) -> Option<crate::Frontier> {
        if !self.requested || self.flushed.journals.is_empty() {
            return None;
        }
        self.requested = false;
        Some(std::mem::take(&mut self.flushed))
    }
}

impl std::fmt::Debug for FlushState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlushState")
            .field("seq", &self.seq)
            .field("ready", &self.ready)
            .field("in_flight", &self.in_flight.iter().filter(|p| **p).count())
            .finish()
    }
}

impl std::fmt::Debug for ProgressState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProgressState")
            .field("requested", &self.requested)
            .field("flushed", &self.flushed.journals.len())
            .finish()
    }
}

/// Result of speculatively sequencing a document.
/// State is not yet committed — the caller must update producer state
/// after successful I/O.
#[derive(Debug)]
pub struct SequencedDoc {
    /// Whether the document should be enqueued to queue members.
    pub is_enqueue: bool,
    /// Whether this document completed a transaction (triggers flush cycle).
    pub is_commit: bool,
    /// Updated producer state to commit on successful enqueue.
    pub producer_state: ProducerState,
}

/// Gate on `adjusted_clock` relative to `now`: if the clock is in the future,
/// refresh `now` via `update_now` and re-check. Returns `Some(duration)` if
/// the clock is still ahead after the refresh, or `None` if processing can proceed.
pub fn clock_delay(
    adjusted_clock: &uuid::Clock,
    now: &mut uuid::Clock,
    update_now: impl FnOnce() -> uuid::Clock,
) -> Option<std::time::Duration> {
    if adjusted_clock <= now {
        return None;
    }
    *now = update_now();
    if adjusted_clock <= now {
        return None;
    }
    Some(
        adjusted_clock
            .to_time()
            .duration_since(now.to_time())
            .unwrap_or(std::time::Duration::ZERO),
    )
}

/// Resolve a StartRead's checkpoint into producer state and a start offset.
///
/// Conservative read strategy: prefer the minimum uncommitted begin offset.
/// Or, if all producers are committed, the maximum committed end offset.
/// Or, use zero if the checkpoint is empty.
pub fn resolve_checkpoint(
    checkpoint: Vec<shuffle::ProducerFrontier>,
) -> (i64, ProducerMap<ProducerState>) {
    let mut producers = ProducerMap::<ProducerState>::with_capacity_and_hasher(
        checkpoint.len(),
        Default::default(),
    );
    let mut min_uncommitted_begin = i64::MAX;
    let mut max_committed_end = i64::MIN;

    for frontier in checkpoint {
        let shuffle::ProducerFrontier {
            producer,
            last_commit,
            hinted_commit: _,
            offset,
        } = frontier;

        let producer = uuid::Producer::from_i64(producer);
        let last_commit = uuid::Clock::from_u64(last_commit);

        if offset >= 0 {
            // Offset begins an uncommitted producer span.
            min_uncommitted_begin = min_uncommitted_begin.min(offset);
        } else {
            // Offset is the negation of a committed producer span end offset.
            max_committed_end = max_committed_end.max(-offset);
        }

        producers.insert(
            producer,
            ProducerState {
                last_commit,
                max_continue: uuid::Clock::zero(),
                offset,
            },
        );
    }

    let offset = if min_uncommitted_begin != i64::MAX {
        min_uncommitted_begin
    } else if max_committed_end != i64::MIN {
        max_committed_end
    } else {
        0
    };

    (offset, producers)
}

/// Speculatively sequence a document against current producer state.
///
/// Returns a `SequencedDoc` capturing the outcome. State is NOT modified;
/// the caller must update producer state after successful I/O.
pub fn sequence_document(
    read_state: &ReadState,
    binding: &crate::Binding,
    meta: &super::read::Meta,
) -> anyhow::Result<SequencedDoc> {
    let super::read::Meta {
        producer,
        clock,
        flags,
        begin_offset,
        end_offset,
    } = meta;

    // Query for the Producer's latest state: pending
    // (updated since last flush) takes precedence over settled state.
    let mut producer_state = (read_state.pending.get(producer))
        .or_else(|| read_state.settled.get(producer))
        .cloned() // This is a cheap clone.
        .unwrap_or_default();

    // Determine the message's sequencing outcome.
    let outcome = uuid::sequence(
        *flags,
        *clock,
        &mut producer_state.last_commit,
        &mut producer_state.max_continue,
    )
    .with_context(|| {
        format!(
            "failed to sequence journal {} (binding {}) document at offset {begin_offset}",
            read_state.journal,
            binding.state_key(),
        )
    })?;

    // Match over `outcome` to update `producer_state` and determine enqueue/commit.
    let (is_enqueue, is_commit) = match outcome {
        uuid::SequenceOutcome::OutsideCommit => {
            producer_state.offset = -*end_offset;
            (true, true)
        }
        uuid::SequenceOutcome::OutsideDuplicate => (false, false),
        uuid::SequenceOutcome::ContinueBeginSpan => {
            producer_state.offset = *begin_offset;
            (true, false)
        }
        uuid::SequenceOutcome::ContinueExtendSpan => (true, false),
        uuid::SequenceOutcome::ContinueDuplicate => (false, false),
        uuid::SequenceOutcome::AckDeepRollback => {
            tracing::warn!(
                binding=%binding.state_key(),
                clock=?clock,
                journal=%read_state.journal,
                last_commit=?producer_state.last_commit,
                producer=?producer,
                "detected rollback prior to last committed clock of the producer (possible loss of exactly-once guarantees)",
            );
            producer_state.offset = -*end_offset;
            (false, true)
        }
        uuid::SequenceOutcome::AckCommit
        | uuid::SequenceOutcome::AckEmpty
        | uuid::SequenceOutcome::AckCleanRollback => {
            // This ACK commits (or rolls back) the producer's preceding
            // CONTINUE_TXN documents in this journal only. Cross-journal
            // visibility for the same producer transaction is handled separately
            // via `extract_causal_hints`.
            producer_state.offset = -*end_offset;
            (false, true)
        }
        uuid::SequenceOutcome::AckDuplicate => (false, false),
    };

    // A `notBefore` or `notAfter` suppresses document enqueue, but doesn't impact
    // the propagation of flush and progress reporting.
    let is_enqueue = is_enqueue && *clock >= binding.not_before && *clock < binding.not_after;

    tracing::trace!(
        journal = %read_state.journal,
        binding = binding.state_key(),
        ?producer,
        ?clock,
        begin_offset,
        ?outcome,
        is_enqueue,
        is_commit,
        "sequenced document"
    );

    Ok(SequencedDoc {
        is_enqueue,
        is_commit,
        producer_state,
    })
}

/// Sorted index for projecting hinted journal names to (binding_index, cohort),
/// with per-entry partition filters.
///
/// Entries are sorted by prefix then binding index. Because the control plane
/// guarantees no collection is a prefix of another, at most one distinct prefix
/// can match a given journal name — so lookup is a single binary search plus
/// a linear scan of adjacent entries sharing that prefix.
///
/// Hints are filtered to the ACK's cohort because cohorts (unique (priority,
/// read_delay) tuples) have independent visibility semantics. If a producer
/// writes to journals spanning multiple read cohorts within one of its transactions,
/// we only want to gate progress on reading commits from journals matching
/// the ACK's cohort.
///
/// If we didn't filter in this way, then progress of a journal read in real-time
/// could be blocked by the read-delay applied to a hinted journal. Or similarly,
/// a high-priority journal could be blocked by a low-priority hinted journal.
///
/// Hinted journals of other cohorts will have their own ACKs, and will project to
/// hints internal to their own cohort's progress tracking.
pub struct HintIndex(Vec<(Box<str>, u32, u32, PartitionFilter)>); // (prefix, binding_index, cohort, filter)

impl HintIndex {
    pub fn new<'a>(entries: impl Iterator<Item = (&'a str, u32, u32, PartitionFilter)>) -> Self {
        let mut index: Vec<(&str, u32, u32, PartitionFilter)> = entries.collect();

        index.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

        // Now that we've sorted, re-allocate partition name prefixes extended with a trailing slash.
        // This ensures we don't match "acmeCo/anvils/..." to "acmeCo/anvils-two/...",
        // and also aligns memory locality and ordering with our query pattern.
        let owned: Vec<(Box<str>, u32, u32, PartitionFilter)> = index
            .into_iter()
            .map(|(prefix, idx, cohort, filter)| {
                (Box::from(format!("{prefix}/")), idx, cohort, filter)
            })
            .collect();

        Self(owned)
    }

    pub fn from_bindings(bindings: &[crate::Binding]) -> Self {
        Self::new(bindings.iter().map(|b| {
            (
                b.partition_template_name.as_ref(),
                b.index,
                b.cohort,
                PartitionFilter::new(&b.partition_fields, &b.partition_selector),
            )
        }))
    }

    /// Find all binding indices whose prefix matches `journal` within `cohort`,
    /// filtering by each entry's partition selector.
    ///
    /// Because no collection prefix is a prefix of another, there is at most
    /// one matching prefix for any journal name.
    pub fn lookup(&self, journal: &str, cohort: u32, out: &mut Vec<u32>) -> anyhow::Result<()> {
        out.clear();

        // Find the first entry whose partition name prefix is > journal.
        // The matching prefix, if any, is immediately before this position.
        let pos = self
            .0
            .partition_point(|(prefix, _, _, _)| prefix.as_ref() <= journal);
        if pos == 0 {
            return Ok(());
        }

        // Check whether the entry just before `pos` is a prefix of `journal`.
        let matched_prefix = &self.0[pos - 1].0;
        if !journal.starts_with(matched_prefix.as_ref()) {
            return Ok(());
        }

        // Scan all entries sharing this prefix (they are contiguous and sorted).
        for &(ref prefix, binding_idx, binding_cohort, ref filter) in self.0[..pos].iter().rev() {
            if prefix != matched_prefix {
                break;
            }
            if binding_cohort == cohort
                && filter.matches_name_suffix(&journal[matched_prefix.len()..])?
            {
                out.push(binding_idx);
            }
        }

        Ok(())
    }
}

/// Decode causal hints from an ACK document and project them through the
/// `hint_index` into `causal_hints` entries keyed by (journal, binding_index).
pub fn extract_causal_hints<N: json::AsNode>(
    hint_index: &HintIndex,
    ack_journal: &str,
    ack_cohort: u32,
    ack_producer: uuid::Producer,
    ack_clock: uuid::Clock,
    ack_doc: &N,
    causal_hints: &mut super::CausalHints,
) -> anyhow::Result<()> {
    let mut hint_iter =
        publisher::intents::decode_transaction_hints(ack_journal, ack_producer, ack_clock, ack_doc);
    let mut matched_bindings = Vec::new();

    while let Some(result) = hint_iter.next() {
        let (hinted_journal, hinted_producer, hinted_clock) = result.map_err(|err| {
            anyhow::anyhow!("decoding causal hint from ACK in {ack_journal}: {err}")
        })?;

        hint_index.lookup(hinted_journal, ack_cohort, &mut matched_bindings)?;

        for &binding_idx in &matched_bindings {
            causal_hints
                .entry((hinted_journal.into(), binding_idx))
                .or_default()
                .push((hinted_producer, hinted_clock));
        }
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::super::CausalHints;
    use super::super::read::Meta;
    use super::*;
    use crate::testing::test_binding;
    use proto_gazette::broker;
    use proto_gazette::uuid::{Clock, Flags, Producer};

    const OUTSIDE: Flags = Flags(proto_gazette::message_flags::OUTSIDE_TXN as u16);
    const CONTINUE: Flags = Flags(proto_gazette::message_flags::CONTINUE_TXN as u16);
    const ACK: Flags = Flags(proto_gazette::message_flags::ACK_TXN as u16);

    fn meta(
        producer: Producer,
        clock: Clock,
        flags: Flags,
        begin_offset: i64,
        end_offset: i64,
    ) -> Meta {
        Meta {
            producer,
            clock,
            flags,
            begin_offset,
            end_offset,
        }
    }

    fn producer(id: u8) -> Producer {
        Producer::from_bytes([id | 0x01, 0, 0, 0, 0, 0])
    }

    /// Build a checkpoint entry for a producer with zero clocks and a committed offset.
    /// This is the minimal "known producer" state needed by tests.
    fn checkpoint_entry(p: &Producer, committed_end: i64) -> shuffle::ProducerFrontier {
        shuffle::ProducerFrontier {
            producer: p.as_i64(),
            last_commit: 0,
            hinted_commit: 0,
            offset: -committed_end, // Negative = committed end offset.
        }
    }

    struct TestState {
        bindings: Vec<crate::Binding>,
        reads: Vec<ReadState>,
        flush: FlushState,
        progress: ProgressState,
    }

    fn test_state(bindings: Vec<crate::Binding>) -> TestState {
        TestState {
            bindings,
            reads: Vec::new(),
            flush: FlushState::new(),
            progress: ProgressState::new(),
        }
    }

    impl TestState {
        fn commit(&mut self, read_id: usize, producer: Producer, seq: SequencedDoc) {
            _ = self.reads[read_id]
                .pending
                .insert(producer, seq.producer_state);
            if seq.is_commit {
                self.flush.set_ready();
            }
        }
    }

    #[test]
    fn test_resolve_checkpoint() {
        let p1 = producer(0x01);
        let p3 = producer(0x03);
        let p5 = producer(0x05);

        // Empty checkpoint → offset 0.
        let (offset, producers) = resolve_checkpoint(vec![]);
        assert_eq!(offset, 0);
        assert!(producers.is_empty());

        // All committed (negative offsets) → max committed end.
        let (offset, producers) = resolve_checkpoint(vec![
            shuffle::ProducerFrontier {
                producer: p1.as_i64(),
                last_commit: Clock::from_u64(100).as_u64(),
                hinted_commit: 0,
                offset: -500,
            },
            shuffle::ProducerFrontier {
                producer: p3.as_i64(),
                last_commit: Clock::from_u64(200).as_u64(),
                hinted_commit: 0,
                offset: -1000,
            },
        ]);
        assert_eq!(offset, 1000, "max committed end = max(-offset)");
        assert_eq!(producers.len(), 2);

        // Mixed committed/uncommitted → min uncommitted begin.
        let (offset, producers) = resolve_checkpoint(vec![
            shuffle::ProducerFrontier {
                producer: p1.as_i64(),
                last_commit: Clock::from_u64(100).as_u64(),
                hinted_commit: 0,
                offset: -500, // committed
            },
            shuffle::ProducerFrontier {
                producer: p3.as_i64(),
                last_commit: Clock::from_u64(200).as_u64(),
                hinted_commit: 0,
                offset: 300, // uncommitted
            },
            shuffle::ProducerFrontier {
                producer: p5.as_i64(),
                last_commit: Clock::from_u64(50).as_u64(),
                hinted_commit: 0,
                offset: 100, // uncommitted
            },
        ]);
        assert_eq!(offset, 100, "min uncommitted begin");
        assert_eq!(producers.len(), 3);

        // All uncommitted → min begin.
        let (offset, _producers) = resolve_checkpoint(vec![
            shuffle::ProducerFrontier {
                producer: p1.as_i64(),
                last_commit: Clock::from_u64(100).as_u64(),
                hinted_commit: 0,
                offset: 500,
            },
            shuffle::ProducerFrontier {
                producer: p3.as_i64(),
                last_commit: Clock::from_u64(200).as_u64(),
                hinted_commit: 0,
                offset: 200,
            },
        ]);
        assert_eq!(offset, 200, "min uncommitted begin");
    }

    #[test]
    fn test_sequence_not_before_not_after() {
        let mut bindings = vec![test_binding(0, true, None, "/suffix")];
        bindings[0].not_before = Clock::from_u64(100);
        bindings[0].not_after = Clock::from_u64(500);

        let mut s = test_state(bindings);

        let p1 = producer(0x01);

        // Start read with p1 in the checkpoint.
        let (_offset, producers) = resolve_checkpoint(vec![checkpoint_entry(&p1, 0)]);
        s.reads.push(ReadState {
            binding_index: 0,
            journal: "test/journal/A".into(),
            settled: producers,
            pending: Default::default(),
        });

        // Clock before notBefore: suppresses enqueue but not commit.
        let seq = sequence_document(
            &s.reads[0],
            &s.bindings[0],
            &meta(p1, Clock::from_u64(50), OUTSIDE, 0, 50),
        )
        .unwrap();
        assert!(!seq.is_enqueue, "before notBefore → no enqueue");
        assert!(seq.is_commit, "before notBefore → commit still propagates");
        s.commit(0, p1, seq);

        // Clock within range: normal enqueue.
        let seq = sequence_document(
            &s.reads[0],
            &s.bindings[0],
            &meta(p1, Clock::from_u64(200), OUTSIDE, 50, 100),
        )
        .unwrap();
        assert!(seq.is_enqueue, "within range → enqueue");
        assert!(seq.is_commit, "within range → commit");
        s.commit(0, p1, seq);

        // Clock at notAfter boundary: suppresses enqueue (notAfter is exclusive).
        let seq = sequence_document(
            &s.reads[0],
            &s.bindings[0],
            &meta(p1, Clock::from_u64(500), OUTSIDE, 100, 150),
        )
        .unwrap();
        assert!(!seq.is_enqueue, "at notAfter → no enqueue");
        assert!(seq.is_commit, "at notAfter → commit still propagates");
    }

    #[test]
    fn test_flush_state_machine() {
        let mut s = test_state(vec![test_binding(0, true, None, "/suffix")]);

        let p1 = producer(0x01);
        let p3 = producer(0x03);

        // Start read with both producers in the checkpoint.
        let (_offset, producers) =
            resolve_checkpoint(vec![checkpoint_entry(&p1, 0), checkpoint_entry(&p3, 0)]);
        s.reads.push(ReadState {
            binding_index: 0,
            journal: "test/journal/A".into(),
            settled: producers,
            pending: Default::default(),
        });

        // Initially: flush not ready → should_flush false.
        assert!(!s.flush.should_flush());

        // Sequence an OUTSIDE commit (sets flush ready via commit_enqueue).
        let seq = sequence_document(
            &s.reads[0],
            &s.bindings[0],
            &meta(p1, Clock::from_unix(100, 0), OUTSIDE, 0, 50),
        )
        .unwrap();
        s.commit(0, p1, seq);

        // Now should_flush is true.
        assert!(s.flush.should_flush());

        // Add a second producer's document for richer frontier.
        let seq = sequence_document(
            &s.reads[0],
            &s.bindings[0],
            &meta(p3, Clock::from_unix(200, 0), OUTSIDE, 50, 100),
        )
        .unwrap();
        s.commit(0, p3, seq);

        // Build frontier and start flush with 3 members.
        let frontier = super::super::producer::build_flush_frontier(&s.reads, std::iter::empty());
        for read in s.reads.iter_mut() {
            read.settled.extend(read.pending.drain());
        }
        assert!(!frontier.journals.is_empty(), "flushing frontier built");

        let flush_seq = s.flush.start(3, frontier);
        assert_eq!(flush_seq, 1, "flush_seq is the current seq for this cycle");
        assert!(!s.flush.should_flush(), "not ready after start");
        assert!(s.reads[0].pending.is_empty(), "pending drained to settled");

        // Partial flushed: still in flight.
        assert!(s.flush.on_flushed(0).is_none(), "still in flight after 1/3");
        assert!(s.flush.on_flushed(2).is_none(), "still in flight after 2/3");

        // All flushed: returns completed frontier. Seq advances so that
        // a duplicate Flushed with the old seq is rejected as a protocol violation.
        let completed = s.flush.on_flushed(1).expect("all flushed");
        assert_eq!(s.flush.seq, 2, "seq incremented after all members flushed");
        s.progress.on_flush_completed(completed);

        insta::assert_debug_snapshot!("flush_state_machine", &s.progress.flushed);
    }

    #[test]
    fn test_progress_reporting() {
        let mut progress = ProgressState::new();

        // No request + no flushed → None.
        assert!(progress.take_progressed().is_none());

        // Request but no flushed → None.
        progress.request().unwrap();
        assert!(progress.take_progressed().is_none());
        // requested should remain true.
        assert!(progress.requested);

        // Populate flushed directly for this test.
        progress.flushed = crate::Frontier {
            journals: vec![crate::JournalFrontier {
                journal: "test/journal/A".into(),
                binding: 0,
                producers: vec![crate::ProducerFrontier {
                    producer: producer(0x01),
                    last_commit: Clock::from_u64(100),
                    hinted_commit: Clock::from_u64(0),
                    offset: -500,
                }],
            }],
        };

        // Request + flushed → Some(frontier), both cleared.
        let frontier = progress.take_progressed().expect("request + flushed");
        assert!(!progress.requested, "requested cleared");
        assert!(progress.flushed.journals.is_empty(), "flushed cleared");
        assert_eq!(frontier.journals.len(), 1);

        // Double request is an error.
        progress.request().unwrap();
        let err = progress.request().unwrap_err();
        assert!(
            format!("{err}").contains("already pending"),
            "expected double-request error, got: {err}"
        );
    }

    #[test]
    fn test_clock_delay() {
        let mut now = Clock::from_unix(100, 0);

        // Clock in the past: no delay, update_now not called.
        let past = Clock::from_unix(50, 0);
        assert!(clock_delay(&past, &mut now, || panic!("should not refresh")).is_none());

        // Clock equal to now: no delay.
        let equal = Clock::from_unix(100, 0);
        assert!(clock_delay(&equal, &mut now, || panic!("should not refresh")).is_none());

        // Clock in the future, but refresh catches up: no delay.
        let future1 = Clock::from_unix(110, 0);
        assert!(clock_delay(&future1, &mut now, || Clock::from_unix(110, 0)).is_none());
        assert_eq!(now, Clock::from_unix(110, 0), "now was refreshed");

        // Clock in the future even after refresh: returns remaining delay.
        let future2 = Clock::from_unix(120, 0);
        let delay = clock_delay(&future2, &mut now, || Clock::from_unix(115, 0)).unwrap();
        assert_eq!(delay, std::time::Duration::from_secs(5));
        assert_eq!(now, Clock::from_unix(115, 0), "now was refreshed");
    }

    /// Tests the offset tracking and is_enqueue/is_commit disposition that
    /// sequence_document layers on top of uuid::sequence (which has its own
    /// exhaustive tests for clock state transitions).
    #[test]
    fn test_sequence_offset_and_disposition() {
        let bindings = vec![test_binding(0, true, None, "/suffix")];
        let mut s = test_state(bindings);

        let p1 = producer(0x01);

        let (_offset, producers) = resolve_checkpoint(vec![checkpoint_entry(&p1, 0)]);
        s.reads.push(ReadState {
            binding_index: 0,
            journal: "test/journal/A".into(),
            settled: producers,
            pending: Default::default(),
        });

        // ContinueBeginSpan: enqueued, no commit, offset set to begin_offset.
        let seq = sequence_document(
            &s.reads[0],
            &s.bindings[0],
            &meta(p1, Clock::from_unix(10, 0), CONTINUE, 100, 150),
        )
        .unwrap();
        assert!(seq.is_enqueue);
        assert!(!seq.is_commit);
        assert_eq!(
            seq.producer_state.offset, 100,
            "offset = begin of uncommitted span"
        );
        s.commit(0, p1, seq);

        // ContinueExtendSpan: enqueued, no commit, offset preserved from BeginSpan.
        let seq = sequence_document(
            &s.reads[0],
            &s.bindings[0],
            &meta(p1, Clock::from_unix(20, 0), CONTINUE, 150, 200),
        )
        .unwrap();
        assert!(seq.is_enqueue);
        assert!(!seq.is_commit);
        assert_eq!(seq.producer_state.offset, 100, "offset unchanged on extend");
        s.commit(0, p1, seq);

        // ContinueDuplicate: filtered out entirely.
        let seq = sequence_document(
            &s.reads[0],
            &s.bindings[0],
            &meta(p1, Clock::from_unix(15, 0), CONTINUE, 200, 250),
        )
        .unwrap();
        assert!(!seq.is_enqueue);
        assert!(!seq.is_commit);

        // AckCommit: not enqueued, triggers commit/flush, offset becomes committed.
        let seq = sequence_document(
            &s.reads[0],
            &s.bindings[0],
            &meta(p1, Clock::from_unix(20, 0), ACK, 250, 300),
        )
        .unwrap();
        assert!(!seq.is_enqueue, "ACKs are never enqueued");
        assert!(seq.is_commit, "AckCommit triggers flush");
        assert_eq!(
            seq.producer_state.offset, -300,
            "offset = negated committed end"
        );
        s.commit(0, p1, seq);

        // AckDuplicate: no enqueue, no commit (no spurious flush).
        let seq = sequence_document(
            &s.reads[0],
            &s.bindings[0],
            &meta(p1, Clock::from_unix(20, 0), ACK, 300, 350),
        )
        .unwrap();
        assert!(!seq.is_enqueue);
        assert!(!seq.is_commit, "AckDuplicate must not trigger flush");

        // Start a new CONTINUE span, then clean rollback (ACK at last_commit clock).
        let seq = sequence_document(
            &s.reads[0],
            &s.bindings[0],
            &meta(p1, Clock::from_unix(30, 0), CONTINUE, 350, 400),
        )
        .unwrap();
        assert!(seq.is_enqueue);
        assert_eq!(seq.producer_state.offset, 350, "new span begin");
        s.commit(0, p1, seq);

        let seq = sequence_document(
            &s.reads[0],
            &s.bindings[0],
            &meta(p1, Clock::from_unix(20, 0), ACK, 400, 450),
        )
        .unwrap();
        assert!(!seq.is_enqueue);
        assert!(seq.is_commit, "AckCleanRollback triggers flush");
        assert_eq!(
            seq.producer_state.offset, -450,
            "rollback yields committed offset"
        );
    }

    /// Build a passthrough PartitionFilter that accepts any value for the given fields.
    /// The field count must match the partition field segments in the journal suffix.
    fn passthrough_filter(fields: &[&str]) -> PartitionFilter {
        let fields: Vec<String> = fields.iter().map(|f| f.to_string()).collect();
        PartitionFilter::new(&fields, &broker::LabelSelector::default())
    }

    #[test]
    fn test_hint_index_lookup() {
        // Example filter that includes "alpha" category and excludes "bad" region.
        let filter = PartitionFilter::new(
            &["category".to_string(), "region".to_string()],
            &broker::LabelSelector {
                include: Some(labels::build_set(
                    [(
                        "estuary.dev/field/category".to_string(),
                        "alpha".to_string(),
                    )]
                    .into_iter(),
                )),
                exclude: Some(labels::build_set(
                    [("estuary.dev/field/region".to_string(), "bad".to_string())].into_iter(),
                )),
            },
        );

        let cases: Vec<(Vec<(&str, u32, u32, PartitionFilter)>, &str, u32, Vec<u32>)> = vec![
            // Prefix match: anvils has 1 partition field, bananas has 0.
            (
                vec![
                    ("acmeCo/anvils", 0, 0, passthrough_filter(&["part"])),
                    ("acmeCo/bananas", 1, 0, passthrough_filter(&[])),
                ],
                "acmeCo/anvils/part=a/pivot=00",
                0,
                vec![0],
            ),
            (
                vec![
                    ("acmeCo/anvils", 0, 0, passthrough_filter(&["part"])),
                    ("acmeCo/bananas", 1, 0, passthrough_filter(&[])),
                ],
                "acmeCo/bananas/pivot=00",
                0,
                vec![1],
            ),
            // No matching prefix.
            (
                vec![
                    ("acmeCo/anvils", 0, 0, passthrough_filter(&["part"])),
                    ("acmeCo/bananas", 1, 0, passthrough_filter(&[])),
                ],
                "other/collection/pivot=00",
                0,
                vec![],
            ),
            // Cohort filtering: same prefix, different cohorts.
            (
                vec![
                    ("acmeCo/anvils", 0, 0, passthrough_filter(&["part"])),
                    ("acmeCo/anvils", 1, 1, passthrough_filter(&["part"])),
                ],
                "acmeCo/anvils/part=a/pivot=00",
                0,
                vec![0],
            ),
            (
                vec![
                    ("acmeCo/anvils", 0, 0, passthrough_filter(&["part"])),
                    ("acmeCo/anvils", 1, 1, passthrough_filter(&["part"])),
                ],
                "acmeCo/anvils/part=a/pivot=00",
                1,
                vec![1],
            ),
            // Unknown cohort.
            (
                vec![
                    ("acmeCo/anvils", 0, 0, passthrough_filter(&["part"])),
                    ("acmeCo/anvils", 1, 1, passthrough_filter(&["part"])),
                ],
                "acmeCo/anvils/part=a/pivot=00",
                99,
                vec![],
            ),
            // Multiple bindings, same prefix and cohort.
            (
                vec![
                    ("acmeCo/anvils", 0, 0, passthrough_filter(&["part"])),
                    ("acmeCo/anvils", 1, 0, passthrough_filter(&["part"])),
                ],
                "acmeCo/anvils/part=a/pivot=00",
                0,
                vec![0, 1],
            ),
            // Partition filter: "alpha" is included, "eu" not excluded.
            (
                vec![("acmeCo/anvils", 0, 0, filter.clone())],
                "acmeCo/anvils/category=alpha/region=eu/pivot=00",
                0,
                vec![0],
            ),
            // Partition filter: "beta" is not included, "eu" not excluded.
            (
                vec![("acmeCo/anvils", 0, 0, filter.clone())],
                "acmeCo/anvils/category=beta/region=eu/pivot=00",
                0,
                vec![],
            ),
            // Partition filter: "alpha" is included, "bad" is excluded.
            (
                vec![("acmeCo/anvils", 0, 0, filter.clone())],
                "acmeCo/anvils/category=beta/region=bad/pivot=00",
                0,
                vec![],
            ),
        ];

        let mut out = Vec::new();
        for (entries, journal, cohort, expected) in cases {
            let index = HintIndex::new(entries.into_iter());
            index.lookup(&journal, cohort, &mut out).unwrap();
            out.sort();
            assert_eq!(&out, &expected, "journal={journal}, cohort={cohort}");
        }
    }

    fn test_hint_index() -> HintIndex {
        // anvils has 1 partition field ("part"), bananas has 0.
        HintIndex::new(
            [
                ("acmeCo/anvils", 0, 0, passthrough_filter(&["part"])),
                ("acmeCo/bananas", 1, 0, passthrough_filter(&[])),
            ]
            .into_iter(),
        )
    }

    #[test]
    fn test_extract_causal_hints_round_trip() {
        let index = test_hint_index();
        let mut causal_hints = CausalHints::default();

        let p1 = producer(0x00);
        let p2 = producer(0x02);

        // Build a transaction with two producers across two journals.
        let txn = vec![
            (
                p1,
                Clock::from_u64(100),
                vec![
                    "acmeCo/anvils/part=a/pivot=00".to_string(),
                    "acmeCo/bananas/pivot=00".to_string(),
                ],
            ),
            (
                p2,
                Clock::from_u64(200),
                vec!["acmeCo/anvils/part=a/pivot=00".to_string()],
            ),
        ];

        let journal_acks = publisher::intents::build_transaction_intents(&txn);

        // For each journal's first ACK, extract hints into causal_hints.
        for (journal, acks) in &journal_acks {
            let ack = &acks[0];
            let uuid_str = ack["_meta"]["uuid"].as_str().unwrap();
            let (ack_producer, commit_clock, _flags) = uuid::parse_str(uuid_str).unwrap();

            extract_causal_hints(
                &index,
                journal,
                0, // cohort
                ack_producer,
                commit_clock,
                ack,
                &mut causal_hints,
            )
            .unwrap();
        }

        // Collect into sorted tuples for deterministic assertion.
        let mut entries: Vec<_> = causal_hints
            .iter()
            .map(|((j, b), hints)| {
                let mut h: Vec<_> = hints.iter().map(|(p, c)| (p, c)).collect();
                h.sort();
                (j.as_ref().to_string(), *b, h)
            })
            .collect();
        entries.sort();

        insta::assert_debug_snapshot!(entries);
    }

    #[test]
    fn test_extract_causal_hints_no_hints_and_decode_error() {
        let index = test_hint_index();
        let p1 = producer(0x00);

        // ACK doc without hints field: causal_hints remains empty.
        let mut causal_hints = CausalHints::default();
        let doc = serde_json::json!({"_meta": {"uuid": "00000000-0000-0000-0000-000000000000"}});
        extract_causal_hints(
            &index,
            "acmeCo/anvils/x",
            0,
            p1,
            Clock::from_u64(100),
            &doc,
            &mut causal_hints,
        )
        .unwrap();
        assert!(causal_hints.is_empty());

        // Malformed hints field: returns decode error.
        let doc = serde_json::json!({"hints": [{"j": "not-an-array", "p": []}]});
        let err = extract_causal_hints(
            &index,
            "acmeCo/anvils/x",
            0,
            p1,
            Clock::from_u64(100),
            &doc,
            &mut causal_hints,
        )
        .unwrap_err();
        assert!(
            format!("{err}").contains("decoding causal hint"),
            "error should mention decoding: {err}"
        );
    }
}
