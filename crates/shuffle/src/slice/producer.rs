use proto_gazette::uuid::{Clock, Producer};
use std::collections::BTreeMap;

/// A `BuildHasher` for `Producer`-keyed maps that passes through the
/// raw bytes as the hash value. Producer IDs are already uniformly
/// distributed random values, so rehashing them with SipHash is wasted work.
#[derive(Clone, Default)]
pub struct ProducerHasher;

impl std::hash::BuildHasher for ProducerHasher {
    type Hasher = ProducerHasherState;

    #[inline]
    fn build_hasher(&self) -> Self::Hasher {
        ProducerHasherState(0)
    }
}

/// Hasher state for [`ProducerHasher`]. Packs written bytes into a `u64`.
pub struct ProducerHasherState(u64);

impl std::hash::Hasher for ProducerHasherState {
    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }

    #[inline]
    fn write(&mut self, _bytes: &[u8]) {
        unreachable!("ProducerHasherState may only be used with Producer");
    }

    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
}

/// Map keyed by `Producer` using a passthrough hasher. Producer IDs are
/// already uniformly distributed random values, so we skip rehashing.
pub type ProducerMap<V> = std::collections::HashMap<Producer, V, ProducerHasher>;

/// Per-producer sequencing state.
///
/// It's scoped to a single (binding, journal) tuple because an ACK_TXN in
/// journal J commits only that producer's preceding CONTINUE_TXN documents in J.
/// It does NOT commit the same producer's documents in other journals, which
/// will have their own ACKs. Cross-journal commit visibility is coordinated at
/// the Session level via causal hints extracted from ACK documents
/// (see [`extract_causal_hints`]).
///
/// It's additionally binding-scoped because we create an independent ReadState
/// for each (binding, journal) tuple, and separately track producer states
/// for each one.
///
/// `offset` encodes journal position using the same sign convention as the
/// wire format (`ProducerFrontier.offset`):
///   - Non-negative: Begin offset of first pending CONTINUE_TXN
///   - Negative: Negation of end offset of last committing ACK_TXN / OUTSIDE_TXN
/// Internal default state uses zero before any document has been observed.
///
/// Backfill begin/complete control documents carry `Flag_CONTROL` (which
/// implies `OUTSIDE_TXN`) and are immediately committed on first observation;
/// there is no CONTINUE_TXN / ACK_TXN staging step. Observed clocks are placed
/// directly into the `backfill_begin` / `backfill_complete` fields, which are
/// drained into the checkpoint frontier at the next flush cycle.
#[derive(Debug, Clone)]
pub struct ProducerState {
    /// Clock of the last committing ACK_TXN or OUTSIDE_TXN.
    pub last_commit: Clock,
    /// Maximum Clock of an uncommitted CONTINUE_TXN, or zero if no pending span.
    pub max_continue: Clock,
    /// Journal byte offset, sign-encoded (see struct docs).
    pub offset: i64,
    /// Latest observed `BackfillBegin` clock awaiting flush drain.
    pub backfill_begin: Option<Clock>,
    /// Latest observed `BackfillComplete` clock awaiting flush drain.
    pub backfill_complete: Option<Clock>,
}

impl Default for ProducerState {
    fn default() -> Self {
        Self {
            last_commit: Clock::zero(),
            max_continue: Clock::zero(),
            offset: 0,
            backfill_begin: None,
            backfill_complete: None,
        }
    }
}

/// Build a [`crate::Frontier`] by reducing read-derived producer state with
/// causal hints.
///
/// `reads` provides the journal name, binding index, and pending producers
/// for each active read. `bindings` maps binding indices to `journal_read_suffix`
/// for keying backfill metadata. `hints` yields owned `((journal, binding),
/// Vec<(producer, hinted_clock)>)` entries, typically from a HashMap drain.
///
/// `reads` and `hints` may arrive in arbitrary order; outputs are sorted.
/// Observed backfill clocks are drained directly from pending producer state.
pub fn build_flush_frontier(
    reads: &mut [super::read::ReadState],
    bindings: &[crate::Binding],
    hints: impl Iterator<Item = ((Box<str>, u16), Vec<(Producer, Clock)>)>,
    member_count: usize,
) -> crate::Frontier {
    // Walk all journal reads to build their JournalFrontier.
    let mut journals: Vec<crate::JournalFrontier> = Vec::new();
    let mut latest_backfill_begin = BTreeMap::<String, Clock>::new();
    let mut latest_backfill_complete = BTreeMap::<String, Clock>::new();

    for read_state in reads.iter_mut() {
        if read_state.pending.is_empty() {
            // No reportable progress for this journal since the last flush.
            // We intentionally defer offset-based reporting as well:
            // the next reported deltas are computed from prev_read_offset
            // and prev_write_head, so reported values are eventually correct
            // even if offsets advanced meanwhile.
            continue;
        }

        let suffix = &bindings[read_state.binding_index as usize].journal_read_suffix;
        let mut producers = Vec::with_capacity(read_state.pending.len());
        for (&producer, ps) in read_state.pending.iter_mut() {
            if let Some(clock) = ps.backfill_begin.take() {
                latest_backfill_begin
                    .entry(suffix.clone())
                    .and_modify(|c| *c = (*c).max(clock))
                    .or_insert(clock);
            }
            if let Some(clock) = ps.backfill_complete.take() {
                latest_backfill_complete
                    .entry(suffix.clone())
                    .and_modify(|c| *c = (*c).max(clock))
                    .or_insert(clock);
            }

            producers.push(crate::ProducerFrontier {
                producer,
                last_commit: ps.last_commit,
                hinted_commit: Clock::from_u64(0),
                offset: ps.offset,
            });
        }
        producers.sort_by(|a, b| a.producer.cmp(&b.producer));

        let bytes_read_delta = read_state.read_offset - read_state.prev_read_offset;
        let bytes_behind_delta = (read_state.write_head - read_state.read_offset)
            - (read_state.prev_write_head - read_state.prev_read_offset);

        journals.push(crate::JournalFrontier {
            binding: read_state.binding_index,
            journal: read_state.journal.clone().into(),
            producers,
            bytes_read_delta,
            bytes_behind_delta,
        });

        // Update the baselines for the next delta computation.
        read_state.prev_read_offset = read_state.read_offset;
        read_state.prev_write_head = read_state.write_head;
        read_state.settled.extend(read_state.pending.drain());
    }

    journals.sort_by(|a, b| a.journal.cmp(&b.journal).then(a.binding.cmp(&b.binding)));

    let reads_frontier = crate::Frontier {
        journals,
        flushed_lsn: vec![crate::log::Lsn::ZERO; member_count],
        latest_backfill_begin,
        latest_backfill_complete,
    };

    // Build a Frontier from causal hints via single-pass iteration.
    let mut hint_journals: Vec<crate::JournalFrontier> = hints
        .map(|((journal, binding), producers)| {
            let mut producers: Vec<_> = producers
                .into_iter()
                .map(|(producer, hinted_clock)| crate::ProducerFrontier {
                    producer,
                    last_commit: Clock::from_u64(0),
                    hinted_commit: hinted_clock,
                    offset: 0,
                })
                .collect();

            producers.sort_by(|a, b| a.producer.cmp(&b.producer));
            producers.dedup_by(|b, a| {
                a.producer == b.producer && {
                    a.hinted_commit = a.hinted_commit.max(b.hinted_commit);
                    true
                }
            });

            crate::JournalFrontier {
                binding,
                journal,
                producers,
                bytes_read_delta: 0,
                bytes_behind_delta: 0,
            }
        })
        .collect();

    // Sort to restore the sorted Frontier invariant
    // (entries must be unique since they come from HashMap keys).
    hint_journals.sort_by(|a, b| a.journal.cmp(&b.journal).then(a.binding.cmp(&b.binding)));

    reads_frontier.reduce(crate::Frontier {
        journals: hint_journals,
        flushed_lsn: vec![],
        latest_backfill_begin: Default::default(),
        latest_backfill_complete: Default::default(),
    })
}

#[cfg(test)]
mod test {
    use super::*;

    fn producer(id: u8) -> Producer {
        Producer::from_bytes([id | 0x01, 0, 0, 0, 0, 0])
    }

    fn read_state(
        journal: &str,
        binding: u16,
        pending: &[(u8, u64, i64)],
    ) -> super::super::read::ReadState {
        read_state_with_bytes(journal, binding, pending, 0, 0, 0, 0)
    }

    fn read_state_with_bytes(
        journal: &str,
        binding: u16,
        pending: &[(u8, u64, i64)],
        prev_read_offset: i64,
        write_head: i64,
        read_offset: i64,
        prev_write_head: i64,
    ) -> super::super::read::ReadState {
        let mut map = ProducerMap::default();
        for &(id, last_commit, offset) in pending {
            map.insert(
                producer(id),
                ProducerState {
                    last_commit: Clock::from_u64(last_commit),
                    max_continue: Clock::zero(),
                    offset,
                    backfill_begin: None,
                    backfill_complete: None,
                },
            );
        }
        super::super::read::ReadState {
            binding_index: binding,
            journal: journal.into(),
            settled: ProducerMap::default(),
            pending: map,
            read_offset,
            prev_read_offset,
            write_head,
            prev_write_head,
        }
    }

    fn hint(
        journal: &str,
        binding: u16,
        producers: &[(u8, u64)],
    ) -> ((Box<str>, u16), Vec<(Producer, Clock)>) {
        (
            (journal.into(), binding),
            producers
                .iter()
                .map(|&(id, clock)| (producer(id), Clock::from_u64(clock)))
                .collect(),
        )
    }

    #[test]
    fn test_build_flush_frontier() {
        // (case_name, reads, hints)
        let cases: Vec<(
            &str,
            Vec<super::super::read::ReadState>,
            Vec<((Box<str>, u16), Vec<(Producer, Clock)>)>,
        )> = vec![
            // Both empty.
            ("empty", vec![], vec![]),
            // Reads only, reverse input order verifies sorting.
            // Non-zero byte tracking: journal/B is 5000 bytes behind
            // (write_head=50000, read_offset=45000, prev_write_head=43800, prev_read_offset=43800
            //  → behind_delta = 5000 - 0 = 5000),
            // with offset advancement of 1200 (45000-43800). journal/A is catching up (delta=-300).
            (
                "reads_only",
                vec![
                    read_state_with_bytes(
                        "journal/B",
                        0,
                        &[(0x03, 200, -1000)],
                        43800,
                        50000,
                        45000,
                        43800,
                    ),
                    read_state_with_bytes(
                        "journal/A",
                        0,
                        &[(0x01, 100, -500)],
                        8700,
                        10000,
                        9500,
                        9500,
                    ),
                    // No pending producers: not part of frontier, not modified.
                    read_state_with_bytes("journal/C", 0, &[], 0, 25000, 123, 456),
                ],
                vec![],
            ),
            // Hints only, reverse input order verifies sorting.
            (
                "hints_only",
                vec![],
                vec![
                    hint("journal/C", 1, &[(0x03, 300)]),
                    hint("journal/A", 0, &[(0x01, 150)]),
                ],
            ),
            // Empty-pending reads are skipped.
            (
                "empty_pending_skipped",
                vec![
                    read_state("journal/A", 0, &[(0x01, 100, -500)]),
                    read_state("journal/B", 0, &[]),
                ],
                vec![],
            ),
            // Reads and hints reduce: journal/A reads-only, journal/B merged
            // (producer 0x03 gets hint), journal/C hints-only.
            // Offset advancement: journal/A has 500 bytes_read_delta (19000-18500), journal/B has 2000 (75000-73000).
            // Hint-only journal/C gets 0 for both byte fields.
            (
                "reads_and_hints",
                vec![
                    read_state_with_bytes(
                        "journal/A",
                        0,
                        &[(0x01, 100, -500)],
                        18500,
                        20000,
                        19000,
                        19000,
                    ),
                    read_state_with_bytes(
                        "journal/B",
                        0,
                        &[(0x03, 200, -1000), (0x05, 50, -200)],
                        73000,
                        80000,
                        75000,
                        76000,
                    ),
                ],
                vec![
                    hint("journal/B", 0, &[(0x03, 300)]),
                    hint("journal/C", 1, &[(0x03, 300)]),
                ],
            ),
            // Same journal, different bindings: sorted by (journal, binding),
            // each binding's producers independent.
            (
                "same_journal_diff_bindings",
                vec![
                    read_state("journal/X", 2, &[(0x01, 100, -400)]),
                    read_state("journal/X", 0, &[(0x03, 50, -200)]),
                ],
                vec![hint("journal/X", 1, &[(0x05, 250)])],
            ),
            // Duplicate hint producers: same producer hinted twice with
            // different clocks (from two ACK documents). Should be deduped
            // to a single entry with the max clock.
            (
                "duplicate_hint_producers",
                vec![],
                vec![hint("journal/A", 0, &[(0x01, 100), (0x01, 200)])],
            ),
            // Duplicate hint producers merged with reads: the deduped hint
            // should merge cleanly with the read-derived entry.
            (
                "duplicate_hints_merged_with_reads",
                vec![read_state("journal/A", 0, &[(0x01, 50, -300)])],
                vec![hint("journal/A", 0, &[(0x01, 100), (0x01, 200)])],
            ),
        ];

        let bindings = vec![
            crate::testing::test_binding(0, true, None, "suffix/0"),
            crate::testing::test_binding(1, true, None, "suffix/1"),
            crate::testing::test_binding(2, true, None, "suffix/2"),
        ];

        let snap = cases
            .into_iter()
            .map(|(name, mut reads, hints)| {
                let f = build_flush_frontier(&mut reads, &bindings, hints.into_iter(), 3);
                (name, f, reads)
            })
            .collect::<Vec<_>>();

        insta::assert_debug_snapshot!(snap);
    }

    #[test]
    fn test_build_flush_frontier_drains_backfill_clocks_from_pending_producers() {
        let mut has_both = read_state("journal/A", 0, &[(0x01, 100, -500)]);
        let both_producer = producer(0x01);
        has_both
            .pending
            .get_mut(&both_producer)
            .unwrap()
            .backfill_begin = Some(Clock::from_u64(90));
        has_both
            .pending
            .get_mut(&both_producer)
            .unwrap()
            .backfill_complete = Some(Clock::from_u64(100));

        // A producer that observed only a begin (without matching complete yet).
        let mut begin_only = read_state("journal/B", 0, &[(0x03, 0, 700)]);
        let begin_only_producer = producer(0x03);
        begin_only
            .pending
            .get_mut(&begin_only_producer)
            .unwrap()
            .backfill_begin = Some(Clock::from_u64(80));

        let bindings = vec![crate::testing::test_binding(0, true, None, "suffix/0")];
        let mut reads = vec![has_both, begin_only];

        let frontier = build_flush_frontier(&mut reads, &bindings, std::iter::empty(), 2);

        assert_eq!(
            frontier.latest_backfill_begin.get("suffix/0"),
            Some(&Clock::from_u64(90))
        );
        assert_eq!(
            frontier.latest_backfill_complete.get("suffix/0"),
            Some(&Clock::from_u64(100))
        );

        assert!(reads[0].pending.is_empty());
        assert!(reads[1].pending.is_empty());
        assert_eq!(
            reads[0].settled.get(&both_producer).unwrap().backfill_begin,
            None
        );
        assert_eq!(
            reads[0]
                .settled
                .get(&both_producer)
                .unwrap()
                .backfill_complete,
            None
        );
        assert_eq!(
            reads[1]
                .settled
                .get(&begin_only_producer)
                .unwrap()
                .backfill_begin,
            None
        );
        assert_eq!(
            reads[1]
                .settled
                .get(&begin_only_producer)
                .unwrap()
                .backfill_complete,
            None
        );
    }
}
