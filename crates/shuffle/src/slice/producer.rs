use proto_gazette::uuid::{Clock, Producer};

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
#[derive(Debug, Clone)]
pub struct ProducerState {
    /// Clock of the last committing ACK_TXN or OUTSIDE_TXN.
    pub last_commit: Clock,
    /// Maximum Clock of an uncommitted CONTINUE_TXN, or zero if no pending span.
    pub max_continue: Clock,
    /// Journal byte offset, sign-encoded (see struct docs).
    pub offset: i64,
}

impl Default for ProducerState {
    fn default() -> Self {
        Self {
            last_commit: Clock::zero(),
            max_continue: Clock::zero(),
            offset: 0,
        }
    }
}
const _: () = assert!(std::mem::size_of::<ProducerState>() == 24);

/// Build a [`crate::Frontier`] by reducing read-derived producer state with
/// causal hints.
///
/// `reads` provides the journal name, binding index, and pending producers
/// for each active read. `hints` yields owned `((journal, binding),
/// Vec<(producer, hinted_clock)>)` entries, typically from a HashMap drain.
///
/// Both inputs may arrive in arbitrary order; outputs are sorted.
pub fn build_flush_frontier(
    reads: &[super::read::ReadState],
    hints: impl Iterator<Item = ((Box<str>, u16), Vec<(Producer, Clock)>)>,
    member_count: usize,
) -> crate::Frontier {
    // Build JournalFrontier entries from read-derived pending producers.
    let mut by_journal: Vec<(&str, u16, Vec<crate::ProducerFrontier>)> = Vec::new();

    for read_state in reads.iter() {
        if read_state.pending.is_empty() {
            continue;
        }
        let mut producers: Vec<_> = read_state
            .pending
            .iter()
            .map(|(producer, ps)| crate::ProducerFrontier {
                producer: *producer,
                last_commit: ps.last_commit,
                hinted_commit: Clock::from_u64(0),
                offset: ps.offset,
            })
            .collect();
        producers.sort_by(|a, b| a.producer.cmp(&b.producer));

        by_journal.push((&read_state.journal, read_state.binding_index, producers));
    }

    by_journal.sort_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(b.0)));

    let reads_frontier = crate::Frontier {
        journals: by_journal
            .into_iter()
            .map(
                |(journal_name, binding, producers)| crate::JournalFrontier {
                    binding,
                    journal: journal_name.into(),
                    producers,
                },
            )
            .collect(),
        flushed_lsn: vec![crate::log::Lsn::ZERO; member_count],
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
            }
        })
        .collect();

    // Sort to restore the sorted Frontier invariant
    // (entries must be unique since they come from HashMap keys).
    hint_journals.sort_by(|a, b| a.binding.cmp(&b.binding).then(a.journal.cmp(&b.journal)));

    reads_frontier.reduce(crate::Frontier {
        journals: hint_journals,
        flushed_lsn: vec![],
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
        let mut map = ProducerMap::default();
        for &(id, last_commit, offset) in pending {
            map.insert(
                producer(id),
                ProducerState {
                    last_commit: Clock::from_u64(last_commit),
                    max_continue: Clock::zero(),
                    offset,
                },
            );
        }
        super::super::read::ReadState {
            binding_index: binding,
            journal: journal.into(),
            settled: ProducerMap::default(),
            pending: map,
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
            (
                "reads_only",
                vec![
                    read_state("journal/B", 0, &[(0x03, 200, -1000)]),
                    read_state("journal/A", 0, &[(0x01, 100, -500)]),
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
            (
                "reads_and_hints",
                vec![
                    read_state("journal/A", 0, &[(0x01, 100, -500)]),
                    read_state("journal/B", 0, &[(0x03, 200, -1000), (0x05, 50, -200)]),
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

        let snap = cases
            .into_iter()
            .map(|(name, reads, hints)| {
                let f = build_flush_frontier(&reads, hints.into_iter(), 3);
                (name, f)
            })
            .collect::<Vec<_>>();

        insta::assert_debug_snapshot!(snap);
    }
}
