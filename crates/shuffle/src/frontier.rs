use crate::log;
use proto_flow::shuffle;
use proto_gazette::uuid::{Clock, Producer};

/// Frontier state of a single producer within a journal.
#[derive(Debug, Clone)]
pub struct ProducerFrontier {
    pub producer: Producer,
    /// Clock of the last committing ACK_TXN or OUTSIDE_TXN.
    pub last_commit: Clock,
    /// Clock of a hinted (causal) commit, or zero if no hint.
    pub hinted_commit: Clock,
    /// `offset` encodes journal position using positive/negative sign convention:
    /// - Non-negative: begin offset of first pending CONTINUE_TXN.
    /// - Negative: negation of the end offset of last committing ACK_TXN / OUTSIDE_TXN.
    pub offset: i64,
}

impl ProducerFrontier {
    /// Reduce two ProducerFrontier entries for the same producer.
    ///
    /// Maximizes `last_commit` and `hinted_commit`. Takes `offset` with the
    /// largest absolute value, because the sign encodes semantics (negative =
    /// committed end, non-negative = uncommitted begin) and the magnitude
    /// represents how far into the journal we've read.
    pub fn reduce(self, other: Self) -> Self {
        // We cannot simply take the offset from whichever side has the larger
        // `last_commit`, because causal hint resolution (`resolve_hints`) elevates
        // `last_commit` on hint-only entries that carry `offset: 0`. When such a
        // resolved entry is reduced into `ready`, the elevated `last_commit` would
        // win and its zero offset would overwrite the actual journal position from
        // the read-derived side.
        let offset = if self.offset.abs() >= other.offset.abs() {
            self.offset
        } else {
            other.offset
        };
        Self {
            producer: self.producer,
            last_commit: self.last_commit.max(other.last_commit),
            hinted_commit: self.hinted_commit.max(other.hinted_commit),
            offset,
        }
    }
}

/// Frontier state for a single journal under a specific binding.
#[derive(Debug, Clone)]
pub struct JournalFrontier {
    /// Journal name.
    pub journal: Box<str>,
    /// Binding index under which the journal is read.
    pub binding: u16,
    /// Producers of this journal.
    /// Entries are sorted and unique on `producer`.
    pub producers: Vec<ProducerFrontier>,
    /// Delta of journal bytes read since the last checkpoint.
    /// Summed during reduction.
    pub bytes_read_delta: i64,
    /// Delta of bytes-behind (write_head - read_offset) since last checkpoint.
    /// Positive when the reader is falling behind, negative when catching up.
    /// Summed during reduction.
    pub bytes_behind_delta: i64,
}

impl JournalFrontier {
    /// Reduce two JournalFrontier entries for the same (journal, binding)
    /// by sorted-merging their producer lists. Matching producers are reduced
    /// via `ProducerFrontier::reduce`; unmatched producers pass through.
    pub fn reduce(self, other: Self) -> Self {
        let mut merged = Vec::with_capacity(self.producers.len() + other.producers.len());
        let mut a = self.producers.into_iter().peekable();
        let mut b = other.producers.into_iter().peekable();

        loop {
            match (a.peek(), b.peek()) {
                (Some(pa), Some(pb)) => match pa.producer.cmp(&pb.producer) {
                    std::cmp::Ordering::Less => merged.push(a.next().unwrap()),
                    std::cmp::Ordering::Greater => merged.push(b.next().unwrap()),
                    std::cmp::Ordering::Equal => {
                        merged.push(a.next().unwrap().reduce(b.next().unwrap()));
                    }
                },
                (Some(_), None) => {
                    merged.extend(a);
                    break;
                }
                (None, _) => {
                    merged.extend(b);
                    break;
                }
            }
        }
        merged.shrink_to_fit();

        Self {
            journal: self.journal,
            binding: self.binding,
            producers: merged,
            bytes_read_delta: self.bytes_read_delta + other.bytes_read_delta,
            bytes_behind_delta: self.bytes_behind_delta + other.bytes_behind_delta,
        }
    }

    /// Resolve causal hints on producers of `self` using progress from `other`.
    /// Both must be for the same `(journal, binding)`. Returns the count resolved.
    fn resolve_hints(&mut self, other: &JournalFrontier) -> usize {
        let mut resolved = 0usize;
        let mut lhs = self.producers.iter_mut().peekable();
        let mut rhs = other.producers.iter().peekable();

        loop {
            let ord = match (lhs.peek(), rhs.peek()) {
                (Some(l), Some(r)) => l.producer.cmp(&r.producer),
                _ => break,
            };
            match ord {
                std::cmp::Ordering::Less => {
                    lhs.next();
                    continue;
                }
                std::cmp::Ordering::Greater => {
                    rhs.next();
                    continue;
                }
                std::cmp::Ordering::Equal => {}
            }

            let lhs = lhs.next().unwrap();
            let rhs = rhs.next().unwrap();

            if lhs.hinted_commit > lhs.last_commit && rhs.last_commit >= lhs.hinted_commit {
                lhs.last_commit = lhs.hinted_commit;
                resolved += 1;
            }
        }

        resolved
    }

    /// Decode a proto `FrontierChunk` into an iterator of `JournalFrontier`.
    ///
    /// Each chunk is self-contained: the first entry carries the full journal
    /// name (truncate=0, suffix=full name), so decoding requires only
    /// chunk-internal state. This is a pure mapping with no validation;
    /// use `Frontier::new` to validate ordering invariants.
    pub fn decode(chunk: shuffle::FrontierChunk) -> impl Iterator<Item = JournalFrontier> {
        let mut journal_name = String::new();

        chunk.journals.into_iter().map(move |jf| {
            gazette::delta::decode(
                &mut journal_name,
                jf.journal_name_truncate_delta,
                &jf.journal_name_suffix,
            );
            JournalFrontier {
                journal: journal_name.clone().into_boxed_str(),
                binding: jf.binding as u16,
                producers: jf
                    .producers
                    .into_iter()
                    .map(|p| ProducerFrontier {
                        producer: Producer::from_i64(p.producer),
                        last_commit: Clock::from_u64(p.last_commit),
                        hinted_commit: Clock::from_u64(p.hinted_commit),
                        offset: p.offset,
                    })
                    .collect(),
                bytes_read_delta: jf.bytes_read_delta,
                bytes_behind_delta: jf.bytes_behind_delta,
            }
        })
    }

    /// Encode a slice of `JournalFrontier` entries as a proto `FrontierChunk`.
    ///
    /// The chunk is self-contained: the first entry carries the full journal
    /// name (truncate=0, suffix=full name), and subsequent entries are
    /// delta-encoded relative to their predecessor within the chunk.
    pub fn encode(entries: &[Self]) -> shuffle::FrontierChunk {
        let mut prev_journal: &str = "";

        let journals = entries
            .iter()
            .map(|jf| {
                let (truncate_delta, suffix) =
                    gazette::delta::encode(prev_journal, jf.journal.as_ref());
                prev_journal = jf.journal.as_ref();

                shuffle::JournalFrontier {
                    journal_name_truncate_delta: truncate_delta,
                    journal_name_suffix: suffix.to_string(),
                    binding: jf.binding as u32,
                    producers: jf
                        .producers
                        .iter()
                        .map(|p| shuffle::ProducerFrontier {
                            producer: p.producer.as_i64(),
                            last_commit: p.last_commit.as_u64(),
                            hinted_commit: p.hinted_commit.as_u64(),
                            offset: p.offset,
                        })
                        .collect(),
                    bytes_read_delta: jf.bytes_read_delta,
                    bytes_behind_delta: jf.bytes_behind_delta,
                }
            })
            .collect();

        shuffle::FrontierChunk {
            journals,
            flushed_lsn: vec![],
        }
    }
}

/// Frontier tracks journal progress including causal hints.
///
/// A Frontier may represent either a cumulative checkpoint (full state of all
/// journals and producers) or a checkpoint delta (only journals and producers
/// that progressed since the last checkpoint). The `reduce` method merges a
/// delta into a cumulative base: new journals from the delta are added, base
/// journals absent from the delta are preserved, and matching entries are
/// reduced by maximizing clocks.
///
/// See session::CheckpointPipeline for details of how Frontier deltas are built.
#[derive(Debug, Clone, Default)]
pub struct Frontier {
    /// Journals which constitute the frontier.
    /// Entries are sorted and unique on `(journal, binding)`.
    pub journals: Vec<JournalFrontier>,
    /// Per-member flushed LSN (log read-through barrier), indexed by member_index.
    /// Empty when not applicable (e.g. resume checkpoints).
    pub flushed_lsn: Vec<log::Lsn>,
}

impl Frontier {
    /// Construct a `Frontier` from journal entries and per-member flushed LSNs,
    /// validating that entries are sorted and unique on `(journal, binding)` and
    /// that producers within each entry are sorted and unique on `producer`.
    pub fn new(journals: Vec<JournalFrontier>, flushed_lsn: Vec<u64>) -> anyhow::Result<Self> {
        let flushed_lsn = flushed_lsn.into_iter().map(log::Lsn::from_u64).collect();

        for (index, window) in journals.windows(2).enumerate() {
            let (prev, curr) = (&window[0], &window[1]);
            match prev
                .journal
                .as_ref()
                .cmp(curr.journal.as_ref())
                .then(prev.binding.cmp(&curr.binding))
            {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Equal => anyhow::bail!(
                    "JournalFrontier is not unique on (journal, binding) at index {}: ({}, {})",
                    index + 1,
                    curr.journal,
                    curr.binding,
                ),
                std::cmp::Ordering::Greater => anyhow::bail!(
                    "JournalFrontier is not ordered on (journal, binding) at index {}: ({}, {}) follows ({}, {})",
                    index + 1,
                    curr.journal,
                    curr.binding,
                    prev.journal,
                    prev.binding,
                ),
            }
        }
        for jf in &journals {
            for (index, window) in jf.producers.windows(2).enumerate() {
                let (prev, curr) = (&window[0], &window[1]);
                match prev.producer.cmp(&curr.producer) {
                    std::cmp::Ordering::Less => {}
                    std::cmp::Ordering::Equal => anyhow::bail!(
                        "ProducerFrontier is not unique on producer at index {} in ({}, {})",
                        index + 1,
                        jf.binding,
                        jf.journal,
                    ),
                    std::cmp::Ordering::Greater => anyhow::bail!(
                        "ProducerFrontier is not ordered on producer at index {} in ({}, {})",
                        index + 1,
                        jf.binding,
                        jf.journal,
                    ),
                }
            }
        }
        Ok(Self {
            journals,
            flushed_lsn,
        })
    }

    /// Element-wise max of two per-member `flushed_lsn` vectors.
    /// Extends the shorter vector with zeros.
    pub fn merge_flushed_lsn(a: Vec<log::Lsn>, b: Vec<log::Lsn>) -> Vec<log::Lsn> {
        if a.is_empty() {
            return b;
        } else if b.is_empty() {
            return a;
        }
        let len = a.len().max(b.len());
        (0..len)
            .map(|i| {
                let va = a.get(i).copied().unwrap_or(log::Lsn::ZERO);
                let vb = b.get(i).copied().unwrap_or(log::Lsn::ZERO);
                va.max(vb)
            })
            .collect()
    }

    /// Merge two Frontiers by sorted-merging their journal lists.
    /// Typically used to merge a checkpoint delta into a cumulative base:
    /// new journals from the delta are added, base journals absent from the
    /// delta are preserved unchanged, and matching `(journal, binding)` entries
    /// are reduced via `JournalFrontier::reduce` (maximizing clocks).
    /// Both inputs may contain non-unique keys, which are reduced to single entries.
    pub fn reduce(self, other: Self) -> Self {
        let flushed_lsn = Self::merge_flushed_lsn(self.flushed_lsn, other.flushed_lsn);

        if self.journals.is_empty() {
            return Self {
                flushed_lsn,
                ..other
            };
        } else if other.journals.is_empty() {
            return Self {
                flushed_lsn,
                ..self
            };
        }

        let mut merged = Vec::with_capacity(self.journals.len() + other.journals.len());
        let mut a = self.journals.into_iter().peekable();
        let mut b = other.journals.into_iter().peekable();

        loop {
            match (a.peek(), b.peek()) {
                (Some(ja), Some(jb)) => {
                    let ord = ja
                        .journal
                        .as_ref()
                        .cmp(jb.journal.as_ref())
                        .then(ja.binding.cmp(&jb.binding));

                    match ord {
                        std::cmp::Ordering::Less => merged.push(a.next().unwrap()),
                        std::cmp::Ordering::Greater => merged.push(b.next().unwrap()),
                        std::cmp::Ordering::Equal => {
                            merged.push(a.next().unwrap().reduce(b.next().unwrap()));
                        }
                    }
                }
                (Some(_), None) => {
                    merged.extend(a);
                    break;
                }
                (None, _) => {
                    merged.extend(b);
                    break;
                }
            }
        }
        merged.shrink_to_fit();

        Self {
            journals: merged,
            flushed_lsn,
        }
    }

    /// Look up a journal entry by `(journal, binding)`.
    pub fn find_journal(&mut self, journal: &str, binding: u16) -> Option<&mut JournalFrontier> {
        self.journals
            .binary_search_by(|jf| {
                jf.journal
                    .as_ref()
                    .cmp(journal)
                    .then(jf.binding.cmp(&binding))
            })
            .ok()
            .map(|i| &mut self.journals[i])
    }

    /// Resolve causal hints in `self` using progress from `progressed`.
    ///
    /// For each producer in `self` where `hinted_commit > last_commit`,
    /// if `progressed` contains a matching `(journal, binding, producer)`
    /// with `last_commit >= hinted_commit`, set this producer's `last_commit`
    /// to `hinted_commit` (capped, not the full progressed last_commit).
    ///
    /// Uses an ordered merge on `(journal, binding)` then `producer`,
    /// matching the sorted invariants of both frontiers.
    ///
    /// Returns the number of producers that were resolved.
    ///
    /// Liveness: unresolved hints always eventually resolve because a producer's
    /// write-ahead log guarantees that if any journal in a transaction receives an ACK,
    /// all journals in that transaction will eventually receive their ACKs as well.
    /// If the producer WAL commit fails, no ACKs are written to any journal,
    /// so unresolved hints for failed transactions never appear.
    pub fn resolve_hints(&mut self, progressed: &Frontier) -> usize {
        let mut resolved = 0usize;
        let mut lhs = self.journals.iter_mut().peekable();
        let mut rhs = progressed.journals.iter().peekable();

        loop {
            let ord = match (lhs.peek(), rhs.peek()) {
                (Some(l), Some(r)) => l
                    .journal
                    .as_ref()
                    .cmp(r.journal.as_ref())
                    .then(l.binding.cmp(&r.binding)),
                _ => break,
            };
            match ord {
                std::cmp::Ordering::Less => {
                    lhs.next();
                }
                std::cmp::Ordering::Greater => {
                    rhs.next();
                }
                std::cmp::Ordering::Equal => {
                    let lhs = lhs.next().unwrap();
                    let rhs = rhs.next().unwrap();
                    resolved += lhs.resolve_hints(rhs);
                }
            }
        }

        resolved
    }

    /// Count producers with unresolved causal hints (`hinted_commit > last_commit`).
    pub fn count_unresolved_hints(&self) -> usize {
        self.journals
            .iter()
            .flat_map(|jf| &jf.producers)
            .filter(|p| p.hinted_commit > p.last_commit)
            .count()
    }

    /// Extract producers with unresolved causal hints (`hinted_commit > last_commit`)
    /// into a new Frontier, filtering out journals that have no such producers.
    /// Used at startup to project read-through state from `resume_checkpoint`.
    pub fn project_unresolved_hints(&self) -> Frontier {
        let journals = self
            .journals
            .iter()
            .filter_map(|jf| {
                let producers: Vec<ProducerFrontier> = jf
                    .producers
                    .iter()
                    .filter(|p| p.hinted_commit > p.last_commit)
                    .cloned()
                    .collect();

                if producers.is_empty() {
                    None
                } else {
                    Some(JournalFrontier {
                        journal: jf.journal.clone(),
                        binding: jf.binding,
                        producers,
                        bytes_read_delta: 0,
                        bytes_behind_delta: 0,
                    })
                }
            })
            .collect();

        Frontier {
            journals,
            flushed_lsn: vec![],
        }
    }
}

/// Drains a `Frontier` as a sequence of chunked `FrontierChunk` messages.
///
/// Call `start` to begin draining a frontier, `is_empty` to check
/// whether chunks remain, and `next_chunk` to produce the next chunk.
/// The final chunk is an empty terminator (no journals).
///
/// Callers must verify they can act on a chunk (e.g. channel capacity)
/// *before* calling `next_chunk`, which advances internal state.
pub struct Drain {
    /// The frontier being drained. Replaced with `Default` once fully consumed.
    frontier: Frontier,
    /// Index of the next journal to encode. `usize::MAX` means no drain is in progress.
    offset: usize,
    /// Maximum number of journals per emitted `FrontierChunk`.
    journals_per_chunk: usize,
}

impl Drain {
    /// Default number of journals per chunk in production use.
    pub const DEFAULT_JOURNALS_PER_CHUNK: usize = 64;

    pub fn new() -> Self {
        Self {
            frontier: Default::default(),
            offset: usize::MAX,
            journals_per_chunk: Self::DEFAULT_JOURNALS_PER_CHUNK,
        }
    }

    /// Create a Drain with a custom journals-per-chunk size, useful for testing.
    pub fn with_journals_per_chunk(journals_per_chunk: usize) -> Self {
        assert!(journals_per_chunk > 0, "journals_per_chunk must be > 0");
        Self {
            frontier: Default::default(),
            offset: usize::MAX,
            journals_per_chunk,
        }
    }

    /// Begin draining the given frontier.
    /// Panics if a drain is already in progress.
    pub fn start(&mut self, frontier: Frontier) {
        assert!(self.is_empty(), "cannot start while a drain is in progress");
        self.frontier = frontier;
        self.offset = 0;
    }

    /// Whether the drain is complete and has no chunks remaining.
    pub fn is_empty(&self) -> bool {
        self.offset == usize::MAX
    }

    /// Produce the next `FrontierChunk`, advancing the drain offset.
    /// Returns `None` when no drain is in progress.
    /// An empty chunk (no journals) is the end-of-sequence terminator.
    pub fn next_chunk(&mut self) -> Option<shuffle::FrontierChunk> {
        if self.offset == usize::MAX {
            return None;
        }

        let end = (self.offset + self.journals_per_chunk).min(self.frontier.journals.len());
        let mut chunk = JournalFrontier::encode(&self.frontier.journals[self.offset..end]);

        if chunk.journals.is_empty() {
            chunk.flushed_lsn = std::mem::take(&mut self.frontier.flushed_lsn)
                .into_iter()
                .map(|lsn| lsn.as_u64())
                .collect();
            self.frontier = Default::default(); // Release memory.
            self.offset = usize::MAX;
        } else {
            self.offset += chunk.journals.len();
        }

        Some(chunk)
    }
}

impl std::fmt::Debug for Drain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.offset == usize::MAX {
            f.write_str("empty")
        } else {
            f.debug_struct("Drain")
                .field("offset", &self.offset)
                .field("journals", &self.frontier.journals.len())
                .finish()
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::testing::{jf, jf_with_bytes, pf, pf_tuple};
    use log::Lsn;

    #[test]
    fn test_producer_frontier_reduce() {
        // (a_commit, a_hint, a_offset, b_commit, b_hint, b_offset) => (commit, hint, offset)
        let cases: Vec<((u64, u64, i64), (u64, u64, i64), (u64, u64, i64))> = vec![
            // Largest absolute offset wins, regardless of last_commit ordering.
            ((200, 0, -1000), (100, 0, -500), (200, 0, -1000)),
            ((100, 0, -500), (200, 0, -1000), (200, 0, -1000)),
            // Larger absolute offset wins over smaller positive offset.
            ((100, 0, -300), (100, 0, 50), (100, 0, -300)),
            // Default offset=0 (e.g. from hint) does not override meaningful offset.
            ((200, 0, -800), (0, 500, 0), (200, 500, -800)),
        ];

        for (a, b, expect) in cases {
            let r = pf(0x01, a.0, a.1, a.2).reduce(pf(0x01, b.0, b.1, b.2));
            assert_eq!(pf_tuple(&r), expect, "reduce({a:?}, {b:?})");
        }
    }

    #[test]
    fn test_frontier_reduce() {
        // Exercises all three merge outcomes in one call:
        //   journal/A: only in `reads` (pass-through)
        //   journal/B: in both (producers merged; read-derived offset wins, hint adds hinted_commit)
        //   journal/C: only in `hints` (pass-through)
        // Within journal/B's producer merge:
        //   producer 0x03: matched, reduced (last_commit=200 > 0, so reads offset wins)
        //   producer 0x05: only in reads (pass-through)
        let reads = Frontier {
            journals: vec![
                jf_with_bytes("journal/A", 0, vec![pf(0x01, 100, 0, -500)], 200, 1000),
                jf_with_bytes(
                    "journal/B",
                    0,
                    vec![pf(0x03, 200, 0, -1000), pf(0x05, 50, 0, -200)],
                    100,
                    500,
                ),
            ],
            flushed_lsn: vec![Lsn::from_u64(10), Lsn::from_u64(50), Lsn::from_u64(3)],
        };
        let hints = Frontier {
            journals: vec![
                jf_with_bytes("journal/B", 0, vec![pf(0x03, 0, 300, 0)], 50, -300),
                jf("journal/C", 1, vec![pf(0x03, 0, 300, 0)]),
            ],
            flushed_lsn: vec![Lsn::from_u64(40), Lsn::from_u64(20), Lsn::from_u64(30)],
        };
        let r = reads.reduce(hints);

        // journal/A: reads-only pass-through.
        // journal/B: merged; producer 0x03 reduced (commit=200, hint=300, offset=-1000),
        //            producer 0x05 reads-only pass-through.
        // journal/C: hints-only pass-through.
        // Byte deltas are summed during reduction.
        insta::assert_debug_snapshot!(r.journals.iter().map(|j| {
            let ps: Vec<_> = j.producers.iter().map(pf_tuple).collect();
            (&*j.journal, j.binding, ps, j.bytes_read_delta, j.bytes_behind_delta)
        }).collect::<Vec<_>>(), @r#"
        [
            (
                "journal/A",
                0,
                [
                    (
                        100,
                        0,
                        -500,
                    ),
                ],
                200,
                1000,
            ),
            (
                "journal/B",
                0,
                [
                    (
                        200,
                        300,
                        -1000,
                    ),
                    (
                        50,
                        0,
                        -200,
                    ),
                ],
                150,
                200,
            ),
            (
                "journal/C",
                1,
                [
                    (
                        0,
                        300,
                        0,
                    ),
                ],
                0,
                0,
            ),
        ]
        "#);
        assert_eq!(
            r.flushed_lsn,
            vec![Lsn::from_u64(40), Lsn::from_u64(50), Lsn::from_u64(30)],
            "element-wise max of flushed_lsn"
        );

        // Identity: empty reduces are no-ops and preserve flushed_lsn.
        let f = Frontier {
            journals: vec![jf("j", 0, vec![pf(0x01, 1, 0, -1)])],
            flushed_lsn: vec![Lsn::from_u64(10), Lsn::from_u64(20)],
        };
        let r = f.clone().reduce(Frontier::default());
        assert_eq!(r.journals.len(), 1);
        assert_eq!(r.flushed_lsn, vec![Lsn::from_u64(10), Lsn::from_u64(20)]);
        let r = Frontier::default().reduce(f);
        assert_eq!(r.journals.len(), 1);
        assert_eq!(r.flushed_lsn, vec![Lsn::from_u64(10), Lsn::from_u64(20)]);
        assert!(
            Frontier::default()
                .reduce(Frontier::default())
                .journals
                .is_empty()
        );
    }

    #[test]
    fn test_merge_flushed_lsn() {
        // Both empty.
        assert_eq!(
            Frontier::merge_flushed_lsn(vec![], vec![]),
            Vec::<log::Lsn>::new()
        );
        // One empty: returns the other.
        assert_eq!(
            Frontier::merge_flushed_lsn(vec![Lsn::from_u64(10), Lsn::from_u64(20)], vec![],),
            vec![Lsn::from_u64(10), Lsn::from_u64(20)]
        );
        assert_eq!(
            Frontier::merge_flushed_lsn(vec![], vec![Lsn::from_u64(30), Lsn::from_u64(40)],),
            vec![Lsn::from_u64(30), Lsn::from_u64(40)]
        );
        // Same length: element-wise max.
        assert_eq!(
            Frontier::merge_flushed_lsn(
                vec![Lsn::from_u64(10), Lsn::from_u64(50), Lsn::from_u64(30)],
                vec![Lsn::from_u64(40), Lsn::from_u64(20), Lsn::from_u64(60)],
            ),
            vec![Lsn::from_u64(40), Lsn::from_u64(50), Lsn::from_u64(60)]
        );
        // Different lengths: shorter extended with zeros.
        assert_eq!(
            Frontier::merge_flushed_lsn(
                vec![Lsn::from_u64(10), Lsn::from_u64(20)],
                vec![Lsn::from_u64(5), Lsn::from_u64(25), Lsn::from_u64(30)],
            ),
            vec![Lsn::from_u64(10), Lsn::from_u64(25), Lsn::from_u64(30)]
        );
        assert_eq!(
            Frontier::merge_flushed_lsn(
                vec![Lsn::from_u64(10), Lsn::from_u64(20), Lsn::from_u64(30)],
                vec![Lsn::from_u64(5)],
            ),
            vec![Lsn::from_u64(10), Lsn::from_u64(20), Lsn::from_u64(30)]
        );
    }

    #[test]
    fn test_encode_decode_round_trip() {
        let frontier = Frontier::new(
            vec![
                jf_with_bytes(
                    "estuary/tenants/acme/orders/pivot=00",
                    0,
                    vec![pf(0x01, 100, 0, -500)],
                    1500,
                    42000,
                ),
                jf(
                    "estuary/tenants/acme/orders/pivot=00",
                    1,
                    vec![pf(0x03, 200, 0, -1000)],
                ),
                jf(
                    "estuary/tenants/acme/orders/pivot=01",
                    0,
                    vec![pf(0x01, 50, 0, -200)],
                ),
                jf_with_bytes(
                    "estuary/tenants/acme/users/pivot=00",
                    0,
                    vec![pf(0x05, 300, 400, 42)],
                    900,
                    -300,
                ),
                jf(
                    "estuary/tenants/other/events/pivot=00",
                    2,
                    vec![pf(0x07, 10, 0, -100)],
                ),
            ],
            vec![],
        )
        .unwrap();

        // Single chunk round-trips correctly.
        let chunk = JournalFrontier::encode(&frontier.journals);
        assert_eq!(chunk.journals.len(), 5);
        let decoded: Vec<_> = JournalFrontier::decode(chunk).collect();
        assert_eq!(decoded.len(), frontier.journals.len());
        for (a, b) in decoded.iter().zip(frontier.journals.iter()) {
            assert_eq!(&*a.journal, &*b.journal);
            assert_eq!(a.binding, b.binding);
            assert_eq!(a.bytes_read_delta, b.bytes_read_delta);
            assert_eq!(a.bytes_behind_delta, b.bytes_behind_delta);
        }

        // Multi-chunk: each chunk is independently decodable.
        for chunk_size in [1, 2, 3] {
            let mut reassembled = Vec::new();
            let mut offset = 0;
            while offset < frontier.journals.len() {
                let end = (offset + chunk_size).min(frontier.journals.len());
                let chunk = JournalFrontier::encode(&frontier.journals[offset..end]);

                // The first entry of each chunk must have truncate=0
                // and the full journal name as suffix.
                let first = &chunk.journals[0];
                assert_eq!(
                    first.journal_name_truncate_delta, 0,
                    "chunk at offset {offset}: first entry must have truncate=0"
                );
                let expected_name = &*frontier.journals[offset].journal;
                assert_eq!(
                    first.journal_name_suffix, expected_name,
                    "chunk at offset {offset}: first entry suffix must be the full journal name"
                );

                // Each chunk decodes independently (no external state).
                reassembled.extend(JournalFrontier::decode(chunk));
                offset = end;
            }

            // Reassembled frontier matches the original.
            let reassembled = Frontier::new(reassembled, vec![]).unwrap();
            assert_eq!(reassembled.journals.len(), frontier.journals.len());
            for (a, b) in reassembled.journals.iter().zip(frontier.journals.iter()) {
                assert_eq!(&*a.journal, &*b.journal, "chunk_size={chunk_size}");
                assert_eq!(a.binding, b.binding, "chunk_size={chunk_size}");
                assert_eq!(
                    a.bytes_behind_delta, b.bytes_behind_delta,
                    "chunk_size={chunk_size}"
                );
                assert_eq!(
                    a.bytes_read_delta, b.bytes_read_delta,
                    "chunk_size={chunk_size}"
                );
            }
        }
    }

    #[test]
    fn test_encode_empty() {
        let chunk = JournalFrontier::encode(&[]);
        assert!(chunk.journals.is_empty());
    }

    #[test]
    fn test_frontier_new_validates_journal_order() {
        // Out-of-order journals within the same binding.
        let err = Frontier::new(
            vec![jf("journal/B", 0, vec![]), jf("journal/A", 0, vec![])],
            vec![],
        )
        .unwrap_err();
        assert!(
            format!("{err}").contains("not ordered"),
            "expected ordering error, got: {err}"
        );

        // Out-of-order bindings.
        let err = Frontier::new(
            vec![jf("journal/A", 1, vec![]), jf("journal/A", 0, vec![])],
            vec![],
        )
        .unwrap_err();
        assert!(
            format!("{err}").contains("not ordered"),
            "expected ordering error, got: {err}"
        );

        // Duplicate (journal, binding).
        let err = Frontier::new(
            vec![jf("journal/A", 0, vec![]), jf("journal/A", 0, vec![])],
            vec![],
        )
        .unwrap_err();
        assert!(
            format!("{err}").contains("not unique"),
            "expected uniqueness error, got: {err}"
        );
    }

    #[test]
    fn test_frontier_new_validates_producer_order() {
        // Out-of-order producers within a journal.
        let err = Frontier::new(
            vec![jf(
                "journal/A",
                0,
                vec![pf(0x05, 100, 0, -1), pf(0x01, 200, 0, -2)],
            )],
            vec![],
        )
        .unwrap_err();
        assert!(
            format!("{err}").contains("not ordered"),
            "expected ordering error, got: {err}"
        );

        // Duplicate producers.
        let err = Frontier::new(
            vec![jf(
                "journal/A",
                0,
                vec![pf(0x01, 100, 0, -1), pf(0x01, 200, 0, -2)],
            )],
            vec![],
        )
        .unwrap_err();
        assert!(
            format!("{err}").contains("not unique"),
            "expected uniqueness error, got: {err}"
        );
    }

    #[test]
    fn test_resolve_hints() {
        // checkpoint_pending: journal/A has P1 with unresolved hint,
        // journal/B has P3 with unresolved hint and P5 without.
        let mut pending = Frontier {
            journals: vec![
                jf("journal/A", 0, vec![pf(0x01, 50, 200, -100)]),
                jf(
                    "journal/B",
                    0,
                    vec![pf(0x03, 0, 300, 0), pf(0x05, 100, 0, -500)],
                ),
            ],
            flushed_lsn: vec![],
        };

        // Progressed: journal/A has P1 with last_commit=250 (matches hint @200),
        // journal/B has P3 with last_commit=250 (not enough @300).
        let progressed = Frontier {
            journals: vec![
                jf("journal/A", 0, vec![pf(0x01, 250, 0, -800)]),
                jf("journal/B", 0, vec![pf(0x03, 250, 0, -600)]),
            ],
            flushed_lsn: vec![],
        };

        let resolved = pending.resolve_hints(&progressed);
        // Only P1 in journal/A resolved (progressed 250 >= hinted 200).
        // P3 in journal/B not resolved (progressed 250 < hinted 300).
        assert_eq!(resolved, 1);

        // P1's last_commit is set to hinted_commit (200s), not progressed (250s).
        assert_eq!(
            pending.journals[0].producers[0].last_commit.to_unix().0,
            200
        );
        // P3 unchanged.
        assert_eq!(pending.journals[1].producers[0].last_commit.to_unix().0, 0);

        // Second round: P3 now has enough progress.
        let progressed2 = Frontier {
            journals: vec![jf("journal/B", 0, vec![pf(0x03, 400, 0, -900)])],
            flushed_lsn: vec![],
        };
        let resolved2 = pending.resolve_hints(&progressed2);
        assert_eq!(resolved2, 1);
        assert_eq!(
            pending.journals[1].producers[0].last_commit.to_unix().0,
            300
        );

        // Empty progressed resolves nothing.
        assert_eq!(pending.resolve_hints(&Frontier::default()), 0);

        // Empty pending resolves nothing.
        assert_eq!(Frontier::default().resolve_hints(&progressed), 0);
    }

    #[test]
    fn test_resolve_hints_different_bindings() {
        // Pending has journal/X binding=1, progressed has journal/X binding=0.
        // Should NOT match (different bindings).
        let mut pending = Frontier {
            journals: vec![jf("journal/X", 1, vec![pf(0x01, 0, 100, 0)])],
            flushed_lsn: vec![],
        };
        let progressed = Frontier {
            journals: vec![jf("journal/X", 0, vec![pf(0x01, 200, 0, -500)])],
            flushed_lsn: vec![],
        };
        assert_eq!(pending.resolve_hints(&progressed), 0);
    }

    #[test]
    fn test_count_unresolved_hints() {
        let f = Frontier {
            journals: vec![
                jf(
                    "journal/A",
                    0,
                    vec![
                        pf(0x01, 50, 200, -100),  // unresolved: 200 > 50
                        pf(0x03, 300, 100, -500), // resolved: 100 <= 300
                    ],
                ),
                jf(
                    "journal/B",
                    0,
                    vec![
                        pf(0x05, 0, 150, 0), // unresolved: 150 > 0
                    ],
                ),
                jf("journal/C", 1, vec![pf(0x07, 100, 0, -200)]), // no hint
            ],
            flushed_lsn: vec![],
        };
        assert_eq!(f.count_unresolved_hints(), 2);
        assert_eq!(Frontier::default().count_unresolved_hints(), 0);
    }

    #[test]
    fn test_project_unresolved_hints() {
        let f = Frontier {
            journals: vec![
                jf(
                    "journal/A",
                    0,
                    vec![
                        pf(0x01, 50, 200, -100),  // unresolved
                        pf(0x03, 300, 100, -500), // resolved (last >= hinted)
                    ],
                ),
                jf("journal/B", 0, vec![pf(0x05, 100, 0, -200)]), // no hint
                jf("journal/C", 1, vec![pf(0x07, 0, 300, 0)]),    // unresolved
            ],
            flushed_lsn: vec![],
        };

        let projected = f.project_unresolved_hints();

        // journal/A: only P1 (unresolved). journal/B: filtered out (no hints).
        // journal/C: P7 (unresolved).
        insta::assert_debug_snapshot!(projected.journals.iter().map(|j| {
            let ps: Vec<_> = j.producers.iter().map(pf_tuple).collect();
            (&*j.journal, j.binding, ps)
        }).collect::<Vec<_>>(), @r#"
        [
            (
                "journal/A",
                0,
                [
                    (
                        50,
                        200,
                        -100,
                    ),
                ],
            ),
            (
                "journal/C",
                1,
                [
                    (
                        0,
                        300,
                        0,
                    ),
                ],
            ),
        ]
        "#);

        // No hints: empty projection.
        let no_hints = Frontier {
            journals: vec![jf("journal/A", 0, vec![pf(0x01, 100, 0, -200)])],
            flushed_lsn: vec![],
        };
        assert!(no_hints.project_unresolved_hints().journals.is_empty());

        // Empty frontier: empty projection.
        assert!(
            Frontier::default()
                .project_unresolved_hints()
                .journals
                .is_empty()
        );
    }

    fn drain_all(drain: &mut Drain) -> Vec<usize> {
        std::iter::from_fn(|| drain.next_chunk())
            .map(|c| c.journals.len())
            .collect()
    }

    #[test]
    fn test_drain_chunking() {
        // (journal_count, journals_per_chunk) => expected per-chunk journal counts.
        // Every sequence ends with 0 (the empty terminator).
        let cases: &[(usize, usize, &[usize])] = &[
            (0, 2, &[0]),       // empty frontier
            (1, 1, &[1, 0]),    // single journal, chunk size 1
            (2, 2, &[2, 0]),    // exact boundary
            (3, 2, &[2, 1, 0]), // overflow by one
            (3, 100, &[3, 0]),  // chunk larger than frontier
            (5, 2, &[2, 2, 1, 0]),
        ];

        let all_journals: Vec<_> = (0..5)
            .map(|i| jf(&format!("journal/{i}"), 0, vec![pf(0x01, 100, 0, -500)]))
            .collect();

        for &(n, chunk_size, expected) in cases {
            let mut drain = Drain::with_journals_per_chunk(chunk_size);
            drain.start(Frontier {
                journals: all_journals[..n].to_vec(),
                flushed_lsn: vec![],
            });
            assert_eq!(
                drain_all(&mut drain),
                expected,
                "n={n} chunk_size={chunk_size}"
            );
            assert!(drain.is_empty());
        }
    }

    #[test]
    fn test_drain_not_started() {
        let mut drain = Drain::new();
        assert!(drain.is_empty());
        assert!(drain.next_chunk().is_none());
    }

    #[test]
    fn test_drain_reuse() {
        let mut drain = Drain::with_journals_per_chunk(10);
        for _ in 0..3 {
            drain.start(Frontier {
                journals: vec![jf("j", 0, vec![pf(0x01, 1, 0, -1)])],
                flushed_lsn: vec![],
            });
            assert_eq!(drain_all(&mut drain), [1, 0]);
        }
    }

    #[test]
    #[should_panic(expected = "cannot start while a drain is in progress")]
    fn test_drain_double_start_panics() {
        let mut drain = Drain::with_journals_per_chunk(1);
        drain.start(Frontier {
            journals: vec![jf("j", 0, vec![pf(0x01, 1, 0, -1)])],
            flushed_lsn: vec![],
        });
        drain.start(Frontier::default());
    }

    #[test]
    fn test_drain_round_trip() {
        let original = Frontier::new(
            vec![
                jf("journal/A", 0, vec![pf(0x01, 100, 0, -500)]),
                jf("journal/A", 1, vec![pf(0x03, 200, 0, -800)]),
                jf("journal/B", 0, vec![pf(0x05, 300, 400, 42)]),
            ],
            vec![100, 200, 300],
        )
        .unwrap();

        for chunk_size in [1, 2, 3] {
            let mut drain = Drain::with_journals_per_chunk(chunk_size);
            drain.start(original.clone());

            let mut reassembled_journals = Vec::new();
            let mut terminal_flushed_lsn = Vec::new();
            for chunk in std::iter::from_fn(|| drain.next_chunk()) {
                if chunk.journals.is_empty() {
                    terminal_flushed_lsn = chunk.flushed_lsn;
                } else {
                    reassembled_journals.extend(JournalFrontier::decode(chunk));
                }
            }
            let reassembled = Frontier::new(reassembled_journals, terminal_flushed_lsn).unwrap();

            assert_eq!(reassembled.journals.len(), original.journals.len());
            for (a, b) in reassembled.journals.iter().zip(original.journals.iter()) {
                assert_eq!(&*a.journal, &*b.journal);
                assert_eq!(a.binding, b.binding);
                assert_eq!(a.producers.len(), b.producers.len());
            }
            assert_eq!(
                reassembled.flushed_lsn, original.flushed_lsn,
                "flushed_lsn round-trips through drain (chunk_size={chunk_size})"
            );
        }
    }
}
