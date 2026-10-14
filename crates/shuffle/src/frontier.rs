use crate::log;
use proto_flow::shuffle;
use proto_gazette::uuid::{Clock, Producer};
use std::collections::BTreeMap;

/// Lower-bound synthetic `last_commit` for a producer observed through journal
/// reads, distinguishing it from a hint-only producer. All actual Clock values
/// are required to be larger. The distinction matters when evaluating whether
/// a producer hinted at Clock H is "gapped" on recovery:
///
/// Case 1 - {offset: 0, last_commit: 1, hinted_commit: H}
/// This producer wrote a literal CONTINUE_TXN at offset zero which still has an
/// open span (no committing close or rollback yet). If we start reading from
/// offset M > 0, this producer is considered gapped and must replay from zero.
///
/// Case 2 - {offset: 0, last_commit: 0, hinted_commit: H}
/// This hint-only producer has never been observed via journal read. If we start
/// reading from the maximum offset M of any journal producer, then by construction
/// no span of this producer can live in [0, M), as `last_commit` would otherwise
/// be this sentinel value.
///
pub(crate) const OBSERVED_COMMIT_FLOOR: u64 = 1;

/// Frontier state of a single producer within a journal.
#[derive(Debug, Clone)]
pub struct ProducerFrontier {
    pub producer: Producer,
    /// Clock of the last committing close (an ACK_TXN or OUTSIDE_TXN), or
    /// [`OBSERVED_COMMIT_FLOOR`] if the producer has an open span but has
    /// never committed.
    pub last_commit: Clock,
    /// Clock of a hinted (causal) commit, or zero if no hint.
    pub hinted_commit: Clock,
    /// `offset` encodes journal position with a sign convention which is also a
    /// direct readout of whether the producer is open or closed at the cut:
    /// - Non-negative: `+begin` of the open span ⇔ open.
    /// - Negative: negation of the end offset of the last committing close
    ///   (an ACK_TXN or OUTSIDE_TXN) ⇔ closed.
    pub offset: i64,
}

impl ProducerFrontier {
    /// Reduce two ProducerFrontier entries for the same producer.
    ///
    /// Maximizes `last_commit` and `hinted_commit`. Takes `offset` with the
    /// largest absolute value, because the sign encodes semantics (negative =
    /// closed end, non-negative = open `+begin`) and the magnitude
    /// represents how far into the journal we've read. An open `+begin` is
    /// then closed if the other side carries a later commit.
    pub fn reduce(self, other: Self) -> Self {
        // We cannot simply take the offset from whichever side has the larger
        // `last_commit`, because causal hint resolution (`resolve_hints`) elevates
        // `last_commit` on an entry without raising its offset to its true
        // journal position (details: it raises the offset no further than the
        // maximum progress of the original Frontier cut). We don't want to
        // clobber an accurate (ahead) `offset` of a lesser `last_commit` by
        // replacing it with a greater hinted-and-resolved `last_commit` having
        // a lesser, conservative `offset`.
        //
        // On equal magnitude, prefer the non-negative (open `+begin`) side.
        // A producer's next span begins exactly at its previous transaction's
        // committed end offset whenever no other producer appended in between,
        // so a `+F` span begin routinely ties with the prior committed `-O`
        // (F == O) — and the span begin is the strictly newer state. Note `+F`
        // also carries `-O`'s commit as its `last_commit`, so the flip below
        // keeps it open: a tie closes only against a later commit.
        let mut offset = if self.offset.abs() != other.offset.abs() {
            if self.offset.abs() > other.offset.abs() {
                self.offset
            } else {
                other.offset
            }
        } else {
            self.offset.max(other.offset)
        };
        let last_commit = self.last_commit.max(other.last_commit);

        // A later commit of the producer closes its open span. A `+begin` is
        // carried with the delta of a producer that begins a span, and a later
        // `last_commit` of lesser offset magnitude comes from hint resolution:
        // it proves the span's transaction committed, but its conservative
        // `-M` may not reach `+begin` (M is the cut floor of its own delta,
        // and zero if that delta has only a hint of this journal). Keep the
        // magnitude, which is a conservative lower-bound offset of the later
        // `last_commit`, but flip its sign to close it.
        let is_latest = |p: &Self| p.offset == offset && p.last_commit == last_commit;
        if offset > 0 && !is_latest(&self) && !is_latest(&other) {
            offset = -offset;
        }

        Self {
            producer: self.producer,
            last_commit,
            hinted_commit: self.hinted_commit.max(other.hinted_commit),
            offset,
        }
    }
}

/// Frontier state for a single journal under a specific binding.
#[derive(Debug, Clone, Default)]
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
    /// Clock below which causal hints naming this journal are unreachable,
    /// because a byte gap at the read's start removed (or skipped) their ACKs.
    /// Zero if none. An entry may carry a gap floor and no producers.
    /// Maximized during reduction.
    pub gap_floor: Clock,
    /// Largest `last_commit` of a producer which this entry has forgotten:
    /// one removed by [`Self::prune_stale_producers`]. A clock at-or-below it,
    /// of a producer absent from `producers`, may be one this entry once
    /// covered. Never encoded, and so zero in every delta. Maximized during
    /// reduction.
    pub prune_floor: Clock,
}

impl JournalFrontier {
    /// Reduce two JournalFrontier entries for the same (journal, binding)
    /// by merging their producer lists. Matching producers are reduced
    /// via `ProducerFrontier::reduce`; unmatched producers pass through.
    pub fn reduce(mut self, mut other: Self) -> Self {
        // Reduction is symmetric, so fold the shorter list into the longer.
        if self.producers.len() < other.producers.len() {
            std::mem::swap(&mut self.producers, &mut other.producers);
        }
        merge_into(
            &mut self.producers,
            other.producers,
            |base, item| base.producer.cmp(&item.producer),
            |base, item| *base = base.clone().reduce(item),
        );
        self.bytes_read_delta += other.bytes_read_delta;
        self.bytes_behind_delta += other.bytes_behind_delta;
        self.gap_floor = self.gap_floor.max(other.gap_floor);
        self.prune_floor = self.prune_floor.max(other.prune_floor);
        self
    }

    /// Whether this entry carries nothing: no producers, and no floors.
    pub fn is_empty(&self) -> bool {
        self.producers.is_empty()
            && self.gap_floor == Clock::zero()
            && self.prune_floor == Clock::zero()
    }

    /// Remove this entry's retired producers, raising `prune_floor` to the
    /// `last_commit` of each one removed and passing it to `on_pruned`.
    ///
    /// A retired producer no longer writes the journal. Pruning bounds a
    /// Frontier which would otherwise carry every producer forever.
    /// A producer is pruned only when **all** of:
    ///
    /// 1. It has no unresolved hint, and `protected` doesn't claim it.
    /// 2. It's closed (`offset < 0`).
    /// 3. Its `offset.abs()` is below the entry's cut floor.
    /// 4. Its `last_commit` trails the entry's newest `last_commit` by at least
    ///    [`crate::PRODUCER_STALENESS_HORIZON`].
    ///
    /// Pruning relaxes the Frontier invariant's coverage of closes (see
    /// [`Frontier`]), but loses nothing:
    ///
    /// - Condition 3 retains every producer at the cut floor, so the floor
    ///   never regresses. Reads resume at it, and never re-read the content
    ///   of a forgotten producer below it.
    /// - Condition 2 retains open spans, so a gapped producer which resumes is
    ///   still replayed (`slice/replay.rs`).
    /// - A forgotten producer which resumes sequences afresh from the cut floor.
    ///   Its closes below the floor were covered, and won't be read again.
    ///
    /// Forgetting a producer does cost liveness. A pruned producer can be the
    /// target of a future backfill's causal hints, which nothing
    /// forward-looking will resolve: its journal is read past its commits, and
    /// the producer is retired. `prune_floor` is what keeps such a hint
    /// dischargeable, as the hint is bounded by the pruned `last_commit`.
    pub fn prune_stale_producers(
        &mut self,
        protected: impl Fn(Producer) -> bool,
        mut on_pruned: impl FnMut(&ProducerFrontier),
    ) {
        let newest_commit = self
            .producers
            .iter()
            .map(|p| p.last_commit)
            .max()
            .unwrap_or_default();

        let cut_floor = self
            .producers
            .iter()
            .map(|p| p.offset.abs())
            .max()
            .unwrap_or_default();

        let mut prune_floor = self.prune_floor;

        self.producers.retain(|p| {
            let stale = p.hinted_commit <= p.last_commit
                && !protected(p.producer)
                && p.offset < 0
                && p.offset.abs() < cut_floor
                && Clock::delta(newest_commit, p.last_commit) >= crate::PRODUCER_STALENESS_HORIZON;

            if stale {
                prune_floor = prune_floor.max(p.last_commit);
                on_pruned(p);
            }
            !stale
        });
        self.prune_floor = prune_floor;
    }

    /// Whether this entry of the session's cumulative Frontier accounts for
    /// `clock` of `producer` — a commit, or a causal hint if `is_hint` — and by
    /// which authority, in order of precedence:
    ///
    /// 1. [`Accounted::Commit`]: the producer's promoted `last_commit` is
    ///    at-or-above `clock`. A producer's commits on one journal are in offset
    ///    order, so this read has read through `clock`. A commit vouches for its
    ///    own journal alone. A later transaction of the producer promotes once
    ///    its own journals are read, and nothing orders that after reads of an
    ///    earlier transaction's other journals.
    /// 2. [`Accounted::PruneFloor`]: the producer is absent, and `clock` is
    ///    at-or-below the `prune_floor` which bounds every forgotten clock.
    ///    It never accounts for [`OBSERVED_COMMIT_FLOOR`], the `last_commit` of
    ///    a producer which has never committed: the floor bounds forgotten
    ///    commits, not open spans. So a novel producer's open span freezes
    ///    the ratchet, whose cut then stays below the span's `+begin`, and
    ///    the next session reads the span rather than replaying it.
    /// 3. [`Accounted::GapFloor`], for hints only: `clock` is below the
    ///    `gap_floor`, so its ACK is unreachable. A byte gap says nothing about
    ///    whether a producer committed, so it never accounts for a commit.
    pub fn accounts_for(
        &self,
        producer: Producer,
        clock: Clock,
        is_hint: bool,
    ) -> Option<Accounted> {
        match self
            .producers
            .binary_search_by(|p| p.producer.cmp(&producer))
        {
            Ok(index) if self.producers[index].last_commit >= clock => {
                return Some(Accounted::Commit);
            }
            Err(_) if clock <= self.prune_floor && clock.as_u64() > OBSERVED_COMMIT_FLOOR => {
                return Some(Accounted::PruneFloor);
            }
            _ => {}
        }
        (is_hint && clock < self.gap_floor).then_some(Accounted::GapFloor)
    }

    /// Advance `last_commit` on producers of `self` up to each one's
    /// `hinted_commit` using progress from `other`.
    ///
    /// `self` and `other` must be for the same `(journal, binding)`.
    ///
    /// `offset` is conservatively updated on resolution: when `last_commit` is
    /// capped at `hinted_commit` but `other.last_commit` is past it,
    /// `other.offset` corresponds to a journal position that overshoots
    /// where the resolved `last_commit` actually sits. At the same time, hinted
    /// resolution implies a producer's formerly-open span is now closed, and we
    /// don't want it to recover as gapped (which would trigger a replay).
    ///
    /// So resolved producers are bumped to `-M`, the negated cut floor of `self`,
    /// marking them as closed through this progress. This is sound by clause 1
    /// of the Frontier invariant (see [`crate::Frontier`]): self's entries
    /// describe one continuous read through at least `M`, and a hint advancement
    /// implies a committing ACK *above* `M`, so every close of this producer
    /// below `M` is already covered and a re-read would suppress it.
    ///
    /// Returns `(advanced, resolved)`:
    /// - `advanced`: producers whose `last_commit` advanced by any amount.
    /// - `resolved`: producers whose `last_commit` reached `hinted_commit`
    ///   (a subset of `advanced`).
    fn resolve_hints(&mut self, other: &JournalFrontier) -> (usize, usize) {
        let mut advanced = 0usize;
        let mut resolved = 0usize;

        // `M`: self's cut floor, the max offset across this journal's entries.
        let m = self
            .producers
            .iter()
            .map(|p| p.offset.abs())
            .max()
            .unwrap_or(0);

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

            if lhs.hinted_commit > lhs.last_commit && rhs.last_commit > lhs.last_commit {
                lhs.last_commit = rhs.last_commit.min(lhs.hinted_commit);
                lhs.offset = -m;
                advanced += 1;

                if rhs.last_commit >= lhs.hinted_commit {
                    resolved += 1;
                }
            }
        }

        (advanced, resolved)
    }

    /// Decode a proto `Frontier`'s journals into an iterator of `JournalFrontier`.
    ///
    /// Journal names within the proto are delta-encoded, with the first entry
    /// carrying the full journal name (truncate=0, suffix=full name) and
    /// subsequent entries delta-encoded relative to their predecessor.
    /// Decoding is a pure mapping with no validation; use `Frontier::decode`
    /// or `Frontier::new` to validate ordering invariants.
    pub fn decode(proto: shuffle::Frontier) -> impl Iterator<Item = JournalFrontier> {
        let mut journal_name = String::new();

        proto.journals.into_iter().map(move |jf| {
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
                gap_floor: Clock::from_u64(jf.gap_floor),
                prune_floor: Clock::zero(),
            }
        })
    }

    /// Encode a slice of `JournalFrontier` entries as a proto `Frontier`.
    ///
    /// The first entry carries the full journal name (truncate=0, suffix=full
    /// name), and subsequent entries are delta-encoded relative to their
    /// predecessor. The returned proto's `flushed_lsn` is empty; callers
    /// needing it should populate the field directly, or use
    /// `Frontier::encode`.
    pub fn encode(entries: &[Self]) -> shuffle::Frontier {
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
                    gap_floor: jf.gap_floor.as_u64(),
                }
            })
            .collect();

        shuffle::Frontier {
            journals,
            flushed_lsn: vec![],
            ..Default::default()
        }
    }
}

/// Frontier tracks journal progress including causal hints.
///
/// A Frontier is either *cumulative* — the complete reduction of every delta
/// since genesis, leaving out nothing which progressed — or a *delta*, carrying
/// only the journals and producers which progressed since the last checkpoint.
/// `reduce` merges a delta into a cumulative base: new journals from the delta
/// are added, base journals absent from the delta are preserved, and matching
/// entries are reduced by maximizing clocks. Durable resume checkpoints are
/// cumulative, while emitted `ready` frontiers and peeks are deltas which the
/// client is expected to reduce into a cumulative base.
///
/// See session::CheckpointPipeline for details of how Frontier deltas are built.
///
/// # The Frontier invariant
///
/// Every cumulative Frontier *has* this property; it is not a condition to be
/// checked. It is scoped to the journal and producer entries — the Frontier's
/// read state. (`flushed_lsn` and the backfill clocks carry their own,
/// separate documentation.)
///
/// A journal's **cut** is the offset it was read through to produce the
/// Frontier. A cut is never represented explicitly: it is bounded from below
/// only by the **cut floor** `M = max|offset|` across the journal's producer
/// entries. Per journal, then:
///
/// 1. The entries jointly describe one continuous read of the journal through
///    (at least) `M`; and
/// 2. they account for every producer sequencing event in journal content below
///    `M` (content since removed by journal retention owes no account). Each
///    **committing close** is covered by its producer's `last_commit`, and each
///    open span's `+begin` is carried by its producer's entry.
///
/// The consequence is that resuming the read at `M` loses nothing, which is
/// what makes a Frontier a resumption point at all. Content below `M` is never
/// read again, with one exception. An open span is recovered from its `+begin`
/// by gap-replay of its producer alone, triggered by that producer's next
/// activity (see `slice/replay.rs`).
///
/// The closed/open asymmetry follows from what is owed downstream. A committing
/// close's documents are owed even if the producer then goes silent forever, so
/// the close must be covered. An open span's documents are owed to no one yet,
/// and its eventual close is itself the replay trigger. A rolled-back span's
/// documents are owed to no one, ever.
///
/// The invariant reads inductively: the empty Frontier satisfies it trivially,
/// and reducing in the next delta of a continuous read preserves it — which is
/// what proper reduction entails.
///
/// It is agnostic to hint state. An unresolved `hinted_commit` by construction
/// references an ACK beyond the cut, so hints govern transactional visibility
/// and never resumption soundness: the invariant holds equally of resume
/// checkpoints, and delta `ready` emissions & peeks (once accumulated).
///
/// A delta carries the invariant only relative to the baseline it reduces into.
/// An entry which did not advance is nonetheless still current at the delta's
/// cut — its producer not progressing is precisely the absence of any event of
/// theirs since. This is what makes the union of durable frontier rows written
/// at differing cuts coherent.
///
/// Pruning relaxes clause 2: a forgotten producer's closes are no longer
/// covered by its `last_commit`. This loses nothing (see
/// [`JournalFrontier::prune_stale_producers`]), but it does carry a liveness
/// contract. A pruned producer can be the target of a future backfill's causal
/// hints, which nothing forward-looking will ever resolve — its journal is
/// read past them and the producer is retired. What keeps such a hint
/// dischargeable is the entry's `prune_floor`, which bounds the clocks it has
/// forgotten: see [`JournalFrontier::accounts_for`] and
/// [`Frontier::seed_prune_floors`].
#[derive(Debug, Clone, Default)]
pub struct Frontier {
    /// Journals which constitute the frontier.
    /// Entries are sorted and unique on `(journal, binding)`.
    pub journals: Vec<JournalFrontier>,
    /// Per-shard flushed LSN (log read-through barrier), indexed by shard_index.
    /// Empty when not applicable (e.g. resume checkpoints).
    pub flushed_lsn: Vec<log::Lsn>,
    /// Latest committed backfill-begin clock of the checkpoint delta, keyed by
    /// binding index. Folded from immediately-committed CONTROL documents;
    /// does not participate in causal-hint sequencing.
    pub latest_backfill_begin: BTreeMap<u16, Clock>,
    /// Latest committed backfill-complete clock of the checkpoint delta, keyed
    /// by binding index. See `latest_backfill_begin`.
    pub latest_backfill_complete: BTreeMap<u16, Clock>,
    /// Count of `ProducerFrontier` entries with `hinted_commit > last_commit`.
    /// A Frontier with a non-zero count is "partial": readable for processing
    /// (e.g. log scanning), but NOT a transactional boundary.
    pub unresolved_hints: usize,
}

/// The authority by which an entry of the session's cumulative Frontier
/// accounts for a clock. See [`JournalFrontier::accounts_for`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Accounted {
    Commit,
    PruneFloor,
    GapFloor,
}

/// The producer entry which made a delta unaccounted, as returned by
/// [`Frontier::first_unaccounted`]. It's the diagnostic detail of an
/// accounted-progress ratchet freeze.
#[derive(Debug)]
pub struct Unaccounted {
    pub journal: Box<str>,
    pub binding: u16,
    pub producer: Producer,
    /// Whether a read-derived commit or a causal hint exceeded the ceiling.
    pub kind: UnaccountedKind,
    /// The delta clock which exceeds `ceiling`.
    pub clock: Clock,
    /// Highest commit of this producer which the pending checkpoint, its hints,
    /// or its journal's promoted commits can account for.
    pub ceiling: Clock,
}

#[derive(Debug)]
pub enum UnaccountedKind {
    Commit,
    Hint,
}

/// Error returned by `Frontier::new` when its validation invariants fail.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(
        "JournalFrontier is not ordered on (journal, binding) at index {index}: \
         ({curr_journal}, {curr_binding}) follows ({prev_journal}, {prev_binding})"
    )]
    JournalOutOfOrder {
        index: usize,
        prev_journal: String,
        prev_binding: u16,
        curr_journal: String,
        curr_binding: u16,
    },
    #[error(
        "JournalFrontier is not unique on (journal, binding) at index {index}: \
         ({journal}, {binding})"
    )]
    JournalDuplicate {
        index: usize,
        journal: String,
        binding: u16,
    },
    #[error(
        "ProducerFrontier is not ordered on producer at index {index} in ({binding}, {journal})"
    )]
    ProducerOutOfOrder {
        index: usize,
        journal: String,
        binding: u16,
    },
    #[error(
        "ProducerFrontier is not unique on producer at index {index} in ({binding}, {journal})"
    )]
    ProducerDuplicate {
        index: usize,
        journal: String,
        binding: u16,
    },
}

impl Frontier {
    /// Maximum number of unresolved-hint lines rendered by
    /// [`Self::describe_unresolved`] before the remainder is elided.
    const DESCRIBE_UNRESOLVED_MAX_LINES: usize = 20;

    /// Construct a `Frontier` from journal entries and per-shard flushed LSNs,
    /// validating that entries are sorted and unique on `(journal, binding)` and
    /// that producers within each entry are sorted and unique on `producer`.
    pub fn new(journals: Vec<JournalFrontier>, flushed_lsn: Vec<u64>) -> Result<Self, Error> {
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
                std::cmp::Ordering::Equal => {
                    return Err(Error::JournalDuplicate {
                        index: index + 1,
                        journal: curr.journal.to_string(),
                        binding: curr.binding,
                    });
                }
                std::cmp::Ordering::Greater => {
                    return Err(Error::JournalOutOfOrder {
                        index: index + 1,
                        prev_journal: prev.journal.to_string(),
                        prev_binding: prev.binding,
                        curr_journal: curr.journal.to_string(),
                        curr_binding: curr.binding,
                    });
                }
            }
        }
        for jf in &journals {
            for (index, window) in jf.producers.windows(2).enumerate() {
                let (prev, curr) = (&window[0], &window[1]);
                match prev.producer.cmp(&curr.producer) {
                    std::cmp::Ordering::Less => {}
                    std::cmp::Ordering::Equal => {
                        return Err(Error::ProducerDuplicate {
                            index: index + 1,
                            journal: jf.journal.to_string(),
                            binding: jf.binding,
                        });
                    }
                    std::cmp::Ordering::Greater => {
                        return Err(Error::ProducerOutOfOrder {
                            index: index + 1,
                            journal: jf.journal.to_string(),
                            binding: jf.binding,
                        });
                    }
                }
            }
        }
        let unresolved_hints = count_unresolved_hints(&journals);
        Ok(Self {
            journals,
            flushed_lsn,
            latest_backfill_begin: BTreeMap::new(),
            latest_backfill_complete: BTreeMap::new(),
            unresolved_hints,
        })
    }

    fn merge_binding_clocks(
        mut a: BTreeMap<u16, Clock>,
        b: BTreeMap<u16, Clock>,
    ) -> BTreeMap<u16, Clock> {
        for (binding, clock) in b {
            a.entry(binding)
                .and_modify(|current: &mut Clock| *current = (*current).max(clock))
                .or_insert(clock);
        }
        a
    }

    /// Element-wise max of two per-shard `flushed_lsn` vectors.
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
    /// Both inputs must be sorted and unique on `(journal, binding)`.
    ///
    /// Reduction is symmetric, so the shorter journal list is folded into the
    /// longer one in place. Reducing a small delta into a large cumulative
    /// Frontier costs O(delta · log(base / delta)), plus one pass over the
    /// base only if the delta adds journals, and `unresolved_hints` is
    /// adjusted for just the entries the delta touched.
    pub fn reduce(self, other: Self) -> Self {
        // On a tie `other` is the base, so that of two journal-less Frontiers
        // `other`'s `unresolved_hints` is kept, as it always has been.
        let (mut base, delta) = if self.journals.len() > other.journals.len() {
            (self, other)
        } else {
            (other, self)
        };
        let Self {
            journals: delta_journals,
            flushed_lsn: delta_flushed_lsn,
            latest_backfill_begin: delta_backfill_begin,
            latest_backfill_complete: delta_backfill_complete,
            unresolved_hints: delta_unresolved_hints,
        } = delta;

        base.flushed_lsn =
            Self::merge_flushed_lsn(std::mem::take(&mut base.flushed_lsn), delta_flushed_lsn);
        base.latest_backfill_begin = Self::merge_binding_clocks(
            std::mem::take(&mut base.latest_backfill_begin),
            delta_backfill_begin,
        );
        base.latest_backfill_complete = Self::merge_binding_clocks(
            std::mem::take(&mut base.latest_backfill_complete),
            delta_backfill_complete,
        );

        if delta_journals.is_empty() {
            return base;
        }

        // Max-merging can either create or eliminate unresolved-ness, so
        // re-count each reduced entry. Inserted entries carry their count.
        let mut unresolved_hints = base.unresolved_hints + delta_unresolved_hints;

        merge_into(
            &mut base.journals,
            delta_journals,
            |base, item| {
                (base.journal.as_ref(), base.binding).cmp(&(item.journal.as_ref(), item.binding))
            },
            |base, item| {
                unresolved_hints -=
                    count_producer_hints(&base.producers) + count_producer_hints(&item.producers);
                *base = std::mem::take(base).reduce(item);
                unresolved_hints += count_producer_hints(&base.producers);
            },
        );
        debug_assert_eq!(unresolved_hints, count_unresolved_hints(&base.journals));
        base.unresolved_hints = unresolved_hints;

        base
    }

    /// Set every unresolved hint to zero. Call this on a coordinator's cumulative
    /// Frontier directly after it reduces in a Session Frontier with
    /// `unresolved_hints == 0`: that Frontier resolves or discharges every hint
    /// of every earlier peek.
    pub fn clear_discharged_hints(&mut self) {
        let cleared = self.unresolved_hints;
        if cleared == 0 {
            return;
        }
        for pf in self.journals.iter_mut().flat_map(|jf| &mut jf.producers) {
            if pf.hinted_commit > pf.last_commit {
                pf.hinted_commit = Clock::zero();
            }
        }
        self.unresolved_hints = 0;

        service_kit::event!(
            tracing::Level::WARN,
            "coordinator",
            cleared,
            "cleared causal hint(s) of preceding peeks which the Session discharged rather than resolved",
        );
    }

    /// Raise the `gap_floor` of each journal to its binding's floor in `floors`.
    /// Persistence flattens gap floors to one per binding (the largest of its
    /// journals), and this re-hydrates them onto a recovered Frontier.
    pub fn hydrate_gap_floors(&mut self, floors: &BTreeMap<u16, Clock>) {
        for jf in &mut self.journals {
            if let Some(floor) = floors.get(&jf.binding) {
                jf.gap_floor = jf.gap_floor.max(*floor);
            }
        }
    }

    /// Look up a journal entry by `(journal, binding)`.
    pub fn find_journal(&self, journal: &str, binding: u16) -> Option<usize> {
        self.journals
            .binary_search_by(|jf| {
                jf.journal
                    .as_ref()
                    .cmp(journal)
                    .then(jf.binding.cmp(&binding))
            })
            .ok()
    }

    /// Advance `last_commit` on producers of `self` up to each one's
    /// `hinted_commit` using progress from `other`.
    ///
    /// Uses an ordered merge on `(journal, binding)` then `producer`,
    /// matching the sorted invariants of both frontiers.
    ///
    /// Returns `(advanced, resolved)`:
    /// - `advanced`: producers whose `last_commit` advanced by any amount.
    /// - `resolved`: producers whose `last_commit` reached `hinted_commit`
    ///   (a subset of `advanced`).
    pub fn resolve_hints(&mut self, other: &Frontier) -> (usize, usize) {
        let mut advanced = 0usize;
        let mut resolved = 0usize;
        let mut lhs = self.journals.iter_mut().peekable();
        let mut rhs = other.journals.iter().peekable();

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
                    let (a, r) = lhs.resolve_hints(rhs);
                    advanced += a;
                    resolved += r;
                }
            }
        }

        // `resolved` is exact: each producer is visited at most once per ordered-merge
        // walk and counted only when transitioning across `hinted_commit`.
        self.unresolved_hints -= resolved;
        (advanced, resolved)
    }

    /// Clear causal hints of `self` which the session's `cumulative` Frontier
    /// accounts for (see [`JournalFrontier::accounts_for`]). Such a hint is
    /// stale: its actual read has already promoted past it, has forgotten it, or
    /// can't reach it. It's typically held by a journal reading from behind its
    /// peers, such as a re-enabled binding, which may never observe the ACK that
    /// would resolve it on the forward path.
    ///
    /// A hint discharged by a gap floor stamps that floor onto its entry, so
    /// the floor is delivered with exactly the checkpoint which relied on it.
    /// A producer left with neither a hint nor a commit is dropped, as is an
    /// entry left empty. `unresolved_hints` is decremented for each cleared hint.
    ///
    /// Returns counts of hints cleared `(by_commit, by_prune_floor, by_gap_floor)`.
    pub fn prune_hints(&mut self, cumulative: &Frontier) -> (usize, usize, usize) {
        let mut by_commit = 0usize;
        let mut by_prune_floor = 0usize;
        let mut by_gap_floor = 0usize;
        let mut next = 0;

        self.journals.retain_mut(|jf| {
            let Some(completed) = cumulative.gallop_journal(&mut next, &jf.journal, jf.binding)
            else {
                return true; // No authority over this journal.
            };
            let mut stamp_gap_floor = false;

            jf.producers.retain_mut(|pf| {
                if pf.hinted_commit <= pf.last_commit {
                    return true; // Hint is already resolved.
                }
                match completed.accounts_for(pf.producer, pf.hinted_commit, true) {
                    None => return true, // Hint is at the frontier.
                    Some(Accounted::Commit) => by_commit += 1,
                    Some(Accounted::PruneFloor) => by_prune_floor += 1,
                    Some(Accounted::GapFloor) => {
                        by_gap_floor += 1;
                        stamp_gap_floor = true;
                    }
                }
                // Hint is stale. Retain only if we also saw a commit.
                pf.hinted_commit = Clock::zero();
                pf.last_commit != Clock::zero()
            });

            if stamp_gap_floor {
                jf.gap_floor = jf.gap_floor.max(completed.gap_floor);
            }
            !jf.is_empty()
        });

        self.unresolved_hints -= by_commit + by_prune_floor + by_gap_floor;
        (by_commit, by_prune_floor, by_gap_floor)
    }

    /// Judge whether `delta` is *accounted* for by `self`, an unresolved
    /// pending checkpoint, together with the session's `cumulative` Frontier.
    ///
    /// Each producer entry of `delta` reports a read-derived commit and a
    /// causal hint. Each clock must be covered by an accounting authority:
    ///
    /// 1. The pending checkpoint's entry of the producer, if the clock is
    ///    at-or-below its `last_commit` or `hinted_commit`.
    /// 2. The cumulative Frontier's entry of the same `(journal, binding)`, per
    ///    [`JournalFrontier::accounts_for`]: a promoted commit, the prune floor
    ///    of a forgotten producer, or (for hints only) the gap floor.
    ///
    /// `delta` is accounted iff every clock of every entry is covered.
    /// Returns the first unaccounted entry in `(journal, binding, producer)`
    /// order, or None if `delta` is accounted.
    ///
    /// # Soundness
    ///
    /// An accounted delta is reduced wholesale into the pending checkpoint
    /// (`CheckpointPipeline::ratchet`). Otherwise the pending checkpoint's hints
    /// resolve conservatively (`Frontier::resolve_hints`), and the delta waits
    /// for the next checkpoint. Adoption must preserve two things: the
    /// transactions the checkpoint commits, and the Frontier invariant.
    ///
    /// The committed transactions are what this judgment protects. Reduction
    /// takes the max of each clock, and each authority bounds what that max
    /// adds:
    ///
    /// - At-or-below the pending `last_commit`, a commit adds nothing. Up to
    ///   the pending `hinted_commit`, it's the advance `resolve_hints` makes
    ///   anyway, as it caps `last_commit` at `hinted_commit`. A hint up to
    ///   either is already within the pending checkpoint's causal extent.
    /// - At-or-below a promoted commit, a commit belongs to a checkpoint
    ///   already taken, and a re-read sequences as a duplicate. Deltas report
    ///   these routinely: a producer beginning a span carries its prior
    ///   `last_commit`.
    /// - At-or-below the prune floor, a commit of a forgotten producer is no
    ///   newer than the `last_commit` pruning forgot
    ///   ([`JournalFrontier::prune_stale_producers`]), so it belongs to a
    ///   checkpoint already taken. [`crate::PRODUCER_STALENESS_HORIZON`] keeps
    ///   the floor below the unread commits of other producers.
    /// - Below the gap floor, a hint's ACK is unreachable, so holding the
    ///   checkpoint for it gains nothing and may stall forever. A byte gap says
    ///   nothing about whether a producer committed, so it never covers a
    ///   commit.
    ///
    /// `ratchet` prunes the hints that `cumulative` covers before reducing, and
    /// the rest lie within the pending checkpoint's own. So adoption adds no
    /// hint, and newer progress can't starve the checkpoint's resolution. Once
    /// resolved, an adopted checkpoint commits what the conservative path
    /// would, except for clocks that `cumulative` already covers.
    ///
    /// The Frontier invariant needs something else: that each adopted delta is
    /// the next delta of the read. The pending checkpoint and every delta
    /// adopted since are consecutive deltas of one continuous read. Reducing in
    /// the next delta of that read preserves the invariant at the delta's cut,
    /// whether or not the delta is accounted. That cut has the read's true
    /// offsets, including open spans' `+begin`s, in place of the conservative
    /// `-M` bump.
    ///
    /// So rejection must be wholesale and sticky, to keep adopted deltas
    /// consecutive. Within a journal, adopting only the accounted entries could
    /// raise the cut floor past a rejected commit's offset, leaving its close
    /// uncovered. Across deltas, adopting one after a rejected delta would skip
    /// the rejected content. So the first rejection freezes the ratchet until
    /// the next promotion (`CheckpointPipeline::ratchet_frozen`).
    pub fn first_unaccounted(
        &self,
        delta: &Frontier,
        cumulative: &Frontier,
    ) -> Option<Unaccounted> {
        let (mut next_base, mut next_cumulative) = (0, 0);

        for delta_jf in &delta.journals {
            // The pending checkpoint may not carry this journal at all:
            // a read can report journals it never had.
            let base_producers = self
                .gallop_journal(&mut next_base, &delta_jf.journal, delta_jf.binding)
                .map_or(&[][..], |jf| jf.producers.as_slice());
            let completed = cumulative.gallop_journal(
                &mut next_cumulative,
                &delta_jf.journal,
                delta_jf.binding,
            );

            for delta_p in &delta_jf.producers {
                let mut ceiling = Clock::zero();

                if let Ok(index) =
                    base_producers.binary_search_by(|base_p| base_p.producer.cmp(&delta_p.producer))
                {
                    let base_p = &base_producers[index];
                    ceiling = base_p.last_commit.max(base_p.hinted_commit);
                }
                let accounted = |clock: Clock, is_hint: bool| {
                    clock <= ceiling
                        || completed.is_some_and(|completed| {
                            completed
                                .accounts_for(delta_p.producer, clock, is_hint)
                                .is_some()
                        })
                };

                let (kind, clock) = if !accounted(delta_p.last_commit, false) {
                    (UnaccountedKind::Commit, delta_p.last_commit)
                } else if !accounted(delta_p.hinted_commit, true) {
                    (UnaccountedKind::Hint, delta_p.hinted_commit)
                } else {
                    continue;
                };

                if let Some(completed) = completed {
                    if let Ok(index) = completed
                        .producers
                        .binary_search_by(|p| p.producer.cmp(&delta_p.producer))
                    {
                        ceiling = ceiling.max(completed.producers[index].last_commit);
                    }
                }
                return Some(Unaccounted {
                    journal: delta_jf.journal.clone(),
                    binding: delta_jf.binding,
                    producer: delta_p.producer,
                    kind,
                    clock,
                    ceiling,
                });
            }
        }

        None
    }

    /// Find the entry of `(journal, binding)`, galloping forward from `*next`
    /// and leaving `*next` at its position. Successive lookups must be of
    /// ascending keys, as when walking another Frontier's journals.
    fn gallop_journal(
        &self,
        next: &mut usize,
        journal: &str,
        binding: u16,
    ) -> Option<&JournalFrontier> {
        match gallop_by(&self.journals[*next..], |jf| {
            (jf.journal.as_ref(), jf.binding).cmp(&(journal, binding))
        }) {
            Ok(index) => {
                *next += index;
                Some(&self.journals[*next])
            }
            Err(index) => {
                *next += index;
                None
            }
        }
    }

    /// Seed the `prune_floor` of each journal of a resume checkpoint, which
    /// becomes the session's cumulative Frontier. Producers were forgotten
    /// before this session — by the runtime's prune, or by V1 — only once they
    /// trailed their journal's newest commit by at least
    /// [`crate::PRODUCER_STALENESS_HORIZON`], so that newest commit less the
    /// horizon bounds every forgotten clock.
    pub fn seed_prune_floors(&mut self) {
        let horizon = (crate::PRODUCER_STALENESS_HORIZON.as_secs() * 10_000_000) << 4;

        for jf in &mut self.journals {
            let newest_commit = jf
                .producers
                .iter()
                .map(|p| p.last_commit)
                .max()
                .unwrap_or_default();

            let floor = Clock::from_u64(newest_commit.as_u64().saturating_sub(horizon));
            jf.prune_floor = jf.prune_floor.max(floor);
        }
    }

    /// Prune stale producers ([`JournalFrontier::prune_stale_producers`]) of
    /// the journals `touched` names, or of every journal if None, returning
    /// the number pruned. Prunes are possible only when a journal's newest
    /// commit has advanced, so a session prunes only the journals of each
    /// checkpoint it takes.
    pub fn prune_stale_producers(&mut self, touched: Option<&Frontier>) -> usize {
        let mut pruned = 0;

        let Some(touched) = touched else {
            for jf in &mut self.journals {
                jf.prune_stale_producers(|_| false, |_| pruned += 1);
            }
            return pruned;
        };

        let mut next = 0;
        for touched in &touched.journals {
            let found = gallop_by(&self.journals[next..], |jf| {
                (jf.journal.as_ref(), jf.binding).cmp(&(touched.journal.as_ref(), touched.binding))
            });
            match found {
                Ok(index) => {
                    next += index;
                    self.journals[next].prune_stale_producers(|_| false, |_| pruned += 1);
                }
                Err(index) => next += index,
            }
        }
        pruned
    }

    /// Encode this Frontier as a proto `shuffle::Frontier`, including
    /// `flushed_lsn`. Journal names within the proto are delta-encoded —
    /// see `JournalFrontier::encode` for the layout.
    pub fn encode(&self) -> shuffle::Frontier {
        let mut proto = JournalFrontier::encode(&self.journals);
        proto.flushed_lsn = self.flushed_lsn.iter().map(|lsn| lsn.as_u64()).collect();
        proto.latest_backfill_begin = self
            .latest_backfill_begin
            .iter()
            .map(|(binding, clock)| shuffle::frontier::BackfillBegin {
                binding: *binding as u32,
                clock: clock.as_u64(),
            })
            .collect();
        proto.latest_backfill_complete = self
            .latest_backfill_complete
            .iter()
            .map(|(binding, clock)| shuffle::frontier::BackfillComplete {
                binding: *binding as u32,
                clock: clock.as_u64(),
            })
            .collect();
        proto
    }

    /// Decode a proto `shuffle::Frontier` into a validated `Frontier`.
    pub fn decode(mut proto: shuffle::Frontier) -> Result<Self, Error> {
        let flushed_lsn = std::mem::take(&mut proto.flushed_lsn);
        let latest_backfill_begin = std::mem::take(&mut proto.latest_backfill_begin)
            .into_iter()
            .map(|e| (e.binding as u16, Clock::from_u64(e.clock)))
            .collect();
        let latest_backfill_complete = std::mem::take(&mut proto.latest_backfill_complete)
            .into_iter()
            .map(|e| (e.binding as u16, Clock::from_u64(e.clock)))
            .collect();

        let journals: Vec<JournalFrontier> = JournalFrontier::decode(proto).collect();
        let mut frontier = Self::new(journals, flushed_lsn)?;
        frontier.latest_backfill_begin = latest_backfill_begin;
        frontier.latest_backfill_complete = latest_backfill_complete;
        Ok(frontier)
    }

    /// Partition this Frontier into `parts` Frontiers, moving the entries of
    /// each binding into part `part_of(binding)`. Per-shard `flushed_lsn` is
    /// of no binding, and is cloned into every part.
    pub fn partition_bindings(self, parts: usize, part_of: impl Fn(u16) -> usize) -> Vec<Frontier> {
        let Self {
            journals,
            flushed_lsn,
            latest_backfill_begin,
            latest_backfill_complete,
            unresolved_hints: _,
        } = self;

        let mut out: Vec<Frontier> = (0..parts)
            .map(|_| Frontier {
                flushed_lsn: flushed_lsn.clone(),
                ..Default::default()
            })
            .collect();

        // Each part's journals are a subsequence of `journals`, and so remain
        // sorted and unique on (journal, binding).
        for jf in journals {
            out[part_of(jf.binding)].journals.push(jf);
        }
        for (binding, clock) in latest_backfill_begin {
            out[part_of(binding)]
                .latest_backfill_begin
                .insert(binding, clock);
        }
        for (binding, clock) in latest_backfill_complete {
            out[part_of(binding)]
                .latest_backfill_complete
                .insert(binding, clock);
        }
        for part in &mut out {
            part.unresolved_hints = count_unresolved_hints(&part.journals);
        }
        out
    }

    /// Extract producers with unresolved causal hints (`hinted_commit > last_commit`)
    /// into a new Frontier, filtering out journals that have no such producers.
    /// Used at startup to project read-through state from `resume_checkpoint`.
    pub fn project_unresolved_hints(&self) -> Frontier {
        let journals: Vec<JournalFrontier> = self
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
                        ..Default::default()
                    })
                }
            })
            .collect();

        let unresolved_hints = count_unresolved_hints(&journals);
        Frontier {
            journals,
            flushed_lsn: vec![],
            latest_backfill_begin: self.latest_backfill_begin.clone(),
            latest_backfill_complete: self.latest_backfill_complete.clone(),
            unresolved_hints,
        }
    }

    /// Return high-level measures of this Frontier for logging / diagnostics:
    ///  (journals, journal_producers, bytes_read_delta, bytes_behind_delta)
    #[inline]
    pub fn measures(&self) -> (usize, usize, i64, i64) {
        let (bytes_read_delta, bytes_behind_delta, journal_producers) =
            self.journals.iter().fold((0, 0, 0), |(br, bb, jp), jf| {
                (
                    br + jf.bytes_read_delta,
                    bb + jf.bytes_behind_delta,
                    jp + jf.producers.len(),
                )
            });

        (
            self.journals.len(),
            journal_producers,
            bytes_read_delta,
            bytes_behind_delta,
        )
    }

    /// Render a bounded, human-readable description of producers with an
    /// unresolved causal hint (`hinted_commit > last_commit`), one indented
    /// line each. At most [`Self::DESCRIBE_UNRESOLVED_MAX_LINES`] lines are
    /// emitted; any remainder is elided as `… and N more unresolved hint(s)`.
    pub fn describe_unresolved(&self) -> String {
        use std::fmt::Write;

        let mut out = String::new();
        let mut rendered = 0;

        'journals: for jf in &self.journals {
            for p in &jf.producers {
                if p.hinted_commit <= p.last_commit {
                    continue;
                }
                if rendered == Self::DESCRIBE_UNRESOLVED_MAX_LINES {
                    break 'journals;
                }
                write!(
                    &mut out,
                    "\n  journal {:?} binding={} producer={:?} \
                         last_commit={:?} hinted_commit={:?}",
                    jf.journal.as_ref(),
                    jf.binding,
                    p.producer,
                    p.last_commit,
                    p.hinted_commit,
                )
                .unwrap();
                rendered += 1;
            }
        }

        // `unresolved_hints` is maintained as the exact count of unresolved
        // producers, so the remainder needs no second walk.
        let remaining = self.unresolved_hints.saturating_sub(rendered);
        if remaining != 0 {
            write!(&mut out, "\n  … and {remaining} more unresolved hint(s)").unwrap();
        }
        out
    }
}

/// Walk a journal list and count producers with `hinted_commit > last_commit`.
fn count_unresolved_hints(journals: &[JournalFrontier]) -> usize {
    journals
        .iter()
        .map(|jf| count_producer_hints(&jf.producers))
        .sum()
}

fn count_producer_hints(producers: &[ProducerFrontier]) -> usize {
    producers
        .iter()
        .filter(|p| p.hinted_commit > p.last_commit)
        .count()
}

/// Locate the target of `cmp` within sorted `items`, as `binary_search_by`
/// does, but probing outward from the front: the cost is logarithmic in the
/// target's distance from the front rather than in `items.len()`. Walking a
/// short sorted sequence through a long one by galloping from each previous
/// position is what keeps small-into-large merges and lookups cheap.
pub(crate) fn gallop_by<T>(
    items: &[T],
    mut cmp: impl FnMut(&T) -> std::cmp::Ordering,
) -> Result<usize, usize> {
    let mut bound = 1;
    while bound < items.len() && cmp(&items[bound - 1]) == std::cmp::Ordering::Less {
        bound *= 2;
    }
    // items[bound/2 - 1] (if any) precedes the target, and items[bound - 1]
    // (if any) does not.
    let lo = bound / 2;
    let hi = bound.min(items.len());

    match items[lo..hi].binary_search_by(cmp) {
        Ok(index) => Ok(lo + index),
        Err(index) => Err(lo + index),
    }
}

/// Merge sorted and unique `delta` into sorted and unique `base` in place,
/// where `cmp` orders a `base` item against a `delta` item. `fold` reduces a
/// delta item into its matching base item, and unmatched delta items are
/// inserted.
///
/// Each delta item is found by galloping from the previous one, so the cost is
/// O(delta · log(base / delta)), plus one pass over `base` only if a delta item
/// is inserted.
fn merge_into<T>(
    base: &mut Vec<T>,
    delta: Vec<T>,
    cmp: impl Fn(&T, &T) -> std::cmp::Ordering,
    mut fold: impl FnMut(&mut T, T),
) {
    let mut inserts = Vec::new();
    let mut next = 0;

    for item in delta {
        match gallop_by(&base[next..], |probe| cmp(probe, &item)) {
            Ok(index) => {
                next += index;
                fold(&mut base[next], item);
                next += 1;
            }
            Err(index) => {
                next += index;
                inserts.push(item);
            }
        }
    }
    if inserts.is_empty() {
        return;
    }

    let mut merged = Vec::with_capacity(base.len() + inserts.len());
    let mut inserts = inserts.into_iter().peekable();

    for item in base.drain(..) {
        while let Some(insert) =
            inserts.next_if(|insert| cmp(&item, insert) == std::cmp::Ordering::Greater)
        {
            merged.push(insert);
        }
        merged.push(item);
    }
    merged.extend(inserts);
    *base = merged;
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::testing::{jf, jf_with_bytes, pf, pf_tuple};
    use log::Lsn;
    use std::collections::BTreeMap;

    #[test]
    fn test_clear_discharged_hints() {
        // Two peeks carry a hint the resolved frontier never names: the Session
        // discharged it and dropped the producer.
        let peek = || Frontier {
            journals: vec![jf("journal/A", 0, vec![pf(0x09, 0, 100, 0)])],
            unresolved_hints: 1,
            ..Default::default()
        };
        let resolved = Frontier {
            journals: vec![jf("journal/B", 0, vec![pf(0x0b, 200, 0, -700)])],
            ..Default::default()
        };

        let mut f = Frontier::default()
            .reduce(peek())
            .reduce(peek())
            .reduce(resolved);
        assert_eq!(f.unresolved_hints, 1);

        f.clear_discharged_hints();
        assert_eq!(f.unresolved_hints, 0);
        assert_eq!(pf_tuple(&f.journals[0].producers[0]), (0, 0, 0));
        assert_eq!(pf_tuple(&f.journals[1].producers[0]), (200, 0, -700));
    }

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
            // Equal magnitude: the open (non-negative) span begin wins,
            // in either argument order. A producer's next span begins exactly
            // at its previous committed end offset, so this tie is routine —
            // and preferring the committed side would erase the open span from
            // the durable checkpoint, silently skipping its documents on a
            // checkpoint-derived restart.
            ((100, 0, -500), (100, 0, 500), (100, 0, 500)),
            ((100, 0, 500), (100, 0, -500), (100, 0, 500)),
            // A later commit closes an open span begin of larger magnitude, as
            // when a hint-resolved `{H, H, -M}` meets a `+O` with M < O,
            // or with M == 0 from a hint-only delta.
            ((100, 400, 207), (400, 400, -150), (400, 400, -207)),
            ((100, 0, 207), (400, 400, 0), (400, 400, -207)),
            // ... including on an equal magnitude.
            ((100, 0, 500), (400, 400, -500), (400, 400, -500)),
            // An open span begin carrying the latest commit stays open.
            ((400, 0, 900), (400, 400, -500), (400, 400, 900)),
            ((400, 0, 900), (100, 0, -500), (400, 0, 900)),
        ];

        for (a, b, expect) in cases {
            for (a, b) in [(a, b), (b, a)] {
                let r = pf(0x01, a.0, a.1, a.2).reduce(pf(0x01, b.0, b.1, b.2));
                assert_eq!(pf_tuple(&r), expect, "reduce({a:?}, {b:?})");
            }
        }
    }

    #[test]
    fn test_producer_frontier_reduce_groupings() {
        // Checkpoints are reduced in differing groupings: the session folds
        // promoted generations into `ready`, and the runtime folds emitted
        // frontiers into transaction extents. Each case must reduce the same
        // under both groupings, (a, b), c and a, (b, c).
        let cases: Vec<[(u64, u64, i64); 4]> = vec![
            // An open span begin +207, then a peek of its unresolved hint
            // {0, H, 0}, then its resolution {H, H, 0} from a hint-only delta
            // (M == 0). The span recovers closed at its begin.
            [(100, 0, 207), (0, 400, 0), (400, 400, 0), (400, 400, -207)],
            // As above, with the resolution's cut floor M = 396 beyond the
            // span begin. The span recovers closed at M.
            [
                (100, 0, 207),
                (0, 400, 0),
                (400, 400, -396),
                (400, 400, -396),
            ],
            // A resolved span, then the producer's next span begin, which
            // carries the resolved commit and stays open.
            [(100, 0, 207), (400, 400, 0), (400, 0, 900), (400, 400, 900)],
        ];

        for [a, b, c, expect] in cases {
            let [a, b, c] = [a, b, c].map(|(commit, hint, offset)| pf(0x01, commit, hint, offset));
            let left = a.clone().reduce(b.clone()).reduce(c.clone());
            let right = a.reduce(b.reduce(c));
            assert_eq!(pf_tuple(&left), expect, "(a, b), c");
            assert_eq!(pf_tuple(&right), expect, "a, (b, c)");
        }
    }

    /// Horizon expressed in the whole seconds which `pf` speaks.
    const HORIZON_SECS: u64 = crate::PRODUCER_STALENESS_HORIZON.as_secs();

    /// `entry` of a cumulative Frontier, with floors given in seconds.
    fn with_floors(entry: JournalFrontier, prune_floor: u64, gap_floor: u64) -> JournalFrontier {
        JournalFrontier {
            prune_floor: from_secs(prune_floor),
            gap_floor: from_secs(gap_floor),
            ..entry
        }
    }

    /// Clock at `seconds` past the unix epoch, matching `testing::pf`'s
    /// convention that zero is the `Clock::zero()` sentinel.
    fn from_secs(seconds: u64) -> Clock {
        if seconds == 0 {
            Clock::zero()
        } else {
            Clock::from_unix(seconds, 0)
        }
    }

    /// Build a ProducerFrontier with RAW clock values (not seconds), for
    /// OBSERVED_COMMIT_FLOOR cases where the exact raw encoding matters.
    fn pf_raw(id: u8, last_commit: u64, hinted_commit: u64, offset: i64) -> ProducerFrontier {
        ProducerFrontier {
            producer: crate::testing::producer(id),
            last_commit: Clock::from_u64(last_commit),
            hinted_commit: Clock::from_u64(hinted_commit),
            offset,
        }
    }

    #[test]
    fn test_reduce_preserves_observed_commit_floor() {
        let hint_clock = Clock::from_unix(200, 0).as_u64();
        let commit_clock = Clock::from_unix(300, 0).as_u64();

        // A floored entry `{1, 0, 0}` (real span at journal head) reduced with a
        // hint-only entry `{0, H, 0}` keeps the floor in both argument orders:
        // `max(1, 0) = 1`, and the equal-magnitude offset tie keeps 0. Regressing
        // to 0 here would reclassify the span as hint-only on the next recovery,
        // silently dropping it.
        let floored = || pf_raw(0x01, OBSERVED_COMMIT_FLOOR, 0, 0);
        let hint = || pf_raw(0x01, 0, hint_clock, 0);
        for (a, b) in [(floored(), hint()), (hint(), floored())] {
            let r = a.reduce(b);
            assert_eq!(
                r.last_commit.as_u64(),
                OBSERVED_COMMIT_FLOOR,
                "the floor survives a hint merge",
            );
            assert_eq!(r.hinted_commit.as_u64(), hint_clock);
            assert_eq!(r.offset, 0);
        }

        // Read-derived progress supersedes the floor: a real `last_commit`
        // wins by max, and the larger offset magnitude wins.
        let read = || pf_raw(0x01, commit_clock, 0, -500);
        for (a, b) in [(floored(), read()), (read(), floored())] {
            let r = a.reduce(b);
            assert_eq!(r.last_commit.as_u64(), commit_clock);
            assert_eq!(r.offset, -500);
        }
    }

    #[test]
    fn test_resolve_hints_elevates_last_commit_floor() {
        // A floored entry carrying an unresolved hint `{1, H, 0}` IS elevated by
        // read-derived progress: progress with `last_commit >= H` proves the
        // read committed (or rolled back) the offset-zero span, and the elevated
        // `{H, 0}` then correctly recovers as NOT gapped.
        let hint_clock = Clock::from_unix(200, 0);
        let mut pending = Frontier {
            journals: vec![jf(
                "journal/A",
                0,
                vec![pf_raw(0x01, OBSERVED_COMMIT_FLOOR, hint_clock.as_u64(), 0)],
            )],
            flushed_lsn: vec![],
            unresolved_hints: 1,
            ..Default::default()
        };
        let progressed = Frontier {
            journals: vec![jf("journal/A", 0, vec![pf(0x01, 250, 0, -800)])],
            flushed_lsn: vec![],
            unresolved_hints: 0,
            ..Default::default()
        };

        let (advanced, resolved) = pending.resolve_hints(&progressed);
        assert_eq!((advanced, resolved), (1, 1));
        assert_eq!(
            pending.journals[0].producers[0].last_commit, hint_clock,
            "elevated to (and capped at) the hinted commit",
        );
        assert_eq!(
            pending.journals[0].producers[0].offset, 0,
            "offset stays 0: this journal's only entry is the floored span at \
             offset 0, so its cut floor M == 0 and the flip writes -0",
        );
        assert_eq!(pending.unresolved_hints, 0);

        // Companion: the floored entry shares its journal with a committed sibling
        // at a larger offset magnitude, so M > 0 and the flip lands on -M. P3 (a
        // committed -800) sets M = 800; P1's floored span, elevated to its hint,
        // flips from 0 to -800 — recovering NOT gapped and never re-replaying.
        let mut pending = Frontier {
            journals: vec![jf(
                "journal/A",
                0,
                vec![
                    pf_raw(0x01, OBSERVED_COMMIT_FLOOR, hint_clock.as_u64(), 0),
                    pf(0x03, 100, 0, -800),
                ],
            )],
            flushed_lsn: vec![],
            unresolved_hints: 1,
            ..Default::default()
        };
        let progressed = Frontier {
            journals: vec![jf("journal/A", 0, vec![pf(0x01, 250, 0, -900)])],
            flushed_lsn: vec![],
            unresolved_hints: 0,
            ..Default::default()
        };
        let (advanced, resolved) = pending.resolve_hints(&progressed);
        assert_eq!((advanced, resolved), (1, 1));
        assert_eq!(
            pending.journals[0].producers[0].last_commit, hint_clock,
            "the floored entry is elevated to (and capped at) its hinted commit",
        );
        assert_eq!(
            pending.journals[0].producers[0].offset, -800,
            "the flip writes -M, the journal's max offset magnitude (P3's 800)",
        );
        assert_eq!(pending.unresolved_hints, 0);
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
            latest_backfill_begin: BTreeMap::from([(0, Clock::from_u64(100))]),
            latest_backfill_complete: BTreeMap::from([(1, Clock::from_u64(140))]),
            unresolved_hints: 0,
        };
        let hints = Frontier {
            journals: vec![
                jf_with_bytes("journal/B", 0, vec![pf(0x03, 0, 300, 0)], 50, -300),
                jf("journal/C", 1, vec![pf(0x03, 0, 300, 0)]),
            ],
            flushed_lsn: vec![Lsn::from_u64(40), Lsn::from_u64(20), Lsn::from_u64(30)],
            latest_backfill_begin: BTreeMap::from([(0, Clock::from_u64(120))]),
            latest_backfill_complete: BTreeMap::from([(0, Clock::from_u64(130))]),
            unresolved_hints: 2,
        };
        let (mut reads, mut hints) = (reads, hints);
        reads.journals[1].gap_floor = Clock::from_u64(700);
        hints.journals[0].gap_floor = Clock::from_u64(500);
        hints.journals[1].gap_floor = Clock::from_u64(1_100);
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
        // Per-binding max of backfill clocks across both inputs.
        assert_eq!(r.latest_backfill_begin.get(&0), Some(&Clock::from_u64(120)));
        assert_eq!(
            r.latest_backfill_complete.get(&0),
            Some(&Clock::from_u64(130))
        );
        assert_eq!(
            r.latest_backfill_complete.get(&1),
            Some(&Clock::from_u64(140))
        );
        // Per-journal max of gap floors.
        assert_eq!(
            r.journals.iter().map(|j| j.gap_floor).collect::<Vec<_>>(),
            vec![Clock::zero(), Clock::from_u64(700), Clock::from_u64(1_100)],
        );

        // Identity: reducing with an empty frontier preserves all fields.
        let f = Frontier {
            journals: vec![jf("j", 0, vec![pf(0x01, 1, 0, -1)])],
            flushed_lsn: vec![Lsn::from_u64(10), Lsn::from_u64(20)],
            latest_backfill_begin: BTreeMap::from([(0, Clock::from_u64(100))]),
            latest_backfill_complete: BTreeMap::from([(0, Clock::from_u64(200))]),
            unresolved_hints: 0,
        };
        let r = f.clone().reduce(Frontier::default());
        assert_eq!(r.journals.len(), 1);
        assert_eq!(r.flushed_lsn, vec![Lsn::from_u64(10), Lsn::from_u64(20)]);
        assert_eq!(r.latest_backfill_begin, f.latest_backfill_begin);
        assert_eq!(r.latest_backfill_complete, f.latest_backfill_complete);
        let r = Frontier::default().reduce(f);
        assert_eq!(r.journals.len(), 1);
        assert_eq!(r.flushed_lsn, vec![Lsn::from_u64(10), Lsn::from_u64(20)]);
        assert_eq!(r.latest_backfill_begin.get(&0), Some(&Clock::from_u64(100)));
        assert_eq!(
            r.latest_backfill_complete.get(&0),
            Some(&Clock::from_u64(200))
        );
        assert!(
            Frontier::default()
                .reduce(Frontier::default())
                .journals
                .is_empty()
        );
    }

    #[test]
    fn test_gallop_by_matches_binary_search() {
        for len in 0..40usize {
            let items: Vec<usize> = (0..len).map(|i| i * 2 + 1).collect();
            for target in 0..=(len * 2 + 2) {
                assert_eq!(
                    gallop_by(&items, |probe| probe.cmp(&target)),
                    items.binary_search(&target),
                    "len {len} target {target}",
                );
            }
        }
    }

    #[test]
    fn test_merge_into() {
        // Each delta is folded into (odd) matches, or inserted (even) at the
        // front, middle, and back of `base`, through both sides of the
        // insertion splice.
        for delta in [
            vec![],
            vec![(0, 10)],
            vec![(1, 10), (5, 10)],
            vec![(0, 10), (2, 10), (3, 10), (10, 10), (12, 10)],
            vec![(4, 10), (6, 10), (8, 10)],
            vec![(9, 10), (11, 10)],
        ] {
            let mut base: Vec<(u32, u32)> = vec![(1, 1), (3, 1), (5, 1), (7, 1), (9, 1)];

            let mut expect = base.clone();
            for (key, value) in &delta {
                match expect.binary_search_by_key(key, |(k, _)| *k) {
                    Ok(index) => expect[index].1 += value,
                    Err(index) => expect.insert(index, (*key, *value)),
                }
            }

            merge_into(
                &mut base,
                delta.clone(),
                |base, item| base.0.cmp(&item.0),
                |base, item| base.1 += item.1,
            );
            assert_eq!(base, expect, "delta {delta:?}");
        }
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
    fn test_journal_frontier_encode_decode_round_trip() {
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

        let proto = JournalFrontier::encode(&frontier.journals);
        assert_eq!(proto.journals.len(), 5);

        // The first entry must have truncate=0 and the full journal name
        // as suffix; subsequent entries are delta-encoded.
        let first = &proto.journals[0];
        assert_eq!(first.journal_name_truncate_delta, 0);
        assert_eq!(first.journal_name_suffix, &*frontier.journals[0].journal);

        let decoded: Vec<_> = JournalFrontier::decode(proto).collect();
        assert_eq!(decoded.len(), frontier.journals.len());
        for (a, b) in decoded.iter().zip(frontier.journals.iter()) {
            assert_eq!(&*a.journal, &*b.journal);
            assert_eq!(a.binding, b.binding);
            assert_eq!(a.bytes_read_delta, b.bytes_read_delta);
            assert_eq!(a.bytes_behind_delta, b.bytes_behind_delta);
        }
    }

    #[test]
    fn test_encode_empty() {
        let proto = JournalFrontier::encode(&[]);
        assert!(proto.journals.is_empty());
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
            latest_backfill_begin: BTreeMap::new(),
            latest_backfill_complete: BTreeMap::new(),
            unresolved_hints: 2,
        };

        // Progressed: journal/A has P1 with last_commit=250 (matches hint @200),
        // journal/B has P3 with last_commit=250 (not enough @300).
        let progressed = Frontier {
            journals: vec![
                jf("journal/A", 0, vec![pf(0x01, 250, 0, -800)]),
                jf("journal/B", 0, vec![pf(0x03, 250, 0, -600)]),
            ],
            flushed_lsn: vec![],
            latest_backfill_begin: BTreeMap::new(),
            latest_backfill_complete: BTreeMap::new(),
            unresolved_hints: 0,
        };

        let (advanced, resolved) = pending.resolve_hints(&progressed);
        // P1 in journal/A: fully resolved (progressed 250 >= hinted 200).
        // P3 in journal/B: partially advanced (last_commit 0 → 250) but not
        // resolved (still < hinted 300). Both count as "advanced".
        assert_eq!((advanced, resolved), (2, 1));

        // P1's last_commit is set to hinted_commit (200s), capping at hint.
        assert_eq!(
            pending.journals[0].producers[0].last_commit.to_unix().0,
            200
        );
        // P3 partially advanced to progressed.last_commit (250s), still under hint (300s).
        assert_eq!(
            pending.journals[1].producers[0].last_commit.to_unix().0,
            250
        );

        // On advancement, `offset` flips to `-M` — self's own cut floor, the max
        // offset magnitude of the journal's entries — never `progressed.offset`,
        // which could overshoot a capped resolution. journal/A has only P1 at
        // magnitude 100, so its flip lands back on -100 (value unchanged). journal/B's
        // max magnitude is 500 (P5's -500), so P3's partial advancement flips its
        // offset from 0 to -500.
        assert_eq!(pending.journals[0].producers[0].offset, -100);
        assert_eq!(pending.journals[1].producers[0].offset, -500);

        // Second round: P3 now has enough progress to fully resolve.
        let progressed2 = Frontier {
            journals: vec![jf("journal/B", 0, vec![pf(0x03, 400, 0, -900)])],
            flushed_lsn: vec![],
            latest_backfill_begin: BTreeMap::new(),
            latest_backfill_complete: BTreeMap::new(),
            unresolved_hints: 0,
        };
        let (advanced2, resolved2) = pending.resolve_hints(&progressed2);
        assert_eq!((advanced2, resolved2), (1, 1));
        assert_eq!(
            pending.journals[1].producers[0].last_commit.to_unix().0,
            300
        );
        // M recomputed over journal/B is still 500, so P3 stays at -500.
        assert_eq!(pending.journals[1].producers[0].offset, -500);

        // Empty progressed resolves nothing.
        assert_eq!(pending.resolve_hints(&Frontier::default()), (0, 0));

        // Empty pending resolves nothing.
        assert_eq!(Frontier::default().resolve_hints(&progressed), (0, 0));
    }

    #[test]
    fn test_resolve_hints_flips_open_span_to_cut_floor() {
        // An open-span entry `{L, H, +O}` sharing its journal with a committed
        // sibling at a larger magnitude `M > O` flips its positive begin `+O` to the
        // cut floor `-M` on advancement — whether the hint fully resolves or only
        // partially advances. The negative offset un-gaps the producer on recovery.
        let mut pending = Frontier {
            journals: vec![
                // journal/X: fully resolved (progressed 250s >= hint 200s).
                jf(
                    "journal/X",
                    0,
                    vec![pf(0x01, 50, 200, 300), pf(0x03, 100, 0, -900)],
                ),
                // journal/Y: partially advanced (progressed 250s < hint 300s).
                jf(
                    "journal/Y",
                    0,
                    vec![pf(0x01, 50, 300, 300), pf(0x03, 100, 0, -900)],
                ),
            ],
            flushed_lsn: vec![],
            unresolved_hints: 2,
            ..Default::default()
        };
        let progressed = Frontier {
            journals: vec![
                jf("journal/X", 0, vec![pf(0x01, 250, 0, -700)]),
                jf("journal/Y", 0, vec![pf(0x01, 250, 0, -700)]),
            ],
            flushed_lsn: vec![],
            unresolved_hints: 0,
            ..Default::default()
        };

        let (advanced, resolved) = pending.resolve_hints(&progressed);
        assert_eq!((advanced, resolved), (2, 1));

        // (a) journal/X fully resolved → {H, H, -M}: last_commit capped at the hint
        // (200s), offset flipped from +300 to -900 (M = P3's 900).
        assert_eq!(
            pf_tuple(&pending.journals[0].producers[0]),
            (200, 200, -900)
        );
        // (b) journal/Y partial → {C1, H, -M}: last_commit at progressed (250s), the
        // hint (300s) still unresolved, offset flipped from +300 to -900.
        assert_eq!(
            pf_tuple(&pending.journals[1].producers[0]),
            (250, 300, -900)
        );
        assert_eq!(pending.unresolved_hints, 1, "journal/Y hint still open");
    }

    #[test]
    fn test_resolve_hints_different_bindings() {
        // Pending has journal/X binding=1, progressed has journal/X binding=0.
        // Should NOT match (different bindings).
        let mut pending = Frontier {
            journals: vec![jf("journal/X", 1, vec![pf(0x01, 0, 100, 0)])],
            flushed_lsn: vec![],
            latest_backfill_begin: BTreeMap::new(),
            latest_backfill_complete: BTreeMap::new(),
            unresolved_hints: 1,
        };
        let progressed = Frontier {
            journals: vec![jf("journal/X", 0, vec![pf(0x01, 200, 0, -500)])],
            flushed_lsn: vec![],
            latest_backfill_begin: BTreeMap::new(),
            latest_backfill_complete: BTreeMap::new(),
            unresolved_hints: 0,
        };
        assert_eq!(pending.resolve_hints(&progressed), (0, 0));
    }

    #[test]
    fn test_unresolved_hints_count() {
        let f = Frontier::new(
            vec![
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
            vec![],
        )
        .unwrap();
        assert_eq!(f.unresolved_hints, 2);
        assert_eq!(Frontier::default().unresolved_hints, 0);
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
            latest_backfill_begin: BTreeMap::from([(0, Clock::from_u64(9))]),
            latest_backfill_complete: BTreeMap::from([(0, Clock::from_u64(8))]),
            unresolved_hints: 2,
        };

        let projected = f.project_unresolved_hints();

        // The projection preserves the input's per-binding clocks verbatim — the
        // leader controls what it seeds onto the resume frontier (see startup.rs).
        assert_eq!(projected.latest_backfill_begin, f.latest_backfill_begin);
        assert_eq!(
            projected.latest_backfill_complete,
            f.latest_backfill_complete
        );

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
            latest_backfill_begin: BTreeMap::new(),
            latest_backfill_complete: BTreeMap::new(),
            unresolved_hints: 0,
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

    #[test]
    fn test_partition_bindings() {
        let f = Frontier {
            journals: vec![
                jf("journal/A", 0, vec![pf(0x01, 50, 200, -100)]), // unresolved
                jf("journal/A", 1, vec![pf(0x01, 100, 0, -100)]),
                jf("journal/B", 0, vec![pf(0x03, 100, 0, -200)]),
                jf("journal/C", 2, vec![pf(0x05, 0, 300, 0)]), // unresolved
            ],
            flushed_lsn: vec![Lsn::from_u64(10)],
            latest_backfill_begin: BTreeMap::from([(0, Clock::from_u64(9))]),
            latest_backfill_complete: BTreeMap::from([(1, Clock::from_u64(8))]),
            unresolved_hints: 2,
        };

        // Bindings 0 and 2 are of part 0, and binding 1 of part 1.
        let parts = f.partition_bindings(2, |binding| (binding == 1) as usize);

        insta::assert_debug_snapshot!(parts.iter().map(|part| (
            part.journals.iter().map(|j| (&*j.journal, j.binding)).collect::<Vec<_>>(),
            &part.flushed_lsn,
            &part.latest_backfill_begin,
            &part.latest_backfill_complete,
            part.unresolved_hints,
        )).collect::<Vec<_>>(), @r#"
        [
            (
                [
                    (
                        "journal/A",
                        0,
                    ),
                    (
                        "journal/B",
                        0,
                    ),
                    (
                        "journal/C",
                        2,
                    ),
                ],
                [
                    0/10,
                ],
                {
                    0: Clock(0s 36ns),
                },
                {},
                2,
            ),
            (
                [
                    (
                        "journal/A",
                        1,
                    ),
                ],
                [
                    0/10,
                ],
                {},
                {
                    1: Clock(0s 32ns),
                },
                0,
            ),
        ]
        "#);
    }

    #[test]
    fn test_gap_floor_discharges_unreachable_hints() {
        let floor = 500_000;
        let below = floor - 48 * 3600;

        let mut f = Frontier {
            journals: vec![
                // Below the floor: discharged, and dropped for want of a
                // commit. Its journal is retained, carrying the floor.
                jf("journal/A", 0, vec![pf(0x09, 0, below, 0)]),
                // Just below: discharged. The producer keeps its real commit;
                // only the hint is cleared.
                jf("journal/B", 0, vec![pf(0x09, 10_000, floor - 1, -500)]),
                // At the floor: retained, until some other authority covers it.
                jf("journal/C", 0, vec![pf(0x09, 10_000, floor, -500)]),
                // A floor is of its journal alone: journal/D has none, though
                // binding 0's other journals do.
                jf("journal/D", 0, vec![pf(0x09, 10_000, below, -500)]),
                // Nor does journal/A under binding 1.
                jf("journal/A", 1, vec![pf(0x09, 10_000, below, -500)]),
            ],
            unresolved_hints: 5,
            ..Default::default()
        };
        f.journals
            .sort_by(|a, b| (&a.journal, a.binding).cmp(&(&b.journal, b.binding)));

        let cumulative = Frontier::new(
            vec![
                with_floors(jf("journal/A", 0, vec![]), 0, floor),
                with_floors(jf("journal/B", 0, vec![]), 0, floor),
                with_floors(jf("journal/C", 0, vec![]), 0, floor),
            ],
            vec![],
        )
        .unwrap();

        assert_eq!(f.prune_hints(&cumulative), (0, 0, 2));
        assert_eq!(f.unresolved_hints, 3);

        let readout: Vec<_> = f
            .journals
            .iter()
            .map(|jf| {
                (
                    &*jf.journal,
                    jf.binding,
                    jf.producers.iter().map(pf_tuple).collect::<Vec<_>>(),
                    jf.gap_floor.to_unix().0,
                )
            })
            .collect();
        // Each journal of a discharged hint carries the floor which discharged it.
        assert_eq!(
            readout,
            vec![
                ("journal/A", 0, vec![], floor),
                ("journal/A", 1, vec![(10_000, below, -500)], 0),
                ("journal/B", 0, vec![(10_000, 0, -500)], floor),
                ("journal/C", 0, vec![(10_000, floor, -500)], 0),
                ("journal/D", 0, vec![(10_000, below, -500)], 0),
            ],
        );
    }

    #[test]
    fn test_gap_floor_accounts_hints_but_never_commits() {
        let floor = 500_000;
        let cumulative = Frontier::new(
            vec![with_floors(jf("journal/A", 0, vec![]), 0, floor)],
            vec![],
        )
        .unwrap();

        let judge = |delta: Vec<JournalFrontier>| {
            let delta = Frontier::new(delta, vec![]).unwrap();
            Frontier::default()
                .first_unaccounted(&delta, &cumulative)
                .map(|u| match u.kind {
                    UnaccountedKind::Commit => "commit",
                    UnaccountedKind::Hint => "hint",
                })
        };

        // A hint the floor covers is accounted, so it can neither freeze the
        // ratchet nor enter a transactional boundary.
        assert_eq!(
            judge(vec![jf("journal/A", 0, vec![pf(0x09, 0, floor - 1, 0)])]),
            None
        );

        // A read-derived commit below the floor is NOT accounted: a byte gap is
        // evidence that content is gone, never that a producer commit happened.
        assert_eq!(
            judge(vec![jf("journal/A", 0, vec![pf(0x09, floor - 1, 0, -700)])]),
            Some("commit"),
        );
    }

    #[test]
    fn test_frontier_encode_decode_round_trip() {
        let mut original = Frontier::new(
            vec![
                jf("journal/A", 0, vec![pf(0x01, 100, 0, -500)]),
                jf("journal/A", 1, vec![pf(0x03, 200, 0, -800)]),
                jf("journal/B", 0, vec![pf(0x05, 300, 400, 42)]),
            ],
            vec![100, 200, 300],
        )
        .unwrap();
        original.journals[1].gap_floor = Clock::from_u64(700);
        original.journals[1].prune_floor = Clock::from_u64(900);

        let proto = original.encode();
        assert_eq!(proto.journals.len(), 3);
        assert_eq!(proto.flushed_lsn, vec![100, 200, 300]);

        let reassembled = Frontier::decode(proto).unwrap();
        assert_eq!(reassembled.journals.len(), original.journals.len());
        for (a, b) in reassembled.journals.iter().zip(original.journals.iter()) {
            assert_eq!(&*a.journal, &*b.journal);
            assert_eq!(a.binding, b.binding);
            assert_eq!(a.producers.len(), b.producers.len());
            assert_eq!(a.gap_floor, b.gap_floor);
            // `prune_floor` is session-internal, and never encoded.
            assert_eq!(a.prune_floor, Clock::zero());
        }
        assert_eq!(reassembled.flushed_lsn, original.flushed_lsn);
    }

    #[test]
    fn test_frontier_decode_validates() {
        // An out-of-order journals proto should fail to decode (validation).
        let proto = JournalFrontier::encode(&[
            jf("journal/B", 0, vec![pf(0x01, 1, 0, -1)]),
            jf("journal/A", 0, vec![pf(0x01, 1, 0, -1)]),
        ]);
        let err = Frontier::decode(proto).unwrap_err();
        assert!(format!("{err}").contains("not ordered"));
    }

    #[test]
    fn test_first_unaccounted() {
        // Base: an active pending checkpoint. journal/B sits between the two
        // journals the deltas below name, and no delta reports it.
        let base = Frontier {
            journals: vec![
                jf(
                    "journal/A",
                    0,
                    vec![pf(0x01, 50, 200, -100), pf(0x05, 300, 0, -100)],
                ),
                jf("journal/B", 0, vec![pf(0x01, 900, 0, -100)]),
                jf("journal/C", 1, vec![pf(0x03, 10, 400, -100)]),
            ],
            flushed_lsn: vec![],
            unresolved_hints: 2,
            ..Default::default()
        };
        // The session has promoted producer 0x07 at 600s on journal/C under
        // binding 1, and at 900s on journal/D. No floors are in play.
        let cumulative = Frontier::new(
            vec![
                jf("journal/C", 1, vec![pf(0x07, 600, 0, -100)]),
                jf("journal/D", 1, vec![pf(0x07, 900, 0, -100)]),
            ],
            vec![],
        )
        .unwrap();
        let judge = |delta: Vec<JournalFrontier>| {
            let delta = Frontier::new(delta, vec![]).unwrap();
            base.first_unaccounted(&delta, &cumulative).map(|u| {
                (
                    u.journal.to_string(),
                    match u.kind {
                        UnaccountedKind::Commit => "commit",
                        UnaccountedKind::Hint => "hint",
                    },
                    u.clock.to_unix().0,
                    u.ceiling.to_unix().0,
                )
            })
        };

        // Accounted: at the hint (journal/A P1), at a durable last_commit
        // (journal/A P5), and at a promoted commit for a producer the base
        // never names (journal/C P7). Hints are also accounted when the pending
        // frontier or promoted commits already cover them.
        assert_eq!(
            judge(vec![
                jf(
                    "journal/A",
                    0,
                    vec![pf(0x01, 200, 150, -700), pf(0x05, 300, 0, -700)],
                ),
                jf(
                    "journal/C",
                    1,
                    vec![pf(0x03, 0, 400, 0), pf(0x07, 600, 600, -700)],
                ),
            ]),
            None,
        );

        // Unaccounted: one clock tick above the hint.
        assert_eq!(
            judge(vec![jf("journal/A", 0, vec![pf(0x01, 201, 0, -700)])]),
            Some(("journal/A".to_string(), "commit", 201, 200)),
        );
        // Unaccounted: a producer named by neither the base nor the cumulative
        // Frontier, so its ceiling is zero. Reported in (journal, binding,
        // producer) order, after the accounted journal/A entry.
        assert_eq!(
            judge(vec![
                jf("journal/A", 0, vec![pf(0x01, 200, 0, -700)]),
                jf("journal/C", 1, vec![pf(0x09, 5, 0, -700)]),
            ]),
            Some(("journal/C".to_string(), "commit", 5, 0)),
        );
        // A promoted commit vouches for its own journal alone: 0x07's commits
        // on journal/C and journal/D say nothing of journal/A ...
        assert_eq!(
            judge(vec![jf("journal/A", 0, vec![pf(0x07, 600, 0, -700)])]),
            Some(("journal/A".to_string(), "commit", 600, 0)),
        );
        // ... and its 900s commit on journal/D doesn't cover a 700s hint onto
        // journal/C, which has promoted only 600s.
        assert_eq!(
            judge(vec![jf("journal/C", 1, vec![pf(0x07, 0, 700, 0)])]),
            Some(("journal/C".to_string(), "hint", 700, 600)),
        );
        // Binding is part of the key: journal/C under binding 0 does not match
        // the base's journal/C under binding 1.
        assert_eq!(
            judge(vec![jf("journal/C", 0, vec![pf(0x03, 10, 0, -700)])]),
            Some(("journal/C".to_string(), "commit", 10, 0)),
        );
        // A novel causal hint also makes the whole delta unaccounted, including a
        // hint-only producer which has no read-derived commit of its own.
        assert_eq!(
            judge(vec![jf("journal/Z", 0, vec![pf(0x09, 0, 999, 0)])]),
            Some(("journal/Z".to_string(), "hint", 999, 0)),
        );
        assert_eq!(
            judge(vec![jf("journal/A", 0, vec![pf(0x01, 200, 201, -700)])]),
            Some(("journal/A".to_string(), "hint", 201, 200)),
        );
        // An empty delta is trivially accounted.
        assert_eq!(judge(vec![]), None);
    }

    #[test]
    fn test_first_unaccounted_prune_floor() {
        // journal/Z under binding 0 has forgotten producers through `stale`,
        // and retains producer 0x03's old commit. Binding 1 has no entry.
        let leader = HORIZON_SECS + 100_000;
        let stale = leader - HORIZON_SECS;
        let live = leader - 22_800;

        let base = Frontier {
            journals: vec![jf("journal/A", 0, vec![pf(0x01, 250_000, 260_000, -100)])],
            flushed_lsn: vec![],
            unresolved_hints: 1,
            ..Default::default()
        };
        let cumulative = Frontier::new(
            vec![with_floors(
                jf(
                    "journal/Z",
                    0,
                    vec![pf(0x01, leader, 0, -900), pf(0x03, 1_000, 0, -100)],
                ),
                stale,
                0,
            )],
            vec![],
        )
        .unwrap();
        let judge = |delta: Vec<JournalFrontier>| {
            let delta = Frontier::new(delta, vec![]).unwrap();
            base.first_unaccounted(&delta, &cumulative).map(|u| {
                (
                    match u.kind {
                        UnaccountedKind::Commit => "commit",
                        UnaccountedKind::Hint => "hint",
                    },
                    u.clock.to_unix().0,
                    u.ceiling.to_unix().0,
                )
            })
        };

        // A hint naming a forgotten producer — the pruned-producer case — is
        // accounted by the prune floor alone, so the ratchet does not freeze
        // and the hint never enters the boundary.
        assert_eq!(
            judge(vec![jf("journal/Z", 0, vec![pf(0x09, 0, stale, 0)])]),
            None
        );
        // An ancient commit of the same producer is likewise accounted.
        assert_eq!(
            judge(vec![jf("journal/Z", 0, vec![pf(0x09, stale, 0, -700)])]),
            None,
        );
        // A novel producer's open span is not: its OBSERVED_COMMIT_FLOOR is
        // not a forgotten commit.
        assert!(matches!(
            judge(vec![jf(
                "journal/Z",
                0,
                vec![pf_raw(0x09, OBSERVED_COMMIT_FLOOR, 0, 700)]
            )]),
            Some(("commit", _, 0)),
        ));
        // The floor applies to commit and hint independently: this entry's
        // commit is covered (accounted) while its hint is above it (not).
        assert_eq!(
            judge(vec![jf("journal/Z", 0, vec![pf(0x09, stale, live, 0)])]),
            Some(("hint", live, 0)),
        );
        // The floor covers only forgotten producers: 0x03 is retained, so its
        // unread commit below the floor is not accounted.
        assert_eq!(
            judge(vec![jf("journal/Z", 0, vec![pf(0x03, 0, stale, 0)])]),
            Some(("hint", stale, 1_000)),
        );
        // Binding 1's journal/Z has no entry, so the very same clock is
        // unaccounted there.
        assert_eq!(
            judge(vec![jf("journal/Z", 1, vec![pf(0x09, 0, stale, 0)])]),
            Some(("hint", stale, 0)),
        );
    }

    // Gazette's V1 `message.Sequencer` prunes a producer whose newest raw clock
    // is below its journal's `LastAck - ((24h / 100ns) << 4)`. The largest such
    // clock must be covered by the prune floor seeded from that `LastAck`.
    #[test]
    fn test_seeded_prune_floor_covers_gazette_prune() {
        let last_ack = from_secs(HORIZON_SECS + 100_000).as_u64() | 0xf;
        let dropped = last_ack - ((24 * 60 * 60 * 10_000_000) << 4) - 1;

        let mut cumulative = Frontier::new(
            vec![jf(
                "journal/A",
                0,
                vec![ProducerFrontier {
                    last_commit: Clock::from_u64(last_ack),
                    ..pf(0x01, 0, 0, -100)
                }],
            )],
            vec![],
        )
        .unwrap();
        cumulative.seed_prune_floors();

        assert_eq!(
            cumulative.journals[0].accounts_for(
                crate::testing::producer(0x09),
                Clock::from_u64(dropped),
                false
            ),
            Some(Accounted::PruneFloor),
        );
        // A seed never lowers a floor, and an entry with no commit seeds none.
        let mut entry = Frontier::new(
            vec![with_floors(
                jf("journal/A", 0, vec![pf(0x01, 0, 50, 0)]),
                40,
                0,
            )],
            vec![],
        )
        .unwrap();
        entry.seed_prune_floors();
        assert_eq!(entry.journals[0].prune_floor, from_secs(40));
    }

    #[test]
    fn test_prune_stale_producers() {
        let leader = HORIZON_SECS + 1_000_000;

        let mut entry = jf(
            "j/old",
            0,
            vec![
                pf(0x01, leader, 0, -500),         // Newest commit -> retain.
                pf(0x02, 1_000, 0, -900),          // Ancient, but at the cut floor -> retain.
                pf(0x03, 1_000, 0, -100),          // Ancient and closed -> prune.
                pf(0x05, 900_000, 0, -200),        // Ancient and closed -> prune.
                pf(0x07, 1_000, 0, 300),           // Ancient, but open -> retain.
                pf(0x09, leader - 1_000, 0, -100), // Recent -> retain.
                pf(0x0b, 1_000, 2_000, -100),      // Unresolved hint -> retain.
                pf(0x0d, 1_000, 0, -100),          // Protected -> retain.
                pf(0x0f, 0, 0, 400),               // Never committed, open -> retain.
            ],
        );
        entry.prune_floor = from_secs(500);

        let mut pruned = Vec::new();
        entry.prune_stale_producers(
            |producer| producer == crate::testing::producer(0x0d),
            |p| pruned.push(pf_tuple(p)),
        );

        assert_eq!(pruned, vec![(1_000, 0, -100), (900_000, 0, -200)]);
        assert_eq!(
            entry.producers.iter().map(pf_tuple).collect::<Vec<_>>(),
            vec![
                (leader, 0, -500),
                (1_000, 0, -900),
                (1_000, 0, 300),
                (leader - 1_000, 0, -100),
                (1_000, 2_000, -100),
                (1_000, 0, -100),
                (0, 0, 400),
            ],
        );
        // Raised to the largest pruned `last_commit`, from its prior value.
        assert_eq!(entry.prune_floor, from_secs(900_000));

        // Pruning again is a no-op, and never lowers the floor.
        entry.prune_stale_producers(
            |producer| producer == crate::testing::producer(0x0d),
            |_| panic!("nothing is stale"),
        );
        assert_eq!(entry.prune_floor, from_secs(900_000));
    }

    #[test]
    fn test_prune_hints() {
        let leader = HORIZON_SECS + 100_000;
        let stale = leader - HORIZON_SECS;

        let mut f = Frontier {
            journals: vec![
                // A forgotten producer, hint-only: cleared by the prune floor,
                // then dropped for want of a commit, emptying its journal.
                jf("journal/A", 0, vec![pf(0x09, 0, stale, 0)]),
                // Same, but read progress keeps the producer (and journal) alive.
                jf("journal/B", 0, vec![pf(0x09, 10_000, stale, -500)]),
                // One second above the prune floor: retained.
                jf("journal/C", 0, vec![pf(0x09, 10_000, stale + 1, -500)]),
                // A journal the cumulative Frontier knows nothing of: retained.
                jf("journal/D", 0, vec![pf(0x09, 10_000, stale, -500)]),
                // Cleared by its journal's promoted commit.
                jf("journal/E", 1, vec![pf(0x01, 1_000, 5_000, -500)]),
                // A commit vouches for its own journal alone: P1's on journal/E
                // says nothing of journal/F, which hasn't promoted it.
                jf("journal/F", 1, vec![pf(0x01, 1_000, 5_000, -500)]),
                // The prune floor covers only forgotten producers: P9 is
                // retained on journal/G, short of its hint.
                jf("journal/G", 0, vec![pf(0x09, 0, 2_000, 0)]),
            ],
            flushed_lsn: vec![],
            unresolved_hints: 7,
            ..Default::default()
        };
        let cumulative = Frontier::new(
            vec![
                with_floors(
                    jf("journal/A", 0, vec![pf(0x01, leader, 0, -900)]),
                    stale,
                    0,
                ),
                with_floors(
                    jf("journal/B", 0, vec![pf(0x01, leader, 0, -900)]),
                    stale,
                    0,
                ),
                with_floors(
                    jf("journal/C", 0, vec![pf(0x01, leader, 0, -900)]),
                    stale,
                    0,
                ),
                jf("journal/E", 1, vec![pf(0x01, 5_000, 0, -900)]),
                jf("journal/F", 1, vec![pf(0x03, 9_000, 0, -900)]),
                with_floors(
                    jf(
                        "journal/G",
                        0,
                        vec![pf(0x01, leader, 0, -900), pf(0x09, 1_000, 0, -100)],
                    ),
                    stale,
                    0,
                ),
            ],
            vec![],
        )
        .unwrap();

        assert_eq!(f.prune_hints(&cumulative), (1, 2, 0));
        assert_eq!(f.unresolved_hints, 4);

        let readout: Vec<_> = f
            .journals
            .iter()
            .map(|jf| {
                (
                    &*jf.journal,
                    jf.binding,
                    jf.producers.iter().map(pf_tuple).collect::<Vec<_>>(),
                )
            })
            .collect();
        assert_eq!(
            readout,
            vec![
                ("journal/B", 0, vec![(10_000, 0, -500)]),
                ("journal/C", 0, vec![(10_000, stale + 1, -500)]),
                ("journal/D", 0, vec![(10_000, stale, -500)]),
                ("journal/E", 1, vec![(1_000, 0, -500)]),
                ("journal/F", 1, vec![(1_000, 5_000, -500)]),
                ("journal/G", 0, vec![(0, 2_000, 0)]),
            ],
        );
    }

    #[test]
    fn test_frontier_prune_stale_producers() {
        let leader = HORIZON_SECS + 100_000;
        let stale = |journal: &str| {
            jf(
                journal,
                0,
                vec![pf(0x01, leader, 0, -900), pf(0x03, 1_000, 0, -100)],
            )
        };
        let mut cumulative = Frontier::new(
            vec![stale("journal/A"), stale("journal/B"), stale("journal/C")],
            vec![],
        )
        .unwrap();

        // Only journals of `touched` are pruned. journal/Z is unknown, and skipped.
        let touched = Frontier::new(
            vec![jf("journal/B", 0, vec![]), jf("journal/Z", 0, vec![])],
            vec![],
        )
        .unwrap();
        assert_eq!(cumulative.prune_stale_producers(Some(&touched)), 1);

        let readout: Vec<_> = cumulative
            .journals
            .iter()
            .map(|jf| (&*jf.journal, jf.producers.len(), jf.prune_floor.to_unix().0))
            .collect();
        assert_eq!(
            readout,
            vec![
                ("journal/A", 2, 0),
                ("journal/B", 1, 1_000),
                ("journal/C", 2, 0)
            ],
        );

        // With no `touched`, every journal is pruned.
        assert_eq!(cumulative.prune_stale_producers(None), 2);
        assert!(cumulative.journals.iter().all(|jf| jf.producers.len() == 1));
    }

    #[test]
    fn test_describe_unresolved_caps_and_elides() {
        // More unresolved journals than the render cap: the first
        // DESCRIBE_UNRESOLVED_MAX_LINES render and the remainder is elided.
        let extra = 5;
        let count = Frontier::DESCRIBE_UNRESOLVED_MAX_LINES + extra;
        let journals: Vec<_> = (0..count)
            .map(|i| jf(&format!("journal/{i:02}"), 0, vec![pf(0x01, 10, 20, -100)]))
            .collect();
        let f = Frontier {
            journals,
            flushed_lsn: vec![],
            unresolved_hints: count,
            ..Default::default()
        };

        let desc = f.describe_unresolved();
        let rendered = desc.matches("journal \"").count();

        assert_eq!(rendered, Frontier::DESCRIBE_UNRESOLVED_MAX_LINES);
        assert!(
            desc.ends_with(&format!("… and {extra} more unresolved hint(s)")),
            "{desc}",
        );
    }

    #[test]
    fn test_describe_unresolved_skips_resolved_and_omits_elision() {
        // Under the cap and with a resolved producer mixed in: only unresolved
        // producers render, and there is no elision line.
        let f = Frontier {
            journals: vec![
                jf("journal/A", 0, vec![pf(0x01, 10, 20, -100)]), // unresolved
                jf("journal/B", 1, vec![pf(0x03, 30, 0, -200)]),  // no hint → skipped
            ],
            flushed_lsn: vec![],
            unresolved_hints: 1,
            ..Default::default()
        };

        let desc = f.describe_unresolved();
        assert!(desc.contains("journal \"journal/A\""), "{desc}");
        assert!(!desc.contains("journal/B"), "{desc}");
        assert!(!desc.contains("more unresolved"), "{desc}");
    }
}
