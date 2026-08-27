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
    /// represents how far into the journal we've read.
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
        // (F == O) — and the span begin is the strictly newer state.
        let offset = if self.offset.abs() != other.offset.abs() {
            if self.offset.abs() > other.offset.abs() {
                self.offset
            } else {
                other.offset
            }
        } else {
            self.offset.max(other.offset)
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
/// what makes a Frontier a resumption point at all. Below `M`, a re-read
/// document sequences as a duplicate of a covered close; a re-read rollback
/// re-discards; and an open span is recovered from its `+begin` by gap-replay,
/// triggered by its producer's next activity (see `slice/replay.rs`).
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
/// One deliberate, bounded deviation exists: shard recovery prunes ancient,
/// closed, far-behind producers, forgetting their coverage. See
/// `runtime_next::shard::recovery::prune_committed_frontier`, which documents
/// why the resulting double-processing risk is bounded and accepted.
///
/// Forgetting a producer costs liveness as well as coverage, so the prune also
/// carries a liveness contract. A pruned producer can be the target of a future
/// backfill's causal hints, which nothing forward-looking will ever resolve —
/// its journal is read at the head and the producer is retired. What keeps such
/// a hint dischargeable is [`Completed`], which deems any clock at least
/// [`crate::PRODUCER_STALENESS_HORIZON`] older than a binding's promoted
/// progress to be completed for that binding. Pruning uses the same constant for
/// its own clock condition, which is exactly what makes every pruned producer's
/// hints clearable.
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
    /// Gap floor of each binding, keyed by binding index. See
    /// [`Completed::is_gap_stale`].
    pub binding_gap_floors: BTreeMap<u16, Clock>,
    /// Count of `ProducerFrontier` entries with `hinted_commit > last_commit`.
    /// A Frontier with a non-zero count is "partial": readable for processing
    /// (e.g. log scanning), but NOT a transactional boundary.
    pub unresolved_hints: usize,
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
    /// or its cohort's completed clocks can account for. The staleness-horizon
    /// authority is per-binding rather than per-producer and so isn't folded in
    /// here; an `Unaccounted` exists only because it did not apply either.
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
            binding_gap_floors: BTreeMap::new(),
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
    /// Both inputs may contain non-unique keys, which are reduced to single entries.
    pub fn reduce(self, other: Self) -> Self {
        let flushed_lsn = Self::merge_flushed_lsn(self.flushed_lsn, other.flushed_lsn);
        let latest_backfill_begin =
            Self::merge_binding_clocks(self.latest_backfill_begin, other.latest_backfill_begin);
        let latest_backfill_complete = Self::merge_binding_clocks(
            self.latest_backfill_complete,
            other.latest_backfill_complete,
        );
        let binding_gap_floors =
            Self::merge_binding_clocks(self.binding_gap_floors, other.binding_gap_floors);

        if self.journals.is_empty() {
            return Self {
                flushed_lsn,
                latest_backfill_begin,
                latest_backfill_complete,
                binding_gap_floors,
                ..other
            };
        } else if other.journals.is_empty() {
            return Self {
                flushed_lsn,
                latest_backfill_begin,
                latest_backfill_complete,
                binding_gap_floors,
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

        // Max-merging can either create or eliminate unresolved-ness, so recompute.
        let unresolved_hints = count_unresolved_hints(&merged);
        Self {
            journals: merged,
            flushed_lsn,
            latest_backfill_begin,
            latest_backfill_complete,
            binding_gap_floors,
            unresolved_hints,
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

    /// Clear causal hints of `self` which [`Completed`] accounts for: either the
    /// journal's cohort has completed the referenced producer commit, or the
    /// clock trails the binding's promoted progress by at least
    /// [`crate::PRODUCER_STALENESS_HORIZON`]. Such a hint is stale — it's held by
    /// a journal reading from behind the cohort's frontier, such as a re-enabled
    /// binding, which may never observe the ACK that would resolve it on the
    /// forward path. The horizon rule additionally discharges hints whose
    /// producer the runtime has durably pruned and so forgotten entirely. The
    /// binding's gap floor ([`Completed::is_gap_stale`]) discharges hints which
    /// trail it.
    ///
    /// A producer left with neither a hint nor a commit is dropped, as is a
    /// journal left with no producers. `unresolved_hints` is decremented for
    /// each cleared hint.
    ///
    /// Returns `(cleared_by_clock, cleared_by_horizon, cleared_by_gap_floor)`.
    pub fn prune_hints(&mut self, completed: &Completed) -> (usize, usize, usize) {
        let mut by_clock = 0usize;
        let mut by_horizon = 0usize;
        let mut by_gap_floor = 0usize;

        self.journals.retain_mut(|jf| {
            let binding = jf.binding;

            jf.producers.retain_mut(|pf| {
                if pf.hinted_commit <= pf.last_commit {
                    return true; // Hint is already resolved.
                }
                if pf.hinted_commit <= completed.clock(binding, pf.producer) {
                    by_clock += 1;
                } else if completed.is_horizon_stale(binding, pf.hinted_commit) {
                    by_horizon += 1;
                } else if completed.is_gap_stale(binding, pf.hinted_commit) {
                    by_gap_floor += 1;
                } else {
                    return true; // Hint is at the frontier.
                }

                // Hint is stale. Retain only if we also saw a commit.
                pf.hinted_commit = Clock::zero();
                pf.last_commit != Clock::zero()
            });

            !jf.producers.is_empty()
        });

        self.unresolved_hints -= by_clock + by_horizon + by_gap_floor;
        (by_clock, by_horizon, by_gap_floor)
    }

    /// Judge whether `delta` is *accounted* for by `self` — an unresolved pending
    /// checkpoint — together with the [`Completed`] accounting of its bindings.
    ///
    /// A delta is accounted iff every producer entry of every journal reports
    /// clocks — both read-derived commits and causal hints — which some
    /// accounting authority covers. Two are per-producer **ceilings**: the
    /// maximum of that producer's `last_commit` and `hinted_commit` in `self`,
    /// and the clock its cohort has completed. Two are per-binding: the
    /// staleness horizon, and, for hints only, the gap floor. A producer named
    /// by neither of the first two has a zero ceiling, so a clock reported for it
    /// is unaccounted unless a per-binding authority covers it.
    ///
    /// Returns the first unaccounted entry in `(journal, binding, producer)`
    /// order, or None if `delta` is accounted.
    ///
    /// # Soundness
    ///
    /// Four authorities account for a clock, and an accounted delta adds
    /// nothing beyond them:
    ///
    /// 1. The pending checkpoint's own clocks and hints. A commit at-or-below
    ///    `last_commit` is already covered; a clock at-or-below `hinted_commit`
    ///    is within the causal extent the pending boundary already commits to.
    /// 2. Commits the producer's cohort has completed. Such a commit reached a
    ///    fully-resolved checkpoint, so its cross-journal extent is confirmed
    ///    and re-reading it yields duplicates of covered closes.
    /// 3. Clocks at least [`crate::PRODUCER_STALENESS_HORIZON`] older than the
    ///    binding's promoted progress. This is the dual of the runtime's
    ///    committed-frontier prune, which durably *forgets* a producer's
    ///    coverage under the same horizon: the runtime is willing to accept the
    ///    bounded double-processing of content that ancient, so the session is
    ///    equally willing to account for it. Soundness rests on the horizon
    ///    dwarfing intra-cohort source-clock skew, which is bounded by the Slice
    ///    read heap and Log append leveling — both order by priority and then by
    ///    adjusted clock, so a cohort's journals advance in near-lockstep and
    ///    never drift 48 hours apart.
    /// 4. Hints, never commits, below the binding's gap floor. A read-start
    ///    byte gap left such a hint's ACK unreachable, so holding the boundary
    ///    for it gains nothing and may stall forever. A byte gap says nothing
    ///    about whether a producer committed, so a commit past its ceiling
    ///    still freezes the ratchet.
    ///
    /// Open spans ride along as `+begin`s, as clause 2 of the Frontier
    /// invariant prescribes. So the delta's entries jointly satisfy clauses 1
    /// and 2 at the delta's own cut, and reducing them in preserves the invariant
    /// with the true read offsets in place of the conservative `-M` bump. The
    /// first commit or hint no authority can account for freezes the
    /// ratchet. For a commit, rejecting its whole delta leaves the cut at the
    /// prior accounted delta and therefore strictly below that commit's offset;
    /// for a hint, rejection keeps its novel causal extent out of the pending
    /// boundary.
    pub fn first_unaccounted(
        &self,
        delta: &Frontier,
        completed: &Completed,
    ) -> Option<Unaccounted> {
        for delta_jf in &delta.journals {
            // The pending checkpoint may not carry this journal at all:
            // a read can report journals it never had.
            let base_producers = self
                .find_journal(&delta_jf.journal, delta_jf.binding)
                .map_or(&[][..], |index| self.journals[index].producers.as_slice());

            for delta_p in &delta_jf.producers {
                let mut ceiling = completed.clock(delta_jf.binding, delta_p.producer);

                if let Ok(index) =
                    base_producers.binary_search_by(|base_p| base_p.producer.cmp(&delta_p.producer))
                {
                    let base_p = &base_producers[index];
                    ceiling = ceiling.max(base_p.last_commit).max(base_p.hinted_commit);
                }

                let (kind, clock) = if delta_p.last_commit > ceiling
                    && !completed.is_horizon_stale(delta_jf.binding, delta_p.last_commit)
                {
                    (UnaccountedKind::Commit, delta_p.last_commit)
                } else if delta_p.hinted_commit > ceiling
                    && !completed.is_horizon_stale(delta_jf.binding, delta_p.hinted_commit)
                    && !completed.is_gap_stale(delta_jf.binding, delta_p.hinted_commit)
                {
                    (UnaccountedKind::Hint, delta_p.hinted_commit)
                } else {
                    continue;
                };
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
        proto.binding_gap_floors = self
            .binding_gap_floors
            .iter()
            .map(|(binding, clock)| shuffle::frontier::BindingGapFloor {
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

        let binding_gap_floors: BTreeMap<u16, Clock> =
            std::mem::take(&mut proto.binding_gap_floors)
                .into_iter()
                .map(|e| (e.binding as u16, Clock::from_u64(e.clock)))
                .collect();

        let journals: Vec<JournalFrontier> = JournalFrontier::decode(proto).collect();
        let mut frontier = Self::new(journals, flushed_lsn)?;
        frontier.latest_backfill_begin = latest_backfill_begin;
        frontier.latest_backfill_complete = latest_backfill_complete;
        frontier.binding_gap_floors = binding_gap_floors;
        Ok(frontier)
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
                        bytes_read_delta: 0,
                        bytes_behind_delta: 0,
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
            binding_gap_floors: self.binding_gap_floors.clone(),
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

/// Completed accounting: the clocks a binding no longer needs to see resolved.
/// Consumed by [`Frontier::prune_hints`] and [`Frontier::first_unaccounted`].
///
/// It bundles three authorities over the single question "is this clock already
/// accounted for?":
///
/// 1. `clocks`, the per-cohort ledger of producer commits which reached a
///    fully-resolved checkpoint. A clock at-or-below a producer's entry is
///    stale: that commit's cross-journal extent is already confirmed.
/// 2. `binding_max`, the per-binding maximum of promoted read progress. Any
///    clock trailing it by at least [`crate::PRODUCER_STALENESS_HORIZON`] is
///    deemed completed for that binding, whatever producer it names. This is
///    the dual of the runtime's committed-frontier prune, which durably forgets
///    producers under the same horizon; without it, a hint targeting a
///    forgotten producer could never be discharged at all.
/// 3. `binding_gap_floor`, the per-binding clock below which a read-start byte
///    gap left causal hints unreachable. Unlike the other two it discharges
///    hints only.
///
/// The first two are gated on promotion, which is what makes them authorities on
/// "this commit is done": a frontier reaches `ready` only once its own causal
/// hints have resolved. The gap floor is instead ratcheted from `Progressed`
/// deltas ahead of any promotion — sound because a byte gap is a fact about what
/// a read can reach, not about a transaction's outcome.
#[derive(Debug)]
pub struct Completed {
    /// Per-cohort map from Producer to its highest completed Clock.
    clocks: Vec<crate::ProducerMap<Clock>>,
    /// Per-binding maximum promoted commit Clock.
    binding_max: Vec<Clock>,
    /// Per-binding gap floor (zero = none).
    binding_gap_floor: Vec<Clock>,
    /// Maps binding index → cohort index (from `Binding::cohort`).
    binding_cohorts: Vec<u32>,
}

impl Completed {
    /// Build empty accounting over `binding_cohorts`, sizing the cohort ledger
    /// from the largest cohort index that mapping names.
    pub fn new(binding_cohorts: Vec<u32>) -> Self {
        let num_cohorts = binding_cohorts
            .iter()
            .copied()
            .max()
            .map_or(0, |m| m as usize + 1);

        Self {
            clocks: vec![crate::ProducerMap::default(); num_cohorts],
            binding_max: vec![Clock::zero(); binding_cohorts.len()],
            binding_gap_floor: vec![Clock::zero(); binding_cohorts.len()],
            binding_cohorts,
        }
    }

    /// Number of cohorts this accounting spans, for diagnostics.
    pub fn num_cohorts(&self) -> usize {
        self.clocks.len()
    }

    /// Absorb a promoted `frontier` — one which reached `ready`, or a resume
    /// checkpoint, which is the prior session's final promotion. Each producer's
    /// commit is recorded against its cohort, and each binding's promoted
    /// maximum is raised.
    ///
    /// Hint-only entries (`last_commit` of zero) and [`OBSERVED_COMMIT_FLOOR`]
    /// floors need no special case: under `max` they are dominated by any real
    /// commit, and a binding holding nothing else simply has no horizon yet.
    pub fn update(&mut self, frontier: &Frontier) {
        for jf in &frontier.journals {
            let cohort = self.binding_cohorts[jf.binding as usize] as usize;
            let clocks = &mut self.clocks[cohort];
            let binding_max = &mut self.binding_max[jf.binding as usize];

            for pf in &jf.producers {
                clocks
                    .entry(pf.producer)
                    .and_modify(|c| c.update(pf.last_commit))
                    .or_insert(pf.last_commit);
                binding_max.update(pf.last_commit);
            }
        }
    }

    /// Highest Clock which `binding`'s cohort has completed for `producer`, or
    /// zero if it names none. The first authority of the accounting query; pair
    /// it with [`Self::is_horizon_stale`].
    pub fn clock(&self, binding: u16, producer: Producer) -> Clock {
        let cohort = self.binding_cohorts[binding as usize] as usize;
        self.clocks[cohort]
            .get(&producer)
            .copied()
            .unwrap_or(Clock::zero())
    }

    /// Whether `clock` trails `binding`'s promoted read progress by at least
    /// [`crate::PRODUCER_STALENESS_HORIZON`], deeming it completed for that
    /// binding whatever producer it names. The second authority of the
    /// accounting query.
    pub fn is_horizon_stale(&self, binding: u16, clock: Clock) -> bool {
        Clock::delta(self.binding_max[binding as usize], clock) >= crate::PRODUCER_STALENESS_HORIZON
    }

    pub fn advance_gap_floors(&mut self, floors: &BTreeMap<u16, Clock>) {
        for (binding, clock) in floors {
            let floor = &mut self.binding_gap_floor[*binding as usize];
            *floor = (*floor).max(*clock);
        }
    }

    /// Whether `clock` lies below `binding`'s gap floor, deeming the ACK it
    /// references unreachable. The third authority of the accounting query; a
    /// binding with no floor (zero) discharges nothing.
    pub fn is_gap_stale(&self, binding: u16, clock: Clock) -> bool {
        clock < self.binding_gap_floor[binding as usize]
    }
}

/// Walk a journal list and count producers with `hinted_commit > last_commit`.
fn count_unresolved_hints(journals: &[JournalFrontier]) -> usize {
    journals
        .iter()
        .flat_map(|jf| &jf.producers)
        .filter(|p| p.hinted_commit > p.last_commit)
        .count()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::testing::{jf, jf_with_bytes, pf, pf_tuple};
    use log::Lsn;
    use std::collections::BTreeMap;

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
        ];

        for (a, b, expect) in cases {
            let r = pf(0x01, a.0, a.1, a.2).reduce(pf(0x01, b.0, b.1, b.2));
            assert_eq!(pf_tuple(&r), expect, "reduce({a:?}, {b:?})");
        }
    }

    /// Horizon expressed in the whole seconds which `pf` and `completed` speak.
    const HORIZON_SECS: u64 = crate::PRODUCER_STALENESS_HORIZON.as_secs();

    /// Build a `Completed` over `binding_cohorts`, injecting cohort-completed
    /// clocks as `(cohort, producer_id, seconds)` and per-binding promoted
    /// maxima as `(binding, seconds)`. Both are seeded directly rather than
    /// through `update`, so a test states only the accounting it cares about.
    fn completed(
        binding_cohorts: Vec<u32>,
        clocks: &[(usize, u8, u64)],
        binding_max: &[(usize, u64)],
    ) -> Completed {
        let mut completed = Completed::new(binding_cohorts);

        for &(cohort, id, seconds) in clocks {
            completed.clocks[cohort].insert(crate::testing::producer(id), from_secs(seconds));
        }
        for &(binding, seconds) in binding_max {
            completed.binding_max[binding] = from_secs(seconds);
        }
        completed
    }

    /// `Completed` seeding no authority but `binding`'s gap floor, isolating it.
    fn gap_completed(binding: usize, seconds: u64) -> Completed {
        let mut completed = Completed::new(vec![0u32, 0]);
        completed.binding_gap_floor[binding] = from_secs(seconds);
        completed
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
            binding_gap_floors: BTreeMap::from([
                (0, Clock::from_u64(700)),
                (3, Clock::from_u64(900)),
            ]),
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
            binding_gap_floors: BTreeMap::from([
                (0, Clock::from_u64(500)),
                (3, Clock::from_u64(1_100)),
            ]),
            unresolved_hints: 2,
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
        assert_eq!(
            r.binding_gap_floors,
            BTreeMap::from([(0, Clock::from_u64(700)), (3, Clock::from_u64(1_100))]),
        );

        // Identity: reducing with an empty frontier preserves all fields.
        let f = Frontier {
            journals: vec![jf("j", 0, vec![pf(0x01, 1, 0, -1)])],
            flushed_lsn: vec![Lsn::from_u64(10), Lsn::from_u64(20)],
            latest_backfill_begin: BTreeMap::from([(0, Clock::from_u64(100))]),
            latest_backfill_complete: BTreeMap::from([(0, Clock::from_u64(200))]),
            binding_gap_floors: BTreeMap::from([(0, Clock::from_u64(300))]),
            unresolved_hints: 0,
        };
        let r = f.clone().reduce(Frontier::default());
        assert_eq!(r.journals.len(), 1);
        assert_eq!(r.flushed_lsn, vec![Lsn::from_u64(10), Lsn::from_u64(20)]);
        assert_eq!(r.latest_backfill_begin, f.latest_backfill_begin);
        assert_eq!(r.latest_backfill_complete, f.latest_backfill_complete);
        assert_eq!(r.binding_gap_floors, f.binding_gap_floors);
        let r = Frontier::default().reduce(f);
        assert_eq!(r.journals.len(), 1);
        assert_eq!(r.flushed_lsn, vec![Lsn::from_u64(10), Lsn::from_u64(20)]);
        assert_eq!(r.latest_backfill_begin.get(&0), Some(&Clock::from_u64(100)));
        assert_eq!(
            r.latest_backfill_complete.get(&0),
            Some(&Clock::from_u64(200))
        );
        assert_eq!(r.binding_gap_floors.get(&0), Some(&Clock::from_u64(300)));
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
            binding_gap_floors: BTreeMap::new(),
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
            binding_gap_floors: BTreeMap::new(),
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
            binding_gap_floors: BTreeMap::new(),
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
            binding_gap_floors: BTreeMap::new(),
            unresolved_hints: 1,
        };
        let progressed = Frontier {
            journals: vec![jf("journal/X", 0, vec![pf(0x01, 200, 0, -500)])],
            flushed_lsn: vec![],
            latest_backfill_begin: BTreeMap::new(),
            latest_backfill_complete: BTreeMap::new(),
            binding_gap_floors: BTreeMap::new(),
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
            binding_gap_floors: BTreeMap::from([(0, Clock::from_u64(7))]),
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
        assert_eq!(projected.binding_gap_floors, f.binding_gap_floors);

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
            binding_gap_floors: BTreeMap::new(),
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
    fn test_gap_floor_discharges_unreachable_hints() {
        let floor = 500_000;
        let below = floor - 48 * 3600;

        let mut f = Frontier {
            journals: vec![
                // Below the floor: discharged, and dropped for want of a
                // commit, emptying its journal.
                jf("journal/A", 0, vec![pf(0x09, 0, below, 0)]),
                // Just below: discharged. The producer keeps its real commit;
                // only the hint is cleared.
                jf("journal/B", 0, vec![pf(0x09, 10_000, floor - 1, -500)]),
                // At the floor: retained, until some other authority covers it.
                jf("journal/C", 0, vec![pf(0x09, 10_000, floor, -500)]),
                // Binding 1 has no floor of its own, so nothing applies — not
                // even to a hint far below binding 0's floor.
                jf("journal/D", 1, vec![pf(0x09, 10_000, below, -500)]),
            ],
            unresolved_hints: 4,
            ..Default::default()
        };
        let completed = gap_completed(0, floor);

        assert_eq!(f.prune_hints(&completed), (0, 0, 2));
        assert_eq!(f.unresolved_hints, 2);

        let readout: Vec<_> = f
            .journals
            .iter()
            .map(|jf| {
                (
                    &*jf.journal,
                    jf.producers.iter().map(pf_tuple).collect::<Vec<_>>(),
                )
            })
            .collect();
        assert_eq!(
            readout,
            vec![
                ("journal/B", vec![(10_000, 0, -500)]),
                ("journal/C", vec![(10_000, floor, -500)]),
                ("journal/D", vec![(10_000, below, -500)]),
            ],
        );
    }

    #[test]
    fn test_gap_floor_accounts_hints_but_never_commits() {
        let floor = 500_000;
        let completed = gap_completed(0, floor);

        let judge = |delta: Vec<JournalFrontier>| {
            let delta = Frontier::new(delta, vec![]).unwrap();
            Frontier::default()
                .first_unaccounted(&delta, &completed)
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
        original.binding_gap_floors =
            BTreeMap::from([(0, Clock::from_u64(700)), (3, Clock::from_u64(900))]);

        let proto = original.encode();
        assert_eq!(proto.journals.len(), 3);
        assert_eq!(proto.flushed_lsn, vec![100, 200, 300]);

        let reassembled = Frontier::decode(proto).unwrap();
        assert_eq!(reassembled.journals.len(), original.journals.len());
        for (a, b) in reassembled.journals.iter().zip(original.journals.iter()) {
            assert_eq!(&*a.journal, &*b.journal);
            assert_eq!(a.binding, b.binding);
            assert_eq!(a.producers.len(), b.producers.len());
        }
        assert_eq!(reassembled.flushed_lsn, original.flushed_lsn);
        assert_eq!(reassembled.binding_gap_floors, original.binding_gap_floors);
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
        // Cohort 0 serves binding 0; cohort 1 serves binding 1 and has completed
        // producer 0x07 at 600s. No binding has promoted progress, so the
        // staleness horizon never fires and only the clock ceilings are in play.
        let completed = completed(vec![0u32, 1], &[(1, 0x07, 600)], &[]);
        let judge = |delta: Vec<JournalFrontier>| {
            let delta = Frontier::new(delta, vec![]).unwrap();
            base.first_unaccounted(&delta, &completed).map(|u| {
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
        // (journal/A P5), and at a cohort-completed clock for a producer the base
        // never names (journal/C P7). Hints are also accounted when the pending
        // frontier or completed clocks already cover them.
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
        // Unaccounted: a producer named by neither the base nor its cohort's
        // completed clocks, so its ceiling is zero. Reported in (journal,
        // binding, producer) order, after the accounted journal/A entry.
        assert_eq!(
            judge(vec![
                jf("journal/A", 0, vec![pf(0x01, 200, 0, -700)]),
                jf("journal/C", 1, vec![pf(0x09, 5, 0, -700)]),
            ]),
            Some(("journal/C".to_string(), "commit", 5, 0)),
        );
        // The cohort ledger is per-cohort: 0x07's completion in cohort 1 says
        // nothing about binding 0's cohort.
        assert_eq!(
            judge(vec![jf("journal/A", 0, vec![pf(0x07, 600, 0, -700)])]),
            Some(("journal/A".to_string(), "commit", 600, 0)),
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
    fn test_first_unaccounted_horizon() {
        // Binding 0's promoted progress sits at `leader`; binding 1 (cohort 1)
        // has none, which isolates the horizon to the binding that earned it.
        let leader = HORIZON_SECS + 100_000;
        let stale = leader - HORIZON_SECS; // Exactly at the horizon.
        let live = leader - 22_800; // Well inside it.

        let base = Frontier {
            journals: vec![jf("journal/A", 0, vec![pf(0x01, 250_000, 260_000, -100)])],
            flushed_lsn: vec![],
            unresolved_hints: 1,
            ..Default::default()
        };
        let completed = completed(vec![0u32, 1], &[], &[(0, leader)]);
        let judge = |delta: Vec<JournalFrontier>| {
            let delta = Frontier::new(delta, vec![]).unwrap();
            base.first_unaccounted(&delta, &completed).map(|u| {
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

        // A hint naming a producer the base and its cohort both know nothing of —
        // the pruned-producer case — is accounted by the horizon alone, so the
        // ratchet does not freeze and the hint never enters the boundary.
        assert_eq!(
            judge(vec![jf("journal/Z", 0, vec![pf(0x09, 0, stale, 0)])]),
            None
        );
        // An ancient commit for the same unknown producer is likewise accounted.
        assert_eq!(
            judge(vec![jf("journal/Z", 0, vec![pf(0x09, stale, 0, -700)])]),
            None,
        );
        // The horizon is applied to commit and hint independently: this entry's
        // commit is stale (accounted) while its hint is live (not).
        assert_eq!(
            judge(vec![jf("journal/Z", 0, vec![pf(0x09, stale, live, 0)])]),
            Some(("hint", live, 0)),
        );
        // A live hint for an unknown producer remains unaccounted — the horizon
        // does not weaken ordinary ratchet judgment.
        assert_eq!(
            judge(vec![jf("journal/Z", 0, vec![pf(0x09, 0, live, 0)])]),
            Some(("hint", live, 0)),
        );
        // Binding 1 has no promoted progress, so its sentinel is zero and the
        // very same stale clock is unaccounted there.
        assert_eq!(
            judge(vec![jf("journal/Z", 1, vec![pf(0x09, 0, stale, 0)])]),
            Some(("hint", stale, 0)),
        );
    }

    #[test]
    fn test_prune_hints() {
        // Binding 0's promoted progress sits at `leader`; binding 1 shares its
        // cohort but has no progress of its own, isolating the clock authority.
        let leader = HORIZON_SECS + 100_000;
        let stale = leader - HORIZON_SECS; // Exactly at the horizon.

        let mut f = Frontier {
            journals: vec![
                // Wholly unknown producer, hint-only: horizon-cleared, then
                // dropped for want of a commit, emptying its journal.
                jf("journal/A", 0, vec![pf(0x09, 0, stale, 0)]),
                // Same, but read progress keeps the producer (and journal) alive.
                jf("journal/B", 0, vec![pf(0x09, 10_000, stale, -500)]),
                // One second inside the horizon: retained.
                jf("journal/C", 0, vec![pf(0x09, 10_000, stale + 1, -500)]),
                // Comfortably at the frontier: retained.
                jf("journal/D", 0, vec![pf(0x09, 10_000, leader - 1, -500)]),
                // Cleared by its cohort's completed clock, not the horizon:
                // binding 1 has no sentinel at all.
                jf("journal/E", 1, vec![pf(0x01, 1_000, 5_000, -500)]),
            ],
            flushed_lsn: vec![],
            unresolved_hints: 5,
            ..Default::default()
        };
        let completed = completed(vec![0u32, 0], &[(0, 0x01, 5_000)], &[(0, leader)]);

        assert_eq!(f.prune_hints(&completed), (1, 2, 0));
        assert_eq!(f.unresolved_hints, 2);

        insta::assert_debug_snapshot!(f.journals.iter().map(|jf| {
            (&*jf.journal, jf.binding, jf.producers.iter().map(pf_tuple).collect::<Vec<_>>())
        }).collect::<Vec<_>>(), @r#"
        [
            (
                "journal/B",
                0,
                [
                    (
                        10000,
                        0,
                        -500,
                    ),
                ],
            ),
            (
                "journal/C",
                0,
                [
                    (
                        10000,
                        100001,
                        -500,
                    ),
                ],
            ),
            (
                "journal/D",
                0,
                [
                    (
                        10000,
                        272799,
                        -500,
                    ),
                ],
            ),
            (
                "journal/E",
                1,
                [
                    (
                        1000,
                        0,
                        -500,
                    ),
                ],
            ),
        ]
        "#);
    }

    #[test]
    fn test_completed_update() {
        // Bindings 0 and 1 share cohort 0; binding 2 is alone in cohort 1.
        let mut completed = Completed::new(vec![0, 0, 1]);
        assert_eq!(completed.num_cohorts(), 2);

        completed.update(&Frontier {
            journals: vec![
                jf(
                    "journal/A",
                    0,
                    vec![pf(0x01, 500, 0, -100), pf(0x03, 900, 0, -100)],
                ),
                jf("journal/B", 1, vec![pf(0x01, 700, 0, -100)]),
                jf("journal/C", 2, vec![pf(0x05, 0, 800, 0)]),
            ],
            ..Default::default()
        });

        // The cohort ledger max-merges its bindings: P1 takes binding 1's newer
        // 700s, and cohort 1 knows nothing of either.
        assert_eq!(
            completed.clock(0, crate::testing::producer(0x01)),
            from_secs(700)
        );
        assert_eq!(
            completed.clock(1, crate::testing::producer(0x01)),
            from_secs(700)
        );
        assert_eq!(
            completed.clock(0, crate::testing::producer(0x03)),
            from_secs(900)
        );
        assert_eq!(
            completed.clock(2, crate::testing::producer(0x01)),
            Clock::zero()
        );

        // Sentinels are per-binding, from that binding's journals only. Binding
        // 2 holds nothing but a hint, which is no promoted commit at all.
        assert_eq!(completed.binding_max[0], from_secs(900));
        assert_eq!(completed.binding_max[1], from_secs(700));
        assert_eq!(completed.binding_max[2], Clock::zero());

        // A further promotion advances both, and only upward.
        completed.update(&Frontier {
            journals: vec![jf(
                "journal/A",
                0,
                vec![pf(0x01, 400, 0, -1), pf(0x03, 1_200, 0, -1)],
            )],
            ..Default::default()
        });
        assert_eq!(
            completed.clock(0, crate::testing::producer(0x01)),
            from_secs(700),
            "a regressing clock is ignored",
        );
        assert_eq!(
            completed.clock(0, crate::testing::producer(0x03)),
            from_secs(1_200)
        );
        assert_eq!(completed.binding_max[0], from_secs(1_200));
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
