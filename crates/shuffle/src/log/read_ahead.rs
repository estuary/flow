use proto_flow::shuffle;

/// SliceReadAhead is a Log's read-ahead of Appends from one Slice, within a
/// receive window of `APPEND_WINDOW_BYTES`. Items are yielded in merge-position
/// order, which allows higher-priority later Appends to take precedence over
/// lower-priority ones within the receive window. A next round is only read
/// while the flow control window is open, and it may overfill the window.
///
/// Read-ahead provides slack for a Slice's lead over its peers and mitigates
/// the impact of upstream journal I/O stalls, allowing log merges to proceed
/// from the read-ahead buffer while still awaiting a next round.
///
/// A Slice's Appends are strongly correlated with, but not exactly in, merge
/// order: a newly-ready journal read may yield an earlier position than one
/// already sent. Ordering the read-ahead lets such an Append merge as soon as
/// it arrives, rather than queuing behind the Slice's earlier and greater
/// Appends.
///
/// A merge constraint is applied on arrival, and holds until superseded.
pub struct SliceReadAhead {
    /// Next Append sequence number to be read from a received Slice round.
    next_seq: u64,
    /// Heap of unmerged Appends, yielded in merge::Position order.
    heap: std::collections::BinaryHeap<Queued>,
    /// Accounted flow-control window.
    queued_bytes: usize,
    /// Latest merge constraint of the Slice, or `Tailing` once it's EOF.
    constraint: crate::merge::Constraint,
    /// A Flush barrier of the Slice, to be released after its preceeding Appends.
    barrier: Option<FlushBarrier>,
    /// Journal of the Slice's last-received Append, and its decoding buffer.
    journal: std::sync::Arc<str>,
    journal_buf: String,
}

/// An Append read ahead from a Slice, awaiting its merge.
struct Queued {
    append: shuffle::log_request::Append,
    /// Delta-decoded journal name of this Append.
    journal: std::sync::Arc<str>,
    /// Arrival sequence number of this Append from its Slice.
    seq: u64,
}

/// A Slice's requested flush, which awaits the merge of its preceeding Appends.
/// Later-arriving Appends may merge before those do, and must not count towards
/// its release.
#[derive(Debug)]
struct FlushBarrier {
    cycle: u64,
    /// Appends having a lesser arrival sequence preceded the flush request.
    before_seq: u64,
    /// Number of preceding Appends which are not yet merged.
    remaining: usize,
}

/// An Append popped from a SliceReadAhead for its merge.
pub struct Popped {
    pub append: shuffle::log_request::Append,
    /// Delta-decoded journal name of the Append.
    pub journal: std::sync::Arc<str>,
    /// Cycle of a flush which is released by this Append's merge.
    pub released: Option<u64>,
}

impl SliceReadAhead {
    pub fn new() -> Self {
        Self {
            heap: Default::default(),
            queued_bytes: 0,
            next_seq: 0,
            constraint: crate::merge::Constraint::INITIAL,
            barrier: None,
            journal: "".into(),
            journal_buf: String::new(),
        }
    }

    /// Apply a Slice's received round.
    /// Returns the flush's cycle if it's released immediately,
    /// because no queued Append precedes it.
    pub fn on_round(
        &mut self,
        appends: Vec<shuffle::log_request::Append>,
        constraint: &shuffle::log_request::MergeConstraint,
        flush: Option<shuffle::log_request::Flush>,
    ) -> Option<u64> {
        for append in appends {
            self.on_append(append);
        }
        self.constraint = crate::merge::Constraint::from_proto(constraint);

        let shuffle::log_request::Flush { cycle } = flush?;

        assert!(
            self.barrier.is_none(),
            "flush requested while a prior flush is pending"
        );

        let remaining = self.heap.len();
        if remaining == 0 {
            return Some(cycle); // Barrier is already released.
        }

        self.barrier = Some(FlushBarrier {
            cycle,
            before_seq: self.next_seq,
            remaining,
        });
        None
    }

    fn on_append(&mut self, append: shuffle::log_request::Append) {
        // Journal names are delta-encoded against the Slice's previous Append
        // to this Log, which requires decoding in arrival order.
        if append.journal_name_truncate_delta != 0 || !append.journal_name_suffix.is_empty() {
            gazette::delta::decode(
                &mut self.journal_buf,
                append.journal_name_truncate_delta,
                &append.journal_name_suffix,
            );
            self.journal = self.journal_buf.as_str().into();
        }
        self.queued_bytes += crate::merge::append_bytes(&append.doc_archived, &append.packed_key);

        let queued = Queued {
            append,
            journal: self.journal.clone(),
            seq: self.next_seq,
        };
        self.next_seq += 1;

        self.heap.push(queued);
    }

    pub fn on_eof(&mut self) {
        // Treat as Tailing to avoid constraining peers.
        self.constraint = crate::merge::Constraint::Tailing;
    }

    /// Whether the receive window is open, and a next round may be read.
    pub fn window_open(&self) -> bool {
        self.queued_bytes < crate::merge::APPEND_WINDOW_BYTES
    }

    /// Merge position of the heap top.
    pub fn peek_position(&self) -> Option<crate::merge::Position> {
        self.heap
            .peek()
            .map(|queued| crate::merge::Position::from_append(&queued.append))
    }

    /// Pop the heap top Append for its merge.
    pub fn pop(&mut self) -> Popped {
        let Queued {
            append,
            journal,
            seq,
        } = self.heap.pop().expect("pop requires a queued Append");

        self.queued_bytes -= crate::merge::append_bytes(&append.doc_archived, &append.packed_key);

        let mut popped = Popped {
            append,
            journal,
            released: None,
        };
        let Some(barrier) = &mut self.barrier else {
            return popped;
        };
        if seq < barrier.before_seq {
            barrier.remaining -= 1;
        }
        if barrier.remaining == 0 {
            popped.released = self.barrier.take().map(|barrier| barrier.cycle);
        }
        popped
    }
}

// Queued is ordered for a BinaryHeap (a max-heap) to yield the Append which
// merges first, breaking ties by arrival.
impl Ord for Queued {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        crate::merge::Position::from_append(&other.append)
            .cmp(&crate::merge::Position::from_append(&self.append))
            .then(other.seq.cmp(&self.seq))
    }
}
impl PartialOrd for Queued {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for Queued {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl Eq for Queued {}

/// The next step of a Log's merge across its Slice read-aheads.
#[derive(Debug, PartialEq, Eq)]
pub enum NextMerge {
    /// No Appends are queued.
    Idle,
    /// Queued Appends of `slice` may be merged in order, while they're at or
    /// before `through` (the effective next constraint of a peer Slice).
    /// Note that `through` cannot change until a Slice round is read.
    Ready {
        slice: usize,
        through: crate::merge::Position,
    },
    /// The least queued Append may not be merged, because it's constrained by
    /// this Slice. We must await its next round.
    Constrained(usize),
}

/// Determine the next step of the ordered merge across `slices`.
pub fn next_merge(slices: &[SliceReadAhead]) -> NextMerge {
    // Least effective constraint of any Slice, as (slice, (position, is_constraint)).
    let mut least: Option<(usize, (crate::merge::Position, bool))> = None;
    // Least effective constraint of any *other* Slice, which bounds a run of merges from `least`.
    let mut through = crate::merge::Position::MAX;
    // Whether any Slice has a queued Append.
    let mut has_queued = false;

    for (index, slice) in slices.iter().enumerate() {
        // Determine the effective constraint this Slice imposes upon its peers,
        // as its actual heap top (preferred), or its last-advertised constraint.
        let (position, is_constraint) = match (slice.peek_position(), slice.constraint) {
            (Some(position), _) => {
                has_queued = true;
                (position, false)
            }
            (None, crate::merge::Constraint::At(position)) => (position, true),
            (None, crate::merge::Constraint::Tailing) => continue,
        };

        // Update `least` and `through`: order by position, then a queued Append
        // before a constraint. A constraint is inclusive, so an Append at an
        // equal position may merge.
        if let Some((_, prior)) = least
            && (position, is_constraint) >= prior
        {
            through = through.min(position) // Retain next smallest.
        } else {
            if let Some((_, (prior, _))) = least {
                through = prior; // Becomes next smallest.
            }
            least = Some((index, (position, is_constraint)));
        }
    }

    match least {
        Some((slice, (_, false))) => NextMerge::Ready { slice, through },
        Some((slice, (_, true))) if has_queued => NextMerge::Constrained(slice),
        _ => NextMerge::Idle,
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn append(binding: u16, clock: u64, flags: u32, doc: &[u8]) -> shuffle::log_request::Append {
        shuffle::log_request::Append {
            binding: binding as u32,
            clock,
            flags,
            packed_key: bytes::Bytes::from_static(b"k"),
            doc_archived: bytes::Bytes::copy_from_slice(doc),
            ..Default::default()
        }
    }

    #[test]
    fn test_slice_read_ahead_and_merge() {
        let constraint = |clock: u64, tailing: bool| shuffle::log_request::MergeConstraint {
            priority: 0,
            adjusted_clock: clock,
            tailing,
        };
        let flush = |cycle| Some(shuffle::log_request::Flush { cycle });

        // Steps record their observations, with positions as raw clocks.
        let mut trace = Vec::new();
        let mut note = |step: &str, observed: String| trace.push(format!("{step} -> {observed}"));

        let fmt_constraint = |c: crate::merge::Constraint| match c {
            crate::merge::Constraint::INITIAL => "INITIAL".to_string(),
            crate::merge::Constraint::At(p) => format!("At({})", p.adjusted_clock.as_u64()),
            crate::merge::Constraint::Tailing => "Tailing".to_string(),
        };
        let fmt_pop = |popped: Popped| {
            let Popped {
                append, released, ..
            } = popped;
            format!(
                "clock: {}, flags: {}, released: {released:?}",
                append.clock, append.flags
            )
        };
        let fmt_merge = |m: NextMerge| match m {
            NextMerge::Ready { slice, through } if through == crate::merge::Position::MAX => {
                format!("Ready(slice: {slice}, through: MAX)")
            }
            NextMerge::Ready { slice, through } => format!(
                "Ready(slice: {slice}, through: {})",
                through.adjusted_clock.as_u64()
            ),
            m => format!("{m:?}"),
        };
        let constrain = |s: &mut SliceReadAhead, clock: u64, tailing: bool| {
            assert_eq!(
                s.on_round(Vec::new(), &constraint(clock, tailing), None),
                None
            );
        };

        // A merge constraint is applied on arrival.
        let mut s = SliceReadAhead::new();
        note("new", fmt_constraint(s.constraint));
        constrain(&mut s, 10, false);
        note("constrain(10)", fmt_constraint(s.constraint));

        // A flush with no queued Appends is released immediately.
        let released = s.on_round(Vec::new(), &constraint(10, false), flush(3));
        note("on_round(flush 3)", format!("{released:?}"));

        // Appends are popped in merge order, regardless of arrival order.
        s.on_append(append(0, 30, 0, b"doc"));
        s.on_append(append(0, 20, 0, b"doc"));
        s.on_append(append(0, 20, 1, b"doc")); // Ties break by arrival.
        note("pop", fmt_pop(s.pop()));
        note("pop", fmt_pop(s.pop()));

        // A flush requested behind queued Appends {30, 40} (including those of
        // its own round) is released only by the merge of both. A later-arriving
        // Append (25) slots in ahead of them, and must not count towards the release.
        let round = vec![append(0, 40, 0, b"doc")];
        let released = s.on_round(round, &constraint(41, false), flush(7));
        note("on_round([40], 41, flush 7)", format!("{released:?}"));
        s.on_append(append(0, 25, 0, b"doc"));

        note("pop", fmt_pop(s.pop()));
        note("pop", fmt_pop(s.pop()));
        s.on_append(append(0, 35, 0, b"doc"));
        note("pop", fmt_pop(s.pop()));
        note("pop", fmt_pop(s.pop()));
        note(
            "drained",
            format!("barrier: {:?}, queued_bytes: {}", s.barrier, s.queued_bytes),
        );

        // The constraint is retained after the heap drains.
        note("drained", fmt_constraint(s.constraint));

        // Receive window: a next round is read while it's open, and may overfill it.
        s.on_append(append(
            0,
            50,
            0,
            &vec![0; crate::merge::APPEND_WINDOW_BYTES - 1],
        ));
        note(
            "on_append(window - 1)",
            format!("window_open: {}", s.window_open()),
        );
        _ = s.pop();
        note("pop", format!("window_open: {}", s.window_open()));

        // Journal names are delta-decoded on arrival. An unchanged name is shared.
        let mut s = SliceReadAhead::new();
        let named = |truncate: i32, suffix: &str| shuffle::log_request::Append {
            journal_name_truncate_delta: truncate,
            journal_name_suffix: suffix.to_string(),
            ..append(0, 1, 0, b"doc")
        };
        s.on_append(named(0, "acmeCo/one/pivot=00"));
        s.on_append(named(0, ""));
        s.on_append(named(2, "11"));
        let journals: Vec<_> = std::iter::from_fn(|| s.heap.pop())
            .map(|queued| (queued.seq, queued.journal))
            .collect::<std::collections::BTreeMap<_, _>>()
            .into_values()
            .collect();
        note(
            "delta-decoded",
            format!(
                "{:?}, first two shared: {}",
                journals.iter().map(|j| &**j).collect::<Vec<_>>(),
                std::sync::Arc::ptr_eq(&journals[0], &journals[1])
            ),
        );

        // Merge: the least of read-ahead minimums is taken if every Slice lacking
        // queued Appends is constrained at or after it, or is tailing or EOF.
        let mut slices: Vec<_> = (0..5).map(|_| SliceReadAhead::new()).collect();
        note("merge", fmt_merge(next_merge(&slices)));

        // Slices begin with their initial constraint, which constrains everything.
        slices[0].on_append(append(0, 100, 0, b"doc"));
        note("s0 append(100)", fmt_merge(next_merge(&slices)));

        // A run is unbounded if every other Slice is tailing or EOF.
        constrain(&mut slices[1], 0, true);
        constrain(&mut slices[2], 0, true);
        constrain(&mut slices[3], 0, true);
        slices[4].on_eof();
        note("s1-3 tailing, s4 EOF", fmt_merge(next_merge(&slices)));
        constrain(&mut slices[1], 200, false);
        constrain(&mut slices[2], 200, false);
        note("s1, s2 constrain(200)", fmt_merge(next_merge(&slices)));

        // A run is bounded by the second-least read-ahead minimum, or the least
        // constraint, whichever is less. Ties of the least break by index.
        slices[1].on_append(append(0, 99, 0, b"doc"));
        note("s1 append(99)", fmt_merge(next_merge(&slices)));
        constrain(&mut slices[2], 99, false);
        note("s2 constrain(99)", fmt_merge(next_merge(&slices)));
        slices[2].on_append(append(0, 99, 0, b"doc"));
        note("s2 append(99)", fmt_merge(next_merge(&slices)));
        _ = slices[1].pop();
        note("s1 pop", fmt_merge(next_merge(&slices)));
        _ = slices[2].pop();

        // Drained Slices 1 and 2 are again constrained by their constraints.
        constrain(&mut slices[1], 99, false);
        note("s2 pop, s1 constrain(99)", fmt_merge(next_merge(&slices)));
        constrain(&mut slices[1], 150, false);
        note("s1 constrain(150)", fmt_merge(next_merge(&slices)));
        constrain(&mut slices[2], 100, false);
        note("s2 constrain(100)", fmt_merge(next_merge(&slices)));

        // Priority orders before clock: a lower-priority minimum is after the constraint.
        _ = slices[0].pop();
        slices[0].on_append(shuffle::log_request::Append {
            priority: -1,
            ..append(0, 1, 0, b"doc")
        });
        note(
            "s0 pop, append(priority -1, 1)",
            fmt_merge(next_merge(&slices)),
        );

        insta::assert_snapshot!(trace.join("\n"));
    }
}
