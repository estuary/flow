use proto_flow::shuffle;

/// SliceBuffer holds one Slice's current round at a Log until it merges.
///
/// A Log reads a Slice's next round only once its current round has merged
/// (`is_drained`). Until then the Log doesn't read the Slice's request stream,
/// and HTTP/2 flow control holds back the Slice.
///
/// Appends pop in arrival order: the order of the Slice's own merge over its
/// journal reads. A round's flush request is released as its last Append pops.
/// See "Log Merge and Output" and "Flush Cycle" of the crate README.
pub struct SliceBuffer {
    /// Unmerged Appends of the current round, in arrival order.
    queue: std::collections::VecDeque<shuffle::log_request::Append>,
    /// Cycle of the current round's flush request, released once its Appends merge.
    flush_cycle: Option<u64>,
    /// Journal of the last-popped Append. Journal names are delta-encoded
    /// against the Slice's previous Append to this Log, and are decoded as
    /// Appends pop in that same arrival order.
    journal: String,
}

/// An Append popped from a SliceBuffer for its merge.
pub struct Popped<'a> {
    pub append: shuffle::log_request::Append,
    /// Delta-decoded journal name of the Append.
    pub journal: &'a str,
    /// Cycle of a flush which is released by this Append's merge.
    pub flush_cycle: Option<u64>,
}

impl SliceBuffer {
    pub fn new() -> Self {
        Self {
            queue: Default::default(),
            flush_cycle: None,
            journal: String::new(),
        }
    }

    /// Apply a Slice's received round, which must follow the merge of its
    /// prior round. Returns the round's flush cycle if it's released
    /// immediately, because the round has no Appends.
    pub fn on_round(
        &mut self,
        appends: Vec<shuffle::log_request::Append>,
        flush: Option<shuffle::log_request::Flush>,
    ) -> Option<u64> {
        assert!(
            self.is_drained(),
            "a round is read only once its prior round merges"
        );

        let flush_cycle = flush.map(|shuffle::log_request::Flush { cycle }| cycle);
        if appends.is_empty() {
            return flush_cycle;
        }
        self.queue.extend(appends);
        self.flush_cycle = flush_cycle;
        None
    }

    /// Whether the current round has merged, and the Slice's next round may be read.
    pub fn is_drained(&self) -> bool {
        self.queue.is_empty()
    }

    /// Merge position of the next Append to pop.
    pub fn peek_position(&self) -> Option<crate::merge::Position> {
        self.queue.front().map(crate::merge::Position::from_append)
    }

    /// Pop the next arrival-order Append for its merge.
    pub fn pop(&mut self) -> Popped<'_> {
        let append = self
            .queue
            .pop_front()
            .expect("pop requires a queued Append");

        if append.journal_name_truncate_delta != 0 || !append.journal_name_suffix.is_empty() {
            gazette::delta::decode(
                &mut self.journal,
                append.journal_name_truncate_delta,
                &append.journal_name_suffix,
            );
        }

        let flush_cycle = if self.queue.is_empty() {
            self.flush_cycle.take()
        } else {
            None
        };

        Popped {
            append,
            journal: &self.journal,
            flush_cycle,
        }
    }
}

/// The next step of a Log's merge across its Slice buffers.
#[derive(Debug, PartialEq, Eq)]
pub enum NextMerge {
    /// No Appends are queued.
    Idle,
    /// Queued Appends of `slice` may merge in arrival order, while each is at
    /// or before `through`. `through` is the least position of any peer Slice.
    /// It changes only when a round is read, so a run of `slice` Appends may
    /// merge without calling `next_merge` again.
    Ready {
        slice: usize,
        through: crate::merge::Position,
    },
}

/// Determine the next step of the merge across `slices`: the Slice whose next
/// Append is least in merge order (breaking ties by Slice index), through the
/// least next Append of its peers. Slices with nothing queued don't
/// participate, so order across Slices is best-effort.
pub fn next_merge(slices: &[SliceBuffer]) -> NextMerge {
    let mut least: Option<(crate::merge::Position, usize)> = None;
    let mut through = crate::merge::Position::MAX;

    for (index, slice) in slices.iter().enumerate() {
        let Some(position) = slice.peek_position() else {
            continue;
        };
        if let Some((prior, _)) = least {
            if position >= prior {
                through = through.min(position);
            } else {
                through = prior;
                least = Some((position, index));
            }
        } else {
            least = Some((position, index));
        }
    }

    match least {
        None => NextMerge::Idle,
        Some((_, slice)) => NextMerge::Ready { slice, through },
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn append(priority: i32, clock: u64) -> shuffle::log_request::Append {
        shuffle::log_request::Append {
            priority,
            clock,
            packed_key: bytes::Bytes::from_static(b"k"),
            doc_archived: bytes::Bytes::from_static(b"doc"),
            ..Default::default()
        }
    }

    #[test]
    fn test_slice_buffer_and_merge() {
        let flush = |cycle| Some(shuffle::log_request::Flush { cycle });

        // Steps record their observations, with positions as p{priority}@{clock}.
        let mut trace = Vec::new();
        let mut note = |step: &str, observed: String| trace.push(format!("{step} -> {observed}"));

        let fmt_position = |p: crate::merge::Position| match p.adjusted_clock.as_u64() {
            u64::MAX => "MAX".to_string(),
            clock => format!("p{}@{clock}", p.priority),
        };
        let fmt_pop = |popped: Popped| {
            format!(
                "clock {}, released: {:?}",
                popped.append.clock, popped.flush_cycle
            )
        };
        let fmt_merge = |m: NextMerge| match m {
            NextMerge::Ready { slice, through } => {
                format!("Ready(slice: {slice}, through: {})", fmt_position(through))
            }
            m => format!("{m:?}"),
        };
        let fmt_peek = |s: &SliceBuffer| format!("{:?}", s.peek_position().map(fmt_position));

        // A flush of a round without Appends is released immediately.
        let mut s = SliceBuffer::new();
        let released = s.on_round(Vec::new(), flush(3));
        note("on_round([], flush 3)", format!("{released:?}"));

        // Appends pop in arrival order, whatever their positions, and a round's
        // flush is released by the pop of its last Append.
        let released = s.on_round(vec![append(0, 30), append(0, 20), append(0, 25)], flush(7));
        note("on_round([30, 20, 25], flush 7)", format!("{released:?}"));
        for _ in 0..3 {
            note("peek", fmt_peek(&s));
            note("pop", fmt_pop(s.pop()));
        }
        note("drained", format!("{}", s.is_drained()));

        // Journal names are delta-decoded as Appends pop.
        let named = |truncate: i32, suffix: &str| shuffle::log_request::Append {
            journal_name_truncate_delta: truncate,
            journal_name_suffix: suffix.to_string(),
            ..append(0, 1)
        };
        s.on_round(
            vec![
                named(0, "acmeCo/one/pivot=00"),
                named(0, ""),
                named(2, "11"),
            ],
            None,
        );
        let journals: Vec<_> = (0..3).map(|_| s.pop().journal.to_string()).collect();
        note("delta-decoded", format!("{journals:?}"));

        // Merge runs as `LogActor::merge_block` does: in arrival order, while
        // the Slice's next Append is at or before `through`.
        let merge_run = |slices: &mut [SliceBuffer]| {
            let NextMerge::Ready { slice, through } = next_merge(slices) else {
                return fmt_merge(next_merge(slices));
            };
            let mut merged = Vec::new();
            while slices[slice]
                .peek_position()
                .is_some_and(|position| position <= through)
            {
                let Popped {
                    append,
                    flush_cycle,
                    ..
                } = slices[slice].pop();
                let released = flush_cycle.map_or(String::new(), |c| format!(" released {c}"));
                merged.push(format!("p{}@{}{released}", append.priority, append.clock));
            }
            format!("s{slice}: [{}]", merged.join(", "))
        };

        // Merge: the Slice with the least next Append, through the least next
        // Append of its peers. Slices with nothing queued don't participate.
        let mut slices: Vec<_> = (0..3).map(|_| SliceBuffer::new()).collect();
        note("merge", fmt_merge(next_merge(&slices)));

        slices[1].on_round(vec![append(0, 100)], None);
        note("s1 [p0@100]", fmt_merge(next_merge(&slices)));

        slices[2].on_round(vec![append(0, 90)], None);
        note("s2 [p0@90]", fmt_merge(next_merge(&slices)));

        // A higher-priority Append merges first, whatever its clock.
        slices[0].on_round(vec![append(1, 200)], None);
        note("s0 [p1@200]", fmt_merge(next_merge(&slices)));
        note("run", merge_run(&mut slices));
        note("run", merge_run(&mut slices));

        // Ties break by Slice index.
        slices[2].on_round(vec![append(0, 100)], None);
        note("s2 [p0@100]", fmt_merge(next_merge(&slices)));
        note("run", merge_run(&mut slices));

        // A run continues through a lesser Append which follows, and through
        // Appends equal to `through`, and releases its round's flush.
        slices[1].on_round(vec![append(0, 95), append(0, 92), append(0, 100)], flush(9));
        note("s1 [95, 92, 100] flush 9", merge_run(&mut slices));
        note("run", merge_run(&mut slices));
        note("run", merge_run(&mut slices));

        insta::assert_snapshot!(trace.join("\n"));
    }
}
