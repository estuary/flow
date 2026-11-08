use proto_flow::shuffle;
use proto_gazette::uuid;

/// SliceBuffer holds one Slice's rounds at a Log until they merge.
/// Each round is the Slice's Appends, closed by a Barrier.
///
/// Entries pop in arrival order: the order of the Slice's own merge over its
/// journal reads. The queue front defines this Slice's position in the merge.
/// A Barrier is never below an Append the Slice has sent (`on_round` verifies).
///
/// Popping a Barrier adds its quota to `released`, which frees room in the
/// Slice's window. It also yields the Barrier's flush request, if any.
///
/// While nothing is queued, the Slice gates the merge at the position of its
/// last Barrier, unless that Barrier was caught up (`next_merge`).
/// See "Log Merge and Output" and "Flush Cycle" of the crate README.
pub struct SliceBuffer {
    /// Priority of the Slice, at which its entries merge.
    priority: i32,
    /// Position at which the Slice gates the merge while nothing is queued:
    /// that of its last Barrier. None if that Barrier was caught up.
    /// Until its first round, a Slice gates at zero.
    gate: Option<uuid::Clock>,
    /// Greatest adjusted clock of any received Append.
    /// Every Barrier must be at-or-above.
    append_hwm: uuid::Clock,
    /// Unmerged Appends and Barriers, in arrival order.
    queue: std::collections::VecDeque<Entry>,
    /// Lifetime quota of merged Barriers, which releases the Slice's window.
    released: u64,
    /// Journal of the last-popped Append. Journal names are delta-encoded
    /// against the Slice's previous Append to this Log, and are decoded as
    /// Appends pop in that same arrival order.
    journal: String,
}

enum Entry {
    Append(shuffle::log_request::Append),
    Barrier(shuffle::log_request::Barrier),
}

/// An entry popped from a SliceBuffer for its merge.
pub enum Popped<'a> {
    Append {
        append: shuffle::log_request::Append,
        /// Delta-decoded journal name of the Append.
        journal: &'a str,
    },
    /// A merged Barrier, whose quota is released.
    Barrier {
        /// Cycle of a flush which is released by the Barrier's merge.
        flush_cycle: Option<u64>,
    },
}

impl SliceBuffer {
    pub fn new(priority: i32) -> Self {
        Self {
            priority,
            gate: Some(uuid::Clock::zero()),
            append_hwm: uuid::Clock::zero(),
            queue: Default::default(),
            released: 0,
            journal: String::new(),
        }
    }

    /// Verify and apply a Slice's received round,
    /// whose Barrier must be at or above each Append the Slice has sent.
    pub fn on_round(
        &mut self,
        appends: Vec<shuffle::log_request::Append>,
        barrier: shuffle::log_request::Barrier,
    ) -> anyhow::Result<()> {
        for append in appends {
            self.append_hwm = self.append_hwm.max(crate::merge::adjusted_clock(&append));
            self.queue.push_back(Entry::Append(append));
        }

        if barrier.adjusted_clock < self.append_hwm.as_u64() {
            anyhow::bail!(
                "Slice's Barrier {} is below a prior Append at {}",
                barrier.adjusted_clock,
                self.append_hwm.as_u64(),
            );
        }
        // The gate matters only once the queue drains through this Barrier,
        // assuming it's still the Slice's last.
        self.gate = (!barrier.caught_up).then(|| uuid::Clock::from_u64(barrier.adjusted_clock));
        self.queue.push_back(Entry::Barrier(barrier));

        Ok(())
    }

    /// Lifetime quota of merged Barriers, which releases the Slice's window.
    pub fn released(&self) -> u64 {
        self.released
    }

    /// Merge position of the next entry to pop.
    pub fn peek_position(&self) -> Option<crate::merge::Position> {
        self.queue.front().map(|entry| match entry {
            Entry::Append(append) => crate::merge::Position::from_append(self.priority, append),
            Entry::Barrier(barrier) => crate::merge::Position {
                priority: self.priority,
                adjusted_clock: uuid::Clock::from_u64(barrier.adjusted_clock),
            },
        })
    }

    /// Pop the next arrival-order entry for its merge.
    pub fn pop(&mut self) -> Popped<'_> {
        match self.queue.pop_front().expect("pop requires a queued entry") {
            Entry::Append(append) => {
                if append.journal_name_truncate_delta != 0 || !append.journal_name_suffix.is_empty()
                {
                    gazette::delta::decode(
                        &mut self.journal,
                        append.journal_name_truncate_delta,
                        &append.journal_name_suffix,
                    );
                }
                Popped::Append {
                    append,
                    journal: &self.journal,
                }
            }
            Entry::Barrier(shuffle::log_request::Barrier {
                quota, flush_cycle, ..
            }) => {
                self.released += quota;
                Popped::Barrier {
                    flush_cycle: (flush_cycle != 0).then_some(flush_cycle),
                }
            }
        }
    }
}

/// The next step of a Log's merge across its Slice buffers.
#[derive(Debug, PartialEq, Eq)]
pub enum NextMerge {
    /// Nothing is queued, and no Slice gates the merge.
    Idle,
    /// The merge awaits a next round of `slice`, whose gate orders before
    /// every queued entry.
    Gated { slice: usize },
    /// Queued entries of `slice` may merge in arrival order, while each is at
    /// or before `through`. `through` is the least position of any peer Slice.
    /// It changes only when a round is read, so a run of `slice` entries may
    /// merge without calling `next_merge` again.
    Ready {
        slice: usize,
        through: crate::merge::Position,
    },
}

/// Determine the next step of the merge across `slices`.
///
/// Each Slice has a merge position. It's that of its next queued entry or,
/// if nothing is queued, that of its gate. A Slice with neither is skipped.
///
/// The Slice with the least position goes next. A tie goes to an entry over
/// a gate, and then to the lower Slice index. If the least is a gate, the
/// merge waits on that Slice (`Gated`). If it's an entry, that Slice merges
/// through the least position of its peers (`Ready`).
///
/// An entry must win a tie with a gate for the merge to be live
/// (see `super::actor::LogActor`).
pub fn next_merge(slices: &[SliceBuffer]) -> NextMerge {
    // The `least` (position, is_gate, slice) of queued entries and gates,
    // and the second-least merge-`through` Position of its peers.
    let mut least = None;
    let mut through = crate::merge::Position::MAX;

    for (index, slice) in slices.iter().enumerate() {
        let (position, is_gate) = match (slice.peek_position(), slice.gate) {
            (Some(position), _) => (position, false),
            (None, Some(gate)) => {
                let position = crate::merge::Position {
                    priority: slice.priority,
                    adjusted_clock: gate,
                };
                (position, true)
            }
            (None, None) => continue,
        };
        if let Some((prior, prior_is_gate, _)) = least {
            if (position, is_gate) >= (prior, prior_is_gate) {
                through = through.min(position);
            } else {
                through = prior;
                least = Some((position, is_gate, index));
            }
        } else {
            least = Some((position, is_gate, index));
        }
    }

    match least {
        None => NextMerge::Idle,
        Some((_, true, slice)) => NextMerge::Gated { slice },
        Some((_, false, slice)) => NextMerge::Ready { slice, through },
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn append(clock: u64) -> shuffle::log_request::Append {
        shuffle::log_request::Append {
            clock,
            packed_key: bytes::Bytes::from_static(b"k"),
            doc_archived: bytes::Bytes::from_static(b"doc"),
            ..Default::default()
        }
    }

    fn barrier(clock: u64, quota: u64, flush_cycle: u64) -> shuffle::log_request::Barrier {
        shuffle::log_request::Barrier {
            adjusted_clock: clock,
            quota,
            caught_up: false,
            flush_cycle,
        }
    }

    fn caught_up(clock: u64) -> shuffle::log_request::Barrier {
        shuffle::log_request::Barrier {
            caught_up: true,
            ..barrier(clock, 0, 0)
        }
    }

    #[test]
    fn test_slice_buffer_and_merge() {
        // Steps record their observations, with positions as p{priority}@{clock}.
        let mut trace = Vec::new();
        let mut note = |step: &str, observed: String| trace.push(format!("{step} -> {observed}"));

        let fmt_position = |p: crate::merge::Position| match p.adjusted_clock.as_u64() {
            u64::MAX => "MAX".to_string(),
            clock => format!("p{}@{clock}", p.priority),
        };
        let fmt_pop = |popped: Popped| match popped {
            Popped::Append { append, journal } => format!("append @{} {journal:?}", append.clock),
            Popped::Barrier { flush_cycle } => format!("barrier, flush: {flush_cycle:?}"),
        };
        let fmt_merge = |m: NextMerge| match m {
            NextMerge::Ready { slice, through } => {
                format!("Ready(slice: {slice}, through: {})", fmt_position(through))
            }
            NextMerge::Gated { slice } => format!("Gated(slice: {slice})"),
            NextMerge::Idle => "Idle".to_string(),
        };
        let round = |s: &mut SliceBuffer, appends, barrier| s.on_round(appends, barrier).unwrap();

        // Entries pop in arrival order, across rounds, whatever their positions.
        // A Barrier releases its quota, and its flush.
        let mut s = SliceBuffer::new(0);
        round(&mut s, vec![append(30), append(20)], barrier(30, 100, 0));
        round(&mut s, vec![append(25)], barrier(35, 50, 7));
        round(&mut s, Vec::new(), barrier(40, 10, 0));
        while s.peek_position().is_some() {
            let position = fmt_position(s.peek_position().unwrap());
            note(&format!("pop {position}"), fmt_pop(s.pop()));
            note("released", s.released().to_string());
        }

        // A Barrier below a prior Append is an error.
        let result = s.on_round(vec![append(60)], barrier(50, 0, 0));
        note(
            "on_round([60], barrier @50)",
            format!("{:?}", result.map_err(|err| err.to_string())),
        );

        // Journal names are delta-decoded as Appends pop.
        let mut s = SliceBuffer::new(0);
        let named = |truncate: i32, suffix: &str| shuffle::log_request::Append {
            journal_name_truncate_delta: truncate,
            journal_name_suffix: suffix.to_string(),
            ..append(1)
        };
        round(
            &mut s,
            vec![
                named(0, "acmeCo/one/pivot=00"),
                named(0, ""),
                named(2, "11"),
            ],
            barrier(1, 0, 0),
        );
        let journals: Vec<_> = (0..3)
            .map(|_| match s.pop() {
                Popped::Append { journal, .. } => journal.to_string(),
                Popped::Barrier { .. } => unreachable!(),
            })
            .collect();
        note("delta-decoded", format!("{journals:?}"));

        // Merge runs as `LogActor::merge_block` does: in arrival order, while
        // the Slice's next entry is at or before `through`.
        let merge_run = |slices: &mut [SliceBuffer]| {
            let NextMerge::Ready { slice, through } = next_merge(slices) else {
                return fmt_merge(next_merge(slices));
            };
            let priority = slices[slice].priority;
            let mut merged = Vec::new();
            while let Some(position) = slices[slice].peek_position()
                && position <= through
            {
                merged.push(match slices[slice].pop() {
                    Popped::Append { append, .. } => format!("p{priority}@{}", append.clock),
                    Popped::Barrier { flush_cycle } => {
                        let flush = flush_cycle.map_or(String::new(), |c| format!(" flush {c}"));
                        format!("|p{priority}@{}{flush}", position.adjusted_clock.as_u64())
                    }
                });
            }
            format!("s{slice}: [{}]", merged.join(", "))
        };

        // Merge runs until the merge must wait.
        let drain = |slices: &mut [SliceBuffer]| {
            let mut runs = Vec::new();
            loop {
                let run = merge_run(slices);
                let waits = !run.starts_with('s');
                runs.push(run);
                if waits {
                    return runs.join("; ");
                }
            }
        };

        // Slice 0 is of priority 1, and Slices 1 and 2 of priority 0.
        // Barriers are shown as |p{priority}@{clock}.
        let mut slices: Vec<_> = [1, 0, 0].map(SliceBuffer::new).into();

        // Before its first round, a Slice gates every merge at zero.
        note("merge", fmt_merge(next_merge(&slices)));
        for slice in slices.iter_mut() {
            round(slice, Vec::new(), caught_up(0));
        }
        note("all caught up", drain(&mut slices));

        // A Slice with nothing queued gates the merge at its last Barrier.
        round(&mut slices[1], vec![append(100)], barrier(110, 0, 0));
        round(&mut slices[2], Vec::new(), barrier(90, 0, 0));
        note("s1 [p0@100]|110, s2 |90", drain(&mut slices));

        // A Barrier requesting a flush merges only once the merge has passed
        // it, though its Slice appended nothing.
        round(&mut slices[2], Vec::new(), barrier(105, 0, 9));
        note("s2 |105 flush 9", drain(&mut slices));

        // Ties of entries break by Slice index, and an entry merges ahead of a
        // peer's gate at an equal position.
        round(&mut slices[2], vec![append(110)], barrier(110, 0, 0));
        note("s2 [p0@110]|110", drain(&mut slices));

        // The higher-priority Slice merges first, whatever its clock, and its
        // gate holds back every lesser-priority entry until it's caught up.
        round(&mut slices[1], vec![append(120)], barrier(120, 0, 0));
        round(&mut slices[2], Vec::new(), caught_up(110));
        round(&mut slices[0], vec![append(500)], barrier(500, 0, 0));
        note(
            "s1 [p0@120]|120, s2 caught up, s0 [p1@500]|500",
            drain(&mut slices),
        );
        round(&mut slices[0], Vec::new(), caught_up(500));
        note("s0 caught up", drain(&mut slices));

        // A Slice's Appends may regress below its prior Barriers (here p0@125
        // after |p0@140), and merge best-effort.
        round(&mut slices[1], vec![append(130)], barrier(140, 0, 0));
        round(&mut slices[1], vec![append(125)], barrier(150, 0, 0));
        note("s1 [p0@130]|140, [p0@125]|150", drain(&mut slices));

        insta::assert_snapshot!(trace.join("\n"));
    }
}
