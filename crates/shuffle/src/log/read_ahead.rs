use proto_flow::shuffle;

/// SliceReadAhead is a Log's read-ahead of rounds from one Slice. Its queued
/// Appends are bounded by the Slice's Append credits (`APPEND_CREDIT_BYTES`),
/// which it enforces and returns as Appends pop.
///
/// Appends pop in arrival order, which is the order the Slice's own merge
/// produced them, and merge at the priority of the Slice's lane. A Flush
/// barrier counts the Appends which preceded it, and releases as the last of
/// them pops. See "Log Merge and Output" and "Flush Cycle" of the crate README.
pub struct SliceReadAhead {
    /// Priority of the Slice's lane, at which its Appends merge.
    priority: i32,
    /// Unmerged Appends, in arrival order.
    queue: std::collections::VecDeque<shuffle::log_request::Append>,
    /// Bytes of queued Appends, within the Slice's credits.
    queued_bytes: u64,
    /// Cumulative bytes of popped Appends, returned to the Slice as credits.
    merged_bytes: u64,
    /// Barrier of the Slice's Flush, until its preceding Appends merge.
    barrier: Option<FlushBarrier>,
    /// Journal of the last-popped Append. Journal names are delta-encoded
    /// against the Slice's previous Append to this Log, and are decoded as
    /// Appends pop in that same arrival order.
    journal: String,
}

/// A Slice's requested flush, which awaits the merge of its preceding Appends.
#[derive(Debug)]
struct FlushBarrier {
    cycle: u64,
    /// Number of preceding Appends which are not yet merged.
    /// They're at the front of the queue, and merge before any others.
    remaining: usize,
}

/// An Append popped from a SliceReadAhead for its merge.
pub struct Popped<'a> {
    pub append: shuffle::log_request::Append,
    /// Delta-decoded journal name of the Append.
    pub journal: &'a str,
    /// Cycle of a flush which is released by this Append's merge.
    pub released: Option<u64>,
}

impl SliceReadAhead {
    pub fn new(priority: i32) -> Self {
        Self {
            priority,
            queue: Default::default(),
            queued_bytes: 0,
            merged_bytes: 0,
            barrier: None,
            journal: String::new(),
        }
    }

    /// Apply a Slice's received round, verifying that its Appends are within
    /// the Slice's credits. Returns the flush's cycle if it's released
    /// immediately, because no queued Append precedes it.
    pub fn on_round(
        &mut self,
        appends: Vec<shuffle::log_request::Append>,
        flush: Option<shuffle::log_request::Flush>,
    ) -> anyhow::Result<Option<u64>> {
        for append in appends {
            // The Slice admitted this Append having sent every Append we've
            // received before it, and been credited no more than we've merged,
            // so its outstanding bytes then were at least our queued bytes now.
            let bytes = crate::merge::append_bytes(
                crate::merge::APPEND_OVERHEAD_BYTES,
                &append.doc_archived,
                &append.packed_key,
            );
            if self.queued_bytes != 0
                && self.queued_bytes + bytes > crate::merge::APPEND_CREDIT_BYTES
            {
                anyhow::bail!(
                    "Slice exceeded its Append credits: an Append of {bytes} bytes \
                     with {} bytes queued exceeds {}",
                    self.queued_bytes,
                    crate::merge::APPEND_CREDIT_BYTES,
                );
            }
            self.queued_bytes += bytes;
            self.queue.push_back(append);
        }

        let Some(shuffle::log_request::Flush { cycle }) = flush else {
            return Ok(None);
        };

        assert!(
            self.barrier.is_none(),
            "flush requested while a prior flush is pending"
        );

        let remaining = self.queue.len();
        if remaining == 0 {
            return Ok(Some(cycle)); // Barrier is already released.
        }

        self.barrier = Some(FlushBarrier { cycle, remaining });
        Ok(None)
    }

    /// Cumulative bytes of popped Appends, which are the Slice's credits.
    pub fn merged_bytes(&self) -> u64 {
        self.merged_bytes
    }

    /// Merge position of the next Append to pop.
    pub fn peek_position(&self) -> Option<crate::merge::Position> {
        self.queue
            .front()
            .map(|append| crate::merge::Position::from_append(self.priority, append))
    }

    /// Pop the Append which arrived first for its merge.
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

        let bytes = crate::merge::append_bytes(
            crate::merge::APPEND_OVERHEAD_BYTES,
            &append.doc_archived,
            &append.packed_key,
        );
        self.queued_bytes -= bytes;
        self.merged_bytes += bytes;

        let mut released = None;
        if let Some(barrier) = &mut self.barrier {
            barrier.remaining -= 1;
            if barrier.remaining == 0 {
                released = self.barrier.take().map(|barrier| barrier.cycle);
            }
        }

        Popped {
            append,
            journal: &self.journal,
            released,
        }
    }
}

/// The next step of a Log's merge across its Slice read-aheads.
#[derive(Debug, PartialEq, Eq)]
pub enum NextMerge {
    /// No Appends are queued.
    Idle,
    /// Queued Appends of `slice` may be merged in arrival order, while each
    /// is at or before `through`: the least queued position of a peer Slice.
    /// Note that `through` cannot change until a Slice round is read.
    Ready {
        slice: usize,
        through: crate::merge::Position,
    },
}

/// Determine the next step of the merge across `slices`: the Slice whose next
/// Append is least in merge order (breaking ties by Slice index), through the
/// least next Append of its peers. Equal positions share a lane, so ties
/// break by shard (Slices are indexed by lane, then shard).
pub fn next_merge(slices: &[SliceReadAhead]) -> NextMerge {
    let mut least: Option<(usize, crate::merge::Position)> = None;
    let mut through = crate::merge::Position::MAX;

    for (index, slice) in slices.iter().enumerate() {
        let Some(position) = slice.peek_position() else {
            continue;
        };
        match least {
            Some((_, prior)) if position >= prior => {
                through = through.min(position);
            }
            Some((_, prior)) => {
                through = prior;
                least = Some((index, position));
            }
            None => least = Some((index, position)),
        }
    }

    match least {
        Some((slice, _)) => NextMerge::Ready { slice, through },
        None => NextMerge::Idle,
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn append(clock: u64, flags: u32, doc: &[u8]) -> shuffle::log_request::Append {
        shuffle::log_request::Append {
            clock,
            flags,
            packed_key: bytes::Bytes::from_static(b"k"),
            doc_archived: bytes::Bytes::copy_from_slice(doc),
            ..Default::default()
        }
    }

    #[test]
    fn test_slice_read_ahead_and_merge() {
        let flush = |cycle| Some(shuffle::log_request::Flush { cycle });

        // Steps record their observations, with positions as p{priority}@{clock}.
        let mut trace = Vec::new();
        let mut note = |step: &str, observed: String| trace.push(format!("{step} -> {observed}"));

        let fmt_position = |p: crate::merge::Position| match p.adjusted_clock.as_u64() {
            u64::MAX => "MAX".to_string(),
            clock => format!("p{}@{clock}", p.priority),
        };
        let fmt_pop = |popped: Popped| {
            let Popped {
                append, released, ..
            } = popped;
            format!(
                "clock {}, flags: {}, released: {released:?}",
                append.clock, append.flags
            )
        };
        let fmt_merge = |m: NextMerge| match m {
            NextMerge::Ready { slice, through } => {
                format!("Ready(slice: {slice}, through: {})", fmt_position(through))
            }
            m => format!("{m:?}"),
        };
        let fmt_peek = |s: &SliceReadAhead| format!("{:?}", s.peek_position().map(fmt_position));
        let round = |s: &mut SliceReadAhead, appends, flush| s.on_round(appends, flush).unwrap();

        // A flush with no queued Appends is released immediately.
        let mut s = SliceReadAhead::new(0);
        let released = round(&mut s, Vec::new(), flush(3));
        note("on_round([], flush 3)", format!("{released:?}"));

        // Appends pop in arrival order, across rounds, whatever their positions.
        round(
            &mut s,
            vec![append(30, 0, b"doc"), append(20, 0, b"doc")],
            None,
        );
        round(&mut s, vec![append(25, 1, b"doc")], None);
        for _ in 0..3 {
            note("peek", fmt_peek(&s));
            note("pop", fmt_pop(s.pop()));
        }
        note("peek", fmt_peek(&s));

        // A flush requested behind queued Appends {30, 40} (including those of
        // its own round) is released by the merge of the last. A later-arriving
        // Append (25) merges after them.
        round(&mut s, vec![append(30, 0, b"doc")], None);
        let released = round(&mut s, vec![append(40, 0, b"doc")], flush(7));
        note("on_round([40], flush 7)", format!("{released:?}"));
        round(&mut s, vec![append(25, 0, b"doc")], None);

        note("pop", fmt_pop(s.pop()));
        note("pop", fmt_pop(s.pop()));
        note("pop", fmt_pop(s.pop()));
        note(
            "drained",
            format!("barrier: {:?}, queued_bytes: {}", s.barrier, s.queued_bytes),
        );

        // Credits: popped Appends count towards merged bytes, which are
        // returned to the Slice. Bytes are in eighths of credits.
        const CREDIT: u64 = crate::merge::APPEND_CREDIT_BYTES;
        let eighths = |bytes: u64| format!("{}/8", bytes as f64 / (CREDIT / 8) as f64);
        let sized = |clock: u64, bytes: u64| {
            let overhead =
                crate::merge::append_bytes(crate::merge::APPEND_OVERHEAD_BYTES, b"", b"k");
            append(clock, 0, &vec![0; (bytes - overhead) as usize])
        };
        let mut on_round = |s: &mut SliceReadAhead, step: &str, appends| {
            let result = s.on_round(appends, None);
            note(
                step,
                format!(
                    "{:?}, queued: {}, merged: {}",
                    result.map_err(|err| err.to_string()),
                    eighths(s.queued_bytes),
                    eighths(s.merged_bytes()),
                ),
            );
        };
        let mut s = SliceReadAhead::new(0);
        on_round(
            &mut s,
            "on_round([4/8, 4/8])",
            vec![sized(50, CREDIT / 2), sized(51, CREDIT / 2)],
        );
        _ = s.pop();
        on_round(&mut s, "pop, on_round([4/8])", vec![sized(52, CREDIT / 2)]);

        // A Slice which sends beyond its credits is an error.
        on_round(&mut s, "on_round([doc])", vec![append(53, 0, b"doc")]);

        // Any Append is admitted if nothing is queued, however large.
        _ = s.pop();
        _ = s.pop();
        on_round(
            &mut s,
            "pop x2, on_round([16/8])",
            vec![sized(54, CREDIT * 2)],
        );
        _ = s.pop();
        note("pop", format!("merged: {}", eighths(s.merged_bytes())));

        // Journal names are delta-decoded as Appends pop.
        let mut s = SliceReadAhead::new(0);
        let named = |truncate: i32, suffix: &str| shuffle::log_request::Append {
            journal_name_truncate_delta: truncate,
            journal_name_suffix: suffix.to_string(),
            ..append(1, 0, b"doc")
        };
        round(
            &mut s,
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
        let merge_run = |slices: &mut [SliceReadAhead]| {
            let NextMerge::Ready { slice, through } = next_merge(slices) else {
                return fmt_merge(next_merge(slices));
            };
            let priority = slices[slice].priority;
            let mut merged = Vec::new();
            while slices[slice]
                .peek_position()
                .is_some_and(|position| position <= through)
            {
                let Popped {
                    append, released, ..
                } = slices[slice].pop();
                let released = released.map_or(String::new(), |c| format!(" released {c}"));
                merged.push(format!("p{priority}@{}{released}", append.clock));
            }
            format!("s{slice}: [{}]", merged.join(", "))
        };

        // Merge: the Slice with the least next Append, through the least next
        // Append of its peers. Slices with nothing queued don't participate.
        // Slice 0 is of a lane having priority 1, and Slices 1 and 2 of priority 0.
        let mut slices: Vec<_> = [1, 0, 0].map(SliceReadAhead::new).into();
        note("merge", fmt_merge(next_merge(&slices)));

        round(&mut slices[1], vec![append(100, 0, b"doc")], None);
        note("s1 [p0@100]", fmt_merge(next_merge(&slices)));

        round(&mut slices[2], vec![append(90, 0, b"doc")], None);
        note("s2 [p0@90]", fmt_merge(next_merge(&slices)));

        // The higher-priority lane merges first, whatever its clock.
        round(&mut slices[0], vec![append(200, 0, b"doc")], None);
        note("s0 [p1@200]", fmt_merge(next_merge(&slices)));
        note("run", merge_run(&mut slices));
        note("run", merge_run(&mut slices));

        // Ties break by Slice index.
        round(&mut slices[2], vec![append(100, 0, b"doc")], None);
        note("s2 [p0@100]", fmt_merge(next_merge(&slices)));

        // A run continues through a later-arriving lesser Append, and through
        // Appends equal to `through`, and releases a flush barrier mid-run.
        round(
            &mut slices[1],
            vec![
                append(95, 0, b"doc"),
                append(92, 0, b"doc"),
                append(100, 0, b"doc"),
            ],
            flush(9),
        );
        round(&mut slices[1], vec![append(101, 0, b"doc")], None);
        note("s1 [95, 92, 100] flush 9, [101]", merge_run(&mut slices));
        note("run", merge_run(&mut slices));
        note("run", merge_run(&mut slices));
        note("run", merge_run(&mut slices));

        insta::assert_snapshot!(trace.join("\n"));
    }
}
