//! Clock reductions over per-journal offsets, ported from `go/testing/clock.go`.
//!
//! A `Clock` (Gazette's `pb.Offsets`) maps a journal name to a byte offset. In
//! the test harness these are logical offsets into the in-memory collection
//! store rather than broker journal offsets, but the reduction semantics are
//! identical: the scheduler compares and merges reader/writer progress.
//!
//! Journal names carry the collection prefix (`collection/partition`) and, for
//! a reader's read progress, a `;{journal_read_suffix}` suffix — the same
//! string conventions the V1 graph relied on, so the port is verbatim.

use std::collections::BTreeMap;

/// A journal name (`collection/partition` optionally with a `;suffix`).
pub type Journal = String;

/// Per-journal byte offsets. `BTreeMap` (vs. Go's `map`) gives deterministic
/// iteration, which keeps derived snapshots stable.
pub type Clock = BTreeMap<Journal, i64>;

/// Reduce by taking the smallest offset of each common journal.
pub fn min_clock(lhs: &Clock, rhs: &Clock) -> Clock {
    let mut out = lhs.clone();
    for (journal, &r) in rhs {
        out.entry(journal.clone())
            .and_modify(|l| {
                if *l > r {
                    *l = r;
                }
            })
            .or_insert(r);
    }
    out
}

/// Reduce by taking the largest offset of each common journal.
pub fn max_clock(lhs: &Clock, rhs: &Clock) -> Clock {
    let mut out = lhs.clone();
    for (journal, &r) in rhs {
        out.entry(journal.clone())
            .and_modify(|l| {
                if *l < r {
                    *l = r;
                }
            })
            .or_insert(r);
    }
    out
}

/// Returns true if `rhs` is contained within `lhs`: all `rhs` journals are
/// present in `lhs` with an equal or greater offset. This is the check that
/// terminates self-cycles in the scheduler — a task has "read through" its own
/// prior write once its read clock contains the projected write clock.
pub fn contains_clock(lhs: &Clock, rhs: &Clock) -> bool {
    for (journal, &offset) in rhs {
        if lhs.get(journal).copied().unwrap_or(0) < offset {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clock<const N: usize>(entries: [(&str, i64); N]) -> Clock {
        entries
            .into_iter()
            .map(|(j, o)| (j.to_string(), o))
            .collect()
    }

    /// Port of `TestClockReductionAndOrdering` from `go/testing/clock_test.go`.
    #[test]
    fn clock_reduction_and_ordering() {
        let c1 = clock([("one", 1), ("two", 2), ("three", 3)]);
        let c2 = clock([("one", 2), ("two", 1), ("four", 4)]);

        let r_min = min_clock(&c1, &c2);
        let r_max = max_clock(&c1, &c2);

        assert_eq!(
            r_min,
            clock([("one", 1), ("two", 1), ("three", 3), ("four", 4)])
        );
        assert_eq!(
            r_max,
            clock([("one", 2), ("two", 2), ("three", 3), ("four", 4)])
        );

        // Ordering expectations.
        assert!(!contains_clock(&c1, &r_min));
        assert!(!contains_clock(&c2, &r_min));

        assert!(!contains_clock(&c1, &c2));
        assert!(!contains_clock(&c2, &c1));

        assert!(contains_clock(&r_max, &c1));
        assert!(contains_clock(&r_max, &c2));

        assert!(contains_clock(&r_max, &r_min));
        assert!(!contains_clock(&r_min, &r_max));
    }
}
