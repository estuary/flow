//! Clock reductions over per-journal offsets.
//!
//! A `Clock` (Gazette's `pb.Offsets`) maps a journal name to an offset. In the
//! harness these are document indices into the in-memory collection store
//! rather than broker byte offsets, but the reduction semantics are the broker's:
//! the scheduler compares and merges reader/writer progress.
//!
//! Clocks come in two flavors which never mix:
//!
//! - **Write clocks** (`write_at`, `write_clock`) are keyed by plain journal
//!   names, and track how far a collection's journals have been written.
//! - **Read-through clocks** (`read_through`) are keyed by suffixed names,
//!   `{journal};{journal_read_suffix}`, and track one transform's read progress
//!   through those journals — the same journal read by two transforms advances
//!   independently under each suffix.
//!
//! Merging or comparing across flavors is silently meaningless: the key spaces
//! are disjoint, so a reduction would just interleave them. `Graph::project_write`
//! is the one place a write clock becomes a read-through clock, by appending each
//! reader's suffix.

use std::collections::BTreeMap;

/// Per-journal offsets. `BTreeMap` gives the deterministic iteration order that
/// keeps derived snapshots stable.
pub type Clock = BTreeMap<String, i64>;

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
/// present in `lhs` with an equal or greater offset.
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
