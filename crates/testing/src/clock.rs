use itertools::{EitherOrBoth, Itertools};
use protocol::protocol::header::Etcd;
use std::{collections::BTreeMap, fmt::Debug};

/// Clock is vector clock of read or write progress, where the current Etcd revision
/// and each journal offset are viewed as vectorized Clock components.
#[derive(Default, Clone)]
pub struct Clock {
    // Etcd header obtained from a ingestion or shard Stat RPC.
    // Header revisions can be compared to determine relative "happened before"
    // relationships of readers & writers: if a write dynamically creates a new journal,
    // it must necessarily increase the Etcd revision. From there, we can determine whether
    // downstream transforms must be aware of the new journal (or may not be) by
    // comparing against the last revision Stat'd from their shards.
    pub etcd: Etcd,
    // Offsets are a bag of Gazette journals and a byte offset therein.
    pub offsets: BTreeMap<String, i64>,
}

impl Clock {
    /// Build a new Clock from the Etcd header and offsets.
    pub fn new<'a, I, J>(etcd: &Etcd, offsets: I) -> Clock
    where
        I: Iterator<Item = (J, &'a i64)>,
        J: AsRef<str> + 'a,
    {
        let mut clock: Clock = Default::default();
        clock.reduce_max(etcd, offsets);
        clock
    }

    /// Return an empty Clock.
    pub fn empty() -> Clock {
        Default::default()
    }

    /// Reduce a Clock by taking the minimum of Etcd revision and offset for each journal.
    pub fn reduce_min<'a, I, J>(&mut self, rhs_etcd: &Etcd, rhs_offsets: I)
    where
        I: Iterator<Item = (J, &'a i64)>,
        J: AsRef<str> + 'a,
    {
        // Take a smaller LHS revision, so long as it's not zero (uninitialized).
        if self.etcd.revision == 0 || self.etcd.revision > rhs_etcd.revision {
            self.etcd = rhs_etcd.clone()
        };
        // Take the smallest of each common offset.
        for (journal, rhs_offset) in rhs_offsets {
            match self.offsets.remove_entry(journal.as_ref()) {
                Some((journal, lhs_offset)) => {
                    self.offsets.insert(journal, lhs_offset.min(*rhs_offset));
                }
                None => {
                    self.offsets
                        .insert(journal.as_ref().to_owned(), *rhs_offset);
                }
            }
        }
    }

    /// Reduce a Clock by taking the maximum of Etcd revision and offset for each journal.
    pub fn reduce_max<'a, I, J>(&mut self, rhs_etcd: &Etcd, rhs_offsets: I)
    where
        I: Iterator<Item = (J, &'a i64)>,
        J: AsRef<str> + 'a,
    {
        // Take a larger LHS revision.
        if self.etcd.revision < rhs_etcd.revision {
            self.etcd = rhs_etcd.clone()
        };
        // Take the largest of each common offset.
        for (journal, rhs_offset) in rhs_offsets {
            match self.offsets.remove_entry(journal.as_ref()) {
                Some((journal, lhs_offset)) => {
                    self.offsets.insert(journal, lhs_offset.max(*rhs_offset));
                }
                None => {
                    self.offsets
                        .insert(journal.as_ref().to_owned(), *rhs_offset);
                }
            }
        }
    }

    /// Contains is true if the Etcd revision of this Clock is greater or equal to the
    /// revision of the |other|, and if all common journal offsets are also greater or
    /// equal to |other|.
    pub fn contains(&self, other: &Self) -> bool {
        if self.etcd.revision < other.etcd.revision {
            return false;
        }

        for eob in self
            .offsets
            .iter()
            .merge_join_by(other.offsets.iter(), |(l, _), (r, _)| l.cmp(r))
        {
            if matches!(eob, EitherOrBoth::Both((_, l), (_, r)) if l < r) {
                return false;
            }
        }
        true
    }
}

// Equality is implemented (only) to facilitate testing.
impl PartialEq<Clock> for Clock {
    fn eq(&self, other: &Self) -> bool {
        self.etcd.revision == other.etcd.revision && self.offsets == other.offsets
    }
}

impl Eq for Clock {}

impl Debug for Clock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut b = f.debug_struct("Clock");

        // Print only the revision, not the full header.
        b.field("rev", &self.etcd.revision);
        for (j, o) in self.offsets.iter() {
            b.field(&j, o);
        }

        b.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::clock_fixture;

    #[test]
    fn test_clock_reduction_and_ordering() {
        // Two clocks, with overlapping components.
        let c1 = clock_fixture(10, &[("one", 1), ("two", 2), ("three", 3)]);
        let c2 = clock_fixture(20, &[("one", 2), ("two", 1), ("four", 4)]);

        let mut r_min = c1.clone();
        let mut r_max = c1.clone();

        r_min.reduce_min(&c2.etcd, c2.offsets.iter());
        r_max.reduce_max(&c2.etcd, c2.offsets.iter());

        assert_eq!(
            r_min,
            clock_fixture(10, &[("one", 1), ("two", 1), ("three", 3), ("four", 4)]),
        );
        assert_eq!(
            r_max,
            clock_fixture(20, &[("one", 2), ("two", 2), ("three", 3), ("four", 4)]),
        );

        // Verify ordering expectations.
        assert!(r_min == r_min);
        assert!(c1.contains(&r_min));
        assert!(c2.contains(&r_min));

        assert!(r_max == r_max);
        assert!(r_max.contains(&c1));
        assert!(r_max.contains(&c2));

        assert!(!c1.contains(&c2));
        assert!(!c2.contains(&c1));

        assert!(r_min != r_max);
        assert!(r_max.contains(&r_min));
        assert!(!r_min.contains(&r_max));
    }
}
