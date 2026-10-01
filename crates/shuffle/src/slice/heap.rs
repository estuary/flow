use super::read::ReadyRead;
use std::ops::{Deref, DerefMut};

/// ReadyReadEntry holds the merge position of a ReadyRead.
/// We keep this struct small to optimize heap sift operations.
pub struct ReadyReadEntry {
    /// Merge order of the document: its binding priority and adjusted clock.
    pub position: crate::merge::Position,
    /// The actual document data, accessed by pointer indirection.
    /// Always `Some` in normal usage; `Option` allows test construction
    /// without a real ReadyRead (Ord only uses `position`).
    pub inner: Option<Box<ReadyRead>>,
}

/// ReadyReadHeap is a max-heap of ReadyReadEntry. It yields the entry which
/// merges first (see `merge::Position`).
pub struct ReadyReadHeap(std::collections::BinaryHeap<ReadyReadEntry>);

impl ReadyReadHeap {
    pub fn new() -> Self {
        Self(std::collections::BinaryHeap::new())
    }
}

impl Deref for ReadyReadHeap {
    type Target = std::collections::BinaryHeap<ReadyReadEntry>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ReadyReadHeap {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Ord for ReadyReadEntry {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.position.cmp(&self.position)
    }
}

impl PartialOrd for ReadyReadEntry {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ReadyReadEntry {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for ReadyReadEntry {}

#[cfg(test)]
mod test {
    use super::*;
    use proto_gazette::uuid;
    use std::cmp::Ordering;
    use std::collections::BinaryHeap;

    fn test_entry(priority: i32, clock: u64) -> ReadyReadEntry {
        ReadyReadEntry {
            position: crate::merge::Position {
                priority,
                adjusted_clock: uuid::Clock::from_u64(clock),
            },
            inner: None,
        }
    }

    #[test]
    fn test_heap_ordering() {
        // BinaryHeap is a max-heap: pop() returns the greatest element.
        // ReadyReadEntry::Ord should yield Greatest for max priority,
        // then min adjusted_clock (the reverse of merge::Position::Ord).

        assert_eq!(
            test_entry(2, 100).cmp(&test_entry(1, 100)),
            Ordering::Greater,
            "higher priority wins"
        );
        assert_eq!(
            test_entry(1, 50).cmp(&test_entry(1, 100)),
            Ordering::Greater,
            "earlier clock wins at same priority"
        );
        assert_eq!(
            test_entry(1, 100).cmp(&test_entry(1, 100)),
            Ordering::Equal,
            "equal"
        );
        assert_eq!(
            test_entry(0, 100).cmp(&test_entry(-1, 50)),
            Ordering::Greater,
            "default priority beats a negative priority"
        );

        // Verify pop order from a real BinaryHeap.
        let mut heap = BinaryHeap::new();
        heap.push(test_entry(1, 200));
        heap.push(test_entry(2, 100));
        heap.push(test_entry(1, 50));
        heap.push(test_entry(2, 300));
        heap.push(test_entry(-1, 10));

        let pops: Vec<_> = std::iter::from_fn(|| heap.pop())
            .map(|e| (e.position.priority, e.position.adjusted_clock.as_u64()))
            .collect();

        assert_eq!(
            pops,
            vec![
                (2, 100), // high priority, early clock
                (2, 300), // high priority, late clock
                (1, 50),  // low priority, early clock
                (1, 200), // low priority, late clock
                (-1, 10), // negative priority is drained last
            ]
        );
    }
}
