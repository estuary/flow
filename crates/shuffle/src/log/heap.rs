use proto_gazette::uuid;
use std::ops::{Deref, DerefMut};

/// AppendEntry holds the ordering fields for a pending Append and
/// the index of the slice that sent it. The actual Append and the slice's rx
/// stream are stored in `LogActor::slice_appends` indexed by `shard_index`.
/// We keep this struct small to optimize heap sift operations.
pub struct AppendEntry {
    /// Binding priority (higher = more urgent).
    pub priority: u32,
    /// Adjusted clock of the document (publication + read_delay).
    pub adjusted_clock: uuid::Clock,
    /// Index of the Slice shard that sent this Append.
    pub shard_index: usize,
}

/// AppendHeap is a max-heap of AppendEntry. It yields the entry having
/// - Maximum priority, or (if equal)
/// - Minimum adjusted_clock
pub struct AppendHeap(std::collections::BinaryHeap<AppendEntry>);

impl AppendHeap {
    pub fn new() -> Self {
        Self(std::collections::BinaryHeap::new())
    }
}

impl Deref for AppendHeap {
    type Target = std::collections::BinaryHeap<AppendEntry>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for AppendHeap {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Ord for AppendEntry {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority
            .cmp(&other.priority)
            .then(self.adjusted_clock.cmp(&other.adjusted_clock).reverse())
    }
}

impl PartialOrd for AppendEntry {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for AppendEntry {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for AppendEntry {}

#[cfg(test)]
mod test {
    use super::*;
    use std::cmp::Ordering;
    use std::collections::BinaryHeap;

    fn test_entry(priority: u32, clock: u64, shard_index: usize) -> AppendEntry {
        AppendEntry {
            priority,
            adjusted_clock: uuid::Clock::from_u64(clock),
            shard_index,
        }
    }

    #[test]
    fn test_ordering() {
        // BinaryHeap is a max-heap: pop() returns the greatest element.
        // AppendEntry::Ord should yield Greatest for max priority,
        // then min adjusted_clock (via .reverse()).

        assert_eq!(
            test_entry(2, 100, 0).cmp(&test_entry(1, 100, 0)),
            Ordering::Greater,
            "higher priority wins"
        );
        assert_eq!(
            test_entry(1, 50, 0).cmp(&test_entry(1, 100, 0)),
            Ordering::Greater,
            "earlier clock wins at same priority"
        );
        assert_eq!(
            test_entry(1, 100, 0).cmp(&test_entry(1, 100, 1)),
            Ordering::Equal,
            "shard_index does not affect ordering"
        );
        assert_eq!(
            test_entry(1, 100, 0).cmp(&test_entry(2, 50, 0)),
            Ordering::Less,
            "priority takes precedence over clock"
        );
    }

    #[test]
    fn test_heap_pop_order() {
        let mut heap = BinaryHeap::new();
        heap.push(test_entry(1, 200, 0));
        heap.push(test_entry(2, 100, 1));
        heap.push(test_entry(1, 50, 2));
        heap.push(test_entry(2, 300, 3));
        heap.push(test_entry(1, 50, 4)); // same priority+clock as shard 2

        let pops: Vec<_> = std::iter::from_fn(|| heap.pop())
            .map(|e| (e.priority, e.adjusted_clock.as_u64(), e.shard_index))
            .collect();

        assert_eq!(
            pops,
            vec![
                (2, 100, 1), // high priority, early clock
                (2, 300, 3), // high priority, late clock
                (1, 50, 2),  // low priority, early clock (either shard 2 or 4)
                (1, 50, 4),  // low priority, early clock (the other)
                (1, 200, 0), // low priority, late clock
            ]
        );
    }

    #[test]
    fn test_append_heap_wrapper() {
        let mut heap = AppendHeap::new();
        assert!(heap.is_empty());

        heap.push(test_entry(1, 100, 0));
        heap.push(test_entry(2, 50, 1));
        assert_eq!(heap.len(), 2);

        let top = heap.pop().unwrap();
        assert_eq!(top.priority, 2);
        assert_eq!(top.shard_index, 1);

        let next = heap.pop().unwrap();
        assert_eq!(next.priority, 1);
        assert_eq!(next.shard_index, 0);

        assert!(heap.pop().is_none());
    }
}
