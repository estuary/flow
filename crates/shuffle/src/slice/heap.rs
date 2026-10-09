use super::read::ReadyRead;
use proto_gazette::uuid;
use std::ops::{Deref, DerefMut};

/// ReadyReadEntry holds the ordering fields for a ReadyRead.
/// We keep this struct small to optimize heap sift operations.
pub struct ReadyReadEntry {
    /// Adjusted clock of the document (publication + read_delay).
    pub adjusted_clock: uuid::Clock,
    /// The actual document data, accessed by pointer indirection.
    /// Always `Some` in normal usage; `Option` allows test construction
    /// without a real ReadyRead (Ord only uses `adjusted_clock`).
    pub inner: Option<Box<ReadyRead>>,
}

/// ReadyReadHeap is a max-heap of ReadyReadEntry, which yields the entry
/// having the minimum adjusted clock.
///
/// It's not ordered by priority: a Slice reads only bindings of its lane's
/// priority. Were it ordered by priority, a higher-priority document awaiting
/// its read delay would block lesser-priority documents which are due.
/// Instead, its top is always the first document to come due.
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
        self.adjusted_clock.cmp(&other.adjusted_clock).reverse()
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
    use std::collections::BinaryHeap;

    #[test]
    fn test_heap_ordering() {
        // BinaryHeap is a max-heap: pop() returns the greatest element,
        // which ReadyReadEntry::Ord makes the least adjusted clock.
        let mut heap = BinaryHeap::new();
        for clock in [200, 100, 50, 300, 100] {
            heap.push(ReadyReadEntry {
                adjusted_clock: uuid::Clock::from_u64(clock),
                inner: None,
            });
        }
        let pops: Vec<_> = std::iter::from_fn(|| heap.pop())
            .map(|e| e.adjusted_clock.as_u64())
            .collect();

        assert_eq!(pops, vec![50, 100, 100, 200, 300]);
    }
}
