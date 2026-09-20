//! Fixed-size bit sets over block indices.
//!
//! A disk has two. [`crate::image::Image`] holds the allocated bitmap, of the
//! blocks which occupy space in the local image. [`crate::horizon::Horizon`] holds
//! the other, of the allocated blocks whose newest durable copy is older than the
//! recovery horizon which is open. Both are indexed by block, so they are the same
//! shape.

/// Bitmap is a set of block indices in `[0, blocks)`.
///
/// It stores into plain `u64` words rather than atomics, because only a disk's
/// owner thread mutates its bitmaps.
///
/// An index outside `[0, blocks)` panics. Block indices come from the daemon's
/// own arithmetic over a device size it chose. [`crate::chunk::apply`]
/// range-checks a chunk decoded from a journal before it reaches a bitmap.
#[derive(Clone, PartialEq, Eq)]
pub struct Bitmap(bitvec::vec::BitVec<u64, bitvec::order::Lsb0>);

impl Bitmap {
    /// Create an empty bitmap covering `blocks` block indices.
    pub fn new(blocks: u32) -> Self {
        Self(bitvec::vec::BitVec::repeat(false, blocks as usize))
    }

    /// Number of block indices this bitmap covers.
    pub fn blocks(&self) -> u32 {
        self.0.len() as u32
    }

    /// Serving code sets and clears whole ranges, so only a case builds a bitmap
    /// one block at a time.
    #[cfg(test)]
    pub fn set(&mut self, block: u32) {
        self.0.set(block as usize, true);
    }

    #[cfg(test)]
    pub fn clear(&mut self, block: u32) {
        self.0.set(block as usize, false);
    }

    pub fn set_range(&mut self, range: std::ops::Range<u32>) {
        self.0[range.start as usize..range.end as usize].fill(true);
    }

    pub fn clear_range(&mut self, range: std::ops::Range<u32>) {
        self.0[range.start as usize..range.end as usize].fill(false);
    }

    pub fn test(&self, block: u32) -> bool {
        self.0[block as usize]
    }

    /// Count of set bits. For the allocated bitmap this is the disk's live
    /// physical size in blocks. Compaction policy compares that size against the
    /// journal's recovery range.
    pub fn count_ones(&self) -> u32 {
        self.0.count_ones() as u32
    }

    /// Index of the lowest set bit at or after `cursor`, or `None` if there is
    /// none. `cursor` may equal `blocks`, which is the exhausted cursor.
    pub fn first_set_at_or_after(&self, cursor: u32) -> Option<u32> {
        let cursor = cursor as usize;
        self.0[cursor..].first_one().map(|at| (cursor + at) as u32)
    }

    /// Iterate set bits in increasing order.
    pub fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.0.iter_ones().map(|block| block as u32)
    }
}

impl std::fmt::Debug for Bitmap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Bitmap({} of {} set: ", self.count_ones(), self.blocks())?;
        f.debug_list().entries(self.iter()).finish()?;
        write!(f, ")")
    }
}

#[cfg(test)]
mod test {
    use super::Bitmap;

    #[test]
    fn test_set_clear_and_scan() {
        let mut bits = Bitmap::new(200);
        assert_eq!(bits.blocks(), 200);
        assert_eq!(bits.count_ones(), 0);
        assert_eq!(bits.first_set_at_or_after(0), None);

        // Bits spanning several words, including word boundaries.
        for block in [0, 1, 63, 64, 65, 127, 128, 199] {
            bits.set(block);
        }
        assert_eq!(bits.count_ones(), 8);
        assert_eq!(
            bits.iter().collect::<Vec<_>>(),
            vec![0, 1, 63, 64, 65, 127, 128, 199]
        );

        assert!(bits.test(63));
        assert!(!bits.test(62));

        // Scans start at, and skip over, arbitrary cursors.
        assert_eq!(bits.first_set_at_or_after(0), Some(0));
        assert_eq!(bits.first_set_at_or_after(1), Some(1));
        assert_eq!(bits.first_set_at_or_after(2), Some(63));
        assert_eq!(bits.first_set_at_or_after(63), Some(63));
        assert_eq!(bits.first_set_at_or_after(66), Some(127));
        assert_eq!(bits.first_set_at_or_after(129), Some(199));
        assert_eq!(bits.first_set_at_or_after(200), None);

        // Setting an already-set bit and clearing a clear bit are both no-ops.
        bits.set(63);
        bits.clear(62);
        assert_eq!(bits.count_ones(), 8);

        bits.clear(63);
        bits.clear(199);
        assert_eq!(bits.count_ones(), 6);
        assert_eq!(bits.first_set_at_or_after(2), Some(64));
        assert_eq!(bits.first_set_at_or_after(129), None);
    }

    /// A range spans whole words and the partial words at either end.
    #[test]
    fn test_ranges_are_set_and_cleared() {
        let mut bits = Bitmap::new(200);

        bits.set_range(62..130);
        assert_eq!(
            bits.iter().collect::<Vec<_>>(),
            (62..130).collect::<Vec<_>>()
        );

        bits.clear_range(63..129);
        assert_eq!(bits.iter().collect::<Vec<_>>(), vec![62, 129]);
    }

    #[test]
    fn test_debug_rendering() {
        let mut bits = Bitmap::new(16);
        bits.set(2);
        bits.set(11);
        assert_eq!(format!("{bits:?}"), "Bitmap(2 of 16 set: [2, 11])");
    }

    #[test]
    #[should_panic]
    fn test_out_of_range_set_panics() {
        Bitmap::new(16).set(16);
    }

    #[test]
    #[should_panic]
    fn test_out_of_range_cursor_panics() {
        _ = Bitmap::new(16).first_set_at_or_after(17);
    }
}
