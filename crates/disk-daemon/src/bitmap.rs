//! Fixed-size bit sets over block indices.
//!
//! A disk has two. The allocated bitmap holds the blocks which occupy space in
//! the local image. The horizon bitmap holds the allocated blocks whose newest
//! durable copy is older than the active recovery horizon. Both are indexed by
//! block, so they are the same shape.

/// Bitmap is a set of block indices in `[0, blocks)`, backed by `u64` words.
///
/// The words are plain integers rather than atomics. Only a disk's owner thread
/// mutates its bitmaps.
///
/// An index outside `[0, blocks)` panics. Block indices come from the daemon's
/// own arithmetic over a device size it chose. [`crate::chunk::apply`]
/// range-checks a chunk decoded from a journal before it reaches a bitmap.
#[derive(Clone, PartialEq, Eq)]
pub struct Bitmap {
    words: Vec<u64>,
    blocks: u32,
}

impl Bitmap {
    /// Create an empty bitmap covering `blocks` block indices.
    pub fn new(blocks: u32) -> Self {
        let words = (blocks as usize).div_ceil(u64::BITS as usize);
        Self {
            words: vec![0; words],
            blocks,
        }
    }

    /// Number of block indices this bitmap covers.
    pub fn blocks(&self) -> u32 {
        self.blocks
    }

    pub fn set(&mut self, block: u32) {
        let (word, bit) = self.locate(block);
        self.words[word] |= 1 << bit;
    }

    pub fn clear(&mut self, block: u32) {
        let (word, bit) = self.locate(block);
        self.words[word] &= !(1 << bit);
    }

    pub fn test(&self, block: u32) -> bool {
        let (word, bit) = self.locate(block);
        self.words[word] & (1 << bit) != 0
    }

    /// Count of set bits. For the allocated bitmap this is the disk's live
    /// physical size in blocks. Compaction policy compares that size against the
    /// journal's recovery range.
    pub fn count_ones(&self) -> u32 {
        self.words.iter().map(|w| w.count_ones()).sum()
    }

    /// Index of the lowest set bit at or after `cursor`, or `None` if there is
    /// none. `cursor` may equal `blocks`, which is the exhausted cursor.
    pub fn first_set_at_or_after(&self, cursor: u32) -> Option<u32> {
        assert!(
            cursor <= self.blocks,
            "cursor {cursor} exceeds bitmap length {}",
            self.blocks
        );
        if cursor == self.blocks {
            return None;
        }
        let (word, bit) = self.locate(cursor);

        // Mask off the bits below the cursor within its own word, then scan
        // whole words. No bit beyond `blocks` is ever set, so the final word
        // needs no mask.
        let mut masked = self.words[word] & (u64::MAX << bit);
        for index in word..self.words.len() {
            if masked != 0 {
                return Some((index as u32) * u64::BITS + masked.trailing_zeros());
            }
            masked = *self.words.get(index + 1).unwrap_or(&0);
        }
        None
    }

    /// Iterate set bits in increasing order.
    pub fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        std::iter::successors(self.first_set_at_or_after(0), |prev| {
            self.first_set_at_or_after(prev + 1)
        })
    }

    fn locate(&self, block: u32) -> (usize, u32) {
        assert!(
            block < self.blocks,
            "block {block} exceeds bitmap length {}",
            self.blocks
        );
        ((block / u64::BITS) as usize, block % u64::BITS)
    }
}

impl std::fmt::Debug for Bitmap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Bitmap({} of {} set: ", self.count_ones(), self.blocks)?;
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

    #[test]
    fn test_debug_rendering() {
        let mut bits = Bitmap::new(16);
        bits.set(2);
        bits.set(11);
        assert_eq!(format!("{bits:?}"), "Bitmap(2 of 16 set: [2, 11])");
    }

    #[test]
    #[should_panic(expected = "block 16 exceeds bitmap length 16")]
    fn test_out_of_range_set_panics() {
        Bitmap::new(16).set(16);
    }

    #[test]
    #[should_panic(expected = "cursor 17 exceeds bitmap length 16")]
    fn test_out_of_range_cursor_panics() {
        _ = Bitmap::new(16).first_set_at_or_after(17);
    }
}
