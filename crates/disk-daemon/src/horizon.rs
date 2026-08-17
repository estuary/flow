//! Recovery horizons, which keep a replay bounded.
//!
//! A horizon is a journal position with one property:
//!
//! > every allocated block has a committed copy at or after it.
//!
//! Once that property holds, a replay may begin there, and everything below it
//! is dead weight. A horizon opens when the range a replay would read has
//! outgrown the disk itself. It then snapshots the blocks allocated at that
//! moment. The first delta to publish a block discharges it. The device may have
//! changed that block, or the daemon may have copied it out of the image for this
//! purpose alone. The delta which discharges the last block moves the floor to
//! where the horizon opened.
//!
//! Copying is the cost, so it is rationed. A delta may copy only in proportion
//! to what it changed. This bounds journal write amplification during compaction
//! at `1 + copy_ratio`. A disk nothing writes therefore copies nothing. Its
//! journal stops growing at the same time, so an open horizon simply pauses.
//!
//! A block is discharged when its chunks are captured. That is ahead of the
//! image write which applies them, and ahead of the commit of the delta which
//! carries them. A copy therefore never races a mutation of the same block. The
//! mutation supersedes the copy, because it discharges the block before its own
//! value reaches the image. This early discharge cannot be observed. A delta
//! which does not commit ends its session, and this state does not outlive one.

use crate::bitmap::Bitmap;
use proto_gazette::uuid;

/// When a horizon opens, and how quickly it is discharged.
#[derive(Clone, Copy, Debug)]
pub struct Policy {
    /// Journal range above the floor, as a multiple of the live allocated size,
    /// beyond which a horizon opens.
    pub open_ratio: f64,
    /// Unchanged bytes a delta may copy per byte it changed.
    pub copy_ratio: f64,
    /// Range below which no horizon opens, whatever the ratio says. It keeps a
    /// small disk from compacting constantly.
    pub minimum_bytes: u64,
}

impl Policy {
    /// Whether a journal `range` of bytes above the floor opens a horizon over
    /// a disk holding `allocated` bytes.
    pub fn opens(&self, range: u64, allocated: u64) -> bool {
        let scaled = (allocated as f64 * self.open_ratio) as u64;
        range > std::cmp::max(self.minimum_bytes, scaled)
    }

    /// Bytes of unchanged data a delta which changed `changed` bytes may copy.
    pub fn budget(&self, changed: u64) -> u64 {
        (changed as f64 * self.copy_ratio) as u64
    }
}

/// Where a horizon sits in its journal.
#[derive(Clone, Copy, Debug)]
pub struct Position {
    /// Offset of the record which opened it, which is where a replay may begin.
    pub offset: i64,
    /// Clock of that record. This is the floor a session reports to its client,
    /// and which the client hands back to seek a later replay.
    pub clock: uuid::Clock,
}

/// An open horizon, holding the blocks which still owe it a copy.
///
/// Both the writer and a replay hold one, and the two agree because they
/// discharge the same blocks. The writer snapshots live allocation, and a replay
/// snapshots committed allocation. The two differ only by the mutations of the
/// opening delta which the writer had already applied. That same delta publishes
/// those mutations. The writer's set is therefore a superset which converges once
/// the delta commits, and a writer never completes a horizon before a replay of
/// the same journal would.
pub struct Horizon {
    pending: Bitmap,
    /// Blocks below this are discharged. A copy scan resumes here, so each
    /// horizon makes one pass.
    cursor: u32,
    /// Chunk bytes this delta has changed and copied, which ration the next
    /// copy. Neither counts its framing.
    changed: u64,
    copied: u64,
}

impl Horizon {
    /// Open a horizon over the blocks which are `allocated` now.
    pub fn open(allocated: &Bitmap) -> Self {
        Self {
            pending: allocated.clone(),
            cursor: 0,
            changed: 0,
            copied: 0,
        }
    }

    /// Blocks which still owe this horizon a copy.
    pub fn pending(&self) -> u32 {
        self.pending.count_ones()
    }

    /// Discharge `range`, which a delta has published at or after the horizon.
    pub fn published(&mut self, range: std::ops::Range<u32>) {
        for block in range {
            self.pending.clear(block);
        }
    }

    /// Account for `bytes` of changed content, which earns the budget a copy
    /// spends.
    pub fn changed(&mut self, bytes: u64) {
        self.changed += bytes;
    }

    /// Forget the budget of a delta which has ended. Each delta earns its own
    /// budget, and an unspent one must not accumulate into a burst.
    pub fn cut(&mut self) {
        self.changed = 0;
        self.copied = 0;
    }

    /// The next run of at most `limit` blocks to copy, or `None` when this
    /// delta's budget is spent or the horizon is discharged.
    pub fn next_copy(&mut self, policy: &Policy, limit: u32) -> Option<std::ops::Range<u32>> {
        let budget = policy.budget(self.changed).saturating_sub(self.copied);
        let limit = std::cmp::min(limit as u64, budget / crate::BLOCK_SIZE as u64) as u32;

        if limit == 0 {
            return None;
        }
        let Some(start) = self.pending.first_set_at_or_after(self.cursor) else {
            // No bit is ever set again during one horizon. A scan which finds no
            // block therefore leaves the cursor exhausted, so the next call does
            // not sweep the whole bitmap again.
            self.cursor = self.pending.blocks();
            return None;
        };
        let mut end = start + 1;

        while end < std::cmp::min(start + limit, self.pending.blocks()) && self.pending.test(end) {
            end += 1;
        }
        Some(start..end)
    }

    /// Discharge `range`, which has been copied at a cost of `bytes`.
    pub fn copied(&mut self, range: std::ops::Range<u32>, bytes: u64) {
        self.copied += bytes;
        self.cursor = range.end;
        self.published(range);
    }
}

#[cfg(test)]
mod test {
    use super::{Horizon, Policy};
    use crate::BLOCK_SIZE;
    use crate::bitmap::Bitmap;

    /// The shipped policy, which every case varies from.
    fn policy() -> Policy {
        Policy {
            open_ratio: 2.0,
            copy_ratio: 0.5,
            minimum_bytes: 1 << 30,
        }
    }

    fn allocated(blocks: &[u32]) -> Bitmap {
        let mut bits = Bitmap::new(64);
        for &block in blocks {
            bits.set(block);
        }
        bits
    }

    #[test]
    fn test_a_horizon_opens_on_the_larger_of_the_ratio_and_the_minimum() {
        let policy = policy();

        // A small disk is held back by the minimum, whatever it holds.
        assert!(!policy.opens(1 << 30, 0));
        assert!(!policy.opens(1 << 30, 1 << 20));
        assert!(policy.opens((1 << 30) + 1, 1 << 20));

        // A large one is held back by the ratio instead.
        assert!(!policy.opens(4 << 30, 2 << 30));
        assert!(policy.opens((4 << 30) + 1, 2 << 30));

        // Tiny values reach both terms, so a test can exercise each of them.
        let tiny = Policy {
            open_ratio: 0.5,
            minimum_bytes: 100,
            ..policy
        };
        assert!(!tiny.opens(100, 0));
        assert!(tiny.opens(101, 0));
        assert!(!tiny.opens(150, 400));
        assert!(tiny.opens(201, 400));
    }

    #[test]
    fn test_a_delta_copies_in_proportion_to_what_it_changed() {
        let policy = policy();
        let mut horizon = Horizon::open(&allocated(&[1, 2, 3, 40]));

        assert_eq!(horizon.pending(), 4);
        assert_eq!(horizon.next_copy(&policy, 8), None);

        // Half a block of change buys nothing. A whole block buys one copy.
        horizon.changed(BLOCK_SIZE as u64);
        assert_eq!(horizon.next_copy(&policy, 8), None);

        horizon.changed(BLOCK_SIZE as u64);
        assert_eq!(horizon.next_copy(&policy, 8), Some(1..2));

        horizon.copied(1..2, BLOCK_SIZE as u64);
        assert_eq!(horizon.pending(), 3);
        assert_eq!(horizon.next_copy(&policy, 8), None);

        // A run stops at the limit, at a hole, and at the budget.
        horizon.changed(16 * BLOCK_SIZE as u64);
        assert_eq!(horizon.next_copy(&policy, 8), Some(2..4));

        horizon.copied(2..4, 2 * BLOCK_SIZE as u64);
        assert_eq!(horizon.next_copy(&policy, 1), Some(40..41));

        // The delta ends. Its unspent budget does not carry into the next one.
        horizon.cut();
        assert_eq!(horizon.next_copy(&policy, 8), None);
    }

    /// A block the device writes is published like any other. A delta which
    /// rewrites the disk therefore discharges the horizon without any copy.
    #[test]
    fn test_a_rewrite_discharges_without_copying() {
        let policy = policy();
        let mut horizon = Horizon::open(&allocated(&[1, 2, 3, 40]));

        horizon.changed(1 << 20);
        horizon.published(0..4);
        horizon.published(40..41);

        assert_eq!(horizon.pending(), 0);
        assert_eq!(horizon.next_copy(&policy, 8), None);
    }

    /// The cursor only moves forward. A horizon therefore costs one pass over
    /// its bitmap, however many deltas discharge it.
    #[test]
    fn test_the_copy_scan_makes_one_pass() {
        let policy = Policy {
            copy_ratio: 1.0,
            ..policy()
        };
        let mut horizon = Horizon::open(&allocated(&[0, 5, 6, 63]));
        horizon.changed(1 << 20);

        let mut runs = Vec::new();
        while let Some(run) = horizon.next_copy(&policy, 2) {
            horizon.copied(run.clone(), run.len() as u64 * BLOCK_SIZE as u64);
            runs.push(run);
        }

        assert_eq!(runs, vec![0..1, 5..7, 63..64]);
        assert_eq!(horizon.pending(), 0);
        assert_eq!(horizon.cursor, 64);
    }
}
