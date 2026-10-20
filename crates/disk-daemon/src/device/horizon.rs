//! Incremental compaction through recovery horizons.
//!
//! A horizon becomes a recovery floor once every allocated block has a committed
//! copy at or after its opening offset. Snapshot allocation at open, then discharge
//! blocks as mutations or copies publish them. Commit of the delta discharging
//! the last block advances the floor.

use super::image;

/// Pending blocks of an open horizon.
pub(super) struct Horizon {
    /// Blocks not yet discharged by a mutation or copy.
    pending: image::Bitmap,
    /// Set bits of `pending`, kept as blocks discharge. Counting them would scan
    /// the whole bitmap, and a device checks after every mutation.
    remaining: u32,
    /// Blocks below this are discharged. A copy scan resumes here, so each
    /// horizon makes one pass.
    cursor: u32,
    /// Changed chunk bytes since the horizon opened, excluding framing, which earn
    /// copy budget.
    changed: u64,
    /// Copied chunk bytes since the horizon opened, excluding framing, which spend
    /// that budget.
    copied: u64,
    /// The disk's replay bytes as the opening chunk began: an earlier reading of
    /// that running total, not a journal offset. Once the horizon completes, that
    /// chunk's record is the floor, and replay bytes count from here.
    replay_bytes_at_open: u64,
}

impl Horizon {
    /// Open a horizon over the blocks which are `allocated` now, where the disk's
    /// replay bytes are `replay_bytes`.
    pub fn open(allocated: &image::Bitmap, replay_bytes: u64) -> Self {
        Self {
            pending: allocated.clone(),
            remaining: allocated.count_ones() as u32,
            cursor: 0,
            changed: 0,
            copied: 0,
            replay_bytes_at_open: replay_bytes,
        }
    }

    /// Blocks still pending a copy for this horizon.
    pub fn pending(&self) -> u32 {
        self.remaining
    }

    /// The replay bytes where this horizon opened.
    pub fn replay_bytes_at_open(&self) -> u64 {
        self.replay_bytes_at_open
    }

    /// Discharge `range`, which a mutation has published at or after the horizon.
    pub fn published(&mut self, range: std::ops::Range<u32>) {
        let bits = &mut self.pending[range.start as usize..range.end as usize];

        self.remaining -= bits.count_ones() as u32;
        bits.fill(false);
    }

    /// Account for `bytes` of changed content, which earns the budget a copy
    /// spends.
    pub fn changed(&mut self, bytes: u64) {
        self.changed += bytes;
    }

    /// The next run of at most `limit` blocks to copy, or `None` when the budget
    /// is spent or the horizon is discharged.
    pub fn next_copy(
        &mut self,
        horizon_policy: &Policy,
        limit: u32,
    ) -> Option<std::ops::Range<u32>> {
        let budget = horizon_policy
            .budget(self.changed)
            .checked_sub(self.copied)
            .expect("a copy never spends more than its budget");
        let limit = std::cmp::min(limit as u64, budget / crate::BLOCK_SIZE as u64) as u32;

        if limit == 0 {
            return None;
        }
        let blocks = self.pending.len() as u32;

        let Some(start) = self.pending[self.cursor as usize..].first_one() else {
            // No bit is ever set again during one horizon. A scan which finds no
            // block therefore leaves the cursor exhausted, so the next call does
            // not sweep the whole bitmap again.
            self.cursor = blocks;
            return None;
        };
        let start = self.cursor + start as u32;
        let mut end = start + 1;

        while end < std::cmp::min(start + limit, blocks) && self.pending[end as usize] {
            end += 1;
        }
        Some(start..end)
    }

    /// Spend `bytes` of budget on a copy of `range`, and resume the scan after it.
    /// The copy discharges `range` once it's published.
    pub fn copied(&mut self, range: std::ops::Range<u32>, bytes: u64) {
        self.copied += bytes;
        self.cursor = range.end;
    }
}

/// When a horizon opens, and how quickly it is discharged.
#[derive(Clone, Copy, Debug)]
pub struct Policy {
    /// Replay bytes, as a multiple of the live allocated size, beyond which a
    /// horizon opens.
    pub open_ratio: f64,
    /// Unchanged bytes a horizon may copy per byte changed while it is open.
    pub copy_ratio: f64,
    /// Replay bytes below which no horizon opens, whatever the ratio says. It keeps a
    /// small disk from compacting constantly.
    pub minimum_bytes: u64,
}

impl Policy {
    /// Whether `replay_bytes` of chunks above the floor open a horizon over a disk
    /// holding `allocated` bytes.
    pub fn opens(&self, replay_bytes: u64, allocated: u64) -> bool {
        let scaled = (allocated as f64 * self.open_ratio) as u64;
        replay_bytes > std::cmp::max(self.minimum_bytes, scaled)
    }

    /// Bytes of unchanged data a horizon over which `changed` bytes changed may
    /// copy.
    fn budget(&self, changed: u64) -> u64 {
        (changed as f64 * self.copy_ratio) as u64
    }
}

#[cfg(test)]
mod test {
    use super::{Horizon, Policy};
    use crate::BLOCK_SIZE;
    use crate::device::image;
    use std::fmt::Write as _;

    /// The shipped policy, which every case varies from.
    fn shipped() -> Policy {
        Policy {
            open_ratio: 2.0,
            copy_ratio: 0.5,
            minimum_bytes: 1 << 30,
        }
    }

    fn allocated(blocks: &[u32]) -> image::Bitmap {
        let mut bits = image::Bitmap::repeat(false, 64);
        for &block in blocks {
            bits.set(block as usize, true);
        }
        bits
    }

    #[derive(Debug, Clone)]
    enum Step {
        /// Changed bytes, which earn copy budget.
        Changed(u64),
        /// Published block range.
        Published(std::ops::Range<u32>),
        /// Limit the next copy to this many blocks.
        NextCopy(u32),
        /// Copied block range and byte cost.
        Copied(std::ops::Range<u32>, u64),
        /// Copy all available runs up to this block limit, charging full blocks.
        Drain(u32),
    }
    use Step::*;

    /// Trace each step's result, pending blocks, scan position, and byte budget.
    fn trace(horizon_policy: &Policy, blocks: &[u32], steps: &[Step]) -> String {
        let mut horizon = Horizon::open(&allocated(blocks), 0);
        let mut out = String::new();
        let mut row = |step: String, outcome: String, horizon: &Horizon| {
            writeln!(
                out,
                "{step:<22}{outcome:<30}pending {:<3}cursor {:<4}changed {:<9}copied {}",
                horizon.pending(),
                horizon.cursor,
                horizon.changed,
                horizon.copied,
            )
            .unwrap();
        };
        row(format!("Open({blocks:?})"), "-".to_string(), &horizon);

        for step in steps {
            let outcome = match step {
                Changed(bytes) => {
                    horizon.changed(*bytes);
                    "-".to_string()
                }
                Published(range) => {
                    horizon.published(range.clone());
                    "-".to_string()
                }
                NextCopy(limit) => format!("{:?}", horizon.next_copy(horizon_policy, *limit)),
                Copied(range, bytes) => {
                    horizon.copied(range.clone(), *bytes);
                    horizon.published(range.clone());
                    "-".to_string()
                }
                Drain(limit) => {
                    let mut runs = Vec::new();
                    while let Some(run) = horizon.next_copy(horizon_policy, *limit) {
                        horizon.copied(run.clone(), run.len() as u64 * BLOCK_SIZE as u64);
                        horizon.published(run.clone());
                        runs.push(run);
                    }
                    format!("copied {runs:?}")
                }
            };
            row(format!("{step:?}"), outcome, &horizon);
        }
        out
    }

    #[test]
    fn test_a_horizon_opens_on_the_larger_of_the_ratio_and_the_minimum() {
        let shipped = shipped();
        // Tiny values reach both terms, so a test can exercise each of them.
        let tiny = Policy {
            open_ratio: 0.5,
            minimum_bytes: 100,
            ..shipped
        };

        let cases: [(&str, Policy, u64, u64); 9] = [
            ("shipped, minimum binds", shipped, 1 << 30, 0),
            ("shipped, minimum binds", shipped, 1 << 30, 1 << 20),
            ("shipped, minimum binds", shipped, (1 << 30) + 1, 1 << 20),
            ("shipped, ratio binds", shipped, 4 << 30, 2 << 30),
            ("shipped, ratio binds", shipped, (4 << 30) + 1, 2 << 30),
            ("tiny, minimum binds", tiny, 100, 0),
            ("tiny, minimum binds", tiny, 101, 0),
            ("tiny, ratio binds", tiny, 150, 400),
            ("tiny, ratio binds", tiny, 201, 400),
        ];
        let table = cases
            .iter()
            .map(|(label, horizon_policy, replay_bytes, allocated)| {
                format!(
                    "{label:<24}replay {replay_bytes:<12}allocated {allocated:<12}opens {}",
                    horizon_policy.opens(*replay_bytes, *allocated),
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        insta::assert_snapshot!(table, @"
        shipped, minimum binds  replay 1073741824  allocated 0           opens false
        shipped, minimum binds  replay 1073741824  allocated 1048576     opens false
        shipped, minimum binds  replay 1073741825  allocated 1048576     opens true
        shipped, ratio binds    replay 4294967296  allocated 2147483648  opens false
        shipped, ratio binds    replay 4294967297  allocated 2147483648  opens true
        tiny, minimum binds     replay 100         allocated 0           opens false
        tiny, minimum binds     replay 101         allocated 0           opens true
        tiny, ratio binds       replay 150         allocated 400         opens false
        tiny, ratio binds       replay 201         allocated 400         opens true
        ");
    }

    #[test]
    fn test_a_horizon_copies_in_proportion_to_what_changed() {
        let trace = trace(
            &shipped(),
            &[1, 2, 3, 40],
            &[
                NextCopy(8),
                // At a 0.5 copy ratio, two changed blocks buy one copied block.
                Changed(BLOCK_SIZE as u64),
                NextCopy(8),
                Changed(BLOCK_SIZE as u64),
                NextCopy(8),
                Copied(1..2, BLOCK_SIZE as u64),
                NextCopy(8),
                // A run stops at the limit, at a hole, and at the budget.
                Changed(16 * BLOCK_SIZE as u64),
                NextCopy(8),
                Copied(2..4, 2 * BLOCK_SIZE as u64),
                NextCopy(1),
            ],
        );
        insta::assert_snapshot!(trace, @"
        Open([1, 2, 3, 40])   -                             pending 4  cursor 0   changed 0        copied 0
        NextCopy(8)           None                          pending 4  cursor 0   changed 0        copied 0
        Changed(4096)         -                             pending 4  cursor 0   changed 4096     copied 0
        NextCopy(8)           None                          pending 4  cursor 0   changed 4096     copied 0
        Changed(4096)         -                             pending 4  cursor 0   changed 8192     copied 0
        NextCopy(8)           Some(1..2)                    pending 4  cursor 0   changed 8192     copied 0
        Copied(1..2, 4096)    -                             pending 3  cursor 2   changed 8192     copied 4096
        NextCopy(8)           None                          pending 3  cursor 2   changed 8192     copied 4096
        Changed(65536)        -                             pending 3  cursor 2   changed 73728    copied 4096
        NextCopy(8)           Some(2..4)                    pending 3  cursor 2   changed 73728    copied 4096
        Copied(2..4, 8192)    -                             pending 1  cursor 4   changed 73728    copied 12288
        NextCopy(1)           Some(40..41)                  pending 1  cursor 4   changed 73728    copied 12288
        ");
    }

    /// A block the device writes is published like any other. A delta which
    /// rewrites the disk therefore discharges the horizon without any copy.
    #[test]
    fn test_a_rewrite_discharges_without_copying() {
        let trace = trace(
            &shipped(),
            &[1, 2, 3, 40],
            &[
                Changed(1 << 20),
                Published(0..4),
                Published(40..41),
                NextCopy(8),
            ],
        );
        insta::assert_snapshot!(trace, @"
        Open([1, 2, 3, 40])   -                             pending 4  cursor 0   changed 0        copied 0
        Changed(1048576)      -                             pending 4  cursor 0   changed 1048576  copied 0
        Published(0..4)       -                             pending 1  cursor 0   changed 1048576  copied 0
        Published(40..41)     -                             pending 0  cursor 0   changed 1048576  copied 0
        NextCopy(8)           None                          pending 0  cursor 64  changed 1048576  copied 0
        ");
    }

    /// The cursor only moves forward. A horizon therefore costs one pass over
    /// its bitmap, however many writes discharge it.
    #[test]
    fn test_the_copy_scan_makes_one_pass() {
        let horizon_policy = Policy {
            copy_ratio: 1.0,
            ..shipped()
        };
        let trace = trace(
            &horizon_policy,
            &[0, 5, 6, 63],
            &[Changed(1 << 20), Drain(2)],
        );

        insta::assert_snapshot!(trace, @"
        Open([0, 5, 6, 63])   -                             pending 4  cursor 0   changed 0        copied 0
        Changed(1048576)      -                             pending 4  cursor 0   changed 1048576  copied 0
        Drain(2)              copied [0..1, 5..7, 63..64]   pending 0  cursor 64  changed 1048576  copied 16384
        ");
    }
}
