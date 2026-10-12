//! Incremental compaction through recovery horizons.
//!
//! A horizon becomes a recovery floor once every allocated block has a committed
//! copy at or after its opening offset. Snapshot allocation at open, then discharge
//! blocks as mutations or copies publish them. Commit of the delta discharging
//! the last block advances the floor.

use crate::image;

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

/// Pending blocks of an open horizon.
pub struct Horizon {
    /// Blocks not yet discharged by a mutation or copy.
    pending: image::Bitmap,
    /// Blocks below this are discharged. A copy scan resumes here, so each
    /// horizon makes one pass.
    cursor: u32,
    /// Changed chunk bytes in this delta, excluding framing, which earn copy budget.
    changed: u64,
    /// Copied chunk bytes in this delta, excluding framing, which spend that budget.
    copied: u64,
}

impl Horizon {
    /// Open a horizon over the blocks which are `allocated` now.
    pub fn open(allocated: &image::Bitmap) -> Self {
        Self {
            pending: allocated.clone(),
            cursor: 0,
            changed: 0,
            copied: 0,
        }
    }

    /// Blocks still pending a copy for this horizon.
    pub fn pending(&self) -> u32 {
        self.pending.count_ones() as u32
    }

    /// Discharge `range`, which a delta has published at or after the horizon.
    pub fn published(&mut self, range: std::ops::Range<u32>) {
        self.pending[range.start as usize..range.end as usize].fill(false);
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
    use crate::image;
    use std::fmt::Write as _;

    /// The shipped policy, which every case varies from.
    fn policy() -> Policy {
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
        Cut,
    }
    use Step::*;

    /// Trace each step's result, pending blocks, scan position, and byte budget.
    fn trace(policy: &Policy, blocks: &[u32], steps: &[Step]) -> String {
        let mut horizon = Horizon::open(&allocated(blocks));
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
                NextCopy(limit) => format!("{:?}", horizon.next_copy(policy, *limit)),
                Copied(range, bytes) => {
                    horizon.copied(range.clone(), *bytes);
                    "-".to_string()
                }
                Drain(limit) => {
                    let mut runs = Vec::new();
                    while let Some(run) = horizon.next_copy(policy, *limit) {
                        horizon.copied(run.clone(), run.len() as u64 * BLOCK_SIZE as u64);
                        runs.push(run);
                    }
                    format!("copied {runs:?}")
                }
                Cut => {
                    horizon.cut();
                    "-".to_string()
                }
            };
            row(format!("{step:?}"), outcome, &horizon);
        }
        out
    }

    #[test]
    fn test_a_horizon_opens_on_the_larger_of_the_ratio_and_the_minimum() {
        let policy = policy();
        // Tiny values reach both terms, so a test can exercise each of them.
        let tiny = Policy {
            open_ratio: 0.5,
            minimum_bytes: 100,
            ..policy
        };

        let cases: [(&str, Policy, u64, u64); 9] = [
            ("shipped, minimum binds", policy, 1 << 30, 0),
            ("shipped, minimum binds", policy, 1 << 30, 1 << 20),
            ("shipped, minimum binds", policy, (1 << 30) + 1, 1 << 20),
            ("shipped, ratio binds", policy, 4 << 30, 2 << 30),
            ("shipped, ratio binds", policy, (4 << 30) + 1, 2 << 30),
            ("tiny, minimum binds", tiny, 100, 0),
            ("tiny, minimum binds", tiny, 101, 0),
            ("tiny, ratio binds", tiny, 150, 400),
            ("tiny, ratio binds", tiny, 201, 400),
        ];
        let table = cases
            .iter()
            .map(|(label, policy, range, allocated)| {
                format!(
                    "{label:<24}range {range:<12}allocated {allocated:<12}opens {}",
                    policy.opens(*range, *allocated),
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        insta::assert_snapshot!(table, @"
        shipped, minimum binds  range 1073741824  allocated 0           opens false
        shipped, minimum binds  range 1073741824  allocated 1048576     opens false
        shipped, minimum binds  range 1073741825  allocated 1048576     opens true
        shipped, ratio binds    range 4294967296  allocated 2147483648  opens false
        shipped, ratio binds    range 4294967297  allocated 2147483648  opens true
        tiny, minimum binds     range 100         allocated 0           opens false
        tiny, minimum binds     range 101         allocated 0           opens true
        tiny, ratio binds       range 150         allocated 400         opens false
        tiny, ratio binds       range 201         allocated 400         opens true
        ");
    }

    #[test]
    fn test_a_delta_copies_in_proportion_to_what_it_changed() {
        let trace = trace(
            &policy(),
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
                // Unspent copy budget does not carry into the next delta.
                Cut,
                NextCopy(8),
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
        Cut                   -                             pending 1  cursor 4   changed 0        copied 0
        NextCopy(8)           None                          pending 1  cursor 4   changed 0        copied 0
        ");
    }

    /// A block the device writes is published like any other. A delta which
    /// rewrites the disk therefore discharges the horizon without any copy.
    #[test]
    fn test_a_rewrite_discharges_without_copying() {
        let trace = trace(
            &policy(),
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
    /// its bitmap, however many deltas discharge it.
    #[test]
    fn test_the_copy_scan_makes_one_pass() {
        let policy = Policy {
            copy_ratio: 1.0,
            ..policy()
        };
        let trace = trace(&policy, &[0, 5, 6, 63], &[Changed(1 << 20), Drain(2)]);

        insta::assert_snapshot!(trace, @"
        Open([0, 5, 6, 63])   -                             pending 4  cursor 0   changed 0        copied 0
        Changed(1048576)      -                             pending 4  cursor 0   changed 1048576  copied 0
        Drain(2)              copied [0..1, 5..7, 63..64]   pending 0  cursor 64  changed 1048576  copied 16384
        ");
    }
}
