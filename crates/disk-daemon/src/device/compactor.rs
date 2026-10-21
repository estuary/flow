use super::ublk;
use crate::proto;

/// The most blocks one horizon copy covers. A copy is applied and replayed like
/// a device write, so it is capped at the largest device request.
const COPY_BLOCKS: u32 = ublk::MAX_IO_BUF_BYTES / crate::BLOCK_SIZE;

/// Decides when a disk compacts and which blocks it copies.
pub(super) struct Compactor {
    /// Blocks which hold data.
    allocated: Bitmap,
    /// The horizon this disk is discharging, held only while one is open.
    horizon: Option<Horizon>,
    /// Encoded chunk bytes applied since the recovery floor.
    bytes_since_floor: u64,
    /// Horizon opening thresholds and copy budget.
    horizon_policy: Policy,
}

impl Compactor {
    pub fn new(size_blocks: u32, horizon_policy: Policy) -> Self {
        Self {
            allocated: Bitmap::new(size_blocks),
            horizon: None,
            bytes_since_floor: 0,
            horizon_policy,
        }
    }

    /// The next range of the open horizon's pending blocks due to be copied out
    /// of the image. Hand the result to `on_copied`.
    pub fn next_copy_range(&mut self) -> Option<std::ops::Range<u32>> {
        self.horizon.as_mut()?.next_copy_range(&self.horizon_policy)
    }

    /// Account for `copy`, a write of the range `next_copy_range` returned.
    pub fn on_copied(&mut self, copy: proto::Chunk) -> super::LiveChunk {
        let discharges_horizon = self.apply(&copy, false);

        super::LiveChunk {
            chunk: copy,
            opens_horizon: false,
            discharges_horizon,
        }
    }

    /// Account for a chunk the journal replayed.
    pub fn on_replayed(&mut self, chunk: proto::Chunk, opens_horizon: bool) {
        _ = self.apply(&chunk, opens_horizon);
    }

    /// Account for a served write or punch.
    pub fn on_served(&mut self, chunk: proto::Chunk) -> super::LiveChunk {
        let allocated_bytes = self.allocated.ones as u64 * crate::BLOCK_SIZE as u64;
        let opens_horizon = self.horizon.is_none()
            && self
                .horizon_policy
                .opens(self.bytes_since_floor, allocated_bytes);
        let discharges_horizon = self.apply(&chunk, opens_horizon);

        if let Some(horizon) = &mut self.horizon
            && let Some(proto::chunk::Content::Data(data)) = &chunk.content
        {
            horizon.changed += data.len() as u64;
        }

        super::LiveChunk {
            chunk,
            opens_horizon,
            discharges_horizon,
        }
    }

    /// Account for any chunk. Returns whether it discharged the open horizon.
    fn apply(&mut self, chunk: &proto::Chunk, opens_horizon: bool) -> bool {
        if opens_horizon {
            self.horizon = Some(Horizon::open(&self.allocated, self.bytes_since_floor));
        }
        let covered = chunk.block..(chunk.block + chunk.blocks);
        let data = matches!(chunk.content, Some(proto::chunk::Content::Data(_)));

        () = self.allocated.fill(covered.clone(), data);
        self.bytes_since_floor += prost::Message::encoded_len(chunk) as u64;

        let Some(horizon) = &mut self.horizon else {
            return false;
        };
        () = horizon.pending.fill(covered, false);

        if horizon.pending.ones != 0 {
            return false;
        }

        self.bytes_since_floor -= horizon.bytes_since_floor_at_open;
        self.horizon = None;
        true
    }
}

/// Pending blocks of an open horizon.
struct Horizon {
    /// Blocks not yet discharged by a write, punch, or copy.
    pending: Bitmap,
    /// Where the next search for blocks to copy starts. Every block before it
    /// is discharged or already handed out to copy.
    scan_from: u32,
    /// Changed chunk bytes since the horizon opened, excluding framing, which
    /// earn copy budget.
    changed: u64,
    /// Bytes of the blocks handed out to copy since the horizon opened, which
    /// spend that budget.
    copied: u64,
    /// The size of the chunks from the floor up to the opening chunk.
    bytes_since_floor_at_open: u64,
}

impl Horizon {
    fn open(allocated: &Bitmap, bytes_since_floor: u64) -> Self {
        Self {
            pending: allocated.clone(),
            scan_from: 0,
            changed: 0,
            copied: 0,
            bytes_since_floor_at_open: bytes_since_floor,
        }
    }

    /// The next range of blocks to copy.
    fn next_copy_range(&mut self, horizon_policy: &Policy) -> Option<std::ops::Range<u32>> {
        let budget = horizon_policy.budget(self.changed) - self.copied;
        let limit = std::cmp::min(COPY_BLOCKS as u64, budget / crate::BLOCK_SIZE as u64) as u32;

        if limit == 0 {
            return None;
        }
        let size_blocks = self.pending.bits.len() as u32;
        let start =
            self.scan_from + self.pending.bits[self.scan_from as usize..].first_one()? as u32;
        let mut end = start + 1;

        while end < std::cmp::min(start + limit, size_blocks) && self.pending.bits[end as usize] {
            end += 1;
        }
        self.copied += (end - start) as u64 * crate::BLOCK_SIZE as u64;
        self.scan_from = end;

        Some(start..end)
    }
}

/// When a horizon opens, and how quickly it is discharged.
#[derive(Clone, Copy, Debug)]
pub struct Policy {
    /// A horizon opens once bytes since the floor exceed this times the
    /// allocated bytes.
    pub open_ratio: f64,
    /// Unchanged bytes a horizon may copy per byte changed while it is open.
    pub copy_ratio: f64,
    /// Minimum bytes beyond the floor needed for a horizon to open.
    pub minimum_bytes: u64,
}

impl Policy {
    /// The policy a daemon uses unless its flags change it.
    pub const DEFAULT: Self = Self {
        open_ratio: 2.0,
        copy_ratio: 0.5,
        minimum_bytes: 1 << 30,
    };

    fn opens(&self, bytes_since_floor: u64, allocated: u64) -> bool {
        let scaled = (allocated as f64 * self.open_ratio) as u64;
        bytes_since_floor > std::cmp::max(self.minimum_bytes, scaled)
    }

    fn budget(&self, changed: u64) -> u64 {
        (changed as f64 * self.copy_ratio) as u64
    }
}

/// One bit per device block, and how many are set.
#[derive(Clone)]
struct Bitmap {
    bits: bitvec::vec::BitVec<u64, bitvec::order::Lsb0>,
    ones: u32,
}

impl Bitmap {
    fn new(size_blocks: u32) -> Self {
        Self {
            bits: bitvec::vec::BitVec::repeat(false, size_blocks as usize),
            ones: 0,
        }
    }

    /// Set every bit of `range` to `value`, and keep the count of set bits.
    fn fill(&mut self, range: std::ops::Range<u32>, value: bool) {
        let bits = &mut self.bits[range.start as usize..range.end as usize];
        self.ones -= bits.count_ones() as u32;
        bits.fill(value);
        self.ones += bits.count_ones() as u32;
    }
}

#[cfg(test)]
mod test {
    use super::{Bitmap, COPY_BLOCKS, Compactor, Horizon, Policy};
    use crate::BLOCK_SIZE;
    use crate::device::{LiveChunk, chunk};
    use crate::proto;

    #[test]
    fn test_a_horizon_lifecycle() {
        let horizon_policy = Policy {
            open_ratio: 2.0,
            copy_ratio: 1.0,
            minimum_bytes: 48 << 10, // 48 KiB (12 blocks at 4 KiB each)
        };
        let mut compactor = Compactor::new(16, horizon_policy);
        let mut served = Vec::new();

        // Write blocks 0 to 3, twice. The bytes since the floor are then enough
        // to open a horizon (disk is 4 blocks and the journal is 2x that at 8),
        // but the minimum_bytes holds it back (8 blocks is ~32 KiB).
        for block in [0, 1, 2, 3, 0, 1, 2, 3] {
            served.extend(serve(&mut compactor, &write(block, 1, 1)));
        }
        assert!(compactor.bytes_since_floor > 2 * 4 * BLOCK_SIZE as u64);
        assert!(compactor.bytes_since_floor < horizon_policy.minimum_bytes);
        assert_eq!(pending(&compactor), None);

        // A third round of writes just passes the minimum on the final write
        // (12 blocks written, at 4 KiB each + framing overhead), but needs
        // another write for the horizon check to fire.
        for block in [0, 1, 2, 3] {
            served.extend(serve(&mut compactor, &write(block, 1, 1)));
        }
        assert!(compactor.bytes_since_floor > horizon_policy.minimum_bytes);
        assert_eq!(pending(&compactor), None);

        // The next write opens a horizon. It covers blocks 0 to 3: those are
        // the ones that need to be represented after the horizon for the disk
        // replay to be complete from that point. It rewrites block 0 itself,
        // and writes a copy of block 1 from the budget it got for that write.
        let opened_at = served.len();
        served.extend(serve(&mut compactor, &write(0, 1, 1)));
        assert_eq!(effects(&served[opened_at..]), [(0, "opens"), (1, "")]);
        assert_eq!(pending(&compactor), Some(vec![2, 3]));

        // Helper to rebuild a compactor from `chunks`, as a recovery does.
        let replay = |chunks: &[LiveChunk]| {
            let mut rebuilt = Compactor::new(16, horizon_policy);
            for live in chunks {
                () = rebuilt.on_replayed(live.chunk.clone(), live.opens_horizon);
            }
            rebuilt
        };

        // A recovery now would still have to replay from the first chunk,
        // rather than the unfinished horizon. Ensure that a replayed compactor
        // from the beginning matches the current compactor.
        assert_eq!(state(&replay(&served)), state(&compactor));

        // Each later rewrite of block 0 earns a copy of the next pending block. The
        // copy of block 3 discharges the horizon.
        let rewrites = served.len();
        served.extend(serve(&mut compactor, &write(0, 1, 1))); // copies block 2
        served.extend(serve(&mut compactor, &write(0, 1, 1))); // copies block 3
        assert_eq!(
            effects(&served[rewrites..]),
            [(0, ""), (2, ""), (0, ""), (3, "discharges")]
        );
        assert_eq!(pending(&compactor), None);

        // The floor moves to the opener, so the bytes since the floor are those of the
        // opener and every chunk after it.
        let since_opener: u64 = served[opened_at..]
            .iter()
            .map(|live| prost::Message::encoded_len(&live.chunk) as u64)
            .sum();
        assert_eq!(compactor.bytes_since_floor, since_opener);

        // A recovery replays from the floor. The copies hold blocks 1 to 3, so the
        // replay rebuilds the disk without the chunks before the opener.
        assert_eq!(state(&replay(&served[opened_at..])), state(&compactor));
    }

    #[test]
    fn test_serving_opens_and_discharges_horizons() {
        // Opens a horizon whenever none is open and anything was applied since the
        // floor, and copies nothing: only writes and punches discharge it.
        const OPENS_EAGERLY: Policy = Policy {
            open_ratio: 0.0,
            copy_ratio: 0.0,
            minimum_bytes: 0,
        };
        // Like `OPENS_EAGERLY`, but each write earns a copy of as many bytes as it
        // changed.
        const COPIES: Policy = Policy {
            copy_ratio: 1.0,
            ..OPENS_EAGERLY
        };

        // A one-block request that a scenario serves.
        #[derive(Clone, Copy, Debug)]
        enum Request {
            Write(u32),
            Punch(u32),
        }
        use Request::*;

        // A request, the block and horizon effects of each live chunk it produces,
        // and the pending blocks of the open horizon after it.
        type Step = (
            Request,
            &'static [(u32, &'static str)],
            Option<&'static [usize]>,
        );

        let cases: &[(&str, Policy, &[Step])] = &[
            (
                "a punch discharges a horizon",
                OPENS_EAGERLY,
                &[
                    (Write(0), &[(0, "")], None),
                    (Write(1), &[(1, "opens")], Some(&[0])),
                    (Punch(0), &[(0, "discharges")], None),
                ],
            ),
            (
                "a punch earns no copies",
                COPIES,
                &[
                    (Write(0), &[(0, "")], None),
                    (Write(1), &[(1, "opens"), (0, "discharges")], None),
                    (Write(5), &[(5, "opens"), (0, "")], Some(&[1])),
                    (Punch(9), &[(9, "")], Some(&[1])),
                    (Write(9), &[(9, ""), (1, "discharges")], None),
                ],
            ),
            (
                "an opener which covers every pending block discharges its own horizon",
                OPENS_EAGERLY,
                &[
                    (Write(0), &[(0, "")], None),
                    (Write(0), &[(0, "opens, discharges")], None),
                ],
            ),
            (
                "an opener with nothing allocated before it discharges its own horizon",
                OPENS_EAGERLY,
                &[
                    (Write(0), &[(0, "")], None),
                    (Punch(0), &[(0, "opens, discharges")], None),
                    (Write(1), &[(1, "opens, discharges")], None),
                ],
            ),
        ];

        for (case, horizon_policy, steps) in cases {
            let mut compactor = Compactor::new(16, *horizon_policy);

            for (index, (request, expected_effects, expected_pending)) in steps.iter().enumerate() {
                let chunk = match *request {
                    Write(block) => write(block, 1, 1),
                    Punch(block) => chunk::encode_punch(block, 1),
                };
                assert_eq!(
                    effects(&serve(&mut compactor, &chunk)),
                    *expected_effects,
                    "{case}, step {index}"
                );
                assert_eq!(
                    pending(&compactor),
                    expected_pending.map(<[usize]>::to_vec),
                    "{case}, step {index}"
                );
            }
        }
    }

    /// A replay opens a horizon only where a record did, whatever its policy, and
    /// discharges it as the device that served it did.
    #[test]
    fn test_a_replay_opens_horizons_only_where_records_did() {
        // Opens a horizon at every chance, so a replay which consulted it would open one.
        let horizon_policy = Policy {
            open_ratio: 0.0,
            copy_ratio: 0.0,
            minimum_bytes: 0,
        };
        let mut compactor = Compactor::new(16, horizon_policy);

        () = compactor.on_replayed(write(0, 1, 1), false);
        () = compactor.on_replayed(write(1, 1, 1), false);
        assert_eq!(pending(&compactor), None);

        () = compactor.on_replayed(write(2, 1, 1), true);
        assert_eq!(pending(&compactor), Some(vec![0, 1]));

        () = compactor.on_replayed(write(0, 1, 1), false);
        () = compactor.on_replayed(write(1, 1, 1), false);
        assert_eq!(pending(&compactor), None);
    }

    /// Each call hands out the next run of pending blocks, which stops at the budget,
    /// a hole, the copy limit, or the disk end. The scan never goes back, so a
    /// horizon costs one pass over its bitmap.
    #[test]
    fn test_copies_take_pending_runs_in_one_pass() {
        const BLOCK: u64 = BLOCK_SIZE as u64;
        // Blocks of the disk, with room for a pending run longer than one copy.
        const END: u32 = 2 * COPY_BLOCKS;
        const LONG_RUN: u32 = COPY_BLOCKS + 8;
        // One copied byte per changed byte. A horizon does not consult when to open.
        const ONE_TO_ONE: Policy = Policy {
            open_ratio: 0.0,
            copy_ratio: 1.0,
            minimum_bytes: 0,
        };

        // (case, policy, pending blocks, [(bytes changed before a call, the range it returns)])
        let cases: &[(
            &str,
            Policy,
            &[std::ops::Range<u32>],
            &[(u64, Option<std::ops::Range<u32>>)],
        )] = &[
            (
                "budget under a block",
                ONE_TO_ONE,
                &[0..4],
                &[(BLOCK - 1, None)],
            ),
            (
                "the copy ratio scales the budget",
                Policy {
                    copy_ratio: 0.5,
                    ..ONE_TO_ONE
                },
                &[0..4],
                &[(4 * BLOCK, Some(0..2)), (0, None)],
            ),
            (
                "stops at a hole",
                ONE_TO_ONE,
                &[0..2, 3..4],
                &[(4 * BLOCK, Some(0..2)), (0, Some(3..4))],
            ),
            (
                "stops at the copy limit",
                ONE_TO_ONE,
                &[0..LONG_RUN],
                &[
                    (LONG_RUN as u64 * BLOCK, Some(0..COPY_BLOCKS)),
                    (0, Some(COPY_BLOCKS..LONG_RUN)),
                ],
            ),
            (
                "stops at the disk end",
                ONE_TO_ONE,
                &[END - 4..END],
                &[(8 * BLOCK, Some(END - 4..END)), (0, None)],
            ),
            ("nothing pending", ONE_TO_ONE, &[], &[(4 * BLOCK, None)]),
            (
                "hands out each pending block once, in order",
                ONE_TO_ONE,
                &[0..1, 5..7, END - 1..END],
                &[
                    (2 * BLOCK, Some(0..1)),
                    (0, Some(5..6)),
                    (0, None),
                    (2 * BLOCK, Some(6..7)),
                    (0, Some(END - 1..END)),
                    // Budget remains, but every pending block was handed out.
                    (2 * BLOCK, None),
                ],
            ),
        ];

        for (case, horizon_policy, allocated, calls) in cases {
            let mut bits = Bitmap::new(END);
            for range in *allocated {
                () = bits.fill(range.clone(), true);
            }
            let mut horizon = Horizon::open(&bits, 0);

            for (index, (changed, range)) in calls.iter().enumerate() {
                horizon.changed += changed;
                assert_eq!(
                    horizon.next_copy_range(horizon_policy),
                    *range,
                    "{case}, call {index}"
                );
            }
        }
    }

    #[test]
    fn test_a_horizon_opens_past_the_larger_of_the_ratio_and_the_minimum() {
        // (case, bytes since floor, allocated bytes, opens)
        let cases: &[(&str, u64, u64, bool)] = &[
            ("minimum binds, at it", 1 << 30, 1 << 20, false),
            ("minimum binds, past it", (1 << 30) + 1, 1 << 20, true),
            ("ratio binds, at it", 4 << 30, 2 << 30, false),
            ("ratio binds, past it", (4 << 30) + 1, 2 << 30, true),
        ];
        for &(case, bytes_since_floor, allocated, opens) in cases {
            assert_eq!(
                Policy::DEFAULT.opens(bytes_since_floor, allocated),
                opens,
                "{case}"
            );
        }
    }

    #[test]
    fn test_any_disk_rebuilds_from_its_floor() {
        // The size, in blocks, of a property case's disk. Generated ranges stay inside
        // it, and it is small so that random writes often overlap.
        const BLOCKS: u32 = 16;

        #[derive(Clone, Debug)]
        enum Op {
            Write { start: u32, blocks: u32, fill: u8 },
            Punch { start: u32, blocks: u32 },
            // The writer cuts a delta, which later commits.
            Cut,
        }

        impl quickcheck::Arbitrary for Op {
            fn arbitrary(g: &mut quickcheck::Gen) -> Self {
                // Small, overlapping ranges, so that writes rewrite each other's blocks,
                // discharge horizons, and race their copies.
                let start = u32::arbitrary(g) % BLOCKS;
                let blocks = 1 + u32::arbitrary(g) % std::cmp::min(4, BLOCKS - start);

                match u8::arbitrary(g) % 8 {
                    0..=4 => Op::Write {
                        start,
                        blocks,
                        fill: u8::arbitrary(g),
                    },
                    5 => Op::Punch { start, blocks },
                    _ => Op::Cut,
                }
            }
        }

        // Policies a property case serves under, which open and discharge horizons at
        // different rates.
        const HORIZON_POLICIES: [Policy; 3] = [
            // A horizon opens at the first chunk after one closes.
            Policy {
                open_ratio: 0.0,
                copy_ratio: 1.0,
                minimum_bytes: 0,
            },
            Policy {
                open_ratio: 1.0,
                copy_ratio: 0.5,
                minimum_bytes: 0,
            },
            // Copies too slow to do much, so that writes discharge most blocks.
            Policy {
                open_ratio: 0.5,
                copy_ratio: 0.1,
                minimum_bytes: 8192,
            },
        ];

        // Serve `ops`, cutting deltas as a writer does, and commit every delta. At each
        // commit, a writer stores the opener of a horizon which a chunk of that
        // delta discharged as the floor. A replay from the floor must then rebuild the
        // allocation, horizon, and bytes since the floor which the device held. The
        // device must hold the blocks `ops` left allocated.
        fn floors_rebuild_the_compactor(horizon_policy: u8, ops: Vec<Op>) -> bool {
            let horizon_policy = HORIZON_POLICIES[horizon_policy as usize % HORIZON_POLICIES.len()];
            let mut compactor = Compactor::new(BLOCKS, horizon_policy);
            let mut served = Vec::new();
            let mut allocated = std::collections::BTreeSet::new();

            // Each cut's position in `served`, and the device's state then.
            let mut cuts = Vec::new();

            for op in &ops {
                match *op {
                    Op::Write {
                        start,
                        blocks,
                        fill,
                    } => {
                        served.extend(serve(&mut compactor, &write(start, blocks, fill)));
                        allocated.extend(start as usize..(start + blocks) as usize);
                    }
                    Op::Punch { start, blocks } => {
                        served.extend(serve(&mut compactor, &chunk::encode_punch(start, blocks)));
                        allocated.retain(|&block| {
                            !(start as usize..(start + blocks) as usize).contains(&block)
                        });
                    }
                    Op::Cut => cuts.push((served.len(), state(&compactor))),
                }
            }
            cuts.push((served.len(), state(&compactor)));

            let (tracked, ones, _, _) = state(&compactor);
            assert_eq!(
                (tracked, ones as usize),
                (allocated.iter().copied().collect(), allocated.len()),
                "the compactor tracked other blocks: {ops:?}"
            );

            let (mut opener, mut completes, mut floor) = (None, None, 0);
            let mut begins = 0;

            for (ends, held) in cuts {
                for (index, live) in served[begins..ends].iter().enumerate() {
                    if live.opens_horizon {
                        assert_eq!(opener, None, "a horizon opened over an open one");
                        opener = Some(begins + index);
                    }
                    if live.discharges_horizon {
                        completes = Some(opener.take().expect("a chunk discharged no horizon"));
                    }
                }
                begins = ends;

                if let Some(completed) = completes.take() {
                    assert!(completed >= floor, "the floor moved back");
                    floor = completed;
                }
                let mut rebuilt = Compactor::new(BLOCKS, horizon_policy);
                for live in &served[floor..ends] {
                    () = rebuilt.on_replayed(live.chunk.clone(), live.opens_horizon);
                }
                assert_eq!(
                    state(&rebuilt),
                    held,
                    "a replay from the floor rebuilt another state: {ops:?}"
                );
            }
            true
        }

        quickcheck::QuickCheck::new()
            .tests(1024)
            .quickcheck(floors_rebuild_the_compactor as fn(u8, Vec<Op>) -> bool);
    }

    /// A write of `blocks` blocks of `fill` from `start`, as the owner encodes one.
    fn write(start: u32, blocks: u32, fill: u8) -> proto::Chunk {
        let data = vec![fill; (blocks * BLOCK_SIZE) as usize].into();
        chunk::encode_write(start, &data)
    }

    /// Serve `chunk`, and then the copies it earned, as a target does. A copy reads
    /// the image, which these cases don't hold, so each copies blocks of `0xff`.
    fn serve(compactor: &mut Compactor, chunk: &proto::Chunk) -> Vec<LiveChunk> {
        let mut served = vec![compactor.on_served(chunk.clone())];

        while let Some(range) = compactor.next_copy_range() {
            served.push(compactor.on_copied(write(range.start, range.len() as u32, 0xff)));
        }
        served
    }

    /// The pending blocks of the open horizon, or `None` if no horizon is open.
    fn pending(compactor: &Compactor) -> Option<Vec<usize>> {
        let horizon = compactor.horizon.as_ref()?;
        Some(horizon.pending.bits.iter_ones().collect())
    }

    /// The block of each one-block live chunk, and what it did to a horizon.
    fn effects(served: &[LiveChunk]) -> Vec<(u32, &'static str)> {
        served
            .iter()
            .map(|live| {
                assert_eq!(live.chunk.blocks, 1, "effects reports one-block chunks");
                let effect = match (live.opens_horizon, live.discharges_horizon) {
                    (false, false) => "",
                    (true, false) => "opens",
                    (false, true) => "discharges",
                    (true, true) => "opens, discharges",
                };
                (live.chunk.block, effect)
            })
            .collect()
    }

    /// What a replay must rebuild: the allocated blocks, the open horizon's pending
    /// blocks, and the bytes since the floor. Each bitmap's count is included, so
    /// that a stale count fails too.
    fn state(compactor: &Compactor) -> (Vec<usize>, u32, Option<(Vec<usize>, u32)>, u64) {
        (
            compactor.allocated.bits.iter_ones().collect(),
            compactor.allocated.ones,
            compactor.horizon.as_ref().map(|horizon| {
                (
                    horizon.pending.bits.iter_ones().collect(),
                    horizon.pending.ones,
                )
            }),
            compactor.bytes_since_floor,
        )
    }
}
