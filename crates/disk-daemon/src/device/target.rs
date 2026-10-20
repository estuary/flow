use super::chunk;
use super::horizon;
use super::image;
use super::ublk;
use crate::proto;
use anyhow::Context;

/// Most blocks one horizon copy reads, so a copy is a mutation of the same order as
/// the largest device request.
const COPY_BLOCKS: u32 = ublk::MAX_IO_BUF_BYTES / crate::BLOCK_SIZE;

/// A disk's image and recovery horizon. A rebuilder thread replays the journal's
/// chunks into them, and then the owner serves the disk's requests from them. Both
/// apply chunks alike.
pub(super) struct Target {
    /// The disk's content, which reads are served from and chunks applied to.
    image: image::Image,
    /// The horizon this disk is discharging, held only while one is open.
    horizon: Option<horizon::Horizon>,
    /// Horizon opening thresholds and copy budget.
    horizon_policy: horizon::Policy,
    /// Encoded chunk bytes applied since the recovery floor, which a replay from
    /// the floor reads, and which decide whether a horizon opens.
    replay_bytes: u64,
}

impl Target {
    pub fn new(image: image::Image, horizon_policy: horizon::Policy) -> Self {
        Self {
            image,
            horizon: None,
            horizon_policy,
            replay_bytes: 0,
        }
    }

    /// Apply one committed chunk the journal replayed, as the device applied it
    /// while served. A horizon opens where `opens_horizon` says one did, and closes
    /// where nothing it holds is pending: from a floor, a replay rebuilds the same
    /// pending blocks.
    pub fn replay(&mut self, chunk: proto::Chunk, opens_horizon: bool) -> anyhow::Result<()> {
        _ = self
            .apply(&chunk, opens_horizon)
            .with_context(|| format!("applying chunk at block {}", chunk.block))?;
        Ok(())
    }

    /// Apply one served write or punch, and then the copies it earned. Returns the
    /// mutations to send, in order: `chunk`'s own, and then each copy's.
    pub fn serve(&mut self, chunk: proto::Chunk) -> anyhow::Result<Vec<super::Mutation>> {
        let opens_horizon = self.horizon.is_none()
            && self
                .horizon_policy
                .opens(self.replay_bytes, self.image.allocated_bytes());

        let discharges_horizon = self
            .apply(&chunk, opens_horizon)
            .context("applying a chunk to the image")?;

        if opens_horizon {
            tracing::info!(
                replay_bytes = self.replay_bytes,
                "opened a recovery horizon"
            );
        }
        // Only a served chunk earns copies, so a replayed horizon carries no budget
        // into serving.
        if let Some(horizon) = &mut self.horizon {
            () = horizon.changed(chunk::data_bytes(&chunk));
        }
        let mut served = vec![super::Mutation {
            chunk,
            opens_horizon,
            discharges_horizon,
        }];

        // Spend what this chunk earned at once, so the budget cannot build into a
        // burst.
        while let Some(horizon) = &mut self.horizon
            && let Some(run) = horizon.next_copy(&self.horizon_policy, COPY_BLOCKS)
        {
            let mut data = vec![0; run.len() * crate::BLOCK_SIZE as usize];

            () = self
                .image
                .read_at(run.start, &mut data)
                .context("copying a horizon run out of the image")?;

            let copy = chunk::encode_write(run.start, &data.into());
            () = horizon.copied(run.clone(), chunk::data_bytes(&copy));

            // The image already holds the copy's content, so it's published as an
            // applied chunk is, without writing it again.
            let discharges_horizon = self.publish(&copy, run);
            served.push(super::Mutation {
                chunk: copy,
                opens_horizon: false,
                discharges_horizon,
            });
        }
        Ok(served)
    }

    /// Fill `buf` with the image's blocks from `start`.
    pub fn read(&self, start: u32, buf: &mut [u8]) -> anyhow::Result<()> {
        self.image.read_at(start, buf).context("reading the image")
    }

    /// Apply `chunk` to the image, as a served or a replayed one is: open a horizon
    /// first where `opens_horizon`, over the blocks allocated before the chunk,
    /// then publish it. Returns whether that discharged the horizon's last pending
    /// block.
    fn apply(&mut self, chunk: &proto::Chunk, opens_horizon: bool) -> std::io::Result<bool> {
        // A served disk opens a horizon only where none is open, so a replayed
        // opener never replaces one, short of a corrupt journal.
        if opens_horizon {
            self.horizon = Some(horizon::Horizon::open(
                self.image.allocated(),
                self.replay_bytes,
            ));
        }
        let covered = self.image.apply(chunk)?;
        Ok(self.publish(chunk, covered))
    }

    /// Publish `chunk`, whose content the image holds over `covered`: discharge
    /// those blocks from the open horizon, and count the chunk into the replay
    /// bytes. A horizon left with nothing pending closes, and the replay bytes then
    /// count from where it opened. Returns whether one closed.
    fn publish(&mut self, chunk: &proto::Chunk, covered: std::ops::Range<u32>) -> bool {
        if let Some(horizon) = &mut self.horizon {
            () = horizon.published(covered);
        }
        self.replay_bytes += prost::Message::encoded_len(chunk) as u64;

        let Some(horizon) = self.horizon.take_if(|horizon| horizon.pending() == 0) else {
            return false;
        };
        self.replay_bytes -= horizon.replay_bytes_at_open();
        true
    }
}

#[cfg(test)]
mod test {
    use super::super::Mutation;
    use super::{Target, chunk, horizon, image};
    use crate::BLOCK_SIZE;
    use crate::proto;
    use std::fmt::Write as _;

    /// A policy which never opens a horizon.
    const NEVER_OPENS: horizon::Policy = horizon::Policy {
        open_ratio: 1.0,
        copy_ratio: 0.0,
        minimum_bytes: u64::MAX,
    };

    /// Blocks of the image a trace or property case serves.
    const BLOCKS: u32 = 16;

    /// A serving target, and every mutation it returned, in the order a journal
    /// would hold them.
    struct Case {
        target: Target,
        served: Vec<Mutation>,
    }

    impl Case {
        fn new(image: image::Image, horizon_policy: horizon::Policy) -> Self {
            Self {
                target: Target::new(image, horizon_policy),
                served: Vec::new(),
            }
        }

        /// Serve `chunk`, keeping and returning the mutations it returned.
        fn serve(&mut self, chunk: proto::Chunk) -> anyhow::Result<&[Mutation]> {
            let begins = self.served.len();
            self.served.extend(self.target.serve(chunk)?);
            Ok(&self.served[begins..])
        }

        /// How many blocks the open horizon has pending, and the replay bytes.
        fn horizon_state(&self) -> (Option<u32>, u64) {
            horizon_state(&self.target)
        }
    }

    fn horizon_state(target: &Target) -> (Option<u32>, u64) {
        let pending = (target.horizon.as_ref()).map(horizon::Horizon::pending);
        (pending, target.replay_bytes)
    }

    /// A write of `blocks` blocks of `fill` from `start`, as the owner encodes one.
    fn write(start: u32, blocks: u32, fill: u8) -> proto::Chunk {
        let data = vec![fill; (blocks * BLOCK_SIZE) as usize].into();
        chunk::encode_write(start, &data)
    }

    fn fresh_image() -> image::Image {
        image::Image::create(&std::env::temp_dir(), BLOCKS).unwrap()
    }

    /// `/dev/full`, which refuses every write.
    fn full_image() -> image::Image {
        let full = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/full")
            .unwrap();
        image::Image::from_file(full, BLOCKS)
    }

    /// A write-only image, which takes every write and refuses every read.
    fn unreadable_image() -> image::Image {
        let mut options = std::fs::OpenOptions::new();
        options.write(true);
        std::os::unix::fs::OpenOptionsExt::custom_flags(&mut options, libc::O_TMPFILE);
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);

        let file = options.open(std::env::temp_dir()).unwrap();
        file.set_len((BLOCKS * BLOCK_SIZE) as u64).unwrap();
        image::Image::from_file(file, BLOCKS)
    }

    /// Render what a serve returned: the blocks each mutation covers and what it
    /// did to a horizon, or the error.
    fn render(served: anyhow::Result<&[Mutation]>) -> String {
        let served = match served {
            Ok(served) => served,
            Err(err) => return format!("{err:#}"),
        };
        let served: Vec<String> = served
            .iter()
            .map(|mutation| {
                let mut out = format!(
                    "{}..{}",
                    mutation.chunk.block,
                    mutation.chunk.block + mutation.chunk.blocks
                );
                if mutation.opens_horizon {
                    out.push_str(" opens");
                }
                if mutation.discharges_horizon {
                    out.push_str(" discharges");
                }
                out
            })
            .collect();
        format!("[{}]", served.join(", "))
    }

    /// Report allocated blocks and their fill bytes, using `?` for unreadable blocks.
    fn allocated(image: &image::Image) -> String {
        let mut block = vec![0u8; BLOCK_SIZE as usize];

        let blocks: Vec<String> = image
            .allocated()
            .iter_ones()
            .map(|index| match image.read_at(index as u32, &mut block) {
                Ok(()) => format!("{index}={}", block[0]),
                Err(_) => format!("{index}=?"),
            })
            .collect();
        format!("[{}]", blocks.join(" "))
    }

    #[derive(Debug, Clone, Copy)]
    enum Step {
        /// Write the block at `.0`, filled with `.1`.
        Write(u32, u8),
        /// Punch the block at `.0`.
        Punch(u32),
        /// Read the block at `.0`.
        Read(u32),
    }
    use Step::*;

    /// Trace the open horizon's pending blocks, the replay bytes, and image content
    /// after each step, and what the step returned.
    fn trace(case: &mut Case, steps: &[Step]) -> String {
        let mut out = String::new();

        for &step in steps {
            let outcome = match step {
                Write(block, fill) => render(case.serve(write(block, 1, fill))),
                Punch(block) => render(case.serve(chunk::encode_punch(block, 1))),
                Read(block) => {
                    let mut data = vec![0; BLOCK_SIZE as usize];
                    match case.target.read(block, &mut data) {
                        Ok(()) => format!("read {}", data[0]),
                        Err(err) => format!("{err:#}"),
                    }
                }
            };
            let horizon = match &case.target.horizon {
                Some(horizon) => horizon.pending().to_string(),
                None => "-".to_string(),
            };
            let step = format!("{step:?}");

            writeln!(
                out,
                "{step:<12}horizon {horizon:<2} replay {:<6}image {:<18}{outcome}",
                case.target.replay_bytes,
                allocated(&case.target.image),
            )
            .unwrap();
        }
        out
    }

    /// Opens a horizon over any disk once anything was applied, and copies
    /// nothing: writes alone discharge it.
    const OPENS_EAGERLY: horizon::Policy = horizon::Policy {
        open_ratio: 0.0,
        copy_ratio: 0.0,
        minimum_bytes: 0,
    };

    /// As `OPENS_EAGERLY`, where each write earns a copy of as many bytes as it
    /// changed.
    const COPIES: horizon::Policy = horizon::Policy {
        copy_ratio: 1.0,
        ..OPENS_EAGERLY
    };

    /// A named case: the image it serves, its horizon policy, and its steps.
    type Scenario = (
        &'static str,
        fn() -> image::Image,
        horizon::Policy,
        &'static [Step],
    );

    #[test]
    fn test_serves_horizons_and_failures() {
        let cases: &[Scenario] = &[
            (
                "a chunk applies before it is returned, and a read is served from the image",
                fresh_image,
                NEVER_OPENS,
                &[Write(0, 1), Read(0), Punch(0), Read(0)],
            ),
            (
                "a horizon opens over the blocks then allocated, and the mutation which \
                 discharges its last pending block says so",
                fresh_image,
                OPENS_EAGERLY,
                &[
                    // Nothing is applied yet, so no horizon opens.
                    Write(0, 1),
                    Write(1, 2),
                    Write(0, 3),
                    // Replay bytes count from the last horizon's opening, so the next
                    // write opens another, over blocks 0 and 1. Punches discharge too.
                    Write(2, 4),
                    Punch(0),
                    Punch(1),
                ],
            ),
            (
                "the copies a write earns follow it",
                fresh_image,
                COPIES,
                &[
                    Write(0, 1),
                    // Block 0 is pending, and this write earns its copy.
                    Write(1, 2),
                    // Blocks 0 and 1 are pending, and a write earns one of them.
                    Write(5, 5),
                    // A punch earns nothing, and the write after it the rest.
                    Punch(9),
                    Write(9, 6),
                ],
            ),
            (
                "an image write which fails is an error",
                full_image,
                NEVER_OPENS,
                &[Write(0, 1)],
            ),
            (
                "an image read which fails is an error",
                unreadable_image,
                NEVER_OPENS,
                &[Write(0, 1), Read(0)],
            ),
            (
                "a horizon run which cannot be read fails the write which earned the copy",
                unreadable_image,
                COPIES,
                &[Write(0, 1), Write(5, 2)],
            ),
        ];

        let mut out = String::new();
        for (name, image, horizon_policy, steps) in cases {
            let mut case = Case::new(image(), *horizon_policy);
            writeln!(out, "{name}:").unwrap();
            out.push_str(&trace(&mut case, steps));
        }
        insta::assert_snapshot!(out, @"
        a chunk applies before it is returned, and a read is served from the image:
        Write(0, 1) horizon -  replay 4101  image [0=1]             [0..1]
        Read(0)     horizon -  replay 4101  image [0=1]             read 1
        Punch(0)    horizon -  replay 4105  image []                [0..1]
        Read(0)     horizon -  replay 4105  image []                read 0
        a horizon opens over the blocks then allocated, and the mutation which discharges its last pending block says so:
        Write(0, 1) horizon -  replay 4101  image [0=1]             [0..1]
        Write(1, 2) horizon 1  replay 8204  image [0=1 1=2]         [1..2 opens]
        Write(0, 3) horizon -  replay 8204  image [0=3 1=2]         [0..1 discharges]
        Write(2, 4) horizon 2  replay 12307 image [0=3 1=2 2=4]     [2..3 opens]
        Punch(0)    horizon 1  replay 12311 image [1=2 2=4]         [0..1]
        Punch(1)    horizon -  replay 4113  image [2=4]             [1..2 discharges]
        the copies a write earns follow it:
        Write(0, 1) horizon -  replay 4101  image [0=1]             [0..1]
        Write(1, 2) horizon -  replay 8204  image [0=1 1=2]         [1..2 opens, 0..1 discharges]
        Write(5, 5) horizon 1  replay 16408 image [0=1 1=2 5=5]     [5..6 opens, 0..1]
        Punch(9)    horizon 1  replay 16414 image [0=1 1=2 5=5]     [9..10]
        Write(9, 6) horizon -  replay 16416 image [0=1 1=2 5=5 9=6] [9..10, 1..2 discharges]
        an image write which fails is an error:
        Write(0, 1) horizon -  replay 0     image []                applying a chunk to the image: No space left on device (os error 28)
        an image read which fails is an error:
        Write(0, 1) horizon -  replay 4101  image [0=?]             [0..1]
        Read(0)     horizon -  replay 4101  image [0=?]             reading the image: Bad file descriptor (os error 9)
        a horizon run which cannot be read fails the write which earned the copy:
        Write(0, 1) horizon -  replay 4101  image [0=?]             [0..1]
        Write(5, 2) horizon 1  replay 8204  image [0=? 5=?]         copying a horizon run out of the image: Bad file descriptor (os error 9)
        ");
    }

    /// A replay opens a horizon only where a record did, and discharges it as the
    /// device which served it did.
    #[test]
    fn test_a_replay_opens_horizons_where_records_did() {
        let mut target = Target::new(fresh_image(), NEVER_OPENS);
        let mut out = String::new();

        for (what, chunk, opens_horizon) in [
            ("write 0", write(0, 1, 1), false),
            ("write 1", write(1, 1, 2), false),
            ("write 2, opening", write(2, 1, 3), true),
            ("write 0", write(0, 1, 4), false),
            ("write 1", write(1, 1, 6), false),
            ("write 4, opening", write(4, 1, 7), true),
        ] {
            () = target.replay(chunk, opens_horizon).unwrap();

            let (pending, replay_bytes) = horizon_state(&target);
            let pending = match pending {
                Some(pending) => pending.to_string(),
                None => "-".to_string(),
            };
            writeln!(
                out,
                "{what:<18}pending {pending:<4}replay {replay_bytes:<6}image {}",
                allocated(&target.image),
            )
            .unwrap();
        }
        insta::assert_snapshot!(out, @"
        write 0           pending -   replay 4101  image [0=1]
        write 1           pending -   replay 8204  image [0=1 1=2]
        write 2, opening  pending 2   replay 12307 image [0=1 1=2 2=3]
        write 0           pending 1   replay 16408 image [0=4 1=2 2=3]
        write 1           pending -   replay 12307 image [0=4 1=6 2=3]
        write 4, opening  pending 3   replay 16410 image [0=4 1=6 2=3 4=7]
        ");
    }

    #[derive(Clone, Debug)]
    enum Op {
        Write {
            start: u32,
            blocks: u32,
            fill: u8,
        },
        Punch {
            start: u32,
            blocks: u32,
        },
        /// The writer cuts a delta, which later commits.
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

    /// Policies a property case serves under, which open and discharge horizons at
    /// different rates.
    const HORIZON_POLICIES: [horizon::Policy; 3] = [
        // A horizon opens at the first mutation after one closes.
        horizon::Policy {
            open_ratio: 0.0,
            copy_ratio: 1.0,
            minimum_bytes: 0,
        },
        horizon::Policy {
            open_ratio: 1.0,
            copy_ratio: 0.5,
            minimum_bytes: 0,
        },
        // Copies too slow to do much, so that writes discharge most blocks.
        horizon::Policy {
            open_ratio: 0.5,
            copy_ratio: 0.1,
            minimum_bytes: 8192,
        },
    ];

    /// `served`, replayed onto a fresh image.
    fn replay(served: &[Mutation]) -> image::Image {
        let mut replayed = fresh_image();

        for mutation in served {
            _ = replayed.apply(&mutation.chunk).unwrap();
        }
        replayed
    }

    /// `left` and `right` hold the same bytes in the same allocated blocks. A
    /// served stream, replayed, must rebuild the image served, which is why the
    /// local image is disposable.
    fn assert_identical(left: &image::Image, right: &image::Image, what: &str) {
        assert_eq!(
            left.allocated(),
            right.allocated(),
            "{what} tracked other blocks"
        );

        // The block at which they first differ names where a replay went wrong,
        // rather than dumping either image.
        let (mut left_buf, mut right_buf) = (
            vec![0u8; BLOCK_SIZE as usize],
            vec![0u8; BLOCK_SIZE as usize],
        );
        let differs = (0..BLOCKS).find(|&block| {
            () = left.read_at(block, &mut left_buf).unwrap();
            () = right.read_at(block, &mut right_buf).unwrap();
            left_buf != right_buf
        });
        assert_eq!(differs, None, "{what} differs in content");
    }

    /// Serve `ops`, cutting deltas as a writer does, and commit every delta. At each
    /// commit, a writer stores the opener of a horizon which a mutation of that
    /// delta discharged as the floor. Replay from the floor must then rebuild the
    /// image which replay from the start does, and a rebuild from it must hold
    /// the horizon and replay bytes which the device held. Every chunk is served.
    fn floors_rebuild_the_disk(horizon_policy: u8, ops: Vec<Op>) -> bool {
        let horizon_policy = HORIZON_POLICIES[horizon_policy as usize % HORIZON_POLICIES.len()];
        let mut case = Case::new(fresh_image(), horizon_policy);

        // Each cut's position in `case.served`, and the device's horizon state then.
        let mut cuts = Vec::new();

        for op in ops {
            match op {
                Op::Write {
                    start,
                    blocks,
                    fill,
                } => _ = case.serve(write(start, blocks, fill)).unwrap(),
                Op::Punch { start, blocks } => {
                    _ = case.serve(chunk::encode_punch(start, blocks)).unwrap()
                }
                Op::Cut => cuts.push((case.served.len(), case.horizon_state())),
            }
        }
        cuts.push((case.served.len(), case.horizon_state()));

        let (mut opener, mut completes, mut floor) = (None, None, 0);
        let mut begins = 0;

        for (ends, held) in cuts {
            for (index, mutation) in case.served[begins..ends].iter().enumerate() {
                if mutation.opens_horizon {
                    assert_eq!(opener, None, "a horizon opened over an open one");
                    opener = Some(begins + index);
                }
                if mutation.discharges_horizon {
                    completes = Some(opener.take().expect("a mutation discharged no horizon"));
                }
            }
            begins = ends;

            if let Some(completed) = completes.take() {
                assert!(completed >= floor, "the floor moved back");
                floor = completed;
            }
            let from_floor = &case.served[floor..ends];

            assert_identical(
                &replay(from_floor),
                &replay(&case.served[..ends]),
                "a replay from the floor",
            );

            let mut rebuilt = Target::new(fresh_image(), horizon_policy);
            for mutation in from_floor {
                () = rebuilt
                    .replay(mutation.chunk.clone(), mutation.opens_horizon)
                    .unwrap();
            }
            assert!(
                horizon_state(&rebuilt) == held,
                "a replay from the floor rebuilt another horizon"
            );
        }

        assert_identical(&replay(&case.served), &case.target.image, "the replay");
        true
    }

    #[test]
    fn test_any_disk_rebuilds_from_its_floor() {
        quickcheck::QuickCheck::new()
            .tests(256)
            .quickcheck(floors_rebuild_the_disk as fn(u8, Vec<Op>) -> bool);
    }
}
