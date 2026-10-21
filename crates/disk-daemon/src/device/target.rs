use super::chunk;
use super::compactor;
use super::ublk;
use crate::proto;
use anyhow::Context;

/// The zeroes after a chunk's data, which is never longer than the largest device
/// request or horizon copy.
static ZEROES: [u8; ublk::MAX_IO_BUF_BYTES as usize] = [0; ublk::MAX_IO_BUF_BYTES as usize];

/// A disk's image and its compactor. It applies each chunk to both.
pub(super) struct Target {
    /// Sparse file of the disk's blocks.
    image: std::fs::File,
    /// What the applied chunks allocated, and the horizon they discharge.
    compactor: compactor::Compactor,
}

impl Target {
    pub fn new(image: std::fs::File, size_blocks: u32, horizon_policy: compactor::Policy) -> Self {
        Self {
            image,
            compactor: compactor::Compactor::new(size_blocks, horizon_policy),
        }
    }

    /// Apply one committed chunk the journal replayed.
    pub fn replay(&mut self, chunk: &proto::Chunk, opens_horizon: bool) -> anyhow::Result<()> {
        () = self
            .apply_chunk(chunk)
            .with_context(|| format!("applying chunk at block {}", chunk.block))?;
        () = self.compactor.on_replayed(chunk, opens_horizon);
        Ok(())
    }

    /// Apply one served write or punch, then copy pending horizon blocks in
    /// proportion to the bytes it changed. Returns the live chunks to send to the
    /// journal, in order: `chunk`'s own, and then each copy's.
    pub fn serve(&mut self, chunk: proto::Chunk) -> anyhow::Result<Vec<crate::LiveChunk>> {
        () = self
            .apply_chunk(&chunk)
            .with_context(|| format!("applying chunk at block {}", chunk.block))?;
        let mut served = vec![self.compactor.on_served(chunk)];

        while let Some(range) = self.compactor.next_copy_range() {
            let mut copied_data = vec![0; range.len() * crate::BLOCK_SIZE as usize];
            () = self
                .read(range.start, &mut copied_data)
                .context("copying horizon blocks out of the image")?;

            served.push(
                self.compactor
                    .on_copied(chunk::encode_write(range.start, copied_data.into())),
            );
        }
        Ok(served)
    }

    /// Fill `buf` with the image's blocks from `start`.
    pub fn read(&self, start: u32, buf: &mut [u8]) -> anyhow::Result<()> {
        std::os::unix::fs::FileExt::read_exact_at(
            &self.image,
            buf,
            start as u64 * crate::BLOCK_SIZE as u64,
        )
        .context("reading the image")
    }

    fn apply_chunk(&self, chunk: &proto::Chunk) -> std::io::Result<()> {
        let (offset, len) = (
            chunk.block as u64 * crate::BLOCK_SIZE as u64,
            chunk.blocks as usize * crate::BLOCK_SIZE as usize,
        );

        match chunk
            .content
            .as_ref()
            .expect("a journaled chunk has content")
        {
            proto::chunk::Content::Data(data) => {
                () = std::os::unix::fs::FileExt::write_all_at(&self.image, data, offset)?;
                std::os::unix::fs::FileExt::write_all_at(
                    &self.image,
                    &ZEROES[..len - data.len()],
                    offset + data.len() as u64,
                )
            }
            // `KEEP_SIZE` keeps the image's size, which is the device's capacity.
            proto::chunk::Content::Punch(_) => rustix::fs::fallocate(
                &self.image,
                rustix::fs::FallocateFlags::PUNCH_HOLE | rustix::fs::FallocateFlags::KEEP_SIZE,
                offset,
                len as u64,
            )
            .map_err(std::io::Error::from),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{Target, chunk, compactor};
    use crate::BLOCK_SIZE;
    use crate::proto;

    /// Small enough that a property case runs in microseconds.
    const BLOCKS: u32 = 64;

    /// Opens a horizon over any disk once anything was applied, and copies as
    /// many bytes as each write changes.
    const COPIES: compactor::Policy = compactor::Policy {
        open_ratio: 0.0,
        copy_ratio: 1.0,
        minimum_bytes: 0,
    };

    #[test]
    fn test_serve_read_and_replay() {
        // A policy which copies at once, so the second write earns a copy.
        let mut target = Target::new(fresh_image(), BLOCKS, COPIES);
        let mut live = Vec::new();

        // A served write applies to the image before it returns.
        live.extend(target.serve(write(0, 0xaa)).unwrap());
        assert_eq!(read(&target, 0), [0xaa; BLOCK_SIZE as usize]);

        // The next write opens a horizon over block 0, and earns a copy of it.
        let served = target.serve(write(5, 0xbb)).unwrap();
        assert_eq!(served.len(), 2);
        assert_eq!(served[0].chunk, write(5, 0xbb));
        assert_eq!(served[1].chunk, write(0, 0xaa));
        live.extend(served);

        // A short rewrite of block 0 drops its trailing zeroes, but the image still
        // zeroes the rest of the block, over the older content.
        let mut short = vec![0; BLOCK_SIZE as usize];
        short[..10].copy_from_slice(b"0123456789");
        live.extend(
            target
                .serve(chunk::encode_write(0, short.clone().into()))
                .unwrap(),
        );
        assert_eq!(read(&target, 0), short);

        // A punch reads back as zeroes.
        live.extend(target.serve(chunk::encode_punch(5, 1)).unwrap());
        assert_eq!(read(&target, 5), [0; BLOCK_SIZE as usize]);

        // A replay of every live chunk rebuilds the same image.
        let mut replayed = Target::new(fresh_image(), BLOCKS, COPIES);
        for live in live {
            () = replayed.replay(&live.chunk, live.opens_horizon).unwrap();
        }
        assert!(
            content(&replayed.image) == content(&target.image),
            "the replay differs from the served image"
        );
    }

    /// Serve random writes and punches with copies, and apply each also as a plain
    /// write or hole punch to a second file. After every request, a replay of the live
    /// chunks must rebuild the served image. At the end, the served image must hold
    /// what the plain file holds, and the filesystem must allocate both alike.
    #[test]
    fn test_served_requests_match_direct_writes_and_replay() {
        const MAX_OP_BLOCKS: u32 = 8;

        // One generated device request.
        #[derive(Clone, Debug)]
        enum Op {
            Write {
                block: u32,
                blocks: u32,
                fill: u8,
                zero_tail: u32,
            },
            // A discard or write-zeroes request, which the owner encodes as a punch.
            Punch {
                block: u32,
                blocks: u32,
            },
        }

        impl quickcheck::Arbitrary for Op {
            fn arbitrary(g: &mut quickcheck::Gen) -> Self {
                let block = u32::from(u8::arbitrary(g)) % BLOCKS;
                let blocks =
                    1 + u32::from(u8::arbitrary(g)) % std::cmp::min(MAX_OP_BLOCKS, BLOCKS - block);
                let span = blocks * BLOCK_SIZE;

                match u8::arbitrary(g) % 4 {
                    0 | 1 => Op::Punch { block, blocks },
                    _ => Op::Write {
                        block,
                        blocks,
                        fill: u8::arbitrary(g),
                        // Biased towards trim boundaries. The cases are no trim, a full
                        // trim, whole trailing blocks, and an arbitrary cut within a
                        // block.
                        zero_tail: match u8::arbitrary(g) % 4 {
                            0 => 0,
                            1 => span,
                            2 => (u32::from(u8::arbitrary(g)) % (blocks + 1)) * BLOCK_SIZE,
                            _ => u32::from(u16::arbitrary(g)) % (span + 1),
                        },
                    },
                }
            }
        }

        #[derive(Clone, Debug)]
        struct Ops(Vec<Op>);

        impl quickcheck::Arbitrary for Ops {
            fn arbitrary(g: &mut quickcheck::Gen) -> Self {
                let count = usize::from(u8::arbitrary(g) % 24);
                Ops((0..count).map(|_| Op::arbitrary(g)).collect())
            }

            fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
                Box::new(self.0.shrink().map(Ops))
            }
        }

        // Content of a generated write. The interior bytes take every value, zero
        // included. The final `zero_tail` bytes are zero.
        fn patterned_data(blocks: u32, fill: u8, zero_tail: u32) -> bytes::Bytes {
            let span = (blocks * BLOCK_SIZE) as usize;
            let body = span - zero_tail as usize;

            bytes::Bytes::from(
                (0..span)
                    .map(|i| {
                        if i < body {
                            fill.wrapping_add(i as u8)
                        } else {
                            0
                        }
                    })
                    .collect::<Vec<u8>>(),
            )
        }

        // Byte ranges the filesystem reports as allocated. This calls `sync_all`
        // first, so that ext4's delayed allocation has resolved into extents.
        fn data_extents(file: &std::fs::File) -> Vec<(u64, u64)> {
            file.sync_all().unwrap();
            let size = BLOCKS as u64 * BLOCK_SIZE as u64;

            let mut extents = Vec::new();
            let mut cursor = 0;

            while cursor < size {
                let Ok(start) = rustix::fs::seek(file, rustix::fs::SeekFrom::Data(cursor)) else {
                    break; // ENXIO: no data at or after `cursor`.
                };
                let end = rustix::fs::seek(file, rustix::fs::SeekFrom::Hole(start)).unwrap();

                extents.push((start, end));
                cursor = end;
            }
            extents
        }

        fn serves_like_direct_writes_and_replays(Ops(ops): Ops) -> bool {
            let mut target = Target::new(fresh_image(), BLOCKS, COPIES);
            let mut replayed = Target::new(fresh_image(), BLOCKS, COPIES);
            let direct = fresh_image();

            for op in &ops {
                let chunk = match *op {
                    Op::Write {
                        block,
                        blocks,
                        fill,
                        zero_tail,
                    } => {
                        let data = patterned_data(blocks, fill, zero_tail);
                        let offset = block as u64 * BLOCK_SIZE as u64;
                        std::os::unix::fs::FileExt::write_all_at(&direct, &data, offset).unwrap();
                        chunk::encode_write(block, data)
                    }
                    Op::Punch { block, blocks } => {
                        let (offset, len) = (
                            block as u64 * BLOCK_SIZE as u64,
                            blocks as u64 * BLOCK_SIZE as u64,
                        );
                        () = rustix::fs::fallocate(
                            &direct,
                            rustix::fs::FallocateFlags::PUNCH_HOLE
                                | rustix::fs::FallocateFlags::KEEP_SIZE,
                            offset,
                            len,
                        )
                        .unwrap();
                        chunk::encode_punch(block, blocks)
                    }
                };

                for live in target.serve(chunk).unwrap() {
                    () = replayed.replay(&live.chunk, live.opens_horizon).unwrap();
                }
                assert!(
                    content(&replayed.image) == content(&target.image),
                    "the replay differs after {op:?}: {ops:?}"
                );
            }

            assert!(
                content(&direct) == content(&target.image),
                "image content differs: {ops:?}"
            );
            assert_eq!(
                data_extents(&direct),
                data_extents(&target.image),
                "filesystem allocation differs: {ops:?}"
            );
            true
        }

        quickcheck::QuickCheck::new()
            .tests(256)
            .quickcheck(serves_like_direct_writes_and_replays as fn(Ops) -> bool);
    }

    /// A fresh sparse image of `BLOCKS`.
    fn fresh_image() -> std::fs::File {
        let file = tempfile::tempfile().unwrap();
        file.set_len(BLOCKS as u64 * BLOCK_SIZE as u64).unwrap();
        file
    }

    /// Every byte of `image`.
    fn content(image: &std::fs::File) -> Vec<u8> {
        let mut content = vec![0; BLOCKS as usize * BLOCK_SIZE as usize];
        std::os::unix::fs::FileExt::read_exact_at(image, &mut content, 0).unwrap();
        content
    }

    /// A one-block write of `fill` at `block`, as the owner encodes it.
    fn write(block: u32, fill: u8) -> proto::Chunk {
        chunk::encode_write(block, vec![fill; BLOCK_SIZE as usize].into())
    }

    /// The image's block at `block`, as the target reads it.
    fn read(target: &Target, block: u32) -> Vec<u8> {
        let mut data = vec![0; BLOCK_SIZE as usize];
        () = target.read(block, &mut data).unwrap();
        data
    }
}
