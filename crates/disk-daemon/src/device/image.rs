use super::chunk;
use super::compactor;
use crate::proto;
use anyhow::Context;

/// A disk's image and its compactor. It applies each chunk to both.
pub(super) struct Image {
    /// Sparse file of the disk's blocks.
    file: std::fs::File,
    /// What the applied chunks allocated, and the horizon they complete.
    compactor: compactor::Compactor,
}

impl Image {
    pub fn new(file: std::fs::File, size_blocks: u32, horizon_policy: compactor::Policy) -> Self {
        Self {
            file,
            compactor: compactor::Compactor::new(size_blocks, horizon_policy),
        }
    }

    /// Apply one committed chunk the journal replayed.
    pub fn replay(
        &mut self,
        chunk: &proto::Chunk,
        opens_horizon_at: Option<i64>,
    ) -> anyhow::Result<()> {
        () = chunk::apply(&self.file, chunk)
            .with_context(|| format!("applying chunk at block {}", chunk.block))?;
        () = self.compactor.on_replayed(chunk, opens_horizon_at);
        Ok(())
    }

    /// Apply one served write or punch, then copy pending horizon blocks in
    /// proportion to the bytes it changed. Returns the live chunks to send to the
    /// journal, in order: `chunk`'s own, and then each copy's.
    pub fn serve(&mut self, chunk: proto::Chunk) -> anyhow::Result<Vec<crate::LiveChunk>> {
        () = chunk::apply(&self.file, &chunk)
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
            &self.file,
            buf,
            start as u64 * crate::BLOCK_SIZE as u64,
        )
        .context("reading the image")
    }
}

#[cfg(test)]
mod test {
    use super::{Image, chunk, compactor};
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
        let mut image = Image::new(fresh_image(), BLOCKS, COPIES);
        let mut live = Vec::new();

        // A served write applies to the image before `serve` returns.
        live.extend(image.serve(write(0, 0xaa)).unwrap());
        assert_eq!(read(&image, 0), [0xaa; BLOCK_SIZE as usize]);

        // The next write opens a horizon over block 0, and earns a copy of it. The copy
        // follows the write's own live chunk, and carries what the image holds.
        let served = image.serve(write(5, 0xbb)).unwrap();
        let chunks: Vec<_> = served.iter().map(|live| &live.chunk).collect();
        assert_eq!(chunks, [&write(5, 0xbb), &write(0, 0xaa)]);
        live.extend(served);

        // A replay of every live chunk rebuilds the same image.
        let mut replayed = Image::new(fresh_image(), BLOCKS, COPIES);
        for (offset, live) in live.into_iter().enumerate() {
            () = replayed
                .replay(&live.chunk, live.floor_candidate.then_some(offset as i64))
                .unwrap();
        }
        assert!(
            content(&replayed.file) == content(&image.file),
            "the replay differs from the served image"
        );
    }

    /// Serve random writes and punches, with copies. After every request, a replay of
    /// the live chunks so far must rebuild the served image, so each copy must carry
    /// what the image holds over its range.
    #[test]
    fn test_a_replay_rebuilds_the_served_image() {
        // One generated device request.
        #[derive(Clone, Debug)]
        enum Op {
            Write { block: u32, blocks: u32, fill: u8 },
            // A discard or write-zeroes request, which the owner encodes as a punch.
            Punch { block: u32, blocks: u32 },
        }

        impl quickcheck::Arbitrary for Op {
            fn arbitrary(g: &mut quickcheck::Gen) -> Self {
                let block = u32::from(u8::arbitrary(g)) % BLOCKS;
                let blocks = 1 + u32::from(u8::arbitrary(g)) % std::cmp::min(8, BLOCKS - block);

                match u8::arbitrary(g) % 4 {
                    0 | 1 => Op::Punch { block, blocks },
                    _ => Op::Write {
                        block,
                        blocks,
                        fill: u8::arbitrary(g),
                    },
                }
            }
        }

        fn replays_rebuild_the_image(ops: Vec<Op>) -> bool {
            let mut image = Image::new(fresh_image(), BLOCKS, COPIES);
            let mut replayed = Image::new(fresh_image(), BLOCKS, COPIES);

            for op in &ops {
                let chunk = match *op {
                    // Each block holds its fill plus its index, so that a copy of the
                    // wrong blocks differs.
                    Op::Write {
                        block,
                        blocks,
                        fill,
                    } => {
                        let data: Vec<u8> = (block..block + blocks)
                            .flat_map(|b| [fill.wrapping_add(b as u8); BLOCK_SIZE as usize])
                            .collect();
                        chunk::encode_write(block, data.into())
                    }
                    Op::Punch { block, blocks } => chunk::encode_punch(block, blocks),
                };

                // The image doesn't depend on where an opener's record is.
                for live in image.serve(chunk).unwrap() {
                    () = replayed
                        .replay(&live.chunk, live.floor_candidate.then_some(0))
                        .unwrap();
                }
                assert!(
                    content(&replayed.file) == content(&image.file),
                    "the replay differs after {op:?}: {ops:?}"
                );
            }
            true
        }

        // Up to 24 requests, as each compares whole images.
        quickcheck::QuickCheck::new()
            .r#gen(quickcheck::Gen::new(24))
            .tests(256)
            .quickcheck(replays_rebuild_the_image as fn(Vec<Op>) -> bool);
    }

    /// A fresh sparse image of `BLOCKS`.
    fn fresh_image() -> std::fs::File {
        let file = tempfile::tempfile().unwrap();
        file.set_len(BLOCKS as u64 * BLOCK_SIZE as u64).unwrap();
        file
    }

    /// Every byte of `image`.
    fn content(file: &std::fs::File) -> Vec<u8> {
        let mut content = vec![0; BLOCKS as usize * BLOCK_SIZE as usize];
        std::os::unix::fs::FileExt::read_exact_at(file, &mut content, 0).unwrap();
        content
    }

    /// A one-block write of `fill` at `block`, as the owner encodes it.
    fn write(block: u32, fill: u8) -> proto::Chunk {
        chunk::encode_write(block, vec![fill; BLOCK_SIZE as usize].into())
    }

    /// The image's block at `block`, as `Image::read` reads it.
    fn read(image: &Image, block: u32) -> Vec<u8> {
        let mut data = vec![0; BLOCK_SIZE as usize];
        () = image.read(block, &mut data).unwrap();
        data
    }
}
