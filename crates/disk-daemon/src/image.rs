/// One bit per device block, used for allocation and pending horizon copies.
/// Plain words suffice because each bitmap has one owner. Block indices fit
/// `u32`; `Open` rejects larger devices.
pub type Bitmap = bitvec::vec::BitVec<u64, bitvec::order::Lsb0>;

/// A trimmed tail is at most one block.
static ZEROES: [u8; crate::BLOCK_SIZE as usize] = [0; crate::BLOCK_SIZE as usize];

/// An image and the bitmap which tracks what it has allocated.
///
/// Only the disk's owner mutates this, so nothing here is synchronized. The owner
/// serves its disk through these methods, as blocking calls on its own thread.
pub struct Image {
    file: std::fs::File,
    allocated: Bitmap,
}

impl Image {
    /// Create a `blocks` × [`crate::BLOCK_SIZE`] image within `dir`.
    pub fn create(dir: &std::path::Path, blocks: u32) -> std::io::Result<Self> {
        assert!(blocks != 0, "a device has at least one block");

        let mut options = std::fs::OpenOptions::new();
        options.read(true).write(true);
        std::os::unix::fs::OpenOptionsExt::custom_flags(&mut options, libc::O_TMPFILE);
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);

        let file = options.open(dir)?;
        file.set_len(blocks as u64 * crate::BLOCK_SIZE as u64)?;

        Ok(Self {
            file,
            allocated: Bitmap::repeat(false, blocks as usize),
        })
    }

    /// An image of `blocks` over `file`, for a case which needs the host to refuse
    /// it: `/dev/full` refuses every write, and a file opened only for writing
    /// refuses every read.
    #[cfg(test)]
    pub fn from_file(file: std::fs::File, blocks: u32) -> Self {
        Self {
            file,
            allocated: Bitmap::repeat(false, blocks as usize),
        }
    }

    pub fn blocks(&self) -> u32 {
        self.allocated.len() as u32
    }

    pub fn allocated(&self) -> &Bitmap {
        &self.allocated
    }

    /// The file itself, for a case which compares or replays images byte for byte.
    #[cfg(test)]
    pub fn file(&self) -> &std::fs::File {
        &self.file
    }

    /// Read blocks back, which serves a device read, copies a horizon run out of the
    /// image, and lets a test inspect one.
    pub fn read_at(&self, block: u32, buf: &mut [u8]) -> std::io::Result<()> {
        std::os::unix::fs::FileExt::read_exact_at(&self.file, buf, self.offset(block))
    }

    /// Write whole blocks, and record them allocated. This applies a device write.
    ///
    /// A partial block would leave the bitmap describing less than the image holds.
    pub fn write_at(&mut self, block: u32, data: &[u8]) -> std::io::Result<()> {
        let blocks = data.len() / crate::BLOCK_SIZE as usize;
        assert_eq!(
            data.len() % crate::BLOCK_SIZE as usize,
            0,
            "an image write is a whole number of {}-byte blocks",
            crate::BLOCK_SIZE,
        );

        std::os::unix::fs::FileExt::write_all_at(&self.file, data, self.offset(block))?;
        self.allocated[block as usize..block as usize + blocks].fill(true);
        Ok(())
    }

    /// Deallocate `blocks` blocks from `block`, and record them unallocated. This
    /// applies a device discard or write-zeroes.
    pub fn punch(&mut self, block: u32, blocks: u32) -> std::io::Result<()> {
        punch_hole(
            &self.file,
            self.offset(block),
            blocks as u64 * crate::BLOCK_SIZE as u64,
        )?;
        self.allocated[block as usize..(block + blocks) as usize].fill(false);
        Ok(())
    }

    /// Apply a chunk in journal order, with the last change to each block winning.
    /// Zero any trimmed suffix per [`crate::chunk::encode_write`] and reject
    /// out-of-device ranges from untrusted journal content. The caller updates
    /// horizon progress.
    pub fn apply(&mut self, chunk: &crate::proto::Chunk) -> std::io::Result<()> {
        use crate::proto;

        let Some(content) = &chunk.content else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("chunk at block {} has no content", chunk.block),
            ));
        };
        let range = crate::chunk::covered_blocks(chunk);

        if range.end > self.blocks() || range.start >= range.end {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "chunk covers blocks {range:?}, which is not within the device's {} blocks",
                    self.blocks()
                ),
            ));
        }

        let data = match content {
            // The validated range, rather than the count the chunk states: a range
            // which saturated at the last block index is shorter than that count.
            proto::chunk::Content::Punch(_) => {
                return self.punch(range.start, range.end - range.start);
            }
            proto::chunk::Content::Data(data) => data,
        };
        let offset = self.offset(range.start);
        let len = (range.end - range.start) as u64 * crate::BLOCK_SIZE as u64;

        // Without zeroing the trimmed remainder, the tail of a block would keep
        // whatever an earlier chunk left there.
        let Some(pad) = len.checked_sub(data.len() as u64) else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "chunk at block {} carries {} bytes, more than its {len} covered bytes",
                    chunk.block,
                    data.len(),
                ),
            ));
        };
        std::os::unix::fs::FileExt::write_all_at(&self.file, data, offset)?;
        std::os::unix::fs::FileExt::write_all_at(
            &self.file,
            &ZEROES[..pad as usize],
            offset + data.len() as u64,
        )?;

        self.allocated[range.start as usize..range.end as usize].fill(true);
        Ok(())
    }

    /// Byte offset at which `block` begins.
    fn offset(&self, block: u32) -> u64 {
        block as u64 * crate::BLOCK_SIZE as u64
    }
}

/// Deallocate `[offset, offset+len)` of `file`, leaving a hole which reads as
/// zeroes. `FALLOC_FL_KEEP_SIZE` keeps the image's logical size, which is the
/// device's fixed capacity.
fn punch_hole(file: &std::fs::File, offset: u64, len: u64) -> std::io::Result<()> {
    // SAFETY: `file` keeps the descriptor open across the call, and fallocate
    // reads no user memory.
    let rc = unsafe {
        libc::fallocate(
            std::os::fd::AsRawFd::as_raw_fd(file),
            libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
            offset as libc::off_t,
            len as libc::off_t,
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::Image;
    use crate::BLOCK_SIZE;
    use crate::test_support;
    use std::fmt::Write as _;

    const BLOCKS: u32 = 64;

    fn image(dir: &tempfile::TempDir) -> Image {
        Image::create(dir.path(), BLOCKS).unwrap()
    }

    /// Render allocation and per-block byte runs from a single read.
    fn readout(image: &Image, blocks: std::ops::Range<u32>) -> String {
        // Prefill with 0xff so zeroes must come from the read.
        let mut buf = vec![0xffu8; (blocks.end - blocks.start) as usize * BLOCK_SIZE as usize];
        image.read_at(blocks.start, &mut buf).unwrap();

        let mut out = format!(
            "allocated {:?}",
            image.allocated().iter_ones().collect::<Vec<_>>()
        );
        for (block, data) in blocks.zip(buf.chunks(BLOCK_SIZE as usize)) {
            write!(
                out,
                "\n  block {block}: {}",
                test_support::run_lengths(data)
            )
            .unwrap();
        }
        out
    }

    /// A full-size anonymous image starts with no allocated host blocks.
    #[test]
    fn test_fresh_image_is_all_holes() {
        let dir = tempfile::tempdir().unwrap();
        let image = image(&dir);
        let metadata = image.file().metadata().unwrap();

        let fresh = format!(
            "blocks {}, file len {}, st_blocks {}, directory entries {}\n{}",
            image.blocks(),
            metadata.len(),
            std::os::unix::fs::MetadataExt::blocks(&metadata),
            std::fs::read_dir(dir.path()).unwrap().count(),
            readout(&image, BLOCKS - 1..BLOCKS),
        );
        insta::assert_snapshot!(fresh, @"
        blocks 64, file len 262144, st_blocks 0, directory entries 0
        allocated []
          block 63: 0x00x4096
        ");
    }

    #[test]
    fn test_write_read_and_punch() {
        let dir = tempfile::tempdir().unwrap();
        let mut image = image(&dir);
        let mut trace = String::new();

        image
            .write_at(5, &vec![0xab; 3 * BLOCK_SIZE as usize])
            .unwrap();
        writeln!(
            trace,
            "write_at(5, 3 blocks of 0xab): {}",
            readout(&image, 5..8)
        )
        .unwrap();

        // A punch of the middle block clears only its bit. That block reads back
        // as zeroes, and its neighbours are untouched.
        image.punch(6, 1).unwrap();
        writeln!(trace, "punch(6, 1): {}", readout(&image, 5..8)).unwrap();

        insta::assert_snapshot!(trace, @"
        write_at(5, 3 blocks of 0xab): allocated [5, 6, 7]
          block 5: 0xabx4096
          block 6: 0xabx4096
          block 7: 0xabx4096
        punch(6, 1): allocated [5, 7]
          block 5: 0xabx4096
          block 6: 0x00x4096
          block 7: 0xabx4096
        ");
    }

    #[test]
    fn test_punching_an_unallocated_range_is_a_no_op() {
        let dir = tempfile::tempdir().unwrap();
        let mut image = image(&dir);

        image.write_at(0, &vec![1; BLOCK_SIZE as usize]).unwrap();
        image.punch(10, 20).unwrap();

        assert_eq!(image.allocated().iter_ones().collect::<Vec<_>>(), vec![0]);
    }

    /// Replay and its round-trip against a direct mutation of a real sparse file.
    mod replay {
        use super::super::{Image, punch_hole};
        use crate::BLOCK_SIZE;
        use crate::chunk;
        use crate::proto;
        use std::fmt::Write as _;

        /// Small enough that a property case runs in microseconds. Large enough
        /// to span several bitmap words and many chunk spans.
        const DEVICE_BLOCKS: u32 = 64;
        const MAX_OP_BLOCKS: u32 = 8;

        #[test]
        fn test_replay_zero_fills_a_trimmed_tail() {
            let dir = tempfile::tempdir().unwrap();
            let mut image = Image::create(dir.path(), DEVICE_BLOCKS).unwrap();
            let mut trace = String::new();

            // Two blocks of content, then a ten-byte rewrite of the first block.
            // The trimmed tail of the rewrite must not keep the older value.
            for chunk in
                chunk::encode_write(0, &bytes::Bytes::from(vec![0xaa; 2 * BLOCK_SIZE as usize]))
            {
                image.apply(&chunk).unwrap();
            }
            writeln!(
                trace,
                "two blocks of 0xaa: {}",
                super::readout(&image, 0..2)
            )
            .unwrap();

            image
                .apply(&proto::Chunk {
                    block: 0,
                    content: Some(proto::chunk::Content::Data(bytes::Bytes::from_static(
                        b"0123456789",
                    ))),
                })
                .unwrap();
            writeln!(
                trace,
                "ten-byte rewrite of block 0: {}",
                super::readout(&image, 0..2)
            )
            .unwrap();

            insta::assert_snapshot!(trace, @"
            two blocks of 0xaa: allocated [0, 1]
              block 0: 0xaax4096
              block 1: 0xaax4096
            ten-byte rewrite of block 0: allocated [0, 1]
              block 0: 0x30x1 0x31x1 0x32x1 0x33x1 0x34x1 0x35x1 0x36x1 0x37x1 0x38x1 0x39x1 0x00x4086
              block 1: 0xaax4096
            ");
        }

        #[test]
        fn test_apply_rejects_malformed_chunks() {
            let dir = tempfile::tempdir().unwrap();
            let mut image = Image::create(dir.path(), DEVICE_BLOCKS).unwrap();

            let cases = [
                (
                    "no content",
                    proto::Chunk {
                        block: 0,
                        content: None,
                    },
                ),
                // A punch of no blocks covers nothing.
                (
                    "empty punch",
                    proto::Chunk {
                        block: 0,
                        content: Some(proto::chunk::Content::Punch(0)),
                    },
                ),
                // Reaching one block past the end of the device.
                (
                    "punch past the end",
                    proto::Chunk {
                        block: DEVICE_BLOCKS - 1,
                        content: Some(proto::chunk::Content::Punch(2)),
                    },
                ),
                (
                    "data past the end",
                    proto::Chunk {
                        block: DEVICE_BLOCKS,
                        content: Some(proto::chunk::Content::Data(bytes::Bytes::new())),
                    },
                ),
                // Overflowing the block index outright.
                (
                    "overflowing punch",
                    proto::Chunk {
                        block: u32::MAX,
                        content: Some(proto::chunk::Content::Punch(u32::MAX)),
                    },
                ),
            ];
            let mut trace = String::new();
            for (label, case) in cases {
                let err = image.apply(&case).unwrap_err();
                writeln!(trace, "{label:<20}{:?}: {err:#}", err.kind()).unwrap();
            }
            write!(
                trace,
                "allocated {:?}",
                image.allocated().iter_ones().collect::<Vec<_>>()
            )
            .unwrap();

            insta::assert_snapshot!(trace, @"
            no content          InvalidData: chunk at block 0 has no content
            empty punch         InvalidData: chunk covers blocks 0..0, which is not within the device's 64 blocks
            punch past the end  InvalidData: chunk covers blocks 63..65, which is not within the device's 64 blocks
            data past the end   InvalidData: chunk covers blocks 64..65, which is not within the device's 64 blocks
            overflowing punch   InvalidData: chunk covers blocks 4294967295..4294967295, which is not within the device's 64 blocks
            allocated []
            ");
        }

        /// One generated device request.
        #[derive(Clone, Debug)]
        enum Op {
            Write {
                block: u32,
                blocks: u32,
                fill: u8,
                zero_tail: u32,
            },
            Discard {
                block: u32,
                blocks: u32,
            },
            WriteZeroes {
                block: u32,
                blocks: u32,
            },
        }

        impl quickcheck::Arbitrary for Op {
            fn arbitrary(g: &mut quickcheck::Gen) -> Self {
                let block = u32::from(u8::arbitrary(g)) % DEVICE_BLOCKS;
                let blocks = 1 + u32::from(u8::arbitrary(g))
                    % std::cmp::min(MAX_OP_BLOCKS, DEVICE_BLOCKS - block);
                let span = blocks * BLOCK_SIZE;

                match u8::arbitrary(g) % 4 {
                    0 => Op::Discard { block, blocks },
                    1 => Op::WriteZeroes { block, blocks },
                    _ => Op::Write {
                        block,
                        blocks,
                        fill: u8::arbitrary(g),
                        // Biased towards trim boundaries. The cases are no
                        // trim, a full trim, whole trailing blocks, and an
                        // arbitrary cut within a block.
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

        /// Content of a generated write. The interior bytes take every value,
        /// zero included. The final `zero_tail` bytes are zero.
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

        fn sparse_file(dir: &tempfile::TempDir, name: &str) -> std::fs::File {
            let file = std::fs::File::options()
                .read(true)
                .write(true)
                .create_new(true)
                .open(dir.path().join(name))
                .unwrap();
            file.set_len(DEVICE_BLOCKS as u64 * BLOCK_SIZE as u64)
                .unwrap();
            file
        }

        fn write_at(file: &std::fs::File, block: u32, data: &[u8]) {
            std::os::unix::fs::FileExt::write_all_at(file, data, block as u64 * BLOCK_SIZE as u64)
                .unwrap();
        }

        /// Byte ranges the filesystem reports as allocated. This calls `sync_all`
        /// first, so that ext4's delayed allocation has resolved into extents.
        fn data_extents(file: &std::fs::File) -> Vec<(u64, u64)> {
            file.sync_all().unwrap();
            let fd = std::os::fd::AsRawFd::as_raw_fd(file);
            let size = DEVICE_BLOCKS as i64 * BLOCK_SIZE as i64;

            let mut extents = Vec::new();
            let mut cursor = 0;

            while cursor < size {
                // SAFETY: `file` holds the descriptor open across both calls.
                let start = unsafe { libc::lseek(fd, cursor, libc::SEEK_DATA) };
                if start < 0 {
                    break; // ENXIO: no data at or after `cursor`.
                }
                let end = unsafe { libc::lseek(fd, start, libc::SEEK_HOLE) };
                assert!(end > start, "SEEK_HOLE must advance past SEEK_DATA");

                extents.push((start as u64, end as u64));
                cursor = end;
            }
            extents
        }

        /// Apply each operation directly to a sparse file, and its chunk encoding to
        /// an image. The two must then agree on their content, the image's tracked
        /// allocation must match what the operations allocated, and the filesystem
        /// must report both as allocated alike.
        fn replays_identically(Ops(ops): Ops) -> bool {
            let dir = tempfile::tempdir().unwrap();
            let direct = sparse_file(&dir, "direct");
            let mut replayed = Image::create(dir.path(), DEVICE_BLOCKS).unwrap();

            let mut expected = std::collections::BTreeSet::new();

            for op in &ops {
                let chunks = match op {
                    Op::Write {
                        block,
                        blocks,
                        fill,
                        zero_tail,
                    } => {
                        let data = patterned_data(*blocks, *fill, *zero_tail);
                        write_at(&direct, *block, &data);
                        expected.extend(*block..*block + *blocks);

                        chunk::encode_write(*block, &data)
                    }
                    // The daemon encodes both requests as a punch, so the direct
                    // image punches too.
                    Op::Discard { block, blocks } | Op::WriteZeroes { block, blocks } => {
                        punch_hole(
                            &direct,
                            *block as u64 * BLOCK_SIZE as u64,
                            *blocks as u64 * BLOCK_SIZE as u64,
                        )
                        .unwrap();
                        for block in *block..*block + *blocks {
                            expected.remove(&block);
                        }
                        vec![chunk::encode_punch(*block, *blocks)]
                    }
                };
                for chunk in &chunks {
                    replayed.apply(chunk).unwrap();
                }
            }

            let mut content = vec![0; DEVICE_BLOCKS as usize * BLOCK_SIZE as usize];
            replayed.read_at(0, &mut content).unwrap();
            assert_eq!(
                std::fs::read(dir.path().join("direct")).unwrap(),
                content,
                "replayed image content differs: {ops:?}"
            );
            assert_eq!(
                replayed
                    .allocated()
                    .iter_ones()
                    .map(|block| block as u32)
                    .collect::<Vec<_>>(),
                expected.iter().copied().collect::<Vec<_>>(),
                "allocated bitmap differs: {ops:?}"
            );

            let extents = data_extents(replayed.file());
            assert_eq!(
                data_extents(&direct),
                extents,
                "filesystem allocation differs: {ops:?}"
            );
            for block in replayed.allocated().iter_ones() {
                let offset = block as u64 * BLOCK_SIZE as u64;
                assert!(
                    extents
                        .iter()
                        .any(|&(start, end)| offset >= start && offset < end),
                    "block {block} is tracked as allocated but the filesystem reports a hole: {ops:?}"
                );
            }
            true
        }

        #[test]
        fn test_encode_and_replay_reproduce_direct_mutation() {
            quickcheck::QuickCheck::new()
                .tests(64)
                .quickcheck(replays_identically as fn(Ops) -> bool);
        }
    }
}
