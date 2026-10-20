use crate::proto;

/// One bit per device block, used for allocation and pending horizon copies.
/// Plain words suffice because each bitmap has one owner. Block indices fit
/// `u32`; `Open` rejects larger devices.
pub type Bitmap = bitvec::vec::BitVec<u64, bitvec::order::Lsb0>;

/// An image and the bitmap which tracks what it has allocated.
pub struct Image {
    /// Sparse file of the disk's blocks, where each unallocated block is a hole.
    file: std::fs::File,
    /// Blocks which hold data rather than a hole.
    allocated: Bitmap,
    /// Set bits of `allocated`, kept as it changes. Counting them would scan the
    /// whole bitmap, and the device weighs this at every write.
    allocated_blocks: u64,
}

impl Image {
    pub fn create(dir: &std::path::Path, blocks: u32) -> std::io::Result<Self> {
        let mut options = std::fs::OpenOptions::new();
        options.read(true).write(true);
        std::os::unix::fs::OpenOptionsExt::custom_flags(&mut options, libc::O_TMPFILE);
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);

        let file = options.open(dir)?;
        file.set_len(blocks as u64 * crate::BLOCK_SIZE as u64)?;

        Ok(Self::from_file(file, blocks))
    }

    /// An image of `blocks` over `file`.
    pub fn from_file(file: std::fs::File, blocks: u32) -> Self {
        Self {
            file,
            allocated: Bitmap::repeat(false, blocks as usize),
            allocated_blocks: 0,
        }
    }

    pub fn allocated(&self) -> &Bitmap {
        &self.allocated
    }

    /// Bytes of the blocks this image has allocated.
    pub fn allocated_bytes(&self) -> u64 {
        self.allocated_blocks * crate::BLOCK_SIZE as u64
    }

    /// Read blocks back, which serves a device read, copies a horizon run out of the
    /// image, and lets a test inspect one.
    pub fn read_at(&self, block: u32, buf: &mut [u8]) -> std::io::Result<()> {
        std::os::unix::fs::FileExt::read_exact_at(&self.file, buf, self.offset(block))
    }

    /// Apply `chunk`, as a device write, punch, or copy does and as a recovery does:
    /// write its data and zero the rest of its range, or deallocate the range.
    /// Returns the blocks it covers.
    pub fn apply(&mut self, chunk: &proto::Chunk) -> std::io::Result<std::ops::Range<u32>> {
        /// The zeroes after a chunk's data, which is never longer than the largest
        /// device request or horizon copy.
        static ZEROES: [u8; super::ublk::MAX_IO_BUF_BYTES as usize] =
            [0; super::ublk::MAX_IO_BUF_BYTES as usize];

        let range = chunk.block..chunk.block + chunk.blocks;
        let (offset, len) = (
            self.offset(range.start),
            range.len() * crate::BLOCK_SIZE as usize,
        );

        // Taken before anything is written, so a range past the disk panics having
        // changed nothing.
        let bits = &mut self.allocated[range.start as usize..range.end as usize];
        let before = bits.count_ones() as u64;

        let allocated = match chunk
            .content
            .as_ref()
            .expect("a journaled chunk has content")
        {
            // The zeroes overwrite whatever an earlier chunk left in the range.
            proto::chunk::Content::Data(data) => {
                let mut bufs = [
                    std::io::IoSlice::new(data),
                    std::io::IoSlice::new(&ZEROES[..len - data.len()]),
                ];
                () = write_all_at(&self.file, &mut bufs, offset)?;
                true
            }
            proto::chunk::Content::Punch(_) => {
                () = punch(&self.file, offset, len as u64)?;
                false
            }
        };
        bits.fill(allocated);

        self.allocated_blocks = match allocated {
            true => self.allocated_blocks + range.len() as u64 - before,
            false => self.allocated_blocks - before,
        };
        Ok(range)
    }

    /// Byte offset at which `block` begins.
    fn offset(&self, block: u32) -> u64 {
        block as u64 * crate::BLOCK_SIZE as u64
    }
}

/// Write every byte of `bufs`, in order, at `offset` of `file`, in one `pwritev`
/// unless the kernel takes fewer bytes than asked.
fn write_all_at(
    file: &std::fs::File,
    mut bufs: &mut [std::io::IoSlice<'_>],
    mut offset: u64,
) -> std::io::Result<()> {
    while !bufs.is_empty() {
        // SAFETY: `IoSlice` is ABI-compatible with `iovec`, and `bufs` and `file`
        // outlive the call.
        let written = unsafe {
            libc::pwritev(
                std::os::fd::AsRawFd::as_raw_fd(file),
                bufs.as_ptr().cast(),
                bufs.len() as libc::c_int,
                offset as libc::off_t,
            )
        };
        if written < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }
        if written == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        offset += written as u64;
        std::io::IoSlice::advance_slices(&mut bufs, written as usize);
    }
    Ok(())
}

/// Deallocate `len` bytes of `file` from `offset`.
fn punch(file: &std::fs::File, offset: u64, len: u64) -> std::io::Result<()> {
    // `FALLOC_FL_KEEP_SIZE` keeps the image's size, which is the device's capacity.
    // SAFETY: `file` keeps the descriptor open across the call, and fallocate reads
    // no user memory.
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
    use crate::device::chunk;
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
        let metadata = image.file.metadata().unwrap();

        let fresh = format!(
            "blocks {}, file len {}, st_blocks {}, directory entries {}\n{}",
            image.allocated().len(),
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

        let data = bytes::Bytes::from(vec![0xab; 3 * BLOCK_SIZE as usize]);
        _ = image.apply(&chunk::encode_write(5, &data)).unwrap();
        writeln!(
            trace,
            "write 5, 3 blocks of 0xab: {}",
            readout(&image, 5..8)
        )
        .unwrap();

        // A punch of the middle block clears only its bit. That block reads back
        // as zeroes, and its neighbours are untouched.
        _ = image.apply(&chunk::encode_punch(6, 1)).unwrap();
        writeln!(trace, "punch 6, 1 block: {}", readout(&image, 5..8)).unwrap();

        insta::assert_snapshot!(trace, @"
        write 5, 3 blocks of 0xab: allocated [5, 6, 7]
          block 5: 0xabx4096
          block 6: 0xabx4096
          block 7: 0xabx4096
        punch 6, 1 block: allocated [5, 7]
          block 5: 0xabx4096
          block 6: 0x00x4096
          block 7: 0xabx4096
        ");
    }

    #[test]
    fn test_punching_an_unallocated_range_is_a_no_op() {
        let dir = tempfile::tempdir().unwrap();
        let mut image = image(&dir);

        let data = bytes::Bytes::from(vec![1; BLOCK_SIZE as usize]);
        _ = image.apply(&chunk::encode_write(0, &data)).unwrap();
        _ = image.apply(&chunk::encode_punch(10, 20)).unwrap();

        assert_eq!(image.allocated().iter_ones().collect::<Vec<_>>(), vec![0]);
        assert_eq!(image.allocated_bytes(), BLOCK_SIZE as u64);
    }

    /// Replay and its round-trip against a direct mutation of a real sparse file.
    mod replay {
        use super::super::Image;
        use crate::BLOCK_SIZE;
        use crate::device::chunk;
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
            let data = bytes::Bytes::from(vec![0xaa; 2 * BLOCK_SIZE as usize]);
            _ = image.apply(&chunk::encode_write(0, &data)).unwrap();
            writeln!(
                trace,
                "two blocks of 0xaa: {}",
                super::readout(&image, 0..2)
            )
            .unwrap();

            _ = image
                .apply(&proto::Chunk {
                    block: 0,
                    blocks: 1,
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

        /// Apply each operation directly to a named sparse file with plain file
        /// operations, and its chunk encoding to an image. The two must then agree on
        /// their content, the image's tracked allocation must match what the
        /// operations allocated, and the filesystem must report both as allocated
        /// alike.
        fn replays_identically(Ops(ops): Ops) -> bool {
            let dir = tempfile::tempdir().unwrap();
            let direct = sparse_file(&dir, "direct");
            let mut replayed = Image::create(dir.path(), DEVICE_BLOCKS).unwrap();

            let mut expected = std::collections::BTreeSet::new();

            for op in &ops {
                let chunk = match op {
                    Op::Write {
                        block,
                        blocks,
                        fill,
                        zero_tail,
                    } => {
                        let data = patterned_data(*blocks, *fill, *zero_tail);
                        let offset = *block as u64 * BLOCK_SIZE as u64;
                        std::os::unix::fs::FileExt::write_all_at(&direct, &data, offset).unwrap();
                        expected.extend(*block..*block + *blocks);

                        chunk::encode_write(*block, &data)
                    }
                    // The daemon encodes both requests as a punch, so the direct
                    // file is punched too.
                    Op::Discard { block, blocks } | Op::WriteZeroes { block, blocks } => {
                        let (offset, len) = (
                            *block as u64 * BLOCK_SIZE as u64,
                            *blocks as u64 * BLOCK_SIZE as u64,
                        );
                        () = super::super::punch(&direct, offset, len).unwrap();
                        for block in *block..*block + *blocks {
                            expected.remove(&block);
                        }
                        chunk::encode_punch(*block, *blocks)
                    }
                };
                _ = replayed.apply(&chunk).unwrap();
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
            assert_eq!(
                replayed.allocated_bytes(),
                expected.len() as u64 * BLOCK_SIZE as u64,
                "allocated bytes differ: {ops:?}"
            );

            let extents = data_extents(&replayed.file);
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
