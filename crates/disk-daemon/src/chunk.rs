//! The chunk codec. It turns a device mutation into durable journal content, and
//! journal content into a rebuilt image.
//!
//! Two rules carry all of the subtlety:
//!
//! - [`encode_write`] trims trailing zero bytes, so a data chunk may end within
//!   its last block. Replay must zero the range from `len(data)` to the end of
//!   the covered blocks. Otherwise an older value survives in that tail.
//! - A chunk with empty `data` is one allocated block of zeroes, not a hole.
//!   Only a `punch` deallocates. An all-zero write therefore encodes to a few
//!   bytes and still reproduces the allocation the device saw.

use crate::bitmap::Bitmap;
use crate::proto::{Chunk, chunk};

/// Encode a device write of `data` beginning at `block`.
///
/// The result begins with one data chunk. It holds the write up to its last
/// non-zero byte. An empty-data chunk then follows for each block of the write
/// which that first chunk does not cover. Neither shape is a hole, so the chunks
/// reproduce the write's footprint as well as its bytes.
///
/// `data` is a positive multiple of [`crate::BLOCK_SIZE`]. Every write the device
/// accepts is block-aligned in both offset and length.
pub fn encode_write(block: u32, data: &bytes::Bytes) -> Vec<Chunk> {
    assert!(!data.is_empty(), "a device write is never empty");
    assert_eq!(
        data.len() % crate::BLOCK_SIZE as usize,
        0,
        "a device write is a whole number of {}-byte blocks",
        crate::BLOCK_SIZE,
    );
    let blocks = (data.len() / crate::BLOCK_SIZE as usize) as u32;

    let trimmed = data.len() - data.iter().rev().take_while(|&&b| b == 0).count();
    let covered = trimmed.div_ceil(crate::BLOCK_SIZE as usize) as u32;

    let mut out = Vec::with_capacity(1 + (blocks - covered) as usize);

    if trimmed != 0 {
        out.push(Chunk {
            block,
            content: Some(chunk::Content::Data(data.slice(..trimmed))),
        });
    }
    out.extend((covered..blocks).map(|offset| Chunk {
        block: block + offset,
        content: Some(chunk::Content::Data(bytes::Bytes::new())),
    }));

    out
}

/// Encode a device discard or write-zeroes request of `blocks` blocks
/// beginning at `block`.
///
/// Both encode identically. An unallocated block reads as zeroes, and
/// deallocation keeps the rebuilt image sparse.
pub fn encode_punch(block: u32, blocks: u32) -> Chunk {
    assert!(blocks != 0, "a punch is never empty");

    Chunk {
        block,
        content: Some(chunk::Content::Punch(blocks)),
    }
}

/// Range of block indices which `chunk` covers.
///
/// A data chunk covers `max(1, ceil(len(data) / BLOCK_SIZE))` blocks. A chunk
/// with no content at all is malformed and covers nothing, and [`apply`] rejects
/// it.
pub fn covered_blocks(chunk: &Chunk) -> std::ops::Range<u32> {
    let covered = match &chunk.content {
        Some(chunk::Content::Data(data)) => {
            let blocks = std::cmp::max(1, (data.len() as u64).div_ceil(crate::BLOCK_SIZE as u64));
            u32::try_from(blocks).unwrap_or(u32::MAX)
        }
        Some(chunk::Content::Punch(blocks)) => *blocks,
        None => 0,
    };
    chunk.block..chunk.block.saturating_add(covered)
}

/// Content bytes `chunks` carry, excluding framing. A journal grows by this much
/// when they are appended.
///
/// Compaction rations itself by this count, so a punch and a zeroed block are
/// nearly free to copy.
pub fn data_bytes(chunks: &[Chunk]) -> u64 {
    chunks
        .iter()
        .map(|chunk| match &chunk.content {
            Some(chunk::Content::Data(data)) => data.len() as u64,
            _ => 0,
        })
        .sum()
}

/// Apply `chunk` to `file` and to the `allocated` bitmap which tracks it.
///
/// A caller applies chunks in journal order, and the last chunk to cover a block
/// wins. This is therefore a plain forward replay with no ordering state of its
/// own. `allocated` supplies the device's block count. A chunk which reaches
/// beyond that count is rejected, because a chunk read from a journal is
/// untrusted input.
pub fn apply(chunk: &Chunk, file: &std::fs::File, allocated: &mut Bitmap) -> std::io::Result<()> {
    let Some(content) = &chunk.content else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("chunk at block {} has no content", chunk.block),
        ));
    };
    let range = covered_blocks(chunk);

    if range.end > allocated.blocks() || range.start >= range.end {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "chunk covers blocks {range:?}, which is not within the device's {} blocks",
                allocated.blocks()
            ),
        ));
    }
    let offset = range.start as u64 * crate::BLOCK_SIZE as u64;
    let len = (range.end - range.start) as u64 * crate::BLOCK_SIZE as u64;

    match content {
        chunk::Content::Data(data) => {
            // Without zeroing the trimmed remainder, the tail of a block would
            // keep whatever an earlier chunk left there.
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
            std::os::unix::fs::FileExt::write_all_at(file, data, offset)?;
            std::os::unix::fs::FileExt::write_all_at(
                file,
                &vec![0u8; pad as usize],
                offset + data.len() as u64,
            )?;

            for block in range {
                allocated.set(block);
            }
        }
        chunk::Content::Punch(_) => {
            crate::image::punch_hole(file, offset, len)?;

            for block in range {
                allocated.clear(block);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::{Chunk, chunk, covered_blocks, encode_punch, encode_write};
    use crate::BLOCK_SIZE;

    /// Render chunks as one line each. Data is run-length encoded, so a whole
    /// block of content is a few characters.
    fn render(chunks: &[Chunk]) -> String {
        chunks
            .iter()
            .map(|chunk| {
                let range = covered_blocks(chunk);
                let what = match &chunk.content {
                    Some(chunk::Content::Data(data)) if data.is_empty() => {
                        "empty data (one zeroed block)".to_string()
                    }
                    Some(chunk::Content::Data(data)) => {
                        format!("data len {} [{}]", data.len(), run_lengths(data))
                    }
                    Some(chunk::Content::Punch(blocks)) => format!("punch {blocks}"),
                    None => "no content".to_string(),
                };
                format!("blocks {}..{}: {what}", range.start, range.end)
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn run_lengths(data: &[u8]) -> String {
        let mut runs: Vec<(u8, usize)> = Vec::new();
        for &byte in data {
            match runs.last_mut() {
                Some((value, count)) if *value == byte => *count += 1,
                _ => runs.push((byte, 1)),
            }
        }
        runs.iter()
            .map(|(value, count)| format!("{value:#04x}x{count}"))
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// A `blocks`-block write of `fill` bytes whose final `zero_tail` bytes are zero.
    fn write_data(blocks: u32, fill: u8, zero_tail: usize) -> bytes::Bytes {
        let span = (blocks * BLOCK_SIZE) as usize;
        let body = span - zero_tail;
        bytes::Bytes::from(
            (0..span)
                .map(|i| if i < body { fill } else { 0 })
                .collect::<Vec<u8>>(),
        )
    }

    #[test]
    fn test_encode_write_without_trailing_zeroes() {
        let chunks = encode_write(0, &write_data(2, 0xaa, 0));
        insta::assert_snapshot!("write_no_trailing_zeroes", render(&chunks));
    }

    #[test]
    fn test_encode_write_ending_mid_block_after_trim() {
        // Four bytes into the second block. The chunk still covers both blocks,
        // and replay must zero the remainder of the second.
        let chunks = encode_write(7, &write_data(2, 0xaa, 4092));
        insta::assert_snapshot!("write_ends_mid_block", render(&chunks));
    }

    #[test]
    fn test_encode_write_trimming_a_single_byte() {
        let chunks = encode_write(0, &write_data(1, 0xaa, 1));
        insta::assert_snapshot!("write_trims_final_byte", render(&chunks));
    }

    #[test]
    fn test_encode_write_with_all_zero_tail_blocks() {
        // Three trailing blocks are entirely zero, and each becomes its own
        // allocated zero block rather than being dropped.
        let chunks = encode_write(100, &write_data(4, 0xaa, 3 * BLOCK_SIZE as usize));
        insta::assert_snapshot!("write_zero_tail_blocks", render(&chunks));
    }

    #[test]
    fn test_encode_all_zero_write() {
        let chunks = encode_write(5, &write_data(3, 0x00, 0));
        insta::assert_snapshot!("write_all_zero", render(&chunks));
    }

    #[test]
    fn test_encode_single_block_zero_write() {
        let chunks = encode_write(9, &write_data(1, 0x00, 0));
        insta::assert_snapshot!("write_single_block_zero", render(&chunks));
    }

    #[test]
    fn test_encode_punch() {
        insta::assert_snapshot!("punch", render(&[encode_punch(11, 4)]));
    }

    #[test]
    fn test_covered_blocks_of_malformed_chunk() {
        // A chunk decoded from a journal may have no content at all.
        let malformed = Chunk {
            block: 3,
            content: None,
        };
        assert_eq!(covered_blocks(&malformed), 3..3);
    }

    #[test]
    #[should_panic(expected = "whole number of 4096-byte blocks")]
    fn test_unaligned_write_panics() {
        _ = encode_write(0, &bytes::Bytes::from_static(&[1, 2, 3]));
    }

    /// Replay and its round-trip against a real sparse file. Hole punching,
    /// `SEEK_DATA`, and `SEEK_HOLE` are Linux interfaces.
    #[cfg(target_os = "linux")]
    mod replay {
        use super::{BLOCK_SIZE, Chunk, chunk, encode_punch, encode_write};
        use crate::bitmap::Bitmap;
        use crate::chunk::apply;

        /// Small enough that a property case runs in microseconds. Large enough
        /// to span several bitmap words and many chunk spans.
        const DEVICE_BLOCKS: u32 = 64;
        const MAX_OP_BLOCKS: u32 = 8;

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

        #[test]
        fn test_replay_zero_fills_a_trimmed_tail() {
            let dir = tempfile::tempdir().unwrap();
            let file = sparse_file(&dir, "image");
            let mut allocated = Bitmap::new(DEVICE_BLOCKS);

            // Two blocks of content, then a ten-byte rewrite of the first block.
            // The trimmed tail of the rewrite must not keep the older value.
            for chunk in encode_write(0, &bytes::Bytes::from(vec![0xaa; 2 * BLOCK_SIZE as usize])) {
                apply(&chunk, &file, &mut allocated).unwrap();
            }
            apply(
                &Chunk {
                    block: 0,
                    content: Some(chunk::Content::Data(bytes::Bytes::from_static(
                        b"0123456789",
                    ))),
                },
                &file,
                &mut allocated,
            )
            .unwrap();

            let image = std::fs::read(dir.path().join("image")).unwrap();
            assert_eq!(&image[..10], b"0123456789");
            assert!(image[10..BLOCK_SIZE as usize].iter().all(|&b| b == 0));
            assert!(
                image[BLOCK_SIZE as usize..2 * BLOCK_SIZE as usize]
                    .iter()
                    .all(|&b| b == 0xaa)
            );
            assert_eq!(allocated.iter().collect::<Vec<_>>(), vec![0, 1]);
        }

        #[test]
        fn test_apply_rejects_malformed_chunks() {
            let dir = tempfile::tempdir().unwrap();
            let file = sparse_file(&dir, "image");
            let mut allocated = Bitmap::new(DEVICE_BLOCKS);

            let cases = [
                // No content at all.
                Chunk {
                    block: 0,
                    content: None,
                },
                // A punch of no blocks covers nothing.
                Chunk {
                    block: 0,
                    content: Some(chunk::Content::Punch(0)),
                },
                // Reaching one block past the end of the device.
                Chunk {
                    block: DEVICE_BLOCKS - 1,
                    content: Some(chunk::Content::Punch(2)),
                },
                Chunk {
                    block: DEVICE_BLOCKS,
                    content: Some(chunk::Content::Data(bytes::Bytes::new())),
                },
                // Overflowing the block index outright.
                Chunk {
                    block: u32::MAX,
                    content: Some(chunk::Content::Punch(u32::MAX)),
                },
            ];
            for case in cases {
                let err = apply(&case, &file, &mut allocated).unwrap_err();
                assert_eq!(err.kind(), std::io::ErrorKind::InvalidData, "{case:?}");
            }
            assert_eq!(allocated.count_ones(), 0);
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

        /// Apply each operation directly to one image, and its chunk encoding to
        /// another. The two images must then agree on their content, on their
        /// tracked allocation, and on what the filesystem reports as allocated.
        fn replays_identically(Ops(ops): Ops) -> bool {
            let dir = tempfile::tempdir().unwrap();
            let direct = sparse_file(&dir, "direct");
            let replayed = sparse_file(&dir, "replayed");

            let mut allocated = Bitmap::new(DEVICE_BLOCKS);
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

                        encode_write(*block, &data)
                    }
                    // The daemon encodes both requests as a punch, so the direct
                    // image punches too.
                    Op::Discard { block, blocks } | Op::WriteZeroes { block, blocks } => {
                        crate::image::punch_hole(
                            &direct,
                            *block as u64 * BLOCK_SIZE as u64,
                            *blocks as u64 * BLOCK_SIZE as u64,
                        )
                        .unwrap();
                        for block in *block..*block + *blocks {
                            expected.remove(&block);
                        }
                        vec![encode_punch(*block, *blocks)]
                    }
                };
                for chunk in &chunks {
                    apply(chunk, &replayed, &mut allocated).unwrap();
                }
            }

            assert_eq!(
                std::fs::read(dir.path().join("direct")).unwrap(),
                std::fs::read(dir.path().join("replayed")).unwrap(),
                "replayed image content differs: {ops:?}"
            );
            assert_eq!(
                allocated.iter().collect::<Vec<_>>(),
                expected.iter().copied().collect::<Vec<_>>(),
                "allocated bitmap differs: {ops:?}"
            );

            let extents = data_extents(&replayed);
            assert_eq!(
                data_extents(&direct),
                extents,
                "filesystem allocation differs: {ops:?}"
            );
            for block in allocated.iter() {
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
