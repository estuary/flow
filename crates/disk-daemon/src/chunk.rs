use crate::proto::{self, chunk};

/// Encode a device write of `data` beginning at `block`.
///
/// The result begins with one data chunk. It holds the write up to its last
/// non-zero byte. An empty-data chunk then follows for each block of the write
/// which that first chunk does not cover. Neither shape is a hole, so the chunks
/// reproduce the write's footprint as well as its bytes.
///
/// `data` is a positive multiple of [`crate::BLOCK_SIZE`]. Every write the device
/// accepts is block-aligned in both offset and length.
pub fn encode_write(block: u32, data: &bytes::Bytes) -> Vec<proto::Chunk> {
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
        out.push(proto::Chunk {
            block,
            content: Some(chunk::Content::Data(data.slice(..trimmed))),
        });
    }
    out.extend((covered..blocks).map(|offset| proto::Chunk {
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
pub fn encode_punch(block: u32, blocks: u32) -> proto::Chunk {
    assert!(blocks != 0, "a punch is never empty");

    proto::Chunk {
        block,
        content: Some(chunk::Content::Punch(blocks)),
    }
}

/// Range of block indices which `chunk` covers.
///
/// A data chunk covers `max(1, ceil(len(data) / BLOCK_SIZE))` blocks. A chunk
/// with no content at all is malformed and covers nothing, and
/// [`crate::image::Image::apply`] rejects it.
pub fn covered_blocks(chunk: &proto::Chunk) -> std::ops::Range<u32> {
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

/// Data bytes after zero trimming, excluding framing and metadata. This is the
/// compaction budget's unit; punches and allocated zero blocks contribute zero.
pub fn data_bytes(chunks: &[proto::Chunk]) -> u64 {
    chunks
        .iter()
        .map(|chunk| match &chunk.content {
            Some(chunk::Content::Data(data)) => data.len() as u64,
            _ => 0,
        })
        .sum()
}

#[cfg(test)]
mod test {
    use super::{covered_blocks, encode_punch, encode_write};
    use crate::BLOCK_SIZE;
    use crate::proto;
    use crate::test_support;

    fn render(chunks: &[proto::Chunk]) -> String {
        chunks
            .iter()
            .map(test_support::render_chunk)
            .collect::<Vec<_>>()
            .join("\n")
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
        let malformed = proto::Chunk {
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
}
