use crate::proto::{self, chunk};

/// Encode a device write of `data` beginning at `block`.
///
/// The result begins with one data chunk. It holds the write up to its last
/// non-zero byte. An empty-data chunk then follows for each block of the write
/// which that first chunk does not cover.
pub fn encode_write(block: u32, data: &bytes::Bytes) -> Vec<proto::Chunk> {
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
pub fn encode_punch(block: u32, blocks: u32) -> proto::Chunk {
    assert!(blocks != 0, "a punch is never empty");

    proto::Chunk {
        block,
        content: Some(chunk::Content::Punch(blocks)),
    }
}

/// Range of block indices which `chunk` covers.
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
    use super::{encode_punch, encode_write};
    use crate::BLOCK_SIZE;
    use crate::proto;
    use crate::test_support;

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
    fn test_chunk_encoding() {
        let cases = [
            (
                "no trailing zeroes",
                encode_write(0, &write_data(2, 0xaa, 0)),
            ),
            // Replay zeroes the rest of the second block.
            (
                "ends mid-block",
                encode_write(7, &write_data(2, 0xaa, 4092)),
            ),
            // Each whole zero block stays allocated rather than being dropped.
            (
                "zero tail blocks",
                encode_write(100, &write_data(4, 0xaa, 3 * BLOCK_SIZE as usize)),
            ),
            ("all zero", encode_write(5, &write_data(3, 0x00, 0))),
            ("punch", vec![encode_punch(11, 4)]),
            // A chunk decoded from a journal may have no content at all.
            (
                "no content",
                vec![proto::Chunk {
                    block: 3,
                    content: None,
                }],
            ),
        ];

        let mut out = Vec::new();
        for (case, chunks) in cases {
            for chunk in chunks {
                out.push(format!("{case:<20}{}", test_support::render_chunk(&chunk)));
            }
        }
        insta::assert_snapshot!(out.join("\n"), @"
        no trailing zeroes  blocks 0..2: data len 8192 [0xaax8192]
        ends mid-block      blocks 7..9: data len 4100 [0xaax4100]
        zero tail blocks    blocks 100..101: data len 4096 [0xaax4096]
        zero tail blocks    blocks 101..102: empty data (one zeroed block)
        zero tail blocks    blocks 102..103: empty data (one zeroed block)
        zero tail blocks    blocks 103..104: empty data (one zeroed block)
        all zero            blocks 5..6: empty data (one zeroed block)
        all zero            blocks 6..7: empty data (one zeroed block)
        all zero            blocks 7..8: empty data (one zeroed block)
        punch               blocks 11..15: punch 4
        no content          blocks 3..3: no content
        ");
    }
}
