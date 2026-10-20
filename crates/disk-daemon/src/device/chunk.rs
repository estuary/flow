use crate::proto::{self, chunk};

/// Encode a device write of `data` beginning at `block`, as one chunk covering every
/// block it writes. The chunk's data stops at the write's last non-zero byte, and
/// its range zeroes the rest.
pub fn encode_write(block: u32, data: &bytes::Bytes) -> proto::Chunk {
    let trimmed = data.len() - data.iter().rev().take_while(|&&b| b == 0).count();

    proto::Chunk {
        block,
        blocks: (data.len() / crate::BLOCK_SIZE as usize) as u32,
        content: Some(chunk::Content::Data(data.slice(..trimmed))),
    }
}

/// Encode a device discard or write-zeroes request of `blocks` blocks
/// beginning at `block`.
pub fn encode_punch(block: u32, blocks: u32) -> proto::Chunk {
    proto::Chunk {
        block,
        blocks,
        content: Some(chunk::Content::Punch(chunk::Punch {})),
    }
}

/// Data bytes after zero trimming, excluding framing and metadata. This is the
/// compaction budget's unit; punches and allocated zero blocks contribute zero.
pub fn data_bytes(chunk: &proto::Chunk) -> u64 {
    match &chunk.content {
        Some(chunk::Content::Data(data)) => data.len() as u64,
        _ => 0,
    }
}

#[cfg(test)]
mod test {
    use super::{encode_punch, encode_write};
    use crate::BLOCK_SIZE;
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
            ("punch", encode_punch(11, 4)),
        ];

        let mut out = Vec::new();
        for (case, chunk) in cases {
            out.push(format!("{case:<20}{}", test_support::render_chunk(&chunk)));
        }
        insta::assert_snapshot!(out.join("\n"), @"
        no trailing zeroes  blocks 0..2: data len 8192 [0xaax8192]
        ends mid-block      blocks 7..9: data len 4100 [0xaax4100]
        zero tail blocks    blocks 100..104: data len 4096 [0xaax4096]
        all zero            blocks 5..8: zeroed
        punch               blocks 11..15: punch
        ");
    }
}
