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

/// Encode a device discard or write-zeroes request.
pub fn encode_punch(block: u32, blocks: u32) -> proto::Chunk {
    proto::Chunk {
        block,
        blocks,
        content: Some(chunk::Content::Punch(chunk::Punch {})),
    }
}

#[cfg(test)]
mod test {
    use super::{encode_punch, encode_write};
    use crate::proto::{self, chunk};

    const BLOCK: usize = crate::BLOCK_SIZE as usize;

    #[test]
    fn test_encode_a_write_and_a_discard() {
        // Two blocks from block 10, such as the end of a file: a block of content,
        // then 100 bytes of content and zeroes to the end of the block.
        let write = filled(2, BLOCK + 100);

        assert_eq!(
            encode_write(10, &bytes::Bytes::from(write.clone())),
            proto::Chunk {
                block: 10,
                blocks: 2,
                content: Some(chunk::Content::Data(bytes::Bytes::copy_from_slice(
                    &write[..BLOCK + 100]
                ))),
            }
        );

        // A discard of 8 blocks from block 20, such as a deleted file's blocks.
        assert_eq!(
            encode_punch(20, 8),
            proto::Chunk {
                block: 20,
                blocks: 8,
                content: Some(chunk::Content::Punch(chunk::Punch {})),
            }
        );
    }

    #[test]
    fn test_encode_write_trims_only_trailing_zeroes() {
        // Zeroes before the last non-zero byte are content, not a tail.
        let mut interior = vec![0; 2 * BLOCK];
        interior[0] = 0xaa;
        interior[99] = 0xbb;

        // (case, write, length of the chunk's data)
        let cases = [
            ("no trailing zeroes", filled(2, 2 * BLOCK), 2 * BLOCK),
            ("ends mid-block", filled(2, BLOCK + 4), BLOCK + 4),
            ("whole zero blocks", filled(4, BLOCK), BLOCK),
            ("all zero", filled(3, 0), 0),
            ("interior zeroes", interior, 100),
        ];

        for (case, write, kept) in cases {
            let chunk = encode_write(7, &bytes::Bytes::from(write.clone()));

            // The range covers every block of the write, zero blocks included.
            assert_eq!(
                (chunk.block, chunk.blocks),
                (7, (write.len() / BLOCK) as u32),
                "{case}"
            );

            let Some(chunk::Content::Data(data)) = &chunk.content else {
                panic!("{case}: not a data chunk");
            };
            assert_eq!(data.len(), kept, "{case}");

            // Replay writes the data and zeroes the rest of the range.
            let mut replayed = data.to_vec();
            replayed.resize(chunk.blocks as usize * BLOCK, 0);
            assert!(replayed == write, "{case}: replay differs from the write");
        }
    }

    /// `blocks` blocks whose first `filled` bytes are `0xaa`, and the rest zero.
    fn filled(blocks: usize, filled: usize) -> Vec<u8> {
        let mut data = vec![0; blocks * BLOCK];
        data[..filled].fill(0xaa);
        data
    }
}
