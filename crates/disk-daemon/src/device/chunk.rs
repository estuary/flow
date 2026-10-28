use super::ublk;
use crate::proto;

/// The zeroes after a chunk's data, which is never longer than the largest device
/// request or horizon copy.
static ZEROES: [u8; ublk::MAX_IO_BUF_BYTES as usize] = [0; ublk::MAX_IO_BUF_BYTES as usize];

/// Encode a device write of `data` beginning at `block`, as one chunk covering every
/// block it writes. The chunk's data stops at the write's last non-zero byte, and
/// its range zeroes the rest.
pub(super) fn encode_write(block: u32, data: bytes::Bytes) -> proto::Chunk {
    let trimmed = data.len() - data.iter().rev().take_while(|&&b| b == 0).count();

    proto::Chunk {
        block,
        blocks: (data.len() / crate::BLOCK_SIZE as usize) as u32,
        content: Some(proto::chunk::Content::Data(data.slice(..trimmed))),
    }
}

/// Encode a device discard or write-zeroes request.
pub(super) fn encode_punch(block: u32, blocks: u32) -> proto::Chunk {
    proto::Chunk {
        block,
        blocks,
        content: Some(proto::chunk::Content::Punch(proto::chunk::Punch {})),
    }
}

/// Apply `chunk` to `image`. A write's data is followed by zeroes through the
/// end of its range, which `encode_write` trimmed, over whatever the image held
/// there.
pub(super) fn apply(image: &std::fs::File, chunk: &proto::Chunk) -> std::io::Result<()> {
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
            () = std::os::unix::fs::FileExt::write_all_at(image, data, offset)?;
            std::os::unix::fs::FileExt::write_all_at(
                image,
                &ZEROES[..len - data.len()],
                offset + data.len() as u64,
            )
        }
        // `KEEP_SIZE` keeps the image's size, which is the device's capacity.
        proto::chunk::Content::Punch(_) => rustix::fs::fallocate(
            image,
            rustix::fs::FallocateFlags::PUNCH_HOLE | rustix::fs::FallocateFlags::KEEP_SIZE,
            offset,
            len as u64,
        )
        .map_err(std::io::Error::from),
    }
}

#[cfg(test)]
mod test {
    use super::{apply, encode_punch, encode_write};
    use crate::proto;

    const BLOCK: usize = crate::BLOCK_SIZE as usize;

    #[test]
    fn test_encode_write_trims_only_trailing_zeroes() {
        // Zeroes before the last non-zero byte are content, not a tail.
        let mut interior = vec![0; 2 * BLOCK];
        interior[0] = 0xaa;
        interior[99] = 0xbb;

        // (name, write, length of the chunk's data)
        let cases = [
            ("no trailing zeroes", filled(2, 2 * BLOCK), 2 * BLOCK),
            ("ends mid-block", filled(2, BLOCK + 4), BLOCK + 4),
            ("whole zero blocks", filled(4, BLOCK), BLOCK),
            ("all zero", filled(3, 0), 0),
            ("interior zeroes", interior, 100),
        ];

        for (name, write, kept) in cases {
            let chunk = encode_write(7, bytes::Bytes::from(write.clone()));

            // The range covers every block of the write, zero blocks included.
            assert_eq!(
                (chunk.block, chunk.blocks),
                (7, (write.len() / BLOCK) as u32),
                "{name}"
            );

            let Some(proto::chunk::Content::Data(data)) = &chunk.content else {
                panic!("{name}: not a data chunk");
            };
            assert_eq!(data.len(), kept, "{name}");

            // Applied over older content, the chunk leaves exactly what the write did,
            // its trimmed zeroes included.
            let (image, offset) = (tempfile::tempfile().unwrap(), 7 * BLOCK as u64);
            () = std::os::unix::fs::FileExt::write_all_at(&image, &vec![0xff; write.len()], offset)
                .unwrap();
            () = apply(&image, &chunk).unwrap();

            let mut applied = vec![0; write.len()];
            () = std::os::unix::fs::FileExt::read_exact_at(&image, &mut applied, offset).unwrap();
            assert!(
                applied == write,
                "{name}: the applied chunk differs from the write"
            );

            // It also allocates what the write did: trimmed zeroes are written, not
            // left as holes.
            let direct = tempfile::tempfile().unwrap();
            for content in [&vec![0xff; write.len()], &write] {
                () = std::os::unix::fs::FileExt::write_all_at(&direct, content, offset).unwrap();
            }
            assert_eq!(
                data_extents(&image),
                data_extents(&direct),
                "{name}: the applied chunk allocates otherwise than the write"
            );
        }
    }

    #[test]
    fn test_a_punch_leaves_holes() {
        // Applied over blocks 6 to 10, a punch of blocks 7 to 9 reads back as zeroes,
        // and the filesystem holds no data for them. Blocks 6 and 10 are untouched.
        let image = tempfile::tempfile().unwrap();
        () = std::os::unix::fs::FileExt::write_all_at(&image, &[0xff; 5 * BLOCK], 6 * BLOCK as u64)
            .unwrap();
        () = apply(&image, &encode_punch(7, 3)).unwrap();

        let mut applied = vec![0; 5 * BLOCK];
        () = std::os::unix::fs::FileExt::read_exact_at(&image, &mut applied, 6 * BLOCK as u64)
            .unwrap();
        let expect = [[0xff; BLOCK].as_slice(), &[0; 3 * BLOCK], &[0xff; BLOCK]].concat();
        assert!(applied == expect, "the punch left other content");

        let block = |index: u64| index * BLOCK as u64;
        assert_eq!(
            data_extents(&image),
            [(block(6), block(7)), (block(10), block(11))]
        );
    }

    /// Byte ranges the filesystem holds data for. `sync_all` first resolves ext4's
    /// delayed allocation into extents.
    fn data_extents(file: &std::fs::File) -> Vec<(u64, u64)> {
        file.sync_all().unwrap();
        let mut extents = Vec::new();
        let mut cursor = 0;

        // ENXIO ends it: no data lies at or after `cursor`.
        while let Ok(start) = rustix::fs::seek(file, rustix::fs::SeekFrom::Data(cursor)) {
            let end = rustix::fs::seek(file, rustix::fs::SeekFrom::Hole(start)).unwrap();
            extents.push((start, end));
            cursor = end;
        }
        extents
    }

    /// `blocks` blocks whose first `filled` bytes are `0xaa`, and the rest zero.
    fn filled(blocks: usize, filled: usize) -> Vec<u8> {
        let mut data = vec![0; blocks * BLOCK];
        data[..filled].fill(0xaa);
        data
    }
}
