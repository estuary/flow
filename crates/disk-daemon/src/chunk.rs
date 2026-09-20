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

/// A trimmed tail is at most one block.
static ZEROES: [u8; crate::BLOCK_SIZE as usize] = [0; crate::BLOCK_SIZE as usize];

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
                &ZEROES[..pad as usize],
                offset + data.len() as u64,
            )?;

            allocated.set_range(range);
        }
        chunk::Content::Punch(_) => {
            crate::image::punch_hole(file, offset, len)?;

            allocated.clear_range(range);
        }
    }
    Ok(())
}

#[cfg(test)]
mod test;
