use super::Lsn;
use super::block::{self, BlockMeta};
use anyhow::Context;
use proto_gazette::uuid;
use std::collections::HashMap;
use std::io::Write;

mod sealed;
pub use sealed::SealedSegment;

/// Writer appends encoded blocks to segmented log files on disk.
///
/// Each block is preceded by an 8-byte header:
///   - `raw_len`: u32 big-endian, uncompressed byte length
///   - `lz4_len`: u32 big-endian, compressed byte length (0 if not compressed)
///
/// Blocks over a compression threshold are LZ4-compressed, though typically
/// this limit is usize::MAX (no compression). Instead, a follow-behind async
/// compression path re-writes sealed segments that still live after a delay.
///
/// When a segment file exceeds its byte threshold, the writer rolls to a new
/// segment and returns the old one as a `SealedSegment`. Segment files are
/// created with `create_new` to guarantee exclusive ownership of the sequence.
#[derive(Debug)]
pub struct Writer {
    // Base directory for all segment files of the log.
    directory: std::path::PathBuf,
    // Index of this member, used to name its files.
    member_index: u32,
    // The LSN of the next block to be appended.
    next_lsn: Lsn,
    // The current segment being written.
    segment_file: std::fs::File,
    // Number of bytes written to the current segment file.
    segment_bytes: u64,
    // Blocks larger than this are LZ4-compressed.
    compress_threshold: usize,
    // Segment files roll after exceeding this many bytes.
    segment_threshold: u64,
}

impl Writer {
    /// Create a new Writer, opening the first segment file.
    pub fn new(directory: &std::path::Path, member_index: u32) -> anyhow::Result<Self> {
        Self::with_thresholds(
            directory,
            member_index,
            usize::MAX,
            DEFAULT_SEGMENT_THRESHOLD,
        )
    }

    /// Create a Writer with explicit compression and segment-roll thresholds.
    pub fn with_thresholds(
        directory: &std::path::Path,
        member_index: u32,
        compress_threshold: usize,
        segment_threshold: u64,
    ) -> anyhow::Result<Self> {
        let file = create_segment(directory, member_index, 1)?;
        Ok(Self {
            directory: directory.to_owned(),
            member_index,
            next_lsn: Lsn::new(1, 0),
            segment_file: file,
            segment_bytes: 0,
            compress_threshold,
            segment_threshold,
        })
    }

    /// Encode and append a block, returning the LSN at which it was written
    /// and an optional `SealedSegment` if the previous segment was rolled.
    ///
    /// Returns when the complete block has been handed off to the OS page cache,
    /// but no fsync or fdatasync is performed (given our fail-fast failure model).
    pub fn append_block(
        &mut self,
        journals: HashMap<String, u16>,
        producers: HashMap<uuid::Producer, u16>,
        entries: Vec<(BlockMeta, u32, bytes::Bytes, bytes::Bytes)>,
    ) -> anyhow::Result<(Lsn, Option<SealedSegment>)> {
        let raw = block::encode(journals, producers, entries);

        let raw_len: u32 = raw
            .len()
            .try_into()
            .context("encoded block exceeds u32::MAX bytes")?;

        let (payload, raw_len, lz4_len) = if raw.len() > self.compress_threshold {
            let lz4_buf = super::lz4_compress(&raw)?;
            let lz4_len: u32 = lz4_buf
                .len()
                .try_into()
                .context("compressed block exceeds u32::MAX bytes")?;

            (lz4_buf, raw_len.to_be_bytes(), lz4_len.to_be_bytes())
        } else {
            (raw.into_vec(), raw_len.to_be_bytes(), 0u32.to_be_bytes())
        };

        self.segment_file.write_all(&[
            raw_len[0], raw_len[1], raw_len[2], raw_len[3], // Raw length u32, big-endian.
            lz4_len[0], lz4_len[1], lz4_len[2], lz4_len[3], // LZ4 length u32, big-endian.
        ])?;
        self.segment_file.write_all(&payload)?;
        self.segment_bytes += (super::BLOCK_HEADER_LEN + payload.len()) as u64;

        let block_lsn = self.next_lsn;

        tracing::debug!(
            ?block_lsn,
            raw_len = u32::from_be_bytes(raw_len),
            lz4_len = u32::from_be_bytes(lz4_len),
            segment_bytes = self.segment_bytes,
            "appended log segment block",
        );

        // Roll to a new segment if we've exceeded the byte threshold
        // or exhausted the u16 block number space.
        if self.segment_bytes < self.segment_threshold && self.next_lsn.block() < u16::MAX {
            self.next_lsn = self.next_lsn.next_block();
            return Ok((block_lsn, None));
        } else {
            self.next_lsn = self.next_lsn.next_segment();
        }

        // Drop the old segment file descriptor eagerly. Without an open fd,
        // a quick unlink by the reader lets the kernel drop dirty pages from
        // cache without ever writing them to the block device.
        drop(std::mem::replace(
            &mut self.segment_file,
            create_segment(&self.directory, self.member_index, self.next_lsn.segment())?,
        ));

        let sealed = SealedSegment::new(
            super::segment_path(&self.directory, self.member_index, block_lsn.segment()),
            std::mem::take(&mut self.segment_bytes),
        );

        tracing::debug!(
            ?block_lsn,
            raw_len = u32::from_be_bytes(raw_len),
            lz4_len = u32::from_be_bytes(lz4_len),
            sealed_size = sealed.size,
            "sealed log segment",
        );

        Ok((block_lsn, Some(sealed)))
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        let path = super::segment_path(&self.directory, self.member_index, self.next_lsn.segment());
        if let Err(err) = std::fs::remove_file(&path) {
            tracing::warn!(%err, ?path, "failed to unlink in-progress writer segment");
        }
    }
}

/// Open a new segment file with exclusive creation.
fn create_segment(
    directory: &std::path::Path,
    member_index: u32,
    segment: u64,
) -> anyhow::Result<std::fs::File> {
    let path = super::segment_path(directory, member_index, segment);

    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .with_context(|| {
            format!("failed to create log segment {path:?} (file already exists implies a session conflict)")
        })
}

/// Default segment file size threshold: 64 MB.
const DEFAULT_SEGMENT_THRESHOLD: u64 = 64 * 1024 * 1024;

#[cfg(test)]
mod test {
    use super::super::BLOCK_HEADER_LEN;
    use super::*;

    #[test]
    fn test_writer_append_and_segment_roll() {
        let dir = tempfile::tempdir().unwrap();
        // Tiny segment threshold so we roll after the first block.
        let mut writer = Writer::with_thresholds(dir.path(), 0, usize::MAX, 1).unwrap();

        // Verify the initial segment file was created.
        let seg1_path = dir.path().join("mem-000-seg-000000000001.flog");
        assert!(seg1_path.exists());

        let journals: HashMap<String, u16> = [("j/one".to_string(), 0)].into();
        let producers: HashMap<uuid::Producer, u16> =
            [(uuid::Producer([0x01, 0, 0, 0, 0, 0x01]), 0)].into();

        let alloc = doc::HeapNode::new_allocator();
        let node = doc::HeapNode::from_serde(&serde_json::json!({"key": "val"}), &alloc).unwrap();
        let doc_bytes = bytes::Bytes::from(node.to_archive().to_vec());

        let meta = BlockMeta {
            binding: 0,
            journal_bid: 0,
            producer_bid: 0,
            flags: 0x0001,
            clock: 42,
        };
        let entries = vec![(
            meta,
            doc_bytes.len() as u32,
            bytes::Bytes::from_static(b"packed_key_______"),
            doc_bytes,
        )];

        let (lsn, sealed) = writer.append_block(journals, producers, entries).unwrap();
        assert_eq!(lsn, Lsn::new(1, 0));

        // Segment threshold is 1, so the segment rolled.
        let sealed = sealed.expect("segment should have rolled");
        assert_eq!(sealed.path, seg1_path);
        assert!(sealed.size > 0);

        // Read back sealed segment and verify header.
        let data = std::fs::read(&seg1_path).unwrap();
        assert!(data.len() > BLOCK_HEADER_LEN);

        let raw_len = u32::from_be_bytes(data[0..4].try_into().unwrap()) as usize;
        let lz4_len = u32::from_be_bytes(data[4..8].try_into().unwrap()) as usize;

        assert_eq!(lz4_len, 0, "small block should not be compressed");
        assert_eq!(data.len(), BLOCK_HEADER_LEN + raw_len);

        // Second segment file should exist.
        assert!(dir.path().join("mem-000-seg-000000000002.flog").exists());

        // Drop sealed segment — should unlink the first segment file.
        let first_path = sealed.path.clone();
        drop(sealed);
        assert!(
            !first_path.exists(),
            "sealed segment should be unlinked on drop"
        );
    }

    #[test]
    fn test_writer_exclusive_creation_fails_on_conflict() {
        let dir = tempfile::tempdir().unwrap();

        let _writer = Writer::new(dir.path(), 0).unwrap();
        let err = Writer::new(dir.path(), 0).unwrap_err();

        assert!(
            format!("{err:?}").contains("already exists"),
            "expected conflict error, got: {err:?}"
        );
    }
}
