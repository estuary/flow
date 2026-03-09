use super::Lsn;
use super::block::{self, BlockMeta};
use anyhow::Context;
use proto_gazette::uuid;
use std::collections::HashMap;
use std::io::Write;

/// Writer appends encoded blocks to a segmented log on disk.
///
/// Each block is preceded by an 8-byte header:
///   - `raw_len`: u32 big-endian, uncompressed byte length
///   - `lz4_len`: u32 big-endian, compressed byte length (0 if not compressed)
///
/// Blocks over 64 KB are LZ4-compressed. When a segment file exceeds 64 MB,
/// the writer rolls to a new segment. Files are created with `create_new` to
/// guarantee exclusive ownership of the segment sequence.
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
}

impl Writer {
    /// Create a new Writer, opening the first segment file.
    pub fn new(directory: &std::path::Path, member_index: u32) -> anyhow::Result<Self> {
        let file = create_segment(directory, member_index, 1)?;
        Ok(Self {
            directory: directory.to_owned(),
            member_index,
            next_lsn: Lsn::new(1, 0),
            segment_file: file,
            segment_bytes: 0,
        })
    }

    /// Encode and append a block, returning the LSN at which it was written.
    ///
    /// Returns when the complete block has been handed off to the OS page cache,
    /// but no fsync or fdatasync is performed (given our fail-fast failure model).
    pub fn append_block(
        &mut self,
        journals: HashMap<String, u16>,
        producers: HashMap<uuid::Producer, u16>,
        entries: Vec<(BlockMeta, i64, bytes::Bytes, bytes::Bytes)>,
    ) -> anyhow::Result<Lsn> {
        let block_lsn = self.next_lsn;
        let raw = block::encode(journals, producers, entries);

        let (payload, raw_len, lz4_len) = if raw.len() > COMPRESS_THRESHOLD {
            let mut lz4_buf = Vec::with_capacity(lz4::block::compress_bound(raw.len())?);

            // Safety: extend to capacity so compress_to_buffer has a &mut [u8] to write into.
            // Contents are uninitialized but compress_to_buffer treats it as output-only.
            unsafe { lz4_buf.set_len(lz4_buf.capacity()) };

            let n = lz4::block::compress_to_buffer(
                &raw,
                Some(lz4::block::CompressionMode::DEFAULT),
                false,
                &mut lz4_buf,
            )?;
            // Safety: compress_to_buffer initialized exactly n bytes; truncate to that.
            unsafe { lz4_buf.set_len(n) };

            let raw_len = (raw.len() as u32).to_be_bytes();
            let lz4_len = (lz4_buf.len() as u32).to_be_bytes();

            (lz4_buf, raw_len, lz4_len)
        } else {
            let raw_len = (raw.len() as u32).to_be_bytes();
            (raw.into_vec(), raw_len, 0u32.to_be_bytes())
        };

        self.segment_file.write_all(&[
            raw_len[0], raw_len[1], raw_len[2], raw_len[3], // Raw length u32, big-endian.
            lz4_len[0], lz4_len[1], lz4_len[2], lz4_len[3], // LZ4 length u32, big-endian.
        ])?;
        self.segment_file.write_all(&payload)?;
        self.segment_bytes += (BLOCK_HEADER_LEN + payload.len()) as u64;

        // Roll to a new segment if we've exceeded the byte threshold
        // or exhausted the u16 block number space.
        if self.segment_bytes >= SEGMENT_THRESHOLD || self.next_lsn.block() == u16::MAX {
            self.next_lsn = self.next_lsn.next_segment();
            self.segment_file =
                create_segment(&self.directory, self.member_index, self.next_lsn.segment())?;
            self.segment_bytes = 0;
        } else {
            self.next_lsn = self.next_lsn.next_block();
        }

        tracing::debug!(
            ?block_lsn,
            raw_len = u32::from_be_bytes(raw_len),
            lz4_len = u32::from_be_bytes(lz4_len),
            segment = block_lsn.segment(),
            block = block_lsn.block(),
            segment_size = self.segment_bytes,
            "appended log segment block",
        );

        Ok(block_lsn)
    }
}

/// Open a new segment file with exclusive creation.
fn create_segment(
    directory: &std::path::Path,
    member_index: u32,
    segment: u64,
) -> anyhow::Result<std::fs::File> {
    let filename = format!("mem-{member_index:03}-seg-{segment:012x}.flog");
    let path = directory.join(&filename);

    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .with_context(|| {
            format!("failed to create log segment {path:?} (file already exists implies a session conflict)")
        })
}

const BLOCK_HEADER_LEN: usize = 8;
const COMPRESS_THRESHOLD: usize = 64 * 1024;
const SEGMENT_THRESHOLD: u64 = 64 * 1024 * 1024;

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_writer_creates_file_and_appends_block() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = Writer::new(dir.path(), 3).unwrap();

        // Verify the initial file was created.
        let expected_path = dir.path().join("mem-003-seg-000000000001.flog");
        assert!(expected_path.exists());

        // Append a small block (no compression).
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
            100i64,
            bytes::Bytes::from_static(b"packed_key_______"),
            doc_bytes,
        )];

        let lsn = writer.append_block(journals, producers, entries).unwrap();
        assert_eq!(lsn, Lsn::new(1, 0));
        assert_eq!(writer.next_lsn, Lsn::new(1, 1));

        // Read back and verify header.
        let data = std::fs::read(&expected_path).unwrap();
        assert!(data.len() > BLOCK_HEADER_LEN);

        let raw_len = u32::from_be_bytes(data[0..4].try_into().unwrap()) as usize;
        let lz4_len = u32::from_be_bytes(data[4..8].try_into().unwrap()) as usize;

        assert_eq!(lz4_len, 0, "small block should not be compressed");
        assert_eq!(data.len(), BLOCK_HEADER_LEN + raw_len);
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
