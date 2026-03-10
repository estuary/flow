use super::Segment;
use crate::log;
use anyhow::Context;
use std::sync::Arc;

/// Reader reads blocks from segmented log files, as produced by `Writer`.
///
/// The reader is ephemeral: its state is not persisted. On any error the
/// session tears down (fail-fast), and segment files are discarded along with
/// all other session state.
pub struct Reader {
    // Base directory for all segment files of the log.
    directory: std::path::PathBuf,
    // Index of the read member, used to name its files.
    member_index: u32,
    // Upper-bound reported flushed LSN (inclusive) that will not be read beyond.
    flushed_lsn: log::Lsn,
    // Next LSN to read from the head segment.
    head_lsn: log::Lsn,
    // Head segment for new block reads, lazily opened.
    head_segment: Option<Arc<Segment>>,
    // Byte offset of the next block header in the head segment.
    head_offset: u32,
}

pub struct ReadBlock {
    pub segment: Arc<Segment>,
    pub payload_offset: u32,
    pub raw_len: u32,
    pub lz4_len: u32,
    pub block_buffer: rkyv::util::AlignedVec,
}

impl Reader {
    /// Create a new Reader. No files are opened until `FrontierScan::new`.
    pub fn new(directory: &std::path::Path, member_index: u32) -> Self {
        Self {
            directory: directory.to_owned(),
            member_index,
            flushed_lsn: log::Lsn::ZERO,
            head_lsn: log::Lsn::new(1, 0),
            head_segment: None,
            head_offset: 0,
        }
    }

    pub fn member_index(&self) -> u32 {
        self.member_index
    }

    /// Set or update the high-watermark flushed LSN confirmed by the log writer.
    /// This LSN upper-bounds the log portions that will be read by this Reader.
    pub fn set_flushed_lsn(&mut self, flushed_lsn: log::Lsn) -> anyhow::Result<()> {
        if self.flushed_lsn > flushed_lsn {
            anyhow::bail!(
                "new flushed LSN {flushed_lsn:?} regresses past (is less-than) previously-reported LSN {:?}",
                self.flushed_lsn
            );
        }
        self.flushed_lsn = flushed_lsn;
        Ok(())
    }

    /// Read a next block from the log, or None if Reader has reached the flushed LSN.
    pub fn read_next_block(&mut self) -> anyhow::Result<Option<ReadBlock>> {
        loop {
            if self.head_lsn > self.flushed_lsn {
                return Ok(None);
            }

            let segment = self.ensure_segment()?;

            match segment.read_block_header(self.head_offset)? {
                Some((raw_len, lz4_len)) => {
                    self.head_offset = self
                        .head_offset
                        .checked_add(Segment::BLOCK_HEADER_LEN as u32)
                        .context("segment file overflows 4GB after header read")?;

                    let payload_offset = self.head_offset;
                    let block_buffer =
                        segment.read_block_payload(self.head_offset, raw_len, lz4_len)?;

                    self.head_offset = self
                        .head_offset
                        .checked_add(if lz4_len > 0 { lz4_len } else { raw_len })
                        .context("segment file overflows 4GB after payload read")?;

                    self.head_lsn = self.head_lsn.next_block();

                    return Ok(Some(ReadBlock {
                        segment,
                        payload_offset,
                        raw_len,
                        lz4_len,
                        block_buffer,
                    }));
                }
                None => {
                    // EOF: transition to the next segment file.
                    self.head_lsn = self.head_lsn.next_segment();
                    self.head_offset = 0;
                    self.head_segment = None;
                }
            }
        }
    }

    /// Ensure the head segment is open, opening it if necessary.
    fn ensure_segment(&mut self) -> anyhow::Result<Arc<Segment>> {
        if let Some(ref seg) = self.head_segment {
            return Ok(Arc::clone(seg));
        }
        let segment = Arc::new(Segment::open(
            &self.directory,
            self.member_index,
            self.head_lsn.segment(),
        )?);
        self.head_segment = Some(segment.clone());
        Ok(segment)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::log::block;
    use crate::log::reader::test_support::write_block;
    use crate::log::writer::Writer;

    #[test]
    fn test_set_flushed_lsn_monotonic() {
        let dir = tempfile::tempdir().unwrap();
        let mut reader = Reader::new(dir.path(), 0);

        // Increasing is fine.
        reader.set_flushed_lsn(log::Lsn::new(1, 5)).unwrap();
        reader.set_flushed_lsn(log::Lsn::new(1, 10)).unwrap();
        // Equal is fine (non-decreasing).
        reader.set_flushed_lsn(log::Lsn::new(1, 10)).unwrap();
        // Decreasing fails.
        reader.set_flushed_lsn(log::Lsn::new(1, 5)).unwrap_err();
    }

    #[test]
    fn test_read_blocks_and_flushed_lsn_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = Writer::new(dir.path(), 0).unwrap();

        for i in 0..5u64 {
            write_block(&mut writer, &[("j/one", 1, 0, i)]);
        }

        let mut reader = Reader::new(dir.path(), 0);

        // Read stops at flushed_lsn watermark.
        reader.set_flushed_lsn(log::Lsn::new(1, 2)).unwrap();
        for _ in 0..3 {
            assert!(reader.read_next_block().unwrap().is_some());
        }
        assert!(reader.read_next_block().unwrap().is_none());

        // Advance the watermark and read more.
        reader.set_flushed_lsn(log::Lsn::new(1, 4)).unwrap();
        for _ in 0..2 {
            assert!(reader.read_next_block().unwrap().is_some());
        }
        assert!(reader.read_next_block().unwrap().is_none());
    }

    #[test]
    fn test_segment_transition() {
        // segment_threshold=0 forces a segment roll after every block.
        let dir = tempfile::tempdir().unwrap();
        let mut writer = Writer::with_thresholds(dir.path(), 0, usize::MAX, 0).unwrap();

        write_block(&mut writer, &[("j/one", 1, 0, 1)]);
        write_block(&mut writer, &[("j/one", 1, 0, 2)]);

        let mut reader = Reader::new(dir.path(), 0);
        reader.set_flushed_lsn(log::Lsn::new(2, 0)).unwrap();

        // First block from segment 1.
        let b1 = reader.read_next_block().unwrap().unwrap();
        let a1 = unsafe { rkyv::access_unchecked::<block::ArchivedBlock>(&b1.block_buffer) };
        assert_eq!(a1.meta[0].clock.to_native(), 1);

        // Second block from segment 2 (after transition).
        let b2 = reader.read_next_block().unwrap().unwrap();
        let a2 = unsafe { rkyv::access_unchecked::<block::ArchivedBlock>(&b2.block_buffer) };
        assert_eq!(a2.meta[0].clock.to_native(), 2);

        assert!(reader.read_next_block().unwrap().is_none());
    }
}
