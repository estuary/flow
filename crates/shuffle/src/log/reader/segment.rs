use anyhow::Context;

/// An open log segment file, which is unlinked on drop.
pub struct Segment {
    path: std::path::PathBuf,
    file: std::fs::File,
}

impl Segment {
    /// Build the path of the indicated segment file. Matches Writer's naming convention.
    pub fn path(
        directory: &std::path::Path,
        member_index: u32,
        segment: u64,
    ) -> std::path::PathBuf {
        let filename = format!("mem-{member_index:03}-seg-{segment:012x}.flog");
        directory.join(filename)
    }

    /// Open the indicated segment file, returning a Segment with an open file handle.
    pub fn open(
        directory: &std::path::Path,
        member_index: u32,
        segment: u64,
    ) -> anyhow::Result<Self> {
        let path = Self::path(directory, member_index, segment);

        let file = std::fs::File::open(&path)
            .with_context(|| format!("failed to open log segment {path:?}"))?;

        Ok(Self { path, file })
    }

    /// Try to read a block header at the given offset.
    /// Returns None on EOF, or (raw_len, lz4_len).
    pub fn read_block_header(&self, offset: u32) -> anyhow::Result<Option<(u32, u32)>> {
        use std::os::unix::fs::FileExt;

        let mut header = [0u8; Self::BLOCK_HEADER_LEN];
        let n = self
            .file
            .read_at(&mut header, offset as u64)
            .context("reading block header")?;

        if n == 0 {
            return Ok(None); // EOF
        }
        if n != Self::BLOCK_HEADER_LEN {
            anyhow::bail!(
                "unexpected short header read at offset {offset}: got {n} of {} bytes",
                Self::BLOCK_HEADER_LEN
            );
        }

        let raw_len = u32::from_be_bytes(header[0..4].try_into().unwrap());
        let lz4_len = u32::from_be_bytes(header[4..8].try_into().unwrap());
        Ok(Some((raw_len, lz4_len)))
    }

    /// Read a block's payload into `buffer`, decompressing if needed.
    /// `buffer` must be empty on entry (caller is responsible for clearing).
    pub fn read_block_payload(
        &self,
        payload_offset: u32,
        raw_len: u32,
        lz4_len: u32,
    ) -> anyhow::Result<rkyv::util::AlignedVec> {
        use std::os::unix::fs::FileExt;

        let mut block_buffer = rkyv::util::AlignedVec::with_capacity(raw_len as usize);

        // Safety: we read or decompress into `block_buffer`,
        // and return it only after fully initializing its content.
        unsafe { block_buffer.set_len(raw_len as usize) };

        if lz4_len > 0 {
            // Safety: we immediately and fully read into `compressed`
            let mut compressed = Vec::with_capacity(lz4_len as usize);
            unsafe { compressed.set_len(lz4_len as usize) };

            self.file
                .read_exact_at(&mut compressed, payload_offset as u64)
                .context("reading compressed block payload")?;

            // Takes `&mut [u8]`, writing decompressed output from offset 0.
            let n = lz4::block::decompress_to_buffer(
                &compressed,
                Some(raw_len as i32),
                &mut block_buffer,
            )
            .context("decompressing log block")?;

            if n != raw_len as usize {
                anyhow::bail!("decompressed {n} bytes but expected {raw_len}");
            }
        } else {
            self.file
                .read_exact_at(&mut block_buffer, payload_offset as u64)
                .context("reading block payload")?;
        }

        Ok(block_buffer)
    }

    pub const BLOCK_HEADER_LEN: usize = 8;
}

impl Drop for Segment {
    fn drop(&mut self) {
        match std::fs::remove_file(&self.path) {
            Ok(()) => tracing::debug!(path=?self.path, "unlinked log segment"),
            Err(err) => tracing::warn!(%err, path=?self.path, "failed to unlink log segment"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::log::{self, block, reader::test_support::write_block};

    #[test]
    fn test_segment_path_formatting() {
        let cases = [
            (0u32, 1u64, "mem-000-seg-000000000001.flog"),
            (42, 0xABCDEF, "mem-042-seg-000000abcdef.flog"),
            (999, 0, "mem-999-seg-000000000000.flog"),
        ];
        for (member, segment, expected_name) in cases {
            let path = Segment::path(std::path::Path::new("/tmp"), member, segment);
            assert_eq!(
                path.file_name().unwrap().to_str().unwrap(),
                expected_name,
                "member={member}, segment={segment}"
            );
        }
    }

    #[test]
    fn test_segment_read_write_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = log::Writer::new(dir.path(), 0).unwrap();
        write_block(&mut writer, &[("j/one", 1, 0, 100)]);

        let seg = Segment::open(dir.path(), 0, 1).unwrap();
        let (raw_len, lz4_len) = seg.read_block_header(0).unwrap().unwrap();
        assert_eq!(lz4_len, 0, "small block should not be compressed");
        assert!(raw_len > 0);

        let buf = seg
            .read_block_payload(Segment::BLOCK_HEADER_LEN as u32, raw_len, lz4_len)
            .unwrap();
        let _block = rkyv::access::<block::ArchivedBlock, rkyv::rancor::Error>(&buf).unwrap();
    }

    #[test]
    fn test_segment_read_compressed_block() {
        // compress_threshold=0 forces LZ4 compression of every block.
        let dir = tempfile::tempdir().unwrap();
        let mut writer = log::Writer::with_thresholds(dir.path(), 0, 0, u64::MAX).unwrap();
        write_block(&mut writer, &[("j/one", 1, 0, 100)]);

        let seg = Segment::open(dir.path(), 0, 1).unwrap();
        let (raw_len, lz4_len) = seg.read_block_header(0).unwrap().unwrap();
        assert!(lz4_len > 0, "block should be compressed");
        assert!(raw_len > 0);

        let buf = seg
            .read_block_payload(Segment::BLOCK_HEADER_LEN as u32, raw_len, lz4_len)
            .unwrap();
        let archived = rkyv::access::<block::ArchivedBlock, rkyv::rancor::Error>(&buf).unwrap();
        assert_eq!(archived.meta.len(), 1);
    }

    #[test]
    fn test_segment_header_eof() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = log::Writer::new(dir.path(), 0).unwrap();
        write_block(&mut writer, &[("j/one", 1, 0, 100)]);

        let seg = Segment::open(dir.path(), 0, 1).unwrap();
        assert!(seg.read_block_header(1_000_000).unwrap().is_none());
    }

    #[test]
    fn test_segment_header_short_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = Segment::path(dir.path(), 0, 1);
        std::fs::write(&path, &[0u8; 4]).unwrap();

        let seg = Segment::open(dir.path(), 0, 1).unwrap();
        let err = seg.read_block_header(0).unwrap_err();
        assert!(
            format!("{err:?}").contains("short header"),
            "expected short header error, got: {err:?}"
        );
    }

    #[test]
    fn test_segment_drop_unlinks() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = log::Writer::new(dir.path(), 0).unwrap();
        write_block(&mut writer, &[("j/one", 1, 0, 100)]);

        let path = Segment::path(dir.path(), 0, 1);
        assert!(path.exists());

        let seg = Segment::open(dir.path(), 0, 1).unwrap();
        drop(seg);
        assert!(!path.exists(), "segment file should be unlinked after drop");
    }
}
