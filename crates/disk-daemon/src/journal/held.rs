//! Records of the delta which the journal holds but has not acknowledged.
//!
//! A replay cannot apply a delta before its acknowledgement, for the reasons the
//! [`super::replay`] module gives. It holds that delta's records here instead, in a
//! file beside the image. Records go in as they arrive, framed exactly as the
//! journal framed them, and come back out in the same order when the delta is
//! applied. They are dropped if it never is, which costs a hole punch.
//!
//! One delta is held at a time, because a live writer keeps one delta in doubt at a
//! time: the records of the delta behind a cut are held by that writer until the
//! acknowledgement lands, so they never reach the journal ahead of it.
//!
//! This file is the storage and nothing else. Which delta is held, when it is
//! dropped, and what applying it does to the image and its horizon are all
//! [`super::replay::Pass`]'s rules.
//!
//! The file is `O_TMPFILE` like the image, so it cannot outlive its daemon. Nothing
//! recovers it: a standby which dies is replaced by one which replays from the
//! floor.

use anyhow::Context;
use proto_gazette::fixed_framing;

/// Size of the blocks a held delta is read back in. A delta has no size bound of
/// its own — it is whatever the primary wrote between two acknowledgements, and may
/// exceed the disk it belongs to — so it is read back in blocks rather than read
/// whole. A record larger than the buffer is read whole anyway,
/// because framing decodes a record as a unit: replay therefore costs this buffer
/// plus the largest record, and not the delta.
const READ_BYTES: usize = 64 << 10;

/// The unacknowledged delta of one replay.
pub struct HeldDelta {
    file: std::fs::File,
    /// Bytes held, which is also the offset the next record is written at. The file
    /// is written strictly forward and punched back to zero whenever a delta leaves,
    /// so a standby which runs for weeks holds no more than the delta it is holding.
    len: u64,
    records: usize,
}

impl HeldDelta {
    /// Create the file of a held delta within `dir`, which is the daemon's image
    /// directory.
    pub fn create(dir: &std::path::Path) -> std::io::Result<Self> {
        let mut options = std::fs::OpenOptions::new();
        options.read(true).write(true);
        std::os::unix::fs::OpenOptionsExt::custom_flags(&mut options, libc::O_TMPFILE);
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);

        Ok(Self {
            file: options.open(dir)?,
            len: 0,
            records: 0,
        })
    }

    /// Bytes held.
    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.records == 0
    }

    /// Hold `framed`, the journal's own bytes of one record.
    pub(super) fn push(&mut self, framed: &[u8]) -> anyhow::Result<()> {
        () = std::os::unix::fs::FileExt::write_all_at(&self.file, framed, self.len)
            .context("holding a record of an unacknowledged delta")?;

        self.len += framed.len() as u64;
        self.records += 1;

        Ok(())
    }

    /// Hand every held record to `each`, in the order it was held, and then drop
    /// them all.
    ///
    /// The records are read back in blocks and decoded one at a time, so this uses
    /// memory independent of the delta's length.
    pub(super) fn drain(
        &mut self,
        mut each: impl FnMut(crate::proto::DiskRecord) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        // Records are held with positional writes, which never move the cursor, so
        // it is still wherever the tenure's last drain left it.
        () = std::io::Seek::rewind(&mut &self.file)
            .context("seeking to the start of a held delta")?;

        // The file outlives the delta it holds, because `clear` punches it rather
        // than truncating it, so the read stops at this delta's own end.
        let mut reader = std::io::Read::take(&self.file, self.len);
        // Blocks are read straight into `buf`, and records are unpacked out of it
        // without copying: a record's chunks reference `buf` until it is handled.
        let mut buf = bytes::BytesMut::new();
        // Bytes of the delta not yet read into `buf`.
        let mut unread = self.len;

        for _ in 0..self.records {
            let record = loop {
                let framed = match fixed_framing::header(&buf) {
                    // This file framed these records itself, so a header which is not
                    // one is corruption of the file, and not a stream which a reader
                    // joined between frames and can resynchronize with.
                    fixed_framing::Header::Desync { .. } => anyhow::bail!(
                        "a held delta does not decode: a record begins {:02x?}",
                        &buf[..fixed_framing::MAGIC.len()],
                    ),
                    fixed_framing::Header::Incomplete => fixed_framing::HEADER_LEN,
                    fixed_framing::Header::Frame { payload } => {
                        // Nothing stands behind a length the file states, so it sizes
                        // a read only once the delta is known to hold that many bytes.
                        // A record the file holds all but the end of is truncation,
                        // which the read below reports.
                        let held = buf.len() as u64 + unread;
                        let framed = fixed_framing::HEADER_LEN + payload;
                        anyhow::ensure!(
                            payload as u64 <= held,
                            "a held record frames {framed} bytes, which is {} more than \
                             the delta holds",
                            framed as u64 - held,
                        );
                        framed
                    }
                };
                if buf.len() >= framed {
                    match fixed_framing::unpack::<crate::proto::DiskRecord>(&mut buf)
                        .context("decoding a held record")?
                    {
                        fixed_framing::Frame::Record { message, .. } => break message,
                        // The magic word and the whole payload were both checked above.
                        other => panic!("a checked frame unpacked as {other:?}"),
                    }
                }
                anyhow::ensure!(unread != 0, "a held delta ends within a record");

                // A block, or the rest of the record where that is longer.
                let len = buf.len();
                let read = ((framed - len).max(READ_BYTES) as u64).min(unread) as usize;
                buf.resize(len + read, 0);
                () = read_framed(&mut reader, &mut buf[len..])?;
                unread -= read as u64;
            };
            () = each(record)?;
        }
        let trailing = buf.len() as u64 + unread;
        anyhow::ensure!(
            trailing == 0,
            "a held delta ends within a record, with {trailing} trailing bytes which frame none",
        );
        () = self.clear()?;

        Ok(())
    }

    /// Drop every held record. The image is untouched, because nothing of them was
    /// applied.
    pub(super) fn clear(&mut self) -> std::io::Result<()> {
        if self.len != 0 {
            () = crate::image::punch_hole(&self.file, 0, self.len)?;
        }
        self.len = 0;
        self.records = 0;

        Ok(())
    }
}

/// Fill `buf` from `reader`, which reads the held delta and stops at its end.
///
/// The delta was framed as it was held, so every frame it holds must be whole. An
/// end within one is a file which holds less than it counted records for.
fn read_framed(reader: &mut impl std::io::Read, buf: &mut [u8]) -> anyhow::Result<()> {
    match std::io::Read::read_exact(reader, buf) {
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
            anyhow::bail!("a held delta ends within a record")
        }
        result => result.context("reading back a held delta"),
    }
}

#[cfg(test)]
mod test {
    use super::{HeldDelta, READ_BYTES};
    use crate::proto;
    use crate::{BLOCK_SIZE, chunk};
    use proto_gazette::{fixed_framing, uuid};

    fn producer(seed: u8) -> uuid::Producer {
        uuid::Producer::from_bytes([seed | 0x01, 0, 0, 0, 0, seed])
    }

    /// A record of `producer`'s delta carrying `chunks`. Nothing here sequences
    /// records, so one clock serves them all.
    fn record(producer: uuid::Producer, chunks: Vec<proto::Chunk>) -> proto::DiskRecord {
        proto::DiskRecord {
            uuid: bytes::Bytes::copy_from_slice(
                uuid::build(producer, uuid::Clock::UNIX_EPOCH, uuid::Flags::CONTINUE_TXN)
                    .as_bytes()
                    .as_slice(),
            ),
            chunks,
            opens_horizon: false,
            installs_epoch: bytes::Bytes::new(),
        }
    }

    /// A record writing `blocks` blocks of `fill`, beginning at `block`.
    fn write(producer: uuid::Producer, block: u32, blocks: u32, fill: u8) -> proto::DiskRecord {
        let data = bytes::Bytes::from(vec![fill; (blocks * BLOCK_SIZE) as usize]);

        record(producer, chunk::encode_write(block, &data))
    }

    /// A record whose chunk carries `bytes` bytes of `fill`, which is how a case
    /// places its framing at an offset a block-aligned write could not reach.
    fn sized(producer: uuid::Producer, block: u32, bytes: usize, fill: u8) -> proto::DiskRecord {
        record(
            producer,
            vec![proto::Chunk {
                block,
                content: Some(proto::chunk::Content::Data(bytes::Bytes::from(vec![
                    fill;
                    bytes
                ]))),
            }],
        )
    }

    /// Hold `records`, framed exactly as the journal frames them.
    fn hold(held: &mut HeldDelta, records: &[proto::DiskRecord]) {
        let mut framed = bytes::BytesMut::new();

        for record in records {
            framed.clear();
            fixed_framing::encode(record, &mut framed);
            held.push(&framed).unwrap();
        }
    }

    /// Drain `held`, collecting what it hands back and the failure, if any, which
    /// stopped it.
    fn drained(held: &mut HeldDelta) -> (Vec<proto::DiskRecord>, anyhow::Result<()>) {
        let mut records = Vec::new();

        let result = held.drain(|record| {
            records.push(record);
            Ok(())
        });
        (records, result)
    }

    /// A delta of many records, spanning many fills of the reader's buffer, reads
    /// back in the order it was held.
    #[test]
    fn test_a_many_record_delta_reads_back_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let mut held = HeldDelta::create(dir.path()).unwrap();
        let a = producer(0x10);

        let records: Vec<_> = (0..600u32).map(|i| write(a, i % 8, 1, i as u8)).collect();

        hold(&mut held, &records);
        assert!(
            held.len() > 8 * READ_BYTES as u64,
            "many held fills of delta"
        );

        let (read, result) = drained(&mut held);
        () = result.unwrap();

        assert!(read == records, "the delta read back differently");
        assert!(held.is_empty());
        assert_eq!(held.len(), 0);
    }

    /// A record may be larger than the buffer it is read through, and is then read
    /// whole. Framing decodes a record as a unit, and a chunk of the device's
    /// maximum request size already exceeds the buffer.
    #[test]
    fn test_a_record_larger_than_the_read_buffer() {
        let dir = tempfile::tempdir().unwrap();
        let mut held = HeldDelta::create(dir.path()).unwrap();
        let a = producer(0x10);

        let blocks = 2 + READ_BYTES as u32 / BLOCK_SIZE;
        let records = [
            write(a, 0, 1, 0xaa),
            write(a, 1, blocks, 0xbb),
            write(a, 1 + blocks, 1, 0xcc),
        ];

        hold(&mut held, &records);

        let (read, result) = drained(&mut held);
        () = result.unwrap();

        assert!(read == records, "the delta read back differently");
    }

    /// A frame may cross a fill of the reader's buffer at any offset, including
    /// within its own header. Sweeping the size of the first record walks the second
    /// record's start across that edge, and the sweep asserts that it reached the
    /// header case.
    #[test]
    fn test_a_record_may_straddle_the_read_buffer() {
        let dir = tempfile::tempdir().unwrap();
        let a = producer(0x10);
        let mut split_header = false;

        for bytes in READ_BYTES - 72..READ_BYTES + 8 {
            let mut held = HeldDelta::create(dir.path()).unwrap();

            let first = sized(a, 0, bytes, 0xaa);
            let mut framed = bytes::BytesMut::new();
            fixed_framing::encode(&first, &mut framed);

            split_header |=
                (READ_BYTES - fixed_framing::HEADER_LEN..READ_BYTES).contains(&framed.len());

            let records = [first, write(a, 40, 1, 0xbb), write(a, 41, 1, 0xcc)];
            hold(&mut held, &records);

            let (read, result) = drained(&mut held);
            () = result.unwrap();

            assert!(read == records, "with {bytes} bytes of data");
        }
        assert!(split_header, "a frame header straddled a held boundary");
    }

    /// Clearing punches the file without shrinking it. A shorter delta must stop
    /// at its own end, even though the file still extends to the prior delta's end.
    #[test]
    fn test_a_drained_file_holds_the_next_delta() {
        let dir = tempfile::tempdir().unwrap();
        let mut held = HeldDelta::create(dir.path()).unwrap();
        let a = producer(0x10);

        hold(&mut held, &[write(a, 0, 2, 0xaa), write(a, 1, 1, 0xbb)]);
        let prior_len = held.len();

        let (read, result) = drained(&mut held);
        () = result.unwrap();
        assert_eq!(read.len(), 2);
        assert_eq!(held.len(), 0);

        let next = [write(a, 1, 1, 0xcc), write(a, 2, 1, 0xdd)];
        hold(&mut held, &next);
        assert!(held.len() < prior_len, "the next delta is shorter");
        assert_eq!(held.file.metadata().unwrap().len(), prior_len);

        let (read, result) = drained(&mut held);
        () = result.unwrap();

        assert!(
            read == next,
            "the next delta read back with the prior one's tail"
        );
    }

    /// Hold malformed bytes and report the failure of draining them.
    fn refused(dir: &tempfile::TempDir, parts: &[&[u8]]) -> String {
        let mut held = HeldDelta::create(dir.path()).unwrap();

        for part in parts {
            held.push(part).unwrap();
        }
        let (read, result) = drained(&mut held);
        assert!(read.is_empty(), "a malformed delta handed back {read:?}");

        format!("{:#}", result.unwrap_err())
    }

    #[test]
    fn test_unframed_held_content_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let err = refused(&dir, &[b"held content with no magic word anywhere in it"]);

        assert!(err.contains("does not decode"), "{err}");
    }

    #[test]
    fn test_a_delta_which_ends_within_a_record_is_refused() {
        let dir = tempfile::tempdir().unwrap();

        let mut framed = bytes::BytesMut::new();
        fixed_framing::encode(&write(producer(0x10), 0, 1, 0xaa), &mut framed);

        for len in [1, fixed_framing::HEADER_LEN - 1, framed.len() - 1] {
            let err = refused(&dir, &[&framed[..len]]);
            assert!(err.contains("ends within a record"), "{err}");
        }
    }

    /// A header states its payload's length, and that length is checked against the
    /// bytes the file holds before anything is sized from it. The delta here runs
    /// past the first buffer fill, so the check is reached with most of it unread.
    #[test]
    fn test_a_record_which_frames_more_than_is_held_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let a = producer(0x10);

        let mut claims = bytes::BytesMut::from(&fixed_framing::MAGIC[..]);
        claims.extend_from_slice(&u32::MAX.to_le_bytes());

        let mut beyond = bytes::BytesMut::new();
        while beyond.len() < READ_BYTES {
            fixed_framing::encode(&write(a, 0, 1, 0xaa), &mut beyond);
        }

        let err = refused(&dir, &[&claims, &beyond]);
        assert!(err.contains("more than the delta holds"), "{err}");
    }
}
