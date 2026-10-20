use crate::proto;
use anyhow::Context;
use proto_gazette::fixed_framing;

/// Bytes buffered before they spill to the file in one write.
const SPILL_BYTES: usize = 1 << 20;

/// Size of each read of the spill file. A delta may be larger than memory.
const READ_BYTES: usize = 64 << 10;

/// The unacknowledged delta of one replay.
pub struct UncommittedDelta {
    /// Anonymous spill file beside the disk image.
    file: std::fs::File,
    /// Bytes spilled to `file`, which is also its length.
    spilled: u64,
    /// Records held in memory, which follow every record in `file`.
    buffered: bytes::BytesMut,
    /// Bytes buffered before they spill.
    spill_bytes: usize,
}

impl UncommittedDelta {
    /// Create the file of an uncommitted delta within `dir`, which is the daemon's image
    /// directory.
    pub fn create(dir: &std::path::Path) -> std::io::Result<Self> {
        Ok(Self {
            file: tempfile::tempfile_in(dir)?,
            spilled: 0,
            buffered: bytes::BytesMut::new(),
            spill_bytes: SPILL_BYTES,
        })
    }

    /// Hold `framed`, the journal's own bytes of one record. Records spill whole, so
    /// the file always ends at a record boundary.
    pub(super) fn push(&mut self, framed: &[u8]) -> anyhow::Result<()> {
        self.buffered.extend_from_slice(framed);

        if self.buffered.len() < self.spill_bytes {
            return Ok(());
        }
        () = std::os::unix::fs::FileExt::write_all_at(&self.file, &self.buffered, self.spilled)
            .context("spilling an unacknowledged delta")?;

        self.spilled += self.buffered.len() as u64;
        self.buffered.clear();

        Ok(())
    }

    /// Read the delta's records back in order.
    pub(super) fn drain(&mut self) -> Drain<'_> {
        Drain {
            delta: self,
            buf: bytes::BytesMut::new(),
            offset: 0,
        }
    }

    /// Drop buffered records and truncate the spill file without touching the image.
    pub(super) fn clear(&mut self) -> std::io::Result<()> {
        if self.spilled != 0 {
            () = self.file.set_len(0)?;
        }
        self.spilled = 0;
        self.buffered = bytes::BytesMut::new();

        Ok(())
    }
}

/// Reads an uncommitted delta's records back, in the order they were held.
pub(super) struct Drain<'a> {
    /// The delta whose records are read back.
    delta: &'a mut UncommittedDelta,
    /// Bytes read back and not yet unpacked.
    buf: bytes::BytesMut,
    /// Spilled bytes read back so far.
    offset: u64,
}

impl Drain<'_> {
    /// The next record, or `None` once every record has been read and the delta is
    /// cleared.
    pub fn next(&mut self) -> anyhow::Result<Option<proto::DiskRecord>> {
        loop {
            if let Some(record) = unpack(&mut self.buf) {
                return Ok(Some(record));
            }

            if self.offset != self.delta.spilled {
                let held = self.buf.len();
                let len = std::cmp::min(READ_BYTES as u64, self.delta.spilled - self.offset);
                self.buf.resize(held + len as usize, 0);

                () = std::os::unix::fs::FileExt::read_exact_at(
                    &self.delta.file,
                    &mut self.buf[held..],
                    self.offset,
                )
                .context("reading back an uncommitted delta")?;
                self.offset += len;

                continue;
            }

            // Records spill whole, so the file ended at a record boundary, and the
            // records held in memory follow it.
            if !self.delta.buffered.is_empty() {
                assert!(self.buf.is_empty(), "a spilled record was cut short");
                self.buf = std::mem::take(&mut self.delta.buffered);

                continue;
            }

            () = self.delta.clear()?;
            return Ok(None);
        }
    }
}

/// Decode the first record of `buf`, or `None` where it holds no whole record.
fn unpack(buf: &mut bytes::BytesMut) -> Option<proto::DiskRecord> {
    match fixed_framing::unpack::<proto::DiskRecord>(buf).expect("an uncommitted record decodes") {
        fixed_framing::Frame::Record { message, .. } => Some(message),
        fixed_framing::Frame::Desync { skipped } => {
            panic!("{} unframed bytes in an uncommitted delta", skipped.len())
        }
        fixed_framing::Frame::Incomplete => None,
    }
}

#[cfg(test)]
pub(super) mod test {
    use super::{READ_BYTES, SPILL_BYTES, UncommittedDelta};
    use crate::BLOCK_SIZE;
    use crate::proto;
    use crate::test_support;
    use proto_gazette::{fixed_framing, uuid};
    use std::fmt::Write as _;

    /// Each way a delta may be held: every record spilled, records spilled in batches
    /// with the rest held in memory, and every record held in memory.
    const SPILLS: [usize; 3] = [0, 3 * READ_BYTES, SPILL_BYTES];

    /// A record writing `blocks` blocks of `fill`, beginning at `block`. Nothing here
    /// sequences records, so every one is at the epoch.
    fn write(producer: uuid::Producer, block: u32, blocks: u32, fill: u8) -> proto::DiskRecord {
        let chunk = test_support::write_chunk(block, blocks, fill);

        test_support::record(producer, 0, uuid::Flags::CONTINUE_TXN, Some(chunk))
    }

    /// A record whose chunk carries `bytes` bytes of `fill`, which is how a case
    /// places its framing at an offset a block-aligned write could not reach.
    fn sized(producer: uuid::Producer, block: u32, bytes: usize, fill: u8) -> proto::DiskRecord {
        let chunk = proto::Chunk {
            block,
            blocks: bytes.div_ceil(BLOCK_SIZE as usize) as u32,
            content: Some(proto::chunk::Content::Data(bytes::Bytes::from(vec![
                fill;
                bytes
            ]))),
        };
        test_support::record(producer, 0, uuid::Flags::CONTINUE_TXN, Some(chunk))
    }

    /// Bytes `uncommitted` holds, in memory and spilled.
    pub(in crate::journal) fn held(uncommitted: &UncommittedDelta) -> u64 {
        uncommitted.spilled + uncommitted.buffered.len() as u64
    }

    /// Hold `records`, framed exactly as the journal frames them.
    fn hold(uncommitted: &mut UncommittedDelta, records: &[proto::DiskRecord]) {
        for record in records {
            uncommitted.push(&test_support::frame([record])).unwrap();
        }
    }

    /// An uncommitted delta within `dir` which spills past `spill_bytes`.
    fn delta(dir: &tempfile::TempDir, spill_bytes: usize) -> UncommittedDelta {
        UncommittedDelta {
            spill_bytes,
            ..UncommittedDelta::create(dir.path()).unwrap()
        }
    }

    /// Drain `uncommitted`, collecting what it hands back and the failure, if any,
    /// which stopped it.
    fn drained(uncommitted: &mut UncommittedDelta) -> (Vec<proto::DiskRecord>, anyhow::Result<()>) {
        let mut drain = uncommitted.drain();
        let mut records = Vec::new();

        let result = loop {
            match drain.next() {
                Ok(Some(record)) => records.push(record),
                Ok(None) => break Ok(()),
                Err(err) => break Err(err),
            }
        };
        (records, result)
    }

    /// Trace bytes held and the spill file's length after `step`.
    fn row(step: &str, uncommitted: &UncommittedDelta) -> String {
        format!(
            "{step:<13}held {:<8}file {}",
            held(uncommitted),
            uncommitted.file.metadata().unwrap().len(),
        )
    }

    /// A delta of many records, spanning many fills of the reader's buffer, reads
    /// back in the order it was held, however much of it spilled. One smaller than
    /// [`SPILL_BYTES`] never touches the file.
    #[test]
    fn test_a_many_record_delta_reads_back_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let a = test_support::producer('a');
        let records: Vec<_> = (0..200u32).map(|i| write(a, i % 8, 1, i as u8)).collect();
        let mut trace = String::new();

        for spill_bytes in SPILLS {
            let mut uncommitted = delta(&dir, spill_bytes);

            hold(&mut uncommitted, &records);
            assert!(
                held(&uncommitted) > 8 * READ_BYTES as u64,
                "many held fills of delta"
            );
            let step = row("hold", &uncommitted);
            writeln!(trace, "spill past {spill_bytes:<9}{step}").unwrap();

            let (read, result) = drained(&mut uncommitted);
            () = result.unwrap();

            assert!(read == records, "spilling past {spill_bytes} bytes");
            let step = row(&format!("drained {}", read.len()), &uncommitted);
            writeln!(trace, "spill past {spill_bytes:<9}{step}").unwrap();
        }
        insta::assert_snapshot!(trace, @"
        spill past 0        hold         held 826350  file 826350
        spill past 0        drained 200  held 0       file 0
        spill past 196608   hold         held 826350  file 793296
        spill past 196608   drained 200  held 0       file 0
        spill past 1048576  hold         held 826350  file 0
        spill past 1048576  drained 200  held 0       file 0
        ");
    }

    /// A record may be larger than the buffer it is read through, and is then read
    /// whole. Framing decodes a record as a unit, and a chunk of the device's
    /// maximum request size already exceeds the buffer.
    #[test]
    fn test_a_record_larger_than_the_read_buffer() {
        let dir = tempfile::tempdir().unwrap();
        let a = test_support::producer('a');

        let blocks = 2 + READ_BYTES as u32 / BLOCK_SIZE;
        let records = [
            write(a, 0, 1, 0xaa),
            write(a, 1, blocks, 0xbb),
            write(a, 1 + blocks, 1, 0xcc),
        ];

        for spill_bytes in SPILLS {
            let mut uncommitted = delta(&dir, spill_bytes);
            hold(&mut uncommitted, &records);

            let (read, result) = drained(&mut uncommitted);
            () = result.unwrap();

            assert!(read == records, "spilling past {spill_bytes} bytes");
        }
    }

    /// A spilled frame may cross a fill of the reader's buffer at any offset,
    /// including within its own header. Sweeping the size of the first record walks
    /// the second record's start across that edge, and the sweep asserts that it
    /// reached the header case.
    #[test]
    fn test_a_record_may_straddle_the_read_buffer() {
        let dir = tempfile::tempdir().unwrap();
        let a = test_support::producer('a');
        let mut split_header = false;

        for bytes in READ_BYTES - 72..READ_BYTES + 8 {
            let mut uncommitted = delta(&dir, 0);

            let first = sized(a, 0, bytes, 0xaa);
            let framed = test_support::frame([&first]);

            split_header |=
                (READ_BYTES - fixed_framing::HEADER_LEN..READ_BYTES).contains(&framed.len());

            let records = [first, write(a, 40, 1, 0xbb), write(a, 41, 1, 0xcc)];
            hold(&mut uncommitted, &records);

            let (read, result) = drained(&mut uncommitted);
            () = result.unwrap();

            assert!(read == records, "with {bytes} bytes of data");
        }
        assert!(split_header, "a frame header straddled a held boundary");
    }

    /// A drain truncates the file it spilled to, so the next delta's spill starts
    /// over and reads back without the prior delta's tail.
    #[test]
    fn test_a_drained_file_holds_the_next_delta() {
        let dir = tempfile::tempdir().unwrap();
        let mut uncommitted = delta(&dir, 0);
        let a = test_support::producer('a');
        let mut trace = String::new();

        hold(
            &mut uncommitted,
            &[write(a, 0, 2, 0xaa), write(a, 1, 1, 0xbb)],
        );
        let prior_len = held(&uncommitted);
        writeln!(trace, "{}", row("hold prior", &uncommitted)).unwrap();

        let (read, result) = drained(&mut uncommitted);
        () = result.unwrap();
        let step = row(&format!("drained {}", read.len()), &uncommitted);
        writeln!(trace, "{step}").unwrap();

        let next = [write(a, 1, 1, 0xcc), write(a, 2, 1, 0xdd)];
        hold(&mut uncommitted, &next);
        assert!(held(&uncommitted) < prior_len, "the next delta is shorter");
        writeln!(trace, "{}", row("hold next", &uncommitted)).unwrap();

        let (read, result) = drained(&mut uncommitted);
        () = result.unwrap();

        assert!(
            read == next,
            "the next delta read back with the prior one's tail"
        );
        let step = row(&format!("drained {}", read.len()), &uncommitted);
        writeln!(trace, "{step}").unwrap();

        insta::assert_snapshot!(trace, @"
        hold prior   held 12358   file 12358
        drained 2    held 0       file 0
        hold next    held 8264    file 8264
        drained 2    held 0       file 0
        ");
    }
}
