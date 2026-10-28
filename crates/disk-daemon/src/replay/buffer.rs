use anyhow::Context;
use proto_gazette::fixed_framing;

/// Bytes held in memory before they spill to the file.
const SPILL_BYTES: usize = 1 << 20; // 1 MiB

/// Capacity of the reader of the spill file. A delta may be larger than memory.
const READ_BYTES: usize = 64 << 10; // 64 KiB

/// Bytes of a frame's offset, which is held and spilled with it.
const OFFSET_LEN: usize = std::mem::size_of::<i64>();

/// The unacknowledged delta of one replay, as the journal framed its records, with
/// the offset each begins at. Once it commits, its frames are read back in the order
/// they were held.
pub(super) struct Buffer {
    /// Directory of the unnamed spill file: the daemon's image directory.
    dir: std::path::PathBuf,
    /// Spilled frames, which precede `held`, each after its little-endian offset.
    file: Option<std::fs::File>,
    /// Frames in memory and their offsets, which share the buffers they were framed from.
    held: Vec<(i64, bytes::Bytes)>,
    /// Bytes of the frames in `held`, and of their offsets.
    held_bytes: usize,
}

impl Buffer {
    /// An empty delta, which spills to an unnamed file in `dir`.
    pub fn new(dir: &std::path::Path) -> Self {
        Self {
            dir: dir.to_owned(),
            file: None,
            held: Vec::new(),
            held_bytes: 0,
        }
    }

    /// Hold `framed`, the journal's own bytes of one record, which begins at journal
    /// `offset`. Past `SPILL_BYTES`, append every held frame to the delta's file.
    pub fn push(&mut self, offset: i64, framed: bytes::Bytes) -> anyhow::Result<()> {
        self.held_bytes += OFFSET_LEN + framed.len();
        self.held.push((offset, framed));

        if self.held_bytes >= SPILL_BYTES {
            let file = match &mut self.file {
                Some(file) => file,
                None => self.file.insert(
                    tempfile::tempfile_in(&self.dir)
                        .with_context(|| format!("creating a spill file in {:?}", self.dir))?,
                ),
            };
            let mut spill = std::io::BufWriter::new(file);
            for (offset, framed) in self.held.drain(..) {
                () = std::io::Write::write_all(&mut spill, &offset.to_le_bytes())
                    .and_then(|()| std::io::Write::write_all(&mut spill, &framed))
                    .context("spilling an unacknowledged delta")?;
            }
            () = std::io::Write::flush(&mut spill).context("spilling an unacknowledged delta")?;
            self.held_bytes = 0;
        }
        Ok(())
    }

    /// Drain the delta, leaving the buffer empty for the next one.
    pub fn drain(&mut self) -> Drain {
        let spilled = self.file.take().map(|mut file| {
            // Spills leave the cursor at the end, and rewinding a regular file can't fail.
            () = std::io::Seek::rewind(&mut file).expect("rewinding a spill file");
            std::io::BufReader::with_capacity(READ_BYTES, file)
        });
        self.held_bytes = 0;

        Drain {
            spilled,
            held: std::mem::take(&mut self.held).into_iter(),
        }
    }

    /// Drop the delta's frames, and with them any file it spilled to.
    pub fn clear(&mut self) {
        self.file = None;
        self.held.clear();
        self.held_bytes = 0;
    }
}

/// A committed delta's frames and their offsets.
pub(super) struct Drain {
    spilled: Option<std::io::BufReader<std::fs::File>>,
    held: std::vec::IntoIter<(i64, bytes::Bytes)>,
}

impl Drain {
    /// The delta's next frame and its offset, or `None` once it's drained.
    pub fn drain_next(&mut self) -> anyhow::Result<Option<(i64, bytes::Bytes)>> {
        let Some(reader) = &mut self.spilled else {
            return Ok(self.held.next());
        };
        if std::io::BufRead::fill_buf(reader)?.is_empty() {
            self.spilled = None; // Release the spill file.
            return Ok(self.held.next());
        }
        let mut offset = [0; OFFSET_LEN];
        () = std::io::Read::read_exact(reader, &mut offset)?;

        let mut head = [0; fixed_framing::HEADER_LEN];
        () = std::io::Read::read_exact(reader, &mut head)?;

        let fixed_framing::Header::Frame { payload } = fixed_framing::header(&head) else {
            panic!("a spilled frame has no header");
        };
        let mut framed = bytes::BytesMut::zeroed(fixed_framing::HEADER_LEN + payload);
        framed[..fixed_framing::HEADER_LEN].copy_from_slice(&head);
        () = std::io::Read::read_exact(reader, &mut framed[fixed_framing::HEADER_LEN..])?;

        Ok(Some((i64::from_le_bytes(offset), framed.freeze())))
    }
}

#[cfg(test)]
mod test {
    use super::Buffer;
    use crate::proto;
    use proto_gazette::fixed_framing;

    #[test]
    fn test_deltas_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let mut buffer = Buffer::new(dir.path());

        // (name, frames held, blocks per frame, spills, cleared rather than drained)
        //
        // Each delta is held after the one before it was drained or cleared, and reads
        // back none of that prior delta.
        let cases = [
            // Frames read back in order from the file and then from memory, each whole
            // though larger than READ_BYTES: a frame of 20 blocks exceeds it, and the
            // 13th crosses SPILL_BYTES, so the 14th stays in memory.
            ("spills", 14, 20, true, false),
            // The prior drain released its file, so a delta that never spills reads
            // back only what memory holds.
            ("in memory", 10, 1, false, false),
            // Clearing drops both the spill file and the frames in memory.
            ("cleared", 14, 20, true, true),
        ];

        for (name, count, blocks, spills, cleared) in cases {
            // Each frame's offset is its block, so that a frame read back with
            // another's offset is caught.
            let frames: Vec<(i64, bytes::Bytes)> = (0..count)
                .map(|block| {
                    let data = vec![block as u8; (blocks * crate::BLOCK_SIZE) as usize];
                    let record = proto::DiskRecord {
                        chunk: Some(proto::Chunk {
                            block,
                            blocks,
                            content: Some(proto::chunk::Content::Data(data.into())),
                        }),
                        ..Default::default()
                    };
                    let mut framed = bytes::BytesMut::new();
                    fixed_framing::encode(&record, &mut framed);

                    (block as i64, framed.freeze())
                })
                .collect();

            for (offset, framed) in &frames {
                () = buffer.push(*offset, framed.clone()).unwrap();
            }
            assert_eq!(buffer.file.is_some(), spills, "{name}");

            let expect = if cleared {
                buffer.clear();
                Vec::new()
            } else {
                frames
            };
            let mut drain = buffer.drain();
            let mut read = Vec::new();
            while let Some(frame) = drain.drain_next().unwrap() {
                read.push(frame);
            }
            assert!(
                read == expect,
                "{name}: read back {} frames of {}",
                read.len(),
                expect.len()
            );
        }
    }
}
