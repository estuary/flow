use crate::proto;
use anyhow::Context;
use proto_gazette::fixed_framing;

mod buffer;
mod sequencer;

/// Turns the journal content a standby reads into the records of committed
/// deltas, which its device applies.
pub struct Replay {
    /// Per-producer ordering and acknowledgement state.
    sequencer: sequencer::Sequencer,
    /// Records of the current delta, held until it's acknowledged.
    buffer: buffer::Buffer,
    /// Content of earlier responses which ends partway through a record.
    remainder: bytes::BytesMut,
}

impl Replay {
    pub fn new(spill_dir: &std::path::Path) -> Self {
        Self {
            sequencer: sequencer::Sequencer::default(),
            buffer: buffer::Buffer::new(spill_dir),
            remainder: bytes::BytesMut::new(),
        }
    }

    /// Push a read response's `content`, which begins at journal `offset`, sequencing
    /// its records in order, and hand each committed delta's records to `apply` with
    /// the offsets they begin at.
    pub fn push(
        &mut self,
        offset: i64,
        mut content: bytes::Bytes,
        mut apply: impl FnMut(i64, proto::DiskRecord) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let decode = |framed: &bytes::Bytes| -> anyhow::Result<proto::DiskRecord> {
            prost::Message::decode(framed.slice(fixed_framing::HEADER_LEN..))
                .context("decoding a record")
        };
        // A remainder continues into this response: the reader refuses a gap once it
        // has delivered content.
        let mut next_offset = offset - self.remainder.len() as i64;

        while let Some(framed) = self.next_frame(&mut content)? {
            let at = next_offset;
            next_offset += framed.len() as i64;

            let record = decode(&framed)?;
            let (rolled_back, action) = self.sequencer.on_record(&record.uuid)?;

            if rolled_back {
                self.buffer.clear();
            }

            match action {
                sequencer::Action::Skip => {}
                sequencer::Action::Hold => () = self.buffer.push(at, framed)?,
                sequencer::Action::Commit => {
                    for held in self.buffer.drain() {
                        let (at, framed) = held?;
                        () = apply(at, decode(&framed)?)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Split off the next whole record of `content`, or `None` where none is
    /// whole yet.
    fn next_frame(&mut self, content: &mut bytes::Bytes) -> anyhow::Result<Option<bytes::Bytes>> {
        // Fast path: No remainder, `content` starts with a valid frame header,
        // and the whole frame is in `content`.
        if self.remainder.is_empty()
            && let fixed_framing::Header::Frame { payload } = fixed_framing::header(content)
            && content.len() >= fixed_framing::HEADER_LEN + payload
        {
            return Ok(Some(content.split_to(fixed_framing::HEADER_LEN + payload)));
        }

        // Copy from the front of `content` until `remainder` holds `len` bytes,
        // or `content` runs out.
        let fill = |remainder: &mut bytes::BytesMut, content: &mut bytes::Bytes, len: usize| {
            let missing = len.saturating_sub(remainder.len());
            remainder.reserve(missing);
            remainder.extend_from_slice(&content.split_to(missing.min(content.len())));
        };

        // Build the record in `remainder`: first its header, then the rest of the
        // frame the header declares.
        () = fill(&mut self.remainder, content, fixed_framing::HEADER_LEN);

        let payload_len = match fixed_framing::header(&self.remainder) {
            fixed_framing::Header::Frame { payload } => payload,
            fixed_framing::Header::Incomplete => return Ok(None),
            fixed_framing::Header::Desync { .. } => {
                anyhow::bail!("encountered unframed content")
            }
        };
        let len = fixed_framing::HEADER_LEN + payload_len;
        () = fill(&mut self.remainder, content, len);

        if self.remainder.len() < len {
            return Ok(None);
        }

        Ok(Some(self.remainder.split().freeze()))
    }
}

#[cfg(test)]
mod test {
    use super::Replay;
    use crate::proto;
    use proto_gazette::{fixed_framing, uuid};

    #[test]
    fn test_replay() {
        let dir = tempfile::tempdir().unwrap();
        let mut replay = Replay::new(dir.path());
        let a = uuid::Producer::from_bytes([0x01, 0, 0, 0, 0, 1]);
        let (write, ack) = (uuid::Flags::CONTINUE_TXN, uuid::Flags::ACK_TXN);
        let fill = |block, byte| {
            Some(proto::Chunk {
                block,
                blocks: 1,
                content: Some(proto::chunk::Content::Data(
                    vec![byte; crate::BLOCK_SIZE as usize].into(),
                )),
            })
        };

        // Two deltas, which a read from a floor at offset 1000 returns.
        let records = [
            record(a, 1, write, fill(1, 0xaa)),
            record(a, 2, write, fill(2, 0xbb)),
            record(a, 3, ack, None),
            record(a, 4, write, fill(3, 0xcc)),
            record(a, 5, ack, None),
        ];
        let at = offsets(1000, &records);
        let journal = frame(&records).freeze();
        let end = 1000 + journal.len() as i64;

        // Push the journal's content from offset `begin` to `end` as one read response,
        // and return the records it applies, with the offsets they begin at.
        let mut push = |begin: i64, end: i64| {
            let content = journal.slice((begin - 1000) as usize..(end - 1000) as usize);
            let mut applied = Vec::new();
            () = replay
                .push(begin, content, |offset, record| {
                    applied.push((offset, record));
                    Ok(())
                })
                .unwrap();
            applied
        };

        // The first delta is held, and applies nothing yet.
        assert!(push(at[0], at[2]).is_empty());

        // Its acknowledgement commits it, and its records apply in order, each at the
        // offset it begins at.
        assert_eq!(
            push(at[2], at[3]),
            [(at[0], records[0].clone()), (at[1], records[1].clone())]
        );

        // A response may end partway through a record, which the next response
        // completes. The record begins in the earlier response.
        assert!(push(at[3], at[3] + 10).is_empty());
        assert_eq!(push(at[3] + 10, end), [(at[3], records[3].clone())]);
    }

    #[test]
    fn test_push() {
        let dir = tempfile::tempdir().unwrap();
        let [a, b] = [1, 2].map(|n| uuid::Producer::from_bytes([0x01, 0, 0, 0, 0, n]));
        let (write, ack, fence) = (
            uuid::Flags::CONTINUE_TXN,
            uuid::Flags::ACK_TXN,
            uuid::Flags::OUTSIDE_TXN,
        );
        let punch = |block| {
            Some(proto::Chunk {
                block,
                blocks: 1,
                content: Some(proto::chunk::Content::Punch(proto::chunk::Punch {})),
            })
        };

        // Producer a commits block 1. Its next delta, of block 2, is rolled back by
        // producer b, which commits block 3.
        let records = [
            record(a, 1, write, punch(1)),
            record(a, 2, ack, None),
            record(a, 3, write, punch(2)),
            record(b, 0, fence, None),
            record(b, 1, write, punch(3)),
            record(b, 2, ack, None),
        ];
        let at = offsets(0, &records);
        let journal = frame(&records);
        let committed: &[(i64, u32)] = &[(at[0], 1), (at[4], 3)];

        // (name, content, bytes per response, offset and block of each record applied,
        // error)
        let cases = [
            ("one response", &journal[..], usize::MAX, committed, None),
            // Responses end partway through headers and bodies, and also carry
            // whole records.
            ("50 bytes per response", &journal[..], 50, committed, None),
            ("a byte per response", &journal[..], 1, committed, None),
            (
                "unframed",
                b"these bytes frame no record".as_slice(),
                usize::MAX,
                &[],
                Some("encountered unframed content"),
            ),
        ];

        for (name, content, size, expect_applied, expect_error) in cases {
            let mut replay = Replay::new(dir.path());
            let mut applied = Vec::new();

            let outcome = content
                .chunks(size)
                .enumerate()
                .try_for_each(|(index, response)| {
                    let begin = (index * size) as i64;
                    replay.push(
                        begin,
                        bytes::Bytes::copy_from_slice(response),
                        |offset, record| {
                            applied.push((offset, record.chunk.unwrap().block));
                            Ok(())
                        },
                    )
                });
            let error = outcome.err().map(|err| format!("{err:#}"));

            assert_eq!(
                (applied.as_slice(), error.as_deref()),
                (expect_applied, expect_error),
                "{name}"
            );
        }
    }

    /// A record of `producer` at `clock`, carrying `chunk`.
    fn record(
        producer: uuid::Producer,
        clock: u64,
        flags: uuid::Flags,
        chunk: Option<proto::Chunk>,
    ) -> proto::DiskRecord {
        let uuid = uuid::build(producer, uuid::Clock::from_u64(clock), flags);

        proto::DiskRecord {
            uuid: bytes::Bytes::copy_from_slice(uuid.as_bytes()),
            chunk,
            opens_horizon: false,
        }
    }

    /// `records`, framed as the journal frames them.
    fn frame(records: &[proto::DiskRecord]) -> bytes::BytesMut {
        let mut framed = bytes::BytesMut::new();
        for record in records {
            fixed_framing::encode(record, &mut framed);
        }
        framed
    }

    /// The journal offset at which each of `records` begins, framed from `begin`.
    fn offsets(begin: i64, records: &[proto::DiskRecord]) -> Vec<i64> {
        records
            .iter()
            .scan(begin, |next, record| {
                let at = *next;
                *next += (fixed_framing::HEADER_LEN + prost::Message::encoded_len(record)) as i64;
                Some(at)
            })
            .collect()
    }
}
