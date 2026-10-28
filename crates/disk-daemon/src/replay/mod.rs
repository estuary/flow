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
    /// Pushed content not yet framed, which may end partway through a record.
    pending: bytes::BytesMut,
    /// Journal offset at which `pending` begins.
    offset: i64,
    /// Records of a committed delta not yet returned.
    committed: Option<buffer::Drain>,
}

impl Replay {
    pub fn new(spill_dir: &std::path::Path) -> Self {
        Self {
            sequencer: sequencer::Sequencer::default(),
            buffer: buffer::Buffer::new(spill_dir),
            pending: bytes::BytesMut::new(),
            offset: 0,
            committed: None,
        }
    }

    /// Push a read response's `content`, which begins at journal `offset`, for
    /// `next_committed` to sequence.
    pub fn push(&mut self, offset: i64, content: bytes::Bytes) {
        // Pending content continues into this response: the reader refuses a
        // gap once it has delivered content.
        self.offset = offset - self.pending.len() as i64;
        self.pending.extend_from_slice(&content);
    }

    /// The next committed chunk, and the offset of its record if that record opens
    /// a horizon, or `None` once the pushed content holds no more.
    pub fn next_committed(&mut self) -> anyhow::Result<Option<(proto::Chunk, Option<i64>)>> {
        loop {
            if let Some(committed) = &mut self.committed
                && let Some((at, framed)) = committed
                    .drain_next()
                    .context("reading back an uncommitted delta")?
            {
                let proto::DiskRecord {
                    chunk,
                    opens_horizon,
                    ..
                } = prost::Message::decode(framed.slice(fixed_framing::HEADER_LEN..))
                    .context("decoding a record")?;
                let chunk = chunk.context("a committed record carries no chunk")?;
                return Ok(Some((chunk, opens_horizon.then_some(at))));
            }
            self.committed = None;

            let frame = fixed_framing::unpack::<proto::DiskRecord>(&mut self.pending)
                .context("decoding a record")?;
            let (record, framed) = match frame {
                fixed_framing::Frame::Record { message, framed } => (message, framed),
                fixed_framing::Frame::Incomplete => return Ok(None),
                fixed_framing::Frame::Desync { .. } => {
                    anyhow::bail!("encountered unframed content")
                }
            };
            let at = self.offset;
            self.offset += framed.len() as i64;

            let (rolled_back, action) = self.sequencer.on_record(&record.uuid)?;

            if rolled_back {
                self.buffer.clear();
            }

            match action {
                sequencer::Action::Skip => {}
                sequencer::Action::Hold => () = self.buffer.push(at, framed)?,
                sequencer::Action::Commit => self.committed = Some(self.buffer.drain()),
            }
        }
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

        // Two deltas, which a read from a floor at offset 1000 returns. The second
        // record opens no horizon, so replay reports no offset for it.
        let records = [
            record(a, 1, write, fill(1, 0xaa)),
            proto::DiskRecord {
                opens_horizon: false,
                ..record(a, 2, write, fill(2, 0xbb))
            },
            record(a, 3, ack, None),
            record(a, 4, write, fill(3, 0xcc)),
            record(a, 5, ack, None),
        ];
        let at = offsets(1000, &records);
        let journal = frame(&records).freeze();
        let end = 1000 + journal.len() as i64;
        let chunk = |index: usize| records[index].chunk.clone().unwrap();

        // Push the journal's content from offset `begin` to `end` as one read response,
        // and return the chunks it applies, with the offsets of horizon openers.
        let mut push = |begin: i64, end: i64| {
            let content = journal.slice((begin - 1000) as usize..(end - 1000) as usize);
            let mut applied = Vec::new();
            replay.push(begin, content);
            while let Some(committed) = replay.next_committed().unwrap() {
                applied.push(committed);
            }
            applied
        };

        // The first delta is held, and applies nothing yet.
        assert!(push(at[0], at[2]).is_empty());

        // Its acknowledgement commits it, and its chunks apply in order. An opener
        // reports the offset its record begins at.
        assert_eq!(
            push(at[2], at[3]),
            [(chunk(0), Some(at[0])), (chunk(1), None)]
        );

        // A response may end partway through a record, which the next response
        // completes. The record begins in the earlier response.
        assert!(push(at[3], at[3] + 10).is_empty());
        assert_eq!(push(at[3] + 10, end), [(chunk(3), Some(at[3]))]);
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
                    replay.push(begin, bytes::Bytes::copy_from_slice(response));
                    while let Some((chunk, opens_horizon_at)) = replay.next_committed()? {
                        applied.push((opens_horizon_at.unwrap(), chunk.block));
                    }
                    anyhow::Ok(())
                });
            let error = outcome.err().map(|err| format!("{err:#}"));

            assert_eq!(
                (applied.as_slice(), error.as_deref()),
                (expect_applied, expect_error),
                "{name}"
            );
        }
    }

    /// A record of `producer` at `clock`, carrying `chunk`. A record with a chunk
    /// opens a horizon, so that replay reports the offset it begins at.
    fn record(
        producer: uuid::Producer,
        clock: u64,
        flags: uuid::Flags,
        chunk: Option<proto::Chunk>,
    ) -> proto::DiskRecord {
        let uuid = uuid::build(producer, uuid::Clock::from_u64(clock), flags);

        proto::DiskRecord {
            uuid: bytes::Bytes::copy_from_slice(uuid.as_bytes()),
            opens_horizon: chunk.is_some(),
            chunk,
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
