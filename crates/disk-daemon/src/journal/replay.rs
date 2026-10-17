use super::{sequencer, uncommitted};
use crate::horizon;
use crate::image;
use crate::proto;
use anyhow::Context;
use proto_gazette::{broker, fixed_framing};

/// Image and sequencing state shared by backfill, tailing, and promotion.
pub(super) struct Replay {
    /// The disk's committed state, rebuilt so far.
    image: image::Image,
    /// Per-producer ordering and acknowledgement state.
    sequencer: sequencer::Sequencer,
    /// Records of the current delta, held until it's acknowledged.
    uncommitted: uncommitted::UncommittedDelta,
    /// Offset of the held record which opens a horizon, if any.
    held_opens_at: Option<i64>,
    /// A horizon which has opened and is not yet discharged.
    horizon: Option<OpenHorizon>,
    /// Counts across reads and cancellation. Promotion tells committed state from a
    /// fresh disk by this count.
    applied_chunks: usize,
    /// Whether this replay has taken a record. Only before then may a read skip
    /// ahead, past content pruned below the floor. Any later skip is a gap.
    read_any: bool,
    /// Next record boundary, initially the floor.
    offset: i64,
}

impl Replay {
    /// A replay into `image` from `floor`, which holds each delta in `uncommitted`
    /// until its acknowledgement.
    pub fn new(
        image: image::Image,
        uncommitted: uncommitted::UncommittedDelta,
        floor: i64,
    ) -> Self {
        Self {
            image,
            sequencer: sequencer::Sequencer::default(),
            uncommitted,
            held_opens_at: None,
            horizon: None,
            applied_chunks: 0,
            read_any: false,
            offset: floor,
        }
    }

    /// Chunks applied, which is zero for a journal with no committed state.
    pub fn applied_chunks(&self) -> usize {
        self.applied_chunks
    }

    /// The replay's sequencing, which says which acknowledgements it can honor.
    pub fn sequencer(&self) -> &sequencer::Sequencer {
        &self.sequencer
    }

    /// Take the image and any horizon still open, once the replay is done. A delta
    /// still held is dropped: past the fence, no acknowledgement can commit it.
    pub fn into_parts(self) -> (image::Image, Option<OpenHorizon>) {
        (self.image, self.horizon)
    }

    /// Take `framed`, a whole record which promotion appended at this replay's
    /// offset, as if it were read from the journal.
    pub fn take_appended(&mut self, framed: &[u8]) -> anyhow::Result<()> {
        let mut buf = bytes::BytesMut::new();
        () = self.take(&mut buf, self.offset, framed)?;

        assert!(buf.is_empty(), "an appended record is whole");
        Ok(())
    }

    /// Consume a read response, retaining partial frames in `buf`.
    fn take(
        &mut self,
        buf: &mut bytes::BytesMut,
        offset: i64,
        content: &[u8],
    ) -> anyhow::Result<()> {
        let expected = self.offset + buf.len() as i64;

        if offset != expected {
            anyhow::ensure!(
                !self.read_any && buf.is_empty(),
                "content this replay needed was deleted from the store, \
                 from offset {expected} to {offset}",
            );
            self.offset = offset;
        }
        buf.extend_from_slice(content);

        loop {
            let at = self.offset;

            match fixed_framing::unpack::<proto::DiskRecord>(buf)
                .with_context(|| format!("decoding a record at offset {at}"))?
            {
                fixed_framing::Frame::Record { message, framed } => {
                    () = self
                        .record(&message, &framed)
                        .with_context(|| format!("taking the record at offset {at}"))?;
                }
                fixed_framing::Frame::Desync { skipped } => anyhow::bail!(
                    "{} unframed bytes at offset {at}, which a replay cannot skip",
                    skipped.len(),
                ),
                fixed_framing::Frame::Incomplete => return Ok(()),
            }
        }
    }

    fn record(&mut self, record: &proto::DiskRecord, framed: &[u8]) -> anyhow::Result<()> {
        let offset = self.offset;
        self.read_any = true;
        let sequencer::Step { displaced, action } = self.sequencer.on_record(record)?;
        self.offset = offset + framed.len() as i64;

        if displaced.is_some() {
            () = self.uncommitted.clear()?;
            self.held_opens_at = None;
        }

        match action {
            sequencer::Action::Skip => Ok(()),
            sequencer::Action::Hold => {
                if record.opens_horizon {
                    self.held_opens_at = Some(offset);
                }
                self.uncommitted.push(framed)
            }
            sequencer::Action::Commit => {
                // Apply before checking completion: these chunks may discharge the
                // horizon's last pending blocks.
                let opens_at = self.held_opens_at.take();
                self.applied_chunks += self.apply_held(opens_at)?;

                // A committed delta which discharged the last block of the horizon
                // completes it, and its writer stored the floor as it committed.
                _ = self.horizon.take_if(|open| open.blocks.pending() == 0);
                Ok(())
            }
        }
    }

    /// Apply the acknowledged delta in record order and count its chunks.
    fn apply_held(&mut self, opens_at: Option<i64>) -> anyhow::Result<usize> {
        let Self {
            image,
            uncommitted,
            horizon,
            ..
        } = self;
        let mut applied = 0;

        () = uncommitted.drain(|record| {
            if record.opens_horizon {
                let at = opens_at.expect("the replay held the offset of an opening record");
                let blocks = horizon::Horizon::open(image.allocated());
                *horizon = Some(OpenHorizon { at, blocks });
            }

            // A horizon opens at a record and a replay is a forward pass, so every
            // chunk here is at or after any horizon which is open.
            for chunk in &record.chunks {
                () = image
                    .apply(chunk)
                    .with_context(|| format!("applying chunk at block {}", chunk.block))?;

                if let Some(open) = horizon {
                    () = open.blocks.published(crate::chunk::covered_blocks(chunk));
                }
            }
            applied += record.chunks.len();

            Ok(())
        })?;

        Ok(applied)
    }
}

/// Continue from the replay's next record boundary. `Some(head)` reads to a
/// broker-confirmed head; `None` tails until cancelled or an error occurs.
/// [`Replay::take`] checks any offset jump returned by the broker.
pub(super) async fn read(
    client: &gazette::journal::Client,
    journal: &str,
    through: Option<i64>,
    replay: &mut Replay,
) -> anyhow::Result<()> {
    let (end_offset, block) = match through {
        Some(head) => (head, false),
        None => (0, true),
    };

    let stream = client.clone().read(broker::ReadRequest {
        journal: journal.to_string(),
        offset: replay.offset,
        end_offset,
        block,
        ..Default::default()
    });
    futures::pin_mut!(stream);

    // Content of this read which is not yet unpacked into records. A record may
    // straddle the responses which carry it.
    let mut buf = bytes::BytesMut::new();

    while let Some(response) = futures::StreamExt::next(&mut stream).await {
        let response = match response {
            Ok(response) => response,
            Err(gazette::RetryError { attempt, inner }) if inner.is_transient() => {
                tracing::warn!(journal, attempt, %inner, "journal read failed (will retry)");
                continue;
            }
            Err(gazette::RetryError { inner, .. }) => {
                return Err(anyhow::Error::new(inner).context(format!("reading {journal}")));
            }
        };

        () = replay
            .take(&mut buf, response.offset, &response.content)
            .with_context(|| format!("replaying {journal}"))?;
    }
    Ok(())
}

/// A recovery horizon this replay has opened.
pub(super) struct OpenHorizon {
    /// Offset at which the record which opened this horizon begins.
    pub at: i64,
    /// Committed blocks still pending a copy for this horizon.
    pub blocks: horizon::Horizon,
}

#[cfg(test)]
mod test {
    use super::Replay;
    use crate::chunk;
    use crate::image;
    use crate::proto;
    use crate::test_support;
    use proto_gazette::uuid;
    use std::fmt::Write as _;

    const BLOCKS: u32 = 64;

    /// `record` as the first of a delta which opens a horizon.
    fn opens(record: proto::DiskRecord) -> proto::DiskRecord {
        proto::DiskRecord {
            opens_horizon: true,
            ..record
        }
    }

    fn write(producer: uuid::Producer, clock: u64, block: u32, fill: u8) -> proto::DiskRecord {
        let chunks = test_support::broker::write(block, fill).chunks;
        test_support::record(producer, clock, uuid::Flags::CONTINUE_TXN, chunks)
    }

    /// A record of `producer`'s delta which punches `block`. It frames far smaller than
    /// a write, which keeps the offsets of a case small enough to read.
    fn punch(producer: uuid::Producer, clock: u64, block: u32) -> proto::DiskRecord {
        let chunks = vec![chunk::encode_punch(block, 1)];
        test_support::record(producer, clock, uuid::Flags::CONTINUE_TXN, chunks)
    }

    fn ack(producer: uuid::Producer, clock: u64) -> proto::DiskRecord {
        test_support::record(producer, clock, uuid::Flags::ACK_TXN, Vec::new())
    }

    /// Journal offset at which the record at `index` begins.
    fn offset_of(records: &[proto::DiskRecord], index: usize) -> i64 {
        records[..index]
            .iter()
            .map(|record| test_support::frame([record]).len() as i64)
            .sum()
    }

    /// A replay from `floor` into an image of its own within `dir`.
    fn replay(dir: &tempfile::TempDir, floor: i64) -> Replay {
        Replay::new(
            image::Image::create(dir.path(), BLOCKS).unwrap(),
            super::uncommitted::UncommittedDelta::create(dir.path()).unwrap(),
            floor,
        )
    }

    /// `write a@2 3=aa` fills block 3 with 0xaa at producer a's clock 2.
    /// `punch` discards a block; `opens` is a write that opens a horizon.
    fn label(record: &proto::DiskRecord) -> String {
        let (producer, clock, flags) =
            uuid::parse(uuid::Uuid::from_slice(&record.uuid).unwrap()).unwrap();
        if flags.is_outside() {
            return format!("fence {}", test_support::name(producer));
        }
        let who = format!(
            "{}@{}",
            test_support::name(producer),
            clock.to_unix().1 / 1_000
        );

        if flags.is_ack() {
            return format!("ack {who}");
        }
        let [change] = &record.chunks[..] else {
            panic!("a record of {} chunks", record.chunks.len());
        };
        let (kind, what) = match &change.content {
            Some(proto::chunk::Content::Data(data)) => {
                ("write", format!("{}={:02x}", change.block, data[0]))
            }
            Some(proto::chunk::Content::Punch(_)) => ("punch", change.block.to_string()),
            None => panic!("a chunk without content"),
        };
        let kind = if record.opens_horizon { "opens" } else { kind };

        format!("{kind} {who} {what}")
    }

    /// Trace each record's offset, committed blocks, held bytes, and recovery state.
    /// Stop at the first rejected record. The final offset is where the next read
    /// resumes.
    fn play(replay: &mut Replay, records: &[proto::DiskRecord]) -> String {
        let mut out = String::new();

        for record in records {
            write!(out, "at {:<7}{:<17}", replay.offset, label(record)).unwrap();

            if let Err(err) = replay.record(record, &test_support::frame([record])) {
                writeln!(out, "{err:#}").unwrap();
                break;
            }
            let blocks: Vec<String> = test_support::allocated(&replay.image)
                .into_iter()
                .map(|(block, fill)| format!("{block}={fill:02x}"))
                .collect();
            let blocks = match blocks.is_empty() {
                true => "-".to_string(),
                false => blocks.join(" "),
            };
            let held = match super::uncommitted::test::held(&replay.uncommitted) {
                0 => "-".to_string(),
                held => held.to_string(),
            };
            let horizon = match &replay.horizon {
                Some(open) => format!("{} pending {}", open.at, open.blocks.pending()),
                None => "-".to_string(),
            };

            writeln!(out, "blocks {blocks:<11}held {held:<6}horizon {horizon}",).unwrap();
        }
        writeln!(out, "stands at {}", replay.offset).unwrap();
        out
    }

    /// A delta reaches the image only at its acknowledgement, and all of it lands
    /// there at once.
    #[test]
    fn test_a_delta_applies_at_its_acknowledgement() {
        let dir = tempfile::tempdir().unwrap();
        let a = test_support::producer('a');

        let trace = play(
            &mut replay(&dir, 0),
            &[
                test_support::fence(a),
                write(a, 2, 3, 0xaa),
                write(a, 3, 4, 0xbb),
                ack(a, 4),
            ],
        );
        insta::assert_snapshot!(trace, @"
        at 0      fence a          blocks -          held -     horizon -
        at 26     write a@2 3=aa   blocks -          held 4130  horizon -
        at 4156   write a@3 4=bb   blocks -          held 8260  horizon -
        at 8286   ack a@4          blocks 3=aa 4=bb  held -     horizon -
        stands at 8312
        ");
    }

    /// Keep unacknowledged changes out of the committed image, but resume reads
    /// after them so continuation does not fetch the held delta again.
    #[test]
    fn test_a_delta_still_in_doubt_is_held_and_not_applied() {
        let dir = tempfile::tempdir().unwrap();
        let a = test_support::producer('a');

        let trace = play(
            &mut replay(&dir, 0),
            &[
                write(a, 1, 3, 0xaa),
                ack(a, 2),
                // A delta the attachment never acknowledged.
                write(a, 3, 4, 0xbb),
                write(a, 4, 5, 0xcc),
            ],
        );
        insta::assert_snapshot!(trace, @"
        at 0      write a@1 3=aa   blocks -          held 4130  horizon -
        at 4130   ack a@2          blocks 3=aa       held -     horizon -
        at 4156   write a@3 4=bb   blocks 3=aa       held 4130  horizon -
        at 8286   write a@4 5=cc   blocks 3=aa       held 8260  horizon -
        stands at 12416
        ");
    }

    /// A delta which a replacement attachment's records follow is abandoned. Those
    /// records displace it, so it is dropped rather than carried for the rest of the
    /// replay, and nothing of it is ever applied.
    #[test]
    fn test_a_delta_a_replacement_attachment_abandoned_is_not_applied() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = (test_support::producer('a'), test_support::producer('b'));

        let trace = play(
            &mut replay(&dir, 0),
            &[
                write(a, 1, 3, 0xaa),
                ack(a, 2),
                write(a, 3, 5, 0xcc),
                test_support::fence(b),
                write(b, 5, 6, 0xee),
                ack(b, 6),
            ],
        );
        insta::assert_snapshot!(trace, @"
        at 0      write a@1 3=aa   blocks -          held 4130  horizon -
        at 4130   ack a@2          blocks 3=aa       held -     horizon -
        at 4156   write a@3 5=cc   blocks 3=aa       held 4130  horizon -
        at 8286   fence b          blocks 3=aa       held 4130  horizon -
        at 8312   write b@5 6=ee   blocks 3=aa       held 4130  horizon -
        at 12442  ack b@6          blocks 3=aa 6=ee  held -     horizon -
        stands at 12468
        ");
    }

    /// A promotion repairs the acknowledgement its client held before it appends
    /// anything of its own, so the delta that fence displaced is still applied.
    #[test]
    fn test_a_delta_a_promotion_repaired_is_applied() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = (test_support::producer('a'), test_support::producer('b'));

        let trace = play(
            &mut replay(&dir, 0),
            &[
                write(a, 1, 3, 0xaa),
                test_support::fence(b),
                ack(a, 3),
                write(b, 4, 5, 0xcc),
                ack(b, 5),
            ],
        );
        insta::assert_snapshot!(trace, @"
        at 0      write a@1 3=aa   blocks -          held 4130  horizon -
        at 4130   fence b          blocks -          held 4130  horizon -
        at 4156   ack a@3          blocks 3=aa       held -     horizon -
        at 4182   write b@4 5=cc   blocks 3=aa       held 4130  horizon -
        at 8312   ack b@5          blocks 3=aa 5=cc  held -     horizon -
        stands at 8338
        ");
    }

    /// The range may begin within a delta. That delta's acknowledgement then
    /// commits only the records which were in range.
    #[test]
    fn test_a_delta_which_begins_below_the_range_commits_what_is_in_it() {
        let dir = tempfile::tempdir().unwrap();
        let a = test_support::producer('a');

        let trace = play(&mut replay(&dir, 0), &[write(a, 5, 2, 0xaa), ack(a, 9)]);
        insta::assert_snapshot!(trace, @"
        at 0      write a@5 2=aa   blocks -          held 4130  horizon -
        at 4130   ack a@9          blocks 2=aa       held -     horizon -
        stands at 4156
        ");
    }

    /// At-least-once appends repeat records. Sequencing drops a repeat rather than
    /// applying it a second time over a newer value.
    #[test]
    fn test_duplicate_records_are_not_applied_again() {
        let dir = tempfile::tempdir().unwrap();
        let a = test_support::producer('a');

        let trace = play(
            &mut replay(&dir, 0),
            &[
                write(a, 1, 2, 0xaa),
                write(a, 2, 2, 0xbb),
                write(a, 1, 2, 0xaa),
                ack(a, 3),
                ack(a, 3),
            ],
        );
        insta::assert_snapshot!(trace, @"
        at 0      write a@1 2=aa   blocks -          held 4130  horizon -
        at 4130   write a@2 2=bb   blocks -          held 8260  horizon -
        at 8260   write a@1 2=aa   blocks -          held 8260  horizon -
        at 12390  ack a@3          blocks 2=bb       held -     horizon -
        at 12416  ack a@3          blocks 2=bb       held -     horizon -
        stands at 12442
        ");
    }

    #[test]
    fn test_a_punch_deallocates_what_an_earlier_delta_wrote() {
        let dir = tempfile::tempdir().unwrap();
        let a = test_support::producer('a');

        let trace = play(
            &mut replay(&dir, 0),
            &[
                write(a, 1, 8, 0xaa),
                write(a, 2, 9, 0xbb),
                ack(a, 3),
                punch(a, 4, 8),
                ack(a, 5),
            ],
        );
        insta::assert_snapshot!(trace, @"
        at 0      write a@1 8=aa   blocks -          held 4130  horizon -
        at 4130   write a@2 9=bb   blocks -          held 8260  horizon -
        at 8260   ack a@3          blocks 8=aa 9=bb  held -     horizon -
        at 8286   punch a@4 8      blocks 8=aa 9=bb  held 32    horizon -
        at 8318   ack a@5          blocks 9=bb       held -     horizon -
        stands at 8344
        ");
    }

    /// A record which opens a horizon snapshots the blocks allocated before its own
    /// chunks apply, and committed chunks discharge them. A horizon the range leaves
    /// open is the next attachment's to resume, with both where it opened and what it
    /// has left to discharge. The commit which discharges its last block closes it.
    #[test]
    fn test_a_horizon_stays_open_until_a_commit_discharges_it() {
        let dir = tempfile::tempdir().unwrap();
        let a = test_support::producer('a');

        let trace = play(
            &mut replay(&dir, 0),
            &[
                write(a, 1, 3, 0xaa),
                write(a, 2, 4, 0xbb),
                ack(a, 3),
                opens(write(a, 4, 3, 0xcc)),
                ack(a, 5),
                write(a, 6, 4, 0xdd),
                ack(a, 7),
            ],
        );
        insta::assert_snapshot!(trace, @"
        at 0      write a@1 3=aa   blocks -          held 4130  horizon -
        at 4130   write a@2 4=bb   blocks -          held 8260  horizon -
        at 8260   ack a@3          blocks 3=aa 4=bb  held -     horizon -
        at 8286   opens a@4 3=cc   blocks 3=aa 4=bb  held 4132  horizon -
        at 12418  ack a@5          blocks 3=cc 4=bb  held -     horizon 8286 pending 1
        at 12444  write a@6 4=dd   blocks 3=cc 4=bb  held 4130  horizon 8286 pending 1
        at 16574  ack a@7          blocks 3=cc 4=dd  held -     horizon -
        stands at 16600
        ");
    }

    /// A range may hold several horizons. Each replaces the one before it, so the one
    /// a range leaves open is the last which opened: here the second, whose block 3
    /// is still pending a copy, where the first would have closed.
    #[test]
    fn test_a_later_horizon_replaces_an_earlier_one() {
        let dir = tempfile::tempdir().unwrap();
        let a = test_support::producer('a');

        let trace = play(
            &mut replay(&dir, 0),
            &[
                write(a, 1, 3, 0xaa),
                write(a, 2, 4, 0xbb),
                ack(a, 3),
                opens(write(a, 4, 3, 0xcc)),
                ack(a, 5),
                opens(write(a, 6, 4, 0xdd)),
                ack(a, 7),
            ],
        );
        insta::assert_snapshot!(trace, @"
        at 0      write a@1 3=aa   blocks -          held 4130  horizon -
        at 4130   write a@2 4=bb   blocks -          held 8260  horizon -
        at 8260   ack a@3          blocks 3=aa 4=bb  held -     horizon -
        at 8286   opens a@4 3=cc   blocks 3=aa 4=bb  held 4132  horizon -
        at 12418  ack a@5          blocks 3=cc 4=bb  held -     horizon 8286 pending 1
        at 12444  opens a@6 4=dd   blocks 3=cc 4=bb  held 4132  horizon 8286 pending 1
        at 16576  ack a@7          blocks 3=cc 4=dd  held -     horizon 12444 pending 1
        stands at 16602
        ");
    }

    /// A horizon belongs to its delta. A horizon whose delta is never acknowledged
    /// never existed, exactly as its chunks never applied, and neither did one whose
    /// delta another producer's records displaced.
    #[test]
    fn test_a_horizon_of_an_uncommitted_delta_does_not_exist() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = (test_support::producer('a'), test_support::producer('b'));

        let trace = [
            play(
                &mut replay(&dir, 0),
                &[write(a, 1, 3, 0xaa), ack(a, 2), opens(write(a, 3, 4, 0xbb))],
            ),
            play(
                &mut replay(&dir, 0),
                &[
                    write(a, 1, 3, 0xaa),
                    ack(a, 2),
                    opens(write(a, 3, 4, 0xbb)),
                    write(b, 4, 5, 0xcc),
                    ack(b, 5),
                ],
            ),
        ]
        .join("\n");
        insta::assert_snapshot!(trace, @"
        at 0      write a@1 3=aa   blocks -          held 4130  horizon -
        at 4130   ack a@2          blocks 3=aa       held -     horizon -
        at 4156   opens a@3 4=bb   blocks 3=aa       held 4132  horizon -
        stands at 8288

        at 0      write a@1 3=aa   blocks -          held 4130  horizon -
        at 4130   ack a@2          blocks 3=aa       held -     horizon -
        at 4156   opens a@3 4=bb   blocks 3=aa       held 4132  horizon -
        at 8288   write b@4 5=cc   blocks 3=aa       held 4130  horizon -
        at 12418  ack b@5          blocks 3=aa 5=cc  held -     horizon -
        stands at 12444
        ");
    }

    #[test]
    fn test_malformed_records_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let a = test_support::producer('a');

        let zero_clock = bytes::Bytes::copy_from_slice(
            uuid::build(a, uuid::Clock::zero(), uuid::Flags::CONTINUE_TXN).as_bytes(),
        );

        let cases: [(&str, proto::DiskRecord); 4] = [
            (
                "a short UUID",
                proto::DiskRecord {
                    uuid: bytes::Bytes::from_static(b"short"),
                    ..write(a, 1, 0, 0xaa)
                },
            ),
            (
                "an ack with a chunk",
                proto::DiskRecord {
                    chunks: vec![chunk::encode_punch(0, 1)],
                    ..ack(a, 9)
                },
            ),
            (
                "a fence with a chunk",
                proto::DiskRecord {
                    chunks: vec![chunk::encode_punch(0, 1)],
                    ..test_support::fence(a)
                },
            ),
            (
                "a zero-clock write",
                proto::DiskRecord {
                    uuid: zero_clock,
                    ..write(a, 1, 0, 0xaa)
                },
            ),
        ];

        // Each case is the first record of its own replay, so no case is rejected for
        // the sequencing state another one left behind.
        let mut out = String::new();
        for (what, record) in cases {
            let err = replay(&dir, 0)
                .record(&record, &test_support::frame([&record]))
                .unwrap_err();

            writeln!(out, "{what:<22}{err:#}").unwrap();
        }
        insta::assert_snapshot!(out, @"
        a short UUID          record carries no message UUID: invalid length: expected 16 bytes, found 5
        an ack with a chunk   an acknowledgement carries 1 chunks, which change disk content it does not commit
        a fence with a chunk  a fence carries 1 chunks, which change disk content it does not commit
        a zero-clock write    a record of Producer(61:00:00:00:00:61) at a zero clock, though only a fence's clock is zero
        ");
    }

    /// An acknowledgement cannot order two deltas whose records interleaved, so it
    /// is rejected.
    #[test]
    fn test_an_interleaved_acknowledgement_is_an_ordering_error() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = (test_support::producer('a'), test_support::producer('b'));

        let trace = play(
            &mut replay(&dir, 0),
            &[
                write(a, 1, 2, 0xaa),
                write(b, 2, 3, 0xbb),
                ack(b, 3),
                ack(a, 4),
            ],
        );
        insta::assert_snapshot!(trace, @"
        at 0      write a@1 2=aa   blocks -          held 4130  horizon -
        at 4130   write b@2 3=bb   blocks -          held 4130  horizon -
        at 8260   ack b@3          blocks 3=bb       held -     horizon -
        at 8286   ack a@4          acknowledgement of a delta of Producer(61:00:00:00:00:61) which another producer's records interleaved
        stands at 8286
        ");
    }

    /// A displaced delta has lost its earlier records. Drop any resumed fragment
    /// and reject both recovered and journaled acknowledgements.
    ///
    /// This requires writes past a replacement's fence, possible if etcd loses
    /// the `author` register.
    #[test]
    fn test_a_displaced_delta_which_resumes_is_never_committed() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = (test_support::producer('a'), test_support::producer('b'));

        // `b` displaces and commits past `a`'s delta, and then `a` resumes.
        let displaced = [
            write(a, 1, 2, 0xaa),
            write(b, 2, 3, 0xbb),
            ack(b, 3),
            write(a, 4, 4, 0xcc),
        ];
        let mut resumed = replay(&dir, 0);
        let mut first = play(&mut resumed, &displaced);
        writeln!(
            first,
            "recovered ack a@5  {:?}",
            resumed.sequencer().recovered_ack(a, test_support::clock(5)),
        )
        .unwrap();

        let mut acknowledged = displaced.to_vec();
        acknowledged.push(ack(a, 5));

        // A resumed fragment interleaves the delta held behind it too, exactly as any
        // other producer's record does, so that delta cannot be committed either.
        let interleaving = [
            write(a, 1, 2, 0xaa),
            write(b, 2, 3, 0xbb),
            write(a, 3, 4, 0xcc),
            ack(b, 4),
        ];

        let trace = [
            first,
            play(&mut replay(&dir, 0), &acknowledged),
            play(&mut replay(&dir, 0), &interleaving),
        ]
        .join("\n");
        insta::assert_snapshot!(trace, @"
        at 0      write a@1 2=aa   blocks -          held 4130  horizon -
        at 4130   write b@2 3=bb   blocks -          held 4130  horizon -
        at 8260   ack b@3          blocks 3=bb       held -     horizon -
        at 8286   write a@4 4=cc   blocks 3=bb       held -     horizon -
        stands at 12416
        recovered ack a@5  Refused

        at 0      write a@1 2=aa   blocks -          held 4130  horizon -
        at 4130   write b@2 3=bb   blocks -          held 4130  horizon -
        at 8260   ack b@3          blocks 3=bb       held -     horizon -
        at 8286   write a@4 4=cc   blocks 3=bb       held -     horizon -
        at 12416  ack a@5          acknowledgement of a delta of Producer(61:00:00:00:00:61) which another producer's records interleaved
        stands at 12416

        at 0      write a@1 2=aa   blocks -          held 4130  horizon -
        at 4130   write b@2 3=bb   blocks -          held 4130  horizon -
        at 8260   write a@3 4=cc   blocks -          held -     horizon -
        at 12390  ack b@4          acknowledgement of a delta of Producer(63:00:00:00:00:62) which another producer's records interleaved
        stands at 12390
        ");
    }

    #[test]
    fn test_an_acknowledgement_which_rolls_back_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let a = test_support::producer('a');

        let trace = play(
            &mut replay(&dir, 0),
            &[
                write(a, 5, 2, 0xaa),
                ack(a, 6),
                write(a, 7, 3, 0xbb),
                ack(a, 6),
            ],
        );
        insta::assert_snapshot!(trace, @"
        at 0      write a@5 2=aa   blocks -          held 4130  horizon -
        at 4130   ack a@6          blocks 2=aa       held -     horizon -
        at 4156   write a@7 3=bb   blocks 2=aa       held 4130  horizon -
        at 8286   ack a@6          acknowledgement of Producer(61:00:00:00:00:61) at Clock(0s 6000ns) rolls back records this replay sequenced
        stands at 8286
        ");
    }

    /// Hand each `(offset, content)` to `replay` as the responses of one read, and
    /// render the offset the replay stands at after each, or the failure which ends
    /// the read, then what the read holds of a record it ended within.
    fn trace(replay: &mut Replay, responses: &[(i64, &[u8])]) -> String {
        let mut buf = bytes::BytesMut::new();
        let mut out = String::new();

        for &(offset, content) in responses {
            match replay.take(&mut buf, offset, content) {
                Ok(()) => writeln!(out, "at {offset:<5}stands at {}", replay.offset).unwrap(),
                Err(err) => {
                    writeln!(out, "at {offset:<5}{err:#}").unwrap();
                    break;
                }
            }
        }
        writeln!(out, "holds {} bytes", buf.len()).unwrap();
        out
    }

    /// Records straddle the responses which carry them, header and body alike, and
    /// each is taken whole once its last byte arrives.
    #[test]
    fn test_records_are_taken_across_responses() {
        let dir = tempfile::tempdir().unwrap();
        let a = test_support::producer('a');

        let records = [punch(a, 1, 3), punch(a, 2, 4), ack(a, 3)];
        let journal = test_support::frame(&records).to_vec();
        let two = offset_of(&records, 2);

        // Cut within the first header, the second body, and the third header.
        let cuts = [0, 3, two - 4, two + 5, journal.len() as i64];
        let responses: Vec<(i64, &[u8])> = cuts
            .windows(2)
            .map(|w| (w[0], &journal[w[0] as usize..w[1] as usize]))
            .collect();

        insta::assert_snapshot!(trace(&mut replay(&dir, 0), &responses), @"
        at 0    stands at 0
        at 3    stands at 32
        at 60   stands at 64
        at 69   stands at 90
        holds 0 bytes
        ");
    }

    /// A replay's first read may land past the floor it asked for, because what the
    /// store no longer holds is below the real floor. Once the replay has begun
    /// taking content, a skip is a gap: within a record, between records, or at the
    /// start of a read which continues the replay.
    #[test]
    fn test_only_a_replay_s_start_may_skip() {
        let dir = tempfile::tempdir().unwrap();
        let a = test_support::producer('a');
        let (one, two) = (
            test_support::frame([&punch(a, 1, 3)]),
            test_support::frame([&punch(a, 2, 4)]),
        );
        let after = 100 + one.len() as i64;

        let mut continued = replay(&dir, 40);
        let trace = [
            trace(
                &mut replay(&dir, 40),
                &[(100, &one), (after, &two[..10]), (500, &two)],
            ),
            trace(&mut replay(&dir, 40), &[(100, &one[..10]), (500, &two)]),
            trace(&mut continued, &[(100, &one)]) + &trace(&mut continued, &[(500, &two)]),
        ]
        .join("\n");
        insta::assert_snapshot!(trace, @"
        at 100  stands at 132
        at 132  stands at 132
        at 500  content this replay needed was deleted from the store, from offset 142 to 500
        holds 10 bytes

        at 100  stands at 100
        at 500  content this replay needed was deleted from the store, from offset 110 to 500
        holds 10 bytes

        at 100  stands at 132
        holds 0 bytes
        at 500  content this replay needed was deleted from the store, from offset 132 to 500
        holds 0 bytes
        ");
    }

    /// Content which frames no record is refused rather than skipped over.
    #[test]
    fn test_unframed_content_is_refused() {
        let dir = tempfile::tempdir().unwrap();

        let trace = trace(
            &mut replay(&dir, 0),
            &[(0, b"these bytes frame no record at all")],
        );
        insta::assert_snapshot!(trace, @"
        at 0    31 unframed bytes at offset 0, which a replay cannot skip
        holds 3 bytes
        ");
    }
}
