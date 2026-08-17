//! Rebuilding a disk from the acknowledged deltas of its journal.
//!
//! These rules are the durability guarantee, so they are stated here in full:
//!
//! - The range is fixed at `[floor, head)` before the pass begins. `head` is
//!   broker-confirmed, which makes the read fresh. A broker which served that
//!   append holds an index covering every fragment below it.
//! - [`uuid::sequence`] sequences records per producer. It drops the duplicates
//!   at-least-once appends produce, and it releases only acknowledged deltas.
//!   Chunks apply in physical journal order, and the live append barrier makes
//!   that commit order.
//! - Fence records change no disk content. They are validated and skipped.
//! - The range may begin within a delta. Records below the floor are unnecessary,
//!   because a completed horizon puts a copy of every allocated block at or after
//!   it.
//! - Horizons are rebuilt by the same rules the writer applies. The record which
//!   opens one snapshots the blocks allocated before its own chunks apply. Every
//!   chunk from there on discharges the blocks it covers. The acknowledgement of
//!   the delta which discharged the last block puts the floor at the opening
//!   record. A later horizon replaces an earlier one. A horizon still open at the
//!   end of the range is one the next session resumes.
//!
//! Nothing is buffered to hold an uncommitted delta. Every delta is applied as
//! it is read, and a delta which is never acknowledged is discovered only at the
//! end of the pass. The image is then discarded and the range read again, this
//! time reading over that delta's records. That costs one extra read of the
//! range in exactly the case where a session did not shut down cleanly, and
//! nothing at all otherwise.

use crate::horizon::Position;
use crate::image::Image;
use crate::proto;
use anyhow::Context;
use gazette::journal::framing;
use proto_gazette::{broker, uuid};

/// What one replay rebuilt, beyond the image itself.
pub struct Replayed {
    /// Chunks applied. Zero means the journal holds no committed state at all. That
    /// disk has never published, or its first use failed.
    pub applied: usize,
    /// Offset at which a replay of this journal may begin from now on. It is the
    /// last completed horizon, or where this replay began if there was none.
    pub floor: i64,
    /// Clock of a floor this pass derived. A horizon completed within the range, so
    /// the caller reports this clock to its client as the disk's floor.
    pub derived: Option<uuid::Clock>,
    /// A horizon the range left open. This session resumes it rather than restarts
    /// it. Its bitmap belongs to the image.
    pub horizon: Option<Position>,
}

/// Rebuild `image` from the committed deltas of `journal` between
/// `begin_mod_time` and `head`.
pub async fn replay(
    client: &gazette::journal::Client,
    journal: &str,
    begin_mod_time: i64,
    head: i64,
    image: &mut Image,
) -> anyhow::Result<Replayed> {
    let mut pass = Pass::default();

    for _ in 0..2 {
        let applied = read(client, journal, begin_mod_time, head, image, &mut pass).await?;

        if !pass.restart() {
            return Ok(Replayed {
                applied,
                floor: pass.floor.map_or(pass.begin, |floor| floor.offset),
                derived: pass.floor.map(|floor| floor.clock),
                horizon: pass.horizon,
            });
        }
        tracing::info!(journal, "discarding a replayed image to read it again");

        () = image.reset().context("discarding a replayed image")?;
    }
    // The first pass finds every unacknowledged delta of the range. Reading over
    // one removes only its own records, so the second pass finds none.
    unreachable!("a replay discovered an unacknowledged delta it had already read over")
}

/// Read `[begin_mod_time, head)` of `journal` into `image`, and report the
/// chunks applied.
async fn read(
    client: &gazette::journal::Client,
    journal: &str,
    begin_mod_time: i64,
    head: i64,
    image: &mut Image,
    pass: &mut Pass,
) -> anyhow::Result<usize> {
    let stream = client.clone().read(broker::ReadRequest {
        journal: journal.to_string(),
        offset: 0,
        end_offset: head,
        block: false,
        begin_mod_time,
        ..Default::default()
    });
    futures::pin_mut!(stream);

    // Journal offset at which `buf` begins. A record this reader has not finished
    // decoding starts there.
    let mut buf = bytes::BytesMut::new();
    let mut offset = 0;
    let mut applied = 0;
    // Offset the broker served first, which is where the seek landed.
    let mut begin = None;

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

        // The broker skipped some content, either for the seek this read began
        // with or for a hole in the offset space. No partial record can be
        // finished across that gap.
        if response.offset != offset + buf.len() as i64 {
            tracing::debug!(journal, from = offset, to = response.offset, "offset jump");

            buf.clear();
            offset = response.offset;
        }
        _ = begin.get_or_insert(response.offset);
        buf.extend_from_slice(&response.content);

        loop {
            match framing::decode::<proto::DiskRecord>(&buf)
                .with_context(|| format!("decoding a record of {journal} at offset {offset}"))?
            {
                framing::Frame::Record { message, consumed } => {
                    applied += pass
                        .record(&message, offset, image)
                        .with_context(|| format!("replaying {journal} at offset {offset}"))?;

                    offset += consumed as i64;
                    _ = buf.split_to(consumed);
                }
                framing::Frame::Desync { skipped } => {
                    tracing::warn!(journal, offset, skipped, "skipped unframed journal content");

                    offset += skipped as i64;
                    _ = buf.split_to(skipped);
                }
                framing::Frame::Incomplete => break,
            }
        }
    }

    if !buf.is_empty() {
        tracing::warn!(
            journal,
            offset,
            bytes = buf.len(),
            "the replayed range ends within a record",
        );
    }
    pass.begin = begin.unwrap_or(head);

    Ok(applied)
}

/// One forward pass over the range.
#[derive(Default)]
struct Pass {
    /// Clocks of the deltas an earlier pass proved uncommitted, keyed by the
    /// producer which wrote them. This pass reads over their records rather than
    /// sequencing them, so it sees the journal as if they were never appended.
    abandoned: std::collections::HashMap<uuid::Producer, Abandoned>,
    /// Sequencing state of each producer of the range.
    producers: std::collections::HashMap<uuid::Producer, Sequence>,
    /// Producer whose delta is being applied.
    open: Option<uuid::Producer>,
    /// Offset the range began at.
    begin: i64,
    /// Horizon which has opened and is not yet discharged.
    horizon: Option<Position>,
    /// Last horizon a delta of the range completed. That position is the floor.
    floor: Option<Position>,
}

/// Clocks of a producer's records which belong to a delta that was never
/// acknowledged.
///
/// One range describes them all. A producer's unacknowledged delta is always its
/// last, because a later record of that producer extends the same delta rather than
/// beginning another.
struct Abandoned {
    /// Clock the producer last committed at before the delta began.
    after: uuid::Clock,
    /// Highest clock the delta reached.
    through: uuid::Clock,
}

#[derive(Default)]
struct Sequence {
    /// Clocks which [`uuid::sequence`] transitions.
    last_commit: uuid::Clock,
    max_continue: uuid::Clock,
    /// Clock this producer's open delta began after.
    began_after: uuid::Clock,
}

impl Pass {
    /// Apply `record`, which begins at `offset`, and report the chunks it
    /// applied.
    fn record(
        &mut self,
        record: &proto::DiskRecord,
        offset: i64,
        image: &mut Image,
    ) -> anyhow::Result<usize> {
        let uuid =
            uuid::Uuid::from_slice(&record.uuid).context("record carries no message UUID")?;
        let (producer, clock, flags) = uuid::parse(uuid)?;

        if let Some(abandoned) = self.abandoned.get(&producer)
            && clock > abandoned.after
            && clock <= abandoned.through
        {
            return Ok(0);
        }
        let state = self.producers.entry(producer).or_default();
        let began_after = state.last_commit;

        let outcome = uuid::sequence(
            flags,
            clock,
            &mut state.last_commit,
            &mut state.max_continue,
        )?;

        anyhow::ensure!(
            !record.opens_horizon
                || matches!(
                    outcome,
                    uuid::SequenceOutcome::ContinueBeginSpan
                        | uuid::SequenceOutcome::ContinueDuplicate
                ),
            "record of {producer:?} at {clock:?} opens a horizon but does not begin a delta",
        );

        match outcome {
            // A fence carries the epoch it installs and changes no disk content.
            uuid::SequenceOutcome::OutsideCommit | uuid::SequenceOutcome::OutsideDuplicate => {
                anyhow::ensure!(
                    record.installs_epoch.len() == std::mem::size_of::<uuid::Producer>(),
                    "fence record of {producer:?} installs {} bytes of epoch",
                    record.installs_epoch.len(),
                );
                () = ensure_no_chunks(record, "a fence")?;
            }
            uuid::SequenceOutcome::ContinueBeginSpan => {
                state.began_after = began_after;
                self.open = Some(producer);

                if record.opens_horizon {
                    let pending = image.open_horizon();
                    self.horizon = Some(Position { offset, clock });

                    tracing::debug!(?producer, offset, pending, "replay opened a horizon");
                }
                return apply(record, image);
            }
            uuid::SequenceOutcome::ContinueExtendSpan => {
                self.open = Some(producer);

                return apply(record, image);
            }
            uuid::SequenceOutcome::ContinueDuplicate => (),

            // Another producer's records interleaved this delta, so its
            // acknowledgement cannot be honored. Its chunks are already applied,
            // in an order which does not match the order the two deltas committed.
            uuid::SequenceOutcome::AckCommit => {
                anyhow::ensure!(
                    self.open == Some(producer),
                    "acknowledgement of a delta of {producer:?} which {:?} interleaved",
                    self.open,
                );
                self.open = None;
                () = ensure_no_chunks(record, "an acknowledgement")?;

                // A committed delta which discharged the last block of the
                // horizon puts a copy of every allocated block at or after it,
                // making it the floor.
                if self.horizon.is_some() && image.horizon_pending() == 0 {
                    self.floor = self.horizon.take();
                    image.close_horizon();

                    tracing::debug!(?producer, floor = ?self.floor, "replay completed a horizon");
                }
            }
            // A delta whose records are all below the floor, or an
            // acknowledgement which was appended twice.
            uuid::SequenceOutcome::AckEmpty | uuid::SequenceOutcome::AckDuplicate => {
                () = ensure_no_chunks(record, "an acknowledgement")?;
            }

            // A rollback would have to undo chunks this pass already applied. The
            // append barrier makes one impossible from this daemon, and this
            // daemon is the only writer a disk journal has.
            uuid::SequenceOutcome::AckCleanRollback | uuid::SequenceOutcome::AckDeepRollback => {
                anyhow::bail!(
                    "acknowledgement of {producer:?} at {clock:?} rolls back records which were applied",
                )
            }
        }
        Ok(0)
    }

    /// Ready this pass to run over the same range again. The next pass reads over
    /// every delta which reached the end of the range unacknowledged.
    ///
    /// False when there were none. The image of that pass is then the disk.
    fn restart(&mut self) -> bool {
        let mut again = false;

        for (producer, state) in self.producers.drain() {
            if state.max_continue == uuid::Clock::zero() {
                continue;
            }
            tracing::info!(
                ?producer,
                after = ?state.began_after,
                through = ?state.max_continue,
                "a delta reached the end of the recovery range unacknowledged",
            );
            self.abandoned.insert(
                producer,
                Abandoned {
                    after: state.began_after,
                    through: state.max_continue,
                },
            );
            again = true;
        }
        if again {
            // The pass discards the image, so the horizon rebuilt against it goes
            // too.
            self.open = None;
            self.horizon = None;
            self.floor = None;
        }
        again
    }
}

fn apply(record: &proto::DiskRecord, image: &mut Image) -> anyhow::Result<usize> {
    for chunk in &record.chunks {
        () = image
            .apply(chunk)
            .with_context(|| format!("applying chunk at block {}", chunk.block))?;
    }
    Ok(record.chunks.len())
}

fn ensure_no_chunks(record: &proto::DiskRecord, what: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        record.chunks.is_empty(),
        "{what} carries {} chunks, which change disk content it does not commit",
        record.chunks.len(),
    );
    Ok(())
}

#[cfg(test)]
mod test {
    use super::Pass;
    use crate::BLOCK_SIZE;
    use crate::chunk::{encode_punch, encode_write};
    use crate::image::Image;
    use crate::proto;
    use proto_gazette::uuid;

    const BLOCKS: u32 = 64;

    fn producer(seed: u8) -> uuid::Producer {
        uuid::Producer::from_bytes([seed | 0x01, 0, 0, 0, 0, seed])
    }

    /// A clock `ticks` microseconds after the epoch. Each case then reads as a
    /// sequence of small numbers.
    fn clock(ticks: u64) -> uuid::Clock {
        let mut clock = uuid::Clock::UNIX_EPOCH;
        for _ in 0..ticks {
            _ = clock.tick();
        }
        clock
    }

    fn record(
        producer: uuid::Producer,
        ticks: u64,
        flags: uuid::Flags,
        chunks: Vec<proto::Chunk>,
    ) -> proto::DiskRecord {
        proto::DiskRecord {
            uuid: bytes::Bytes::copy_from_slice(
                uuid::build(producer, clock(ticks), flags)
                    .as_bytes()
                    .as_slice(),
            ),
            chunks,
            opens_horizon: false,
            installs_epoch: bytes::Bytes::new(),
        }
    }

    /// `record` as the first of a delta which opens a horizon.
    fn opens(record: proto::DiskRecord) -> proto::DiskRecord {
        proto::DiskRecord {
            opens_horizon: true,
            ..record
        }
    }

    fn write(producer: uuid::Producer, clock: u64, block: u32, fill: u8) -> proto::DiskRecord {
        record(
            producer,
            clock,
            uuid::Flags::CONTINUE_TXN,
            encode_write(block, &bytes::Bytes::from(vec![fill; BLOCK_SIZE as usize])),
        )
    }

    fn ack(producer: uuid::Producer, clock: u64) -> proto::DiskRecord {
        record(producer, clock, uuid::Flags::ACK_TXN, Vec::new())
    }

    fn fence(producer: uuid::Producer, clock: u64, installs: uuid::Producer) -> proto::DiskRecord {
        proto::DiskRecord {
            installs_epoch: bytes::Bytes::copy_from_slice(installs.as_bytes()),
            ..record(producer, clock, uuid::Flags::OUTSIDE_TXN, Vec::new())
        }
    }

    /// Replay `records` as [`super::replay`] does, and restart a pass which found
    /// an unacknowledged delta. Returns the pass alongside each block's fill byte.
    /// Offsets treat each record as one byte long, which is enough to tell them
    /// apart.
    fn replay(
        dir: &tempfile::TempDir,
        records: &[proto::DiskRecord],
    ) -> (Pass, Image, Vec<(u32, u8)>) {
        let mut image = Image::create(dir.path(), BLOCKS).unwrap();
        let mut pass = Pass::default();

        for _ in 0..2 {
            for (offset, record) in records.iter().enumerate() {
                _ = pass.record(record, offset as i64, &mut image).unwrap();
            }
            if !pass.restart() {
                break;
            }
            image.reset().unwrap();
        }

        let mut block = vec![0u8; BLOCK_SIZE as usize];
        let blocks = image
            .allocated()
            .iter()
            .map(|index| {
                image.read_at(index, &mut block).unwrap();
                (index, block[0])
            })
            .collect();

        (pass, image, blocks)
    }

    /// Blocks a replay of `records` leaves allocated, and their fill bytes.
    fn replayed(dir: &tempfile::TempDir, records: &[proto::DiskRecord]) -> Vec<(u32, u8)> {
        replay(dir, records).2
    }

    #[test]
    fn test_only_acknowledged_deltas_are_applied() {
        let dir = tempfile::tempdir().unwrap();
        let (a, f) = (producer(0x10), producer(0x20));

        // One committed delta, then one the session never acknowledged.
        assert_eq!(
            replayed(
                &dir,
                &[
                    fence(f, 1, a),
                    write(a, 2, 3, 0xaa),
                    write(a, 3, 4, 0xbb),
                    ack(a, 4),
                    write(a, 5, 3, 0xcc),
                    write(a, 6, 7, 0xdd),
                ],
            ),
            vec![(3, 0xaa), (4, 0xbb)],
        );
    }

    /// A delta which a replacement session's records follow is abandoned in the
    /// middle of the range, and not only at its end.
    #[test]
    fn test_a_delta_a_replacement_session_abandoned_is_not_applied() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b, f) = (producer(0x10), producer(0x30), producer(0x20));

        assert_eq!(
            replayed(
                &dir,
                &[
                    write(a, 1, 3, 0xaa),
                    ack(a, 2),
                    write(a, 3, 5, 0xcc),
                    fence(f, 4, b),
                    write(b, 5, 6, 0xee),
                    ack(b, 6),
                ],
            ),
            vec![(3, 0xaa), (6, 0xee)],
        );
    }

    /// The range may begin within a delta. That delta's acknowledgement then
    /// commits only the records which were in range.
    #[test]
    fn test_a_delta_which_begins_below_the_range_commits_what_is_in_it() {
        let dir = tempfile::tempdir().unwrap();
        let a = producer(0x10);

        assert_eq!(
            replayed(&dir, &[write(a, 5, 2, 0xaa), ack(a, 9)]),
            vec![(2, 0xaa)],
        );
    }

    /// At-least-once appends repeat records. Sequencing drops a repeat rather than
    /// applying it a second time over a newer value.
    #[test]
    fn test_duplicate_records_are_not_applied_again() {
        let dir = tempfile::tempdir().unwrap();
        let a = producer(0x10);

        assert_eq!(
            replayed(
                &dir,
                &[
                    write(a, 1, 2, 0xaa),
                    write(a, 2, 2, 0xbb),
                    write(a, 1, 2, 0xaa),
                    ack(a, 3),
                    ack(a, 3),
                ],
            ),
            vec![(2, 0xbb)],
        );
    }

    #[test]
    fn test_a_punch_deallocates_what_an_earlier_delta_wrote() {
        let dir = tempfile::tempdir().unwrap();
        let a = producer(0x10);

        assert_eq!(
            replayed(
                &dir,
                &[
                    write(a, 1, 8, 0xaa),
                    write(a, 2, 9, 0xbb),
                    ack(a, 3),
                    record(a, 4, uuid::Flags::CONTINUE_TXN, vec![encode_punch(8, 1)]),
                    ack(a, 5),
                ],
            ),
            vec![(9, 0xbb)],
        );
    }

    /// A record which opens a horizon snapshots the blocks allocated before its own
    /// chunks apply. The acknowledgement of the delta which discharges the last of
    /// those blocks moves the floor to that record.
    #[test]
    fn test_a_discharged_horizon_derives_the_floor() {
        let dir = tempfile::tempdir().unwrap();
        let a = producer(0x10);

        let (pass, image, blocks) = replay(
            &dir,
            &[
                write(a, 1, 3, 0xaa),
                write(a, 2, 4, 0xbb),
                ack(a, 3),
                // This delta opens a horizon over both blocks and rewrites them,
                // which discharges the horizon without any copy.
                opens(write(a, 4, 3, 0xcc)),
                write(a, 5, 4, 0xdd),
                ack(a, 6),
            ],
        );

        assert_eq!(blocks, vec![(3, 0xcc), (4, 0xdd)]);
        assert_eq!(image.horizon_pending(), 0);
        assert!(pass.horizon.is_none());

        let floor = pass.floor.expect("the horizon completed");
        assert_eq!(floor.offset, 3);
        assert_eq!(floor.clock, clock(4));
    }

    /// The next session resumes a horizon the range leaves open. The image holds
    /// what that horizon has left to discharge, and the pass holds where it opened.
    #[test]
    fn test_an_open_horizon_outlives_the_pass() {
        let dir = tempfile::tempdir().unwrap();
        let a = producer(0x10);

        let (pass, image, _blocks) = replay(
            &dir,
            &[
                write(a, 1, 3, 0xaa),
                write(a, 2, 4, 0xbb),
                ack(a, 3),
                opens(write(a, 4, 3, 0xcc)),
                ack(a, 5),
            ],
        );

        assert_eq!(image.horizon_pending(), 1);
        assert!(pass.floor.is_none());
        assert_eq!(pass.horizon.expect("a horizon is open").offset, 3);
    }

    /// A range may hold several horizons. Each one replaces the one before it, so
    /// the floor is the last horizon which a delta discharged.
    #[test]
    fn test_a_later_horizon_replaces_an_earlier_one() {
        let dir = tempfile::tempdir().unwrap();
        let a = producer(0x10);

        let (pass, image, _blocks) = replay(
            &dir,
            &[
                write(a, 1, 3, 0xaa),
                write(a, 2, 4, 0xbb),
                ack(a, 3),
                opens(write(a, 4, 3, 0xcc)),
                ack(a, 5),
                opens(write(a, 6, 3, 0xdd)),
                write(a, 7, 4, 0xee),
                ack(a, 8),
            ],
        );

        assert_eq!(image.horizon_pending(), 0);
        assert_eq!(pass.floor.expect("the second horizon completed").offset, 5);
    }

    /// A horizon belongs to its delta. A horizon whose delta is never acknowledged
    /// never existed, exactly as its chunks never applied.
    #[test]
    fn test_a_horizon_of_an_uncommitted_delta_does_not_exist() {
        let dir = tempfile::tempdir().unwrap();
        let a = producer(0x10);

        let (pass, image, blocks) = replay(
            &dir,
            &[write(a, 1, 3, 0xaa), ack(a, 2), opens(write(a, 3, 4, 0xbb))],
        );

        assert_eq!(blocks, vec![(3, 0xaa)]);
        assert_eq!(image.horizon_pending(), 0);
        assert!(pass.horizon.is_none() && pass.floor.is_none());
    }

    #[test]
    fn test_malformed_records_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut image = Image::create(dir.path(), BLOCKS).unwrap();
        let (a, f) = (producer(0x10), producer(0x20));

        let cases: [(proto::DiskRecord, &str); 5] = [
            (
                proto::DiskRecord {
                    uuid: bytes::Bytes::from_static(b"short"),
                    ..write(a, 1, 0, 0xaa)
                },
                "no message UUID",
            ),
            (
                proto::DiskRecord {
                    opens_horizon: true,
                    ..ack(a, 1)
                },
                "does not begin a delta",
            ),
            (
                proto::DiskRecord {
                    installs_epoch: bytes::Bytes::from_static(b"nope"),
                    ..fence(f, 1, a)
                },
                "4 bytes of epoch",
            ),
            (
                proto::DiskRecord {
                    chunks: vec![encode_punch(0, 1)],
                    ..fence(f, 1, a)
                },
                "a fence carries 1 chunks",
            ),
            (
                proto::DiskRecord {
                    chunks: vec![encode_punch(0, 1)],
                    ..ack(a, 9)
                },
                "an acknowledgement carries 1 chunks",
            ),
        ];

        for (record, expect) in cases {
            let err = Pass::default().record(&record, 0, &mut image).unwrap_err();
            assert!(format!("{err:#}").contains(expect), "{expect}: {err:#}");
        }
    }

    /// An acknowledgement cannot order two deltas whose records interleaved, so it
    /// is rejected.
    #[test]
    fn test_an_interleaved_acknowledgement_is_an_ordering_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut image = Image::create(dir.path(), BLOCKS).unwrap();
        let (a, b) = (producer(0x10), producer(0x30));
        let mut pass = Pass::default();

        for (offset, record) in [write(a, 1, 2, 0xaa), write(b, 2, 3, 0xbb), ack(b, 3)]
            .iter()
            .enumerate()
        {
            _ = pass.record(record, offset as i64, &mut image).unwrap();
        }
        let err = pass.record(&ack(a, 4), 3, &mut image).unwrap_err();

        assert!(format!("{err:#}").contains("interleaved"), "{err:#}");
    }

    #[test]
    fn test_an_acknowledgement_which_rolls_back_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut image = Image::create(dir.path(), BLOCKS).unwrap();
        let a = producer(0x10);
        let mut pass = Pass::default();

        for (offset, record) in [write(a, 5, 2, 0xaa), ack(a, 6), write(a, 7, 3, 0xbb)]
            .iter()
            .enumerate()
        {
            _ = pass.record(record, offset as i64, &mut image).unwrap();
        }
        let err = pass.record(&ack(a, 6), 3, &mut image).unwrap_err();

        assert!(format!("{err:#}").contains("rolls back"), "{err:#}");
    }
}
