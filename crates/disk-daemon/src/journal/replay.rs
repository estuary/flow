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
//! - Another producer's records displace the delta held, and a displaced delta is
//!   never taken up again. Records of it which follow are a fragment whose start
//!   was dropped, so none of them is held, and its acknowledgement is refused.
//! - The range may begin within a delta. Records below the floor are unnecessary,
//!   because a completed horizon puts a copy of every allocated block at or after
//!   it.
//! - Horizons are rebuilt by the same rules the writer applies. The record which
//!   opens one snapshots the blocks allocated before its own chunks apply. Every
//!   chunk from there on discharges the blocks it covers. The acknowledgement of
//!   the delta which discharged the last block puts the floor at the opening
//!   record. A later horizon replaces an earlier one. A horizon still open at the
//!   end of the range is one the next tenure resumes.
//!
//! A delta is not applied as it is read. Its records are held in a [`HeldDelta`] as
//! they are sequenced, and they apply only when the acknowledgement of that delta
//! arrives. A delta's chunks, the horizon it opens, and the blocks that horizon
//! discharges therefore all land together, or never land at all.
//!
//! A pass must hold them, because it may have no end of range at which to discover
//! an unacknowledged delta: a standby tails the journal, where the delta at the head
//! is open because a primary is still writing it. An image which applied that delta
//! would hold writes the client never committed. A delta which is never acknowledged
//! is instead dropped: another producer's records displace the one being held, and
//! one still held when the pass ends goes with the pass. Nothing is ever taken back,
//! because nothing of it was applied. A bounded recovery runs this same pass, so
//! there is one read path and one set of rules for both.
//!
//! The rules are carried out in three parts. [`read`] streams the range, and
//! `reassembly` turns its content into records and finds what the broker skipped.
//! `sequencer` decides what each record does to the delta held. [`Pass`] holds,
//! drops, and applies accordingly, which is where horizons open and discharge.

use super::held::HeldDelta;
use super::reassembly::{Reassembled, Reassembly, Received};
use super::sequencer::{Action, Sequencer, Step};
use crate::horizon::Horizon;
use crate::image::Image;
use crate::proto;
use anyhow::Context;
use proto_gazette::broker;

/// How far a read goes.
#[derive(Clone, Copy, Debug)]
pub enum Extent {
    /// Read to this head and stop, as a recovery does. The head is broker-confirmed,
    /// which is what makes the read fresh.
    Bounded(i64),
    /// Read until the caller stops, as a standby does. New records are followed as
    /// they arrive, so this read does not end on its own.
    Tail,
}

/// Content a read still needed was deleted from the fragment store.
///
/// A bounded recovery may skip a gap: it seeks from the floor, and the broker starts
/// it at the first offset the store still holds. A tail may not. It holds a partial
/// image, and a block which was deallocated below the gap has no record above it to
/// punch, so a skip would leave that block allocated forever. The image must be
/// discarded and rebuilt from the current floor.
#[derive(Debug)]
pub struct Gap {
    pub at: i64,
}

impl std::fmt::Display for Gap {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "content this read needed was deleted from the store, at offset {}",
            self.at,
        )
    }
}

impl std::error::Error for Gap {}

/// Read `journal` from `floor` into `image`, and report the chunks applied.
///
/// The floor is a record boundary, so the read asks for it exactly. A broker whose
/// store no longer holds that offset serves the next one it has, which only adds
/// records this replay does not need.
///
/// A [`Extent::Tail`] read blocks at the head and returns only on an error or a
/// [`Gap`]. Its caller stops it by cancelling it.
pub(super) async fn read(
    client: &gazette::journal::Client,
    journal: &str,
    floor: i64,
    extent: Extent,
    image: &mut Image,
    pass: &mut Pass,
) -> anyhow::Result<usize> {
    let (end_offset, block) = match extent {
        Extent::Bounded(head) => (head, false),
        Extent::Tail => (0, true),
    };

    let stream = client.clone().read(broker::ReadRequest {
        journal: journal.to_string(),
        offset: floor,
        end_offset,
        block,
        ..Default::default()
    });
    futures::pin_mut!(stream);

    let mut reassembly = Reassembly::new(extent);
    let mut applied = 0;

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

        match reassembly.on_response(response.offset, &response.content) {
            Ok(Received::Contiguous) => (),
            Ok(Received::Skipped { from, to }) => tracing::debug!(journal, from, to, "offset jump"),
            Err(gap) => {
                return Err(anyhow::Error::new(gap).context(format!("tailing {journal}")));
            }
        }
        while let Some(Reassembled { at, record, framed }) = reassembly
            .next_record()
            .with_context(|| format!("reading {journal}"))?
        {
            applied += pass
                .record(&record, &framed, at, image)
                .with_context(|| format!("replaying {journal} at offset {at}"))?;
        }
    }

    if let Some((offset, bytes)) = reassembly.remainder() {
        tracing::warn!(
            journal,
            offset,
            bytes,
            "the replayed range ends within a record",
        );
    }
    Ok(applied)
}

/// One forward pass over the range.
///
/// Its [`Sequencer`] decides what each record does to the delta held. The pass
/// carries that out: it holds records in its held delta, drops a delta which was
/// displaced, and at a commit applies the held delta to the image, which is where a
/// horizon opens and discharges and where the floor is derived.
pub(super) struct Pass {
    sequencer: Sequencer,
    /// The delta which is not yet acknowledged, held rather than applied.
    held: HeldDelta,
    /// A horizon which has opened and is not yet discharged.
    horizon: Option<OpenHorizon>,
    /// Offset of the last horizon a delta of the range completed, which is the
    /// floor.
    floor: Option<i64>,
    /// Chunks this pass has applied. It counts across reads, and it survives a read
    /// which is cancelled, so a playback which is promoted mid-backfill still knows
    /// whether the journal held committed state.
    applied_chunks: usize,
}

/// A recovery horizon this pass has opened.
///
/// The offset and the bitmap are held together because they are one fact: `at` is
/// the floor which completing the horizon establishes, and `blocks` is what the
/// horizon still owes before it may. A live disk keeps the pair apart, per
/// [`super::Recovered::horizon`].
pub(super) struct OpenHorizon {
    /// Offset at which the record which opened this horizon begins.
    pub(super) at: i64,
    /// Committed blocks which still owe this horizon a copy.
    pub(super) blocks: Horizon,
}

impl Pass {
    /// A pass which holds each delta until its acknowledgement, in `held`.
    pub(super) fn new(held: HeldDelta) -> Self {
        Self {
            sequencer: Sequencer::default(),
            held,
            horizon: None,
            floor: None,
            applied_chunks: 0,
        }
    }

    /// Chunks applied, which is zero for a journal with no committed state.
    pub(super) fn applied_chunks(&self) -> usize {
        self.applied_chunks
    }

    /// Recovery floor this pass derived, if a horizon completed within it.
    pub(super) fn derived_floor(&self) -> Option<i64> {
        self.floor
    }

    /// Offset through which committed state is applied. A held delta is not part of
    /// it, so this never runs ahead of what the client committed.
    pub(super) fn applied_offset(&self) -> i64 {
        self.sequencer.applied()
    }

    /// The pass's sequencing, which says which acknowledgements it can honor.
    pub(super) fn sequencer(&self) -> &Sequencer {
        &self.sequencer
    }

    /// Take the held delta, the floor, and any horizon still open, to continue
    /// this pass elsewhere.
    pub(super) fn into_parts(self) -> (HeldDelta, Option<i64>, Option<OpenHorizon>) {
        (self.held, self.floor, self.horizon)
    }

    /// Sequence `record`, which begins at `offset` and was framed as `framed`, carry
    /// out what it does to the delta held, and report the chunks it applied.
    fn record(
        &mut self,
        record: &proto::DiskRecord,
        framed: &[u8],
        offset: i64,
        image: &mut Image,
    ) -> anyhow::Result<usize> {
        let Step { displaced, action } = self.sequencer.on_record(record, offset, framed.len())?;

        if let Some(held) = displaced {
            tracing::debug!(
                ?held,
                bytes = self.held.len(),
                "dropping a held delta which another producer's records displaced",
            );
            () = self.held.clear()?;
        }

        match action {
            Action::Skip => Ok(0),
            Action::Hold => {
                () = self.held.push(framed)?;
                Ok(0)
            }
            Action::Commit { opens_at } => {
                // The delta is committed, so the records held for it apply now. This
                // must precede the horizon check below, because the chunks which
                // discharge that horizon are among the ones applied here.
                let applied = self.apply_held(image, opens_at)?;
                self.applied_chunks += applied;

                // A committed delta which discharged the last block of the horizon
                // puts a copy of every allocated block at or after it, making it the
                // floor.
                if self
                    .horizon
                    .as_ref()
                    .is_some_and(|open| open.blocks.pending() == 0)
                {
                    self.floor = self.horizon.take().map(|open| open.at);

                    tracing::debug!(floor = ?self.floor, "replay completed a horizon");
                }
                Ok(applied)
            }
        }
    }

    /// Apply the held delta to `image`, which its acknowledgement has committed, and
    /// report the chunks it applied. `opens_at` is the offset at which the delta's
    /// first record opened a horizon, if it opened one.
    ///
    /// The records apply in the order they were held. The record which opens a
    /// horizon therefore snapshots the blocks allocated before this delta, and the
    /// chunks which discharge that horizon apply after it. A delta's effects are
    /// whole: they all land here, or none of them ever land.
    fn apply_held(&mut self, image: &mut Image, opens_at: Option<i64>) -> anyhow::Result<usize> {
        let Self { held, horizon, .. } = self;
        let mut applied = 0;

        () = held.drain(|record| {
            if record.opens_horizon {
                let at = opens_at.expect("the sequencer held the offset of an opening record");
                let blocks = Horizon::open(image.allocated());

                tracing::debug!(
                    pending = blocks.pending(),
                    at,
                    "a held delta opened a recovery horizon",
                );
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

#[cfg(test)]
mod test {
    use super::Pass;
    use crate::BLOCK_SIZE;
    use crate::chunk::{encode_punch, encode_write};
    use crate::image::Image;
    use crate::proto;
    use crate::test_support;
    use proto_gazette::{fixed_framing, uuid};

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

    /// `record` framed exactly as the journal frames it. A pass keeps the journal's
    /// own bytes of a held record and decodes them again when its delta commits, so
    /// every case frames for real rather than standing in for these bytes.
    fn frame(record: &proto::DiskRecord) -> bytes::BytesMut {
        let mut framed = bytes::BytesMut::new();
        fixed_framing::encode(record, &mut framed);

        framed
    }

    /// Journal offset at which the record at `index` begins.
    fn offset_of(records: &[proto::DiskRecord], index: usize) -> i64 {
        records[..index]
            .iter()
            .map(|record| frame(record).len() as i64)
            .sum()
    }

    /// Replay `records` through a pass, which holds the delta in doubt until its
    /// acknowledgement arrives. Returns the pass alongside each block's fill byte.
    fn replay(
        dir: &tempfile::TempDir,
        records: &[proto::DiskRecord],
    ) -> (Pass, Image, Vec<(u32, u8)>) {
        let mut image = Image::create(dir.path(), BLOCKS).unwrap();
        let mut pass = Pass::new(super::HeldDelta::create(dir.path()).unwrap());
        let mut offset = 0;

        for record in records {
            let framed = frame(record);

            _ = pass.record(record, &framed, offset, &mut image).unwrap();
            offset += framed.len() as i64;
        }
        let blocks = test_support::allocated(&image);

        (pass, image, blocks)
    }

    /// Blocks a replay of `records` leaves allocated, and their fill bytes.
    fn replayed(dir: &tempfile::TempDir, records: &[proto::DiskRecord]) -> Vec<(u32, u8)> {
        replay(dir, records).2
    }

    /// A delta reaches the image only at its acknowledgement, and all of it lands
    /// there at once.
    #[test]
    fn test_a_delta_applies_at_its_acknowledgement() {
        let dir = tempfile::tempdir().unwrap();
        let (a, f) = (producer(0x10), producer(0x20));

        let (pass, _image, blocks) = replay(
            &dir,
            &[
                fence(f, 1, a),
                write(a, 2, 3, 0xaa),
                write(a, 3, 4, 0xbb),
                ack(a, 4),
            ],
        );
        assert_eq!(blocks, vec![(3, 0xaa), (4, 0xbb)]);

        let (held, _floor, _horizon) = pass.into_parts();
        assert!(held.is_empty(), "the acknowledged delta was released");
    }

    /// The delta which is still in doubt stays held, and nothing of it reaches the
    /// image. That is what keeps a standby's image at its client's committed edge.
    #[test]
    fn test_a_delta_still_in_doubt_is_held_and_not_applied() {
        let dir = tempfile::tempdir().unwrap();
        let a = producer(0x10);

        let (pass, _image, blocks) = replay(
            &dir,
            &[
                write(a, 1, 3, 0xaa),
                ack(a, 2),
                // A delta the tenure never acknowledged.
                write(a, 3, 4, 0xbb),
                write(a, 4, 5, 0xcc),
            ],
        );
        assert_eq!(blocks, vec![(3, 0xaa)]);

        let (held, _floor, _horizon) = pass.into_parts();
        assert!(!held.is_empty(), "the delta in doubt is still held");
    }

    /// A delta which a replacement tenure's records follow is abandoned. Those
    /// records displace it, so it is dropped rather than carried for the rest of the
    /// pass, and nothing of it is ever applied.
    #[test]
    fn test_a_delta_a_replacement_tenure_abandoned_is_not_applied() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b, f) = (producer(0x10), producer(0x30), producer(0x20));

        let (pass, _image, blocks) = replay(
            &dir,
            &[
                write(a, 1, 3, 0xaa),
                ack(a, 2),
                write(a, 3, 5, 0xcc),
                fence(f, 4, b),
                write(b, 5, 6, 0xee),
                ack(b, 6),
            ],
        );
        assert_eq!(blocks, vec![(3, 0xaa), (6, 0xee)]);

        let (held, _floor, _horizon) = pass.into_parts();
        assert!(held.is_empty(), "the displaced delta was dropped");
    }

    /// A promotion repairs the acknowledgement its client held before it appends
    /// anything of its own, so the delta that fence displaced is still applied.
    #[test]
    fn test_a_delta_a_promotion_repaired_is_applied() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b, f) = (producer(0x10), producer(0x30), producer(0x20));

        assert_eq!(
            replayed(
                &dir,
                &[
                    write(a, 1, 3, 0xaa),
                    fence(f, 2, b),
                    ack(a, 3),
                    write(b, 4, 5, 0xcc),
                    ack(b, 5),
                ],
            ),
            vec![(3, 0xaa), (5, 0xcc)],
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
    /// those blocks moves the floor to that record. The horizon is held with its
    /// delta until then, exactly as that delta's chunks are.
    #[test]
    fn test_a_discharged_horizon_derives_the_floor() {
        let dir = tempfile::tempdir().unwrap();
        let a = producer(0x10);

        let records = [
            write(a, 1, 3, 0xaa),
            write(a, 2, 4, 0xbb),
            ack(a, 3),
            // This delta opens a horizon over both blocks and rewrites them, which
            // discharges the horizon without any copy.
            opens(write(a, 4, 3, 0xcc)),
            write(a, 5, 4, 0xdd),
            ack(a, 6),
        ];
        let (pass, _image, blocks) = replay(&dir, &records);

        assert_eq!(blocks, vec![(3, 0xcc), (4, 0xdd)]);

        let (_held, floor, horizon) = pass.into_parts();

        assert!(horizon.is_none(), "the horizon completed");
        assert_eq!(
            floor.expect("a floor was derived"),
            offset_of(&records, 3),
            "the floor is the offset of the record which opened the horizon",
        );
    }

    /// The next tenure resumes a horizon the range leaves open. The pass holds both
    /// halves of it: where it opened, and what it has left to discharge.
    #[test]
    fn test_an_open_horizon_outlives_the_pass() {
        let dir = tempfile::tempdir().unwrap();
        let a = producer(0x10);

        let records = [
            write(a, 1, 3, 0xaa),
            write(a, 2, 4, 0xbb),
            ack(a, 3),
            opens(write(a, 4, 3, 0xcc)),
            ack(a, 5),
        ];
        let (pass, _image, _blocks) = replay(&dir, &records);
        let (_held, floor, horizon) = pass.into_parts();
        let horizon = horizon.expect("a horizon is open");

        assert!(floor.is_none());
        assert_eq!(horizon.at, offset_of(&records, 3));
        assert_eq!(
            horizon.blocks.pending(),
            1,
            "the block it still owes a copy"
        );
    }

    /// A range may hold several horizons. Each one replaces the one before it, so
    /// the floor is the last horizon which a delta discharged.
    #[test]
    fn test_a_later_horizon_replaces_an_earlier_one() {
        let dir = tempfile::tempdir().unwrap();
        let a = producer(0x10);

        let records = [
            write(a, 1, 3, 0xaa),
            write(a, 2, 4, 0xbb),
            ack(a, 3),
            opens(write(a, 4, 3, 0xcc)),
            ack(a, 5),
            opens(write(a, 6, 3, 0xdd)),
            write(a, 7, 4, 0xee),
            ack(a, 8),
        ];
        let (pass, _image, _blocks) = replay(&dir, &records);
        let (_held, floor, horizon) = pass.into_parts();

        assert!(horizon.is_none(), "the second horizon completed");
        assert_eq!(
            floor.expect("the second horizon completed"),
            offset_of(&records, 5),
        );
    }

    /// A horizon belongs to its delta. A horizon whose delta is never acknowledged
    /// never existed, exactly as its chunks never applied.
    #[test]
    fn test_a_horizon_of_an_uncommitted_delta_does_not_exist() {
        let dir = tempfile::tempdir().unwrap();
        let a = producer(0x10);

        let (pass, _image, blocks) = replay(
            &dir,
            &[write(a, 1, 3, 0xaa), ack(a, 2), opens(write(a, 3, 4, 0xbb))],
        );

        assert_eq!(blocks, vec![(3, 0xaa)]);

        let (_held, floor, horizon) = pass.into_parts();
        assert!(horizon.is_none() && floor.is_none());
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

        // Each case is the first record of its own pass, so no case is rejected for
        // the sequencing state another one left behind.
        for (record, expect) in cases {
            let err = Pass::new(super::HeldDelta::create(dir.path()).unwrap())
                .record(&record, &frame(&record), 0, &mut image)
                .unwrap_err();

            assert!(format!("{err:#}").contains(expect), "{expect}: {err:#}");
        }
    }

    /// Sequence `records` through one pass, and report the failure of the last of
    /// them. Every record before it must be accepted.
    fn refused(dir: &tempfile::TempDir, records: &[proto::DiskRecord]) -> String {
        let mut image = Image::create(dir.path(), BLOCKS).unwrap();
        let mut pass = Pass::new(super::HeldDelta::create(dir.path()).unwrap());

        let (last, accepted) = records.split_last().expect("a case has records");

        for (index, record) in accepted.iter().enumerate() {
            _ = pass
                .record(
                    record,
                    &frame(record),
                    offset_of(records, index),
                    &mut image,
                )
                .unwrap();
        }
        let err = pass
            .record(
                last,
                &frame(last),
                offset_of(records, accepted.len()),
                &mut image,
            )
            .unwrap_err();

        format!("{err:#}")
    }

    /// An acknowledgement cannot order two deltas whose records interleaved, so it
    /// is rejected.
    #[test]
    fn test_an_interleaved_acknowledgement_is_an_ordering_error() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = (producer(0x10), producer(0x30));

        let err = refused(
            &dir,
            &[
                write(a, 1, 2, 0xaa),
                write(b, 2, 3, 0xbb),
                ack(b, 3),
                ack(a, 4),
            ],
        );
        assert!(err.contains("interleaved"), "{err}");
    }

    /// A delta which another producer's records displaced is not taken up again when
    /// its own records resume. Its earlier records were dropped, so what follows is a
    /// fragment of it: nothing of that fragment is held, and the acknowledgement which
    /// follows is refused as an interleaved one rather than committing the fragment.
    ///
    /// Only a writer which appended past a replacement's fence produces this, which the
    /// `author` register prevents unless etcd lost it.
    #[test]
    fn test_a_displaced_delta_which_resumes_is_never_committed() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = (producer(0x10), producer(0x30));

        // `b` displaces and commits past `a`'s delta, and then `a` resumes.
        let displaced = [
            write(a, 1, 2, 0xaa),
            write(b, 2, 3, 0xbb),
            ack(b, 3),
            write(a, 4, 4, 0xcc),
        ];

        let (pass, _image, blocks) = replay(&dir, &displaced);
        assert_eq!(blocks, vec![(3, 0xbb)], "only the committed delta applied");
        assert!(
            !pass.sequencer().can_acknowledge(a, clock(5)),
            "a recovered acknowledgement of the fragment would be appended",
        );
        let (held, _floor, _horizon) = pass.into_parts();
        assert!(held.is_empty(), "the fragment was held");

        let mut acknowledged = displaced.to_vec();
        acknowledged.push(ack(a, 5));

        let err = refused(&dir, &acknowledged);
        assert!(err.contains("interleaved"), "{err}");

        // A resumed fragment interleaves the delta held behind it too, exactly as any
        // other producer's record does, so that delta cannot be committed either.
        let err = refused(
            &dir,
            &[
                write(a, 1, 2, 0xaa),
                write(b, 2, 3, 0xbb),
                write(a, 3, 4, 0xcc),
                ack(b, 4),
            ],
        );
        assert!(err.contains("interleaved"), "{err}");
    }

    #[test]
    fn test_an_acknowledgement_which_rolls_back_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let a = producer(0x10);

        let err = refused(
            &dir,
            &[
                write(a, 5, 2, 0xaa),
                ack(a, 6),
                write(a, 7, 3, 0xbb),
                ack(a, 6),
            ],
        );
        assert!(err.contains("rolls back"), "{err}");
    }
}
