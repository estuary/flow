use super::sequencer;
use super::uncommitted;
use crate::horizon;
use crate::image;
use crate::proto;
use anyhow::Context;
use proto_gazette::{broker, fixed_framing};

/// Continue from the rebuild's next record boundary. `Some(head)` reads to a
/// broker-confirmed head; `None` tails until cancelled or an error occurs.
/// [`Rebuild::take`] checks any offset jump returned by the broker.
pub(in crate::journal) async fn read(
    client: &gazette::journal::Client,
    journal: &str,
    through: Option<i64>,
    rebuild: &mut Rebuild,
) -> anyhow::Result<()> {
    let (end_offset, block) = match through {
        Some(head) => (head, false),
        None => (0, true),
    };

    let stream = client.clone().read(broker::ReadRequest {
        journal: journal.to_string(),
        offset: rebuild.offset,
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
                return Err(
                    crate::journal::broker_failure(inner).context(format!("reading {journal}"))
                );
            }
        };

        () = rebuild
            .take(&mut buf, response.offset, &response.content)
            .with_context(|| format!("replaying {journal}"))?;
    }

    if !buf.is_empty() {
        tracing::warn!(
            journal,
            offset = rebuild.offset,
            bytes = buf.len(),
            "the replayed range ends within a record",
        );
    }
    Ok(())
}

/// Image and sequencing state shared by backfill, tailing, and promotion.
/// Keeping one rebuild preserves producer clocks and the pending delta across
/// reads, so promotion can recognize and apply its recovered acknowledgement.
/// Applies the decisions of [`Sequencer`](sequencer::Sequencer).
pub(in crate::journal) struct Rebuild {
    /// The disk's committed state, rebuilt so far. Nothing of a delta in doubt is in it.
    image: image::Image,
    /// Per-producer ordering and acknowledgement state.
    sequencer: sequencer::Sequencer,
    /// The delta which is not yet acknowledged, held rather than applied.
    uncommitted: uncommitted::UncommittedDelta,
    /// A horizon which has opened and is not yet discharged.
    horizon: Option<OpenHorizon>,
    /// Offset of the last horizon a delta of the range completed, which is the
    /// floor.
    floor: Option<i64>,
    /// Counts across reads and cancellation, so promotion during backfill can
    /// still distinguish committed state from a fresh disk.
    applied_chunks: usize,
    /// Whether this rebuild has taken a record. Until it has, a read may land past the
    /// offset it asked for: see [`Rebuild::take`].
    read_any: bool,
    /// Next record boundary, initially the floor. Advances past every complete
    /// record, including held or skipped records, so continuing reads preserve
    /// the pending delta without fetching it again.
    offset: i64,
}

impl Rebuild {
    /// A rebuild into `image` from `floor`, which holds each delta in `uncommitted`
    /// until its acknowledgement.
    pub(in crate::journal) fn new(
        image: image::Image,
        uncommitted: uncommitted::UncommittedDelta,
        floor: i64,
    ) -> Self {
        Self {
            image,
            sequencer: sequencer::Sequencer::default(),
            uncommitted,
            horizon: None,
            floor: None,
            applied_chunks: 0,
            read_any: false,
            offset: floor,
        }
    }

    /// Chunks applied, which is zero for a journal with no committed state.
    pub(in crate::journal) fn applied_chunks(&self) -> usize {
        self.applied_chunks
    }

    /// Recovery floor this rebuild derived, if a horizon completed within it.
    pub(in crate::journal) fn derived_floor(&self) -> Option<i64> {
        self.floor
    }

    /// The rebuild's sequencing, which says which acknowledgements it can honor.
    pub(in crate::journal) fn sequencer(&self) -> &sequencer::Sequencer {
        &self.sequencer
    }

    /// Take the image, the uncommitted delta, the floor, and any horizon still open,
    /// once the rebuild is done.
    pub(in crate::journal) fn into_parts(
        self,
    ) -> (
        image::Image,
        uncommitted::UncommittedDelta,
        Option<i64>,
        Option<OpenHorizon>,
    ) {
        (self.image, self.uncommitted, self.floor, self.horizon)
    }

    /// Consume a read response, retaining partial frames in `buf`.
    ///
    /// Only the initial read may skip content pruned below the current floor.
    /// Later gaps can hide discards and leave stale blocks allocated; recovery
    /// must restart from the current floor. Unframed bytes also fail, since
    /// skipping them could lose a block's only copy.
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
                "content this rebuild needed was deleted from the store, \
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
                    "{} unframed bytes at offset {at}, which a rebuild cannot skip",
                    skipped.len(),
                ),
                fixed_framing::Frame::Incomplete => return Ok(()),
            }
        }
    }

    /// Sequence `record`, which begins at the rebuild's offset and was framed as
    /// `framed`, and carry out what it does to the delta held.
    fn record(&mut self, record: &proto::DiskRecord, framed: &[u8]) -> anyhow::Result<()> {
        let offset = self.offset;
        self.read_any = true;
        let sequencer::Step { displaced, action } = self.sequencer.on_record(record, offset)?;
        self.offset = offset + framed.len() as i64;

        if let Some(held) = displaced {
            tracing::debug!(
                ?held,
                bytes = self.uncommitted.len(),
                "dropping an uncommitted delta which another producer's records displaced",
            );
            () = self.uncommitted.clear()?;
        }

        match action {
            sequencer::Action::Skip => Ok(()),
            sequencer::Action::Hold => self.uncommitted.push(framed),
            sequencer::Action::Commit { opens_at } => {
                // Apply before checking completion: these chunks may discharge the
                // horizon's last pending blocks.
                self.applied_chunks += self.apply_held(opens_at)?;

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
                Ok(())
            }
        }
    }

    /// Apply the acknowledged delta in record order and count its chunks.
    /// Snapshot a newly opened horizon before applying any of the delta's chunks,
    /// so they discharge the allocation that existed before the delta.
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
                let at = opens_at.expect("the sequencer held the offset of an opening record");
                let blocks = horizon::Horizon::open(image.allocated());

                tracing::debug!(
                    pending = blocks.pending(),
                    at,
                    "a committed delta opened a recovery horizon",
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

/// A recovery horizon this rebuild has opened.
///
/// The offset and the bitmap are held together because they are one fact: `at` is
/// the floor which completing the horizon establishes, and `blocks` is what the
/// horizon still owes before it may. A live disk keeps the pair apart, per
/// [`crate::journal::Recovered::horizon`].
pub(in crate::journal) struct OpenHorizon {
    /// Offset at which the record which opened this horizon begins.
    pub(in crate::journal) at: i64,
    /// Committed blocks which still owe this horizon a copy.
    pub(in crate::journal) blocks: horizon::Horizon,
}

#[cfg(test)]
mod test {
    use super::Rebuild;
    use crate::BLOCK_SIZE;
    use crate::chunk;
    use crate::image;
    use crate::proto;
    use crate::test_support;
    use proto_gazette::{fixed_framing, uuid};
    use std::fmt::Write as _;

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
            installs_author: bytes::Bytes::new(),
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
            chunk::encode_write(block, &bytes::Bytes::from(vec![fill; BLOCK_SIZE as usize])),
        )
    }

    /// A record of `producer`'s delta which punches `block`. It frames far smaller than
    /// a write, which keeps the offsets of a case small enough to read.
    fn punch(producer: uuid::Producer, clock: u64, block: u32) -> proto::DiskRecord {
        record(
            producer,
            clock,
            uuid::Flags::CONTINUE_TXN,
            vec![chunk::encode_punch(block, 1)],
        )
    }

    fn ack(producer: uuid::Producer, clock: u64) -> proto::DiskRecord {
        record(producer, clock, uuid::Flags::ACK_TXN, Vec::new())
    }

    fn fence(producer: uuid::Producer, clock: u64, installs: uuid::Producer) -> proto::DiskRecord {
        proto::DiskRecord {
            installs_author: bytes::Bytes::copy_from_slice(installs.as_bytes()),
            ..record(producer, clock, uuid::Flags::OUTSIDE_TXN, Vec::new())
        }
    }

    /// `record` framed exactly as the journal frames it. A rebuild keeps the journal's
    /// own bytes of an uncommitted record and decodes them again when its delta commits, so
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

    /// A rebuild from `floor` into an image of its own within `dir`.
    fn rebuild(dir: &tempfile::TempDir, floor: i64) -> Rebuild {
        Rebuild::new(
            image::Image::create(dir.path(), BLOCKS).unwrap(),
            super::uncommitted::UncommittedDelta::create(dir.path()).unwrap(),
            floor,
        )
    }

    fn name(producer: uuid::Producer) -> char {
        match producer.as_bytes()[5] {
            0x10 => 'a',
            0x20 => 'f',
            0x30 => 'b',
            _ => '?',
        }
    }

    /// `write a@2 3=aa` fills block 3 with 0xaa at producer a's clock 2.
    /// `punch` discards a block; `opens` is a write that opens a horizon.
    fn label(record: &proto::DiskRecord) -> String {
        let (producer, clock, flags) =
            uuid::parse(uuid::Uuid::from_slice(&record.uuid).unwrap()).unwrap();
        let who = format!("{}@{}", name(producer), clock.to_unix().1 / 1_000);

        if flags.is_ack() {
            return format!("ack {who}");
        }
        if flags.is_outside() {
            let installs = <[u8; 6]>::try_from(&record.installs_author[..]).unwrap();
            return format!(
                "fence {who} for {}",
                name(uuid::Producer::from_bytes(installs))
            );
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
    fn replay(rebuild: &mut Rebuild, records: &[proto::DiskRecord]) -> String {
        let mut out = String::new();

        for record in records {
            write!(out, "at {:<7}{:<17}", rebuild.offset, label(record)).unwrap();

            if let Err(err) = rebuild.record(record, &frame(record)) {
                writeln!(out, "{err:#}").unwrap();
                break;
            }
            let blocks: Vec<String> = test_support::allocated(&rebuild.image)
                .into_iter()
                .map(|(block, fill)| format!("{block}={fill:02x}"))
                .collect();
            let blocks = match blocks.is_empty() {
                true => "-".to_string(),
                false => blocks.join(" "),
            };
            let held = match rebuild.uncommitted.is_empty() {
                true => "-".to_string(),
                false => rebuild.uncommitted.len().to_string(),
            };
            let floor = match rebuild.floor {
                Some(floor) => floor.to_string(),
                None => "-".to_string(),
            };
            let horizon = match &rebuild.horizon {
                Some(open) => format!("{} pending {}", open.at, open.blocks.pending()),
                None => "-".to_string(),
            };

            writeln!(
                out,
                "blocks {blocks:<11}held {held:<6}floor {floor:<7}horizon {horizon}",
            )
            .unwrap();
        }
        writeln!(out, "stands at {}", rebuild.offset).unwrap();
        out
    }

    /// A delta reaches the image only at its acknowledgement, and all of it lands
    /// there at once.
    #[test]
    fn test_a_delta_applies_at_its_acknowledgement() {
        let dir = tempfile::tempdir().unwrap();
        let (a, f) = (producer(0x10), producer(0x20));

        let trace = replay(
            &mut rebuild(&dir, 0),
            &[
                fence(f, 1, a),
                write(a, 2, 3, 0xaa),
                write(a, 3, 4, 0xbb),
                ack(a, 4),
            ],
        );
        insta::assert_snapshot!(trace, @"
        at 0      fence f@1 for a  blocks -          held -     floor -      horizon -
        at 34     write a@2 3=aa   blocks -          held 4130  floor -      horizon -
        at 4164   write a@3 4=bb   blocks -          held 8260  floor -      horizon -
        at 8294   ack a@4          blocks 3=aa 4=bb  held -     floor -      horizon -
        stands at 8320
        ");
    }

    /// Keep unacknowledged changes out of the committed image, but resume reads
    /// after them so continuation does not fetch the held delta again.
    #[test]
    fn test_a_delta_still_in_doubt_is_held_and_not_applied() {
        let dir = tempfile::tempdir().unwrap();
        let a = producer(0x10);

        let trace = replay(
            &mut rebuild(&dir, 0),
            &[
                write(a, 1, 3, 0xaa),
                ack(a, 2),
                // A delta the tenure never acknowledged.
                write(a, 3, 4, 0xbb),
                write(a, 4, 5, 0xcc),
            ],
        );
        insta::assert_snapshot!(trace, @"
        at 0      write a@1 3=aa   blocks -          held 4130  floor -      horizon -
        at 4130   ack a@2          blocks 3=aa       held -     floor -      horizon -
        at 4156   write a@3 4=bb   blocks 3=aa       held 4130  floor -      horizon -
        at 8286   write a@4 5=cc   blocks 3=aa       held 8260  floor -      horizon -
        stands at 12416
        ");
    }

    /// A delta which a replacement tenure's records follow is abandoned. Those
    /// records displace it, so it is dropped rather than carried for the rest of the
    /// rebuild, and nothing of it is ever applied.
    #[test]
    fn test_a_delta_a_replacement_tenure_abandoned_is_not_applied() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b, f) = (producer(0x10), producer(0x30), producer(0x20));

        let trace = replay(
            &mut rebuild(&dir, 0),
            &[
                write(a, 1, 3, 0xaa),
                ack(a, 2),
                write(a, 3, 5, 0xcc),
                fence(f, 4, b),
                write(b, 5, 6, 0xee),
                ack(b, 6),
            ],
        );
        insta::assert_snapshot!(trace, @"
        at 0      write a@1 3=aa   blocks -          held 4130  floor -      horizon -
        at 4130   ack a@2          blocks 3=aa       held -     floor -      horizon -
        at 4156   write a@3 5=cc   blocks 3=aa       held 4130  floor -      horizon -
        at 8286   fence f@4 for b  blocks 3=aa       held 4130  floor -      horizon -
        at 8320   write b@5 6=ee   blocks 3=aa       held 4130  floor -      horizon -
        at 12450  ack b@6          blocks 3=aa 6=ee  held -     floor -      horizon -
        stands at 12476
        ");
    }

    /// A promotion repairs the acknowledgement its client held before it appends
    /// anything of its own, so the delta that fence displaced is still applied.
    #[test]
    fn test_a_delta_a_promotion_repaired_is_applied() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b, f) = (producer(0x10), producer(0x30), producer(0x20));

        let trace = replay(
            &mut rebuild(&dir, 0),
            &[
                write(a, 1, 3, 0xaa),
                fence(f, 2, b),
                ack(a, 3),
                write(b, 4, 5, 0xcc),
                ack(b, 5),
            ],
        );
        insta::assert_snapshot!(trace, @"
        at 0      write a@1 3=aa   blocks -          held 4130  floor -      horizon -
        at 4130   fence f@2 for b  blocks -          held 4130  floor -      horizon -
        at 4164   ack a@3          blocks 3=aa       held -     floor -      horizon -
        at 4190   write b@4 5=cc   blocks 3=aa       held 4130  floor -      horizon -
        at 8320   ack b@5          blocks 3=aa 5=cc  held -     floor -      horizon -
        stands at 8346
        ");
    }

    /// The range may begin within a delta. That delta's acknowledgement then
    /// commits only the records which were in range.
    #[test]
    fn test_a_delta_which_begins_below_the_range_commits_what_is_in_it() {
        let dir = tempfile::tempdir().unwrap();
        let a = producer(0x10);

        let trace = replay(&mut rebuild(&dir, 0), &[write(a, 5, 2, 0xaa), ack(a, 9)]);
        insta::assert_snapshot!(trace, @"
        at 0      write a@5 2=aa   blocks -          held 4130  floor -      horizon -
        at 4130   ack a@9          blocks 2=aa       held -     floor -      horizon -
        stands at 4156
        ");
    }

    /// At-least-once appends repeat records. Sequencing drops a repeat rather than
    /// applying it a second time over a newer value.
    #[test]
    fn test_duplicate_records_are_not_applied_again() {
        let dir = tempfile::tempdir().unwrap();
        let a = producer(0x10);

        let trace = replay(
            &mut rebuild(&dir, 0),
            &[
                write(a, 1, 2, 0xaa),
                write(a, 2, 2, 0xbb),
                write(a, 1, 2, 0xaa),
                ack(a, 3),
                ack(a, 3),
            ],
        );
        insta::assert_snapshot!(trace, @"
        at 0      write a@1 2=aa   blocks -          held 4130  floor -      horizon -
        at 4130   write a@2 2=bb   blocks -          held 8260  floor -      horizon -
        at 8260   write a@1 2=aa   blocks -          held 8260  floor -      horizon -
        at 12390  ack a@3          blocks 2=bb       held -     floor -      horizon -
        at 12416  ack a@3          blocks 2=bb       held -     floor -      horizon -
        stands at 12442
        ");
    }

    #[test]
    fn test_a_punch_deallocates_what_an_earlier_delta_wrote() {
        let dir = tempfile::tempdir().unwrap();
        let a = producer(0x10);

        let trace = replay(
            &mut rebuild(&dir, 0),
            &[
                write(a, 1, 8, 0xaa),
                write(a, 2, 9, 0xbb),
                ack(a, 3),
                punch(a, 4, 8),
                ack(a, 5),
            ],
        );
        insta::assert_snapshot!(trace, @"
        at 0      write a@1 8=aa   blocks -          held 4130  floor -      horizon -
        at 4130   write a@2 9=bb   blocks -          held 8260  floor -      horizon -
        at 8260   ack a@3          blocks 8=aa 9=bb  held -     floor -      horizon -
        at 8286   punch a@4 8      blocks 8=aa 9=bb  held 32    floor -      horizon -
        at 8318   ack a@5          blocks 9=bb       held -     floor -      horizon -
        stands at 8344
        ");
    }

    /// A record which opens a horizon snapshots the blocks allocated before its own
    /// chunks apply. The acknowledgement of the delta which discharges the last of
    /// those blocks moves the floor to that record. The horizon is held with its
    /// delta until then, exactly as that delta's chunks are.
    #[test]
    fn test_a_discharged_horizon_derives_the_floor() {
        let dir = tempfile::tempdir().unwrap();
        let a = producer(0x10);

        let trace = replay(
            &mut rebuild(&dir, 0),
            &[
                write(a, 1, 3, 0xaa),
                write(a, 2, 4, 0xbb),
                ack(a, 3),
                // This delta opens a horizon over both blocks and rewrites them, which
                // discharges the horizon without any copy.
                opens(write(a, 4, 3, 0xcc)),
                write(a, 5, 4, 0xdd),
                ack(a, 6),
            ],
        );
        // The floor is the offset of the record which opened the horizon.
        insta::assert_snapshot!(trace, @"
        at 0      write a@1 3=aa   blocks -          held 4130  floor -      horizon -
        at 4130   write a@2 4=bb   blocks -          held 8260  floor -      horizon -
        at 8260   ack a@3          blocks 3=aa 4=bb  held -     floor -      horizon -
        at 8286   opens a@4 3=cc   blocks 3=aa 4=bb  held 4132  floor -      horizon -
        at 12418  write a@5 4=dd   blocks 3=aa 4=bb  held 8262  floor -      horizon -
        at 16548  ack a@6          blocks 3=cc 4=dd  held -     floor 8286   horizon -
        stands at 16574
        ");
    }

    /// The next tenure resumes a horizon the range leaves open. The rebuild holds both
    /// halves of it: where it opened, and what it has left to discharge.
    #[test]
    fn test_an_open_horizon_outlives_the_pass() {
        let dir = tempfile::tempdir().unwrap();
        let a = producer(0x10);

        let trace = replay(
            &mut rebuild(&dir, 0),
            &[
                write(a, 1, 3, 0xaa),
                write(a, 2, 4, 0xbb),
                ack(a, 3),
                opens(write(a, 4, 3, 0xcc)),
                ack(a, 5),
            ],
        );
        // The horizon still owes block 4 a copy.
        insta::assert_snapshot!(trace, @"
        at 0      write a@1 3=aa   blocks -          held 4130  floor -      horizon -
        at 4130   write a@2 4=bb   blocks -          held 8260  floor -      horizon -
        at 8260   ack a@3          blocks 3=aa 4=bb  held -     floor -      horizon -
        at 8286   opens a@4 3=cc   blocks 3=aa 4=bb  held 4132  floor -      horizon -
        at 12418  ack a@5          blocks 3=cc 4=bb  held -     floor -      horizon 8286 pending 1
        stands at 12444
        ");
    }

    /// A range may hold several horizons. Each one replaces the one before it, so
    /// the floor is the last horizon which a delta discharged.
    #[test]
    fn test_a_later_horizon_replaces_an_earlier_one() {
        let dir = tempfile::tempdir().unwrap();
        let a = producer(0x10);

        let trace = replay(
            &mut rebuild(&dir, 0),
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
        // The second horizon completed, so the floor is where it opened.
        insta::assert_snapshot!(trace, @"
        at 0      write a@1 3=aa   blocks -          held 4130  floor -      horizon -
        at 4130   write a@2 4=bb   blocks -          held 8260  floor -      horizon -
        at 8260   ack a@3          blocks 3=aa 4=bb  held -     floor -      horizon -
        at 8286   opens a@4 3=cc   blocks 3=aa 4=bb  held 4132  floor -      horizon -
        at 12418  ack a@5          blocks 3=cc 4=bb  held -     floor -      horizon 8286 pending 1
        at 12444  opens a@6 3=dd   blocks 3=cc 4=bb  held 4132  floor -      horizon 8286 pending 1
        at 16576  write a@7 4=ee   blocks 3=cc 4=bb  held 8262  floor -      horizon 8286 pending 1
        at 20706  ack a@8          blocks 3=dd 4=ee  held -     floor 12444  horizon -
        stands at 20732
        ");
    }

    /// A horizon belongs to its delta. A horizon whose delta is never acknowledged
    /// never existed, exactly as its chunks never applied.
    #[test]
    fn test_a_horizon_of_an_uncommitted_delta_does_not_exist() {
        let dir = tempfile::tempdir().unwrap();
        let a = producer(0x10);

        let trace = replay(
            &mut rebuild(&dir, 0),
            &[write(a, 1, 3, 0xaa), ack(a, 2), opens(write(a, 3, 4, 0xbb))],
        );
        insta::assert_snapshot!(trace, @"
        at 0      write a@1 3=aa   blocks -          held 4130  floor -      horizon -
        at 4130   ack a@2          blocks 3=aa       held -     floor -      horizon -
        at 4156   opens a@3 4=bb   blocks 3=aa       held 4132  floor -      horizon -
        stands at 8288
        ");
    }

    #[test]
    fn test_malformed_records_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let (a, f) = (producer(0x10), producer(0x20));

        let cases: [(&str, proto::DiskRecord); 5] = [
            (
                "a short UUID",
                proto::DiskRecord {
                    uuid: bytes::Bytes::from_static(b"short"),
                    ..write(a, 1, 0, 0xaa)
                },
            ),
            (
                "an ack which opens a horizon",
                proto::DiskRecord {
                    opens_horizon: true,
                    ..ack(a, 1)
                },
            ),
            (
                "a fence of a 4-byte author",
                proto::DiskRecord {
                    installs_author: bytes::Bytes::from_static(b"nope"),
                    ..fence(f, 1, a)
                },
            ),
            (
                "a fence with a chunk",
                proto::DiskRecord {
                    chunks: vec![chunk::encode_punch(0, 1)],
                    ..fence(f, 1, a)
                },
            ),
            (
                "an ack with a chunk",
                proto::DiskRecord {
                    chunks: vec![chunk::encode_punch(0, 1)],
                    ..ack(a, 9)
                },
            ),
        ];

        // Each case is the first record of its own rebuild, so no case is rejected for
        // the sequencing state another one left behind.
        let mut out = String::new();
        for (what, record) in cases {
            let err = rebuild(&dir, 0)
                .record(&record, &frame(&record))
                .unwrap_err();

            writeln!(out, "{what:<30}{err:#}").unwrap();
        }
        insta::assert_snapshot!(out, @"
        a short UUID                  record carries no message UUID: invalid length: expected 16 bytes, found 5
        an ack which opens a horizon  record of Producer(11:00:00:00:00:10) at Clock(0s 1000ns) opens a horizon but does not begin a delta
        a fence of a 4-byte author    fence record of Producer(21:00:00:00:00:20) installs 4 bytes of author
        a fence with a chunk          a fence carries 1 chunks, which change disk content it does not commit
        an ack with a chunk           an acknowledgement carries 1 chunks, which change disk content it does not commit
        ");
    }

    /// An acknowledgement cannot order two deltas whose records interleaved, so it
    /// is rejected.
    #[test]
    fn test_an_interleaved_acknowledgement_is_an_ordering_error() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = (producer(0x10), producer(0x30));

        let trace = replay(
            &mut rebuild(&dir, 0),
            &[
                write(a, 1, 2, 0xaa),
                write(b, 2, 3, 0xbb),
                ack(b, 3),
                ack(a, 4),
            ],
        );
        insta::assert_snapshot!(trace, @"
        at 0      write a@1 2=aa   blocks -          held 4130  floor -      horizon -
        at 4130   write b@2 3=bb   blocks -          held 4130  floor -      horizon -
        at 8260   ack b@3          blocks 3=bb       held -     floor -      horizon -
        at 8286   ack a@4          acknowledgement of a delta of Producer(11:00:00:00:00:10) which another producer's records interleaved
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
        let (a, b) = (producer(0x10), producer(0x30));

        // `b` displaces and commits past `a`'s delta, and then `a` resumes.
        let displaced = [
            write(a, 1, 2, 0xaa),
            write(b, 2, 3, 0xbb),
            ack(b, 3),
            write(a, 4, 4, 0xcc),
        ];
        let mut resumed = rebuild(&dir, 0);
        let mut first = replay(&mut resumed, &displaced);
        writeln!(
            first,
            "recovered ack a@5  {:?}",
            resumed.sequencer().recovered_ack(a, clock(5)),
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
            replay(&mut rebuild(&dir, 0), &acknowledged),
            replay(&mut rebuild(&dir, 0), &interleaving),
        ]
        .join("\n");
        insta::assert_snapshot!(trace, @"
        at 0      write a@1 2=aa   blocks -          held 4130  floor -      horizon -
        at 4130   write b@2 3=bb   blocks -          held 4130  floor -      horizon -
        at 8260   ack b@3          blocks 3=bb       held -     floor -      horizon -
        at 8286   write a@4 4=cc   blocks 3=bb       held -     floor -      horizon -
        stands at 12416
        recovered ack a@5  Refused

        at 0      write a@1 2=aa   blocks -          held 4130  floor -      horizon -
        at 4130   write b@2 3=bb   blocks -          held 4130  floor -      horizon -
        at 8260   ack b@3          blocks 3=bb       held -     floor -      horizon -
        at 8286   write a@4 4=cc   blocks 3=bb       held -     floor -      horizon -
        at 12416  ack a@5          acknowledgement of a delta of Producer(11:00:00:00:00:10) which another producer's records interleaved
        stands at 12416

        at 0      write a@1 2=aa   blocks -          held 4130  floor -      horizon -
        at 4130   write b@2 3=bb   blocks -          held 4130  floor -      horizon -
        at 8260   write a@3 4=cc   blocks -          held -     floor -      horizon -
        at 12390  ack b@4          acknowledgement of a delta of Producer(31:00:00:00:00:30) which another producer's records interleaved
        stands at 12390
        ");
    }

    #[test]
    fn test_an_acknowledgement_which_rolls_back_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let a = producer(0x10);

        let trace = replay(
            &mut rebuild(&dir, 0),
            &[
                write(a, 5, 2, 0xaa),
                ack(a, 6),
                write(a, 7, 3, 0xbb),
                ack(a, 6),
            ],
        );
        insta::assert_snapshot!(trace, @"
        at 0      write a@5 2=aa   blocks -          held 4130  floor -      horizon -
        at 4130   ack a@6          blocks 2=aa       held -     floor -      horizon -
        at 4156   write a@7 3=bb   blocks 2=aa       held 4130  floor -      horizon -
        at 8286   ack a@6          acknowledgement of Producer(11:00:00:00:00:10) at Clock(0s 6000ns) rolls back records this rebuild sequenced
        stands at 8286
        ");
    }

    /// Hand each `(offset, content)` to `rebuild` as the responses of one read, and
    /// render the offset the rebuild stands at after each, or the failure which ends
    /// the read, then what the read holds of a record it ended within.
    fn trace(rebuild: &mut Rebuild, responses: &[(i64, &[u8])]) -> String {
        let mut buf = bytes::BytesMut::new();
        let mut out = String::new();

        for &(offset, content) in responses {
            match rebuild.take(&mut buf, offset, content) {
                Ok(()) => writeln!(out, "at {offset:<5}stands at {}", rebuild.offset).unwrap(),
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
        let a = producer(0x10);

        let records = [punch(a, 1, 3), punch(a, 2, 4), ack(a, 3)];
        let journal: Vec<u8> = records.iter().flat_map(|r| frame(r).to_vec()).collect();
        let two = offset_of(&records, 2);

        // Cut within the first header, the second body, and the third header.
        let cuts = [0, 3, two - 4, two + 5, journal.len() as i64];
        let responses: Vec<(i64, &[u8])> = cuts
            .windows(2)
            .map(|w| (w[0], &journal[w[0] as usize..w[1] as usize]))
            .collect();

        insta::assert_snapshot!(trace(&mut rebuild(&dir, 0), &responses), @"
        at 0    stands at 0
        at 3    stands at 32
        at 60   stands at 64
        at 69   stands at 90
        holds 0 bytes
        ");
    }

    /// A rebuild's first read may land past the floor it asked for, because what the
    /// store no longer holds is below the real floor. Once the rebuild has begun
    /// taking content, a skip is a gap: within a record, between records, or at the
    /// start of a read which continues the rebuild.
    #[test]
    fn test_only_a_rebuild_s_start_may_skip() {
        let dir = tempfile::tempdir().unwrap();
        let a = producer(0x10);
        let (one, two) = (frame(&punch(a, 1, 3)), frame(&punch(a, 2, 4)));
        let after = 100 + one.len() as i64;

        let mut continued = rebuild(&dir, 40);
        let trace = [
            trace(
                &mut rebuild(&dir, 40),
                &[(100, &one), (after, &two[..10]), (500, &two)],
            ),
            trace(&mut rebuild(&dir, 40), &[(100, &one[..10]), (500, &two)]),
            trace(&mut continued, &[(100, &one)]) + &trace(&mut continued, &[(500, &two)]),
        ]
        .join("\n");
        insta::assert_snapshot!(trace, @"
        at 100  stands at 132
        at 132  stands at 132
        at 500  content this rebuild needed was deleted from the store, from offset 142 to 500
        holds 10 bytes

        at 100  stands at 100
        at 500  content this rebuild needed was deleted from the store, from offset 110 to 500
        holds 10 bytes

        at 100  stands at 132
        holds 0 bytes
        at 500  content this rebuild needed was deleted from the store, from offset 132 to 500
        holds 0 bytes
        ");
    }

    /// Content which frames no record is refused rather than skipped over.
    #[test]
    fn test_unframed_content_is_refused() {
        let dir = tempfile::tempdir().unwrap();

        let trace = trace(
            &mut rebuild(&dir, 0),
            &[(0, b"these bytes frame no record at all")],
        );
        insta::assert_snapshot!(trace, @"
        at 0    31 unframed bytes at offset 0, which a rebuild cannot skip
        holds 3 bytes
        ");
    }
}
