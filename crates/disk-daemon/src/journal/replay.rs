use super::{sequencer, uncommitted};
use crate::proto;
use anyhow::Context;
use proto_gazette::{broker, fixed_framing};

/// Sequencing state shared by backfill, tailing, and promotion, which hands each
/// delta to the device as it commits.
pub(super) struct Replay {
    /// Per-producer ordering and acknowledgement state.
    sequencer: sequencer::Sequencer,
    /// Records of the current delta, held until it's acknowledged.
    uncommitted: uncommitted::UncommittedDelta,
    /// Offset of the last held record which opens a horizon, if any.
    held_opens_at: Option<i64>,
    /// Offset of the last committed record which opened a horizon. The device knows
    /// whether that horizon is still open, and this is its floor once it completes.
    opener: Option<i64>,
    /// Chunks committed, across reads and cancellation. Promotion tells committed
    /// state from a fresh disk by this count.
    applied_chunks: usize,
    /// Whether this replay has taken a record. Only before then may a read skip
    /// ahead, past content pruned below the floor. Any later skip is a gap.
    read_any: bool,
    /// Next record boundary, initially the floor.
    offset: i64,
}

impl Replay {
    /// A replay from `floor`, which holds each delta in `uncommitted` until its
    /// acknowledgement.
    pub fn new(uncommitted: uncommitted::UncommittedDelta, floor: i64) -> Self {
        Self {
            sequencer: sequencer::Sequencer::default(),
            uncommitted,
            held_opens_at: None,
            opener: None,
            applied_chunks: 0,
            read_any: false,
            offset: floor,
        }
    }

    /// The replay's sequencing, which says which acknowledgements it can honor.
    pub fn sequencer(&self) -> &sequencer::Sequencer {
        &self.sequencer
    }

    /// Take the offset of the last committed horizon opener, and the chunks
    /// committed, which is zero for a journal with no
    /// committed state. A delta still held is dropped: past the fence, no
    /// acknowledgement can commit it.
    pub fn into_parts(self) -> (Option<i64>, usize) {
        (self.opener, self.applied_chunks)
    }

    /// Consume a read response, retaining partial frames in `buf`.
    async fn take(
        &mut self,
        buf: &mut bytes::BytesMut,
        offset: i64,
        content: &[u8],
        replayed_tx: &tokio::sync::mpsc::Sender<(proto::Chunk, bool)>,
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
                        .record(&message, &framed, replayed_tx)
                        .await
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

    /// Take `record`, whole and framed as `framed`, at this replay's offset.
    pub async fn record(
        &mut self,
        record: &proto::DiskRecord,
        framed: &[u8],
        replayed_tx: &tokio::sync::mpsc::Sender<(proto::Chunk, bool)>,
    ) -> anyhow::Result<()> {
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
                () = self.apply_held(replayed_tx).await?;

                if let Some(opens_at) = self.held_opens_at.take() {
                    self.opener = Some(opens_at);
                }
                Ok(())
            }
        }
    }

    /// Hand the acknowledged delta to the device in record order, and count its
    /// chunks.
    async fn apply_held(
        &mut self,
        replayed_tx: &tokio::sync::mpsc::Sender<(proto::Chunk, bool)>,
    ) -> anyhow::Result<()> {
        let mut held = self.uncommitted.drain();

        while let Some(record) = held.next()? {
            self.applied_chunks += 1;

            let chunk = record.chunk.expect("a delta's record carries a chunk");
            () = replayed_tx
                .send((chunk, record.opens_horizon))
                .await
                .map_err(|_| anyhow::anyhow!("the disk's device failed to rebuild its image"))?;
        }
        Ok(())
    }
}

/// Continue from the replay's next record boundary. `Some(head)` reads to a
/// broker-confirmed head; `None` tails until cancelled or an error occurs. Each
/// committed chunk goes to `replayed_tx`, with whether it opens a horizon.
/// `Replay::take` checks any offset jump returned by the broker.
pub(super) async fn read(
    client: &gazette::journal::Client,
    journal: &str,
    through: Option<i64>,
    replay: &mut Replay,
    replayed_tx: &tokio::sync::mpsc::Sender<(proto::Chunk, bool)>,
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
            .take(&mut buf, response.offset, &response.content, replayed_tx)
            .await
            .with_context(|| format!("replaying {journal}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::Replay;
    use crate::proto;
    use crate::test_support;
    use proto_gazette::uuid;
    use std::fmt::Write as _;

    /// `record` as one which opens a horizon.
    fn opens(record: proto::DiskRecord) -> proto::DiskRecord {
        proto::DiskRecord {
            opens_horizon: true,
            ..record
        }
    }

    fn write(producer: uuid::Producer, clock: u64, block: u32, fill: u8) -> proto::DiskRecord {
        let chunk = test_support::broker::write(block, fill).chunk;
        test_support::record(producer, clock, uuid::Flags::CONTINUE_TXN, Some(chunk))
    }

    /// A record of `producer`'s delta which punches `block`. It frames far smaller than
    /// a write, which keeps the offsets of a case small enough to read.
    fn punch(producer: uuid::Producer, clock: u64, block: u32) -> proto::DiskRecord {
        let chunk = test_support::punch_chunk(block, 1);
        test_support::record(producer, clock, uuid::Flags::CONTINUE_TXN, Some(chunk))
    }

    fn ack(producer: uuid::Producer, clock: u64) -> proto::DiskRecord {
        test_support::record(producer, clock, uuid::Flags::ACK_TXN, None)
    }

    /// Journal offset at which the record at `index` begins.
    fn offset_of(records: &[proto::DiskRecord], index: usize) -> i64 {
        records[..index]
            .iter()
            .map(|record| test_support::frame([record]).len() as i64)
            .sum()
    }

    /// A replay, and both ends of its replayed chunks.
    struct Case {
        replay: Replay,
        replayed_tx: tokio::sync::mpsc::Sender<(proto::Chunk, bool)>,
        replayed_rx: tokio::sync::mpsc::Receiver<(proto::Chunk, bool)>,
    }

    /// A replay from `floor`, which holds deltas within `dir`.
    fn replay(dir: &tempfile::TempDir, floor: i64) -> Case {
        let (replayed_tx, replayed_rx) = tokio::sync::mpsc::channel(16);

        Case {
            replay: Replay::new(
                super::uncommitted::UncommittedDelta::create(dir.path()).unwrap(),
                floor,
            ),
            replayed_tx,
            replayed_rx,
        }
    }

    /// `3=aa` fills block 3 with 0xaa, and `punch 3` discards it.
    fn change(chunk: &proto::Chunk) -> String {
        match &chunk.content {
            Some(proto::chunk::Content::Data(data)) => format!("{}={:02x}", chunk.block, data[0]),
            Some(proto::chunk::Content::Punch(_)) => format!("punch {}", chunk.block),
            None => panic!("a chunk without content"),
        }
    }

    /// `write a@2 3=aa` fills block 3 with 0xaa at producer a's clock 2.
    /// `punch` discards a block; `opens` is a record that opens a horizon.
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
        let what = change(
            record
                .chunk
                .as_ref()
                .expect("a delta's record carries a chunk"),
        );

        match (record.opens_horizon, what.strip_prefix("punch ")) {
            (true, _) => format!("opens {who} {what}"),
            (false, Some(block)) => format!("punch {who} {block}"),
            (false, None) => format!("write {who} {what}"),
        }
    }

    /// Trace each record's offset, the chunks it hands to the device, held bytes,
    /// and the last committed opener. Stop at the first rejected record. The final
    /// offset is where the next read resumes.
    async fn play(case: &mut Case, records: &[proto::DiskRecord]) -> String {
        let Case {
            replay,
            replayed_tx,
            replayed_rx,
        } = case;
        let mut out = String::new();

        for record in records {
            write!(out, "at {:<7}{:<17}", replay.offset, label(record)).unwrap();

            if let Err(err) = replay
                .record(record, &test_support::frame([record]), replayed_tx)
                .await
            {
                writeln!(out, "{err:#}").unwrap();
                break;
            }
            let mut applies = Vec::new();
            while let Ok((chunk, opens_horizon)) = replayed_rx.try_recv() {
                let opens = if opens_horizon { "opens " } else { "" };
                applies.push(format!("{opens}{}", change(&chunk)));
            }
            let applies = match applies.is_empty() {
                true => "-".to_string(),
                false => applies.join(" "),
            };
            let held = match super::uncommitted::test::held(&replay.uncommitted) {
                0 => "-".to_string(),
                held => held.to_string(),
            };
            let opener = match replay.opener {
                Some(opener) => opener.to_string(),
                None => "-".to_string(),
            };

            writeln!(out, "applies {applies:<19}held {held:<6}opener {opener}").unwrap();
        }
        writeln!(out, "stands at {}", replay.offset).unwrap();
        out
    }

    /// A delta reaches the image only at its acknowledgement, and all of it lands
    /// there at once.
    #[tokio::test]
    async fn test_a_delta_applies_at_its_acknowledgement() {
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
        )
        .await;
        insta::assert_snapshot!(trace, @"
        at 0      fence a          applies -                  held -     opener -
        at 26     write a@2 3=aa   applies -                  held 4132  opener -
        at 4158   write a@3 4=bb   applies -                  held 8264  opener -
        at 8290   ack a@4          applies 3=aa 4=bb          held -     opener -
        stands at 8316
        ");
    }

    /// Keep unacknowledged changes out of the committed image, but resume reads
    /// after them so continuation does not fetch the held delta again.
    #[tokio::test]
    async fn test_a_delta_still_in_doubt_is_held_and_not_applied() {
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
        )
        .await;
        insta::assert_snapshot!(trace, @"
        at 0      write a@1 3=aa   applies -                  held 4132  opener -
        at 4132   ack a@2          applies 3=aa               held -     opener -
        at 4158   write a@3 4=bb   applies -                  held 4132  opener -
        at 8290   write a@4 5=cc   applies -                  held 8264  opener -
        stands at 12422
        ");
    }

    /// A delta which a replacement attachment's records follow is abandoned. Those
    /// records displace it, so it is dropped rather than carried for the rest of the
    /// replay, and nothing of it is ever applied.
    #[tokio::test]
    async fn test_a_delta_a_replacement_attachment_abandoned_is_not_applied() {
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
        )
        .await;
        insta::assert_snapshot!(trace, @"
        at 0      write a@1 3=aa   applies -                  held 4132  opener -
        at 4132   ack a@2          applies 3=aa               held -     opener -
        at 4158   write a@3 5=cc   applies -                  held 4132  opener -
        at 8290   fence b          applies -                  held 4132  opener -
        at 8316   write b@5 6=ee   applies -                  held 4132  opener -
        at 12448  ack b@6          applies 6=ee               held -     opener -
        stands at 12474
        ");
    }

    /// A promotion repairs the acknowledgement its client held before it appends
    /// anything of its own, so the delta that fence displaced is still applied.
    #[tokio::test]
    async fn test_a_delta_a_promotion_repaired_is_applied() {
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
        )
        .await;
        insta::assert_snapshot!(trace, @"
        at 0      write a@1 3=aa   applies -                  held 4132  opener -
        at 4132   fence b          applies -                  held 4132  opener -
        at 4158   ack a@3          applies 3=aa               held -     opener -
        at 4184   write b@4 5=cc   applies -                  held 4132  opener -
        at 8316   ack b@5          applies 5=cc               held -     opener -
        stands at 8342
        ");
    }

    /// The range may begin within a delta. That delta's acknowledgement then
    /// commits only the records which were in range.
    #[tokio::test]
    async fn test_a_delta_which_begins_below_the_range_commits_what_is_in_it() {
        let dir = tempfile::tempdir().unwrap();
        let a = test_support::producer('a');

        let trace = play(&mut replay(&dir, 0), &[write(a, 5, 2, 0xaa), ack(a, 9)]).await;
        insta::assert_snapshot!(trace, @"
        at 0      write a@5 2=aa   applies -                  held 4132  opener -
        at 4132   ack a@9          applies 2=aa               held -     opener -
        stands at 4158
        ");
    }

    /// At-least-once appends repeat records. Sequencing drops a repeat rather than
    /// applying it a second time over a newer value.
    #[tokio::test]
    async fn test_duplicate_records_are_not_applied_again() {
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
        )
        .await;
        insta::assert_snapshot!(trace, @"
        at 0      write a@1 2=aa   applies -                  held 4132  opener -
        at 4132   write a@2 2=bb   applies -                  held 8264  opener -
        at 8264   write a@1 2=aa   applies -                  held 8264  opener -
        at 12396  ack a@3          applies 2=aa 2=bb          held -     opener -
        at 12422  ack a@3          applies -                  held -     opener -
        stands at 12448
        ");
    }

    #[tokio::test]
    async fn test_a_punch_deallocates_what_an_earlier_delta_wrote() {
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
        )
        .await;
        insta::assert_snapshot!(trace, @"
        at 0      write a@1 8=aa   applies -                  held 4132  opener -
        at 4132   write a@2 9=bb   applies -                  held 8264  opener -
        at 8264   ack a@3          applies 8=aa 9=bb          held -     opener -
        at 8290   punch a@4 8      applies -                  held 34    opener -
        at 8324   ack a@5          applies punch 8            held -     opener -
        stands at 8350
        ");
    }

    /// A record which opens a horizon reaches the device, flagged, once its delta
    /// commits, and replay keeps its offset.
    #[tokio::test]
    async fn test_a_committed_opener_is_its_horizon_s_floor() {
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
        )
        .await;
        insta::assert_snapshot!(trace, @"
        at 0      write a@1 3=aa   applies -                  held 4132  opener -
        at 4132   write a@2 4=bb   applies -                  held 8264  opener -
        at 8264   ack a@3          applies 3=aa 4=bb          held -     opener -
        at 8290   opens a@4 3=cc   applies -                  held 4134  opener -
        at 12424  ack a@5          applies opens 3=cc         held -     opener 8290
        at 12450  write a@6 4=dd   applies -                  held 4132  opener 8290
        at 16582  ack a@7          applies 4=dd               held -     opener 8290
        stands at 16608
        ");
    }

    /// A range may hold several horizons. A device opens one only where none is open,
    /// so the only horizon it can hold open after a replay is the last which opened.
    #[tokio::test]
    async fn test_a_later_opener_replaces_an_earlier_one() {
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
        )
        .await;
        insta::assert_snapshot!(trace, @"
        at 0      write a@1 3=aa   applies -                  held 4132  opener -
        at 4132   write a@2 4=bb   applies -                  held 8264  opener -
        at 8264   ack a@3          applies 3=aa 4=bb          held -     opener -
        at 8290   opens a@4 3=cc   applies -                  held 4134  opener -
        at 12424  ack a@5          applies opens 3=cc         held -     opener 8290
        at 12450  opens a@6 4=dd   applies -                  held 4134  opener 8290
        at 16584  ack a@7          applies opens 4=dd         held -     opener 12450
        stands at 16610
        ");
    }

    /// A horizon may open at any record of a delta, and its floor is then that
    /// record's own offset, not the delta's start.
    #[tokio::test]
    async fn test_an_opener_within_a_delta_keeps_its_own_offset() {
        let dir = tempfile::tempdir().unwrap();
        let a = test_support::producer('a');

        let trace = play(
            &mut replay(&dir, 0),
            &[write(a, 1, 3, 0xaa), opens(write(a, 2, 4, 0xbb)), ack(a, 3)],
        )
        .await;
        insta::assert_snapshot!(trace, @"
        at 0      write a@1 3=aa   applies -                  held 4132  opener -
        at 4132   opens a@2 4=bb   applies -                  held 8266  opener -
        at 8266   ack a@3          applies 3=aa opens 4=bb    held -     opener 4132
        stands at 8292
        ");
    }

    /// A horizon belongs to its delta. A horizon whose delta is never acknowledged
    /// never existed, exactly as its chunks never applied, and neither did one whose
    /// delta another producer's records displaced. Neither reaches the device.
    #[tokio::test]
    async fn test_a_horizon_of_an_uncommitted_delta_does_not_exist() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = (test_support::producer('a'), test_support::producer('b'));

        let trace = [
            play(
                &mut replay(&dir, 0),
                &[write(a, 1, 3, 0xaa), ack(a, 2), opens(write(a, 3, 4, 0xbb))],
            )
            .await,
            play(
                &mut replay(&dir, 0),
                &[
                    write(a, 1, 3, 0xaa),
                    ack(a, 2),
                    opens(write(a, 3, 4, 0xbb)),
                    write(b, 4, 5, 0xcc),
                    ack(b, 5),
                ],
            )
            .await,
        ]
        .join("\n");
        insta::assert_snapshot!(trace, @"
        at 0      write a@1 3=aa   applies -                  held 4132  opener -
        at 4132   ack a@2          applies 3=aa               held -     opener -
        at 4158   opens a@3 4=bb   applies -                  held 4134  opener -
        stands at 8292

        at 0      write a@1 3=aa   applies -                  held 4132  opener -
        at 4132   ack a@2          applies 3=aa               held -     opener -
        at 4158   opens a@3 4=bb   applies -                  held 4134  opener -
        at 8292   write b@4 5=cc   applies -                  held 4132  opener -
        at 12424  ack b@5          applies 5=cc               held -     opener -
        stands at 12450
        ");
    }

    #[tokio::test]
    async fn test_malformed_records_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let a = test_support::producer('a');

        let cases: [(&str, proto::DiskRecord); 1] = [(
            "a short UUID",
            proto::DiskRecord {
                uuid: bytes::Bytes::from_static(b"short"),
                ..write(a, 1, 0, 0xaa)
            },
        )];

        // Each case is the first record of its own replay, so no case is rejected for
        // the sequencing state another one left behind.
        let mut out = String::new();
        for (what, record) in cases {
            let mut case = replay(&dir, 0);
            let err = case
                .replay
                .record(&record, &test_support::frame([&record]), &case.replayed_tx)
                .await
                .unwrap_err();

            writeln!(out, "{what:<22}{err:#}").unwrap();
        }
        insta::assert_snapshot!(out, @"
        a short UUID          record carries no message UUID: invalid length: expected 16 bytes, found 5
        ");
    }

    /// An acknowledgement cannot order two deltas whose records interleaved, so it
    /// is rejected.
    #[tokio::test]
    async fn test_an_interleaved_acknowledgement_is_an_ordering_error() {
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
        )
        .await;
        insta::assert_snapshot!(trace, @"
        at 0      write a@1 2=aa   applies -                  held 4132  opener -
        at 4132   write b@2 3=bb   applies -                  held 4132  opener -
        at 8264   ack b@3          applies 3=bb               held -     opener -
        at 8290   ack a@4          acknowledgement of a delta of Producer(61:00:00:00:00:61) which another producer's records interleaved
        stands at 8290
        ");
    }

    /// A displaced delta has lost its earlier records. Drop any resumed fragment
    /// and reject both recovered and journaled acknowledgements.
    ///
    /// This requires writes past a replacement's fence, possible if etcd loses
    /// the `author` register.
    #[tokio::test]
    async fn test_a_displaced_delta_which_resumes_is_never_committed() {
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
        let mut first = play(&mut resumed, &displaced).await;
        writeln!(
            first,
            "recovered ack a@5  {:?}",
            resumed
                .replay
                .sequencer()
                .recovered_ack(a, test_support::clock(5)),
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
            play(&mut replay(&dir, 0), &acknowledged).await,
            play(&mut replay(&dir, 0), &interleaving).await,
        ]
        .join("\n");
        insta::assert_snapshot!(trace, @"
        at 0      write a@1 2=aa   applies -                  held 4132  opener -
        at 4132   write b@2 3=bb   applies -                  held 4132  opener -
        at 8264   ack b@3          applies 3=bb               held -     opener -
        at 8290   write a@4 4=cc   applies -                  held -     opener -
        stands at 12422
        recovered ack a@5  Refused

        at 0      write a@1 2=aa   applies -                  held 4132  opener -
        at 4132   write b@2 3=bb   applies -                  held 4132  opener -
        at 8264   ack b@3          applies 3=bb               held -     opener -
        at 8290   write a@4 4=cc   applies -                  held -     opener -
        at 12422  ack a@5          acknowledgement of a delta of Producer(61:00:00:00:00:61) which another producer's records interleaved
        stands at 12422

        at 0      write a@1 2=aa   applies -                  held 4132  opener -
        at 4132   write b@2 3=bb   applies -                  held 4132  opener -
        at 8264   write a@3 4=cc   applies -                  held -     opener -
        at 12396  ack b@4          acknowledgement of a delta of Producer(63:00:00:00:00:62) which another producer's records interleaved
        stands at 12396
        ");
    }

    #[tokio::test]
    async fn test_an_acknowledgement_which_rolls_back_is_rejected() {
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
        )
        .await;
        insta::assert_snapshot!(trace, @"
        at 0      write a@5 2=aa   applies -                  held 4132  opener -
        at 4132   ack a@6          applies 2=aa               held -     opener -
        at 4158   write a@7 3=bb   applies -                  held 4132  opener -
        at 8290   ack a@6          acknowledgement of Producer(61:00:00:00:00:61) at Clock(0s 6000ns) rolls back records this replay sequenced
        stands at 8290
        ");
    }

    /// Hand each `(offset, content)` to `replay` as the responses of one read, and
    /// render the offset the replay stands at after each, or the failure which ends
    /// the read, then what the read holds of a record it ended within.
    async fn trace(case: &mut Case, responses: &[(i64, &[u8])]) -> String {
        let Case {
            replay,
            replayed_tx,
            ..
        } = case;
        let mut buf = bytes::BytesMut::new();
        let mut out = String::new();

        for &(offset, content) in responses {
            match replay.take(&mut buf, offset, content, replayed_tx).await {
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
    #[tokio::test]
    async fn test_records_are_taken_across_responses() {
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

        insta::assert_snapshot!(trace(&mut replay(&dir, 0), &responses).await, @"
        at 0    stands at 0
        at 3    stands at 34
        at 64   stands at 68
        at 73   stands at 94
        holds 0 bytes
        ");
    }

    /// A replay's first read may land past the floor it asked for, because what the
    /// store no longer holds is below the real floor. Once the replay has begun
    /// taking content, a skip is a gap: within a record, between records, or at the
    /// start of a read which continues the replay.
    #[tokio::test]
    async fn test_only_a_replay_s_start_may_skip() {
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
            )
            .await,
            trace(&mut replay(&dir, 40), &[(100, &one[..10]), (500, &two)]).await,
            trace(&mut continued, &[(100, &one)]).await
                + &trace(&mut continued, &[(500, &two)]).await,
        ]
        .join("\n");
        insta::assert_snapshot!(trace, @"
        at 100  stands at 134
        at 134  stands at 134
        at 500  content this replay needed was deleted from the store, from offset 144 to 500
        holds 10 bytes

        at 100  stands at 100
        at 500  content this replay needed was deleted from the store, from offset 110 to 500
        holds 10 bytes

        at 100  stands at 134
        holds 0 bytes
        at 500  content this replay needed was deleted from the store, from offset 134 to 500
        holds 0 bytes
        ");
    }

    /// Content which frames no record is refused rather than skipped over.
    #[tokio::test]
    async fn test_unframed_content_is_refused() {
        let dir = tempfile::tempdir().unwrap();

        let trace = trace(
            &mut replay(&dir, 0),
            &[(0, b"these bytes frame no record at all")],
        )
        .await;
        insta::assert_snapshot!(trace, @"
        at 0    31 unframed bytes at offset 0, which a replay cannot skip
        holds 3 bytes
        ");
    }
}
