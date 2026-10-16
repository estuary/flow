use super::ledger;
use crate::device;
use crate::journal;
use anyhow::Context;
use futures::FutureExt;

/// An attachment's journal writer.
pub struct Writer {
    /// Journal, ledger, and appender. `None` while an append is in flight.
    appending: Option<Appending>,
    /// The append in flight, which holds `appending`.
    append_fut: Option<AppendFut>,
    /// The device's mutations.
    recorded: device::Recorded,
    /// Set when the device closes the recording channel.
    drained: bool,
    /// The attachment's token. A writer failure cancels it.
    ended: tokio_util::sync::CancellationToken,
}

/// Appends one mutation and gives back the state.
type AppendFut = futures::future::BoxFuture<'static, (Appending, anyhow::Result<()>)>;

impl Writer {
    pub(in crate::journal) fn new(
        journal: journal::Journal,
        appender: publisher::Appender,
        horizon: Option<i64>,
        recorded: device::Recorded,
    ) -> Self {
        let ended = journal.ended.clone();

        Self {
            appending: Some(Appending {
                ledger: ledger::Ledger::new(journal.author, horizon),
                appender,
                journal,
            }),
            append_fut: None,
            recorded,
            drained: false,
            ended,
        }
    }

    /// Run `fut` to completion and append the device's mutations meanwhile.
    pub async fn drain_while<T>(
        &mut self,
        fut: impl std::future::Future<Output = T>,
    ) -> anyhow::Result<T> {
        let mut fut = std::pin::pin!(fut);
        let ended = self.ended.clone();

        let failure = tokio::select! {
            biased;

            () = ended.cancelled() => None,
            out = &mut fut => return Ok(out),
            failure = self.drain() => Some(failure),
        };
        () = self.fail();

        let out = fut.await;
        failure.map_or(Ok(out), Err)
    }

    /// Cut the delta and return its acknowledgement. Close device admission
    /// before you call this.
    pub async fn prepare(
        &mut self,
        completes_horizon: bool,
    ) -> anyhow::Result<(Option<bytes::Bytes>, u64)> {
        let ended = self.ended.clone();

        let prepared = ended
            .run_until_cancelled(async {
                () = self.finish_append().await?;
                let appending = self
                    .appending
                    .as_mut()
                    .expect("with no append in flight, the writer holds its state");

                appending
                    .prepare(&mut self.recorded, completes_horizon)
                    .await
            })
            .await;

        prepared
            .unwrap_or_else(|| Err(super::attachment_ended()))
            .inspect_err(|_| self.fail())
    }

    /// Append the acknowledgement and wait for the broker to confirm it.
    pub async fn acknowledge(&mut self, ack: bytes::Bytes) -> anyhow::Result<()> {
        let ended = self.ended.clone();

        let acknowledged = ended
            .run_until_cancelled(async {
                () = self.finish_append().await?;
                let appending = self
                    .appending
                    .as_mut()
                    .expect("with no append in flight, the writer holds its state");

                appending.acknowledge(ack).await
            })
            .await;

        acknowledged
            .unwrap_or_else(|| Err(super::attachment_ended()))
            .inspect_err(|_| self.fail())
    }

    /// Append the device's mutations one at a time, and return the first failure. Wait
    /// while a prepared delta has no acknowledgement, and after the device closes the
    /// channel.
    async fn drain(&mut self) -> anyhow::Error {
        loop {
            if let Err(err) = self.finish_append().await {
                return err;
            }
            let appending = self
                .appending
                .as_ref()
                .expect("with no append in flight, the writer holds its state");

            if self.drained || appending.ledger.awaiting_ack() {
                return std::future::pending().await;
            }
            let Some(mutation) = self.recorded.recv().await else {
                self.drained = true;
                continue;
            };
            let mut appending = self
                .appending
                .take()
                .expect("with no append in flight, the writer holds its state");

            self.append_fut = Some(
                async move {
                    let appended = appending.append_mutation(mutation).await;
                    (appending, appended)
                }
                .boxed(),
            );
        }
    }

    /// Finish the parked append, if any, and take back its state. Cancel-safe.
    async fn finish_append(&mut self) -> anyhow::Result<()> {
        let Some(append) = &mut self.append_fut else {
            return Ok(());
        };
        let (appending, appended) = append.await;

        self.append_fut = None;
        self.appending = Some(appending);
        appended
    }

    /// End the writer and its attachment. The device discards its later mutations.
    fn fail(&mut self) {
        self.append_fut = None;
        self.recorded.close();
        self.ended.cancel();
    }
}

/// Journal, ledger, and appender of a [`Writer`].
struct Appending {
    /// Journal identity, ownership, and recovery offsets.
    journal: journal::Journal,
    /// Delta sequencing and commit state.
    ledger: ledger::Ledger,
    /// Fenced append pipeline of this attachment.
    appender: publisher::Appender,
}

impl Appending {
    /// Serve [`Writer::prepare`].
    async fn prepare(
        &mut self,
        recorded: &mut device::Recorded,
        completes_horizon: bool,
    ) -> anyhow::Result<(Option<bytes::Bytes>, u64)> {
        // Refuse before the drain, which would take mutations past the awaited ack.
        if self.ledger.awaiting_ack() {
            return Err(crate::failed_precondition(
                "a prepared delta is still awaiting its commit",
            ));
        }

        // Admission is closed, so the channel cannot refill.
        while let Ok(mutation) = recorded.try_recv() {
            () = self.append_mutation(mutation).await?;
        }

        let ack = if self.ledger.delta_is_empty() {
            assert!(
                !completes_horizon,
                "the device reported a discharge for an empty delta"
            );
            None
        } else {
            // Confirm every record of the delta before the cut.
            () = self.journal.flush(&mut self.appender).await?;

            Some(
                self.ledger
                    .cut(std::time::SystemTime::now(), completes_horizon),
            )
        };
        Ok((
            ack,
            self.ledger.range(self.journal.head, self.journal.floor),
        ))
    }

    /// Append `ack` to commit the prepared delta, and store the recovery floor it
    /// installs, if any.
    async fn acknowledge(&mut self, ack: bytes::Bytes) -> anyhow::Result<()> {
        let floor = self.ledger.begin_acknowledge(&ack)?;

        () = self.journal.append_ack(&mut self.appender, &ack).await?;

        let Some(floor) = floor else {
            return Ok(());
        };
        self.journal.floor = floor;

        tracing::info!(
            journal = self.journal.name,
            floor,
            head = self.journal.head,
            "completed a recovery horizon",
        );

        let journal::Journal { client, name, .. } = &self.journal;

        // A failed update does not fail the attachment. The old floor stays, which is
        // safe but makes replay longer.
        if let Err(err) = async {
            let Some(listing) = client.get_journal(name).await.context("listing")? else {
                anyhow::bail!("the journal no longer exists");
            };
            let Some(change) = journal::floor_change(listing, floor) else {
                return Ok(());
            };
            _ = client
                .apply(proto_gazette::broker::ApplyRequest {
                    changes: vec![change],
                })
                .await
                .context("applying the spec change")?;

            anyhow::Ok(())
        }
        .await
        {
            tracing::warn!(
                journal = name,
                floor,
                ?err,
                "could not store the disk's recovery floor"
            );
        }
        Ok(())
    }

    /// Append one mutation to the delta.
    async fn append_mutation(&mut self, mutation: device::Mutation) -> anyhow::Result<()> {
        let device::Mutation {
            chunks,
            opens_horizon,
        } = mutation;

        // Only a delta's first record opens a horizon. That record follows a flush,
        // so it lands at the confirmed head.
        let opens_at = opens_horizon.then_some(self.journal.head);
        let record = self
            .ledger
            .build_uncommitted(std::time::SystemTime::now(), chunks, opens_at);

        self.journal
            .append_record(&mut self.appender, &record)
            .await
    }
}

#[cfg(test)]
mod broker_test {
    use crate::BLOCK_SIZE;
    use crate::chunk;
    use crate::test_support;
    use test_support::broker;

    #[tokio::test]
    async fn test_a_writer_over_a_real_broker() {
        let fixture = broker::Fixture::start().await;

        a_committed_delta_reads_back_as_its_chunks(&fixture).await;
        mutations_offered_while_a_commit_is_outstanding_wait_for_it(&fixture).await;
        a_large_delta_carries_one_record_per_mutation(&fixture).await;
        a_delta_which_spans_several_appends_keeps_its_records(&fixture).await;
        a_completed_horizon_stores_its_openers_offset(&fixture).await;
        a_drain_dropped_mid_append_keeps_its_records(&fixture).await;
        a_failed_drain_lets_its_future_finish(&fixture).await;
        an_ended_attachment_answers_its_client(&fixture).await;

        fixture.stop().await;
    }

    /// A committed delta reads back as exactly the recorded chunks.
    async fn a_committed_delta_reads_back_as_its_chunks(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/delta";
        let (recorder, mut writer) = fixture.open(journal).await.unwrap();

        let mutations = vec![
            chunk::encode_write(0, &bytes::Bytes::from(vec![0x11; 8192])),
            chunk::encode_write(2, &bytes::Bytes::from(vec![0; 4096])),
            vec![chunk::encode_punch(3, 4)],
        ];
        for chunks in mutations {
            recorder.try_send(broker::mutation(chunks)).unwrap();
        }

        let ack = writer.prepare(false).await.unwrap().0.unwrap();
        () = writer.acknowledge(ack.clone()).await.unwrap();

        let records = broker::render(&fixture.read(journal).await);

        // A second commit of the same delta is refused.
        let err = writer.acknowledge(ack).await.unwrap_err();

        insta::assert_snapshot!(format!("{records}\n\nacknowledged again: {err:#}"), @"
        fence     author 1
        continue  author 1  blocks 0..2: data len 8192 [0x11x8192]
        continue  author 1  blocks 2..3: empty data (one zeroed block)
        continue  author 1  blocks 3..7: punch 4
        ack       author 1

        acknowledged again: the acknowledgement matches no prepared delta (FailedPrecondition)
        ");
    }

    /// Mutations stay in the channel until the outstanding acknowledgement commits.
    async fn mutations_offered_while_a_commit_is_outstanding_wait_for_it(
        fixture: &broker::Fixture,
    ) {
        let journal = "acmeCo/disk/outstanding";
        let (recorder, mut writer) = fixture.open(journal).await.unwrap();

        recorder.try_send(broker::write(1, 0xaa)).unwrap();
        let first = writer.prepare(false).await.unwrap().0.unwrap();

        // Offer the second delta's mutations before the first delta commits.
        recorder.try_send(broker::write(2, 0xbb)).unwrap();
        recorder.try_send(broker::write(3, 0xcc)).unwrap();
        () = writer
            .drain_while(tokio::time::sleep(std::time::Duration::from_millis(50)))
            .await
            .unwrap();
        assert!(
            !is_drained(&recorder),
            "the writer took a mutation while an acknowledgement was outstanding",
        );

        () = writer.acknowledge(first).await.unwrap();
        () = writer.drain_while(drains(&recorder)).await.unwrap();

        let second = writer.prepare(false).await.unwrap().0.unwrap();
        () = writer.acknowledge(second).await.unwrap();
        drop((recorder, writer));

        let records = broker::render(&fixture.read(journal).await);

        let (_recorder, _writer, blocks) = fixture.recover(journal, None).await.unwrap();

        insta::assert_snapshot!(format!("{records}\n\nrecovered blocks {}", test_support::render_blocks(&blocks)), @"
        fence     author 1
        continue  author 1  blocks 1..2: data len 4096 [0xaax4096]
        ack       author 1
        continue  author 1  blocks 2..3: data len 4096 [0xbbx4096]
        continue  author 1  blocks 3..4: data len 4096 [0xccx4096]
        ack       author 1

        recovered blocks 1..2 filled 0xaa, 2..3 filled 0xbb, 3..4 filled 0xcc
        ");
    }

    /// A delta has exactly one record per mutation.
    async fn a_large_delta_carries_one_record_per_mutation(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/bounded";
        const WRITES: usize = 8;

        let (recorder, mut writer) = fixture.open(journal).await.unwrap();
        let write = chunk::encode_write(0, &bytes::Bytes::from(vec![0x22; 128 * 1024]));

        for _ in 0..WRITES {
            recorder.try_send(broker::mutation(write.clone())).unwrap();
        }

        let ack = writer.prepare(false).await.unwrap().0.unwrap();
        () = writer.acknowledge(ack).await.unwrap();

        let records = broker::render(&fixture.read(journal).await);

        insta::assert_snapshot!(records, @"
        fence     author 1
        continue  author 1  blocks 0..32: data len 131072 [0x22x131072]
        continue  author 1  blocks 0..32: data len 131072 [0x22x131072]
        continue  author 1  blocks 0..32: data len 131072 [0x22x131072]
        continue  author 1  blocks 0..32: data len 131072 [0x22x131072]
        continue  author 1  blocks 0..32: data len 131072 [0x22x131072]
        continue  author 1  blocks 0..32: data len 131072 [0x22x131072]
        continue  author 1  blocks 0..32: data len 131072 [0x22x131072]
        continue  author 1  blocks 0..32: data len 131072 [0x22x131072]
        ack       author 1
        ");
    }

    /// A delta that spans several append RPCs keeps one record per mutation, in order.
    async fn a_delta_which_spans_several_appends_keeps_its_records(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/batched";

        // Make the delta larger than the appender's flush threshold.
        const MUTATIONS: usize = 40;
        let each = test_support::broker::BLOCKS as usize * BLOCK_SIZE as usize;
        assert!(MUTATIONS * each > publisher::Appender::BUFFER_FLUSH_THRESHOLD);

        let (recorder, mut writer) = fixture.open(journal).await.unwrap();

        for index in 0..MUTATIONS {
            let fill = (index + 1) as u8;
            recorder
                .try_send(broker::mutation(chunk::encode_write(
                    0,
                    &bytes::Bytes::from(vec![fill; each]),
                )))
                .unwrap();
        }

        let ack = writer.prepare(false).await.unwrap().0.unwrap();
        () = writer.acknowledge(ack).await.unwrap();
        drop((recorder, writer));

        // Distinct fill bytes expose any split, merge, or reordering across appends.
        let records = broker::render(&fixture.read(journal).await);

        // The journal is past the threshold, so more than one append built it.
        assert!(fixture.head(journal).await as usize > publisher::Appender::BUFFER_FLUSH_THRESHOLD);

        let (_recorder, _writer, blocks) = fixture.recover(journal, None).await.unwrap();

        insta::assert_snapshot!(format!("{records}\n\nrecovered blocks {}", test_support::render_blocks(&blocks)), @"
        fence     author 1
        continue  author 1  blocks 0..64: data len 262144 [0x01x262144]
        continue  author 1  blocks 0..64: data len 262144 [0x02x262144]
        continue  author 1  blocks 0..64: data len 262144 [0x03x262144]
        continue  author 1  blocks 0..64: data len 262144 [0x04x262144]
        continue  author 1  blocks 0..64: data len 262144 [0x05x262144]
        continue  author 1  blocks 0..64: data len 262144 [0x06x262144]
        continue  author 1  blocks 0..64: data len 262144 [0x07x262144]
        continue  author 1  blocks 0..64: data len 262144 [0x08x262144]
        continue  author 1  blocks 0..64: data len 262144 [0x09x262144]
        continue  author 1  blocks 0..64: data len 262144 [0x0ax262144]
        continue  author 1  blocks 0..64: data len 262144 [0x0bx262144]
        continue  author 1  blocks 0..64: data len 262144 [0x0cx262144]
        continue  author 1  blocks 0..64: data len 262144 [0x0dx262144]
        continue  author 1  blocks 0..64: data len 262144 [0x0ex262144]
        continue  author 1  blocks 0..64: data len 262144 [0x0fx262144]
        continue  author 1  blocks 0..64: data len 262144 [0x10x262144]
        continue  author 1  blocks 0..64: data len 262144 [0x11x262144]
        continue  author 1  blocks 0..64: data len 262144 [0x12x262144]
        continue  author 1  blocks 0..64: data len 262144 [0x13x262144]
        continue  author 1  blocks 0..64: data len 262144 [0x14x262144]
        continue  author 1  blocks 0..64: data len 262144 [0x15x262144]
        continue  author 1  blocks 0..64: data len 262144 [0x16x262144]
        continue  author 1  blocks 0..64: data len 262144 [0x17x262144]
        continue  author 1  blocks 0..64: data len 262144 [0x18x262144]
        continue  author 1  blocks 0..64: data len 262144 [0x19x262144]
        continue  author 1  blocks 0..64: data len 262144 [0x1ax262144]
        continue  author 1  blocks 0..64: data len 262144 [0x1bx262144]
        continue  author 1  blocks 0..64: data len 262144 [0x1cx262144]
        continue  author 1  blocks 0..64: data len 262144 [0x1dx262144]
        continue  author 1  blocks 0..64: data len 262144 [0x1ex262144]
        continue  author 1  blocks 0..64: data len 262144 [0x1fx262144]
        continue  author 1  blocks 0..64: data len 262144 [0x20x262144]
        continue  author 1  blocks 0..64: data len 262144 [0x21x262144]
        continue  author 1  blocks 0..64: data len 262144 [0x22x262144]
        continue  author 1  blocks 0..64: data len 262144 [0x23x262144]
        continue  author 1  blocks 0..64: data len 262144 [0x24x262144]
        continue  author 1  blocks 0..64: data len 262144 [0x25x262144]
        continue  author 1  blocks 0..64: data len 262144 [0x26x262144]
        continue  author 1  blocks 0..64: data len 262144 [0x27x262144]
        continue  author 1  blocks 0..64: data len 262144 [0x28x262144]
        ack       author 1

        recovered blocks 0..64 filled 0x28
        ");
    }

    /// The commit that completes a horizon stores the offset of the horizon's first
    /// record as the floor.
    async fn a_completed_horizon_stores_its_openers_offset(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/horizon-offset";
        let (recorder, mut writer) = fixture.open(journal).await.unwrap();

        // Each delta rewrites the whole disk, discharging the horizon it opens.
        let each = test_support::broker::BLOCKS as usize * BLOCK_SIZE as usize;
        let mut out = Vec::new();

        for delta in 0..2u8 {
            for record in 0..4 {
                let fill = delta * 4 + record + 1;
                recorder
                    .try_send(crate::device::Mutation {
                        chunks: chunk::encode_write(0, &bytes::Bytes::from(vec![fill; each])),
                        opens_horizon: record == 0,
                    })
                    .unwrap();
            }
            let (ack, range) = writer.prepare(true).await.unwrap();
            () = writer.acknowledge(ack.unwrap()).await.unwrap();

            let floor = fixture.floor_label(journal).await.unwrap();
            let head = fixture.head(journal).await;
            out.push(format!(
                "after delta {delta}  floor {floor}  head {head:#x}  next range {range:#x}",
            ));
        }
        drop((recorder, writer));

        // The floor names the record that opened the second delta's horizon.
        let floor = fixture.floor_label(journal).await.unwrap();
        let floor = i64::from_str_radix(&floor, 16).unwrap();
        out.push(broker::render(&fixture.read_from(journal, floor).await));

        let (_recorder, _writer, blocks) = fixture.recover(journal, None).await.unwrap();
        out.push(format!(
            "recovered blocks {}",
            test_support::render_blocks(&blocks)
        ));

        insta::assert_snapshot!(out.join("\n"), @"
        after delta 0  floor 000000000000001a  head 0x1000be  next range 0x10008a
        after delta 1  floor 00000000001000be  head 0x200162  next range 0x10008a
        continue  author 1  blocks 0..64: data len 262144 [0x05x262144], opens a horizon
        continue  author 1  blocks 0..64: data len 262144 [0x06x262144]
        continue  author 1  blocks 0..64: data len 262144 [0x07x262144]
        continue  author 1  blocks 0..64: data len 262144 [0x08x262144]
        ack       author 1
        recovered blocks 0..64 filled 0x08
        ");
    }

    /// A drain dropped during an append leaves the append parked, and the next prepare
    /// finishes it. The journal is the same as without the drop.
    async fn a_drain_dropped_mid_append_keeps_its_records(fixture: &broker::Fixture) {
        // Enough to fill the appender's buffer past its flush threshold during the first
        // append, so that a checkpoint waits on that append. The runtime has one thread,
        // so the append cannot finish during the drain's first poll.
        const MUTATIONS: usize = 36;
        let each = test_support::broker::BLOCKS as usize * BLOCK_SIZE as usize;
        assert!((MUTATIONS - 2) * each > publisher::Appender::BUFFER_FLUSH_THRESHOLD);

        let mut rendered = Vec::new();

        for (journal, drop_a_drain) in [
            ("acmeCo/disk/undisturbed", false),
            ("acmeCo/disk/dropped-drain", true),
        ] {
            let (recorder, mut writer) = fixture.open(journal).await.unwrap();

            for index in 0..MUTATIONS {
                let fill = (index + 1) as u8;
                recorder
                    .try_send(broker::mutation(chunk::encode_write(
                        0,
                        &bytes::Bytes::from(vec![fill; each]),
                    )))
                    .unwrap();
            }
            if drop_a_drain {
                // `yield_now` completes on the second poll, which drops the drain.
                () = writer.drain_while(tokio::task::yield_now()).await.unwrap();
                assert!(
                    writer.append_fut.is_some(),
                    "the drain was dropped with an append in flight"
                );
            }
            let ack = writer.prepare(false).await.unwrap().0.unwrap();
            () = writer.acknowledge(ack).await.unwrap();

            rendered.push(broker::render(&fixture.read(journal).await));
        }

        assert_eq!(rendered[0], rendered[1]);
    }

    /// A failed append ends the writer and its attachment. The drained future still
    /// runs to completion before the failure returns.
    async fn a_failed_drain_lets_its_future_finish(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/failed-drain";
        let (recorder, mut writer) = fixture.open(journal).await.unwrap();

        // A replacement attachment fences this one out, so its next append fails.
        let (_recorder, _replacement, _blocks) = fixture.recover(journal, None).await.unwrap();

        let mut finished = false;
        let drained = writer
            .drain_while(async {
                // Offer mutations until the failed writer closes its channel.
                while !matches!(
                    recorder.try_send(broker::write(1, 0x11)),
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_))
                ) {
                    () = tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                finished = true;
            })
            .await;

        let out = [
            format!("drained   {:#}", drained.unwrap_err()),
            format!("finished  {finished}"),
            format!("ended     {}", writer.ended.is_cancelled()),
        ];
        insta::assert_snapshot!(out.join("\n"), @"
        drained   journal acmeCo/disk/failed-drain registers do not match (Aborted)
        finished  true
        ended     true
        ");
    }

    /// An ended attachment refuses requests at once, without waiting on the broker.
    async fn an_ended_attachment_answers_its_client(fixture: &broker::Fixture) {
        let mut outcomes = Vec::new();

        for (journal, has_delta) in [
            ("acmeCo/disk/ended-empty", false),
            ("acmeCo/disk/ended", true),
        ] {
            let (recorder, mut writer) = fixture.open(journal).await.unwrap();

            if has_delta {
                recorder.try_send(broker::write(6, 0x33)).unwrap();
                () = writer.drain_while(drains(&recorder)).await.unwrap();
            }
            () = writer.ended.cancel();

            let err =
                tokio::time::timeout(std::time::Duration::from_secs(10), writer.prepare(false))
                    .await
                    .expect("an ended attachment answers rather than waiting")
                    .expect_err("an ended attachment commits nothing");

            outcomes.push(format!("{journal:<25}{err:#}"));
        }

        insta::assert_snapshot!(outcomes.join("\n"), @"
        acmeCo/disk/ended-empty  the attachment ended (Unavailable)
        acmeCo/disk/ended        the attachment ended (Unavailable)
        ");
    }

    /// Wait for the writer to take every mutation the recording channel holds.
    async fn drains(recorder: &broker::Recorder) {
        for _ in 0..1000 {
            if is_drained(recorder) {
                return;
            }
            () = tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("the writer did not take the mutations offered to it");
    }

    /// Whether the writer has taken every mutation the recording channel held.
    fn is_drained(recorder: &broker::Recorder) -> bool {
        recorder.capacity() == recorder.max_capacity()
    }
}
