use super::ledger;
use crate::device;
use crate::journal;
use anyhow::Context;

/// Handle to an attachment's journal writer.
pub struct Writer {
    /// Prepare and acknowledge requests to the task.
    commands: tokio::sync::mpsc::Sender<Command>,
    /// The attachment's token, cancelled by `abandon`.
    ended: tokio_util::sync::CancellationToken,
    /// Returns the failure that ended the writer, if no request received it.
    task: Option<tokio::task::JoinHandle<Option<anyhow::Error>>>,
}

impl Writer {
    /// Start a task that appends the mutations of `recorded` to `journal`, until the
    /// returned handle is dropped or the attachment ends.
    pub(in crate::journal) fn spawn(
        journal: journal::Journal,
        appender: publisher::Appender,
        horizon: Option<i64>,
        recorded: device::Recorded,
    ) -> Self {
        let ended = journal.ended.clone();

        let appending = Appending {
            ledger: ledger::Ledger::new(journal.author, horizon),
            appender,
            journal,
        };
        let (commands, receiver) = tokio::sync::mpsc::channel(1);

        let task = tokio::spawn({
            let ended = ended.clone();

            async move {
                tokio::select! {
                    biased;

                    () = ended.cancelled() => Some(proto_grpc::status_to_anyhow(
                        tonic::Status::unavailable("the attachment ended"),
                    )),
                    failure = appending.serve(recorded, receiver) => failure,
                }
            }
        });

        Self {
            commands,
            ended,
            task: Some(task),
        }
    }

    /// Cut the delta and return its acknowledgement, or `None` for an empty delta,
    /// and the next delta's [range](ledger::Ledger::range). Close device admission
    /// before you call this.
    pub async fn prepare(
        &mut self,
        completes_horizon: bool,
    ) -> anyhow::Result<(Option<bytes::Bytes>, u64)> {
        self.call(|reply| Command::Prepare(completes_horizon, reply))
            .await
    }

    /// Append the acknowledgement from [`Writer::prepare`] and wait for the broker
    /// to confirm it.
    pub async fn acknowledge(&mut self, ack: bytes::Bytes) -> anyhow::Result<()> {
        self.call(|reply| Command::Acknowledge(ack, reply)).await
    }

    /// Stop broker work in progress and discard all later mutations.
    pub fn abandon(&self) {
        self.ended.cancel();
    }

    async fn call<T>(&mut self, command: impl FnOnce(Reply<T>) -> Command) -> anyhow::Result<T> {
        let (reply, response) = tokio::sync::oneshot::channel();

        if self.commands.send(command(reply)).await.is_ok()
            && let Ok(result) = response.await
        {
            return result;
        }

        // The writer has ended. Only the first request after that receives its failure.
        let unreported = match self.task.take() {
            Some(task) => task.await.context("the journal writer panicked")?,
            None => None,
        };
        Err(unreported.unwrap_or_else(|| anyhow::anyhow!("the attachment is over")))
    }
}

enum Command {
    Prepare(bool, Reply<(Option<bytes::Bytes>, u64)>),
    Acknowledge(bytes::Bytes, Reply<()>),
}

/// Sends the result of one request to the client that made it.
type Reply<T> = tokio::sync::oneshot::Sender<anyhow::Result<T>>;

/// State of the task behind a [`Writer`].
struct Appending {
    /// Journal identity, ownership, and recovery offsets.
    journal: journal::Journal,
    /// Delta sequencing and commit state.
    ledger: ledger::Ledger,
    /// Fenced append pipeline of this attachment.
    appender: publisher::Appender,
}

impl Appending {
    /// Append mutations and serve requests until the handle is dropped or one of them
    /// fails.
    async fn serve(
        mut self,
        mut recorded: device::Recorded,
        mut commands: tokio::sync::mpsc::Receiver<Command>,
    ) -> Option<anyhow::Error> {
        // Set when the device closes the recording channel.
        let mut drained = false;

        loop {
            tokio::select! {
                biased;

                // Requests before mutations. `prepare` appends the queued mutations itself.
                command = commands.recv() => {
                    let failed = match command {
                        None => return None,
                        Some(Command::Prepare(completes_horizon, reply)) => {
                            let result = self.prepare(&mut recorded, completes_horizon).await;
                            let failed = result.is_err();
                            _ = reply.send(result);
                            failed
                        }
                        Some(Command::Acknowledge(ack, reply)) => {
                            let result = self.acknowledge(ack).await;
                            let failed = result.is_err();
                            _ = reply.send(result);
                            failed
                        }
                    };
                    if failed {
                        return None;
                    }
                }
                // One mutation per iteration, so that requests are served during
                // sustained write load.
                mutation = recorded.recv(), if !drained && !self.ledger.awaiting_ack() => match mutation {
                    Some(mutation) => {
                        if let Err(err) = self.append_mutation(mutation).await {
                            return Some(err);
                        }
                    }
                    None => drained = true,
                },
            }
        }
    }

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
mod test {
    /// An ended writer gives its unreported failure to the next request only.
    #[tokio::test]
    async fn test_an_unreported_failure_reaches_the_next_request_alone() {
        // The task has ended, so its command receiver is gone.
        let (commands, _) = tokio::sync::mpsc::channel(1);
        let mut writer = super::Writer {
            commands,
            ended: tokio_util::sync::CancellationToken::new(),
            task: Some(tokio::spawn(async {
                Some(anyhow::anyhow!("the broker refused"))
            })),
        };
        let mut out = Vec::new();

        for what in ["next prepare", "later prepare"] {
            let err = writer.prepare(false).await.unwrap_err();
            out.push(format!("{what:<15}{err:#}"));
        }

        insta::assert_snapshot!(out.join("\n"), @"
        next prepare   the broker refused
        later prepare  the attachment is over
        ");
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
        an_abandoned_attachment_answers_its_client(&fixture).await;

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
        () = tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !is_drained(&recorder),
            "the writer took a mutation while an acknowledgement was outstanding",
        );

        () = writer.acknowledge(first).await.unwrap();
        () = drains(&recorder).await;

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

    /// An abandoned attachment refuses requests at once, without waiting on the broker.
    async fn an_abandoned_attachment_answers_its_client(fixture: &broker::Fixture) {
        let mut outcomes = Vec::new();

        for (journal, has_delta) in [
            ("acmeCo/disk/abandoned-empty", false),
            ("acmeCo/disk/abandoned", true),
        ] {
            let (recorder, mut writer) = fixture.open(journal).await.unwrap();

            if has_delta {
                recorder.try_send(broker::write(6, 0x33)).unwrap();
                () = drains(&recorder).await;
            }
            () = writer.abandon();

            let err =
                tokio::time::timeout(std::time::Duration::from_secs(10), writer.prepare(false))
                    .await
                    .expect("an abandoned attachment answers rather than waiting")
                    .expect_err("an abandoned attachment commits nothing");

            outcomes.push(format!("{journal:<29}{err:#}"));
        }

        insta::assert_snapshot!(outcomes.join("\n"), @"
        acmeCo/disk/abandoned-empty  the attachment ended (Unavailable)
        acmeCo/disk/abandoned        the attachment ended (Unavailable)
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
