use super::ledger;
use crate::device;
use anyhow::Context;

/// Handle to an attachment's journal writer: a task which appends the device's
/// live chunks, and serves prepares and acknowledgements.
pub struct Writer {
    /// Requests to the task.
    commands: tokio::sync::mpsc::Sender<Command>,
    /// The failure which ended the writer.
    failed: std::sync::Arc<std::sync::OnceLock<tonic::Status>>,
    /// The attachment's cancellation, which a request gives up on.
    cancel: tokio_util::sync::CancellationToken,
    /// Held for its drop, which aborts the task.
    _task: tokio_util::task::AbortOnDropHandle<()>,
}

/// A request of the writer's task.
enum Command {
    Prepare(Reply<Option<bytes::Bytes>>),
    Acknowledge(bytes::Bytes, Reply<()>),
}

/// Sends the result of one request to the caller which made it.
type Reply<T> = tokio::sync::oneshot::Sender<anyhow::Result<T>>;

impl Writer {
    /// Start a task which appends `served_rx` to the promoted `journal`. `opener` is
    /// the offset of the last committed record which opened a horizon.
    pub(super) fn spawn(
        journal: super::Journal,
        appender: publisher::Appender,
        opener: Option<i64>,
        served_rx: tokio::sync::mpsc::Receiver<device::LiveChunk>,
    ) -> Self {
        let cancel = journal.cancel.clone();
        let failed = std::sync::Arc::new(std::sync::OnceLock::new());
        let (commands, received) = tokio::sync::mpsc::channel(1);

        let task = Task {
            appending: Appending {
                ledger: ledger::Ledger::new(journal.author, opener),
                appender,
                journal,
            },
            served_rx,
            failed: failed.clone(),
            cancel: cancel.clone(),
        };

        Self {
            commands,
            failed,
            cancel,
            _task: tokio_util::task::AbortOnDropHandle::new(tokio::spawn(task.serve(received))),
        }
    }

    /// Cut the delta and return its unappended acknowledgement, or `None` for an
    /// empty delta. Sync the disk's filesystem first: the delta then holds every
    /// live chunk which the device served before the sync returned.
    pub async fn prepare(&self) -> anyhow::Result<Option<bytes::Bytes>> {
        self.call(Command::Prepare).await
    }

    /// Append the acknowledgement and wait for the broker to confirm it.
    pub async fn acknowledge(&self, ack: bytes::Bytes) -> anyhow::Result<()> {
        self.call(|reply| Command::Acknowledge(ack, reply)).await
    }

    /// The failure which ended the writer, if one did.
    pub fn failure(&self) -> Option<tonic::Status> {
        self.failed.get().cloned()
    }

    async fn call<T>(&self, command: impl FnOnce(Reply<T>) -> Command) -> anyhow::Result<T> {
        let (reply, replied) = tokio::sync::oneshot::channel();

        let answered = self
            .cancel
            .run_until_cancelled(async {
                _ = self.commands.send(command(reply)).await;
                replied.await.ok()
            })
            .await
            .flatten();

        // A writer which failed, or a cancelled attachment, answers with why.
        answered.unwrap_or_else(|| match self.failure() {
            Some(failure) => Err(proto_grpc::status_to_anyhow(failure)),
            None => Err(crate::cancelled()),
        })
    }
}

struct Task {
    /// Journal, ledger, and appender.
    appending: Appending,
    /// The device's live chunks.
    served_rx: tokio::sync::mpsc::Receiver<device::LiveChunk>,
    /// Set by the task's first failure, which its handle reads.
    failed: std::sync::Arc<std::sync::OnceLock<tonic::Status>>,
    /// The attachment's cancellation, which gives up a broker call and which a
    /// failure triggers.
    cancel: tokio_util::sync::CancellationToken,
}

impl Task {
    /// Append live chunks and serve requests until the handle is dropped. A failure
    /// ends appending, but every later request is still answered with it.
    async fn serve(mut self, mut commands: tokio::sync::mpsc::Receiver<Command>) {
        loop {
            // A prepared delta's acknowledgement must land before the next delta's
            // records: Gazette refuses an ack clocked below a pending record.
            let taking = self.failed.get().is_none() && !self.appending.ledger.awaiting_ack();

            tokio::select! {
                biased;

                command = commands.recv() => match command {
                    None => return,
                    Some(Command::Prepare(reply)) => {
                        _ = reply.send(self.prepare().await);
                    }
                    Some(Command::Acknowledge(ack, reply)) => {
                        _ = reply.send(self.acknowledge(ack).await);
                    }
                },
                live = self.served_rx.recv(), if taking => {
                    let appended = match live {
                        Some(live) => {
                            let appending = self.appending.append_chunk(live);
                            super::unless_cancelled(&self.cancel, appending).await
                        }
                        None => Err(anyhow::anyhow!("the disk's device failed")),
                    };
                    if let Err(err) = appended {
                        _ = self.fail(err);
                    }
                }
            }
        }
    }

    async fn prepare(&mut self) -> anyhow::Result<Option<bytes::Bytes>> {
        self.ready()?;
        let prepared = self.appending.prepare(&mut self.served_rx);

        match super::unless_cancelled(&self.cancel, prepared).await {
            Ok(ack) => Ok(ack),
            Err(err) => Err(self.fail(err)),
        }
    }

    async fn acknowledge(&mut self, ack: bytes::Bytes) -> anyhow::Result<()> {
        self.ready()?;
        let acknowledged = self.appending.acknowledge(ack);

        match super::unless_cancelled(&self.cancel, acknowledged).await {
            Ok(()) => Ok(()),
            Err(err) => Err(self.fail(err)),
        }
    }

    /// The failure which ended the writer, if one did.
    fn ready(&self) -> anyhow::Result<()> {
        match self.failed.get() {
            Some(failure) => Err(proto_grpc::status_to_anyhow(failure.clone())),
            None => Ok(()),
        }
    }

    /// Fail the writer with `err`, which then answers every later request. It
    /// takes no more live chunks, so the device discards them, and it cancels its
    /// attachment. Returns `err` as each request receives it.
    fn fail(&mut self, err: anyhow::Error) -> anyhow::Error {
        let failure = self
            .failed
            .get_or_init(|| proto_grpc::anyhow_to_status(err))
            .clone();

        self.served_rx.close();
        self.cancel.cancel();

        proto_grpc::status_to_anyhow(failure)
    }
}

/// Journal, ledger, and appender of a [`Writer`].
struct Appending {
    /// Journal identity, ownership, and recovery offsets.
    journal: super::Journal,
    /// Delta sequencing and commit state.
    ledger: ledger::Ledger,
    /// Fenced append pipeline of this attachment.
    appender: publisher::Appender,
}

impl Appending {
    /// Serve `Writer::prepare`.
    async fn prepare(
        &mut self,
        served_rx: &mut tokio::sync::mpsc::Receiver<device::LiveChunk>,
    ) -> anyhow::Result<Option<bytes::Bytes>> {
        // Refuse before the drain, which would take live chunks past the awaited ack.
        if self.ledger.awaiting_ack() {
            return Err(crate::failed_precondition(
                "a prepared delta is still awaiting its commit",
            ));
        }

        // A write completes only once the device has sent it, so every write
        // the sync waited on has reached the channel by now. Once it is empty, the
        // delta holds them all.
        loop {
            match served_rx.try_recv() {
                Ok(live) => () = self.append_chunk(live).await?,
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    anyhow::bail!("the disk's device failed")
                }
            }
        }
        if self.ledger.delta_is_empty() {
            return Ok(None);
        }

        // Confirm every record of the delta before the cut.
        () = self.journal.flush(&mut self.appender).await?;

        Ok(Some(self.ledger.cut(std::time::SystemTime::now())))
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

        let super::Journal { client, name, .. } = &self.journal;

        // A failed update does not fail the attachment. The old floor stays, which is
        // safe but makes replay longer.
        if let Err(err) = async {
            let Some(listing) = client.get_journal(name).await.context("listing")? else {
                anyhow::bail!("the journal no longer exists");
            };
            _ = client
                .apply(proto_gazette::broker::ApplyRequest {
                    changes: vec![super::floor_change(listing, floor)],
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

    async fn append_chunk(&mut self, live: device::LiveChunk) -> anyhow::Result<()> {
        let device::LiveChunk {
            chunk,
            opens_horizon,
            discharges_horizon,
        } = live;

        // A horizon's floor is its opening record's offset. Confirm the delta's
        // earlier records first, so that the opener lands at the confirmed head. A
        // delta's first record follows a flush already.
        let opens_at = if opens_horizon {
            if !self.ledger.delta_is_empty() {
                () = self.journal.flush(&mut self.appender).await?;
            }
            Some(self.journal.head)
        } else {
            None
        };
        let record = self.ledger.build_uncommitted(
            std::time::SystemTime::now(),
            chunk,
            opens_at,
            discharges_horizon,
        );

        self.journal
            .append_record(&mut self.appender, &record)
            .await
    }
}

#[cfg(test)]
mod broker_test {
    use crate::BLOCK_SIZE;
    use crate::proto;
    use crate::test_support;
    use test_support::broker;

    #[tokio::test]
    async fn test_a_writer_over_a_real_broker() {
        let fixture = broker::Fixture::start().await;

        a_committed_delta_reads_back_as_its_chunks(&fixture).await;
        chunks_offered_while_a_commit_is_outstanding_wait_for_it(&fixture).await;
        a_large_delta_carries_one_record_per_chunk(&fixture).await;
        a_delta_which_spans_several_appends_keeps_its_records(&fixture).await;
        a_completed_horizon_stores_its_openers_offset(&fixture).await;
        an_opener_within_a_delta_stores_its_own_offset(&fixture).await;
        a_failed_append_answers_every_later_request(&fixture).await;
        a_cancelled_attachment_answers_its_client(&fixture).await;

        fixture.stop().await;
    }

    /// A committed delta reads back as exactly the served_rx chunks.
    async fn a_committed_delta_reads_back_as_its_chunks(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/delta";
        let (served_tx, writer) = fixture.open(journal).await.unwrap();

        let chunks = [
            test_support::write_chunk(0, 2, 0x11),
            // A zeroed block, which a device records as empty data.
            proto::Chunk {
                block: 2,
                blocks: 1,
                content: Some(proto::chunk::Content::Data(bytes::Bytes::new())),
            },
            test_support::punch_chunk(3, 4),
        ];
        for chunk in chunks {
            served_tx.try_send(broker::live_chunk(chunk)).unwrap();
        }

        let ack = writer.prepare().await.unwrap().unwrap();
        () = writer.acknowledge(ack.clone()).await.unwrap();

        let records = broker::render(&fixture.read(journal).await);

        // A second commit of the same delta is refused.
        let err = writer.acknowledge(ack).await.unwrap_err();

        insta::assert_snapshot!(format!("{records}\n\nacknowledged again: {err:#}"), @"
        fence     author 1
        continue  author 1  blocks 0..2: data len 8192 [0x11x8192]
        continue  author 1  blocks 2..3: zeroed
        continue  author 1  blocks 3..7: punch
        ack       author 1

        acknowledged again: the acknowledgement matches no prepared delta (FailedPrecondition)
        ");
    }

    /// Live chunks stay in the channel until the outstanding acknowledgement commits.
    async fn chunks_offered_while_a_commit_is_outstanding_wait_for_it(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/outstanding";
        let (served_tx, writer) = fixture.open(journal).await.unwrap();

        served_tx.try_send(broker::write(1, 0xaa)).unwrap();
        let first = writer.prepare().await.unwrap().unwrap();

        // Offer the second delta's live chunks before the first delta commits.
        served_tx.try_send(broker::write(2, 0xbb)).unwrap();
        served_tx.try_send(broker::write(3, 0xcc)).unwrap();
        () = tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !is_drained(&served_tx),
            "the writer took a live chunk while an acknowledgement was outstanding",
        );

        () = writer.acknowledge(first).await.unwrap();
        () = drains(&served_tx).await;

        let second = writer.prepare().await.unwrap().unwrap();
        () = writer.acknowledge(second).await.unwrap();
        drop((served_tx, writer));

        let records = broker::render(&fixture.read(journal).await);

        let (_served_tx, _writer, blocks) = fixture.recover(journal, None).await.unwrap();

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

    /// A delta has exactly one record per live chunk.
    async fn a_large_delta_carries_one_record_per_chunk(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/bounded";
        const WRITES: usize = 8;

        let (served_tx, writer) = fixture.open(journal).await.unwrap();
        let write = test_support::write_chunk(0, 32, 0x22);

        for _ in 0..WRITES {
            served_tx
                .try_send(broker::live_chunk(write.clone()))
                .unwrap();
        }

        let ack = writer.prepare().await.unwrap().unwrap();
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

    /// A delta that spans several append RPCs keeps one record per live chunk, in order.
    async fn a_delta_which_spans_several_appends_keeps_its_records(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/batched";

        // Make the delta larger than the appender's flush threshold.
        const CHUNKS: usize = 40;
        let each = test_support::broker::BLOCKS as usize * BLOCK_SIZE as usize;
        assert!(CHUNKS * each > publisher::Appender::BUFFER_FLUSH_THRESHOLD);

        let (served_tx, writer) = fixture.open(journal).await.unwrap();

        for index in 0..CHUNKS {
            let fill = (index + 1) as u8;
            served_tx
                .try_send(broker::live_chunk(test_support::write_chunk(
                    0,
                    test_support::broker::BLOCKS,
                    fill,
                )))
                .unwrap();
        }

        let ack = writer.prepare().await.unwrap().unwrap();
        () = writer.acknowledge(ack).await.unwrap();
        drop((served_tx, writer));

        // Distinct fill bytes expose any split, merge, or reordering across appends.
        let records = broker::render(&fixture.read(journal).await);

        // The journal is past the threshold, so more than one append built it.
        assert!(fixture.head(journal).await as usize > publisher::Appender::BUFFER_FLUSH_THRESHOLD);

        let (_served_tx, _writer, blocks) = fixture.recover(journal, None).await.unwrap();

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
        let (served_tx, writer) = fixture.open(journal).await.unwrap();

        // Each delta rewrites the whole disk, discharging the horizon it opens.
        let mut out = Vec::new();

        for delta in 0..2u8 {
            for record in 0..4 {
                let fill = delta * 4 + record + 1;
                served_tx
                    .try_send(crate::device::LiveChunk {
                        chunk: test_support::write_chunk(0, test_support::broker::BLOCKS, fill),
                        opens_horizon: record == 0,
                        discharges_horizon: record == 3,
                    })
                    .unwrap();
            }
            let ack = writer.prepare().await.unwrap();
            () = writer.acknowledge(ack.unwrap()).await.unwrap();

            let floor = fixture.floor_label(journal).await.unwrap();
            let head = fixture.head(journal).await;
            out.push(format!(
                "after delta {delta}  floor {floor}  head {head:#x}"
            ));
        }
        drop((served_tx, writer));

        // The floor names the record that opened the second delta's horizon.
        let floor = fixture.floor_label(journal).await.unwrap();
        let floor = i64::from_str_radix(&floor, 16).unwrap();
        out.push(broker::render(&fixture.read_from(journal, floor).await));

        let (_served_tx, _writer, blocks) = fixture.recover(journal, None).await.unwrap();
        out.push(format!(
            "recovered blocks {}",
            test_support::render_blocks(&blocks)
        ));

        insta::assert_snapshot!(out.join("\n"), @"
        after delta 0  floor 000000000000001a  head 0x1000c6
        after delta 1  floor 00000000001000c6  head 0x200172
        continue  author 1  blocks 0..64: data len 262144 [0x05x262144], opens a horizon
        continue  author 1  blocks 0..64: data len 262144 [0x06x262144]
        continue  author 1  blocks 0..64: data len 262144 [0x07x262144]
        continue  author 1  blocks 0..64: data len 262144 [0x08x262144]
        ack       author 1
        recovered blocks 0..64 filled 0x08
        ");
    }

    /// A horizon may open at a record within a delta, whose earlier records are then
    /// confirmed first. The floor is that record's own offset, and a replay from it
    /// begins mid-delta, which still commits.
    async fn an_opener_within_a_delta_stores_its_own_offset(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/horizon-within";
        let (served_tx, writer) = fixture.open(journal).await.unwrap();

        for (record, opens_horizon, discharges_horizon) in
            [(0, false, false), (1, true, false), (2, false, true)]
        {
            served_tx
                .try_send(crate::device::LiveChunk {
                    chunk: test_support::write_chunk(0, test_support::broker::BLOCKS, record + 1),
                    opens_horizon,
                    discharges_horizon,
                })
                .unwrap();
        }
        let ack = writer.prepare().await.unwrap().unwrap();
        () = writer.acknowledge(ack).await.unwrap();
        drop((served_tx, writer));

        let floor = fixture.floor_label(journal).await.unwrap();
        let floor = i64::from_str_radix(&floor, 16).unwrap();
        let mut out = vec![format!("floor {floor:#x}")];
        out.push(broker::render(&fixture.read_from(journal, floor).await));

        let (_served_tx, _writer, blocks) = fixture.recover(journal, None).await.unwrap();
        out.push(format!(
            "recovered blocks {}",
            test_support::render_blocks(&blocks)
        ));

        insta::assert_snapshot!(out.join("\n"), @"
        floor 0x4003e
        continue  author 1  blocks 0..64: data len 262144 [0x02x262144], opens a horizon
        continue  author 1  blocks 0..64: data len 262144 [0x03x262144]
        ack       author 1
        recovered blocks 0..64 filled 0x03
        ");
    }

    /// A failed append fails the writer and cancels its attachment. Every later request
    /// is answered with that failure, and the device's later live chunks are refused.
    async fn a_failed_append_answers_every_later_request(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/failed-append";
        let (served_tx, writer) = fixture.open(journal).await.unwrap();

        // A replacement attachment fences this one out, so its next append fails.
        let (_served_tx, _replacement, _blocks) = fixture.recover(journal, None).await.unwrap();

        // Offer live chunks until the failed writer closes its channel.
        while !matches!(
            served_tx.try_send(broker::write(1, 0x11)),
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_))
        ) {
            () = tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        let mut out = vec![
            format!("cancelled       {}", writer.cancel.is_cancelled()),
            format!(
                "failure         {:?}",
                writer.failure().map(|status| status.code())
            ),
        ];
        for what in ["prepare", "later prepare"] {
            let err = writer.prepare().await.unwrap_err();
            out.push(format!("{what:<16}{err:#}"));
        }
        insta::assert_snapshot!(out.join("\n"), @"
        cancelled       true
        failure         Some(Aborted)
        prepare         journal acmeCo/disk/failed-append registers do not match (Aborted)
        later prepare   journal acmeCo/disk/failed-append registers do not match (Aborted)
        ");
    }

    /// A cancelled attachment refuses requests at once, without waiting on the broker.
    async fn a_cancelled_attachment_answers_its_client(fixture: &broker::Fixture) {
        let mut outcomes = Vec::new();

        for (journal, has_delta) in [
            ("acmeCo/disk/cancelled-empty", false),
            ("acmeCo/disk/cancelled", true),
        ] {
            let (served_tx, writer) = fixture.open(journal).await.unwrap();

            if has_delta {
                served_tx.try_send(broker::write(6, 0x33)).unwrap();
                () = drains(&served_tx).await;
            }
            () = writer.cancel.cancel();

            let err = tokio::time::timeout(std::time::Duration::from_secs(10), writer.prepare())
                .await
                .expect("a cancelled attachment answers rather than waiting")
                .expect_err("a cancelled attachment commits nothing");

            outcomes.push(format!("{journal:<29}{err:#}"));
        }

        insta::assert_snapshot!(outcomes.join("\n"), @"
        acmeCo/disk/cancelled-empty  the attachment was cancelled (Unavailable)
        acmeCo/disk/cancelled        the attachment was cancelled (Unavailable)
        ");
    }

    /// Wait for the writer to take every live chunk the recording channel holds.
    async fn drains(served_tx: &broker::ServedTx) {
        for _ in 0..1000 {
            if is_drained(served_tx) {
                return;
            }
            () = tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("the writer did not take the live chunks offered to it");
    }

    /// Whether the writer has taken every live chunk the recording channel held.
    fn is_drained(served_tx: &broker::ServedTx) -> bool {
        served_tx.capacity() == served_tx.max_capacity()
    }
}
