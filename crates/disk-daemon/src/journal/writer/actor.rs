use super::ledger;
use crate::device;
use crate::journal;
use crate::proto;
use anyhow::Context;

/// Background task serving one journal for a [`Writer`](super::Writer)'s tenure.
pub(in crate::journal) struct WriterActor {
    /// Journal identity, ownership, and recovery offsets.
    journal: journal::Journal,
    /// Delta sequencing and commit state.
    ledger: ledger::Ledger,
    /// Dropped when appending stops, aborting any background retry loop.
    appender: Option<publisher::Appender>,
    /// The disk this journal serves. It owns its horizon's bitmap.
    compactor: Option<device::Compactor>,
}

impl WriterActor {
    /// The task which serves `journal`, at the start of a delta it has not yet taken
    /// anything into, resuming the `horizon` its replay left open. Nothing runs until
    /// [`WriterActor::spawn`].
    pub(in crate::journal) fn new(
        journal: journal::Journal,
        appender: publisher::Appender,
        horizon: Option<i64>,
        compactor: Option<device::Compactor>,
    ) -> Self {
        Self {
            ledger: ledger::Ledger::new(journal.author, horizon),
            appender: Some(appender),
            journal,
            compactor,
        }
    }

    /// Begin appending the mutations of `recorded` as they arrive, until the
    /// returned handle is dropped.
    pub(in crate::journal) fn spawn(self, recorded: device::Recorded) -> super::Writer {
        let ended = self.journal.ended.clone();

        #[cfg(test)]
        let author = self.journal.author;

        let (commands, receiver) = tokio::sync::mpsc::channel(1);
        tokio::spawn(self.run(recorded, receiver));

        super::Writer {
            commands,
            ended,
            #[cfg(test)]
            author,
        }
    }

    async fn run(
        mut self,
        mut recorded: device::Recorded,
        mut commands: tokio::sync::mpsc::Receiver<super::Command>,
    ) {
        let ended = self.journal.ended.clone();
        // Set once the device has released its half of the recording channel, which
        // then has nothing more to take.
        let mut drained = false;

        loop {
            tokio::select! {
                // Requests come first. A cut must observe every mutation queued
                // before it rather than race them, and `prepare` takes those.
                biased;

                command = commands.recv() => {
                    let Some(command) = command else { return };

                    () = self.command(command, &mut recorded).await;
                }
                // Drop promptly so background retries stop even if no request follows.
                _ = ended.cancelled(), if self.appender.is_some() => {
                    self.appender = None;
                    self.ledger.end(None);
                }
                // One mutation per iteration, so that a disk under sustained
                // write load still serves its requests between them.
                chunks = recorded.recv(), if !drained && self.ledger.taking() => match chunks {
                    Some(chunks) => {
                        // No request is waiting on a failure here, so the ledger keeps
                        // it for the next one to take.
                        if let Err(err) = self.take(chunks).await
                            && self.ending(&err)
                        {
                            self.ledger.end(Some(err));
                        }
                    }
                    None => drained = true,
                },
            }
        }
    }

    /// Serve one request and answer the client which made it.
    ///
    /// A request which fails ends the tenure, and that client is handed the
    /// failure rather than the ledger keeping it: the ledger refuses every later
    /// request without it.
    async fn command(&mut self, command: super::Command, recorded: &mut device::Recorded) {
        match command {
            super::Command::Prepare(reply) => {
                let result = self.prepare(recorded).await;
                _ = reply.send(result.inspect_err(|err| self.stop(err)));
            }
            super::Command::Acknowledge(ack, reply) => {
                let result = self.acknowledge(ack).await;
                _ = reply.send(result.inspect_err(|err| self.stop(err)));
            }
        }
    }

    /// Finish the delta and construct its acknowledgement, per
    /// [`Writer::prepare`](super::Writer::prepare).
    async fn prepare(
        &mut self,
        recorded: &mut device::Recorded,
    ) -> anyhow::Result<Option<bytes::Bytes>> {
        () = self.ledger.begin_prepare(!self.appending())?;

        // Admission is closed, so the queue cannot refill. Drain it to finish
        // the cut without the per-iteration limit used by `run`.
        while let Ok(chunks) = recorded.try_recv() {
            () = self.take(chunks).await?;
        }
        if self.ledger.delta_is_empty() {
            return Ok(None);
        }
        // Confirm the buffered batch as well as the append in flight before
        // building the acknowledgement.
        () = self.flush().await?;

        let completes_horizon = match &self.compactor {
            Some(compactor) if self.ledger.horizon().is_some() => compactor.pending().await? == 0,
            _ => false,
        };
        Ok(Some(
            self.ledger
                .cut(std::time::SystemTime::now(), completes_horizon),
        ))
    }

    /// Append `ack`, which commits the delta this tenure prepared, and store the
    /// recovery floor it established, if any. The writer takes mutations again once
    /// this returns, per [`Ledger::taking`](ledger::Ledger::taking).
    async fn acknowledge(&mut self, ack: bytes::Bytes) -> anyhow::Result<()> {
        let floor = self.ledger.begin_acknowledge(!self.appending(), &ack)?;

        let (journal, appender) = self.writing();
        () = journal.append_ack(appender, &ack).await?;

        let Some(floor) = floor else {
            // The disk has committed state now. A journal whose floor label says so is
            // one a later promotion refuses to format, should its fragments be lost.
            if !self.journal.marked {
                () = self.journal.store_floor(0).await;
            }
            return Ok(());
        };
        self.journal.floor = floor;

        if let Some(compactor) = &self.compactor {
            () = compactor.close()?;
        }
        tracing::info!(
            journal = self.journal.name,
            floor,
            head = self.journal.head,
            "completed a recovery horizon",
        );
        () = self.journal.store_floor(floor).await;

        Ok(())
    }

    /// Take one mutation into this delta.
    async fn take(&mut self, chunks: Vec<proto::Chunk>) -> anyhow::Result<()> {
        if !self.appending() {
            return Ok(()); // A tenure which is over commits nothing more.
        }
        let range = self
            .ledger
            .horizon_range(self.journal.head, self.journal.floor);

        let opens = match (&self.compactor, range) {
            // The compactor judges the range against the disk's live allocated size,
            // which only it knows.
            (Some(compactor), Some(range)) => compactor.open(range).await?,
            _ => false,
        };
        self.append_mutation(chunks, opens).await
    }

    /// Append one mutation. If it opens a horizon, flush it alone and use the
    /// broker's `begin` as the horizon offset; batching or retries make local
    /// byte counts insufficient.
    async fn append_mutation(
        &mut self,
        chunks: Vec<proto::Chunk>,
        opens: bool,
    ) -> anyhow::Result<()> {
        let opens = opens && self.ledger.delta_is_empty();

        if opens {
            () = self.flush().await?;
        }
        let record = self
            .ledger
            .build_uncommitted(std::time::SystemTime::now(), chunks, opens);

        let (journal, appender) = self.writing();
        () = journal.append_record(appender, &record).await?;

        if !opens {
            return Ok(());
        }
        // Taken before the flush which lands the record, because the barrier is
        // satisfied by the appends that flush starts.
        let opened = {
            let (_journal, appender) = self.writing();
            appender.barrier()
        };
        () = self.flush().await?;

        let response = opened
            .await
            .map_err(crate::journal::append_failure)
            .context("awaiting the append which opened a horizon")?;
        let begin = response
            .commit
            .context("the append which opened a horizon reported no committed fragment")?
            .begin;

        self.ledger.on_horizon_opened(begin);
        Ok(())
    }

    /// End the tenure over `err`, which the request it failed is told of. `run` ends it
    /// over a failure no request is waiting on, which the ledger keeps instead.
    fn stop(&mut self, err: &anyhow::Error) {
        if self.ending(err) {
            self.ledger.end(None);
        }
    }

    /// Drop the appender on any failure. Return whether this newly ended the
    /// tenure, excluding cancellation by its own teardown.
    fn ending(&mut self, err: &anyhow::Error) -> bool {
        if self.appender.take().is_none() {
            return false;
        }
        if self.ended() {
            tracing::debug!(journal = self.journal.name, ?err, "journal writer stopped");
        } else {
            tracing::error!(journal = self.journal.name, ?err, "journal writer failed");
        }
        true
    }

    /// Whether the tenure's own cancellation has fired. A drain cancels it under a
    /// request which is already in flight, ahead of `run` dropping the appender.
    fn ended(&self) -> bool {
        self.journal.ended.is_cancelled()
    }

    /// Check cancellation as well as the appender: a request in flight may see
    /// the token before `run` handles it and drops the appender.
    fn appending(&self) -> bool {
        self.appender.is_some() && !self.ended()
    }

    /// The journal and appender of a tenure which is still appending.
    ///
    /// Every caller has passed a check of the ledger or [`WriterActor::appending`], and
    /// nothing stops the ledger while one of `run`'s own branches is running.
    fn writing(&mut self) -> (&mut journal::Journal, &mut publisher::Appender) {
        let Self {
            journal, appender, ..
        } = self;

        (journal, appender.as_mut().expect("the tenure is appending"))
    }

    /// Append everything the appender holds and await the broker's confirmation.
    async fn flush(&mut self) -> anyhow::Result<()> {
        let (journal, appender) = self.writing();

        journal.flush(appender).await
    }
}

#[cfg(test)]
mod broker_test {
    use crate::BLOCK_SIZE;
    use crate::chunk;
    use crate::device::recording;
    use crate::proto;
    use crate::test_support::broker;
    use proto_gazette::uuid;

    #[tokio::test]
    async fn test_a_writer_over_a_real_broker() {
        let fixture = broker::Fixture::start().await;

        a_committed_delta_reads_back_as_its_chunks(&fixture).await;
        mutations_offered_while_a_commit_is_outstanding_wait_for_it(&fixture).await;
        a_large_delta_carries_one_record_per_mutation(&fixture).await;
        a_delta_which_spans_several_appends_keeps_its_records(&fixture).await;
        a_horizon_records_its_openers_own_offset(&fixture).await;
        an_abandoned_tenure_answers_its_client(&fixture).await;
        a_first_commit_marks_the_floor_label(&fixture).await;

        fixture.stop().await;
    }

    /// A committed delta reads back as exactly the chunks which were recorded.
    async fn a_committed_delta_reads_back_as_its_chunks(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/delta";
        let (mut recorder, writer) = fixture.open(journal).await.unwrap();

        let mutations = vec![
            chunk::encode_write(0, &bytes::Bytes::from(vec![0x11; 8192])),
            chunk::encode_write(2, &bytes::Bytes::from(vec![0; 4096])),
            vec![chunk::encode_punch(3, 4)],
        ];
        for mutation in &mutations {
            recorder.reserve().unwrap().send(mutation.clone());
        }

        let ack = writer.prepare().await.unwrap().unwrap();
        () = writer.acknowledge(ack.clone()).await.unwrap();

        let records = render(&fixture.read(journal).await, writer.author());

        // A second commit is a protocol violation. The delta it acknowledged is
        // already committed.
        let err = writer.acknowledge(ack).await.unwrap_err();

        insta::assert_snapshot!(format!("{records}\n\nacknowledged again: {err:#}"), @"
        fence    other producer
        continue author          blocks 0..2: data len 8192 [0x11x8192]
        continue author          blocks 2..3: empty data (one zeroed block)
        continue author          blocks 3..7: punch 4
        ack      author

        acknowledged again: no prepared delta is awaiting a commit (FailedPrecondition)
        ");
    }

    /// The second delta must follow the first delta's acknowledgement; see
    /// [`Ledger::taking`](super::ledger::Ledger::taking).
    async fn mutations_offered_while_a_commit_is_outstanding_wait_for_it(
        fixture: &broker::Fixture,
    ) {
        let journal = "acmeCo/disk/outstanding";
        let (mut recorder, writer) = fixture.open(journal).await.unwrap();

        let author = writer.author();

        recorder.reserve().unwrap().send(broker::write(1, 0xaa));
        let first = writer.prepare().await.unwrap().unwrap();

        // Mutations of the second delta, offered while the client holds the first
        // delta's acknowledgement. The writer leaves them in the channel.
        recorder.reserve().unwrap().send(broker::write(2, 0xbb));
        recorder.reserve().unwrap().send(broker::write(3, 0xcc));
        () = tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !recorder.is_empty(),
            "the writer took a mutation while an acknowledgement was outstanding",
        );

        () = writer.acknowledge(first).await.unwrap();
        () = drains(&recorder).await;

        let second = writer.prepare().await.unwrap().unwrap();
        () = writer.acknowledge(second).await.unwrap();
        drop((recorder, writer));

        let records = render(&fixture.read(journal).await, author);

        let (_recorder, _writer, blocks) = fixture.recover(journal, None).await.unwrap();

        insta::assert_snapshot!(format!("{records}\n\nrecovered blocks {}", render_blocks(&blocks)), @"
        fence    other producer
        continue author          blocks 1..2: data len 4096 [0xaax4096]
        ack      author
        continue author          blocks 2..3: data len 4096 [0xbbx4096]
        continue author          blocks 3..4: data len 4096 [0xccx4096]
        ack      author

        recovered blocks 1..2 filled 0xaa, 2..3 filled 0xbb, 3..4 filled 0xcc
        ");
    }

    /// A delta of many mutations carries exactly one record per mutation. Those records
    /// cover exactly the blocks the mutations wrote.
    async fn a_large_delta_carries_one_record_per_mutation(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/bounded";
        const WRITES: usize = 8;

        let (mut recorder, writer) = fixture.open(journal).await.unwrap();
        let write = chunk::encode_write(0, &bytes::Bytes::from(vec![0x22; 128 * 1024]));

        for _ in 0..WRITES {
            recorder.reserve().unwrap().send(write.clone());
        }

        let ack = writer.prepare().await.unwrap().unwrap();
        () = writer.acknowledge(ack).await.unwrap();

        // Nothing splits a mutation, so each write is exactly one record.
        let records = render(&fixture.read(journal).await, writer.author());

        insta::assert_snapshot!(records, @"
        fence    other producer
        continue author          blocks 0..32: data len 131072 [0x22x131072]
        continue author          blocks 0..32: data len 131072 [0x22x131072]
        continue author          blocks 0..32: data len 131072 [0x22x131072]
        continue author          blocks 0..32: data len 131072 [0x22x131072]
        continue author          blocks 0..32: data len 131072 [0x22x131072]
        continue author          blocks 0..32: data len 131072 [0x22x131072]
        continue author          blocks 0..32: data len 131072 [0x22x131072]
        continue author          blocks 0..32: data len 131072 [0x22x131072]
        ack      author
        ");
    }

    /// A delta larger than the appender's buffer threshold is appended in several
    /// RPCs, and the boundary between them changes nothing of what the journal
    /// holds: one record per mutation, in the order they were recorded, and a
    /// recovery which rebuilds the disk from them.
    async fn a_delta_which_spans_several_appends_keeps_its_records(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/batched";

        // Enough of the disk, enough times over, to carry the delta past the
        // threshold at which the appender stops buffering and waits.
        const MUTATIONS: usize = 40;
        let each = crate::test_support::broker::BLOCKS as usize * BLOCK_SIZE as usize;
        assert!(MUTATIONS * each > publisher::Appender::BUFFER_FLUSH_THRESHOLD);

        let (mut recorder, writer) = fixture.open(journal).await.unwrap();

        for index in 0..MUTATIONS {
            let fill = (index + 1) as u8;
            recorder.reserve().unwrap().send(chunk::encode_write(
                0,
                &bytes::Bytes::from(vec![fill; each]),
            ));
        }

        let ack = writer.prepare().await.unwrap().unwrap();
        () = writer.acknowledge(ack).await.unwrap();
        let author = writer.author();
        drop((recorder, writer));

        // Distinct fill bytes expose any split, merge, or reordering across appends.
        let records = render(&fixture.read(journal).await, author);

        // The journal is past the threshold, so more than one append built it.
        assert!(fixture.head(journal).await as usize > publisher::Appender::BUFFER_FLUSH_THRESHOLD);

        let (_recorder, _writer, blocks) = fixture.recover(journal, None).await.unwrap();

        insta::assert_snapshot!(format!("{records}\n\nrecovered blocks {}", render_blocks(&blocks)), @"
        fence    other producer
        continue author          blocks 0..64: data len 262144 [0x01x262144]
        continue author          blocks 0..64: data len 262144 [0x02x262144]
        continue author          blocks 0..64: data len 262144 [0x03x262144]
        continue author          blocks 0..64: data len 262144 [0x04x262144]
        continue author          blocks 0..64: data len 262144 [0x05x262144]
        continue author          blocks 0..64: data len 262144 [0x06x262144]
        continue author          blocks 0..64: data len 262144 [0x07x262144]
        continue author          blocks 0..64: data len 262144 [0x08x262144]
        continue author          blocks 0..64: data len 262144 [0x09x262144]
        continue author          blocks 0..64: data len 262144 [0x0ax262144]
        continue author          blocks 0..64: data len 262144 [0x0bx262144]
        continue author          blocks 0..64: data len 262144 [0x0cx262144]
        continue author          blocks 0..64: data len 262144 [0x0dx262144]
        continue author          blocks 0..64: data len 262144 [0x0ex262144]
        continue author          blocks 0..64: data len 262144 [0x0fx262144]
        continue author          blocks 0..64: data len 262144 [0x10x262144]
        continue author          blocks 0..64: data len 262144 [0x11x262144]
        continue author          blocks 0..64: data len 262144 [0x12x262144]
        continue author          blocks 0..64: data len 262144 [0x13x262144]
        continue author          blocks 0..64: data len 262144 [0x14x262144]
        continue author          blocks 0..64: data len 262144 [0x15x262144]
        continue author          blocks 0..64: data len 262144 [0x16x262144]
        continue author          blocks 0..64: data len 262144 [0x17x262144]
        continue author          blocks 0..64: data len 262144 [0x18x262144]
        continue author          blocks 0..64: data len 262144 [0x19x262144]
        continue author          blocks 0..64: data len 262144 [0x1ax262144]
        continue author          blocks 0..64: data len 262144 [0x1bx262144]
        continue author          blocks 0..64: data len 262144 [0x1cx262144]
        continue author          blocks 0..64: data len 262144 [0x1dx262144]
        continue author          blocks 0..64: data len 262144 [0x1ex262144]
        continue author          blocks 0..64: data len 262144 [0x1fx262144]
        continue author          blocks 0..64: data len 262144 [0x20x262144]
        continue author          blocks 0..64: data len 262144 [0x21x262144]
        continue author          blocks 0..64: data len 262144 [0x22x262144]
        continue author          blocks 0..64: data len 262144 [0x23x262144]
        continue author          blocks 0..64: data len 262144 [0x24x262144]
        continue author          blocks 0..64: data len 262144 [0x25x262144]
        continue author          blocks 0..64: data len 262144 [0x26x262144]
        continue author          blocks 0..64: data len 262144 [0x27x262144]
        continue author          blocks 0..64: data len 262144 [0x28x262144]
        ack      author

        recovered blocks 0..64 filled 0x28
        ");
    }

    /// A horizon's offset is where its opening record begins, and stays exact
    /// where a delta is batched into several appends: the opening record is
    /// appended alone, so the broker's own `begin` for it is the horizon.
    ///
    /// The writer's primitives are driven directly, because opening a horizon
    /// otherwise needs a compactor, and a compactor needs a real device.
    async fn a_horizon_records_its_openers_own_offset(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/horizon-offset";

        // Every append checks the author the claim installs, so the journal is promoted
        // as a tenure's is even though the scenario drives the writer's primitives
        // directly.
        let (promoted, _blocks) = fixture.promote(journal, None).await.unwrap();

        // The writer's actor, but not spawned: this drives its primitives itself.
        let mut actor = promoted.into_actor(None);

        let each = crate::test_support::broker::BLOCKS as usize * BLOCK_SIZE as usize;
        let write = || chunk::encode_write(0, &bytes::Bytes::from(vec![0x55; each]));

        // Records ahead of the horizon, batched by the appender.
        for _ in 0..8 {
            () = actor.append_mutation(write(), false).await.unwrap();
        }
        () = actor.flush().await.unwrap();

        // Cut, so that the next record is a delta's first and opens a horizon. The
        // cut's acknowledgement need not reach the journal for what this checks.
        let ack = actor.ledger.cut(std::time::SystemTime::now(), false);
        _ = actor.ledger.begin_acknowledge(false, &ack).unwrap();

        () = actor.append_mutation(write(), true).await.unwrap();
        let horizon = actor.ledger.horizon().expect("a horizon was opened");

        // More records behind it, which must not move what it recorded.
        for _ in 0..8 {
            () = actor.append_mutation(write(), false).await.unwrap();
        }
        () = actor.flush().await.unwrap();

        // The offset names the record which carries the flag, and a replay from
        // it reads that record first, then every record behind it.
        let records = render(
            &fixture.read_from(journal, horizon).await,
            actor.journal.author,
        );

        insta::assert_snapshot!(records, @"
        continue author          blocks 0..64: data len 262144 [0x55x262144]  opens a horizon
        continue author          blocks 0..64: data len 262144 [0x55x262144]
        continue author          blocks 0..64: data len 262144 [0x55x262144]
        continue author          blocks 0..64: data len 262144 [0x55x262144]
        continue author          blocks 0..64: data len 262144 [0x55x262144]
        continue author          blocks 0..64: data len 262144 [0x55x262144]
        continue author          blocks 0..64: data len 262144 [0x55x262144]
        continue author          blocks 0..64: data len 262144 [0x55x262144]
        continue author          blocks 0..64: data len 262144 [0x55x262144]
        ");
    }

    /// A tenure which is abandoned commits nothing more, and says so at once
    /// rather than waiting on a broker. Its appender is dropped with it, so no
    /// append RPC of that tenure outlives it.
    async fn an_abandoned_tenure_answers_its_client(fixture: &broker::Fixture) {
        let mut outcomes = Vec::new();

        for (journal, has_delta) in [
            ("acmeCo/disk/abandoned-empty", false),
            ("acmeCo/disk/abandoned", true),
        ] {
            let (mut recorder, writer) = fixture.open(journal).await.unwrap();

            if has_delta {
                recorder.reserve().unwrap().send(broker::write(6, 0x33));
                () = drains(&recorder).await;
            }
            () = writer.abandon();

            // Let the writer drop its appender and drain an unmount mutation
            // before a request arrives.
            recorder.reserve().unwrap().send(broker::write(7, 0x44));
            () = drains(&recorder).await;

            let err = tokio::time::timeout(std::time::Duration::from_secs(10), writer.prepare())
                .await
                .expect("an abandoned tenure answers rather than waiting")
                .expect_err("an abandoned tenure commits nothing");

            outcomes.push(format!("{journal:<29}{err:#}"));
        }

        insta::assert_snapshot!(outcomes.join("\n"), @"
        acmeCo/disk/abandoned-empty  the tenure is over
        acmeCo/disk/abandoned        the tenure is over
        ");
    }

    /// A disk's first commit marks its journal's recovery-floor label, which is how a
    /// later promotion knows the disk holds committed state.
    async fn a_first_commit_marks_the_floor_label(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/marked";
        let (mut recorder, writer) = fixture.open(journal).await.unwrap();
        let mut labels = vec![("opened", fixture.floor_label(journal).await)];

        recorder.reserve().unwrap().send(broker::write(1, 0xaa));
        let ack = writer.prepare().await.unwrap().unwrap();
        labels.push((
            "prepared, not yet committed",
            fixture.floor_label(journal).await,
        ));

        () = writer.acknowledge(ack).await.unwrap();
        labels.push(("acknowledged", fixture.floor_label(journal).await));

        // A commit without a completed horizon marks the journal with floor zero.
        let trace: Vec<_> = labels
            .iter()
            .map(|(step, label)| format!("{step:<29}{label:?}"))
            .collect();

        insta::assert_snapshot!(trace.join("\n"), @r#"
        opened                       None
        prepared, not yet committed  None
        acknowledged                 Some("0000000000000000")
        "#);
    }

    /// Render record kinds, author roles, and chunks. Omit random producer IDs
    /// and wall-clock values to keep snapshots stable.
    fn render(
        records: &[(broker::UuidParts, proto::DiskRecord)],
        author: uuid::Producer,
    ) -> String {
        records
            .iter()
            .map(|((producer, _clock, flags), record)| {
                let kind = match (flags.is_outside(), flags.is_continue()) {
                    (true, _) => "fence",
                    (_, true) => "continue",
                    _ => "ack",
                };
                let producer = if *producer == author {
                    "author"
                } else {
                    "other producer"
                };
                let chunks = record
                    .chunks
                    .iter()
                    .map(crate::test_support::render_chunk)
                    .collect::<Vec<_>>()
                    .join("; ");
                let opens = if record.opens_horizon {
                    "  opens a horizon"
                } else {
                    ""
                };

                format!("{kind:<9}{producer:<16}{chunks}{opens}")
                    .trim_end()
                    .to_string()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Render the blocks a recovery left allocated as runs of consecutive blocks
    /// holding one fill byte.
    fn render_blocks(blocks: &[(u32, u8)]) -> String {
        let mut runs: Vec<(std::ops::Range<u32>, u8)> = Vec::new();
        for &(block, fill) in blocks {
            match runs.last_mut() {
                Some((range, last)) if range.end == block && *last == fill => range.end += 1,
                _ => runs.push((block..block + 1, fill)),
            }
        }
        runs.iter()
            .map(|(range, fill)| format!("{}..{} filled {fill:#04x}", range.start, range.end))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Wait for the writer to take every mutation the recording channel holds.
    ///
    /// The writer takes on a task of its own, so a scenario which asserts what it did
    /// with a mutation must first see that it has one.
    async fn drains(recorder: &recording::Recorder) {
        for _ in 0..1000 {
            if recorder.is_empty() {
                return;
            }
            () = tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("the writer did not take the mutations offered to it");
    }
}
