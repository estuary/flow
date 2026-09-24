//! The task which appends one tenure's deltas, and the [`Writer`] handle a tenure
//! drives it with.
//!
//! [`Task`] owns the journal for the length of the tenure. It takes each recorded
//! mutation as the device accepts it, has its [`Ledger`] stamp it as a record, and
//! hands it to the appender. [`Writer`] is the handle the tenure's `Prepare` and
//! `Acknowledge` reach it over.
//!
//! An acknowledgement commits a delta, and the client makes it durable in its own
//! store before handing it back, so the writer holds the two halves of that boundary
//! apart. `prepare` finishes and confirms every data append and returns the
//! acknowledgement's exact bytes. `acknowledge` appends those bytes and awaits the
//! broker's confirmation. Between the two the writer takes no mutation, for the
//! reason [`Ledger::taking`] gives.
//!
//! Every decision about a delta is the ledger's. The task carries them out: it
//! appends, flushes, awaits the broker, asks the disk's compactor, and tells the
//! ledger what each of those found.

use super::ledger::{Ledger, Stopped};
use super::{Journal, Promoted};
use crate::device::Compactor;
use crate::proto;
use crate::recording::Recorded;
use anyhow::Context;

impl Promoted {
    /// Begin appending the mutations of `recorded` as they arrive, until the
    /// returned handle is dropped.
    ///
    /// The journal is claimed already. Every append this writer makes checks the
    /// epoch that claim installed, so one which served an unclaimed journal would
    /// fail its very first record.
    ///
    /// `recorded` and `compactor` are the disk's, as `Device::create` handed them
    /// out. This writer opens and completes that disk's horizons.
    pub fn serve(self, recorded: Recorded, compactor: Compactor) -> Writer {
        self.into_task(Some(compactor)).spawn(recorded)
    }

    /// [`Promoted::serve`], for a writer with no disk behind it. It appends what it
    /// is given and compacts nothing, because a compactor needs a real device.
    #[cfg(test)]
    pub fn serve_uncompacted(self, recorded: Recorded) -> Writer {
        self.into_task(None).spawn(recorded)
    }

    /// Build the task which serves this journal, at the start of a delta it has
    /// not yet taken anything into. Nothing runs until [`Promoted::serve`] spawns it.
    pub(super) fn into_task(self, compactor: Option<Compactor>) -> Task {
        let Self {
            journal,
            appender,
            horizon,
        } = self;

        Task {
            ledger: Ledger::new(journal.epoch, horizon),
            appender: Some(appender),
            journal,
            compactor,
        }
    }
}

/// Handle to a tenure's journal writer.
///
/// A writer which fails answers the request which met that failure with it, and
/// refuses every request after that. A tenure ends at its first failure, so the
/// cause reaches a client exactly once.
pub struct Writer {
    commands: tokio::sync::mpsc::Sender<Command>,
    ended: tokio_util::sync::CancellationToken,
    /// Only `broker_test` reads this back. A tenure's epoch decides nothing the
    /// daemon does after the claim installs it, so nothing else asks.
    #[cfg(test)]
    epoch: proto_gazette::uuid::Producer,
}

impl Writer {
    /// Finish and confirm every append of the delta before this cut, then
    /// return the acknowledgement which commits it. The acknowledgement is not
    /// appended.
    ///
    /// The caller must have stopped admitting mutations and awaited those it
    /// admitted, because only then is draining the recording channel a cut.
    ///
    /// A tenure prepares one delta at a time. A `Prepare` which reaches this writer
    /// behind an `Acknowledge` is served after that acknowledgement has landed,
    /// because the writer serves its commands in order, so a client holds at most one
    /// acknowledgement which a broker has not confirmed however it pipelines.
    ///
    /// `None` when the delta is empty. The transaction did not change the disk,
    /// and the client owes no acknowledgement.
    pub async fn prepare(&self) -> anyhow::Result<Option<bytes::Bytes>> {
        self.call(Command::Prepare).await
    }

    /// Append the acknowledgement returned by [`Writer::prepare`], and await the
    /// broker's confirmation of it.
    ///
    /// A delta which completes a recovery horizon also moves the recovery floor
    /// stored on this journal.
    pub async fn acknowledge(&self, ack: bytes::Bytes) -> anyhow::Result<()> {
        self.call(|reply| Command::Acknowledge(ack, reply)).await
    }

    /// Stop appending, and discard every mutation which follows.
    ///
    /// A tenure which is ending prepares nothing more, so what its disk does
    /// on the way out cannot be committed and a replay would ignore it. Those
    /// mutations are still taken, per `Ledger::taking`.
    ///
    /// It cancels rather than asking, and so gives up whatever broker call is in
    /// flight, per `until_ended`. A writer retrying an append would otherwise
    /// never answer.
    pub fn abandon(&self) {
        self.ended.cancel();
    }

    /// Epoch this tenure installs as the journal's author.
    #[cfg(test)]
    pub fn epoch(&self) -> proto_gazette::uuid::Producer {
        self.epoch
    }

    async fn call<T>(&self, command: impl FnOnce(Reply<T>) -> Command) -> anyhow::Result<T> {
        let (reply, response) = tokio::sync::oneshot::channel();

        self.commands
            .send(command(reply))
            .await
            .map_err(|_| anyhow::anyhow!("the journal writer has stopped"))?;

        response
            .await
            .map_err(|_| anyhow::anyhow!("the journal writer has stopped"))?
    }
}

enum Command {
    Prepare(Reply<Option<bytes::Bytes>>),
    Acknowledge(bytes::Bytes, Reply<()>),
}

/// How one request's outcome reaches the client which made it.
///
/// A failure is moved rather than shared, and keeps its own causes: a tenure
/// stream derives its error code by downcasting through them, so a writer which
/// a replacement tenure fenced is reported as fenced.
type Reply<T> = tokio::sync::oneshot::Sender<anyhow::Result<T>>;

/// The task behind a [`Writer`]. One of these serves a journal for the length of
/// its tenure.
///
/// A few of its fields and primitives are `pub(super)` rather than private, because
/// `broker_test` drives them directly: opening a horizon otherwise needs a compactor,
/// and a compactor needs a real device.
pub(super) struct Task {
    pub(super) journal: Journal,
    pub(super) ledger: Ledger,
    /// Present exactly while the ledger appends. It is dropped the moment the tenure
    /// stops appending, which aborts an append RPC that would otherwise go on
    /// retrying for a disk which no longer has a writer.
    appender: Option<publisher::Appender>,
    /// The disk this journal serves. It owns its horizon's bitmap.
    compactor: Option<Compactor>,
}

impl Task {
    fn spawn(self, recorded: Recorded) -> Writer {
        let ended = self.journal.ended.clone();

        #[cfg(test)]
        let epoch = self.journal.epoch;

        let (commands, receiver) = tokio::sync::mpsc::channel(1);
        tokio::spawn(self.run(recorded, receiver));

        Writer {
            commands,
            ended,
            #[cfg(test)]
            epoch,
        }
    }

    async fn run(
        mut self,
        mut recorded: Recorded,
        mut commands: tokio::sync::mpsc::Receiver<Command>,
    ) {
        // Cloned, so that the branch below does not borrow `self` for the
        // length of the select.
        let ended = self.journal.ended.clone();

        loop {
            tokio::select! {
                // Requests come first. A cut must observe every mutation queued
                // before it rather than race them, and `prepare` takes those.
                biased;

                command = commands.recv() => {
                    let Some(command) = command else { return };

                    () = self.command(command, &mut recorded).await;
                }
                // A tenure which has ended appends no more, and its appender is
                // dropped here rather than at the next request: one left running
                // retries a broker it cannot reach for as long as the outage lasts.
                _ = ended.cancelled(), if self.ledger.appending() => {
                    self.ledger.abandon();
                    self.appender = None;
                }
                // One mutation per iteration, so that a disk under sustained
                // write load still serves its requests between them.
                chunks = recorded.recv(), if self.ledger.taking() => match chunks {
                    Some(chunks) => {
                        // No request is waiting on a failure here, so the ledger keeps
                        // it for the next one to take.
                        if let Err(err) = self.take(chunks).await {
                            self.fail(err);
                        }
                    }
                    None => self.ledger.on_drained(),
                },
            }
        }
    }

    /// Serve one request and answer the client which made it.
    ///
    /// A request which fails ends the tenure, and that client is handed the
    /// failure rather than the ledger keeping it: the ledger refuses every later
    /// request without it.
    async fn command(&mut self, command: Command, recorded: &mut Recorded) {
        match command {
            Command::Prepare(reply) => {
                let result = self.prepare(recorded).await;
                _ = reply.send(result.inspect_err(|err| self.stop(err)));
            }
            Command::Acknowledge(ack, reply) => {
                let result = self.acknowledge(ack).await;
                _ = reply.send(result.inspect_err(|err| self.stop(err)));
            }
        }
    }

    /// Finish the delta and construct its acknowledgement, per [`Writer::prepare`].
    async fn prepare(&mut self, recorded: &mut Recorded) -> anyhow::Result<Option<bytes::Bytes>> {
        () = self.ledger.begin_prepare(self.ended())?;

        // Admission is closed and every admitted mutation has been offered, so
        // taking until the queue is empty is the cut. It cannot refill behind
        // this loop, which is why `prepare` may take more than one mutation
        // where `run` takes exactly one.
        while let Ok(chunks) = recorded.try_recv() {
            () = self.take(chunks).await?;
        }
        if self.ledger.delta_is_empty() {
            return Ok(None);
        }
        // Every record of this delta is confirmed before its acknowledgement is
        // built. The appender holds a batch back while it pipelines the one
        // before it, so the cut is not a cut until that batch has landed.
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
    /// this returns, per [`Ledger::taking`].
    async fn acknowledge(&mut self, ack: bytes::Bytes) -> anyhow::Result<()> {
        let completes_horizon = self.ledger.begin_acknowledge(self.ended(), &ack)?;

        let (journal, appender) = self.writing();
        () = journal.append_ack(appender, &ack).await?;

        if !completes_horizon {
            return Ok(());
        }
        let floor = self.ledger.complete_horizon();
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

    /// Have the ledger stamp one mutation as this delta's next record, and hand it
    /// to the appender, which decides when a batch of them becomes an append.
    ///
    /// `opens` asks that a delta's first record open a recovery horizon, and is
    /// ignored for any other. A horizon is an offset, so that record is appended
    /// alone and confirmed — the appender is drained before it and flushed after —
    /// and the broker's own `begin` for that append is the horizon.
    pub(super) async fn append_mutation(
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
            .stamp_mutation(std::time::SystemTime::now(), chunks, opens);

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
            .context("awaiting the append which opened a horizon")?;
        let begin = response
            .commit
            .context("the append which opened a horizon reported no committed fragment")?
            .begin;

        self.ledger.on_horizon_opened(begin);
        Ok(())
    }

    /// End the tenure over `err`, which a request is told of. It is logged here, and
    /// the appender goes with the tenure whatever failed: an owner or protocol error
    /// ends the tenure just as a broker one does, and an append RPC left running
    /// would go on retrying for a disk which has no writer.
    fn stop(&mut self, err: &anyhow::Error) {
        match self.ledger.stop(self.ended()) {
            Stopped::AlreadyOver => return,
            Stopped::Ended => {
                tracing::debug!(journal = self.journal.name, ?err, "journal writer stopped")
            }
            Stopped::Failed => {
                tracing::error!(journal = self.journal.name, ?err, "journal writer failed")
            }
        }
        self.appender = None;
    }

    /// [`Task::stop`] over a failure no request is waiting on, which the ledger keeps
    /// for the next request to take.
    fn fail(&mut self, err: anyhow::Error) {
        () = self.stop(&err);
        self.ledger.keep(err);
    }

    /// Whether the tenure's own cancellation has fired. A drain cancels it under a
    /// request which is already in flight, ahead of `run` abandoning the ledger.
    fn ended(&self) -> bool {
        self.journal.ended.is_cancelled()
    }

    /// Whether mutations are appended rather than discarded.
    ///
    /// A cancelled token answers this before `run` has abandoned the ledger. The two
    /// differ only there: `run` selects on the token ahead of the recording channel,
    /// so the ledger is never stale when a mutation arrives.
    fn appending(&self) -> bool {
        self.ledger.appending() && !self.ended()
    }

    /// The journal and appender of a tenure which is still appending.
    ///
    /// Every caller has passed a check of the ledger or [`Task::appending`], and
    /// nothing stops the ledger while one of `run`'s own branches is running.
    fn writing(&mut self) -> (&mut Journal, &mut publisher::Appender) {
        let Self {
            journal, appender, ..
        } = self;

        (journal, appender.as_mut().expect("the tenure is appending"))
    }

    /// Append everything the appender holds and await the broker's confirmation.
    pub(super) async fn flush(&mut self) -> anyhow::Result<()> {
        let (journal, appender) = self.writing();

        journal.flush(appender).await
    }
}
