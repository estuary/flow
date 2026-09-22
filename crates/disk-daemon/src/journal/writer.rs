//! The actor which appends one tenure's deltas, and the handle a tenure drives it
//! with.
//!
//! [`Task`] owns the journal for the length of the tenure. It takes each captured
//! mutation as the device accepts it, stamps it as a record, and hands it to the
//! appender. [`Writer`] is the handle the tenure's `Prepare` and `Acknowledge`
//! reach it over.
//!
//! An acknowledgement commits a delta, and the client makes it durable in its own
//! store before handing it back, so the writer holds the two halves of that boundary
//! apart. `prepare` finishes and confirms every data append and returns the
//! acknowledgement's exact bytes. `acknowledge` appends those bytes and awaits the
//! broker's confirmation. Between the two the writer takes no mutation, for the
//! reason [`Task::taking`] gives.

use super::{Journal, Promoted, uuid_bytes};
use crate::capture::Captured;
use crate::device::Compactor;
use crate::failure;
use crate::proto;
use anyhow::Context;
use proto_gazette::uuid;

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
    epoch: uuid::Producer,
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

impl Promoted {
    /// Build the actor which serves this journal, at the start of a delta it has
    /// not yet taken anything into. Nothing runs until [`Promoted::serve`] spawns it.
    pub(super) fn into_task(self, compactor: Option<Compactor>) -> Task {
        let Self {
            journal,
            appender,
            horizon,
        } = self;

        Task {
            journal,
            phase: Phase::Appending {
                appender,
                prepared: None,
            },
            horizon,
            clock: uuid::Clock::zero(),
            delta_records: 0,
            compactor,
            drained: false,
        }
    }

    /// Begin appending the mutations of `captured` as they arrive, until the
    /// returned handle is dropped.
    ///
    /// The journal is claimed already. Every append this writer makes checks the
    /// epoch that claim installed, so one which served an unclaimed journal would
    /// fail its very first record.
    ///
    /// `compactor` is the disk whose horizons this writer opens and completes. A
    /// writer without one appends what it is given and compacts nothing. This
    /// crate's own tests build such a writer.
    pub fn serve(self, captured: Captured, compactor: Option<Compactor>) -> Writer {
        let task = self.into_task(compactor);
        let ended = task.journal.ended.clone();

        #[cfg(test)]
        let epoch = task.journal.epoch;

        let (commands, receiver) = tokio::sync::mpsc::channel(1);
        tokio::spawn(task.run(captured, receiver));

        Writer {
            commands,
            ended,
            #[cfg(test)]
            epoch,
        }
    }
}

impl Writer {
    /// Epoch this tenure installs as the journal's author.
    #[cfg(test)]
    pub fn epoch(&self) -> uuid::Producer {
        self.epoch
    }

    /// Finish and confirm every append of the delta before this cut, then
    /// return the acknowledgement which commits it. The acknowledgement is not
    /// appended.
    ///
    /// The caller must have stopped admitting mutations and awaited those it
    /// admitted, because only then is draining the capture channel a cut.
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
    /// mutations are still taken, per `Task::taking`.
    ///
    /// It cancels rather than asking, and so gives up whatever broker call is in
    /// flight, per `until_ended`. A writer retrying an append would otherwise
    /// never answer.
    pub fn abandon(&self) {
        self.ended.cancel();
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

/// The actor. One of these serves a journal for the length of its tenure.
///
/// A few of its fields and primitives are `pub(super)` rather than private, because
/// `broker_test` drives them directly: opening a horizon otherwise needs a compactor,
/// and a compactor needs a real device.
pub(super) struct Task {
    pub(super) journal: Journal,
    /// Whether this tenure still appends, and what it owes if it does.
    phase: Phase,
    /// Offset of the horizon this tenure opened or resumed. It is not yet
    /// complete.
    pub(super) horizon: Option<i64>,
    clock: uuid::Clock,
    /// Records appended into the delta which is accumulating now. It returns to
    /// zero at each cut, which is where one delta ends and the next begins.
    pub(super) delta_records: usize,
    /// The disk this journal serves. It owns its horizon's bitmap.
    compactor: Option<Compactor>,
    /// Set once the owner has released its half of the capture channel. It is
    /// orthogonal to the phase: a tenure which still appends may have no device
    /// left to append for.
    drained: bool,
}

/// Whether a tenure still appends, and what it owes if it does.
///
/// A tenure begins [`Phase::Appending`] and leaves it once, in one direction. What
/// leaves it drops the appender, which aborts an append RPC that would otherwise go
/// on retrying for a disk which no longer has a writer. Mutations are still taken
/// afterwards, per [`Task::taking`], and discarded instead of appended.
enum Phase {
    /// Appending under the claim this tenure installed. `prepared` is the delta
    /// which was cut and whose acknowledgement the client holds, and while it is
    /// `Some` the writer takes nothing — see [`Task::taking`].
    Appending {
        appender: publisher::Appender,
        prepared: Option<Prepared>,
    },
    /// [`Writer::abandon`] ended the tenure. It did not fail, so every request is
    /// refused for the tenure being over rather than for a failure.
    ///
    /// A delta this tenure had prepared goes with the appender. Nothing can commit
    /// it: an `Acknowledge` of it is refused here exactly as a `Prepare` is.
    Abandoned,
    /// A failure ended the tenure. It is kept here when no request was waiting on
    /// it, for the next request to take, and reported to that request alone.
    Failed(Option<anyhow::Error>),
}

/// A delta which was cut, and whose acknowledgement its client holds.
struct Prepared {
    /// Exact bytes returned to the client, which its `Acknowledge` must repeat.
    ack: bytes::Bytes,
    /// Whether the cut found the open horizon discharged. Committing this delta
    /// then completes that horizon and moves the recovery floor to it.
    completes_horizon: bool,
}

impl Task {
    async fn run(
        mut self,
        mut captured: Captured,
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

                    () = self.command(command, &mut captured).await;
                }
                // A tenure which has ended appends no more, and its appender is
                // dropped here rather than at the next request: one left running
                // retries a broker it cannot reach for as long as the outage lasts.
                _ = ended.cancelled(), if matches!(self.phase, Phase::Appending { .. }) => {
                    self.phase = Phase::Abandoned;
                }
                // One mutation per iteration, so that a disk under sustained
                // write load still serves its requests between them.
                chunks = captured.recv(), if self.taking() => match chunks {
                    Some(chunks) => {
                        // No request is waiting on a failure here, so `fail` keeps
                        // it for the next one to take.
                        if let Err(err) = self.capture(chunks).await {
                            self.fail(err);
                        }
                    }
                    None => self.drained = true,
                },
            }
        }
    }

    /// Whether mutations are taken from the capture channel.
    ///
    /// Taking stops while a prepared delta awaits its commit. Every record carries
    /// the tenure's epoch as its producer, and Gazette sequences per producer: an
    /// `ACK_TXN` commits the pending records of its producer with clocks at or below
    /// its own and drops the rest, and the acknowledgement's clock was fixed at the
    /// cut. A record of the next delta which reached the journal ahead of it would
    /// be lost, so none is taken until it has landed. This is the rule the Gazette
    /// consumer framework applies between one transaction's `StartCommit` and the
    /// last transaction's pending acknowledgement.
    ///
    /// Meanwhile the device parks after a queue depth of mutations, so a workload
    /// which writes heavily across a boundary waits for `Acknowledge`, and a client
    /// keeps that interval short.
    ///
    /// A tenure which appends no more keeps taking, because an unmount writes and a
    /// device whose mutations nothing takes cannot be unmounted.
    fn taking(&self) -> bool {
        !self.drained
            && match &self.phase {
                Phase::Appending { prepared, .. } => prepared.is_none(),
                Phase::Abandoned | Phase::Failed(_) => true,
            }
    }

    /// Whether mutations are appended rather than discarded.
    ///
    /// A cancelled token answers this before `run` has moved the phase, because a
    /// drain cancels it under a request which is already in flight. The two differ
    /// only there: `run` selects on the token ahead of the capture channel, so the
    /// phase is never stale when a mutation arrives.
    fn appending(&self) -> bool {
        matches!(self.phase, Phase::Appending { .. }) && !self.journal.ended.is_cancelled()
    }

    /// The journal and appender of a tenure which is still appending.
    ///
    /// Every caller has passed [`Task::check`] or [`Task::appending`], and nothing
    /// moves the phase while one of `run`'s own branches is running.
    fn writing(&mut self) -> (&mut Journal, &mut publisher::Appender) {
        let Self { journal, phase, .. } = self;

        let Phase::Appending { appender, .. } = phase else {
            panic!("the tenure is appending");
        };
        (journal, appender)
    }

    /// Append everything the appender holds and await the broker's confirmation.
    pub(super) async fn flush(&mut self) -> anyhow::Result<()> {
        let (journal, appender) = self.writing();

        journal.flush(appender).await
    }

    /// Serve one request and answer the client which made it.
    ///
    /// A request which fails ends the tenure, and that client is handed the
    /// failure rather than the writer keeping it: [`Task::check`] refuses every
    /// later request without it.
    async fn command(&mut self, command: Command, captured: &mut Captured) {
        match command {
            Command::Prepare(reply) => {
                let result = self.prepare(captured).await;
                _ = reply.send(result.inspect_err(|err| self.stop(err)));
            }
            Command::Acknowledge(ack, reply) => {
                let result = self.acknowledge(ack).await;
                _ = reply.send(result.inspect_err(|err| self.stop(err)));
            }
        }
    }

    /// Finish the delta and construct its acknowledgement, per [`Writer::prepare`].
    async fn prepare(&mut self, captured: &mut Captured) -> anyhow::Result<Option<bytes::Bytes>> {
        () = self.check()?;

        if matches!(
            self.phase,
            Phase::Appending {
                prepared: Some(_),
                ..
            }
        ) {
            return Err(anyhow::Error::new(failure::Failure::OutOfOrder(
                "a prepared delta is still awaiting its commit".to_string(),
            )));
        }
        // Admission is closed and every admitted mutation has been offered, so
        // taking until the queue is empty is the cut. It cannot refill behind
        // this loop, which is why `prepare` may take more than one mutation
        // where `run` takes exactly one.
        while let Some(chunks) = captured.try_recv() {
            () = self.capture(chunks).await?;
        }

        if self.delta_records == 0 {
            return Ok(None);
        }
        // Every record of this delta is confirmed before its acknowledgement is
        // built. The appender holds a batch back while it pipelines the one
        // before it, so the cut is not a cut until that batch has landed.
        () = self.flush().await?;

        // The horizon is sampled here at the cut, and not when the delta commits.
        // Mutations admitted between the two belong to the next delta. They must
        // not complete a horizon this delta did not complete.
        let completes_horizon = match &self.compactor {
            Some(compactor) => self.horizon.is_some() && compactor.pending().await? == 0,
            None => false,
        };

        let (record, _clock) = self.stamp(uuid::Flags::ACK_TXN, Vec::new(), false);
        let mut buf = bytes::BytesMut::new();
        proto_gazette::fixed_framing::encode(&record, &mut buf);

        let ack = buf.freeze();
        let Phase::Appending { prepared, .. } = &mut self.phase else {
            panic!("the tenure is appending");
        };
        *prepared = Some(Prepared {
            ack: ack.clone(),
            completes_horizon,
        });
        self.delta_records = 0;

        Ok(Some(ack))
    }

    /// Append `ack`, which commits the delta this tenure prepared, and store the
    /// recovery floor it established, if any. The writer takes mutations again once
    /// this returns, per [`Task::taking`].
    async fn acknowledge(&mut self, ack: bytes::Bytes) -> anyhow::Result<()> {
        () = self.check()?;

        let Phase::Appending { prepared, .. } = &mut self.phase else {
            panic!("the tenure is appending");
        };
        let Some(prepared) = prepared.take() else {
            return Err(anyhow::Error::new(failure::Failure::OutOfOrder(
                "no prepared delta is awaiting a commit".to_string(),
            )));
        };

        if ack != prepared.ack {
            return Err(anyhow::Error::new(failure::Failure::OutOfOrder(
                "commit acknowledgement differs from the prepared one".to_string(),
            )));
        }
        let (journal, appender) = self.writing();
        () = journal.append_ack(appender, &ack).await?;

        if !prepared.completes_horizon {
            return Ok(());
        }
        let floor = self.complete_horizon()?;
        () = self.journal.store_floor(floor).await;

        Ok(())
    }

    /// Move the recovery floor to the horizon this acknowledgement completed, and
    /// report the offset it moved to.
    ///
    /// Its opening record now has a committed copy of every allocated block at
    /// or after it, so a replay may begin there.
    fn complete_horizon(&mut self) -> anyhow::Result<i64> {
        let floor = self.horizon.take().expect("a horizon was open");
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

        Ok(floor)
    }

    /// Take one mutation into this delta.
    async fn capture(&mut self, chunks: Vec<proto::Chunk>) -> anyhow::Result<()> {
        if !self.appending() {
            return Ok(()); // A tenure which is over commits nothing more.
        }
        let opens = self.delta_records == 0 && self.open_horizon().await?;

        self.append_mutation(chunks, opens).await
    }

    /// Stamp one mutation as this delta's next record and hand it to the
    /// appender, which decides when a batch of them becomes an append.
    ///
    /// `opens` marks a delta whose first record opens a recovery horizon. Only
    /// that record carries the flag, however many follow it: a reader which
    /// starts at the horizon must see every chunk which discharges it.
    ///
    /// A horizon is an offset, so that record is appended alone and confirmed —
    /// the appender is drained before it and flushed after — and the broker's
    /// own `begin` for that append is the horizon.
    pub(super) async fn append_mutation(
        &mut self,
        chunks: Vec<proto::Chunk>,
        opens: bool,
    ) -> anyhow::Result<()> {
        let opens = opens && self.delta_records == 0;

        if opens {
            () = self.flush().await?;
        }
        let (record, _clock) = self.stamp(uuid::Flags::CONTINUE_TXN, chunks, opens);
        let (journal, appender) = self.writing();
        () = journal.append_record(appender, &record).await?;

        self.delta_records += 1;

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

        self.horizon = Some(begin);
        Ok(())
    }

    /// Whether this delta's first record opens a recovery horizon.
    ///
    /// The decision is taken here at the record which carries the flag, and not at
    /// the cut before it. Both terms of the comparison have moved since that cut.
    /// The range is what a replay would read now, and the allocated size is what a
    /// horizon would have to discharge now.
    async fn open_horizon(&mut self) -> anyhow::Result<bool> {
        let Some(compactor) = &self.compactor else {
            return Ok(false);
        };
        if self.horizon.is_some() {
            return Ok(false);
        }
        let range = self.journal.head.saturating_sub(self.journal.floor).max(0) as u64;

        Ok(compactor.open(range).await?.is_some())
    }

    /// Build the tenure's next record, and report the clock it carries.
    ///
    /// Every record but a fence carries the tenure's epoch as its producer, which
    /// is why the writer takes nothing while an acknowledgement is outstanding: see
    /// [`Task::taking`].
    ///
    /// The clock only advances. It therefore orders each delta's records ahead of
    /// the acknowledgement which commits them, and ahead of every record of the
    /// prior delta. It also follows the wall clock. A recovery floor is the clock
    /// of a horizon's opening record, and a recovery turns that clock back into
    /// the modification time of the fragments to read from.
    fn stamp(
        &mut self,
        flags: uuid::Flags,
        chunks: Vec<proto::Chunk>,
        opens_horizon: bool,
    ) -> (proto::DiskRecord, uuid::Clock) {
        self.clock
            .update(uuid::Clock::from_time(std::time::SystemTime::now()));
        let clock = self.clock.tick();

        let record = proto::DiskRecord {
            uuid: uuid_bytes(self.journal.epoch, clock, flags),
            chunks,
            opens_horizon,
            installs_epoch: bytes::Bytes::new(),
        };
        (record, clock)
    }

    /// End the tenure over `err`, which is logged here and kept nowhere.
    ///
    /// The appender goes with the phase whatever failed, because an owner or
    /// protocol error ends the tenure just as a broker one does, and an append RPC
    /// left running would go on retrying for a disk which has no writer.
    ///
    /// A tenure which is already over did not fail. An append which
    /// [`Writer::abandon`] cancelled is the teardown working.
    fn stop(&mut self, err: &anyhow::Error) {
        if !matches!(self.phase, Phase::Appending { .. }) {
            return;
        }
        if self.journal.ended.is_cancelled() {
            tracing::debug!(journal = self.journal.name, ?err, "journal writer stopped");
        } else {
            tracing::error!(journal = self.journal.name, ?err, "journal writer failed");
        }
        self.phase = Phase::Failed(None);
    }

    /// [`Task::stop`] over a failure no request is waiting on, which is kept for the
    /// next request to take.
    fn fail(&mut self, err: anyhow::Error) {
        () = self.stop(&err);

        if let Phase::Failed(kept @ None) = &mut self.phase {
            *kept = Some(err);
        }
    }

    /// Refuse a request once the tenure is over.
    ///
    /// A failure nothing has reported is reported here, to the next request and
    /// to it alone. Requests after that are refused for the tenure being over,
    /// because the client which holds the failure holds its causes.
    fn check(&mut self) -> anyhow::Result<()> {
        match &mut self.phase {
            // The token is cancelled ahead of the phase under a drain, per
            // [`Task::appending`].
            Phase::Appending { .. } if self.journal.ended.is_cancelled() => {
                anyhow::bail!("the tenure ended")
            }
            Phase::Appending { .. } => Ok(()),
            Phase::Abandoned => anyhow::bail!("the tenure ended"),
            Phase::Failed(kept) => match kept.take() {
                Some(err) => Err(err),
                None => anyhow::bail!("the tenure has failed"),
            },
        }
    }
}
