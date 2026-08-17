//! The journal writer: one session's sole authority over one disk journal.
//!
//! A session appends each mutation as its device accepts it, and its delta is
//! committed by an acknowledgement which the client makes durable elsewhere
//! before handing it back. The writer therefore holds the two halves of a
//! boundary apart: `publish` finishes and confirms every data append and
//! returns the acknowledgement's exact bytes, and `commit` appends those bytes
//! and awaits the broker's confirmation.
//!
//! The journal is created and claimed by the session's first append rather than
//! when it is opened, so a disk which is never written creates no journal.
//! Recovery is the exception: a disk with committed state claims before
//! anything reads or repairs it.

use crate::capture::Captured;
use crate::horizon::Position;
use crate::image::Image;
use crate::owner::{Compactor, Snapshotter};
use crate::proto;
use anyhow::Context;
use gazette::journal::framing;
use proto_gazette::{broker, uuid};

pub mod fence;
pub mod floor;
pub mod replay;
pub mod spec;

/// Bytes of one chunk of an append's byte stream, matching
/// `publisher::Appender`. An append is a whole drain of the capture channel,
/// which can be the device's largest request times that channel's capacity, and
/// a broker refuses a gRPC message beyond its own limit.
const CHUNK_BYTES: usize = 32 << 10; // 32 KiB.

/// The client every session derives its own from, so that they share
/// connections to brokers and to fragment stores, and the routing table its
/// owner sweeps to close connections no disk needs any more.
///
/// The zone is empty because routing to a nearby replica is a fact a host does
/// not have, and there is no default metadata because a session supplies its
/// own endpoint and credential.
pub fn shared_client() -> (gazette::journal::Client, gazette::Router) {
    let router = gazette::Router::new("");

    let client = gazette::journal::Client::new(
        String::new(),
        gazette::journal::Client::new_fragment_client(),
        proto_grpc::Metadata::new(),
        router.clone(),
    );
    (client, router)
}

/// Inputs of one session's journal.
pub struct Open {
    /// Journal of the disk and how to create it.
    pub journal: proto::JournalConfig,
    /// Brokers serving the journal.
    pub broker: proto::Broker,
    /// Label of the journal's own spec which carries its recovery floor. The
    /// session reads it to seek its replay, and writes it as horizons complete.
    pub floor_label: String,
}

/// A session's journal before its disk exists.
///
/// Recovery is a step of its own because a disk with committed state must be
/// rebuilt before a device can be created over it, while the journal must be
/// claimed before it is read.
pub struct Opening {
    task: Task,
    set_broker: SetBroker,
}

/// Handle to a session's journal writer.
pub struct Writer {
    commands: tokio::sync::mpsc::Sender<Command>,
    ended: tokio_util::sync::CancellationToken,
    set_broker: SetBroker,
    epoch: uuid::Producer,
}

enum Command {
    Publish(Reply<Option<bytes::Bytes>>),
    Commit(bytes::Bytes, Reply<()>),
}

type Reply<T> = tokio::sync::oneshot::Sender<Result<T, Failure>>;

/// The failure which ended a session. It is shared rather than moved, because
/// every request after it is answered with the same one, unshaped: [`Writer`] is
/// the only layer which turns it into something a client reads.
type Failure = std::sync::Arc<anyhow::Error>;

/// How a session's failure reaches its client, applied exactly once by
/// [`Writer::call`].
///
/// It keeps the original as its cause, because a session stream's error code is
/// derived from that: a writer fenced by a replacement session must still be
/// reported as fenced, however many requests later its client asks.
#[derive(Debug)]
struct Failed(Failure);

impl std::fmt::Display for Failed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the session has failed")
    }
}

impl std::error::Error for Failed {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref().as_ref())
    }
}

/// Replaces the endpoint and credential the client dials with.
type SetBroker = Box<
    dyn Fn(tonic::Result<proto::Broker>) -> Option<tokens::WaitForCancellationFutureOwned>
        + Send
        + Sync,
>;

impl Opening {
    /// Open `journal` and read the author register a claim must replace.
    ///
    /// Nothing is appended and no journal is created here. `ended` is the
    /// session's own cancellation, which every broker call of this journal
    /// gives up on: see [`until_ended`].
    pub async fn new(
        client: &gazette::journal::Client,
        open: Open,
        ended: tokio_util::sync::CancellationToken,
    ) -> anyhow::Result<Self> {
        let Open {
            journal,
            broker,
            floor_label,
        } = open;

        crate::ensure_valid!(
            !broker.endpoint.is_empty(),
            "the session named no broker endpoint",
        );
        let spec = spec::build(&journal)?;
        let metrics = crate::metrics::Journal::new(&spec.name);

        let (tokens, set_broker) = tokens::manual::<proto::Broker>();
        _ = set_broker(Ok(broker));

        let client = client.with_tokens(
            |broker: &proto::Broker| {
                let metadata = match broker.credential.is_empty() {
                    true => proto_grpc::Metadata::new(),
                    false => proto_grpc::Metadata::new().with_bearer_token(&broker.credential)?,
                };
                Ok((metadata, broker.endpoint.clone()))
            },
            tokens,
        );

        let epoch = fresh_producer();
        let probe = until_ended(&ended, "probing", fence::probe(&client, &spec.name)).await?;

        Ok(Self {
            set_broker: Box::new(set_broker),
            task: Task {
                journal: spec.name.clone(),
                spec,
                client,
                metrics,
                ended,
                floor_label,
                epoch,
                clock: uuid::Clock::zero(),
                fence: Fence::Deferred {
                    prior: probe.author,
                    exists: probe.exists,
                },
                head: probe.head,
                floor: 0,
                horizon: None,
                completes_horizon: false,
                delta_records: 0,
                snapshot: None,
                compactor: None,
                pending_ack: None,
                drained: false,
                terminal: None,
            },
        })
    }

    /// Rebuild `image` from every acknowledged delta of the journal, having
    /// first claimed it and appended each of `recovered_acks` exactly.
    ///
    /// Returns false for a journal with no committed state, whose disk is fresh
    /// and the caller's to format.
    ///
    /// The claim comes first because everything which follows reads or repairs
    /// state the previous writer must no longer be able to change. It is made
    /// for any journal which exists, since whether its records are committed is
    /// only knowable by reading them, and an orphan journal's fence is one this
    /// session was going to install with its own first append anyway.
    pub async fn recover(
        &mut self,
        image: &mut Image,
        recovered_acks: Vec<bytes::Bytes>,
    ) -> anyhow::Result<bool> {
        let task = &mut self.task;

        if recovered_acks.is_empty() && matches!(task.fence, Fence::Deferred { exists: false, .. })
        {
            return Ok(false);
        }
        () = task.claim().await?;

        for ack in recovered_acks {
            _ = task
                .append(ack)
                .await
                .context("repairing a recovered acknowledgement")?;
        }

        // The head is read after the repair and from a broker which has just
        // served an append, so its index covers every fragment below it. That
        // is what fixes the end of this recovery and makes its read fresh.
        let (head, seek, replayed) = until_ended(&task.ended, "recovering", async {
            let head = fence::probe(&task.client, &task.journal).await?.head;
            let seek = floor::seek(&task.client, &task.journal, &task.floor_label).await?;

            let replayed = replay::replay(&task.client, &task.journal, seek, head, image).await?;

            anyhow::Ok((head, seek, replayed))
        })
        .await?;

        tracing::info!(
            journal = task.journal,
            head,
            seek,
            applied = replayed.applied,
            floor = replayed.floor,
            horizon = ?replayed.horizon,
            "replayed a disk from its journal",
        );

        task.head = head;
        task.floor = replayed.floor;
        task.horizon = replayed.horizon;
        task.metrics.floor_seconds.set(seek as f64);
        task.report_range();

        // A floor the label does not hold is one an earlier session derived and
        // could not record, which this session records for it.
        if let Some(clock) = replayed.derived {
            floor::advance(
                task.client.clone(),
                task.journal.clone(),
                task.floor_label.clone(),
                clock,
            );
        }
        Ok(replayed.applied != 0)
    }

    /// Begin appending the mutations of `captured` as they arrive, until the
    /// returned handle is dropped.
    ///
    /// `snapshot` is a fresh disk's image, which its first append publishes
    /// ahead of the mutation which triggered it. A recovered disk has none: its
    /// journal already holds the filesystem, and the writes its mount issues
    /// belong to the next delta like any other mutation.
    ///
    /// `compactor` is the disk whose horizons this writer opens and completes. A
    /// writer without one, which this crate's own tests build, appends what it
    /// is given and compacts nothing.
    pub fn serve(
        self,
        captured: Captured,
        snapshot: Option<Snapshotter>,
        compactor: Option<Compactor>,
    ) -> Writer {
        let Self {
            mut task,
            set_broker,
        } = self;
        task.snapshot = snapshot;
        task.compactor = compactor;

        let (epoch, ended) = (task.epoch, task.ended.clone());
        let (commands, receiver) = tokio::sync::mpsc::channel(1);
        tokio::spawn(task.run(captured, receiver));

        Writer {
            commands,
            ended,
            set_broker,
            epoch,
        }
    }
}

impl Writer {
    /// Epoch this session installs as the journal's author.
    pub fn epoch(&self) -> uuid::Producer {
        self.epoch
    }

    /// Finish and confirm every append of the delta before this cut, then
    /// return the acknowledgement which commits it. The acknowledgement is not
    /// appended, and no further mutation is appended until it is committed.
    ///
    /// The caller must have stopped admitting mutations and awaited those it
    /// admitted, because draining what the capture channel holds is what makes
    /// this the cut.
    ///
    /// `None` when the delta is empty, which is a disk the transaction did not
    /// change and a client which owes no commit.
    pub async fn publish(&self) -> anyhow::Result<Option<bytes::Bytes>> {
        self.call(Command::Publish).await
    }

    /// Append the acknowledgement returned by [`Writer::publish`] and await the
    /// broker's confirmation of it.
    pub async fn commit(&self, ack: bytes::Bytes) -> anyhow::Result<()> {
        self.call(|reply| Command::Commit(ack, reply)).await
    }

    /// Replace the endpoint and credential of the session's broker connection.
    ///
    /// It does not go through the writer, which may be retrying an append
    /// against the very broker being replaced. Each attempt reads the pair
    /// afresh, so a replacement reaches one already in flight.
    pub fn set_broker(&self, broker: proto::Broker) -> anyhow::Result<()> {
        crate::ensure_valid!(
            !broker.endpoint.is_empty(),
            "the session named no broker endpoint",
        );
        _ = (self.set_broker)(Ok(broker));

        Ok(())
    }

    /// Stop appending, and discard every mutation which follows.
    ///
    /// A session which is ending publishes nothing more, so what its disk does
    /// on the way out cannot be committed and a replay would ignore it. Those
    /// mutations are still taken, because a device whose mutations nothing
    /// takes cannot be unmounted.
    ///
    /// It cancels rather than asking, and gives up whatever broker call is in
    /// flight. A writer retrying an append against an unreachable broker would
    /// otherwise never answer, and the disk under it could not be unmounted for
    /// as long as the outage lasted.
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
            .map_err(|err| anyhow::Error::new(Failed(err)))
    }
}

/// Whether the session has taken the journal's author register.
enum Fence {
    /// Not yet claimed. `prior` is the author read at open, which the claim
    /// must still find, and `exists` is false if the journal must be created.
    Deferred {
        prior: Option<String>,
        exists: bool,
    },
    Claimed,
}

struct Task {
    journal: String,
    spec: broker::JournalSpec,
    client: gazette::journal::Client,
    metrics: crate::metrics::Journal,
    /// Cancelled once the session is over, which is what stops this writer
    /// appending and gives up whatever broker call is in flight.
    ended: tokio_util::sync::CancellationToken,
    floor_label: String,
    epoch: uuid::Producer,
    clock: uuid::Clock,
    fence: Fence,
    /// Write head confirmed by the broker, which with `floor` is the range a
    /// recovery of this disk would have to read.
    head: i64,
    floor: i64,
    /// Horizon this session opened or resumed, which is not yet complete.
    horizon: Option<Position>,
    /// Set when the cut of a publication found the open horizon discharged, so
    /// that the commit of that delta completes it.
    completes_horizon: bool,
    /// Records appended since the last acknowledgement committed.
    delta_records: usize,
    /// A fresh disk's image, awaiting the mutation it is published ahead of.
    snapshot: Option<Snapshotter>,
    /// The disk this journal serves, which owns its horizon's bitmap.
    compactor: Option<Compactor>,
    /// Acknowledgement returned to the client and not yet committed.
    pending_ack: Option<bytes::Bytes>,
    /// Set once the owner has released its half of the capture channel.
    drained: bool,
    /// Failure which ended the session, reported to every later request.
    terminal: Option<Failure>,
}

impl Task {
    async fn run(mut self, captured: Captured, mut commands: tokio::sync::mpsc::Receiver<Command>) {
        loop {
            tokio::select! {
                // Requests come first: a cut must observe every mutation queued
                // before it, which `publish` drains, rather than race them.
                biased;

                command = commands.recv() => {
                    let Some(command) = command else { return };

                    if let Err(err) = self.command(command, &captured).await {
                        self.fail(err);
                    }
                }
                chunks = captured.recv(), if self.taking() => match chunks {
                    Some(chunks) => {
                        if let Err(err) = self.drain(&captured, Some(chunks)).await {
                            self.fail(std::sync::Arc::new(err));
                        }
                    }
                    None => self.drained = true,
                },
            }
        }
    }

    /// Whether mutations are taken from the capture channel. Taking stops while
    /// a published delta awaits its commit, which is what keeps Gazette from
    /// grouping two deltas into one pending transaction. A session which
    /// appends no more keeps taking, because a device whose mutations nothing
    /// takes cannot be unmounted.
    fn taking(&self) -> bool {
        !self.drained && (!self.appending() || self.pending_ack.is_none())
    }

    /// Whether mutations are appended rather than discarded.
    fn appending(&self) -> bool {
        !self.ended.is_cancelled() && self.terminal.is_none()
    }

    async fn command(&mut self, command: Command, captured: &Captured) -> Result<(), Failure> {
        match command {
            Command::Publish(reply) => reply_with(reply, self.publish(captured).await),
            Command::Commit(ack, reply) => reply_with(reply, self.commit(ack).await),
        }
    }

    /// Finish the delta and construct its acknowledgement.
    async fn publish(&mut self, captured: &Captured) -> Result<Option<bytes::Bytes>, Failure> {
        () = self.check()?;

        if self.pending_ack.is_some() {
            return Err(anyhow::anyhow!("a published delta is still awaiting its commit").into());
        }
        () = self.drain(captured, None).await?;

        if self.delta_records == 0 {
            return Ok(None);
        }
        // The horizon is sampled here, at the cut, and not when the delta
        // commits: mutations admitted between the two belong to the next delta
        // and must not complete a horizon this one did not.
        if let Some(compactor) = &self.compactor {
            self.completes_horizon = self.horizon.is_some() && compactor.pending().await? == 0;
        }

        // Appends are issued one at a time and awaited, so reaching here is
        // every chunk of the delta having been confirmed by the broker.
        let (record, _clock) = self.stamp(uuid::Flags::ACK_TXN, Vec::new(), false);
        let mut buf = bytes::BytesMut::new();
        gazette::journal::framing::encode(&record, &mut buf);

        let ack = buf.freeze();
        self.pending_ack = Some(ack.clone());
        self.metrics.publishes.increment(1);

        Ok(Some(ack))
    }

    async fn commit(&mut self, ack: bytes::Bytes) -> Result<(), Failure> {
        () = self.check()?;

        let published = self
            .pending_ack
            .take()
            .context("no published delta is awaiting a commit")?;

        if ack != published {
            return Err(
                anyhow::anyhow!("commit acknowledgement differs from the published one").into(),
            );
        }
        _ = self.append(ack).await?;
        self.delta_records = 0;
        self.metrics.commits.increment(1);

        if std::mem::take(&mut self.completes_horizon) {
            () = self.complete_horizon()?;
        }
        Ok(())
    }

    /// Move the recovery floor to the horizon this commit completed.
    ///
    /// Its opening record now has a committed copy of every allocated block at
    /// or after it, which is what a replay may begin from and what the floor
    /// label records.
    fn complete_horizon(&mut self) -> anyhow::Result<()> {
        let Position { offset, clock } = self.horizon.take().expect("a horizon was open");
        self.floor = offset;

        if let Some(compactor) = &self.compactor {
            () = compactor.close()?;
        }
        self.metrics.horizons.increment(1);
        self.metrics.floor_seconds.set(clock.to_unix().0 as f64);
        self.report_range();

        tracing::info!(
            journal = self.journal,
            offset,
            head = self.head,
            "completed a recovery horizon",
        );

        floor::advance(
            self.client.clone(),
            self.journal.clone(),
            self.floor_label.clone(),
            clock,
        );
        Ok(())
    }

    /// Append every mutation the capture channel holds, beginning with `first`.
    ///
    /// A fresh disk's image goes ahead of them, and only ever alongside a
    /// mutation, so that the first delta carries all of the filesystem's
    /// allocated metadata and a disk which is never written creates no journal.
    /// A mutation the snapshot already reflects is simply applied again, which
    /// is why the two need not be taken at the same instant.
    ///
    /// That image is as large as the device may be, so it is taken and appended
    /// a batch at a time. What a session holds is then bounded by the batch and
    /// by the capture channel, whatever size of disk it serves.
    async fn drain(
        &mut self,
        captured: &Captured,
        first: Option<Vec<proto::Chunk>>,
    ) -> anyhow::Result<()> {
        let mut mutations: Vec<Vec<proto::Chunk>> = first.into_iter().collect();

        while let Some(chunks) = captured.try_recv() {
            mutations.push(chunks);
        }
        if !self.appending() {
            self.snapshot = None;
            return Ok(());
        }
        if mutations.is_empty() {
            return Ok(());
        }
        let snapshot = self.snapshot.take();
        let opens = self.delta_records == 0 && self.open_horizon().await?;

        if let Some(snapshotter) = snapshot {
            let mut from = 0;

            loop {
                let (runs, next) = snapshotter.snapshot(from).await?;
                () = self.append_records(runs, opens).await?;

                let Some(next) = next else { break };
                from = next;
            }
        }
        self.append_records(mutations, opens).await
    }

    /// Stamp `mutations` as this delta's next records and append them.
    ///
    /// `opens` puts the horizon flag on the first record of the *delta*, which
    /// only the first call within one can do: a snapshot arrives in several
    /// batches, and a reader starting at the horizon must see every chunk which
    /// discharges it.
    async fn append_records(
        &mut self,
        mutations: Vec<Vec<proto::Chunk>>,
        opens: bool,
    ) -> anyhow::Result<()> {
        let opens = opens && self.delta_records == 0;

        let mut buf = bytes::BytesMut::new();
        let mut opened = None;

        for (index, chunks) in mutations.into_iter().enumerate() {
            let (record, clock) =
                self.stamp(uuid::Flags::CONTINUE_TXN, chunks, opens && index == 0);

            if opens && index == 0 {
                opened = Some(clock);
            }
            framing::encode(&record, &mut buf);
            self.delta_records += 1;
            self.metrics.appended_records.increment(1);
        }
        if buf.is_empty() {
            return Ok(());
        }
        let begin = self.append(buf.freeze()).await?;

        if let Some(clock) = opened {
            self.horizon = Some(Position {
                offset: begin,
                clock,
            });
        }
        Ok(())
    }

    /// Whether this delta's first record opens a recovery horizon.
    ///
    /// The decision is taken here, at the record which carries the flag, rather
    /// than at the cut before it, because both terms it compares have moved
    /// since: the range is what a replay would read now, and the disk's
    /// allocated size is what a horizon would have to discharge now.
    async fn open_horizon(&mut self) -> anyhow::Result<bool> {
        let Some(compactor) = &self.compactor else {
            return Ok(false);
        };
        if self.horizon.is_some() {
            return Ok(false);
        }
        let range = self.head.saturating_sub(self.floor).max(0) as u64;

        Ok(compactor.open(range).await?.is_some())
    }

    /// Append `content` under this session's fence, and report the offset it
    /// begins at.
    ///
    /// A retry re-appends identical bytes, so an append which landed but was
    /// reported as failed duplicates its records, which a reader de-duplicates
    /// by UUID.
    async fn append(&mut self, content: bytes::Bytes) -> anyhow::Result<i64> {
        () = self.claim().await?;

        let request = broker::AppendRequest {
            journal: self.journal.clone(),
            check_registers: Some(fence::held_by(self.epoch)),
            ..Default::default()
        };
        let source = || {
            let content = content.clone();

            futures::stream::iter((0..content.len()).step_by(CHUNK_BYTES).map(move |at| {
                Ok(content.slice(at..std::cmp::min(at + CHUNK_BYTES, content.len())))
            }))
        };

        let response = until_ended(&self.ended, "appending", async {
            append(&self.client, request, source)
                .await
                .with_context(|| format!("appending to {}", self.journal))
        })
        .await?;

        let commit = response
            .commit
            .context("append response carries no committed fragment")?;

        self.head = commit.end;
        self.metrics.appended_bytes.increment(content.len() as u64);
        self.report_range();

        Ok(commit.begin)
    }

    /// Report the journal range a recovery of this disk would now read, which
    /// is what a horizon exists to bound.
    fn report_range(&self) {
        self.metrics
            .recovery_range
            .set(self.head.saturating_sub(self.floor).max(0) as f64);
    }

    /// Create and claim the journal, unless this session already has.
    async fn claim(&mut self) -> anyhow::Result<()> {
        let (prior, exists) = match &self.fence {
            Fence::Claimed => return Ok(()),
            Fence::Deferred { prior, exists } => (prior.clone(), *exists),
        };

        () = until_ended(&self.ended, "claiming", async {
            if !exists {
                () = spec::create(&self.client, self.spec.clone())
                    .await
                    .with_context(|| format!("creating {}", self.journal))?;
            }
            fence::claim(
                &self.client,
                &self.journal,
                prior.as_deref(),
                self.epoch,
                fence::record(self.epoch),
            )
            .await
        })
        .await?;

        self.fence = Fence::Claimed;
        Ok(())
    }

    /// Build the session's next record, and report the clock it carries.
    ///
    /// That clock only advances, which orders each delta's records ahead of the
    /// acknowledgement that commits them and ahead of every record of the prior
    /// delta. It also follows the wall clock, because the floor label carries
    /// the clock of a horizon's opening record and a reader turns that back into
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
            uuid: uuid_bytes(self.epoch, clock, flags),
            chunks,
            opens_horizon,
            installs_epoch: bytes::Bytes::new(),
        };
        (record, clock)
    }

    /// Remember the failure which ends the session.
    ///
    /// A session which is already over did not fail: an append cancelled by
    /// [`Writer::abandon`] is the teardown working.
    fn fail(&mut self, err: Failure) {
        if self.terminal.is_some() {
            return;
        }
        if self.ended.is_cancelled() {
            tracing::debug!(journal = self.journal, ?err, "journal writer stopped");
        } else {
            tracing::error!(journal = self.journal, ?err, "journal writer failed");
        }
        self.terminal = Some(err);
    }

    /// Refuse a request because the session has already failed, handing back
    /// that same failure rather than a description of it.
    fn check(&self) -> Result<(), Failure> {
        match &self.terminal {
            Some(err) => Err(err.clone()),
            None => Ok(()),
        }
    }
}

/// Run `work`, failing if the session ends before it finishes.
///
/// Every broker call a session makes retries a transient error until it
/// succeeds, which is right while the disk is live and wrong the moment the
/// session is over: a teardown which waited on an unreachable broker would hold
/// the disk's device and its mount for as long as the outage lasted, and a
/// draining daemon would leave both behind.
async fn until_ended<T>(
    ended: &tokio_util::sync::CancellationToken,
    what: &str,
    work: impl Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    tokio::select! {
        _ = ended.cancelled() => anyhow::bail!("the session ended while {what}"),
        result = work => result,
    }
}

/// Send a request's outcome, and return its failure so that the session ends.
fn reply_with<T>(reply: Reply<T>, result: Result<T, Failure>) -> Result<(), Failure> {
    let failure = result.as_ref().err().cloned();
    _ = reply.send(result);

    match failure {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

fn uuid_bytes(producer: uuid::Producer, clock: uuid::Clock, flags: uuid::Flags) -> bytes::Bytes {
    bytes::Bytes::copy_from_slice(uuid::build(producer, clock, flags).as_bytes())
}

fn fresh_producer() -> uuid::Producer {
    let bytes = uuid::Uuid::new_v4().into_bytes();

    // Per RFC 4122 the multicast bit marks a node ID which is not a MAC address.
    uuid::Producer::from_bytes([
        bytes[0] | 0x01,
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
    ])
}

/// Issue one append, retrying transient broker errors, and return its response.
async fn append<S>(
    client: &gazette::journal::Client,
    request: broker::AppendRequest,
    source: impl Fn() -> S + Send + Sync,
) -> gazette::Result<broker::AppendResponse>
where
    S: futures::Stream<Item = std::io::Result<bytes::Bytes>> + Send + 'static,
{
    let journal = request.journal.clone();
    let stream = client.append(request, source);
    futures::pin_mut!(stream);

    loop {
        match futures::StreamExt::next(&mut stream).await {
            Some(Ok(response)) => return Ok(response),
            Some(Err(gazette::RetryError { attempt, inner })) if inner.is_transient() => {
                tracing::warn!(journal, attempt, %inner, "append failed (will retry)");
            }
            Some(Err(gazette::RetryError { inner, .. })) => return Err(inner),
            None => unreachable!("an append stream does not end without a response"),
        }
    }
}
