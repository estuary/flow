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
//! by [`Writer::open`], so a disk which is never written creates no journal.
//! Recovery is the exception: a disk with committed state claims at open,
//! before anything reads or repairs it.

use crate::capture::Captured;
use crate::proto;
use anyhow::Context;
use gazette::journal::framing;
use proto_gazette::{broker, uuid};

pub mod fence;
pub mod spec;

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
    /// Whether the disk has committed state, which is an acknowledged delta in
    /// the journal or an acknowledgement the client recovered. Such a disk
    /// claims its journal at open, because everything which follows reads or
    /// repairs state the previous writer must no longer be able to change.
    pub committed_state: bool,
    /// Mutations of formatting and mounting a fresh disk, which are appended
    /// ahead of the first mutation which follows them rather than as they
    /// arrive. A disk which is only formatted carries no information, because
    /// formatting it again reproduces it, so it publishes nothing at all.
    ///
    /// A recovered disk retains nothing: its journal exists, and its mount
    /// writes belong to the next delta like any other mutation.
    pub retained: Vec<Vec<proto::Chunk>>,
}

/// Handle to a session's journal writer.
pub struct Writer {
    commands: tokio::sync::mpsc::Sender<Command>,
    epoch: uuid::Producer,
}

enum Command {
    Publish(Reply<Option<bytes::Bytes>>),
    Commit(bytes::Bytes, Reply<()>),
    Broker(proto::Broker, Reply<()>),
    Abandon(Reply<()>),
}

type Reply<T> = tokio::sync::oneshot::Sender<anyhow::Result<T>>;

/// Replaces the endpoint and credential the client dials with.
type SetBroker = Box<
    dyn Fn(tonic::Result<proto::Broker>) -> Option<tokens::WaitForCancellationFutureOwned> + Send,
>;

impl Writer {
    /// Open `journal`, reading the author register it must replace and claiming
    /// it now if the disk has committed state.
    ///
    /// Mutations are appended from `captured` as they arrive, until the returned
    /// handle is dropped.
    pub async fn open(
        client: &gazette::journal::Client,
        open: Open,
        captured: Captured,
    ) -> anyhow::Result<Self> {
        let Open {
            journal,
            broker,
            committed_state,
            retained,
        } = open;

        anyhow::ensure!(
            !broker.endpoint.is_empty(),
            "the session named no broker endpoint",
        );
        let spec = spec::build(&journal)?;

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
        let probe = fence::probe(&client, &spec.name).await?;

        let mut task = Task {
            journal: spec.name.clone(),
            spec,
            client,
            set_broker: Box::new(set_broker),
            epoch,
            clock: uuid::Clock::from_time(std::time::SystemTime::now()),
            fence: Fence::Deferred {
                prior: probe.author,
                exists: probe.exists,
            },
            delta_records: 0,
            retained,
            pending_ack: None,
            abandoned: false,
            drained: false,
            terminal: None,
        };

        if committed_state {
            task.claim().await?;
        }
        let (commands, receiver) = tokio::sync::mpsc::channel(1);
        tokio::spawn(task.run(captured, receiver));

        Ok(Self { commands, epoch })
    }

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
    pub async fn set_broker(&self, broker: proto::Broker) -> anyhow::Result<()> {
        self.call(|reply| Command::Broker(broker, reply)).await
    }

    /// Stop appending, and discard every mutation which follows.
    ///
    /// A session which is ending publishes nothing more, so what its disk does
    /// on the way out cannot be committed and a replay would ignore it. Those
    /// mutations are still taken, because a device whose mutations nothing
    /// takes cannot be unmounted.
    pub async fn abandon(&self) -> anyhow::Result<()> {
        self.call(Command::Abandon).await
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
    set_broker: SetBroker,
    epoch: uuid::Producer,
    clock: uuid::Clock,
    fence: Fence,
    /// Records appended since the last acknowledgement committed.
    delta_records: usize,
    /// Format and mount output awaiting the mutation it is appended ahead of.
    retained: Vec<Vec<proto::Chunk>>,
    /// Acknowledgement returned to the client and not yet committed.
    pending_ack: Option<bytes::Bytes>,
    /// Set once the session is ending, so that mutations are discarded.
    abandoned: bool,
    /// Set once the owner has released its half of the capture channel.
    drained: bool,
    /// Failure which ended the session, reported to every later request.
    terminal: Option<String>,
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
                            self.fail(err);
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
        !self.abandoned && self.terminal.is_none()
    }

    async fn command(&mut self, command: Command, captured: &Captured) -> anyhow::Result<()> {
        match command {
            Command::Publish(reply) => reply_with(reply, self.publish(captured).await),
            Command::Commit(ack, reply) => reply_with(reply, self.commit(ack).await),
            Command::Broker(broker, reply) => reply_with(reply, self.broker(broker)),
            Command::Abandon(reply) => {
                self.abandoned = true;
                self.retained.clear();

                reply_with(reply, Ok(()))
            }
        }
    }

    /// Finish the delta and construct its acknowledgement.
    async fn publish(&mut self, captured: &Captured) -> anyhow::Result<Option<bytes::Bytes>> {
        () = self.check()?;
        anyhow::ensure!(
            self.pending_ack.is_none(),
            "a published delta is still awaiting its commit",
        );
        () = self.drain(captured, None).await?;

        if self.delta_records == 0 {
            return Ok(None);
        }
        // Appends are issued one at a time and awaited, so reaching here is
        // every chunk of the delta having been confirmed by the broker.
        let mut buf = bytes::BytesMut::new();
        gazette::journal::framing::encode(&self.stamp(uuid::Flags::ACK_TXN, Vec::new()), &mut buf);

        let ack = buf.freeze();
        self.pending_ack = Some(ack.clone());

        Ok(Some(ack))
    }

    async fn commit(&mut self, ack: bytes::Bytes) -> anyhow::Result<()> {
        () = self.check()?;

        let published = self
            .pending_ack
            .take()
            .context("no published delta is awaiting a commit")?;

        anyhow::ensure!(
            ack == published,
            "commit acknowledgement differs from the published one",
        );
        () = self.append(ack).await?;
        self.delta_records = 0;

        Ok(())
    }

    fn broker(&mut self, broker: proto::Broker) -> anyhow::Result<()> {
        () = self.check()?;
        anyhow::ensure!(
            !broker.endpoint.is_empty(),
            "the session named no broker endpoint",
        );
        _ = (self.set_broker)(Ok(broker));

        Ok(())
    }

    /// Append every mutation the capture channel holds, beginning with `first`.
    ///
    /// Retained format and mount output goes ahead of them, and only ever
    /// alongside a mutation, so that the first delta carries all of the
    /// filesystem's allocated metadata and a disk which is never written
    /// creates no journal.
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
            self.retained.clear();
            return Ok(());
        }
        if mutations.is_empty() {
            return Ok(());
        }
        let mut buf = bytes::BytesMut::new();

        for chunks in std::mem::take(&mut self.retained)
            .into_iter()
            .chain(mutations)
        {
            let record = self.stamp(uuid::Flags::CONTINUE_TXN, chunks);

            framing::encode(&record, &mut buf);
            self.delta_records += 1;
        }

        self.append(buf.freeze()).await
    }

    /// Append `content` under this session's fence.
    ///
    /// A retry re-appends identical bytes, so an append which landed but was
    /// reported as failed duplicates its records, which a reader de-duplicates
    /// by UUID.
    async fn append(&mut self, content: bytes::Bytes) -> anyhow::Result<()> {
        () = self.claim().await?;

        let request = broker::AppendRequest {
            journal: self.journal.clone(),
            check_registers: Some(fence::held_by(self.epoch)),
            ..Default::default()
        };
        let source = || futures::stream::once(futures::future::ready(Ok(content.clone())));

        _ = append(&self.client, request, source)
            .await
            .with_context(|| format!("appending to {}", self.journal))?;

        Ok(())
    }

    /// Create and claim the journal, unless this session already has.
    async fn claim(&mut self) -> anyhow::Result<()> {
        let (prior, exists) = match &self.fence {
            Fence::Claimed => return Ok(()),
            Fence::Deferred { prior, exists } => (prior.clone(), *exists),
        };

        if !exists {
            () = spec::create(&self.client, self.spec.clone())
                .await
                .with_context(|| format!("creating {}", self.journal))?;
        }
        () = fence::claim(
            &self.client,
            &self.journal,
            prior.as_deref(),
            self.epoch,
            fence::record(self.epoch),
        )
        .await?;

        self.fence = Fence::Claimed;
        Ok(())
    }

    /// Build the session's next record. Its clock only advances, which orders
    /// each delta's records ahead of the acknowledgement that commits them and
    /// ahead of every record of the prior delta.
    fn stamp(&mut self, flags: uuid::Flags, chunks: Vec<proto::Chunk>) -> proto::DiskRecord {
        proto::DiskRecord {
            uuid: uuid_bytes(self.epoch, self.clock.tick(), flags),
            chunks,
            opens_horizon: false,
            installs_epoch: bytes::Bytes::new(),
        }
    }

    /// Remember the failure which ends the session.
    fn fail(&mut self, err: anyhow::Error) {
        if self.terminal.is_none() {
            tracing::error!(journal = self.journal, ?err, "journal writer failed");
            self.terminal = Some(format!("{err:#}"));
        }
    }

    fn check(&self) -> anyhow::Result<()> {
        match &self.terminal {
            Some(err) => Err(anyhow::anyhow!("the session has failed: {err}")),
            None => Ok(()),
        }
    }
}

/// Send a request's outcome, and return its failure so that the session ends.
fn reply_with<T>(reply: Reply<T>, result: anyhow::Result<T>) -> anyhow::Result<()> {
    let failure = result.as_ref().err().map(|err| anyhow::anyhow!("{err:#}"));
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
