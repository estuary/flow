//! Client API for opening and controlling disks served by the disk daemon.
//!
//! A disk is a mounted filesystem whose contents commit atomically with the caller's
//! own transaction. [`Client`] connects to the daemon's Unix socket and speaks its
//! tenure gRPC and nothing else. [`Client::open`] serves a disk at once, and
//! [`Client::standby`] holds one ready to be promoted. Either yields a [`Disk`] and
//! the path of its mount.
//!
//! Each open disk can take part in a two-phase commit. The caller coordinates the
//! transaction: it prepares pending disk changes, saves the acknowledgement with its
//! own transaction state, and then hands that acknowledgement back. The daemon enforces
//! this sequence and rejects requests made out of order with `FAILED_PRECONDITION`;
//! the client does not keep a second copy of the protocol state.
//!
//! [`Disk::acknowledge`] does not wait for the daemon to confirm the commit, in the
//! way the materialize protocol's `Acknowledge` does not: the confirmation gates the
//! next [`Disk::prepare`] rather than the start of the caller's next transaction. A
//! caller therefore commits and carries straight on. The confirmation is consumed by
//! whichever of `prepare`, [`Disk::acknowledged`], or [`Disk::close`] comes first,
//! and a commit which failed is reported there.
//!
//! What a caller may not do is treat its transaction as durable before that
//! confirmation. Until then the acknowledgement is one it must hand back in
//! `Promote.recovered_acks` after a failure, exactly as if it had never sent it.
//!
//! See the crate's `examples/` for usage.

use crate::proto;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("could not connect to the disk daemon")]
    Connect(#[from] tonic::transport::Error),
    #[error("the disk daemon rejected the request: {}", .0.message())]
    Invalid(tonic::Status),
    #[error("another tenure took ownership of this disk: {}", .0.message())]
    Fenced(tonic::Status),
    #[error("the broker rejected the tenure credentials: {}", .0.message())]
    Unauthorized(tonic::Status),
    #[error("the broker is unavailable: {}", .0.message())]
    Unavailable(tonic::Status),
    #[error("the disk tenure failed (gRPC code {:?}): {}", .0.code(), .0.message())]
    Failed(tonic::Status),
    #[error("the disk tenure ended")]
    Ended,
    #[error("unexpected reply from the disk daemon: {0}")]
    Unexpected(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// Returns whether opening a replacement tenure may succeed without changing
    /// its inputs.
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Connect(_) | Self::Unavailable(_))
    }

    fn of(status: tonic::Status) -> Self {
        match status.code() {
            tonic::Code::InvalidArgument | tonic::Code::FailedPrecondition => Self::Invalid(status),
            tonic::Code::Aborted => Self::Fenced(status),
            tonic::Code::Unauthenticated | tonic::Code::PermissionDenied => {
                Self::Unauthorized(status)
            }
            tonic::Code::Unavailable => Self::Unavailable(status),
            _ => Self::Failed(status),
        }
    }

    fn unexpected(reply: proto::response::Response) -> Self {
        Self::Unexpected(format!("{reply:?}"))
    }
}

#[derive(Clone)]
pub struct Client {
    channel: tonic::transport::Channel,
}

impl Client {
    pub async fn connect(uds_path: &std::path::Path) -> Result<Self> {
        let channel =
            tonic::transport::Endpoint::from_shared(format!("unix://{}", uds_path.display()))?
                .connect()
                .await?;

        Ok(Self { channel })
    }

    /// Opens a disk and serves it, returning a handle and the absolute path of the
    /// mounted filesystem.
    ///
    /// `open` names the journal the disk lives in, which must already exist. The
    /// daemon creates none: the journal's specification belongs to whoever applied
    /// it, and the daemon validates that live specification rather than converging
    /// it. The one field the daemon writes is the recovery floor.
    ///
    /// The caller keeps no state of its own about the disk. What a recovery needs in
    /// order to be cheap, the daemon stores on the journal itself.
    ///
    /// Use [`Client::standby`] instead to hold a disk ready without serving it.
    pub async fn open(
        &self,
        open: proto::Open,
        recovered_acks: Vec<bytes::Bytes>,
    ) -> Result<(Disk, std::path::PathBuf)> {
        self.standby(open).await?.promote(recovered_acks).await
    }

    /// Opens a disk and replays its journal, without serving it.
    ///
    /// `open` names a journal which must already exist, exactly as [`Client::open`]
    /// does. A standby of an empty one parks without reading until it promotes.
    ///
    /// The returned handle is a hot standby. It rebuilds the disk from the journal and
    /// then follows new records, so a [`Standby::promote`] once [`Standby::ready`] has
    /// resolved costs only a fence and a mount. Promote it when this caller becomes
    /// the disk's writer.
    ///
    /// A standby is not the writer: it has not fenced the journal, and another tenure
    /// may be serving the same disk elsewhere. So it cannot prepare or acknowledge, and
    /// it cannot repair a recovered acknowledgement. Those belong to `promote`.
    ///
    /// This returns as soon as the request is sent, so a caller may promote at once
    /// rather than waiting to be ready.
    pub async fn standby(&self, open: proto::Open) -> Result<Standby> {
        let (requests, receiver) = tokio::sync::mpsc::channel(1);
        let replies = proto_grpc::disk::disk_client::DiskClient::new(self.channel.clone())
            .tenure(tokio_stream::wrappers::ReceiverStream::new(receiver))
            .await
            .map_err(Error::of)?
            .into_inner();

        let mut disk = Disk {
            requests,
            replies,
            owed: false,
        };
        () = disk.send(proto::request::Request::Open(open)).await?;

        Ok(Standby {
            disk,
            opened: false,
        })
    }
}

/// A disk which is replayed and followed, but not served.
///
/// Its journal is not fenced, so holding one costs the disk nothing and does not
/// disturb whichever tenure is serving it now.
pub struct Standby {
    disk: Disk,
    /// Set once the daemon has reported that the replay is current, so that the reply
    /// is consumed exactly once however the caller sequences its calls.
    opened: bool,
}

impl Standby {
    /// Waits until the replay has read the history of the journal and follows it.
    ///
    /// A promotion from here costs a fence, the records which arrive after it, and a
    /// mount. That is what makes this standby hot, and a caller which reports its own
    /// readiness reports it from here.
    ///
    /// This is not a precondition of [`Standby::promote`]. A caller which has just
    /// become the writer promotes without waiting.
    pub async fn ready(&mut self) -> Result<()> {
        if self.opened {
            return Ok(());
        }
        match self.disk.reply().await? {
            proto::response::Response::Opened(proto::Opened {}) => {
                self.opened = true;
                Ok(())
            }
            reply => Err(Error::unexpected(reply)),
        }
    }

    /// Fence the journal, finish the replay, and serve the disk.
    ///
    /// This is legal at any time, including while the replay still reads history. The
    /// daemon fences as soon as it reads the request, which bounds what the replay has
    /// left to do, and it serves the disk once the replay is current. So a caller which
    /// has just become the writer promotes at once, and does not wait to be ready.
    ///
    /// `recovered_acks` are acknowledgements this caller committed but which may not
    /// have reached the journal, as [`Disk::prepare`] returned them. They arrive here
    /// rather than at open because a tenure cannot append before it fences.
    pub async fn promote(
        mut self,
        recovered_acks: Vec<bytes::Bytes>,
    ) -> Result<(Disk, std::path::PathBuf)> {
        () = self
            .disk
            .send(proto::request::Request::Promote(proto::Promote {
                recovered_acks,
            }))
            .await?;

        // A promotion which did not wait to be ready is owed `Opened` first, because
        // the two are ordered replies to two requests.
        () = self.ready().await?;

        match self.disk.reply().await? {
            proto::response::Response::Promoted(promoted) => {
                Ok((self.disk, promoted.mount_path.into()))
            }
            reply => Err(Error::unexpected(reply)),
        }
    }

    /// Ends the tenure and stops the replay.
    pub async fn close(self) -> Result<()> {
        self.disk.close().await
    }
}

/// A disk this caller is the writer of.
///
/// Dropping this handle or calling [`Disk::close`] ends the tenure, which makes the
/// daemon unmount the filesystem and delete the device. `close` waits for that cleanup
/// to finish.
pub struct Disk {
    requests: tokio::sync::mpsc::Sender<proto::Request>,
    replies: tonic::Streaming<proto::Response>,
    /// Set while an `Acknowledged` is owed, because [`Disk::acknowledge`] does not
    /// wait for one. At most one is ever owed: the daemon confirms it before it
    /// prepares another delta.
    owed: bool,
}

impl Disk {
    /// Makes all disk changes so far durable without committing them.
    ///
    /// When this returns, the broker has stored the change's data records, but not the
    /// acknowledgement that adds them to the committed disk state. This is the
    /// prepared phase of a two-phase commit. The method returns `None` if the disk has
    /// not changed and there is nothing to commit.
    ///
    /// A returned acknowledgement is an opaque commit token. The caller must save the
    /// exact bytes in the same atomic transaction as its own related state, then pass
    /// them unchanged to [`Disk::acknowledge`]. If the caller stops after saving the
    /// token but before acknowledging it, it must pass the token in
    /// `Promote.recovered_acks` the next time it opens the disk. The daemon will then
    /// recover the prepared change as committed.
    ///
    /// A commit which was left in flight by [`Disk::acknowledge`] is confirmed here
    /// first, and its failure is reported here.
    pub async fn prepare(&mut self) -> Result<Option<bytes::Bytes>> {
        () = self.acknowledged().await?;

        match self
            .call(proto::request::Request::Prepare(proto::Prepare {}))
            .await?
        {
            proto::response::Response::Prepared(cut) if cut.ack.is_empty() => Ok(None),
            proto::response::Response::Prepared(cut) => Ok(Some(cut.ack)),
            reply => Err(Error::unexpected(reply)),
        }
    }

    /// Commits the prepared change which `ack` acknowledges.
    ///
    /// Save `ack` atomically with the application state that depends on the prepared
    /// disk change before passing it here. If this call is interrupted, supply the
    /// saved acknowledgement when reopening the disk so recovery can finish the commit.
    ///
    /// This returns once the request is sent, without waiting for the daemon to
    /// confirm the commit. The confirmation gates the next [`Disk::prepare`] and not
    /// the caller's next transaction, so a caller carries on meanwhile. Read the
    /// confirmation with [`Disk::acknowledged`] where the commit having landed is a
    /// precondition of what comes next; `prepare` and [`Disk::close`] consume one
    /// which is still owed.
    pub async fn acknowledge(&mut self, ack: bytes::Bytes) -> Result<()> {
        () = self
            .send(proto::request::Request::Acknowledge(proto::Acknowledge {
                ack,
            }))
            .await?;
        self.owed = true;

        Ok(())
    }

    /// Waits for the daemon to confirm the commit of the last [`Disk::acknowledge`],
    /// and does nothing when no commit is owed one.
    ///
    /// Until this returns, the acknowledgement is one the caller must still hand back
    /// in `Promote.recovered_acks` if this tenure fails.
    pub async fn acknowledged(&mut self) -> Result<()> {
        if !self.owed {
            return Ok(());
        }
        // Taken before the reply is read, so that a failure is reported once and a
        // retry of it does not wait for a reply the daemon will never send.
        self.owed = false;

        match self.reply().await? {
            proto::response::Response::Acknowledged(_acknowledged) => Ok(()),
            reply => Err(Error::unexpected(reply)),
        }
    }

    /// Ends the tenure and waits for the daemon to unmount the filesystem and delete
    /// the device.
    ///
    /// A commit which [`Disk::acknowledge`] left in flight is confirmed first, so a
    /// `close` which follows an `acknowledge` still promises that the commit landed.
    pub async fn close(mut self) -> Result<()> {
        () = self.acknowledged().await?;
        drop(self.requests);

        match self.replies.message().await.map_err(Error::of)? {
            None => Ok(()),
            Some(proto::Response { response }) => match response {
                Some(reply) => Err(Error::unexpected(reply)),
                None => Err(Error::Ended),
            },
        }
    }

    async fn send(&mut self, request: proto::request::Request) -> Result<()> {
        self.requests
            .send(proto::Request {
                request: Some(request),
            })
            .await
            .map_err(|_| Error::Ended)
    }

    async fn reply(&mut self) -> Result<proto::response::Response> {
        match self.replies.message().await.map_err(Error::of)? {
            Some(proto::Response {
                response: Some(reply),
            }) => Ok(reply),
            _ => Err(Error::Ended),
        }
    }

    async fn call(
        &mut self,
        request: proto::request::Request,
    ) -> Result<proto::response::Response> {
        () = self.send(request).await?;

        self.reply().await
    }
}
