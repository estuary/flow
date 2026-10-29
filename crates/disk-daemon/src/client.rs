use crate::proto;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("could not connect to the disk-daemon")]
    Connect(#[from] tonic::transport::Error),
    #[error("the disk-daemon rejected the request: {}", .0.message())]
    Invalid(tonic::Status),
    #[error("another attachment took ownership of this disk: {}", .0.message())]
    Fenced(tonic::Status),
    #[error("the broker rejected the disk-daemon's credentials: {}", .0.message())]
    Unauthorized(tonic::Status),
    #[error("the disk-daemon is unavailable: {}", .0.message())]
    Unavailable(tonic::Status),
    #[error("the disk attachment failed (gRPC code {:?}): {}", .0.code(), .0.message())]
    Failed(tonic::Status),
    #[error("the disk attachment closed")]
    Closed,
    #[error("unexpected reply from the disk-daemon: {0}")]
    Unexpected(String),
}

impl Error {
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
            tonic::Code::Unavailable | tonic::Code::Cancelled => Self::Unavailable(status),
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

    /// Open and promote a disk, returning its handle and absolute mount path.
    /// `ack_intent` is as for [`Standby::promote`].
    ///
    /// Use [`Client::standby`] to replay without serving the disk yet.
    pub async fn open(
        &self,
        open: proto::Open,
        ack_intent: bytes::Bytes,
    ) -> Result<(Disk, std::path::PathBuf)> {
        self.standby(open).await?.promote(ack_intent).await
    }

    /// Start replay without fencing or serving the disk. An empty journal parks
    /// until promotion. Otherwise replay follows new commits after backfill,
    /// reducing the work left at failover.
    ///
    /// Returns once `Open` is sent. The caller may wait for [`Standby::ready`]
    /// or promote at once. Preparing, acknowledging, and re-acknowledging
    /// recovered ACK intents require promotion.
    pub async fn standby(&self, open: proto::Open) -> Result<Standby> {
        let (requests, receiver) = tokio::sync::mpsc::channel(1);
        let replies = proto_grpc::disk::disk_client::DiskClient::new(self.channel.clone())
            .attach(tokio_stream::wrappers::ReceiverStream::new(receiver))
            .await
            .map_err(Error::of)?
            .into_inner();

        let mut disk = Disk {
            requests,
            replies,
            awaiting_acknowledged: false,
        };
        () = disk.send(proto::request::Request::Open(open)).await?;

        Ok(Standby {
            disk,
            opened: false,
        })
    }
}

/// A replaying disk that has not claimed its journal or mounted a filesystem.
///
/// Dropping it cancels the attachment.
pub struct Standby {
    /// Attach stream retained through promotion.
    disk: Disk,
    /// Set once the daemon has reported that the replay is current, so that the
    /// reply is consumed exactly once however the caller sequences its calls.
    opened: bool,
}

impl Standby {
    /// Wait for replay to reach the head observed at `Open`. Readiness remains
    /// set even if replay later falls behind.
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

    /// Wait for readiness, then fence, re-acknowledge a recovered ACK intent,
    /// finish replay, and mount. `ack_intent` is the ACK intent saved by the
    /// last [`Disk::prepare`], or empty if there is none.
    ///
    /// Fails with [`Error::Fenced`] if another promotion's fence lands while
    /// this one fences. A client which still wants the disk recovers with a new
    /// standby and the same `ack_intent`.
    pub async fn promote(mut self, ack_intent: bytes::Bytes) -> Result<(Disk, std::path::PathBuf)> {
        // The daemon serves `Promote` only once it has sent `Opened`.
        () = self.ready().await?;

        () = self
            .disk
            .send(proto::request::Request::Promote(proto::Promote {
                ack_intent,
            }))
            .await?;

        match self.disk.reply().await? {
            proto::response::Response::Promoted(promoted) => {
                Ok((self.disk, promoted.mount_path.into()))
            }
            reply => Err(Error::unexpected(reply)),
        }
    }
}

/// A disk this caller is the primary of.
///
/// Calling [`Disk::close`] closes the attachment, and dropping this handle
/// cancels it. Either makes the daemon unmount the filesystem and delete the
/// device, but only `close` waits for that cleanup to finish.
pub struct Disk {
    /// Requests sent on this disk's attachment stream.
    requests: tokio::sync::mpsc::Sender<proto::Request>,
    /// Ordered responses from the daemon.
    replies: tonic::Streaming<proto::Response>,
    /// Whether an `Acknowledged` reply is still to be read.
    awaiting_acknowledged: bool,
}

impl Disk {
    /// Make pending disk changes durable and return their ACK intent, which is
    /// empty if nothing changed. Confirm any prior acknowledgement first.
    ///
    /// Flush application buffers and pause logical writes before calling. The
    /// daemon flushes filesystem writes with `syncfs`, but cannot determine
    /// which application writes belong to this transaction.
    ///
    /// Save the exact opaque bytes atomically with the corresponding
    /// application state, then call [`Disk::acknowledge`], even when they're
    /// empty: every prepare is acknowledged before the next. Retain them until
    /// confirmation; after failure, pass them to [`Standby::promote`] so
    /// promotion can finish the committed transaction.
    pub async fn prepare(&mut self) -> Result<bytes::Bytes> {
        () = self.acknowledged().await?;

        match self
            .call(proto::request::Request::Prepare(proto::Prepare {}))
            .await?
        {
            proto::response::Response::Prepared(prepared) => Ok(prepared.ack_intent),
            reply => Err(Error::unexpected(reply)),
        }
    }

    /// Commit the prepared delta, after saving its ACK intent atomically
    /// with the corresponding application state, per [`Disk::prepare`].
    ///
    /// Returns once sent, allowing the next application transaction to begin.
    /// [`Disk::acknowledged`], the next prepare, or close waits for
    /// confirmation and reports any failure.
    pub async fn acknowledge(&mut self) -> Result<()> {
        () = self
            .send(proto::request::Request::Acknowledge(proto::Acknowledge {}))
            .await?;
        self.awaiting_acknowledged = true;

        Ok(())
    }

    /// Waits for the daemon to confirm the commit of the last
    /// [`Disk::acknowledge`], or returns at once when there is none to wait
    /// for.
    ///
    /// Until this returns, the ACK intent is one the caller must still
    /// hand back to [`Standby::promote`] if this attachment fails.
    pub async fn acknowledged(&mut self) -> Result<()> {
        if !self.awaiting_acknowledged {
            return Ok(());
        }
        // Taken before the reply is read, so that a failure is reported once and a
        // retry of it does not wait for a reply the daemon will never send.
        self.awaiting_acknowledged = false;

        match self.reply().await? {
            proto::response::Response::Acknowledged(_acknowledged) => Ok(()),
            reply => Err(Error::unexpected(reply)),
        }
    }

    /// Closes the attachment and waits for the daemon to unmount the filesystem
    /// and delete the device.
    ///
    /// A commit which [`Disk::acknowledge`] left in flight is confirmed first,
    /// so a `close` which follows an `acknowledge` still promises that the
    /// commit landed.
    pub async fn close(mut self) -> Result<()> {
        () = self.acknowledged().await?;
        drop(self.requests);

        match self.replies.message().await.map_err(Error::of)? {
            None => Ok(()),
            Some(response) => Err(Error::Unexpected(format!("{response:?}"))),
        }
    }

    async fn send(&mut self, request: proto::request::Request) -> Result<()> {
        self.requests
            .send(proto::Request {
                request: Some(request),
            })
            .await
            .map_err(|_| Error::Closed)
    }

    async fn reply(&mut self) -> Result<proto::response::Response> {
        match self.replies.message().await.map_err(Error::of)? {
            Some(proto::Response {
                response: Some(reply),
            }) => Ok(reply),
            _ => Err(Error::Closed),
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
