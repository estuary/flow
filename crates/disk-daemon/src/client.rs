use crate::proto;

pub type Result<T> = std::result::Result<T, Error>;

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

    /// Open and promote a disk, returning its handle and absolute mount path.
    ///
    /// The caller must create the journal and preserve its device size across
    /// tenures. The daemon validates the live spec and writes only its recovery
    /// floor. Pass committed but unconfirmed acknowledgements in `recovered_acks`;
    /// see [`Standby::promote`].
    ///
    /// Use [`Client::standby`] to replay without serving the disk yet.
    pub async fn open(
        &self,
        open: proto::Open,
        recovered_acks: std::collections::BTreeMap<String, bytes::Bytes>,
    ) -> Result<(Disk, std::path::PathBuf)> {
        self.standby(open).await?.promote(recovered_acks).await
    }

    /// Start replay without fencing or serving the disk. The journal must exist;
    /// an empty journal parks until promotion. Otherwise replay follows new
    /// commits after backfill, reducing the work left at failover.
    ///
    /// Returns once `Open` is sent. The caller may promote immediately or wait for
    /// [`Standby::ready`]. Preparing, acknowledging, and repairing recovered acks
    /// require promotion.
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

/// A replaying disk that has not claimed its journal or mounted a filesystem.
/// It does not fence the serving tenure.
pub struct Standby {
    /// Tenure stream retained through promotion.
    disk: Disk,
    /// Set once the daemon has reported that the replay is current, so that the reply
    /// is consumed exactly once however the caller sequences its calls.
    opened: bool,
}

impl Standby {
    /// Wait for replay to reach the head observed at `Open`. Readiness remains set
    /// even if replay later falls behind. Promotion need not wait for readiness.
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

    /// Fence, finish replay, repair a recovered acknowledgement, and mount.
    /// May be called during backfill: fencing bounds the remaining replay.
    ///
    /// `recovered_acks` contains committed but unconfirmed values returned by
    /// [`Disk::prepare`], keyed by journal as in Gazette checkpoint ack intents.
    /// The tenure takes only its own journal's entry. Repair waits until promotion
    /// because appending requires the fence.
    pub async fn promote(
        mut self,
        recovered_acks: std::collections::BTreeMap<String, bytes::Bytes>,
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
    /// Requests sent on this disk's tenure stream.
    requests: tokio::sync::mpsc::Sender<proto::Request>,
    /// Ordered responses from the daemon.
    replies: tonic::Streaming<proto::Response>,
    /// Set while an `Acknowledged` is owed, because [`Disk::acknowledge`] does not
    /// wait for one. At most one is ever owed: the daemon confirms it before it
    /// prepares another delta.
    owed: bool,
}

impl Disk {
    /// Make pending disk changes durable and return their unappended acknowledgement,
    /// or `None` if nothing changed. Confirm any prior acknowledgement first.
    ///
    /// Flush application buffers and pause logical writes before calling. The
    /// daemon flushes filesystem writes with `syncfs`, but cannot determine which
    /// application writes belong to this transaction.
    ///
    /// Save the exact opaque bytes atomically with the corresponding application
    /// state, then pass them to [`Disk::acknowledge`]. Retain them until confirmation;
    /// after failure, supply them in `Promote.recovered_acks` so promotion can finish
    /// the committed transaction.
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

    /// Send the prepared acknowledgement after saving it atomically with the
    /// corresponding application state, per [`Disk::prepare`].
    ///
    /// Returns once sent, allowing the next application transaction to begin.
    /// [`Disk::acknowledged`], the next prepare, or close waits for confirmation
    /// and reports any failure.
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
