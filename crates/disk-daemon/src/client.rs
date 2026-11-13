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
    #[error("could not open the disk's block device {path:?}: {source}")]
    Device {
        path: std::path::PathBuf,
        source: std::io::Error,
    },
    #[error("{path:?} is no longer the disk's device: its diskseq is {found}, not {promoted}")]
    Replaced {
        path: std::path::PathBuf,
        promoted: u64,
        found: u64,
    },
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

    /// Open and promote a disk, returning its handle and the absolute path of its
    /// block device. `ack_intent` is as for [`Standby::promote`].
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
            awaiting: Awaiting::Nothing,
            _device: None,
        };
        () = disk.send(proto::request::Request::Open(open)).await?;

        Ok(Standby {
            disk,
            opened: false,
        })
    }
}

/// A replaying disk that has not claimed its journal or started a device.
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
    /// finish replay, and start the device. Returns the disk's handle and the
    /// absolute path of its block device. `ack_intent` is the ACK intent saved by
    /// the last [`Disk::prepare`], or empty if there is none.
    ///
    /// Fails with [`Error::Fenced`] if another promotion's fence lands while
    /// this one fences. A client which still wants the disk recovers with a new
    /// standby and the same `ack_intent`.
    ///
    /// Fails with `Error::Device` if it can't open the path, or with `Error::Replaced`
    /// if the path names another device than the one the daemon started.
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
                let device_path = std::path::PathBuf::from(promoted.device_path);
                self.disk._device = Some(open_device(&device_path, promoted.diskseq)?);
                Ok((self.disk, device_path))
            }
            reply => Err(Error::unexpected(reply)),
        }
    }
}

/// Open the block device at `path`, and require it to be the device of `diskseq`.
fn open_device(path: &std::path::Path, diskseq: u64) -> Result<std::fs::File> {
    /// `BLKGETDISKSEQ` of linux/fs.h.
    const BLKGETDISKSEQ: rustix::ioctl::Opcode = rustix::ioctl::opcode::read::<u64>(0x12, 128);

    let failed = |source| Error::Device {
        path: path.to_path_buf(),
        source,
    };
    let device = std::fs::File::open(path).map_err(failed)?;

    // SAFETY: `BLKGETDISKSEQ` writes a `u64`.
    let found = unsafe {
        rustix::ioctl::ioctl(&device, rustix::ioctl::Getter::<BLKGETDISKSEQ, u64>::new())
    }
    .map_err(|errno| failed(errno.into()))?;

    if found != diskseq {
        return Err(Error::Replaced {
            path: path.to_path_buf(),
            promoted: diskseq,
            found,
        });
    }
    Ok(device)
}

/// A disk this caller is the primary of.
///
/// Calling [`Disk::close`] closes the attachment, and dropping this handle
/// cancels it. Either makes the daemon stop and delete the device, but only
/// `close` waits for that. Release the device first: anything still using it
/// then gets I/O errors.
pub struct Disk {
    /// Requests sent on this disk's attachment stream.
    requests: tokio::sync::mpsc::Sender<proto::Request>,
    /// Ordered responses from the daemon.
    replies: tonic::Streaming<proto::Response>,
    /// A reply still to be read, which a cancelled call leaves for the next.
    awaiting: Awaiting,
    /// The block device, held open so its path can't go to another device.
    _device: Option<std::fs::File>,
}

enum Awaiting {
    Nothing,
    Prepared,
    Acknowledged,
}

impl Disk {
    /// Make pending disk changes durable and return their ACK intent, which is
    /// empty if nothing changed. Confirm any prior acknowledgement first.
    ///
    /// Before calling, flush the writes this delta should hold to the device, as
    /// with a `syncfs` of a filesystem on it, and pause writes. The delta holds
    /// what the device completed before the daemon received `Prepare`.
    ///
    /// Keep writes paused until [`Disk::acknowledge`]. The daemon records
    /// nothing while the prepared delta awaits its acknowledgement. A write
    /// which reaches the device in that time waits there, and the device serves
    /// no other request, reads included, until the acknowledgement lands. So
    /// sending the acknowledgement must never wait on disk I/O.
    ///
    /// Save the exact opaque bytes atomically with the corresponding
    /// application state, then call [`Disk::acknowledge`], even when they're
    /// empty: every prepare is acknowledged before the next. At every
    /// promotion, pass the last bytes saved to [`Standby::promote`], so it can
    /// finish a commit that a failure interrupted.
    ///
    /// After a call is cancelled, the next returns the cancelled call's ACK intent.
    pub async fn prepare(&mut self) -> Result<bytes::Bytes> {
        () = self.acknowledged().await?;

        if let Awaiting::Nothing = self.awaiting {
            () = self
                .send(proto::request::Request::Prepare(proto::Prepare {}))
                .await?;
            self.awaiting = Awaiting::Prepared;
        }
        let reply = self.reply().await;
        self.awaiting = Awaiting::Nothing;

        match reply? {
            proto::response::Response::Prepared(prepared) => Ok(prepared.ack_intent),
            reply => Err(Error::unexpected(reply)),
        }
    }

    /// Commit the prepared delta, after saving its ACK intent atomically
    /// with the corresponding application state, per [`Disk::prepare`].
    ///
    /// Never waits, so the next application transaction can begin at once.
    /// [`Disk::acknowledged`], the next prepare, or close waits for
    /// confirmation and reports any failure, including a failure to send.
    pub fn acknowledge(&mut self) {
        assert!(
            matches!(self.awaiting, Awaiting::Nothing),
            "acknowledge without a completed prepare"
        );
        let request = proto::Request {
            request: Some(proto::request::Request::Acknowledge(proto::Acknowledge {})),
        };
        match self.requests.try_send(request) {
            Ok(()) => (),
            // The attachment ended, so the request wasn't sent. The daemon's reason is
            // in the reply stream, and `Disk::acknowledged` reads and reports it.
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => (),
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                unreachable!("an earlier request is still unsent")
            }
        }
        self.awaiting = Awaiting::Acknowledged;
    }

    /// Waits for the daemon to confirm the commit of the last
    /// [`Disk::acknowledge`], or returns at once when there is none to wait
    /// for.
    pub async fn acknowledged(&mut self) -> Result<()> {
        let Awaiting::Acknowledged = self.awaiting else {
            return Ok(());
        };
        let reply = self.reply().await;
        self.awaiting = Awaiting::Nothing;

        match reply? {
            proto::response::Response::Acknowledged(_acknowledged) => Ok(()),
            reply => Err(Error::unexpected(reply)),
        }
    }

    /// Closes the attachment and waits for the daemon to stop and delete the
    /// device.
    ///
    /// A commit which [`Disk::acknowledge`] left in flight is confirmed first,
    /// so a `close` which follows an `acknowledge` still promises that the
    /// commit landed.
    pub async fn close(mut self) -> Result<()> {
        () = self.acknowledged().await?;

        // A cancelled prepare's delta is never acknowledged, so it doesn't commit.
        if let Awaiting::Prepared = self.awaiting {
            match self.reply().await? {
                proto::response::Response::Prepared(_prepared) => (),
                reply => return Err(Error::unexpected(reply)),
            }
        }
        drop(self.requests);

        match self.replies.message().await.map_err(Error::of)? {
            None => Ok(()),
            Some(response) => Err(Error::Unexpected(format!("{response:?}"))),
        }
    }

    async fn send(&mut self, request: proto::request::Request) -> Result<()> {
        let request = proto::Request {
            request: Some(request),
        };
        if self.requests.send(request).await.is_ok() {
            return Ok(());
        }

        match self.replies.message().await.map_err(Error::of)? {
            None => Err(Error::Closed),
            Some(response) => Err(Error::Unexpected(format!("{response:?}"))),
        }
    }

    async fn reply(&mut self) -> Result<proto::response::Response> {
        match self.replies.message().await.map_err(Error::of)? {
            Some(proto::Response {
                response: Some(reply),
            }) => Ok(reply),
            _ => Err(Error::Closed),
        }
    }
}
