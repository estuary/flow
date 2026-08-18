//! A client of the session gRPC, for a program which serves disks.
//!
//! The daemon is one participant of a two-phase commit, and its client is the
//! coordinator. This client holds no protocol state of its own: the daemon owns the
//! rules of the exchange, and reports a request out of turn as
//! `FAILED_PRECONDITION`. [`Error`] separates a request which can never succeed from
//! a disk whose journal was taken and from one which may work on a retry.
//!
//! `examples/basic.rs` is the smallest use of it, and
//! `examples/two_phase_commit.rs` coordinates four disks. `tests/daemon.rs` speaks
//! the protocol directly instead, because it asserts what the daemon does with
//! requests this type cannot express.

use crate::proto;

/// Why a session call failed, which decides what its client may do next.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The daemon could not be reached.
    #[error("cannot reach the disk daemon")]
    Connect(#[from] tonic::transport::Error),
    /// The request broke a rule of the protocol, so no retry of it can succeed.
    #[error("invalid request: {}", .0.message())]
    Invalid(tonic::Status),
    /// Another session claimed this disk's journal. This session is over, and the
    /// session which displaced it holds the disk.
    #[error("another session holds this disk: {}", .0.message())]
    Fenced(tonic::Status),
    /// A broker refused the session's credential. A new session may succeed with a
    /// fresh one.
    #[error("a broker refused the credential: {}", .0.message())]
    Unauthorized(tonic::Status),
    /// A broker could not serve the session, for a reason which may not recur.
    #[error("a broker is unavailable: {}", .0.message())]
    Unavailable(tonic::Status),
    /// The session failed for a reason of the daemon or of its host.
    #[error("the session failed: gRPC code: {:?}, message: {}", .0.code(), .0.message())]
    Failed(tonic::Status),
    /// The daemon ended the stream, so this session is over.
    #[error("the session has ended")]
    Ended,
    /// The daemon replied with something the protocol does not allow here.
    #[error("the daemon replied with {0}")]
    Unexpected(String),
}

impl Error {
    // `gazette::Error` and `catalog_stats::Error` also carry `with_attempt`, which
    // stamps an attempt count onto a `RetryError`. This client has none, because it
    // retries nothing: a failed disk is replaced by opening another, which is its
    // caller's decision to make.

    /// Whether a new session may succeed where this one failed.
    ///
    /// The codes here do not mean what they mean to every client of this workspace.
    /// `ABORTED` is retryable for BigTable, per `catalog_stats::Error`. Here it is a
    /// lost fence, which no retry undoes: another session holds the disk, and only
    /// the writer which should hold it may open the disk again.
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Connect(_) | Self::Unavailable(_))
    }

    /// Sort a status by what its client may do next. The codes are the ones
    /// `session::failed` produces.
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

pub type Result<T> = std::result::Result<T, Error>;

/// A recovery floor the daemon reported, where zero means it reported none.
///
/// A client persists the greatest floor it has been given, hands it back as the next
/// `Open`'s `floor_hint`, and may delete journal fragments below it. That store is
/// best-effort: a floor is a seek and never a filter, so losing one costs replay work
/// and nothing else.
fn floor(reported: u64) -> Option<u64> {
    (reported != 0).then_some(reported)
}

/// A connection to one daemon.
///
/// Every session of a client shares its connection, which multiplexes them over one
/// HTTP/2 transport, so a program which serves many disks connects once. A client is
/// cheap to clone.
///
/// One connection is also one identity: the daemon reads the peer credential of the
/// socket to decide who owns each mount it serves.
#[derive(Clone)]
pub struct Client {
    channel: tonic::transport::Channel,
}

impl Client {
    /// Connect to the daemon which listens on `uds_path`.
    pub async fn connect(uds_path: &std::path::Path) -> Result<Self> {
        let channel =
            tonic::transport::Endpoint::from_shared(format!("unix://{}", uds_path.display()))?
                .connect()
                .await?;

        Ok(Self { channel })
    }

    /// Open the disk of `open`.
    ///
    /// Returns the disk, the absolute path of its mounted filesystem, and the
    /// recovery floor this recovery derived, if it derived one.
    ///
    /// The daemon gives that filesystem to the user of this process, so a client
    /// needs no privilege of its own to read and write the mount.
    pub async fn open(&self, open: proto::Open) -> Result<(Disk, std::path::PathBuf, Option<u64>)> {
        let (requests, receiver) = tokio::sync::mpsc::channel(1);
        let replies = proto_grpc::disk::disk_client::DiskClient::new(self.channel.clone())
            .session(tokio_stream::wrappers::ReceiverStream::new(receiver))
            .await
            .map_err(Error::of)?
            .into_inner();

        let mut disk = Disk { requests, replies };

        match disk.call(proto::request::Request::Open(open)).await? {
            proto::response::Response::Opened(opened) => {
                Ok((disk, opened.mount_path.into(), floor(opened.floor)))
            }
            reply => Err(Error::unexpected(reply)),
        }
    }
}

/// One disk, held for the life of one bidirectional stream.
///
/// This is a client's side of a disk. [`crate::disk::Disk`] is the daemon's side of
/// the same thing: a sparse image, a `ublk` device, and the thread which owns them.
/// A client never holds that one.
///
/// The disk lasts as long as this value. Dropping it, or [`Disk::close`], has the
/// daemon unmount the filesystem and delete the device.
pub struct Disk {
    requests: tokio::sync::mpsc::Sender<proto::Request>,
    replies: tonic::Streaming<proto::Response>,
}

impl Disk {
    /// Cut a point-in-time boundary of the device, and finish the delta before it.
    ///
    /// Every data record of that delta is broker-confirmed when this returns, and
    /// the acknowledgement which commits them is not appended. The delta is durable
    /// and uncommitted, which is a prepared state. `None` reports that the disk did
    /// not change, so there is nothing to commit.
    ///
    /// The bytes it returns are opaque, and only these exact bytes commit this delta.
    /// Store them with the client's own state, in one atomic external commit, and
    /// then hand them back to [`Disk::commit`] unchanged. A client which stops after
    /// that external commit hands them instead to the `recovered_acks` of its next
    /// `Open`, which is what makes the delta durable after all.
    pub async fn prepare(&mut self) -> Result<Option<bytes::Bytes>> {
        match self
            .call(proto::request::Request::Prepare(proto::Prepare {}))
            .await?
        {
            proto::response::Response::Prepared(cut) if cut.ack.is_empty() => Ok(None),
            proto::response::Response::Prepared(cut) => Ok(Some(cut.ack)),
            reply => Err(Error::unexpected(reply)),
        }
    }

    /// Append `ack`, which [`Disk::prepare`] returned, and wait for the broker to
    /// confirm it. The delta is durable disk state once this returns.
    ///
    /// The client must already have made `ack` durable in its own store, in the same
    /// atomic commit as the state which that delta belongs to.
    ///
    /// Returns the recovery floor this commit established, if it established one.
    pub async fn commit(&mut self, ack: bytes::Bytes) -> Result<Option<u64>> {
        match self
            .call(proto::request::Request::Commit(proto::Commit { ack }))
            .await?
        {
            proto::response::Response::Committed(committed) => Ok(floor(committed.floor)),
            reply => Err(Error::unexpected(reply)),
        }
    }

    /// Replace the broker endpoint and credential of this session.
    ///
    /// This has no reply, so a broker which cannot be reached surfaces at the next
    /// prepare. Send it well before the current credential expires.
    pub async fn broker(&mut self, broker: proto::Broker) -> Result<()> {
        self.send(proto::request::Request::Broker(broker)).await
    }

    /// End the session. The daemon closes its half only once the disk is unmounted
    /// and its device is deleted, so this waits for that teardown.
    pub async fn close(mut self) -> Result<()> {
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

    async fn call(
        &mut self,
        request: proto::request::Request,
    ) -> Result<proto::response::Response> {
        () = self.send(request).await?;

        match self.replies.message().await.map_err(Error::of)? {
            Some(proto::Response {
                response: Some(reply),
            }) => Ok(reply),
            _ => Err(Error::Ended),
        }
    }
}
