//! The tenure service. One bidirectional stream serves exactly one disk.
//!
//! A tenure is a state machine over its stream. It begins with `Open`, which
//! creates the image and begins rebuilding it from the journal. `Promote` claims the
//! journal, finishes the replay, and creates the device, the filesystem, and the
//! mount over it. It then serves `Prepare` and `Acknowledge` pairs, which move the
//! disk's durable state forward atomically with the client's own commit. It ends
//! when the stream ends, for any reason at all, by unmounting and destroying
//! everything it made.
//!
//! One request is served at a time, and its replies are sent before the next one
//! is read. A client may pipeline, so a request queued behind `Promote` is served
//! once the disk is promoted.
//!
//! Every error is terminal. A device or broker failure is terminal because the
//! disk's contents can no longer be trusted to reach its journal. A protocol
//! violation is terminal because the client has lost track of which delta it owes
//! an acknowledgement. What differs is the code the stream ends with. That code is
//! the only part of a failure a client can act on. See `failed`.

use crate::device::Device;
use crate::filesystem::{self, Mount};
use crate::image::Image;
use crate::journal::{self, Writer};
use crate::proto;
use crate::ublk::Control;
use anyhow::Context;

/// Serves the `Disk` gRPC service.
pub struct Service {
    daemon: std::sync::Arc<crate::daemon::Config>,
    control: std::sync::Arc<Control>,
    /// Cancelled when the daemon drains, so that no tenure outlives it. Each
    /// tenure takes a child of this token and cancels it during teardown.
    draining: tokio_util::sync::CancellationToken,
}

impl Service {
    pub fn new(
        daemon: std::sync::Arc<crate::daemon::Config>,
        control: std::sync::Arc<Control>,
        draining: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self {
            daemon,
            control,
            draining,
        }
    }

    pub fn into_tonic_service(self) -> proto_grpc::disk::disk_server::DiskServer<Self> {
        proto_grpc::disk::disk_server::DiskServer::new(self)
    }
}

#[tonic::async_trait]
impl proto_grpc::disk::disk_server::Disk for Service {
    type TenureStream = tokio_stream::wrappers::ReceiverStream<tonic::Result<proto::Response>>;

    async fn tenure(
        &self,
        request: tonic::Request<tonic::Streaming<proto::Request>>,
    ) -> tonic::Result<tonic::Response<Self::TenureStream>> {
        let (responses, stream) = tokio::sync::mpsc::channel(1);

        let (daemon, control) = (self.daemon.clone(), self.control.clone());
        let ended = self.draining.child_token();

        // The mount this tenure returns belongs to the client rather than to this
        // daemon, so that a client needs no privilege of its own. A Unix socket
        // carries the peer's credential, which is the one identity a client cannot
        // claim falsely.
        let owner = request
            .extensions()
            .get::<tonic::transport::server::UdsConnectInfo>()
            .and_then(|info| info.peer_cred)
            .map(|cred| (cred.uid(), cred.gid()));

        // The tenure owns its disk, so it outlives this call and tears the disk
        // down as it ends. A client which drops the stream both ends `requests`
        // and closes `responses`. Either of those ends the tenure.
        let tenure = Tenure {
            daemon,
            control,
            owner,
            journal: String::new(),
            ended,
            state: State::Fresh,
        };
        tokio::spawn(tenure.run(request.into_inner(), responses));

        Ok(tonic::Response::new(
            tokio_stream::wrappers::ReceiverStream::new(stream),
        ))
    }
}

struct Tenure {
    daemon: std::sync::Arc<crate::daemon::Config>,
    control: std::sync::Arc<Control>,
    /// User and group which own the mount this tenure serves, taken from the
    /// credential of its stream.
    owner: Option<(u32, u32)>,
    /// Journal of the disk this tenure opened, and empty before `Open`. A daemon
    /// serves many tenures at once, so a failure names the disk it ended.
    journal: String,
    /// Cancelled when this tenure is over. That happens when the daemon drains,
    /// or when the tenure's own teardown begins. Every broker call the tenure
    /// makes gives up on this token, because each one retries indefinitely.
    ended: tokio_util::sync::CancellationToken,
    state: State,
}

enum State {
    /// Before `Open`, which must be a tenure's first request.
    Fresh,
    /// The journal is accepted and its replay runs. The disk is not served, and this
    /// tenure has not claimed the journal. `Promote` moves it on.
    ///
    /// Boxed because an `Opening` carries a whole journal writer's state, which is
    /// several times the size of a `Serving`.
    Standing(Box<Standing>),
    Serving(Serving),
}

/// A tenure whose replay runs, before it is promoted.
struct Standing {
    opening: journal::Opening,
    playback: journal::playback::Playback,
    /// Set once `Opened` has been sent, which happens exactly once.
    ///
    /// It is also what tells a `Promote` whether it must wait for the replay:
    /// `Playback::caught_up` resolves once, and afterwards pends or reports the
    /// failure which ended the playback.
    opened: bool,
}

/// What a tenure acts on next.
enum Event {
    /// A request to serve, or `None` where the client closed its half of the stream
    /// and the tenure is over.
    ///
    /// Boxed because a `Request` carries whichever of the protocol's messages is
    /// largest, beside a `CaughtUp` which carries none.
    Request(Option<Box<proto::Request>>),
    /// The replay has read the history of the journal and now follows it.
    CaughtUp,
}

/// What one open disk consists of.
///
/// The fields are declared in the order [`teardown`] runs them, which is also
/// the order they drop in. The filesystem unmounts before the device under it
/// stops. The writer outlives both, because an unmount writes.
struct Serving {
    mount: Mount,
    device: Device,
    writer: Writer,
}

impl Tenure {
    async fn run(
        mut self,
        mut requests: tonic::Streaming<proto::Request>,
        responses: tokio::sync::mpsc::Sender<tonic::Result<proto::Response>>,
    ) {
        let outcome = self.serve(&mut requests, &responses).await;

        // "The tenure is over" now means one thing, whatever state it ended in.
        // `teardown` has nothing to unmount for a tenure which was still standing by,
        // and its playback would otherwise go on tailing the journal — holding the
        // image and the delta buffer open — until the daemon itself exits. A serving
        // tenure's teardown cancels this token anyway, through `Writer::abandon`.
        () = self.ended.cancel();

        // The disk is torn down before the failure which ended the tenure is
        // reported, so that a client which sees its tenure end sees a disk
        // which is already gone. It is also how a draining daemon waits for its
        // tenures. This stream stays open until its disk is destroyed.
        let state = std::mem::replace(&mut self.state, State::Fresh);
        () = teardown(state).await;

        if let Err(status) = outcome {
            tracing::warn!(journal = self.journal, %status, "tenure failed");
            _ = responses.send(Err(status)).await;
        }
    }

    /// Serve requests until the client stops sending them, or something fails.
    ///
    /// One request is served at a time, and its replies are sent before the next is
    /// read. A client may still pipeline: the requests queue in the stream, and the
    /// replies come back in the order the requests were sent. A request queued behind
    /// a `Promote` is therefore served once the disk is promoted, because that handler
    /// waits for the replay itself and answers `Opened` and `Promoted` together.
    ///
    /// A failure ends the stream with its status rather than a reply. A client of a
    /// failed tenure has to open another one and repair the acknowledgement it holds
    /// either way, and Gazette de-duplicates a repair of one which had in fact landed.
    async fn serve(
        &mut self,
        requests: &mut tonic::Streaming<proto::Request>,
        responses: &tokio::sync::mpsc::Sender<tonic::Result<proto::Response>>,
    ) -> tonic::Result<()> {
        let cancelled = || tonic::Status::cancelled("the client dropped its tenure");

        loop {
            let replies = match self.next(requests).await? {
                Event::Request(None) => return Ok(()), // The client closed its half.
                Event::Request(Some(request)) => self.request(*request).await?,
                Event::CaughtUp => {
                    let State::Standing(standing) = &mut self.state else {
                        panic!("only a standing tenure waits on its playback");
                    };
                    standing.opened = true;

                    vec![reply(proto::response::Response::Opened(proto::Opened {}))]
                }
            };

            for reply in replies {
                responses.send(Ok(reply)).await.map_err(|_| cancelled())?;
            }
        }
    }

    /// Wait for whatever this tenure must act on next.
    ///
    /// A standing tenure waits on its playback as well as on its client. That is
    /// what lets `Opened` arrive without a client having asked for it, and it is also
    /// how a tenure which stands by learns that its playback died.
    ///
    /// This does not race a request which is already being served. What that request
    /// waits on gives up instead: its broker calls, and the replay a `Promote` waits
    /// to become current. Those are the only waits a tenure has which no timeout
    /// bounds.
    async fn next(
        &mut self,
        requests: &mut tonic::Streaming<proto::Request>,
    ) -> tonic::Result<Event> {
        let ended = self.ended.clone();

        let playback = match &mut self.state {
            State::Standing(standing) => Some(&mut standing.playback),
            _ => None,
        };
        let caught_up = async {
            match playback {
                Some(playback) => playback.caught_up().await,
                None => std::future::pending().await,
            }
        };

        tokio::select! {
            _ = ended.cancelled() => Err(tonic::Status::unavailable("the daemon is draining")),
            request = requests.message() => Ok(Event::Request(request?.map(Box::new))),
            caught_up = caught_up => {
                () = caught_up.map_err(failed)?;
                Ok(Event::CaughtUp)
            }
        }
    }

    async fn request(&mut self, request: proto::Request) -> tonic::Result<Vec<proto::Response>> {
        use proto::request::Request;
        use proto::response::Response;

        let request = request
            .request
            .ok_or_else(|| tonic::Status::invalid_argument("request carries no message"))?;

        let response = match request {
            Request::Open(open) => {
                if !matches!(self.state, State::Fresh) {
                    return Err(tonic::Status::failed_precondition(
                        "a tenure opens exactly one disk",
                    ));
                }
                let standing = self.open(open).await.map_err(failed)?;
                self.state = State::Standing(Box::new(standing));

                // `Opened` follows when the replay has read the journal's history.
                Vec::new()
            }
            Request::Promote(proto::Promote { recovered_acks }) => {
                // The state is checked before it is taken. Replacing a `Serving` here
                // would drop its disk out of order, ahead of the teardown which
                // unmounts it.
                if !matches!(self.state, State::Standing(_)) {
                    return Err(tonic::Status::failed_precondition(
                        "a tenure promotes a disk which it opened, and exactly once",
                    ));
                }
                // `Fresh` stands in for as long as this handler runs. Every failure
                // within it is terminal, no request is read meanwhile, and `run`
                // cancels `ended` afterwards — so a playback dropped here stops
                // tailing the journal rather than outliving the tenure.
                let State::Standing(standing) = std::mem::replace(&mut self.state, State::Fresh)
                else {
                    panic!("the state was just found standing");
                };
                let Standing {
                    opening,
                    mut playback,
                    opened,
                } = *standing;

                // The claim comes now, and not when the replay is current. A client
                // which pipelines this behind its `Open` is asking for the disk at
                // once, and fencing bounds what is left to read: a head no other
                // writer can move is one the replay converges on rather than chases.
                let claimed = opening.claim_journal().await.map_err(failed)?;

                let mut replies = Vec::new();

                if !opened {
                    // `Opened` is owed first, so this promotion waits for it here
                    // rather than resuming from `next`. A drain reaches this wait
                    // through the playback, which reports `Ended` as its failure, so
                    // there is nothing for the tenure's own token to race.
                    () = playback.caught_up().await.map_err(failed)?;

                    replies.push(reply(Response::Opened(proto::Opened {})));
                }

                let serving = self
                    .serve_disk(claimed, playback, recovered_acks)
                    .await
                    .map_err(failed)?;

                let promoted = proto::Promoted {
                    mount_path: serving.mount.path().display().to_string(),
                };
                self.state = State::Serving(serving);
                replies.push(reply(Response::Promoted(promoted)));

                replies
            }
            Request::Prepare(proto::Prepare {}) => {
                let ack = self.serving()?.prepare().await.map_err(failed)?;

                vec![reply(Response::Prepared(proto::Prepared {
                    ack: ack.unwrap_or_default(),
                }))]
            }
            Request::Acknowledge(proto::Acknowledge { ack }) => {
                () = self
                    .serving()?
                    .writer
                    .acknowledge(ack)
                    .await
                    .map_err(failed)?;

                vec![reply(Response::Acknowledged(proto::Acknowledged {}))]
            }
        };

        Ok(response)
    }

    /// Begin the replay of the disk `open` names, without claiming or mounting
    /// anything.
    ///
    /// The journal must already exist: the daemon creates none, so an `Open` which
    /// names one nothing created is refused here. Its live specification is checked
    /// before a device exists, because a specification a disk could not be recovered
    /// from is a disk which can never prepare a delta.
    async fn open(&mut self, open: proto::Open) -> anyhow::Result<Standing> {
        let proto::Open {
            journal,
            device_size,
        } = open;

        crate::ensure_valid!(!journal.is_empty(), "the tenure named no journal");

        let blocks = blocks(device_size)?;
        self.journal = journal.clone();

        let mut opening =
            journal::Opening::new(&self.daemon.client, journal, self.ended.clone()).await?;

        let image = Image::create(&self.daemon.image_dir, blocks)
            .with_context(|| format!("creating an image in {:?}", self.daemon.image_dir))?;

        let buffer = journal::buffer::Buffer::create(&self.daemon.image_dir)
            .with_context(|| format!("creating a delta buffer in {:?}", self.daemon.image_dir,))?;
        let playback = opening.play(image, buffer);

        Ok(Standing {
            opening,
            playback,
            opened: false,
        })
    }

    /// Finish `playback` and serve the disk it rebuilt. The journal is claimed
    /// already, by the `Promote` which reached this tenure.
    ///
    /// A disk with committed state is served from what the replay rebuilt. A disk
    /// without it is formatted instead. Either way the daemon's own setup writes: an
    /// `mkfs` on a fresh disk, and the bookkeeping ext4 does at any mount. Those are
    /// ordinary mutations of a writer which is already running, and this tenure cuts
    /// and acknowledges them itself before it answers `Promoted`. A client's
    /// acknowledgements therefore cover only its own writes, it owes nothing for a
    /// disk it never writes, and a reopen of one recovers the filesystem rather than
    /// formatting it again.
    async fn serve_disk(
        &self,
        mut claimed: journal::Claimed,
        playback: journal::playback::Playback,
        recovered_acks: Vec<bytes::Bytes>,
    ) -> anyhow::Result<Serving> {
        // A horizon the replay leaves open is handed to the disk, which resumes it
        // rather than opening a new one over whatever this tenure finds allocated.
        // Its offset stays with the writer `claimed` goes on to build.
        let (image, journal::Recovered { recovered, horizon }) =
            claimed.promote(playback, recovered_acks).await?;

        let control = self.control.clone();
        let policy = self.daemon.horizon;

        // Creating a device is a handshake with the kernel and with the thread
        // which will own it. Neither handshake is async.
        let (device, captured) = tokio::task::spawn_blocking(move || {
            Device::create(&control, image, crate::ublk::QUEUE_DEPTH, horizon, policy)
        })
        .await??;

        let compactor = Some(device.compactor()?);
        let block_path = device.block_path();
        let mount_path = self.daemon.mount_dir.join(format!(
            "{}{}",
            crate::daemon::MOUNT_PREFIX,
            device.dev_id()
        ));

        // The writer runs before anything writes to the device, because the capture
        // channel is bounded and a mutation nothing takes parks the device. That is
        // true of a fresh disk's `mkfs` as much as of a recovered disk's mount.
        let writer = claimed.serve(captured, compactor);

        if !recovered {
            () = filesystem::format(&block_path, self.owner, filesystem::MKFS_TIMEOUT).await?;
        }
        let mount = Mount::new(
            &block_path,
            mount_path,
            self.owner,
            filesystem::MOUNT_TIMEOUT,
        )
        .await?;

        let mut serving = Serving {
            mount,
            device,
            writer,
        };

        // The bootstrap commit. It is the same cut a client's `Prepare` makes, so
        // whatever the format and the mount wrote is committed state of the journal
        // before the client is told the disk exists, and nothing of the daemon's own
        // is left for a client's acknowledgement to carry or for an idle restart to
        // orphan. A mount which wrote nothing is an empty delta, and owes nothing.
        if let Some(ack) = serving.prepare().await? {
            () = serving.writer.acknowledge(ack).await?;
        }

        tracing::info!(
            dev_id = serving.device.dev_id(),
            mount = ?serving.mount.path(),
            recovered,
            "opened a disk",
        );

        Ok(serving)
    }

    fn serving(&mut self) -> tonic::Result<&mut Serving> {
        match &mut self.state {
            State::Serving(serving) => Ok(serving),
            State::Standing(_) => Err(tonic::Status::failed_precondition(
                "this tenure has not promoted the disk it opened, so it is not its writer",
            )),
            State::Fresh => Err(tonic::Status::failed_precondition(
                "the first request of a tenure must be Open",
            )),
        }
    }
}

/// One response of the tenure stream.
fn reply(response: proto::response::Response) -> proto::Response {
    proto::Response {
        response: Some(response),
    }
}

/// Unmount, destroy the device, and drop the image.
async fn teardown(state: State) {
    let State::Serving(Serving {
        mut mount,
        mut device,
        writer,
    }) = state
    else {
        return;
    };

    // This tenure prepares nothing more. The writer takes what the unmount
    // mutates and then discards it.
    () = writer.abandon();

    let dev_id = device.dev_id();

    if let Err(err) = mount.unmount(filesystem::MOUNT_TIMEOUT).await {
        tracing::error!(?err, dev_id, "failed to unmount a disk");
    }

    match tokio::task::spawn_blocking(move || device.stop()).await {
        Ok(Ok(_image)) => (),
        Ok(Err(err)) => tracing::error!(?err, dev_id, "failed to stop a device"),
        Err(panic) => tracing::error!(?panic, dev_id, "panicked stopping a device"),
    }
    drop(writer);

    tracing::info!(dev_id, "closed a disk");
}

impl Serving {
    /// Cut a point-in-time boundary of the disk and finish the delta
    /// before it.
    ///
    /// The cut runs in this order. The mount flushes, admission closes, and every
    /// mutation which was admitted lands. A mutation is captured before it is
    /// applied, so each one then falls entirely before or after the boundary. The
    /// writer can therefore finish exactly the delta which precedes it.
    ///
    /// Admission resumes as soon as the acknowledgement exists. The mutations
    /// admitted from then on belong to the next delta. The writer takes none of them
    /// until this acknowledgement is appended, so a device which writes more than
    /// the capture channel holds in the meantime waits for `Acknowledge`.
    async fn prepare(&mut self) -> anyhow::Result<Option<bytes::Bytes>> {
        let mount = self.mount.path().to_path_buf();

        tokio::task::spawn_blocking(move || filesystem::sync(&mount))
            .await?
            .context("syncing a disk's filesystem")?;

        () = self.device.close_admission().await?;
        let prepared = self.writer.prepare().await;

        // Admission resumes even where the prepare failed. The unmount which
        // follows a failed tenure writes.
        if let Err(err) = self.device.resume_admission() {
            let dev_id = self.device.dev_id();
            tracing::error!(?err, dev_id, "failed to resume a disk's admission");
        }
        prepared
    }
}

/// Block count of a device. `device_size` is the one durable geometry a tenure
/// supplies, because the block size is [`crate::BLOCK_SIZE`] for every disk.
fn blocks(device_size: u64) -> anyhow::Result<u32> {
    crate::ensure_valid!(
        device_size != 0 && device_size.is_multiple_of(crate::BLOCK_SIZE as u64),
        "device size {device_size} must be a non-zero multiple of the {} byte block size",
        crate::BLOCK_SIZE,
    );
    let blocks = device_size / crate::BLOCK_SIZE as u64;

    crate::ensure_valid!(
        blocks <= u32::MAX as u64,
        "a device of {blocks} blocks exceeds the 2^32 which a chunk indexes",
    );
    Ok(blocks as u32)
}

/// gRPC code of a failure which ends a tenure.
///
/// A client cannot act on a message, so the code is the contract:
///
/// - `INVALID_ARGUMENT` is what the tenure asked for. A retry cannot succeed.
/// - `ABORTED` is a lost fence. Another tenure owns this disk, and this one must
///   not take it back.
/// - `UNAUTHENTICATED` is a credential the broker refused. A client should
///   refresh it and open again.
/// - `UNAVAILABLE` is a broker this daemon could not reach, or a tenure the
///   daemon's drain cut short. Another host may reach that broker, or serve
///   that disk.
/// - `INTERNAL` is everything else, which is the daemon or its host failing.
///
/// `Tenure::request` reports a tenure's own state as `FAILED_PRECONDITION`, and
/// that never reaches here. The crate README says what a client should do with
/// each code.
fn failed(err: anyhow::Error) -> tonic::Status {
    let code = match err
        .chain()
        .find_map(|cause| cause.downcast_ref::<crate::Failure>())
    {
        Some(crate::Failure::Invalid(_)) => tonic::Code::InvalidArgument,
        Some(crate::Failure::OutOfOrder(_)) => tonic::Code::FailedPrecondition,
        Some(crate::Failure::Ended(_)) => tonic::Code::Unavailable,
        // Anything the tenure did not bring on itself is the daemon, its host, or
        // its brokers, and only a broker failure carries a code beyond `INTERNAL`.
        None => broker_code(&err),
    };

    tonic::Status::new(code, format!("{err:#}"))
}

/// gRPC code of a failure which is not the tenure's own, per [`failed`].
fn broker_code(err: &anyhow::Error) -> tonic::Code {
    match err
        .chain()
        .find_map(|cause| cause.downcast_ref::<gazette::Error>())
    {
        Some(gazette::Error::BrokerStatus(proto_gazette::broker::Status::RegisterMismatch)) => {
            tonic::Code::Aborted
        }
        // `UNAUTHENTICATED` is not a promise that every credential problem
        // arrives this way. A broker may refuse whatever it was doing rather
        // than the credential. Gazette answers an expired token on an append
        // with `DeadlineExceeded`, because the pipeline the append waited for
        // is what timed out.
        Some(gazette::Error::Grpc(status))
            if matches!(
                status.code(),
                tonic::Code::Unauthenticated | tonic::Code::PermissionDenied,
            ) =>
        {
            tonic::Code::Unauthenticated
        }
        Some(broker) if broker.is_transient() => tonic::Code::Unavailable,
        _ => tonic::Code::Internal,
    }
}

#[cfg(test)]
mod test {
    use super::{blocks, failed};

    #[test]
    fn test_a_devices_geometry_is_checked_before_it_exists() {
        assert_eq!(blocks(1 << 30).unwrap(), 262144);

        for (device_size, expect) in [
            (0, "non-zero multiple"),
            (4097, "non-zero multiple"),
            (1 << 47, "exceeds the 2^32"),
        ] {
            let err = blocks(device_size).unwrap_err();
            assert!(format!("{err}").contains(expect), "{err}");
        }
    }

    /// A cause is classified however deeply context is stacked over it. Every
    /// failure reaches the tenure stream that way.
    #[test]
    fn test_a_failure_is_classified_by_its_cause() {
        let cases: Vec<(anyhow::Error, tonic::Code)> = vec![
            (
                blocks(0).unwrap_err().context("creating a disk"),
                tonic::Code::InvalidArgument,
            ),
            (
                anyhow::Error::new(gazette::Error::BrokerStatus(
                    proto_gazette::broker::Status::RegisterMismatch,
                ))
                .context("appending to acmeCo/disk/one"),
                tonic::Code::Aborted,
            ),
            (
                anyhow::Error::new(gazette::Error::Grpc(tonic::Status::unauthenticated(
                    "token has expired",
                )))
                .context("appending to acmeCo/disk/one"),
                tonic::Code::Unauthenticated,
            ),
            (
                anyhow::Error::new(gazette::Error::Grpc(tonic::Status::permission_denied(
                    "not authorized to append",
                ))),
                tonic::Code::Unauthenticated,
            ),
            // Whatever the broker was doing refuses a credential which runs out
            // under a live append, so it does not report this code.
            (
                anyhow::Error::new(gazette::Error::Grpc(tonic::Status::deadline_exceeded(
                    "waiting for pipeline",
                ))),
                tonic::Code::Internal,
            ),
            (
                anyhow::Error::new(gazette::Error::UnexpectedEof).context("probing"),
                tonic::Code::Unavailable,
            ),
            (
                anyhow::Error::new(crate::Failure::Ended(
                    "the tenure ended while its playback backfilled".to_string(),
                ))
                .context("promoting acmeCo/disk/one"),
                tonic::Code::Unavailable,
            ),
            (
                anyhow::Error::new(gazette::Error::BrokerStatus(
                    proto_gazette::broker::Status::JournalNotFound,
                )),
                tonic::Code::Internal,
            ),
            (
                anyhow::anyhow!("the image could not be written"),
                tonic::Code::Internal,
            ),
        ];

        for (err, expect) in cases {
            let status = failed(err);
            assert_eq!(status.code(), expect, "{status}");
        }
    }
}
