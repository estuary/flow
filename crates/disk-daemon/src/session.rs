//! The session service. One bidirectional stream serves exactly one disk.
//!
//! A session is a state machine over its stream. It begins with `Open`, which
//! creates the image, rebuilds it from the journal or formats it, and then creates
//! the device and the mount over it. It then serves `Prepare` and `Commit` pairs,
//! which move the disk's durable state forward atomically with the client's own
//! commit. It ends when the stream ends, for any reason at all, by unmounting and
//! destroying everything it made.
//!
//! Every error is terminal. A device or broker failure is terminal because the
//! disk's contents can no longer be trusted to reach its journal. A protocol
//! violation is terminal because the client has lost track of which delta it owes
//! a commit. What differs is the code the stream ends with. That code is the only
//! part of a failure a client can act on. See `failed`.

use crate::capture::Captured;
use crate::disk::Disk;
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
    registry: service_kit::Registry,
    /// Cancelled when the daemon drains, so that no session outlives it. Each
    /// session takes a child of this token and cancels it during teardown.
    draining: tokio_util::sync::CancellationToken,
}

impl Service {
    pub fn new(
        daemon: std::sync::Arc<crate::daemon::Config>,
        control: std::sync::Arc<Control>,
        registry: service_kit::Registry,
        draining: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self {
            daemon,
            control,
            registry,
            draining,
        }
    }

    pub fn into_tonic_service(self) -> proto_grpc::disk::disk_server::DiskServer<Self> {
        proto_grpc::disk::disk_server::DiskServer::new(self)
    }
}

#[tonic::async_trait]
impl proto_grpc::disk::disk_server::Disk for Service {
    type SessionStream = tokio_stream::wrappers::ReceiverStream<tonic::Result<proto::Response>>;

    async fn session(
        &self,
        request: tonic::Request<tonic::Streaming<proto::Request>>,
    ) -> tonic::Result<tonic::Response<Self::SessionStream>> {
        let (responses, stream) = tokio::sync::mpsc::channel(1);

        let (daemon, control) = (self.daemon.clone(), self.control.clone());
        let (registry, ended) = (self.registry.clone(), self.draining.child_token());

        // The mount this session returns belongs to the client rather than to this
        // daemon, so that a client needs no privilege of its own. A Unix socket
        // carries the peer's credential, which is the one identity a client cannot
        // claim falsely.
        let owner = request
            .extensions()
            .get::<tonic::transport::server::UdsConnectInfo>()
            .and_then(|info| info.peer_cred)
            .map(|cred| (cred.uid(), cred.gid()));

        // The session owns its disk, so it outlives this call and tears the disk
        // down as it ends. A client which drops the stream both ends `requests`
        // and closes `responses`. Either of those ends the session.
        //
        // The handler registers inside the spawn rather than outside it. A handler
        // span captures the tracing dispatcher of whichever task creates it, and a
        // spawn does not carry that dispatcher across.
        tokio::spawn(async move {
            let session = Session {
                daemon,
                control,
                owner,
                handler: registry.register("Disk.Session"),
                ended,
                state: State::Fresh,
            };
            let span = session.handler.span();

            tracing::Instrument::instrument(session.run(request.into_inner(), responses), span)
                .await
        });

        Ok(tonic::Response::new(
            tokio_stream::wrappers::ReceiverStream::new(stream),
        ))
    }
}

struct Session {
    daemon: std::sync::Arc<crate::daemon::Config>,
    control: std::sync::Arc<Control>,
    /// User and group which own the mount this session serves, taken from the
    /// credential of its stream.
    owner: Option<(u32, u32)>,
    handler: service_kit::HandlerGuard,
    /// Cancelled when this session is over. That happens when the daemon drains,
    /// or when the session's own teardown begins. Every broker call the session
    /// makes gives up on this token, because each one retries indefinitely.
    ended: tokio_util::sync::CancellationToken,
    state: State,
}

enum State {
    /// Before `Open`, which must be a session's first request.
    Fresh,
    Serving(Serving),
}

/// What one open disk consists of.
///
/// The fields are declared in the order [`teardown`] runs them, which is also
/// the order they drop in. The filesystem unmounts before the device under it
/// stops. The writer outlives both, because an unmount writes.
struct Serving {
    mount: Mount,
    disk: Disk,
    writer: Writer,
}

impl Session {
    async fn run(
        mut self,
        mut requests: tonic::Streaming<proto::Request>,
        responses: tokio::sync::mpsc::Sender<tonic::Result<proto::Response>>,
    ) {
        let outcome = self.serve(&mut requests, &responses).await;

        // The disk is torn down before the failure which ended the session is
        // reported, so that a client which sees its session end sees a disk
        // which is already gone. It is also how a draining daemon waits for its
        // sessions. This stream stays open until its disk is destroyed.
        self.handler.set_phase("closing");
        let state = std::mem::replace(&mut self.state, State::Fresh);
        () = teardown(state).await;

        match outcome {
            Ok(()) => self.handler.finish_ok(),
            Err(status) => {
                tracing::warn!(%status, "session failed");
                self.handler.finish_err(&status.to_string());
                _ = responses.send(Err(status)).await;
            }
        }
    }

    async fn serve(
        &mut self,
        requests: &mut tonic::Streaming<proto::Request>,
        responses: &tokio::sync::mpsc::Sender<tonic::Result<proto::Response>>,
    ) -> tonic::Result<()> {
        loop {
            // This does not race a request which is already in flight. The broker
            // calls within that request give up instead. They are the only waits
            // a session has which no timeout bounds.
            let request = tokio::select! {
                _ = self.ended.cancelled() => {
                    return Err(tonic::Status::unavailable("the daemon is draining"));
                }
                request = requests.message() => request?,
            };
            let Some(request) = request else {
                return Ok(()); // The client closed its half of the session.
            };

            if let Some(response) = self.request(request).await? {
                responses
                    .send(Ok(response))
                    .await
                    .map_err(|_| tonic::Status::cancelled("the client dropped its session"))?;
            }
        }
    }

    async fn request(&mut self, request: proto::Request) -> tonic::Result<Option<proto::Response>> {
        use proto::request::Request;
        use proto::response::Response;

        let request = request
            .request
            .ok_or_else(|| tonic::Status::invalid_argument("request carries no message"))?;

        let response = match request {
            Request::Open(open) => {
                if !matches!(self.state, State::Fresh) {
                    return Err(tonic::Status::failed_precondition(
                        "a session opens exactly one disk",
                    ));
                }
                self.handler.set_phase("opening");
                let (serving, floor) = self.open(open).await.map_err(failed)?;

                let opened = proto::Opened {
                    mount_path: serving.mount.path().display().to_string(),
                    floor,
                };
                self.state = State::Serving(serving);
                self.handler.set_phase("serving");

                Some(Response::Opened(opened))
            }
            Request::Prepare(proto::Prepare {}) => {
                self.handler.set_phase("preparing");
                let ack = self.serving()?.prepare().await.map_err(failed)?;
                self.handler.set_phase("serving");

                Some(Response::Prepared(proto::Prepared {
                    ack: ack.unwrap_or_default(),
                }))
            }
            Request::Commit(proto::Commit { ack }) => {
                self.handler.set_phase("committing");
                let floor = self.serving()?.writer.commit(ack).await.map_err(failed)?;
                self.handler.set_phase("serving");

                Some(Response::Committed(proto::Committed { floor }))
            }
            // A replaced broker has no reply, so a client which cannot reach
            // its brokers learns of it from its next prepare.
            Request::Broker(broker) => {
                tracing::info!(endpoint = broker.endpoint, "replacing a session's broker");
                () = self.serving()?.writer.set_broker(broker).map_err(failed)?;

                None
            }
        };

        Ok(response.map(|response| proto::Response {
            response: Some(response),
        }))
    }

    /// Create the disk of `open`, mount its filesystem, and report the recovery
    /// floor the open derived.
    ///
    /// A disk with committed state is rebuilt from its journal. The session claims
    /// that journal before it reads. A disk without committed state is formatted
    /// instead, and its journal is not claimed. The session only reads the author
    /// register which a later first append must replace.
    async fn open(&self, open: proto::Open) -> anyhow::Result<(Serving, u64)> {
        let proto::Open {
            journal,
            device_size,
            broker,
            recovered_acks,
            floor_hint,
        } = open;

        let blocks = blocks(device_size)?;
        self.handler.set_label(journal.clone());

        // This is resolved before a device exists. A journal which does not exist,
        // or which a disk could not be recovered from, is a disk which can never
        // prepare a delta.
        let mut opening = journal::Opening::new(
            &self.daemon.client,
            journal::Open {
                journal: journal.clone(),
                broker: broker.unwrap_or_default(),
            },
            self.ended.clone(),
        )
        .await?;

        let mut image = Image::create(&self.daemon.image_dir, blocks)
            .with_context(|| format!("creating an image in {:?}", self.daemon.image_dir))?;

        // A horizon the replay leaves open belongs to the image. The disk resumes
        // that horizon rather than opening a new one over whatever this session
        // finds allocated.
        let journal::Recovered { recovered, floor } = opening
            .recover(&mut image, recovered_acks, floor_hint)
            .await?;

        let control = self.control.clone();
        let horizon = self.daemon.horizon;
        let metrics = crate::metrics::Device::new(&journal, &self.daemon.footprint);

        // Creating a device is a handshake with the kernel and with the thread
        // which will own it. Neither handshake is async.
        let (disk, captured) = tokio::task::spawn_blocking(move || {
            Disk::create(&control, image, crate::ublk::QUEUE_DEPTH, horizon, metrics)
        })
        .await??;

        self.handler.set_field("dev_id", disk.dev_id());

        let compactor = Some(disk.compactor()?);
        let block_path = disk.block_path();
        let mount_path =
            self.daemon
                .mount_dir
                .join(format!("{}{}", crate::daemon::MOUNT_PREFIX, disk.dev_id()));

        // A recovered disk serves before it is mounted, because the writes its
        // mount issues belong to the next delta. A fresh disk works the other way
        // round. Its format and mount output is dropped, and the snapshot its
        // first append takes reproduces that output, so it begins serving once
        // both steps are done.
        let (mount, writer) = match recovered {
            true => {
                let writer = opening.serve(captured, None, compactor);

                let mount = Mount::new(
                    filesystem::Type::Ext4,
                    &block_path,
                    mount_path,
                    self.owner,
                    filesystem::MOUNT_TIMEOUT,
                )
                .await?;

                (mount, writer)
            }
            false => {
                let mount = draining(&captured, async {
                    () = filesystem::format(
                        filesystem::Type::Ext4,
                        &block_path,
                        self.owner,
                        filesystem::MKFS_TIMEOUT,
                    )
                    .await?;

                    Mount::new(
                        filesystem::Type::Ext4,
                        &block_path,
                        mount_path,
                        self.owner,
                        filesystem::MOUNT_TIMEOUT,
                    )
                    .await
                })
                .await?;

                (
                    mount,
                    opening.serve(captured, Some(disk.snapshotter()?), compactor),
                )
            }
        };

        tracing::info!(
            dev_id = disk.dev_id(),
            mount = ?mount.path(),
            recovered,
            floor,
            "opened a disk",
        );

        Ok((
            Serving {
                mount,
                disk,
                writer,
            },
            floor,
        ))
    }

    fn serving(&mut self) -> tonic::Result<&mut Serving> {
        match &mut self.state {
            State::Serving(serving) => Ok(serving),
            State::Fresh => Err(tonic::Status::failed_precondition(
                "the first request of a session must be Open",
            )),
        }
    }
}

/// Unmount, destroy the device, and drop the image.
async fn teardown(state: State) {
    let State::Serving(Serving {
        mut mount,
        mut disk,
        writer,
    }) = state
    else {
        return;
    };

    // This session prepares nothing more. The writer takes what the unmount
    // mutates and then discards it.
    () = writer.abandon();

    if let Err(err) = mount.unmount(filesystem::MOUNT_TIMEOUT).await {
        tracing::error!(?err, "failed to unmount a disk");
    }
    let dev_id = disk.dev_id();

    match tokio::task::spawn_blocking(move || disk.stop()).await {
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
    /// Admission resumes as soon as the acknowledgement exists. The writer then
    /// holds those mutations until it commits.
    async fn prepare(&mut self) -> anyhow::Result<Option<bytes::Bytes>> {
        let mount = self.mount.path().to_path_buf();

        tokio::task::spawn_blocking(move || filesystem::sync(&mount))
            .await?
            .context("syncing a disk's filesystem")?;

        () = self.disk.close_admission().await?;
        let prepared = self.writer.prepare().await;

        // Admission resumes even where the prepare failed. The unmount which
        // follows a failed session writes.
        if let Err(err) = self.disk.resume_admission() {
            tracing::error!(?err, "failed to resume a disk's admission");
        }
        prepared
    }
}

/// Run `work`, taking and dropping every mutation the disk makes while it runs.
///
/// A format and a mount of a fresh disk both write to it, and the capture channel
/// is bounded, so a mutation nothing takes would park the device. Nothing here
/// needs keeping. The first append snapshots exactly what these writes leave in
/// the image. A disk which is never written prepares nothing at all, because
/// formatting it again reproduces it.
async fn draining<T>(
    captured: &Captured,
    work: impl Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    futures::pin_mut!(work);

    loop {
        tokio::select! {
            // This observes completion as soon as it happens, however busy the
            // device is.
            biased;

            result = &mut work => return result,

            chunks = captured.recv() => {
                if chunks.is_none() {
                    anyhow::bail!("the device stopped serving before it was mounted");
                }
            }
        }
    }
}

/// Block count of a device. `device_size` is the one durable geometry a session
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

/// gRPC code of a failure which ends a session.
///
/// A client cannot act on a message, so the code is the contract:
///
/// - `INVALID_ARGUMENT` is what the session asked for. A retry cannot succeed.
/// - `ABORTED` is a lost fence. Another session owns this disk, and this one must
///   not take it back.
/// - `UNAUTHENTICATED` is a credential the broker refused. A client should
///   refresh it and open again.
/// - `UNAVAILABLE` is a broker this daemon could not reach. Another host may
///   reach it.
/// - `INTERNAL` is everything else, which is the daemon or its host failing.
///
/// `Session::request` reports a session's own state as `FAILED_PRECONDITION`, and
/// that never reaches here. The crate README says what a client should do with
/// each code.
fn failed(err: anyhow::Error) -> tonic::Status {
    let code = if err.chain().any(|cause| cause.is::<crate::Invalid>()) {
        tonic::Code::InvalidArgument
    } else if err.chain().any(|cause| cause.is::<crate::OutOfOrder>()) {
        tonic::Code::FailedPrecondition
    } else {
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
    };

    tonic::Status::new(code, format!("{err:#}"))
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
    /// failure reaches the session stream that way.
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
