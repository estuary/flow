//! The session service: one bidirectional stream serves exactly one disk.
//!
//! A session is a state machine over its stream. It begins with `Open`, which
//! creates the image, rebuilds it from the journal or formats it, and then
//! creates the device and the mount over it; it then serves `Publish` and
//! `Commit` pairs, which move the disk's durable state forward atomically with
//! the client's own commit; and it ends when the stream ends, for any reason at
//! all, by unmounting and destroying everything it made.
//!
//! Every error is terminal. A device or broker failure is terminal because the
//! disk's contents can no longer be trusted to reach its journal, and a
//! protocol violation is terminal because the client has lost track of which
//! delta it owes a commit.

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
    /// Ends every session, so that a draining daemon leaves no device behind.
    shutdown: tokio::sync::broadcast::Sender<()>,
}

impl Service {
    pub fn new(
        daemon: std::sync::Arc<crate::daemon::Config>,
        control: std::sync::Arc<Control>,
        registry: service_kit::Registry,
        shutdown: tokio::sync::broadcast::Sender<()>,
    ) -> Self {
        Self {
            daemon,
            control,
            registry,
            shutdown,
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

        let session = Session {
            daemon: self.daemon.clone(),
            control: self.control.clone(),
            handler: self.registry.register("Disk.Session"),
            shutdown: self.shutdown.subscribe(),
            state: State::Fresh,
        };
        // The session owns its disk, so it outlives this call and tears the
        // disk down as it ends. A client which drops the stream both ends
        // `requests` and closes `responses`, either of which ends the session.
        tokio::spawn(session.run(request.into_inner(), responses));

        Ok(tonic::Response::new(
            tokio_stream::wrappers::ReceiverStream::new(stream),
        ))
    }
}

struct Session {
    daemon: std::sync::Arc<crate::daemon::Config>,
    control: std::sync::Arc<Control>,
    handler: service_kit::HandlerGuard,
    shutdown: tokio::sync::broadcast::Receiver<()>,
    state: State,
}

enum State {
    /// Before `Open`, which must be a session's first request.
    Fresh,
    Serving(Serving),
}

/// What one open disk consists of.
///
/// The fields are declared in the order [`teardown`] runs, which is also the
/// order they drop in: the filesystem is unmounted before the device under it
/// stops, and the writer outlives both, because unmounting writes.
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
        // which is already gone. It is also what lets a draining daemon wait
        // for its sessions: this stream is open until its disk is destroyed.
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
            let request = tokio::select! {
                _ = self.shutdown.recv() => {
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
                let serving = self.open(open).await.map_err(failed)?;

                let opened = proto::Opened {
                    mount_path: serving.mount.path().display().to_string(),
                };
                self.state = State::Serving(serving);

                Some(Response::Opened(opened))
            }
            Request::Publish(proto::Publish {}) => {
                let ack = self.serving()?.publish().await.map_err(failed)?;

                Some(Response::Published(proto::Published {
                    ack: ack.unwrap_or_default(),
                }))
            }
            Request::Commit(proto::Commit { ack }) => {
                () = self.serving()?.writer.commit(ack).await.map_err(failed)?;

                Some(Response::Committed(proto::Committed {}))
            }
            // A replaced broker has no reply, so a client which cannot reach
            // its brokers learns of it from its next publication.
            Request::Broker(broker) => {
                () = self
                    .serving()?
                    .writer
                    .set_broker(broker)
                    .await
                    .map_err(failed)?;

                None
            }
        };

        Ok(response.map(|response| proto::Response {
            response: Some(response),
        }))
    }

    /// Create the disk of `open` and mount its filesystem.
    ///
    /// A disk with committed state is rebuilt from its journal, which the
    /// session claims before it reads. One without is formatted, and its
    /// journal is neither created nor claimed: only the author register that a
    /// later first append must replace is read.
    async fn open(&self, open: proto::Open) -> anyhow::Result<Serving> {
        let proto::Open {
            journal_config,
            device_size,
            block_size,
            broker,
            recovered_acks,
        } = open;

        let journal_config = journal_config.unwrap_or_default();
        let blocks = blocks(device_size, block_size)?;
        self.handler.set_label(journal_config.journal.clone());

        // Built before a device exists, because a journal which cannot be
        // created is a disk which can never publish.
        let mut opening = journal::Opening::new(
            &self.daemon.client,
            journal::Open {
                journal: journal_config,
                broker: broker.unwrap_or_default(),
                floor_label: self.daemon.floor_label.clone(),
            },
        )
        .await?;

        let mut image = Image::create(&self.daemon.image_dir, blocks, block_size)
            .with_context(|| format!("creating an image in {:?}", self.daemon.image_dir))?;

        // A horizon the replay leaves open is the image's, so the disk resumes
        // it rather than beginning again from what this session happens to find
        // allocated.
        let recovered = opening.recover(&mut image, recovered_acks).await?;

        let control = self.control.clone();
        let horizon = self.daemon.horizon;

        // Creating a device is a handshake with the kernel and with the thread
        // which will own it, neither of which is async.
        let (disk, captured) = tokio::task::spawn_blocking(move || {
            Disk::create(&control, image, crate::ublk::QUEUE_DEPTH, horizon)
        })
        .await??;

        self.handler.set_field("dev_id", disk.dev_id());

        let compactor = Some(disk.compactor()?);
        let block_path = disk.block_path();
        let mount_path =
            self.daemon
                .mount_dir
                .join(format!("{}{}", crate::daemon::MOUNT_PREFIX, disk.dev_id()));

        // A recovered disk is serving before it is mounted, because the writes
        // its mount issues belong to the next delta. A fresh disk's format and
        // mount output is instead dropped and reproduced by the snapshot its
        // first append takes, so it begins serving once both are done.
        let (mount, writer) = match recovered {
            true => {
                let writer = opening.serve(captured, None, compactor);

                let mount = Mount::new(
                    filesystem::Type::Ext4,
                    &block_path,
                    mount_path,
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
                        block_size,
                        filesystem::MKFS_TIMEOUT,
                    )
                    .await?;

                    Mount::new(
                        filesystem::Type::Ext4,
                        &block_path,
                        mount_path,
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
            "opened a disk",
        );

        Ok(Serving {
            mount,
            disk,
            writer,
        })
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

    // This session publishes nothing more, so the writer takes what unmounting
    // mutates and discards it.
    if let Err(err) = writer.abandon().await {
        tracing::error!(?err, "failed to abandon a journal writer");
    }
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
    /// Cut a point-in-time boundary of the disk and finish publishing the delta
    /// before it.
    ///
    /// The cut is this order: the mount is flushed, admission closes, and every
    /// mutation which was admitted lands. Because a mutation is captured before
    /// it is applied, each is then entirely before or after the boundary, and
    /// the writer can finish exactly the delta which precedes it.
    ///
    /// Admission resumes as soon as the acknowledgement exists. The writer is
    /// what then holds those mutations, until the acknowledgement commits.
    async fn publish(&mut self) -> anyhow::Result<Option<bytes::Bytes>> {
        let mount = self.mount.path().to_path_buf();

        tokio::task::spawn_blocking(move || filesystem::sync(&mount))
            .await?
            .context("syncing a disk's filesystem")?;

        () = self.disk.close_admission().await?;
        let published = self.writer.publish().await;

        // Admission resumes even where the publication failed, because the
        // unmount which follows a failed session writes.
        if let Err(err) = self.disk.resume_admission() {
            tracing::error!(?err, "failed to resume a disk's admission");
        }
        published
    }
}

/// Run `work`, taking and dropping every mutation the disk makes while it does.
///
/// Formatting and mounting a fresh disk writes to it, and the capture channel
/// is bounded, so a mutation nothing takes would park the device. Nothing here
/// needs keeping: what these writes leave in the image is exactly what the
/// first append snapshots, and a disk which is never written publishes nothing
/// at all, because formatting it again reproduces it.
async fn draining<T>(
    captured: &Captured,
    work: impl Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    futures::pin_mut!(work);

    loop {
        tokio::select! {
            // Completion is observed as soon as it happens, however busy the
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

/// Block count of a device.
///
/// Device and block size are durable per-disk facts which a session must
/// supply, because a configured default would reinterpret every disk on the
/// host at once: silently for a grown device, and catastrophically for a
/// changed block size, which misplaces every chunk a replay applies.
fn blocks(device_size: u64, block_size: u32) -> anyhow::Result<u32> {
    anyhow::ensure!(
        block_size != 0 && block_size.is_power_of_two(),
        "block size {block_size} must be a power of two",
    );
    anyhow::ensure!(
        block_size as u64 >= crate::ublk::sys::SECTOR_SIZE,
        "block size {block_size} is smaller than the {} bytes a device addresses",
        crate::ublk::sys::SECTOR_SIZE,
    );
    anyhow::ensure!(
        device_size != 0 && device_size.is_multiple_of(block_size as u64),
        "device size {device_size} must be a non-zero multiple of the block size {block_size}",
    );
    let blocks = device_size / block_size as u64;

    u32::try_from(blocks).map_err(|_| {
        anyhow::anyhow!("a device of {blocks} blocks exceeds the 2^32 which a chunk indexes")
    })
}

fn failed(err: anyhow::Error) -> tonic::Status {
    tonic::Status::internal(format!("{err:#}"))
}

#[cfg(test)]
mod test {
    use super::blocks;

    #[test]
    fn test_a_devices_geometry_is_checked_before_it_exists() {
        assert_eq!(blocks(1 << 30, 4096).unwrap(), 262144);

        for (device_size, block_size, expect) in [
            (1 << 30, 0, "power of two"),
            (1 << 30, 3000, "power of two"),
            (1 << 30, 256, "smaller than"),
            (0, 4096, "non-zero multiple"),
            (4097, 4096, "non-zero multiple"),
            (1 << 44, 512, "exceeds the 2^32"),
        ] {
            let err = blocks(device_size, block_size).unwrap_err();
            assert!(format!("{err}").contains(expect), "{err}");
        }
    }
}
