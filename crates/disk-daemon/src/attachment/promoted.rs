use crate::device;
use crate::filesystem;
use crate::journal;
use crate::proto;
use anyhow::Context;
use proto::{request, response};
use tokio::sync::mpsc;

/// A promoted attachment, whose device serves the mount.
pub(super) struct Promoted {
    /// Where the attachment is in its lifecycle.
    phase: Phase,
    /// The device, once it has started.
    device: Option<device::Device>,
    /// The disk's filesystem, once it's mounted.
    mount: Option<filesystem::Mount>,
    /// The writer this attachment's fence created.
    writer: journal::Writer,
}

enum Phase {
    /// The device is starting.
    Starting {
        /// The device start's blocking task.
        start: tokio::task::JoinHandle<anyhow::Result<device::Started>>,
        /// Parent directory of per-device mounts.
        mount_dir: std::path::PathBuf,
        /// Client's user and group, which own the mount.
        peer: (u32, u32),
        /// Promotion re-acknowledged a recovered ACK intent.
        recovered: bool,
    },
    /// Mounting a disk, formatting it first if needed.
    Mounting(futures::future::BoxFuture<'static, anyhow::Result<filesystem::Mount>>),
    /// Syncing the filesystem prior to a commit.
    Syncing {
        /// The sync's blocking task.
        sync: futures::future::BoxFuture<'static, anyhow::Result<()>>,
        /// It commits the attachment's own setup, rather than a client's `Prepare`.
        setup: bool,
    },
    /// Mounted, with nothing in flight, awaiting the client's next `Prepare`.
    Serving,
    /// A delta was prepared and its ACK intent sent, awaiting the client's `Acknowledge`.
    Prepared {
        /// The delta's ACK intent, which the commit appends.
        ack_intent: bytes::Bytes,
        /// The recovery floor the commit installs, if the delta named one.
        floor: Option<i64>,
    },
    /// The prior step is done, and nothing is in flight.
    Done,
}

/// The result of the step a `Phase` was running.
enum Finished {
    Mounting(anyhow::Result<filesystem::Mount>),
    Syncing {
        synced: anyhow::Result<()>,
        setup: bool,
    },
}

impl Phase {
    /// Wait for the mount or sync in flight, or forever if neither is.
    async fn finished(&mut self) -> Finished {
        let finished = match self {
            Phase::Mounting(mounting) => Finished::Mounting(mounting.await),
            Phase::Syncing { sync, setup } => Finished::Syncing {
                synced: sync.await,
                setup: *setup,
            },
            _ => return std::future::pending().await,
        };
        *self = Phase::Done;
        finished
    }
}

impl Promoted {
    /// Begin starting a standby's `device`, which ends its rebuild of the image
    /// `content_tx` sends it, as the attachment whose fence created `writer`.
    pub fn start(
        device: device::Standby,
        content_tx: mpsc::Sender<(i64, bytes::Bytes)>,
        writer: journal::Writer,
        mount_dir: std::path::PathBuf,
        peer: (u32, u32),
        recovered: bool,
    ) -> Self {
        let start = tokio::task::spawn_blocking(move || device.start(content_tx));

        Self {
            phase: Phase::Starting {
                start,
                mount_dir,
                peer,
                recovered,
            },
            device: None,
            mount: None,
            writer,
        }
    }

    /// Wait for the device to finish mounting, then serve each `Prepare` and
    /// its `Acknowledge`, until the client ends its stream.
    pub async fn serve(
        &mut self,
        requests: &mut tonic::Streaming<proto::Request>,
        responses: &mpsc::Sender<tonic::Result<proto::Response>>,
    ) -> anyhow::Result<()> {
        let mut live_rx = self.await_device().await?;

        loop {
            let awaiting_request = matches!(self.phase, Phase::Serving | Phase::Prepared { .. });
            let recording = !matches!(self.phase, Phase::Prepared { .. });

            tokio::select! {
                finished = self.phase.finished() => match finished {
                    Finished::Mounting(mounted) => () = self.on_mounted(mounted)?,
                    Finished::Syncing { synced, setup } => {
                        () = self.on_synced(synced, setup, &mut live_rx, responses).await?
                    }
                },
                request = super::recv(requests), if awaiting_request => match request? {
                    None => return Ok(()),
                    Some(request) => () = self.on_request(request, responses).await?,
                },
                chunk = live_rx.recv(), if recording => {
                    let live = chunk.context("the disk's device failed")??;
                    () = self.writer.append(live).await?;
                }
            }
        }
    }

    async fn await_device(
        &mut self,
    ) -> anyhow::Result<mpsc::Receiver<anyhow::Result<crate::LiveChunk>>> {
        let Phase::Starting {
            start,
            mount_dir,
            peer,
            recovered,
        } = &mut self.phase
        else {
            panic!("a promoted attachment is served once");
        };
        let started = start.await;
        let (mount_dir, peer, recovered) = (std::mem::take(mount_dir), *peer, *recovered);
        self.phase = Phase::Done;

        let device::Started {
            device,
            block_path,
            live_rx,
            fresh,
        } = started??;
        self.device = Some(device);

        // A fresh disk would be formatted, over the state a recovered ACK intent shows.
        if recovered && fresh {
            return Err(crate::invalid_argument(format!(
                "replay of journal {} found no committed data with a recovered ACK intent",
                self.writer.name,
            )));
        }

        // A mount point names the device it presents.
        let name = block_path.file_name().expect("a block device has a name");
        let mount_path = mount_dir.join(name);

        self.phase = Phase::Mounting(Box::pin(filesystem::Mount::new(
            block_path, mount_path, peer, fresh,
        )));
        Ok(live_rx)
    }

    /// The disk is formatted and mounted. Sync it: the mount's own writes reach
    /// the device only with a sync.
    fn on_mounted(&mut self, mounted: anyhow::Result<filesystem::Mount>) -> anyhow::Result<()> {
        let mount = mounted?;

        self.phase = Phase::Syncing {
            sync: Box::pin(mount.sync()),
            setup: true,
        };
        self.mount = Some(mount);
        Ok(())
    }

    /// The filesystem is synced. Take every live chunk the sync waited on into
    /// the delta, and prepare it.
    async fn on_synced(
        &mut self,
        synced: anyhow::Result<()>,
        setup: bool,
        live_rx: &mut mpsc::Receiver<anyhow::Result<crate::LiveChunk>>,
        responses: &mpsc::Sender<tonic::Result<proto::Response>>,
    ) -> anyhow::Result<()> {
        () = synced.context("syncing a disk's filesystem")?;

        loop {
            match live_rx.try_recv() {
                Ok(chunk) => () = self.writer.append(chunk?).await?,
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    anyhow::bail!("the disk's device failed")
                }
            }
        }

        let (ack_intent, floor) = self.writer.prepare().await?;

        if setup {
            () = self.writer.acknowledge(ack_intent, floor).await?;
            let mount = self.mount.as_ref().expect("a synced disk is mounted");
            let mount_path = mount.path().display().to_string();

            () = super::send(
                responses,
                response::Response::Promoted(proto::Promoted { mount_path }),
            )
            .await?;
            self.phase = Phase::Serving;
        } else {
            let prepared = proto::Prepared {
                ack_intent: ack_intent.clone(),
            };
            () = super::send(responses, response::Response::Prepared(prepared)).await?;
            self.phase = Phase::Prepared { ack_intent, floor };
        }

        Ok(())
    }

    /// Serve the client's `request`.
    async fn on_request(
        &mut self,
        request: request::Request,
        responses: &mpsc::Sender<tonic::Result<proto::Response>>,
    ) -> anyhow::Result<()> {
        match request {
            request::Request::Prepare(proto::Prepare {})
                if matches!(self.phase, Phase::Serving) =>
            {
                let mount = self.mount.as_ref().expect("a serving disk is mounted");
                self.phase = Phase::Syncing {
                    sync: Box::pin(mount.sync()),
                    setup: false,
                };
                Ok(())
            }
            request::Request::Acknowledge(proto::Acknowledge {}) => {
                let Phase::Prepared { ack_intent, floor } =
                    std::mem::replace(&mut self.phase, Phase::Done)
                else {
                    return Err(crate::failed_precondition(
                        "no prepared delta awaits acknowledgement",
                    ));
                };
                () = self.writer.acknowledge(ack_intent, floor).await?;
                () = super::send(
                    responses,
                    response::Response::Acknowledged(proto::Acknowledged {}),
                )
                .await?;
                self.phase = Phase::Serving;
                Ok(())
            }
            request::Request::Prepare(_) => Err(crate::failed_precondition(
                "a prepared delta is still awaiting its commit",
            )),
            _ => Err(crate::failed_precondition(
                "a promoted attachment serves only Prepare and Acknowledge",
            )),
        }
    }

    /// Tear down the disk: finish a device start or mount still in flight, so nothing
    /// it creates is left behind, then unmount, then stop and delete the device. The
    /// unmount writes to the device, so it comes first, and since `serve` has dropped
    /// its receiver of live chunks, those writes don't wait on the journal. A sync in
    /// flight isn't waited for. A failed unmount or device stop is returned, after
    /// the rest of teardown, and other failures are only logged.
    pub async fn stop(self) -> anyhow::Result<()> {
        let Self {
            phase,
            mut device,
            mut mount,
            ..
        } = self;

        match phase {
            Phase::Starting { start, .. } => match start
                .await
                .map_err(anyhow::Error::from)
                .and_then(|started| started)
            {
                Ok(started) => device = Some(started.device),
                Err(err) => tracing::error!(?err, "a disk's device failed to start"),
            },
            Phase::Mounting(mounting) => match mounting.await {
                Ok(mounted) => mount = Some(mounted),
                Err(err) => tracing::error!(?err, "a disk failed to mount"),
            },
            Phase::Syncing { .. } | Phase::Serving | Phase::Prepared { .. } | Phase::Done => (),
        }
        let Some(device) = device else {
            return Ok(());
        };

        // Each step is logged, so a teardown which stalls shows the step it stalled in.
        let mount_path = mount.as_ref().map(|mount| mount.path().to_path_buf());
        tracing::info!(mount = ?mount_path, "tearing down a disk");

        let unmounted = match mount {
            Some(mount) => mount
                .unmount()
                .await
                .inspect(|()| tracing::info!(mount = ?mount_path, "unmounted a disk")),
            None => Ok(()),
        };
        let stopped = tokio::task::spawn_blocking(move || device.stop())
            .await
            .map_err(anyhow::Error::from)
            .and_then(|stopped| stopped)
            .inspect(|()| tracing::info!(mount = ?mount_path, "closed a disk"));

        () = unmounted?;
        stopped
    }
}
