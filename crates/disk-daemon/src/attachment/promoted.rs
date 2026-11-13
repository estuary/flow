use crate::device;
use crate::journal;
use crate::proto;
use anyhow::Context;
use proto::{request, response};
use tokio::sync::mpsc;

/// A promoted attachment, whose device the client uses.
pub(super) struct Promoted {
    /// Where the attachment is in its lifecycle.
    phase: Phase,
    /// The device, once it has started.
    device: Option<device::Device>,
    /// The writer this attachment's fence created.
    writer: journal::Writer,
    /// The mount's directory, once the client has sent `Mount`.
    mount: Option<std::path::PathBuf>,
}

enum Phase {
    /// The device start's blocking task.
    Starting(tokio::task::JoinHandle<anyhow::Result<device::Started>>),
    /// Started, awaiting the client's next request.
    Serving,
    /// Mounting the filesystem, while `serve` records its writes.
    Mounting(tokio::task::JoinHandle<anyhow::Result<()>>),
    /// Syncing for a `Prepare`, while `serve` records its writes.
    Syncing(tokio::task::JoinHandle<anyhow::Result<()>>),
    /// A delta was prepared and its ACK intent sent, awaiting the client's `Acknowledge`.
    Prepared {
        /// The delta's ACK intent, which the commit appends.
        ack_intent: bytes::Bytes,
        /// The recovery floor the commit installs, if the delta named one.
        floor: Option<i64>,
    },
}

impl Promoted {
    /// Begin starting a standby's `device`, which ends its rebuild of the image
    /// `content_tx` sends it, as the attachment whose fence created `writer`.
    pub fn start(
        device: device::Standby,
        content_tx: mpsc::Sender<(i64, bytes::Bytes)>,
        writer: journal::Writer,
    ) -> Self {
        let start = tokio::task::spawn_blocking(move || device.start(content_tx));

        Self {
            phase: Phase::Starting(start),
            device: None,
            writer,
            mount: None,
        }
    }

    /// Wait for the device to start and reply `Promoted`, then serve a `Mount`,
    /// and each `Prepare` and its `Acknowledge`, until the client ends its stream.
    pub async fn serve(
        &mut self,
        owner: u32,
        mount_dir: &std::path::Path,
        requests: &mut tonic::Streaming<proto::Request>,
        responses: &mpsc::Sender<tonic::Result<proto::Response>>,
    ) -> anyhow::Result<()> {
        let Phase::Starting(start) = &mut self.phase else {
            panic!("a promoted attachment is served once");
        };
        // Awaited in place, so that if serving ends here, `stop` still finds the start
        // and tears down the device it creates.
        let started = start.await;
        self.phase = Phase::Serving;

        let device::Started {
            device,
            device_path,
            diskseq,
            blank,
            mut live_rx,
        } = started??;
        self.device = Some(device);

        () = std::os::unix::fs::chown(&device_path, Some(owner), None)
            .with_context(|| format!("giving {device_path} to uid {owner}"))?;

        () = super::send(
            responses,
            response::Response::Promoted(proto::Promoted {
                device_path: device_path.clone(),
                diskseq,
                blank,
            }),
        )
        .await?;

        loop {
            let reading = matches!(self.phase, Phase::Serving | Phase::Prepared { .. });
            let recording = !matches!(self.phase, Phase::Prepared { .. });

            tokio::select! {
                request = super::recv(requests), if reading => match request? {
                    None => return Ok(()),
                    Some(request::Request::Mount(mount)) => {
                        () = self.on_mount(mount, owner, mount_dir, &device_path)?
                    }
                    Some(request) => () = self.on_request(request, &mut live_rx, responses).await?,
                },
                chunk = live_rx.recv(), if recording => {
                    let live = chunk.context("the disk's device failed")??;
                    () = self.writer.append(live).await?;
                }
                (ended, done) = end_blocking(&mut self.phase) => {
                    () = done??;
                    match ended {
                        Phase::Mounting(_) => {
                            let dir = self.mount.as_deref().expect("a mount sets its directory");
                            let root = crate::mount::root(dir);
                            () = super::send(
                                responses,
                                response::Response::Mounted(proto::Mounted {
                                    path: root.to_string_lossy().into_owned(),
                                }),
                            )
                            .await?
                        }
                        Phase::Syncing(_) => () = self.prepare(&mut live_rx, responses).await?,
                        _ => unreachable!("only a mount or sync ends on its task"),
                    }
                }
            }
        }
    }

    fn on_mount(
        &mut self,
        proto::Mount { fstype, options }: proto::Mount,
        owner: u32,
        mount_dir: &std::path::Path,
        device_path: &str,
    ) -> anyhow::Result<()> {
        if matches!(self.phase, Phase::Prepared { .. }) {
            return Err(crate::failed_precondition(
                "a prepared delta is still awaiting its commit",
            ));
        }
        if self.mount.is_some() {
            return Err(crate::failed_precondition("the disk is already mounted"));
        }

        let name = std::path::Path::new(device_path).file_name();
        let dir = mount_dir.join(name.expect("a device has a name"));

        let task = {
            let (dir, device) = (dir.clone(), device_path.to_string());
            tokio::task::spawn_blocking(move || {
                crate::mount::mount(&dir, &device, &fstype, &options, owner)
            })
        };
        self.mount = Some(dir);
        self.phase = Phase::Mounting(task);
        Ok(())
    }

    async fn on_request(
        &mut self,
        request: request::Request,
        live_rx: &mut mpsc::Receiver<anyhow::Result<crate::LiveChunk>>,
        responses: &mpsc::Sender<tonic::Result<proto::Response>>,
    ) -> anyhow::Result<()> {
        match request {
            request::Request::Prepare(proto::Prepare {})
                if matches!(self.phase, Phase::Serving) =>
            {
                let Some(dir) = &self.mount else {
                    return self.prepare(live_rx, responses).await;
                };

                let dir = dir.clone();
                self.phase = Phase::Syncing(tokio::task::spawn_blocking(move || {
                    crate::mount::sync(&dir)
                }));

                Ok(())
            }
            request::Request::Acknowledge(proto::Acknowledge {}) => {
                let Phase::Prepared { ack_intent, floor } =
                    std::mem::replace(&mut self.phase, Phase::Serving)
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

    async fn prepare(
        &mut self,
        live_rx: &mut mpsc::Receiver<anyhow::Result<crate::LiveChunk>>,
        responses: &mpsc::Sender<tonic::Result<proto::Response>>,
    ) -> anyhow::Result<()> {
        // A write's chunks are sent before it completes, so at most one chunk
        // of the flushed writes still waits here.
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

        let prepared = proto::Prepared {
            ack_intent: ack_intent.clone(),
        };
        () = super::send(responses, response::Response::Prepared(prepared)).await?;
        self.phase = Phase::Prepared { ack_intent, floor };

        Ok(())
    }

    /// Tear down the disk: finish a device start still in flight, so the device it
    /// creates isn't left behind, then unmount its filesystem, and stop and delete
    /// the device. `serve` has dropped its receiver of live chunks, so the device
    /// applies the writes it still serves without waiting on the journal.
    pub async fn stop(self) -> anyhow::Result<()> {
        let Self {
            phase,
            mut device,
            mount,
            ..
        } = self;

        match phase {
            Phase::Starting(start) => match start
                .await
                .map_err(anyhow::Error::from)
                .and_then(|started| started)
            {
                Ok(started) => device = Some(started.device),
                Err(err) => tracing::error!(?err, "a disk's device failed to start"),
            },
            // Wait out a mount still in flight, so the unmount below removes it.
            Phase::Mounting(task) => _ = task.await,
            Phase::Serving | Phase::Syncing(_) | Phase::Prepared { .. } => (),
        }
        let Some(device) = device else {
            return Ok(());
        };

        tokio::task::spawn_blocking(move || {
            let cleared = mount.map_or(Ok(()), |dir| {
                crate::mount::unmount(&dir)
                    .inspect_err(|err| tracing::error!(?err, "failed to unmount a disk"))
            });
            device.stop().and(cleared)
        })
        .await
        .map_err(anyhow::Error::from)
        .and_then(|stopped| stopped)
    }
}

async fn end_blocking(
    phase: &mut Phase,
) -> (Phase, Result<anyhow::Result<()>, tokio::task::JoinError>) {
    let (Phase::Mounting(task) | Phase::Syncing(task)) = phase else {
        return std::future::pending().await;
    };
    let done = task.await;

    (std::mem::replace(phase, Phase::Serving), done)
}
