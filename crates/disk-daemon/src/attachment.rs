use crate::device;
use crate::filesystem;
use crate::image;
use crate::journal;
use crate::proto;
use crate::ublk;
use anyhow::Context;

/// Run one attachment over its `Attach` stream, until the stream ends and its disk
/// is torn down.
///
/// However `serve` ends, whether it returns, fails, or panics, the disk is torn
/// down here before the stream closes. Teardown is async, so a `Drop` can't do it.
pub(crate) async fn run(
    config: std::sync::Arc<crate::daemon::Config>,
    control: std::sync::Arc<ublk::Control>,
    peer: Option<(u32, u32)>,
    ended: tokio_util::sync::CancellationToken,
    mut requests: tonic::Streaming<proto::Request>,
    responses: tokio::sync::mpsc::Sender<tonic::Result<proto::Response>>,
) {
    let mut attachment = Attachment {
        config,
        control,
        peer,
        ended,
        disk: None,
    };

    // A client which drops its stream ends the attachment, even mid-request: its
    // broker calls give up, and its abandoned writer frees a format or mount which
    // stalled behind a broker it cannot reach.
    let ended = attachment.ended.clone();
    let outcome = tokio::select! {
        outcome = proto_grpc::catch_panic(attachment.serve(&mut requests, &responses)) => outcome,
        () = async {
            responses.closed().await;
            ended.cancel();
            std::future::pending().await
        } => unreachable!("dropping the stream ends `serve`"),
    };

    // Keep the stream open through teardown, so clients and daemon drain
    // observe completion only after cleanup.
    if let Some(disk) = attachment.disk.take() {
        () = disk.teardown().await;
    }

    if let Err(status) = outcome {
        tracing::warn!(%status, "attachment failed");
        _ = responses.send(Err(status)).await;
    }
}

/// What one attachment holds while it serves its stream.
struct Attachment {
    /// Daemon configuration.
    config: std::sync::Arc<crate::daemon::Config>,
    /// Kernel device control handle.
    control: std::sync::Arc<ublk::Control>,
    /// Client's user and group, which own the mount.
    peer: Option<(u32, u32)>,
    /// Cancelled when the attachment ends, giving up its broker calls.
    ended: tokio_util::sync::CancellationToken,
    /// The disk once it's promoted, which `run` tears down.
    disk: Option<MountedDisk>,
}

impl Attachment {
    /// Serve requests in protocol order: `Open`, then `Promote`, then any number of
    /// `Prepare` and `Acknowledge`. Returns when the client closes its half of the
    /// stream.
    async fn serve(
        &mut self,
        requests: &mut tonic::Streaming<proto::Request>,
        responses: &tokio::sync::mpsc::Sender<tonic::Result<proto::Response>>,
    ) -> anyhow::Result<()> {
        let open = match next(requests, &self.ended).await? {
            None => return Ok(()),
            Some(proto::request::Request::Open(open)) => open,
            Some(_) => {
                return Err(crate::failed_precondition(
                    "the first request of an attachment must be Open",
                ));
            }
        };
        let (journal, blocks) = check_open(open)?;

        // Replay to the head `Open` observed before reading another request, so
        // `Opened` precedes any `Promote`.
        let mut standby = self.open(journal, blocks).await?;
        () = send(
            responses,
            proto::response::Response::Opened(proto::Opened {}),
        )
        .await?;

        // Stand by, following the journal, until `Promote`.
        let ack = tokio::select! {
            request = next(requests, &self.ended) => match request? {
                None => return Ok(()),
                Some(proto::request::Request::Promote(proto::Promote { ack })) => ack,
                Some(_) => {
                    return Err(crate::failed_precondition(
                        "a standby attachment serves only Promote",
                    ));
                }
            },
            failure = standby.follow() => return Err(failure),
        };

        // Fence the previous writer, finish replay at the fenced head, and repair
        // any ack the client recovered.
        let recovered_ack = (!ack.is_empty()).then_some(ack);
        let (promoted, recovered) = standby.promote(recovered_ack).await?;

        // Held by the attachment, so that `run` tears it down however `serve` ends.
        let disk = self.disk.insert(
            MountedDisk::open(&self.config, &self.control, self.peer, promoted, recovered).await?,
        );

        let mount_path = disk.mount.path().display().to_string();
        () = send(
            responses,
            proto::response::Response::Promoted(proto::Promoted { mount_path }),
        )
        .await?;

        loop {
            let response = match next(requests, &self.ended).await? {
                None => return Ok(()),
                Some(proto::request::Request::Prepare(proto::Prepare {})) => {
                    proto::response::Response::Prepared(proto::Prepared {
                        ack: disk.prepare().await?.unwrap_or_default(),
                    })
                }
                Some(proto::request::Request::Acknowledge(proto::Acknowledge { ack })) => {
                    () = disk.writer.acknowledge(ack).await?;
                    proto::response::Response::Acknowledged(proto::Acknowledged {})
                }
                Some(_) => {
                    return Err(crate::failed_precondition(
                        "a promoted attachment serves only Prepare and Acknowledge",
                    ));
                }
            };
            () = send(responses, response).await?;
        }
    }

    /// Replay `journal` into a new image of `blocks` through the head it resolves,
    /// and keep following it, without fencing or mounting anything.
    async fn open(&self, journal: String, blocks: u32) -> anyhow::Result<journal::Standby> {
        let image = image::Image::create(&self.config.image_dir, blocks)
            .with_context(|| format!("creating an image in {:?}", self.config.image_dir))?;

        let uncommitted =
            journal::UncommittedDelta::create(&self.config.image_dir).with_context(|| {
                format!(
                    "creating an uncommitted delta in {:?}",
                    self.config.image_dir
                )
            })?;

        journal::Standby::new(
            &self.config.brokers,
            journal,
            self.ended.clone(),
            image,
            uncommitted,
        )
        .await
    }
}

/// Check an `Open` before anything is created for it, returning its journal and the
/// device's block count. `device_size` is the one durable geometry an attachment
/// supplies, because the block size is [`crate::BLOCK_SIZE`] for every disk.
fn check_open(open: proto::Open) -> anyhow::Result<(String, u32)> {
    let proto::Open {
        journal,
        device_size,
    } = open;

    if journal.is_empty() {
        return Err(crate::invalid_argument(
            "the attachment named no journal".to_string(),
        ));
    }
    if device_size == 0 || !device_size.is_multiple_of(crate::BLOCK_SIZE as u64) {
        return Err(crate::invalid_argument(format!(
            "device size {device_size} must be a non-zero multiple of the {} byte block size",
            crate::BLOCK_SIZE,
        )));
    }
    let blocks = device_size / crate::BLOCK_SIZE as u64;

    if blocks > u32::MAX as u64 {
        return Err(crate::invalid_argument(format!(
            "a device of {blocks} blocks exceeds the 2^32 which a chunk indexes",
        )));
    }
    Ok((journal, blocks as u32))
}

async fn next(
    requests: &mut tonic::Streaming<proto::Request>,
    ended: &tokio_util::sync::CancellationToken,
) -> anyhow::Result<Option<proto::request::Request>> {
    // Requests first, so that a client which dropped its stream reports that,
    // rather than the cancellation it caused.
    let message = tokio::select! {
        biased;

        message = requests.message() => message.map_err(proto_grpc::status_to_anyhow)?,
        _ = ended.cancelled() => {
            return Err(proto_grpc::status_to_anyhow(tonic::Status::unavailable(
                "the daemon is draining",
            )));
        }
    };
    let Some(proto::Request { request }) = message else {
        return Ok(None);
    };
    request
        .map(Some)
        .ok_or_else(|| crate::invalid_argument("request carries no message".to_string()))
}

async fn send(
    responses: &tokio::sync::mpsc::Sender<tonic::Result<proto::Response>>,
    response: proto::response::Response,
) -> anyhow::Result<()> {
    responses
        .send(Ok(proto::Response {
            response: Some(response),
        }))
        .await
        .map_err(|_| {
            proto_grpc::status_to_anyhow(tonic::Status::cancelled(
                "the client dropped its attachment",
            ))
        })
}

/// A promoted disk: its device, the filesystem mounted on it, and the writer
/// recording it to the journal.
struct MountedDisk {
    mount: filesystem::Mount,
    device: device::Device,
    writer: journal::Writer,
}

impl MountedDisk {
    /// Serve a recovered image as a mounted filesystem.
    async fn open(
        config: &crate::daemon::Config,
        control: &std::sync::Arc<crate::ublk::Control>,
        peer: Option<(u32, u32)>,
        promoted: journal::Promoted,
        recovered: journal::Recovered,
    ) -> anyhow::Result<Self> {
        let journal::Recovered {
            image,
            fresh,
            horizon,
        } = recovered;

        let control = control.clone();
        let policy = config.horizon;

        // Creating a device blocks on handshakes with the kernel and with the thread
        // which will own it, so it runs on a blocking thread rather than this
        // runtime thread.
        let (device, recorded) = tokio::task::spawn_blocking(move || {
            device::Device::create(&control, image, crate::ublk::QUEUE_DEPTH, horizon, policy)
        })
        .await??;

        let block_path = crate::ublk::block_path(device.dev_id());
        // A mount point names the device it presents.
        let mount_path = config.mount_dir.join(format!("disk-{}", device.dev_id()));

        // Start draining before `mkfs` or mount writes can fill the bounded
        // recording channel and stall the device.
        let writer = promoted.serve(recorded);

        // Failures must use ordered teardown: a plain drop could block this
        // runtime thread stopping a device whose writer is retrying broker I/O.
        let mounted = async {
            if fresh {
                () = filesystem::format(&block_path, peer).await?;
            }
            filesystem::Mount::new(&block_path, mount_path, peer).await
        }
        .await;

        let mount = match mounted {
            Ok(mount) => mount,
            Err(err) => {
                () = take_apart(None, device, writer).await;
                return Err(err);
            }
        };
        let mut disk = Self {
            mount,
            device,
            writer,
        };

        // Commit setup writes before returning the mount to the client.
        let committed = async {
            if let Some(ack) = disk.prepare().await? {
                () = disk.writer.acknowledge(ack).await?;
            }
            anyhow::Ok(())
        }
        .await;

        if let Err(err) = committed {
            () = disk.teardown().await;
            return Err(err);
        }

        tracing::info!(
            dev_id = disk.device.dev_id(),
            mount = ?disk.mount.path(),
            fresh,
            "opened a disk",
        );

        Ok(disk)
    }

    async fn prepare(&mut self) -> anyhow::Result<Option<bytes::Bytes>> {
        let mount = self.mount.path().to_path_buf();

        tokio::task::spawn_blocking(move || filesystem::sync(&mount))
            .await?
            .context("syncing a disk's filesystem")?;

        let discharged = self.device.close_admission().await?;
        let prepared = self.writer.prepare(discharged).await;

        // Admission resumes even where the prepare failed. The unmount which
        // follows a failed attachment writes, under a range which opens no horizon.
        let range = prepared.as_ref().map_or(0, |(_ack, range)| *range);
        let resumed = self.device.resume_admission(range);

        let (ack, _range) = prepared?;
        () = resumed.context("resuming a disk's admission")?;
        Ok(ack)
    }

    async fn teardown(self) {
        let Self {
            mount,
            device,
            writer,
        } = self;

        take_apart(Some(mount), device, writer).await
    }
}

/// Take a disk apart in the one order which cannot deadlock. `mount` is `None` for
/// a disk which failed before it was mounted.
async fn take_apart(
    mount: Option<filesystem::Mount>,
    mut device: device::Device,
    writer: journal::Writer,
) {
    () = writer.abandon();

    let dev_id = device.dev_id();

    if let Some(mut mount) = mount
        && let Err(err) = mount.unmount().await
    {
        tracing::error!(?err, dev_id, "failed to unmount a disk");
    }

    match tokio::task::spawn_blocking(move || device.stop()).await {
        Ok(Ok(())) => (),
        Ok(Err(err)) => tracing::error!(?err, dev_id, "failed to stop a device"),
        Err(panic) => tracing::error!(?panic, dev_id, "panicked stopping a device"),
    }
    drop(writer);

    tracing::info!(dev_id, "closed a disk");
}

#[cfg(test)]
mod test {
    use super::check_open;
    use crate::proto;

    #[test]
    fn test_an_open_is_checked_before_anything_exists() {
        let mut out = Vec::new();

        for (journal, device_size) in [
            ("acmeCo/disk", 1 << 30),
            ("", 1 << 30),
            ("acmeCo/disk", 0),
            ("acmeCo/disk", 4097),
            ("acmeCo/disk", 1 << 47),
        ] {
            let open = proto::Open {
                journal: journal.to_string(),
                device_size,
            };
            let outcome = match check_open(open) {
                Ok((journal, blocks)) => format!("{journal} of {blocks} blocks"),
                Err(err) => format!("{err:#}"),
            };
            let journal = format!("{journal:?}");
            out.push(format!("{journal:<14}{device_size:<16}{outcome}"));
        }
        insta::assert_snapshot!(out.join("\n"), @"
        \"acmeCo/disk\" 1073741824      acmeCo/disk of 262144 blocks
        \"\"            1073741824      the attachment named no journal (InvalidArgument)
        \"acmeCo/disk\" 0               device size 0 must be a non-zero multiple of the 4096 byte block size (InvalidArgument)
        \"acmeCo/disk\" 4097            device size 4097 must be a non-zero multiple of the 4096 byte block size (InvalidArgument)
        \"acmeCo/disk\" 140737488355328 a device of 34359738368 blocks exceeds the 2^32 which a chunk indexes (InvalidArgument)
        ");
    }
}
