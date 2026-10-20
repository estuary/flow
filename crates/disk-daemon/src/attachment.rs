use crate::device;
use crate::filesystem;
use crate::journal;
use crate::proto;
use anyhow::Context;

/// Run one attachment over its `Attach` stream, until `serve` returns and its disk
/// is torn down.
///
/// However `serve` ends, whether it returns, fails, or panics, the disk is torn
/// down here before the stream closes. Teardown is async, so a `Drop` can't do it.
pub async fn run(
    config: std::sync::Arc<crate::daemon::Config>,
    peer: (u32, u32),
    cancel: tokio_util::sync::CancellationToken,
    mut requests: tonic::Streaming<proto::Request>,
    responses: tokio::sync::mpsc::Sender<tonic::Result<proto::Response>>,
) {
    let mut attachment = Attachment {
        config,
        peer,
        cancel,
        device_standby: None,
        device: None,
        writer: None,
        mount: None,
    };

    // A client which drops its stream cancels the attachment.
    let cancel = attachment.cancel.clone();
    let outcome = tokio::select! {
        outcome = proto_grpc::catch_panic(attachment.serve(&mut requests, &responses)) => outcome,
        () = async {
            responses.closed().await;
            cancel.cancel();
            std::future::pending().await
        } => unreachable!("a cancelled attachment returns from `serve`"),
    };

    // A writer which failed cancelled the attachment, and its failure says why.
    let failure = attachment
        .writer
        .as_ref()
        .and_then(journal::Writer::failure);
    let outcome = match (outcome, failure) {
        (Err(_), Some(failure)) => Err(failure),
        (outcome, _) => outcome,
    };

    // Keep the stream open through teardown, so clients and daemon drain
    // observe completion only after cleanup.
    let recovered = attachment.teardown().await;

    // A device whose rebuild failed refused the replay feeding it, which failed the
    // attachment, and the rebuild's failure says why.
    let outcome = match (outcome, recovered) {
        (Err(_), Err(err)) => Err(proto_grpc::anyhow_to_status(err)),
        (outcome, Err(err)) => {
            tracing::error!(?err, "a disk's rebuild failed");
            outcome
        }
        (outcome, Ok(())) => outcome,
    };

    if let Err(status) = outcome {
        tracing::warn!(%status, "attachment failed");
        _ = responses.send(Err(status)).await;
    }
}

/// What one attachment holds while it serves its stream, including the disk which
/// `run` tears down however `serve` ends.
struct Attachment {
    /// Daemon configuration.
    config: std::sync::Arc<crate::daemon::Config>,
    /// Client's user and group, which own the mount.
    peer: (u32, u32),
    /// Cancelled by a daemon drain, a client which drops its stream, or a writer
    /// failure. Cancellation gives up the attachment's broker calls.
    cancel: tokio_util::sync::CancellationToken,
    /// The disk's device while it stands by, until `Promote`.
    device_standby: Option<device::Standby>,
    /// The disk's device once promoted.
    device: Option<device::Device>,
    /// The journal writer of a promoted disk.
    writer: Option<journal::Writer>,
    /// The filesystem mounted on a promoted disk.
    mount: Option<filesystem::Mount>,
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
        let open = match next(requests, &self.cancel).await? {
            None => return Ok(()),
            Some(proto::request::Request::Open(open)) => open,
            Some(_) => {
                return Err(crate::failed_precondition(
                    "the first request of an attachment must be Open",
                ));
            }
        };
        let (journal, blocks) = check_open(open)?;
        let config = &self.config;

        let (device_standby, replayed_tx) =
            device::Standby::open(&config.image_dir, blocks, config.horizon_policy)?;
        self.device_standby = Some(device_standby);

        let uncommitted = journal::UncommittedDelta::create(&config.image_dir)
            .with_context(|| format!("creating an uncommitted delta in {:?}", config.image_dir))?;

        // Replay to the head `Open` observed before reading another request, so
        // `Opened` precedes any `Promote`.
        let mut journal_standby = journal::Standby::new(
            &config.brokers,
            journal,
            self.cancel.clone(),
            replayed_tx,
            uncommitted,
        )
        .await?;
        () = send(
            responses,
            proto::response::Response::Opened(proto::Opened {}),
        )
        .await?;

        // Stand by, following the journal, until `Promote`.
        let ack = tokio::select! {
            request = next(requests, &self.cancel) => match request? {
                None => return Ok(()),
                Some(proto::request::Request::Promote(proto::Promote { ack })) => ack,
                Some(_) => {
                    return Err(crate::failed_precondition(
                        "a standby attachment serves only Promote",
                    ));
                }
            },
            failure = journal_standby.follow() => return Err(failure),
        };

        // Fence the previous writer, finish replay at the fenced head, and repair
        // any ack the client recovered.
        let recovered_ack = (!ack.is_empty()).then_some(ack);
        let (promoted, replayed_tx) = journal_standby.promote(recovered_ack).await?;

        let device_standby = self
            .device_standby
            .take()
            .expect("a disk's device stands by until its promotion");
        let (device, block_path, served_rx) = device_standby.promote(replayed_tx).await?;
        self.device = Some(device);

        let fresh = promoted.fresh();
        let writer = self.writer.insert(promoted.into_writer(served_rx));

        if fresh {
            () = filesystem::format(&block_path, self.peer).await?;
        }
        // A mount point names the device it presents.
        let name = block_path.file_name().expect("a block device has a name");
        let mount_path = config.mount_dir.join(name);
        let mount = self
            .mount
            .insert(filesystem::Mount::new(&block_path, mount_path, self.peer).await?);

        tracing::info!(mount = ?mount.path(), fresh, "opened a disk");

        // Commit setup writes before returning the mount to the client.
        if let Some(ack) = prepare(mount, writer).await? {
            () = writer.acknowledge(ack).await?;
        }

        let mount_path = mount.path().display().to_string();
        () = send(
            responses,
            proto::response::Response::Promoted(proto::Promoted { mount_path }),
        )
        .await?;

        loop {
            let response = match next(requests, &self.cancel).await? {
                None => return Ok(()),
                Some(proto::request::Request::Prepare(proto::Prepare {})) => {
                    proto::response::Response::Prepared(proto::Prepared {
                        ack: prepare(mount, writer).await?.unwrap_or_default(),
                    })
                }
                Some(proto::request::Request::Acknowledge(proto::Acknowledge { ack })) => {
                    () = writer.acknowledge(ack).await?;
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

    /// Take the disk apart in the one order which cannot deadlock. Dropping the
    /// writer first lets the device's writes stop waiting on its channel. The
    /// unmount writes, and the device's stop waits for every request in flight.
    /// Returns how the rebuild of a device still standing by went.
    async fn teardown(&mut self) -> anyhow::Result<()> {
        drop(self.writer.take());

        if let Some(mount) = self.mount.take()
            && let Err(err) = mount.unmount().await
        {
            tracing::error!(?err, "failed to unmount a disk");
        }

        if let Some(device) = self.device.take() {
            match device.stop().await {
                Ok(()) => tracing::info!("closed a disk"),
                Err(err) => tracing::error!(?err, "a disk's device failed"),
            }
        }

        // `serve` has returned, dropping the journal's standby and the sender of the
        // chunks it replayed, so the rebuilder exits once it applies what it holds.
        match self.device_standby.take() {
            Some(device_standby) => device_standby.join().await,
            None => Ok(()),
        }
    }
}

/// Sync the disk's filesystem, and then cut the delta which holds what it wrote.
async fn prepare(
    mount: &filesystem::Mount,
    writer: &journal::Writer,
) -> anyhow::Result<Option<bytes::Bytes>> {
    let path = mount.path().to_path_buf();

    () = tokio::task::spawn_blocking(move || filesystem::sync(&path))
        .await?
        .context("syncing a disk's filesystem")?;

    writer.prepare().await
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
    let blocks = u32::try_from(device_size / crate::BLOCK_SIZE as u64)
        .expect("a device has fewer than 2^32 blocks, which a chunk indexes");

    Ok((journal, blocks))
}

async fn next(
    requests: &mut tonic::Streaming<proto::Request>,
    cancel: &tokio_util::sync::CancellationToken,
) -> anyhow::Result<Option<proto::request::Request>> {
    // Requests first, so that a client which dropped its stream reports that,
    // rather than the cancellation it caused.
    let message = tokio::select! {
        biased;

        message = requests.message() => message.map_err(proto_grpc::status_to_anyhow)?,
        () = cancel.cancelled() => return Err(crate::cancelled()),
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

#[cfg(test)]
mod test {
    use super::check_open;
    use crate::proto;

    #[test]
    fn test_an_open_is_checked_before_anything_exists() {
        let mut out = Vec::new();

        for (journal, device_size) in [("acmeCo/disk", 1 << 30), ("", 1 << 30)] {
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
        ");
    }
}
