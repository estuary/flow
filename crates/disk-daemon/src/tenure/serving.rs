use crate::device;
use crate::filesystem;
use crate::journal;
use anyhow::Context;

/// What one open disk consists of.
///
/// The fields are declared in the order [`Serving::teardown`] runs them, which is
/// also the order they drop in. The filesystem unmounts before the device under it
/// stops. The writer outlives both, because an unmount writes.
pub struct Serving {
    pub mount: filesystem::Mount,
    device: device::Device,
    pub writer: journal::Writer,
}

impl Serving {
    /// Finish replay, repair any recovered ack, and serve the image. Format only
    /// if it has no committed state.
    ///
    /// Commit format and mount writes before returning, so client acknowledgements
    /// cover only client writes and reopening an untouched disk recovers its
    /// filesystem without formatting again.
    pub async fn open(
        config: &crate::daemon::Config,
        control: &std::sync::Arc<crate::ublk::Control>,
        peer: Option<(u32, u32)>,
        claimed: journal::Claimed,
        playback: journal::Playback,
        recovered_ack: Option<bytes::Bytes>,
    ) -> anyhow::Result<Self> {
        let (
            promoted,
            journal::Recovered {
                image,
                fresh,
                horizon,
            },
        ) = claimed.promote(playback, recovered_ack).await?;

        let control = control.clone();
        let policy = config.horizon;

        // Creating a device is a handshake with the kernel and with the thread
        // which will own it. Neither handshake is async.
        let (device, recorded, compactor) = tokio::task::spawn_blocking(move || {
            device::Device::create(&control, image, crate::ublk::QUEUE_DEPTH, horizon, policy)
        })
        .await??;

        let block_path = crate::ublk::block_path(device.dev_id());
        let mount_path = config.mount_dir.join(format!(
            "{}{}",
            crate::daemon::MOUNT_PREFIX,
            device.dev_id()
        ));

        // Start draining before `mkfs` or mount writes can fill the bounded
        // recording channel and stall the device.
        let writer = promoted.serve(recorded, compactor);

        // Failures must use ordered teardown: a plain drop could block this
        // runtime thread stopping a device whose writer is retrying broker I/O.
        let mounted = async {
            if fresh {
                () = filesystem::format(&block_path, peer, filesystem::MKFS_TIMEOUT).await?;
            }
            // A mount which stalls waits on the writer, which may be retrying a
            // broker it cannot reach. Abandoning it frees the mount to land or fail.
            let release = || writer.abandon();

            filesystem::Mount::new(
                &block_path,
                mount_path,
                peer,
                filesystem::MOUNT_TIMEOUT,
                release,
            )
            .await
        }
        .await;

        let mount = match mounted {
            Ok(mount) => mount,
            Err(err) => {
                () = tear_down(None, device, writer).await;
                return Err(err);
            }
        };
        let mut serving = Self {
            mount,
            device,
            writer,
        };

        // Commit setup writes before returning the mount to the client.
        let committed = async {
            if let Some(ack) = serving.prepare().await? {
                () = serving.writer.acknowledge(ack).await?;
            }
            anyhow::Ok(())
        }
        .await;

        if let Err(err) = committed {
            () = serving.teardown().await;
            return Err(err);
        }

        tracing::info!(
            dev_id = serving.device.dev_id(),
            mount = ?serving.mount.path(),
            fresh,
            "opened a disk",
        );

        Ok(serving)
    }

    /// Flush the filesystem, close admission, and prepare the recorded delta.
    /// Each mutation is recorded and applied before the owner can process the cut.
    /// Resume admission afterward; the writer holds the next delta back until
    /// this one's acknowledgement lands.
    pub async fn prepare(&mut self) -> anyhow::Result<Option<bytes::Bytes>> {
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

    /// Unmount, destroy the device, and drop the image.
    pub async fn teardown(self) {
        let Self {
            mount,
            device,
            writer,
        } = self;

        tear_down(Some(mount), device, writer).await
    }
}

/// Take a disk apart in the one order which cannot deadlock. `mount` is `None` for
/// a disk which failed before it was mounted.
///
/// The writer outlives both the unmount and the stop, taking whatever the device
/// mutates. An unmount writes, and a stop waits for every request in flight,
/// parked ones included.
async fn tear_down(
    mount: Option<filesystem::Mount>,
    mut device: device::Device,
    writer: journal::Writer,
) {
    // This tenure prepares nothing more. The writer takes what the unmount
    // mutates and then discards it.
    () = writer.abandon();

    let dev_id = device.dev_id();

    if let Some(mut mount) = mount
        && let Err(err) = mount.unmount(filesystem::MOUNT_TIMEOUT).await
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
