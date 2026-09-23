//! One open disk: its mount, the device under that mount, and the journal writer
//! behind both.
//!
//! [`Serving::open`] builds a disk from a claimed journal and the playback which
//! rebuilt its image, and commits the daemon's own setup writes before the client
//! sees it. [`Serving::prepare`] cuts the disk at a point in time. [`Serving::teardown`]
//! takes it apart again, in the one order which cannot deadlock, and so does an
//! `open` which fails partway.

use crate::device::Device;
use crate::filesystem::{self, Mount};
use crate::journal::{self, Writer};
use crate::ublk::Control;
use anyhow::Context;

/// What one open disk consists of.
///
/// The fields are declared in the order [`Serving::teardown`] runs them, which is
/// also the order they drop in. The filesystem unmounts before the device under it
/// stops. The writer outlives both, because an unmount writes.
pub struct Serving {
    pub mount: Mount,
    device: Device,
    pub writer: Writer,
}

impl Serving {
    /// Finish `playback` and serve the disk it rebuilt. `claimed` is the journal the
    /// `Promote` which reached this tenure has already claimed.
    ///
    /// A disk with committed state is served from what the replay rebuilt. A disk
    /// without it is formatted instead. Either way the daemon's own setup writes: an
    /// `mkfs` on a fresh disk, and the bookkeeping ext4 does at any mount. Those are
    /// ordinary mutations of a writer which is already running, and this cuts and
    /// acknowledges them itself before it returns. A client's acknowledgements
    /// therefore cover only its own writes, it owes nothing for a disk it never
    /// writes, and a reopen of one recovers the filesystem rather than formatting it
    /// again.
    pub async fn open(
        daemon: &crate::daemon::Config,
        control: &std::sync::Arc<Control>,
        owner: Option<(u32, u32)>,
        claimed: journal::Claimed,
        playback: journal::playback::Playback,
        recovered_acks: Vec<bytes::Bytes>,
    ) -> anyhow::Result<Self> {
        let (
            promoted,
            journal::Recovered {
                image,
                recovered,
                horizon,
            },
        ) = claimed.promote(playback, recovered_acks).await?;

        let control = control.clone();
        let policy = daemon.horizon;

        // Creating a device is a handshake with the kernel and with the thread
        // which will own it. Neither handshake is async.
        let (device, captured) = tokio::task::spawn_blocking(move || {
            Device::create(&control, image, crate::ublk::QUEUE_DEPTH, horizon, policy)
        })
        .await??;

        let compactor = device.compactor();
        let block_path = device.block_path();
        let mount_path = daemon.mount_dir.join(format!(
            "{}{}",
            crate::daemon::MOUNT_PREFIX,
            device.dev_id()
        ));

        // The writer runs before anything writes to the device, because the capture
        // channel is bounded and a mutation nothing takes parks the device. That is
        // true of a fresh disk's `mkfs` as much as of a recovered disk's mount.
        let writer = promoted.serve(captured, Some(compactor));

        // A failure from here on tears the disk down as `teardown` does. Dropping it
        // instead would stop the device on this runtime's thread, without the
        // abandon which cancels a broker call the writer may be retrying, and a
        // stop waits for every request that writer would otherwise take.
        let mounted = async {
            if !recovered {
                () = filesystem::format(&block_path, owner, filesystem::MKFS_TIMEOUT).await?;
            }
            Mount::new(&block_path, mount_path, owner, filesystem::MOUNT_TIMEOUT).await
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

        // The bootstrap commit. It is the same cut a client's `Prepare` makes, so
        // whatever the format and the mount wrote is committed state of the journal
        // before the client is told the disk exists. A mount which wrote nothing is
        // an empty delta, and owes nothing.
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
            recovered,
            "opened a disk",
        );

        Ok(serving)
    }

    /// Cut a point-in-time boundary of the disk and finish the delta before it.
    ///
    /// The cut runs in this order. The mount flushes, and admission closes. The
    /// owner captures a mutation and applies it in one step, so each one falls
    /// entirely before or after the boundary. The writer can therefore finish
    /// exactly the delta which precedes it.
    ///
    /// Admission resumes as soon as the acknowledgement exists. The mutations
    /// admitted from then on belong to the next delta, which the writer holds back
    /// until this one's acknowledgement has landed.
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
async fn tear_down(mount: Option<Mount>, mut device: Device, writer: Writer) {
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
        Ok(Ok(_image)) => (),
        Ok(Err(err)) => tracing::error!(?err, dev_id, "failed to stop a device"),
        Err(panic) => tracing::error!(?panic, dev_id, "panicked stopping a device"),
    }
    drop(writer);

    tracing::info!(dev_id, "closed a disk");
}
