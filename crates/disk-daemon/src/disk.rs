//! One disk's lifecycle: a sparse image, a `ublk` device over it, and the
//! thread which serves that device.

use crate::capture::Captured;
use crate::image::Image;
use crate::owner;
use crate::ublk::{self, Control};

pub struct Disk {
    control: std::sync::Arc<Control>,
    /// Taken by the first teardown, so `stop` and `drop` cannot both run it.
    owner: Option<owner::Handle>,
    dev_id: u32,
}

impl Disk {
    /// Create a device over `image` and serve it. The returned [`Captured`] is
    /// the consumer half of the disk's capture channel, which the journal
    /// appender takes.
    ///
    /// The caller supplies the image. A recovered disk is rebuilt from its
    /// journal before any device may read it.
    ///
    /// The kernel forces the order of these steps. Parameters may only be set
    /// before the device starts. Starting it then blocks until its queue is
    /// fetching, which only the owner can arrange.
    pub fn create(
        control: &std::sync::Arc<Control>,
        image: Image,
        queue_depth: u16,
        horizon: crate::horizon::Policy,
        metrics: crate::metrics::Device,
    ) -> anyhow::Result<(Self, Captured)> {
        let info = control.add_dev(queue_depth, ublk::MAX_IO_BUF_BYTES)?;

        let served = Self::serve(control, image, queue_depth, horizon, metrics, info.dev_id);

        let (disk, captured) = match served {
            Ok(served) => served,
            Err(err) => {
                // No owner took the device, so nothing else will delete it.
                if let Err(err) = control.del_dev(info.dev_id) {
                    tracing::error!(
                        dev_id = info.dev_id,
                        ?err,
                        "failed to delete a device which could not be served"
                    );
                }
                return Err(err);
            }
        };

        // A failure here tears the device down by dropping `disk`.
        () = control.start_dev(info.dev_id)?;

        Ok((disk, captured))
    }

    /// Hand the device to an owner. Every failure here is before that owner
    /// exists, so the caller may still delete the device.
    fn serve(
        control: &std::sync::Arc<Control>,
        image: Image,
        queue_depth: u16,
        horizon: crate::horizon::Policy,
        metrics: crate::metrics::Device,
        dev_id: u32,
    ) -> anyhow::Result<(Self, Captured)> {
        let cdev = open_char_device(dev_id)?;
        () = control.set_params(dev_id, &ublk::params(image.blocks()))?;

        // One waker serves both directions. The channel wakes the owner when a
        // mutation it parked may be retried, and a command wakes it to be read.
        let waker = crate::wake::Waker::new()?;
        let (capture, captured) = crate::capture::channel(queue_depth as usize, waker.clone());

        let owner = owner::spawn(owner::Serve {
            dev_id,
            cdev,
            image,
            capture,
            waker,
            queue_depth,
            horizon,
            metrics,
        })?;

        Ok((
            Self {
                control: control.clone(),
                owner: Some(owner),
                dev_id,
            },
            captured,
        ))
    }

    pub fn dev_id(&self) -> u32 {
        self.dev_id
    }

    /// Cut this disk's mutations at a point in time, per
    /// [`owner::Handle::close_admission`].
    pub async fn close_admission(&self) -> anyhow::Result<()> {
        self.handle()?.close_admission().await
    }

    pub fn resume_admission(&self) -> anyhow::Result<()> {
        self.handle()?.resume_admission()
    }

    /// A handle with which the journal writer asks for this disk's image, per
    /// [`owner::Snapshotter`].
    pub fn snapshotter(&self) -> anyhow::Result<owner::Snapshotter> {
        Ok(self.handle()?.snapshotter())
    }

    /// A handle with which the journal writer opens and completes this disk's
    /// recovery horizons, per [`owner::Compactor`].
    pub fn compactor(&self) -> anyhow::Result<owner::Compactor> {
        Ok(self.handle()?.compactor())
    }

    /// Path of the block device to format and mount.
    pub fn block_path(&self) -> std::path::PathBuf {
        ublk::block_path(self.dev_id)
    }

    /// Tear the device down and take back the image. This is idempotent. `Drop`
    /// runs it if the caller has not, so no device node is left behind.
    pub fn stop(&mut self) -> anyhow::Result<Option<Image>> {
        let Some(owner) = self.owner.take() else {
            return Ok(None);
        };
        // Stopping aborts the queue's fetches, which is how the owner learns to
        // quiesce. Deleting then waits for every reference to the device, so the
        // owner must have closed the character device first.
        () = self.control.stop_dev(self.dev_id)?;
        let image = owner.release()?;
        () = self.control.del_dev(self.dev_id)?;

        Ok(Some(image))
    }

    fn handle(&self) -> anyhow::Result<&owner::Handle> {
        self.owner
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("device {} is stopped", self.dev_id))
    }
}

impl Drop for Disk {
    fn drop(&mut self) {
        if let Err(err) = self.stop() {
            tracing::error!(dev_id = self.dev_id, ?err, "failed to tear down a device");
        }
    }
}

/// Open `/dev/ublkcN`, which `devtmpfs` creates as the device is added.
///
/// Nothing here changes the node's ownership. The crate README says how a daemon
/// which does not run as root is granted it.
fn open_char_device(dev_id: u32) -> anyhow::Result<std::fs::File> {
    let path = ublk::char_path(dev_id);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);

    loop {
        match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
        {
            Ok(file) => return Ok(file),
            // Userspace sees the node slightly after the command which added the
            // device completes.
            Err(err)
                if err.kind() == std::io::ErrorKind::NotFound
                    && std::time::Instant::now() < deadline =>
            {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            Err(err) => anyhow::bail!("opening {path:?}: {err}"),
        }
    }
}
