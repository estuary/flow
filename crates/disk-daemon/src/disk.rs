//! One disk's lifecycle: a sparse image, a `ublk` device over it, and the owner
//! which serves that device.

use crate::capture::Captured;
use crate::image::Image;
use crate::owner::{self, Pool};
use crate::ublk::{self, Control};

pub struct Spec {
    /// Directory the image is created in.
    pub image_dir: std::path::PathBuf,
    pub blocks: u32,
    /// Fixed once a disk first publishes: it shapes chunk coverage and bitmap
    /// extent, so a later change would misplace every replayed chunk.
    pub block_size: u32,
    /// Mutations the capture channel holds before a device request must wait.
    pub capture_capacity: usize,
}

pub struct Disk {
    control: std::sync::Arc<Control>,
    owner: owner::Handle,
    dev_id: u32,
    /// Taken by the first teardown, so `stop` and `drop` cannot both run it.
    live: bool,
}

impl Disk {
    /// Create an image, a device over it, and serve it. The returned
    /// [`Captured`] is the consumer half of the disk's capture channel, which
    /// the journal appender takes.
    ///
    /// The order is forced by the kernel: parameters may only be set before the
    /// device starts, and starting it blocks until its queue is fetching, which
    /// only its owner can arrange.
    pub fn create(
        control: &std::sync::Arc<Control>,
        pool: &Pool,
        spec: &Spec,
    ) -> anyhow::Result<(Self, Captured)> {
        let image = Image::create(&spec.image_dir, spec.blocks, spec.block_size)
            .map_err(|err| anyhow::anyhow!("creating an image in {:?}: {err}", spec.image_dir))?;

        let info = control.add_dev(ublk::QUEUE_DEPTH, ublk::MAX_IO_BUF_BYTES)?;
        let owner = pool.owner();
        let (capture, captured) = crate::capture::channel(spec.capture_capacity, owner.waker());

        let disk = Self {
            control: control.clone(),
            owner,
            dev_id: info.dev_id,
            live: true,
        };

        // From here the device exists, so `disk` owns tearing it down and any
        // error below unwinds through its `Drop`.
        let cdev = open_char_device(info.dev_id)?;
        control.set_params(info.dev_id, &ublk::params(spec.blocks, spec.block_size))?;

        disk.owner.serve(owner::Serve {
            dev_id: info.dev_id,
            cdev,
            image,
            capture,
        })?;
        control.start_dev(info.dev_id)?;

        Ok((disk, captured))
    }

    pub fn dev_id(&self) -> u32 {
        self.dev_id
    }

    /// Path of the block device to format and mount.
    pub fn block_path(&self) -> std::path::PathBuf {
        ublk::block_path(self.dev_id)
    }

    /// Tear the device down and take back the image. Idempotent, and run by
    /// `Drop` if it has not been called, so no device node is left behind.
    pub fn stop(&mut self) -> anyhow::Result<Option<Image>> {
        if !std::mem::take(&mut self.live) {
            return Ok(None);
        }
        // Stopping aborts the queue's fetches, which is how the owner learns to
        // quiesce; deleting waits for every reference to the device, so the
        // owner must have closed the character device first.
        self.control.stop_dev(self.dev_id)?;
        let image = self.owner.release(self.dev_id)?;
        self.control.del_dev(self.dev_id)?;

        Ok(image)
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
/// Nothing here changes the node's ownership: production may run as a dedicated
/// UID with ambient `CAP_SYS_ADMIN`, in which case a udev rule grants that UID
/// the node, since `CAP_SYS_ADMIN` does not bypass file permissions.
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
            // Node creation is visible to userspace slightly after the command
            // which added the device completes.
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
