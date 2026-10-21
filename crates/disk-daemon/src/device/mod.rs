use crate::proto;
use anyhow::Context;

mod chunk;
pub mod compactor;
mod owner;
mod target;
mod ublk;

/// The capacity, in chunks, of the channels between the journal and the image.
/// It's set to the ublk queue depth, which is how many simultaneous requests
/// can be received from the device.
const CHUNK_CHANNEL_CAPACITY: usize = ublk::QUEUE_DEPTH as usize;

/// A chunk the live device applied: a device write or punch, or a horizon copy.
pub struct LiveChunk {
    /// The blocks it writes or punches.
    pub chunk: proto::Chunk,
    /// A recovery horizon opened at this chunk.
    pub opens_horizon: bool,
    /// This chunk discharged the open horizon's last pending block.
    pub discharges_horizon: bool,
}

/// A disk's image while it is rebuilt from the journal's replayed chunks,
/// before any kernel device exists.
pub struct Standby {
    /// The disk's size in blocks of `crate::BLOCK_SIZE`.
    size_blocks: u32,
    /// Applies replayed chunks to the image.
    rebuilder: std::thread::JoinHandle<anyhow::Result<target::Target>>,
    /// The sender of replayed chunks.
    replayed_tx: tokio::sync::mpsc::Sender<(proto::Chunk, bool)>,
}

impl Standby {
    /// Start a rebuilder thread over a new sparse image. Send every chunk the
    /// image should hold through `Standby::replayed_tx`.
    pub fn open(
        image_dir: &std::path::Path,
        size_blocks: u32,
        horizon_policy: compactor::Policy,
    ) -> anyhow::Result<Self> {
        let file = tempfile::tempfile_in(image_dir)
            .with_context(|| format!("creating an image in {image_dir:?}"))?;
        () = file
            .set_len(size_blocks as u64 * crate::BLOCK_SIZE as u64)
            .with_context(|| format!("sizing an image in {image_dir:?}"))?;
        let mut target = target::Target::new(file, size_blocks, horizon_policy);

        let (replayed_tx, mut replayed_rx) = tokio::sync::mpsc::channel(CHUNK_CHANNEL_CAPACITY);
        let rebuilder = std::thread::Builder::new()
            .name("disk-rebuilder".to_string())
            .spawn(move || {
                while let Some((chunk, opens_horizon)) = replayed_rx.blocking_recv() {
                    () = target.replay(chunk, opens_horizon)?;
                }
                Ok(target)
            })
            .context("spawning a disk's rebuilder")?;

        Ok(Self {
            size_blocks,
            rebuilder,
            replayed_tx,
        })
    }

    /// Where to send the chunks the image rebuilds from, each with whether a horizon
    /// opened at it.
    pub fn replayed_tx(&self) -> &tokio::sync::mpsc::Sender<(proto::Chunk, bool)> {
        &self.replayed_tx
    }

    /// Finish the rebuild and serve the rebuilt image as a ublk device.
    pub async fn promote(
        self,
    ) -> anyhow::Result<(
        Device,
        std::path::PathBuf,
        tokio::sync::mpsc::Receiver<LiveChunk>,
    )> {
        let Self {
            size_blocks,
            rebuilder,
            replayed_tx,
        } = self;

        // Finish the rebuild.
        drop(replayed_tx);
        let target = tokio::task::spawn_blocking(move || {
            rebuilder
                .join()
                .map_err(|_panic| anyhow::anyhow!("a disk's rebuilder panicked"))?
        })
        .await??;

        // Start the owner thread for the device.
        let (served_tx, served_rx) = tokio::sync::mpsc::channel(CHUNK_CHANNEL_CAPACITY);
        let (control, dev_id, owner) = tokio::task::spawn_blocking(move || {
            let control = ublk::Control::open()?;
            let dev_id = control.add_dev(ublk::QUEUE_DEPTH, ublk::MAX_IO_BUF_BYTES)?;

            // Cleanup the device which `err` left unserved, and return `err`.
            let delete = |err: anyhow::Error| match control.del_dev(dev_id) {
                Ok(()) => err,
                Err(deleted) => err.context(format!(
                    "device {dev_id} could not be served, nor deleted ({deleted:#})"
                )),
            };

            () = control
                .set_params(dev_id, &ublk::params(size_blocks))
                .map_err(delete)?;

            let cdev_path = ublk::char_path(dev_id);
            let cdev = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&cdev_path)
                .with_context(|| format!("opening {cdev_path:?}"))
                .map_err(delete)?;

            match owner::spawn(dev_id, cdev, target, served_tx) {
                Ok(owner) => Ok((control, dev_id, owner)),
                Err(err) => Err(delete(err)),
            }
        })
        .await??;

        let device = Device {
            control: std::sync::Arc::new(control),
            owner,
            dev_id,
        };

        let control = device.control.clone();
        let started = tokio::task::spawn_blocking(move || control.start_dev(dev_id))
            .await
            .map_err(anyhow::Error::new)
            .and_then(|started| started);

        if let Err(err) = started {
            return Err(match device.stop().await {
                Ok(()) => err,
                Err(stopped) => err.context(format!(
                    "device {dev_id} could not be started, nor stopped ({stopped:#})"
                )),
            });
        }

        Ok((device, ublk::block_path(dev_id), served_rx))
    }

    /// Discard the standby and return how its rebuild went.
    pub async fn discard(self) -> anyhow::Result<()> {
        let Self {
            rebuilder,
            replayed_tx,
            ..
        } = self;

        // Finish the rebuild.
        drop(replayed_tx);
        tokio::task::spawn_blocking(move || {
            rebuilder
                .join()
                .map_err(|_panic| anyhow::anyhow!("a disk's rebuilder panicked"))?
                .map(drop)
        })
        .await?
    }
}

/// A promoted disk, whose owner thread serves its image as a ublk device.
pub struct Device {
    /// This device's own kernel control handle, which starts, stops, and deletes it.
    control: std::sync::Arc<ublk::Control>,
    /// The owner's thread, which returns once the kernel stops the device.
    owner: std::thread::JoinHandle<anyhow::Result<()>>,
    /// Kernel device ID.
    dev_id: u32,
}

impl Device {
    /// Stop and join the owner, and delete the kernel device. Drop the receiver
    /// of its live chunks first, and unmount any filesystem on it, since this
    /// would otherwise block on them.
    pub async fn stop(self) -> anyhow::Result<()> {
        let Self {
            control,
            owner,
            dev_id,
        } = self;

        tokio::task::spawn_blocking(move || {
            () = control.stop_dev(dev_id)?;

            let served = match owner.join() {
                Ok(served) => served.with_context(|| format!("serving device {dev_id}")),
                Err(_panic) => Err(anyhow::anyhow!("the owner of device {dev_id} panicked")),
            };
            let deleted = control.del_dev(dev_id);

            () = served?;
            deleted
        })
        .await?
    }
}
