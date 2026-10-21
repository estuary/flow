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

/// A disk's image while it is rebuilt from the journal's replayed chunks,
/// before any kernel device exists.
pub struct Standby {
    /// The disk's size in blocks of `crate::BLOCK_SIZE`.
    size_blocks: u32,
    /// Applies replayed chunks to the image.
    rebuilder: std::thread::JoinHandle<anyhow::Result<target::Target>>,
}

impl Standby {
    /// Start a rebuilder thread over a new sparse image. Returns the standby
    /// and the sender of the chunks its image rebuilds from. Hand the sender to
    /// `Standby::promote`, or drop it and then `Standby::discard`.
    pub fn open(
        image_dir: &std::path::Path,
        size_blocks: u32,
        horizon_policy: compactor::Policy,
    ) -> anyhow::Result<(Self, tokio::sync::mpsc::Sender<crate::ReplayedChunk>)> {
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
                while let Some(crate::ReplayedChunk {
                    chunk,
                    opens_horizon,
                }) = replayed_rx.blocking_recv()
                {
                    () = target.replay(chunk, opens_horizon)?;
                }
                Ok(target)
            })
            .context("spawning a disk's rebuilder")?;

        let standby = Self {
            size_blocks,
            rebuilder,
        };
        Ok((standby, replayed_tx))
    }

    /// End the rebuild, closing `replayed_tx`, and serve the rebuilt image as a
    /// ublk device.
    pub async fn promote(
        self,
        replayed_tx: tokio::sync::mpsc::Sender<crate::ReplayedChunk>,
    ) -> anyhow::Result<(
        Device,
        std::path::PathBuf,
        tokio::sync::mpsc::Receiver<crate::LiveChunk>,
    )> {
        let Self {
            size_blocks,
            rebuilder,
        } = self;

        // The rebuilder returns once it has applied the last chunk sent.
        drop(replayed_tx);
        let target = tokio::task::spawn_blocking(move || {
            rebuilder
                .join()
                .map_err(|_panic| anyhow::anyhow!("a disk's rebuilder panicked"))?
        })
        .await??;

        // Add the device, start its owner thread, and then start the device.
        let (served_tx, served_rx) = tokio::sync::mpsc::channel(CHUNK_CHANNEL_CAPACITY);
        let device = tokio::task::spawn_blocking(move || {
            let control = ublk::Control::add(size_blocks)?;
            let dev_id = control.dev_id();

            let cdev_path = control.char_path();
            let cdev = match std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&cdev_path)
            {
                Ok(cdev) => cdev,
                Err(err) => {
                    return Err(control.abandon(
                        anyhow::Error::new(err).context(format!("opening {cdev_path:?}")),
                    ));
                }
            };
            let owner = match owner::spawn(dev_id, cdev, target, served_tx) {
                Ok(owner) => owner,
                Err(err) => return Err(control.abandon(err)),
            };
            let mut device = Device { control, owner };

            // `owner::spawn` returns once every tag has a fetch in flight, which
            // the start waits for.
            match device.control.start() {
                Ok(()) => Ok(device),
                Err(err) => Err(match device.stop_blocking() {
                    Ok(()) => err,
                    Err(stopped) => err.context(format!(
                        "device {dev_id} could not be started, nor stopped ({stopped:#})"
                    )),
                }),
            }
        })
        .await??;

        let block_path = device.control.block_path();
        Ok((device, block_path, served_rx))
    }

    /// Discard the standby, and return how its rebuild went. Drop the sender of
    /// replayed chunks first, or this waits for it.
    pub async fn discard(self) -> anyhow::Result<()> {
        let Self { rebuilder, .. } = self;

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
    control: ublk::Control,
    /// The owner's thread, which returns once the kernel stops the device.
    owner: std::thread::JoinHandle<anyhow::Result<()>>,
}

impl Device {
    /// Stop and join the owner, and delete the kernel device. Drop the receiver
    /// of its live chunks first, and unmount any filesystem on it, since this
    /// would otherwise block on them.
    pub async fn stop(self) -> anyhow::Result<()> {
        tokio::task::spawn_blocking(move || self.stop_blocking()).await?
    }

    fn stop_blocking(self) -> anyhow::Result<()> {
        let Self { mut control, owner } = self;
        let dev_id = control.dev_id();

        () = control.stop()?;

        let served = match owner.join() {
            Ok(served) => served.with_context(|| format!("serving device {dev_id}")),
            Err(_panic) => Err(anyhow::anyhow!("the owner of device {dev_id} panicked")),
        };
        let deleted = control.delete();

        () = served?;
        deleted
    }
}
