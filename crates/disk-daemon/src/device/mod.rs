use crate::proto;
use anyhow::Context;

mod chunk;
mod compactor;
mod image;
mod owner;
mod reap;
mod ublk;

pub use compactor::Policy;
pub use reap::reap;

/// The capacity, in chunks, of the channel of live chunks from the device to the
/// journal. It's set to the ublk queue depth, which is how many simultaneous
/// requests can be received from the device.
const CHUNK_CHANNEL_CAPACITY: usize = ublk::QUEUE_DEPTH as usize;

/// The capacity, in read responses, of the channel of journal content to a
/// standby's rebuilder. A response may be far larger than a chunk.
const CONTENT_CHANNEL_CAPACITY: usize = 16;

/// A disk's image while it is rebuilt by replaying the journal, before any kernel
/// device exists.
pub struct Standby {
    /// The disk's size in blocks of `crate::BLOCK_SIZE`.
    size_blocks: u32,
    /// Replays journal content into the image.
    rebuilder: std::thread::JoinHandle<anyhow::Result<(image::Image, bool)>>,
}

impl Standby {
    /// Start a rebuilder thread over a new sparse image, which replays the journal
    /// content sent to it. Returns the standby and the sender of that content. Hand
    /// the sender to `Standby::start`, or drop it and then `Standby::discard`.
    pub fn open(
        image_dir: &std::path::Path,
        size_blocks: u32,
        horizon_policy: compactor::Policy,
    ) -> anyhow::Result<(Self, tokio::sync::mpsc::Sender<(i64, bytes::Bytes)>)> {
        let file = tempfile::tempfile_in(image_dir)
            .with_context(|| format!("creating an image in {image_dir:?}"))?;
        () = file
            .set_len(size_blocks as u64 * crate::BLOCK_SIZE as u64)
            .with_context(|| format!("sizing an image in {image_dir:?}"))?;

        let mut image = image::Image::new(file, size_blocks, horizon_policy);
        let mut replay = crate::replay::Replay::new(image_dir);
        let (content_tx, mut content_rx) = tokio::sync::mpsc::channel(CONTENT_CHANNEL_CAPACITY);

        let rebuilder = std::thread::Builder::new()
            .name("disk-rebuilder".to_string())
            .spawn(move || {
                let mut fresh = true;

                while let Some((offset, content)) = content_rx.blocking_recv() {
                    () = replay.push(
                        offset,
                        content,
                        |offset,
                         proto::DiskRecord {
                             chunk,
                             opens_horizon,
                             ..
                         }| {
                            fresh = false;
                            let chunk = chunk.context("a committed record carries no chunk")?;
                            image.replay(&chunk, opens_horizon.then_some(offset))
                        },
                    )?;
                }
                Ok((image, fresh))
            })
            .context("spawning a disk's rebuilder")?;

        let standby = Self {
            size_blocks,
            rebuilder,
        };

        Ok((standby, content_tx))
    }

    /// End the rebuild, closing `content_tx`, and start serving the rebuilt image as
    /// a ublk device.
    pub fn start(
        self,
        content_tx: tokio::sync::mpsc::Sender<(i64, bytes::Bytes)>,
    ) -> anyhow::Result<Started> {
        let Self {
            size_blocks,
            rebuilder,
        } = self;

        // The rebuilder returns once it has replayed the last content sent.
        drop(content_tx);
        let (image, fresh) = rebuilder
            .join()
            .map_err(|_panic| anyhow::anyhow!("a disk's rebuilder panicked"))??;

        // Add the device, start its owner thread, and then start the device.
        let (live_tx, live_rx) = tokio::sync::mpsc::channel(CHUNK_CHANNEL_CAPACITY);
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
                return Err(control
                    .abandon(anyhow::Error::new(err).context(format!("opening {cdev_path:?}"))));
            }
        };
        let owner = match owner::spawn(dev_id, cdev, image, live_tx) {
            Ok(owner) => owner,
            Err(err) => return Err(control.abandon(err)),
        };
        let mut device = Device { control, owner };

        // `owner::spawn` returns once every tag has a fetch in flight, which the
        // start waits for.
        if let Err(err) = device.control.start() {
            return Err(match device.stop() {
                Ok(()) => err,
                Err(stopped) => err.context(format!(
                    "device {dev_id} could not be started, nor stopped ({stopped:#})"
                )),
            });
        }

        let block_path = device.control.block_path();
        Ok(Started {
            device,
            block_path,
            live_rx,
            fresh,
        })
    }

    /// Discard the standby, and return how its rebuild went. Drop the sender of
    /// journal content first, or this waits for it.
    pub fn discard(self) -> anyhow::Result<()> {
        let Self { rebuilder, .. } = self;

        rebuilder
            .join()
            .map_err(|_panic| anyhow::anyhow!("a disk's rebuilder panicked"))?
            .map(drop)
    }
}

/// What `Standby::start` returns.
pub struct Started {
    /// The device, which serves the rebuilt image until it is stopped.
    pub device: Device,
    /// The block device which the kernel presents.
    pub block_path: std::path::PathBuf,
    /// The live chunk of each write, punch, and copy the device applies.
    pub live_rx: tokio::sync::mpsc::Receiver<anyhow::Result<crate::LiveChunk>>,
    /// Replay applied nothing, so the journal holds no committed state.
    pub fresh: bool,
}

/// A started disk, whose owner thread serves its image as a ublk device.
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
    pub fn stop(self) -> anyhow::Result<()> {
        let Self { mut control, owner } = self;
        let dev_id = control.dev_id();

        () = control.stop()?;

        let served = match owner.join() {
            Ok(served) => served.with_context(|| format!("serving device {dev_id}")),
            Err(_panic) => Err(anyhow::anyhow!("the owner of device {dev_id} panicked")),
        };
        if mounted(&control.block_path()).unwrap_or(true) {
            tracing::warn!(
                dev_id,
                "leaving a device which is still mounted for the reaper"
            );
            return served;
        }
        match served {
            Ok(()) => control.delete(),
            Err(err) => Err(control.abandon(err)),
        }
    }
}

/// Whether `block_path` is the source of a mount in this mount namespace.
fn mounted(block_path: &std::path::Path) -> std::io::Result<bool> {
    let mounts = std::fs::read_to_string("/proc/self/mounts")?;
    Ok(mounts
        .lines()
        .any(|line| line.split(' ').next() == block_path.to_str()))
}
