use crate::proto;
use anyhow::Context;

mod chunk;
pub mod horizon;
mod image;
mod owner;
mod queue;
mod target;
mod ublk;

/// A chunk the device served, which one journal record carries: a device write or
/// punch, or a copy which discharges a recovery horizon.
pub struct Mutation {
    /// The blocks it writes or punches.
    pub chunk: proto::Chunk,
    /// A recovery horizon opened at this mutation.
    pub opens_horizon: bool,
    /// This mutation discharged the open horizon's last pending block.
    pub discharges_horizon: bool,
}

/// A disk's image while a rebuilder thread rebuilds it from the journal's replayed
/// chunks, before any kernel device exists. `Standby::promote` turns it into a
/// `Device`.
pub struct Standby {
    /// The disk's size in blocks of `crate::BLOCK_SIZE`, which both the image and
    /// the kernel device take.
    size_blocks: u32,
    /// Applies replayed chunks to the image, and returns it once their sender is
    /// dropped.
    rebuilder: std::thread::JoinHandle<anyhow::Result<target::Target>>,
}

impl Standby {
    /// Start a rebuilder thread over a new sparse image of `size_blocks` within
    /// `image_dir`. Returns the standby and the sender of its replayed chunks: the
    /// committed chunks the image rebuilds from, in journal order, each with
    /// whether a recovery horizon opened at it. A send fails once the rebuild has
    /// failed. Hand the sender to `Standby::promote`, or drop it and then
    /// `Standby::join`.
    pub fn open(
        image_dir: &std::path::Path,
        size_blocks: u32,
        horizon_policy: horizon::Policy,
    ) -> anyhow::Result<(Self, tokio::sync::mpsc::Sender<(proto::Chunk, bool)>)> {
        let image = image::Image::create(image_dir, size_blocks)
            .with_context(|| format!("creating an image in {image_dir:?}"))?;
        let mut target = target::Target::new(image, horizon_policy);

        let (replayed_tx, mut replayed_rx) = tokio::sync::mpsc::channel(ublk::QUEUE_DEPTH as usize);
        let rebuilder = std::thread::Builder::new()
            .name("disk-rebuilder".to_string())
            .spawn(move || {
                while let Some((chunk, opens_horizon)) = replayed_rx.blocking_recv() {
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

    /// End the rebuild and serve the rebuilt image as a ublk device. Returns the
    /// device, its block device's path, and the receiver of its served mutations:
    /// each write, punch, and copy it applies, in order. Every prefix of them
    /// rebuilds a crash-consistent image. They end early only where the image
    /// failed, and while the channel is full the device serves no request. A
    /// failure leaves nothing to tear down.
    pub async fn promote(
        self,
        replayed_tx: tokio::sync::mpsc::Sender<(proto::Chunk, bool)>,
    ) -> anyhow::Result<(
        Device,
        std::path::PathBuf,
        tokio::sync::mpsc::Receiver<Mutation>,
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

        // One ring wait hands over at most a queue's depth of writes, so the owner
        // sends a full batch without waiting on the writer, unless the batch earns
        // copies.
        let (served_tx, served_rx) = tokio::sync::mpsc::channel(ublk::QUEUE_DEPTH as usize);

        // Each step blocks, on the kernel or on the owner arming its queue. An owner
        // which cannot arm closes the character device as it fails, so nothing
        // holds the device, which can then be deleted.
        let (control, dev_id, owner) = tokio::task::spawn_blocking(move || {
            let (control, dev_id, cdev) = add(size_blocks)?;

            match owner::spawn(dev_id, cdev, target, served_tx) {
                Ok(owner) => Ok((control, dev_id, owner)),
                Err(err) => Err(delete(&control, dev_id, err)),
            }
        })
        .await??;

        let device = Device {
            control: std::sync::Arc::new(control),
            owner,
            dev_id,
        };

        // The owner serves an armed queue now, which only a stop ends.
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

    /// Wait for the rebuilder to exit, and return how its rebuild went. Drop the
    /// sender of replayed chunks first, or this waits for it.
    pub async fn join(self) -> anyhow::Result<()> {
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
    control: std::sync::Arc<ublk::Control>,
    /// The owner's thread, which returns once the kernel stops the device.
    owner: std::thread::JoinHandle<anyhow::Result<()>>,
    /// Kernel device ID.
    dev_id: u32,
}

impl Device {
    /// Stop and join the owner, and delete the kernel device. Returns why the owner
    /// failed, if it did. This is the only teardown: a device which is dropped
    /// instead leaks its owner and its kernel device. Drop the receiver of its
    /// served mutations first, and unmount any filesystem on it, or this waits for
    /// them.
    pub async fn stop(self) -> anyhow::Result<()> {
        let Self {
            control,
            owner,
            dev_id,
        } = self;

        tokio::task::spawn_blocking(move || {
            // On failure, return without joining. The owner serves until the kernel
            // stops the device, so a join would hang, and so would a delete, which
            // waits for the owner to close the character device.
            () = control.stop_dev(dev_id)?;

            let served = match owner.join() {
                Ok(served) => served.with_context(|| format!("serving device {dev_id}")),
                // `join` fails only where the thread panicked.
                Err(_panic) => Err(anyhow::anyhow!("the owner of device {dev_id} panicked")),
            };
            // Delete the device whatever the owner returned: it has exited, closing
            // the character device, so nothing holds the device any longer.
            let deleted = control.del_dev(dev_id);

            () = served?;
            deleted
        })
        .await?
    }
}

/// Add a kernel device of `size_blocks` under a control handle of its own,
/// configure it, and open its character device for the owner.
fn add(size_blocks: u32) -> anyhow::Result<(ublk::Control, u32, std::fs::File)> {
    let control = ublk::Control::open()?;
    let dev_id = control.add_dev(ublk::QUEUE_DEPTH, ublk::MAX_IO_BUF_BYTES)?;

    // Before the character device opens: a delete waits for it to close, and
    // would wait forever on a descriptor this frame still holds.
    () = control
        .set_params(dev_id, &ublk::params(size_blocks))
        .map_err(|err| delete(&control, dev_id, err))?;

    // devtmpfs creates the node before the command which added the device completes.
    let cdev_path = ublk::char_path(dev_id);
    let cdev = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&cdev_path)
        .with_context(|| format!("opening {cdev_path:?}"))
        .map_err(|err| delete(&control, dev_id, err))?;

    Ok((control, dev_id, cdev))
}

/// Delete device `dev_id`, which `err` left unserved, and return `err`.
fn delete(control: &ublk::Control, dev_id: u32, err: anyhow::Error) -> anyhow::Error {
    match control.del_dev(dev_id) {
        Ok(()) => err,
        Err(deleted) => err.context(format!(
            "device {dev_id} could not be served, nor deleted ({deleted:#})"
        )),
    }
}

#[cfg(test)]
mod test {
    use super::{Standby, chunk, horizon};
    use crate::BLOCK_SIZE;
    use crate::proto;

    /// A policy which never opens a horizon of its own.
    const NEVER_OPENS: horizon::Policy = horizon::Policy {
        open_ratio: 1.0,
        copy_ratio: 0.0,
        minimum_bytes: u64::MAX,
    };

    /// Blocks of each standby's disk.
    const BLOCKS: u32 = 16;

    /// A replayed write of `block`, filled with `fill`, which opens no horizon.
    fn write(block: u32, fill: u8) -> (proto::Chunk, bool) {
        let data = bytes::Bytes::from(vec![fill; BLOCK_SIZE as usize]);
        (chunk::encode_write(block, &data), false)
    }

    /// A standby applies replayed chunks until their sender is dropped, and then its
    /// rebuilder exits cleanly.
    #[tokio::test]
    async fn test_a_standby_joins_once_its_replay_is_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let (standby, replayed_tx) = Standby::open(dir.path(), BLOCKS, NEVER_OPENS).unwrap();

        for block in 0..4 {
            () = replayed_tx.send(write(block, 0xaa)).await.unwrap();
        }
        drop(replayed_tx);

        () = standby.join().await.unwrap();
    }

    /// A rebuilder which fails takes no more chunks, so a later send fails, and the
    /// standby's join says so. A write past the disk, as only a corrupt journal
    /// holds, panics the rebuilder.
    #[tokio::test]
    async fn test_a_failed_rebuild_refuses_sends_and_joins_with_its_failure() {
        let dir = tempfile::tempdir().unwrap();
        let (standby, replayed_tx) = Standby::open(dir.path(), BLOCKS, NEVER_OPENS).unwrap();

        () = replayed_tx.send(write(BLOCKS, 0xaa)).await.unwrap();

        let mut refused = false;
        for _ in 0..1000 {
            if replayed_tx.send(write(0, 0xbb)).await.is_err() {
                refused = true;
                break;
            }
            () = tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        assert!(refused, "the failed rebuilder still took chunks");
        drop(replayed_tx);

        let err = standby.join().await.unwrap_err();
        insta::assert_snapshot!(format!("{err:#}"), @"a disk's rebuilder panicked");
    }
}
