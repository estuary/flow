//! One disk's device: a `ublk` device over a sparse image, and the thread which
//! owns that device's queue.
//!
//! [`Device`] is the device's life, in the order the kernel forces: add, set
//! parameters, hand the queue to an owner, start — then stop, release, delete. A
//! tenure cuts the disk through it, and its journal writer compacts the disk through
//! a [`Compactor`]. Both are commands to the owner.
//!
//! Exactly one thread owns a disk, and only that thread mutates its image and
//! bitmaps. Every decision about a block is therefore serialized without a lock.
//!
//! It is a thread rather than a task because `ublk` binds a device's queue to the
//! thread which arms its first fetch. It rejects every later command from any
//! other thread with `EINVAL`. The thread blocks in `submit_and_wait`. A
//! [`Waker`] armed on the ring interrupts it when a command arrives or capture
//! capacity frees.
//!
//! An owner never blocks anywhere else. It submits image and character-device
//! I/O to the ring and reaps it later. A disk whose capture channel is full parks
//! only the requests which need that channel.
//!
//! The files of this directory are:
//!
//! - `mod.rs` — [`Device`], the commands it and a [`Compactor`] send, and the
//!   [`Owner`] state the other files act on. Every field lives here; each file adds
//!   the `impl Owner` block of its own concern.
//! - `owner.rs` — the thread itself, and how it takes those commands.
//! - `ring.rs` — the `io_uring`: submission, reaping, and the encoding which names
//!   the request and step a completion belongs to.
//! - `request.rs` — the per-tag path of one device request, from the fetch which
//!   hands it over to the completion which hands it back.
//! - `admission.rs` — what may be captured now: the cut, the requests parked behind a
//!   full capture channel, and the horizon copies a delta's budget pays for.
//! - `inflight.rs` — serialization of overlapping mutations.

use crate::capture::{Capture, Captured};
use crate::horizon::{Horizon, Policy};
use crate::image::Image;
use crate::ublk::{self, Control};
use crate::wake::Waker;
use inflight::InFlight;
use request::Slot;
use ring::Backlog;

mod admission;
mod inflight;
mod owner;
mod request;
mod ring;

#[cfg(test)]
mod device_test;

/// A `ublk` device over one disk's image, and the owner thread which serves it.
pub struct Device {
    control: std::sync::Arc<Control>,
    /// Taken by the first teardown, so `stop` and `drop` cannot both run it.
    commands: Option<Commands>,
    dev_id: u32,
}

impl Device {
    /// Create a device over `image` and serve it. The returned [`Captured`] is
    /// the consumer half of the disk's capture channel, which the journal
    /// writer takes.
    ///
    /// The caller supplies the image, and `horizon` is one the replay of that
    /// image left open, per `journal::Recovered`.
    ///
    /// Parameters may only be set before the device starts. Starting it then
    /// blocks until its queue is fetching, which only the owner can arrange.
    pub fn create(
        control: &std::sync::Arc<Control>,
        image: Image,
        queue_depth: u16,
        horizon: Option<Horizon>,
        policy: Policy,
    ) -> anyhow::Result<(Self, Captured)> {
        let info = control.add_dev(queue_depth, ublk::MAX_IO_BUF_BYTES)?;

        let served = Self::serve(control, image, queue_depth, horizon, policy, info.dev_id);

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
        horizon: Option<Horizon>,
        policy: Policy,
        dev_id: u32,
    ) -> anyhow::Result<(Self, Captured)> {
        let cdev = open_char_device(dev_id)?;
        () = control.set_params(dev_id, &ublk::params(image.blocks()))?;

        // One waker serves both directions. The channel wakes the owner when a
        // mutation it parked may be retried, and a command wakes it to be read.
        let waker = Waker::new()?;
        let (capture, captured) = crate::capture::channel(queue_depth as usize, waker.clone());

        let commands = owner::spawn(owner::Inputs {
            dev_id,
            cdev,
            image,
            capture,
            waker,
            queue_depth,
            horizon,
            policy,
        })?;

        Ok((
            Self {
                control: control.clone(),
                commands: Some(commands),
                dev_id,
            },
            captured,
        ))
    }

    pub fn dev_id(&self) -> u32 {
        self.dev_id
    }

    /// Path of the block device to format and mount.
    pub fn block_path(&self) -> std::path::PathBuf {
        ublk::block_path(self.dev_id)
    }

    /// Stop admitting mutations, and return once the image holds every mutation
    /// which was admitted.
    ///
    /// This is the point-in-time cut of a prepare. A mutation is captured
    /// before it is applied, so each one falls entirely before or after the cut.
    /// Reads continue. A mutation which arrives while admission is closed waits
    /// for [`Device::resume_admission`] rather than failing.
    pub async fn close_admission(&self) -> anyhow::Result<()> {
        let commands = self.commands()?;
        let (quiet, quieted) = tokio::sync::oneshot::channel();
        () = commands.send(Command::CloseAdmission(quiet))?;

        quieted.await.map_err(|_| commands.stopped())
    }

    pub fn resume_admission(&self) -> anyhow::Result<()> {
        self.commands()?.send(Command::ResumeAdmission)
    }

    /// A handle with which the journal writer opens and completes this disk's
    /// recovery horizons.
    pub fn compactor(&self) -> anyhow::Result<Compactor> {
        Ok(Compactor(self.commands()?.clone()))
    }

    /// Tear the device down and take back the image. This is idempotent. `Drop`
    /// runs it if the caller has not, so no device node is left behind.
    pub fn stop(&mut self) -> anyhow::Result<Option<Image>> {
        let Some(commands) = self.commands.take() else {
            return Ok(None);
        };
        // Stopping aborts the queue's fetches, which is how the owner learns to
        // quiesce. Deleting then waits for every reference to the device, so the
        // owner must have closed the character device first, which it does before
        // it hands back the image.
        () = self.control.stop_dev(self.dev_id)?;

        let (reply, replied) = std::sync::mpsc::channel();
        () = commands.send(Command::Release(reply))?;

        let image = replied.recv().map_err(|_| {
            anyhow::anyhow!("device {} was torn down without its image", self.dev_id)
        })?;
        () = self.control.del_dev(self.dev_id)?;

        Ok(Some(image))
    }

    fn commands(&self) -> anyhow::Result<&Commands> {
        self.commands
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("device {} is stopped", self.dev_id))
    }
}

impl Drop for Device {
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

/// Opens and completes one disk's recovery horizon on the journal writer's
/// behalf.
///
/// The owner does the work, because a horizon is over the disk's own bitmaps and
/// image and nothing else may touch those. Only the writer knows the journal
/// range a horizon is judged against.
#[derive(Clone)]
pub struct Compactor(Commands);

impl Compactor {
    /// Open a horizon over the disk's allocated blocks, if a journal `range` of
    /// that many bytes above the floor warrants one. Report what that horizon
    /// must discharge.
    ///
    /// The owner judges the policy rather than the caller, because only the owner
    /// knows the disk's live allocated size.
    pub async fn open(&self, range: u64) -> anyhow::Result<Option<u32>> {
        let (reply, replied) = tokio::sync::oneshot::channel();
        () = self.0.send(Command::OpenHorizon(range, reply))?;

        replied.await.map_err(|_| self.0.stopped())
    }

    /// Blocks which still owe the open horizon a copy.
    ///
    /// The caller must have cut the disk's admission. A horizon which mutations
    /// after the cut completed belongs to the next delta.
    pub async fn pending(&self) -> anyhow::Result<u32> {
        let (reply, replied) = tokio::sync::oneshot::channel();
        () = self.0.send(Command::HorizonPending(reply))?;

        replied.await.map_err(|_| self.0.stopped())
    }

    /// Drop the horizon a commit has completed, and the bitmap with it.
    pub fn close(&self) -> anyhow::Result<()> {
        self.0.send(Command::CloseHorizon)
    }
}

/// Sends a command to a disk's owner, and wakes it to be read.
#[derive(Clone)]
struct Commands {
    dev_id: u32,
    sender: std::sync::mpsc::Sender<Command>,
    waker: Waker,
}

enum Command {
    CloseAdmission(tokio::sync::oneshot::Sender<()>),
    ResumeAdmission,
    OpenHorizon(u64, tokio::sync::oneshot::Sender<Option<u32>>),
    HorizonPending(tokio::sync::oneshot::Sender<u32>),
    CloseHorizon,
    Release(std::sync::mpsc::Sender<Image>),
}

impl Commands {
    fn stopped(&self) -> anyhow::Error {
        anyhow::anyhow!("device {} stopped being served", self.dev_id)
    }

    fn send(&self, command: Command) -> anyhow::Result<()> {
        self.sender.send(command).map_err(|_| self.stopped())?;
        self.waker.wake();

        Ok(())
    }
}

struct Owner {
    dev_id: u32,
    ring: io_uring::IoUring,
    waker: Waker,
    wake_buf: Box<[u8; 8]>,
    cdev: std::fs::File,
    descs: ublk::IoDescs,
    image: Image,
    capture: Capture,
    /// The recovery horizon this disk is discharging, and the blocks which still
    /// owe it a copy. A horizon is compaction state over the image rather than
    /// part of it, so the owner holds it beside the image.
    horizon: Option<Horizon>,
    policy: Policy,
    inflight: InFlight,
    slots: Vec<Slot>,
    backlog: Backlog,
    /// Completions of one pass. They are all taken before any is handled, because
    /// handling one submits more.
    reaped: Vec<(u64, i32)>,
    /// Ring operations outstanding. Buffers and descriptors stay alive until this
    /// reaches zero.
    pending: usize,
    /// Tags whose chunks the capture channel refused, in arrival order.
    parked: std::collections::VecDeque<u16>,
    /// Whether mutations may be captured. The cut of a prepare closes this.
    admitting: bool,
    /// Mutations captured but not yet applied to the image. The cut is reached
    /// once this is zero.
    admitted: usize,
    /// Answered once admission is closed and nothing is admitted.
    quiet: Option<tokio::sync::oneshot::Sender<()>>,
    /// The kernel has aborted the queue, so fetches are not re-armed.
    stopping: bool,
    /// Set once the disk is to be released, and replied to when it is quiet.
    release: Option<std::sync::mpsc::Sender<Image>>,
}
