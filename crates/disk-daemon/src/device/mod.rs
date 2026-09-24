//! One disk's device: a `ublk` device over a sparse image, and the thread which
//! owns that device's queue.
//!
//! [`Device`] is the device's life, in the order the kernel forces: add, set
//! parameters, hand the queue to an owner, start — then stop, join the owner,
//! delete. A tenure cuts the disk through it, and its journal writer compacts the
//! disk through a [`Compactor`]. Both are commands to the owner.
//!
//! Exactly one thread owns a disk, and only that thread mutates its image and
//! bitmaps. Every decision about a block is therefore serialized without a lock.
//!
//! It is a thread rather than a task because `ublk` binds a device's queue to the
//! thread which arms its first fetch. It rejects every later command from any
//! other thread with `EINVAL`. The thread waits in `submit_and_wait`. A
//! [`Waker`] armed on the ring interrupts it when a command arrives or the
//! recording channel frees room.
//!
//! The owner submits the queue's fetches and commits to the ring and collects their
//! completions later. Everything in between, from the copies through the character
//! device to the image's I/O, happens directly on its thread, so a call the host
//! filesystem makes wait also waits every other request of the disk. A disk whose recording channel is full parks
//! only the requests which need that channel.
//!
//! The files of this directory are:
//!
//! - `mod.rs` — [`Device`], and the commands it and a [`Compactor`] send.
//! - `owner.rs` — the thread itself and the `Owner` it runs: the wait on the ring,
//!   the commands, and each request's trip from the queue to the backend and back.
//!   It decides nothing.
//! - `backend.rs` — `Backend`, the image and the admission in front of it: answering
//!   each request, applying each mutation admission lets through, reading horizon
//!   copies, and what each command does. It knows nothing of the ring or the
//!   character device, so its cases need no device.
//! - `queue.rs` — `Queue`, the `ublk` queue over its `io_uring`: fetches, commits,
//!   the wake, and every copy through the character device, so that it hands over a
//!   `Request` and completes a `Reply` as plain data. It knows nothing of the image
//!   or the recording channel.
//! - `admission.rs` — `Admission`, the gate in front of the recording channel, and the
//!   open recovery horizon its recorded mutations discharge: the cut, the mutations parked in
//!   arrival order until the channel has room, the failure which refuses every later
//!   cut, the copy budget a delta earns, and the runs a copy takes. A `Mutation` is a
//!   request's `Change` and the chunks it is recorded as. It knows nothing of the
//!   ring or the image.

use crate::horizon::{Horizon, Policy};
use crate::image::Image;
use crate::recording::Recorded;
use crate::ublk::{self, Control};
use crate::wake::Waker;

mod admission;
mod backend;
mod owner;
mod queue;

/// A `ublk` device over one disk's image, and the owner thread which serves it.
pub struct Device {
    control: std::sync::Arc<Control>,
    commands: Commands,
    /// The owner's thread, which returns once the device has stopped. Taken by the
    /// first teardown, so `stop` and `drop` cannot both run it.
    thread: Option<std::thread::JoinHandle<anyhow::Result<()>>>,
    dev_id: u32,
}

impl Device {
    /// Create a device over `image` and serve it. The returned [`Recorded`] and
    /// [`Compactor`] are the journal writer's: the consumer half of the disk's
    /// recording channel, and the handle through which it compacts the disk.
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
    ) -> anyhow::Result<(Self, Recorded, Compactor)> {
        let (disk, recorded, compactor) =
            Self::serve(control, image, queue_depth, horizon, policy)?;

        // A failure here tears the device down by dropping `disk`.
        () = control.start_dev(disk.dev_id)?;

        Ok((disk, recorded, compactor))
    }

    pub fn dev_id(&self) -> u32 {
        self.dev_id
    }

    /// Stop admitting mutations, and return once the image holds every mutation
    /// which was admitted.
    ///
    /// This is the point-in-time cut of a prepare. The owner records and applies
    /// a mutation in one step, so each one falls entirely before or after the cut.
    /// Reads continue. A mutation which arrives while admission is closed waits
    /// for [`Device::resume_admission`] rather than failing.
    ///
    /// This fails, and admission stays open, once the disk has failed to apply a
    /// mutation or to copy a horizon run. The delta then open holds what the image
    /// lacks, so it must never commit, and the teardown which follows unmounts,
    /// which writes.
    pub async fn close_admission(&self) -> anyhow::Result<()> {
        let (closed, is_closed) = tokio::sync::oneshot::channel();
        () = self.commands.send(Command::CloseAdmission(closed))?;

        is_closed.await.map_err(|_| self.commands.stopped())?
    }

    pub fn resume_admission(&self) -> anyhow::Result<()> {
        self.commands.send(Command::ResumeAdmission)
    }

    /// Tear the device down. This is idempotent. `Drop` runs it if the caller has
    /// not, so no device node is left behind.
    ///
    /// Stopping waits for every request the device has in flight, parked ones
    /// included, so it returns only while something takes from the recording
    /// channel. The kernel then aborts the queue's fetches, which ends the owner.
    /// Deleting waits for every reference to the device, which the owner's
    /// character device held until it exited.
    pub fn stop(&mut self) -> anyhow::Result<()> {
        let Some(thread) = self.thread.take() else {
            return Ok(());
        };
        // A device which did not stop keeps its owner serving, so neither a join
        // nor a delete could return.
        () = self.control.stop_dev(self.dev_id)?;

        let served = match thread.join() {
            Ok(served) => served,
            Err(_panic) => Err(anyhow::anyhow!(
                "the owner of device {} panicked",
                self.dev_id
            )),
        };
        // The owner is gone whatever it returned, and its character device with
        // it, so the device is deleted either way.
        let deleted = self.control.del_dev(self.dev_id);

        () = served?;
        deleted
    }

    /// Everything [`Device::create`] does short of starting the device: add it,
    /// set its parameters, and hand its queue to an owner. A device which fails
    /// before an owner takes it is deleted here.
    fn serve(
        control: &std::sync::Arc<Control>,
        image: Image,
        queue_depth: u16,
        horizon: Option<Horizon>,
        policy: Policy,
    ) -> anyhow::Result<(Self, Recorded, Compactor)> {
        // One waker serves both directions. The channel wakes the owner when a
        // mutation it parked may be retried, and a command wakes it to be read.
        let waker = Waker::new()?;
        let (recorder, recorded) =
            crate::recording::channel(queue_depth as usize, waker.clone().into());
        let admission = admission::Admission::new(recorder, horizon, policy);

        let info = control.add_dev(queue_depth, ublk::MAX_IO_BUF_BYTES)?;
        let dev_id = info.dev_id;

        // Until an owner has taken the device, nothing else will delete it.
        let delete = |err: anyhow::Error| match control.del_dev(dev_id) {
            Ok(()) => err,
            Err(deleted) => err.context(format!(
                "device {dev_id} could not be served, nor deleted ({deleted:#})"
            )),
        };
        let cdev = open_char_device(dev_id).map_err(delete)?;
        () = control
            .set_params(dev_id, &ublk::params(image.blocks()))
            .map_err(delete)?;

        let (commands, thread) = owner::spawn(owner::Inputs {
            dev_id,
            cdev,
            backend: backend::Backend::new(dev_id, admission, image),
            waker,
            queue_depth,
        })
        .map_err(delete)?;

        let compactor = Compactor(commands.clone());
        let disk = Self {
            control: control.clone(),
            commands,
            thread: Some(thread),
            dev_id,
        };
        Ok((disk, recorded, compactor))
    }
}

impl Drop for Device {
    fn drop(&mut self) {
        if let Err(err) = self.stop() {
            tracing::error!(dev_id = self.dev_id, ?err, "failed to tear down a device");
        }
    }
}

/// Opens and completes one disk's recovery horizon on the journal writer's
/// behalf.
///
/// The owner does the work, because a horizon is over the disk's own bitmaps and
/// image and nothing else may touch those. Only the writer knows the journal
/// range a horizon is judged against.
///
/// [`Device::create`] hands out a disk's only one, beside the recording channel the
/// writer also takes. It carries the horizon commands alone, so the writer can
/// neither cut the disk nor tear it down.
pub struct Compactor(Commands);

impl Compactor {
    /// Open a horizon over the disk's allocated blocks, if a journal `range` of
    /// that many bytes above the floor warrants one. Report whether it did.
    ///
    /// The owner judges the policy rather than the caller, because only the owner
    /// knows the disk's live allocated size.
    pub async fn open(&self, range: u64) -> anyhow::Result<bool> {
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

enum Command {
    CloseAdmission(tokio::sync::oneshot::Sender<anyhow::Result<()>>),
    ResumeAdmission,
    OpenHorizon(u64, tokio::sync::oneshot::Sender<bool>),
    HorizonPending(tokio::sync::oneshot::Sender<u32>),
    CloseHorizon,
}

/// Sends a command to a disk's owner, and wakes it to be read.
#[derive(Clone)]
struct Commands {
    dev_id: u32,
    sender: std::sync::mpsc::Sender<Command>,
    waker: Waker,
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
