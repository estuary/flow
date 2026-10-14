use crate::horizon;
use crate::image;
use crate::ublk;

mod admission;
mod backend;
mod owner;
mod queue;
pub(crate) mod recording;
mod wake;

pub use recording::Recorded;

/// A `ublk` device over one disk's image, and the owner thread which serves it.
pub struct Device {
    /// Shared kernel device control handle.
    control: std::sync::Arc<ublk::Control>,
    /// Commands sent to the owner thread.
    commands: Commands,
    /// The owner's thread, which returns once the device has stopped. Taken by the
    /// first teardown, so `stop` and `drop` cannot both run it.
    thread: Option<std::thread::JoinHandle<anyhow::Result<()>>>,
    /// Kernel device ID.
    dev_id: u32,
}

impl Device {
    /// Serve `image`, returning the writer's mutation stream and compactor.
    /// `horizon` resumes any horizon left open by replay.
    ///
    /// Parameters must be set before starting the device. Start then blocks until
    /// the owner has armed the queue's fetches.
    pub fn create(
        control: &std::sync::Arc<ublk::Control>,
        image: image::Image,
        queue_depth: u16,
        horizon: Option<horizon::Horizon>,
        policy: horizon::Policy,
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

    /// Close admission at the prepare boundary. Each admitted mutation is already
    /// applied to the image; later mutations wait for [`Device::resume_admission`].
    /// Reads continue.
    ///
    /// An earlier mutation or horizon-copy failure refuses the cut and leaves
    /// admission open so teardown can unmount, which requires writes.
    pub async fn close_admission(&self) -> anyhow::Result<()> {
        let (closed, is_closed) = tokio::sync::oneshot::channel();
        () = self.commands.send(Command::CloseAdmission(closed))?;

        is_closed.await.map_err(|_| self.commands.stopped())?
    }

    pub fn resume_admission(&self) -> anyhow::Result<()> {
        self.commands.send(Command::ResumeAdmission)
    }

    /// Stop, join, and delete the device. Idempotent; `Drop` also calls this.
    ///
    /// The recording channel must keep draining: stop waits for all requests,
    /// including parked mutations. The kernel then aborts fetches, ending the
    /// owner and releasing its character device so deletion can finish.
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

    /// Set up the device and owner without starting it. Failures before ownership
    /// passes to the owner delete the device here.
    fn serve(
        control: &std::sync::Arc<ublk::Control>,
        image: image::Image,
        queue_depth: u16,
        horizon: Option<horizon::Horizon>,
        policy: horizon::Policy,
    ) -> anyhow::Result<(Self, Recorded, Compactor)> {
        // One waker serves both directions. The channel wakes the owner when a
        // mutation it parked may be retried, and a command wakes it to be read.
        let waker = wake::Waker::new()?;
        let (recorder, recorded) = recording::channel(queue_depth as usize, waker.clone().into());
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

/// The journal writer's handle for horizon work on the owner thread.
///
/// Only the owner can access the image and bitmaps; only the writer knows the
/// journal range. This handle exposes neither admission cuts nor teardown.
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
    waker: wake::Waker,
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

/// Wait for `devtmpfs` to create `/dev/ublkcN`. The daemon must have permission
/// to open the node; this does not change its ownership.
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
