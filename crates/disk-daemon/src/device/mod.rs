use crate::horizon;
use crate::image;
use crate::ublk;
use anyhow::Context;

mod owner;
mod queue;
mod recording;
mod target;
mod wake;

pub use recording::{Mutation, Recorded};

/// A `ublk` device over one disk's image, and the owner thread which serves it.
pub struct Device {
    /// Shared kernel device control handle.
    control: std::sync::Arc<ublk::Control>,
    /// Commands to the owner thread.
    commands: std::sync::mpsc::Sender<Command>,
    /// Wakes the owner from its ring wait to read `commands`.
    waker: wake::Waker,
    /// The owner's thread, which returns once the device has stopped. Taken by the
    /// first teardown, so `stop` and `drop` cannot both run it.
    thread: Option<std::thread::JoinHandle<anyhow::Result<()>>>,
    /// Kernel device ID.
    dev_id: u32,
}

impl Device {
    /// Serve `image`, returning the writer's mutation stream. `horizon` resumes
    /// any horizon left open by replay.
    pub fn create(
        control: &std::sync::Arc<ublk::Control>,
        image: image::Image,
        queue_depth: u16,
        horizon: Option<horizon::Horizon>,
        policy: horizon::Policy,
    ) -> anyhow::Result<(Self, Recorded)> {
        let (device, recorded) = Self::add(control, image, queue_depth, horizon, policy)?;

        // A failure here tears the device down by dropping `device`.
        () = control.start_dev(device.dev_id)?;

        Ok((device, recorded))
    }

    /// Kernel device ID, which names the disk's block device and mount.
    pub fn dev_id(&self) -> u32 {
        self.dev_id
    }

    /// Close admission at the prepare boundary, and return whether the delta it
    /// ends discharged the open horizon.
    pub async fn close_admission(&self) -> anyhow::Result<bool> {
        let (closed, is_closed) = tokio::sync::oneshot::channel();
        () = self.send(Command::CloseAdmission(closed))?;

        is_closed.await.map_err(|_| self.stopped())?
    }

    /// Reopen admission after the prepare boundary. `range` is the next delta's
    /// journal range above the floor, which decides whether it opens a horizon.
    pub fn resume_admission(&self, range: u64) -> anyhow::Result<()> {
        self.send(Command::ResumeAdmission(range))
    }

    /// Stop, join, and delete the device.
    pub fn stop(&mut self) -> anyhow::Result<()> {
        let Some(thread) = self.thread.take() else {
            return Ok(());
        };
        // On failure, return without joining. The owner serves until the kernel
        // stops the device, so a join would hang, and so would a delete, which
        // waits for the owner to close the character device.
        () = self.control.stop_dev(self.dev_id)?;

        let served = match thread.join() {
            Ok(served) => served,
            // `join` fails only where the thread panicked.
            Err(_panic) => Err(anyhow::anyhow!(
                "the owner of device {} panicked",
                self.dev_id
            )),
        };
        // Delete the device whatever the owner returned: it has exited, closing the
        // character device, so nothing holds the device any longer.
        let deleted = self.control.del_dev(self.dev_id);

        () = served?;
        deleted
    }

    /// Set up the device and owner without starting it.
    fn add(
        control: &std::sync::Arc<ublk::Control>,
        image: image::Image,
        queue_depth: u16,
        horizon: Option<horizon::Horizon>,
        policy: horizon::Policy,
    ) -> anyhow::Result<(Self, Recorded)> {
        let waker = wake::Waker::new()?;
        let (recorder, recorded) = recording::channel(queue_depth as usize, waker.clone().into());

        let info = control.add_dev(queue_depth, ublk::MAX_IO_BUF_BYTES)?;
        let dev_id = info.dev_id;

        // Delete the device if a step below fails to hand it to an owner. Until one
        // holds it, nothing else would.
        let delete = |err: anyhow::Error| match control.del_dev(dev_id) {
            Ok(()) => err,
            Err(deleted) => err.context(format!(
                "device {dev_id} could not be served, nor deleted ({deleted:#})"
            )),
        };
        // devtmpfs creates the node before the command which added the device completes.
        let cdev_path = ublk::char_path(dev_id);
        let cdev = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&cdev_path)
            .with_context(|| format!("opening {cdev_path:?}"))
            .map_err(delete)?;
        () = control
            .set_params(dev_id, &ublk::params(image.blocks()))
            .map_err(delete)?;

        let (commands, thread) = owner::spawn(owner::Inputs {
            dev_id,
            cdev,
            target: target::Target::new(dev_id, image, recorder, horizon, policy),
            waker: waker.clone(),
            queue_depth,
        })
        .map_err(delete)?;

        let device = Self {
            control: control.clone(),
            commands,
            waker,
            thread: Some(thread),
            dev_id,
        };
        Ok((device, recorded))
    }

    fn send(&self, command: Command) -> anyhow::Result<()> {
        self.commands.send(command).map_err(|_| self.stopped())?;
        self.waker.wake();

        Ok(())
    }

    fn stopped(&self) -> anyhow::Error {
        anyhow::anyhow!("device {} stopped being served", self.dev_id)
    }
}

impl Drop for Device {
    fn drop(&mut self) {
        if let Err(err) = self.stop() {
            tracing::error!(dev_id = self.dev_id, ?err, "failed to tear down a device");
        }
    }
}

enum Command {
    CloseAdmission(tokio::sync::oneshot::Sender<anyhow::Result<bool>>),
    ResumeAdmission(u64),
}
