//! The thread which serves one disk, and how it takes the commands its [`Device`]
//! and [`Compactor`] send it.
//!
//! The thread is an I/O flusher, per `prctl(PR_SET_IO_FLUSHER)`, because it is the
//! one thread which cleans its disk's dirty pages, and it writes the host's page
//! cache to do it. Dirty-page throttling therefore judges its writes against the
//! host device it writes to, rather than against the host-wide dirty limit. Without
//! that, one disk whose writeback has stalled, as behind a slow journal, fills the
//! host's dirty budget with pages only its owner can clean, and every other owner is
//! throttled for them. Its memory allocations also never wait on I/O, which may be
//! I/O to the very disk it serves.
//!
//! [`Device`]: super::Device
//! [`Compactor`]: super::Compactor

use super::{Command, Commands, Owner};
use crate::capture::Capture;
use crate::horizon::{Horizon, Policy};
use crate::image::Image;
use crate::ublk;
use crate::wake::Waker;

/// `prctl` option which marks the calling thread an I/O flusher. `libc` defines it
/// only for Android.
const PR_SET_IO_FLUSHER: libc::c_int = 57;

/// Stack of an owner thread. Its frames are a reap and a submission. The platform
/// default would reserve far more address space than one uses, for every disk on
/// the host.
const STACK_BYTES: usize = 256 * 1024;

/// A disk for an owner to serve. Its device must already have its parameters
/// set, and must not be started until [`spawn`] returns.
pub(super) struct Inputs {
    pub dev_id: u32,
    pub cdev: std::fs::File,
    pub image: Image,
    pub capture: Capture,
    /// Interrupts this owner's wait on its ring. The caller supplies it, because
    /// `capture` is built around it. Taking a mutation is one of the two events
    /// which must wake an owner.
    pub waker: Waker,
    /// Requests the device may have outstanding.
    pub queue_depth: u16,
    /// A recovery horizon the replay of this disk left open, which the owner
    /// resumes rather than opening one of its own over whatever it now finds
    /// allocated.
    pub horizon: Option<Horizon>,
    pub policy: Policy,
}

/// Serve `inputs` from a thread of its own. This returns once every tag of the
/// queue has a fetch in flight. The caller then starts the device.
///
/// The thread returns the image once the device has stopped.
pub(super) fn spawn(
    inputs: Inputs,
) -> anyhow::Result<(Commands, std::thread::JoinHandle<anyhow::Result<Image>>)> {
    let (dev_id, waker) = (inputs.dev_id, inputs.waker.clone());
    let (commands, received) = std::sync::mpsc::channel();
    let (armed, is_armed) = std::sync::mpsc::channel();

    let owner = std::thread::Builder::new()
        .name(format!("disk-{dev_id}"))
        .stack_size(STACK_BYTES)
        .spawn(move || {
            () = io_flusher(dev_id)?;
            let mut owner = Owner::new(inputs)?;
            () = owner.arm()?;

            _ = armed.send(());
            run(owner, received)
        })?;

    // A thread which fails before its queue is fetching drops `armed` unsent, and
    // returns why.
    if let Err(std::sync::mpsc::RecvError) = is_armed.recv() {
        return Err(match owner.join() {
            Ok(Err(err)) => err,
            Ok(Ok(_image)) => panic!("the owner of device {dev_id} returned without serving it"),
            Err(_panic) => anyhow::anyhow!("the owner of device {dev_id} panicked"),
        });
    }
    let commands = Commands {
        dev_id,
        sender: commands,
        waker,
    };
    Ok((commands, owner))
}

/// Mark the calling thread, which is disk `dev_id`'s owner, an I/O flusher.
fn io_flusher(dev_id: u32) -> anyhow::Result<()> {
    // SAFETY: this `prctl` option reads no user memory.
    let rc = unsafe { libc::prctl(PR_SET_IO_FLUSHER, 1, 0, 0, 0) };

    if rc != 0 {
        anyhow::bail!(
            "marking the owner of device {dev_id} an I/O flusher, which requires \
             CAP_SYS_RESOURCE: {}",
            std::io::Error::last_os_error(),
        );
    }
    Ok(())
}

/// Serve the disk until its device has stopped, and return its image.
fn run(mut owner: Owner, commands: std::sync::mpsc::Receiver<Command>) -> anyhow::Result<Image> {
    // The kernel aborts every tag's fetch once the device has stopped, which is
    // only after every request has completed. That alone ends an owner.
    while owner.aborted < owner.slots.len() {
        owner.drain_commands(&commands);
        owner.compact();
        owner.flush();

        match owner.ring.submit_and_wait(1) {
            Ok(_) => (),
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err) => {
                let dev_id = owner.dev_id;
                return Err(anyhow::Error::new(err).context(format!("serving device {dev_id}")));
            }
        }
        owner.reap();
    }
    assert!(
        owner.parked.is_empty(),
        "device {} stopped with requests parked",
        owner.dev_id,
    );

    // Returning drops the character device and unmaps its descriptors, so the
    // kernel may delete the device. Dropping the capture channel with them tells
    // the journal writer the disk is gone.
    Ok(owner.image)
}

impl Owner {
    fn new(inputs: Inputs) -> anyhow::Result<Self> {
        let Inputs {
            dev_id,
            cdev,
            image,
            capture,
            waker,
            queue_depth,
            horizon,
            policy,
        } = inputs;

        Ok(Self {
            descs: ublk::IoDescs::map(&cdev, ublk::QUEUE_ID, queue_depth)?,
            dev_id,
            ring: io_uring::IoUring::new(super::ring::RING_ENTRIES)?,
            waker,
            wake_buf: Box::new([0; 8]),
            cdev,
            image,
            capture,
            horizon,
            policy,
            slots: (0..queue_depth).map(|_| super::Slot::Idle).collect(),
            backlog: super::Backlog::new(),
            reaped: Vec::new(),
            aborted: 0,
            parked: std::collections::VecDeque::new(),
            admitting: true,
            failed: None,
        })
    }

    /// Take every queued command.
    fn drain_commands(&mut self, commands: &std::sync::mpsc::Receiver<Command>) {
        loop {
            match commands.try_recv() {
                // Every mutation admitted before this is in the image already,
                // because each is applied as it is admitted. The cut is therefore
                // reached as soon as admission closes.
                Ok(Command::CloseAdmission(closed)) => match &self.failed {
                    // Admission stays open, because the teardown which follows
                    // this failure unmounts, and an unmount writes.
                    Some(failed) => {
                        let failed = anyhow::anyhow!("device {} failed: {failed:#}", self.dev_id);
                        _ = closed.send(Err(failed));
                    }
                    None => {
                        self.admitting = false;
                        _ = closed.send(Ok(()));

                        if let Some(horizon) = &mut self.horizon {
                            () = horizon.cut();
                        }
                    }
                },
                Ok(Command::ResumeAdmission) => {
                    self.admitting = true;
                    self.retry_parked();
                }
                Ok(Command::OpenHorizon(range, reply)) => {
                    let allocated =
                        self.image.allocated().count_ones() as u64 * crate::BLOCK_SIZE as u64;

                    // A horizon's bitmap is as large as the allocated bitmap, so it
                    // is held only while a horizon is open.
                    let opened = self.policy.opens(range, allocated).then(|| {
                        let horizon = Horizon::open(self.image.allocated());
                        let pending = horizon.pending();
                        self.horizon = Some(horizon);

                        pending
                    });

                    if let Some(pending) = opened {
                        tracing::info!(
                            dev_id = self.dev_id,
                            range,
                            allocated,
                            pending,
                            "opened a recovery horizon"
                        );
                    }
                    _ = reply.send(opened);
                }
                Ok(Command::HorizonPending(reply)) => {
                    _ = reply.send(self.horizon.as_ref().map_or(0, Horizon::pending))
                }
                Ok(Command::CloseHorizon) => self.horizon = None,
                // Every handle may be gone while the device is still stopping, or
                // while one which failed to stop still serves. The kernel ends an
                // owner, and its handles do not.
                Err(
                    std::sync::mpsc::TryRecvError::Empty
                    | std::sync::mpsc::TryRecvError::Disconnected,
                ) => return,
            }
        }
    }
}
