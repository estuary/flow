//! The thread which serves one disk, and how it takes the commands its [`Device`]
//! and [`Compactor`] send it.
//!
//! [`Device`]: super::Device
//! [`Compactor`]: super::Compactor

use super::{Command, Commands, Owner};
use crate::capture::Capture;
use crate::horizon::{Horizon, Policy};
use crate::image::Image;
use crate::ublk;
use crate::wake::Waker;

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
pub(super) fn spawn(inputs: Inputs) -> anyhow::Result<Commands> {
    let (dev_id, waker) = (inputs.dev_id, inputs.waker.clone());
    let (commands, received) = std::sync::mpsc::channel();
    let (armed, is_armed) = std::sync::mpsc::channel();

    _ = std::thread::Builder::new()
        .name(format!("disk-{dev_id}"))
        .stack_size(STACK_BYTES)
        .spawn(move || {
            match Owner::new(inputs).and_then(|mut owner| {
                owner.arm()?;
                anyhow::Ok(owner)
            }) {
                Ok(owner) => {
                    _ = armed.send(Ok(()));
                    run(owner, received)
                }
                Err(err) => _ = armed.send(Err(err)),
            }
        })?;

    () = is_armed
        .recv()
        .map_err(|_| anyhow::anyhow!("device {dev_id} stopped before it was served"))??;

    Ok(Commands {
        dev_id,
        sender: commands,
        waker,
    })
}

fn run(mut owner: Owner, commands: std::sync::mpsc::Receiver<Command>) {
    // A disconnect means every handle is gone. Nothing is left to serve this
    // disk for, and nothing will ask for its image.
    while let Some(()) = owner.drain_commands(&commands) {
        if owner.release.is_some() && owner.pending == 0 {
            break;
        }
        owner.compact();
        owner.flush();

        match owner.ring.submit_and_wait(1) {
            Ok(_) => (),
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err) => {
                tracing::error!(dev_id = owner.dev_id, ?err, "a disk's ring failed");
                break;
            }
        }
        owner.reap();
        owner.report_quiet();
    }

    let Owner {
        image,
        release,
        cdev,
        descs,
        ..
    } = owner;

    // Dropping these closes the character device and unmaps its descriptors, so
    // the kernel may delete the device. Dropping the capture channel with them
    // tells the journal writer the disk is gone.
    drop((descs, cdev));

    if let Some(reply) = release {
        _ = reply.send(image);
    }
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
            ring: super::ring::ring()?,
            waker,
            wake_buf: Box::new([0; 8]),
            cdev,
            image,
            capture,
            horizon,
            policy,
            inflight: super::InFlight::default(),
            slots: (0..queue_depth).map(|_| super::Slot::Idle).collect(),
            backlog: super::Backlog::new(),
            reaped: Vec::new(),
            pending: 0,
            parked: std::collections::VecDeque::new(),
            admitting: true,
            admitted: 0,
            quiet: None,
            stopping: false,
            release: None,
        })
    }

    /// Take every queued command, or `None` if every handle is gone.
    fn drain_commands(&mut self, commands: &std::sync::mpsc::Receiver<Command>) -> Option<()> {
        loop {
            match commands.try_recv() {
                Ok(Command::CloseAdmission(quiet)) => {
                    self.admitting = false;
                    self.quiet = Some(quiet);
                    self.report_quiet();

                    if let Some(horizon) = &mut self.horizon {
                        () = horizon.cut();
                    }
                }
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
                Ok(Command::Release(reply)) => {
                    self.release = Some(reply);

                    // A request whose chunks the capture channel never accepted
                    // changed nothing, and the stopped device has already
                    // errored it. A request whose chunks were accepted is still
                    // waiting on the image, and it must still apply.
                    for tag in std::mem::take(&mut self.parked) {
                        self.slots[tag as usize] = super::Slot::Idle;
                    }
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => return Some(()),
                Err(std::sync::mpsc::TryRecvError::Disconnected) => return None,
            }
        }
    }
}
