//! One disk's owner. This is the thread which serves its device, the `io_uring`
//! that thread drives, and the state of every request in flight.
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
//! - `mod.rs` — the thread, the commands a tenure sends it, and the [`Owner`] state
//!   the other files act on. Every field lives here; each file adds the `impl Owner`
//!   block of its own concern.
//! - `ring.rs` — the `io_uring` itself: submission, reaping, and the encoding which
//!   names the request and step a completion belongs to.
//! - `request.rs` — the per-tag path of one device request, from the fetch which
//!   hands it over to the completion which hands it back.
//! - `admission.rs` — what may be captured now: the cut, the requests parked behind a
//!   full capture channel, and the horizon copies a delta's budget pays for.

use crate::capture::Capture;
use crate::horizon::{Horizon, Policy};
use crate::image::Image;
use crate::inflight::InFlight;
use crate::ublk;
use crate::wake::Waker;
use request::Slot;
use ring::{Backlog, ring};

mod admission;
mod request;
mod ring;

#[cfg(test)]
mod device_test;

/// Stack of an owner thread. Its frames are a reap and a submission. The platform
/// default would reserve far more address space than one uses, for every disk on
/// the host.
const STACK_BYTES: usize = 256 * 1024;

/// A disk for an owner to serve. Its device must already have its parameters
/// set, and must not be started until [`spawn`] returns.
pub struct Inputs {
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
    /// allocated. It is `Some` exactly when the writer's horizon offset is.
    pub horizon: Option<Horizon>,
    pub policy: Policy,
}

/// Cuts and ends one disk's service.
pub struct Handle(Commands);

/// Opens and completes one disk's recovery horizon on the journal writer's
/// behalf.
///
/// The owner does the work, because a horizon is over the disk's own bitmaps and
/// image and nothing else may touch those. Only the writer knows the journal
/// range a horizon is judged against.
#[derive(Clone)]
pub struct Compactor(Commands);

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

/// Serve `inputs` from a thread of its own. This returns once every tag of the
/// queue has a fetch in flight. The caller then starts the device.
pub fn spawn(inputs: Inputs) -> anyhow::Result<Handle> {
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

    Ok(Handle(Commands {
        dev_id,
        sender: commands,
        waker,
    }))
}

impl Handle {
    /// Stop admitting mutations, and return once the image holds every mutation
    /// which was admitted.
    ///
    /// This is the point-in-time cut of a prepare. A mutation is captured
    /// before it is applied, so each one falls entirely before or after the cut.
    /// Reads continue. A mutation which arrives while admission is closed waits
    /// for [`Handle::resume_admission`] rather than failing.
    pub async fn close_admission(&self) -> anyhow::Result<()> {
        let (quiet, quieted) = tokio::sync::oneshot::channel();
        () = self.0.send(Command::CloseAdmission(quiet))?;

        quieted.await.map_err(|_| self.0.stopped())
    }

    pub fn resume_admission(&self) -> anyhow::Result<()> {
        self.0.send(Command::ResumeAdmission)
    }

    pub fn compactor(&self) -> Compactor {
        Compactor(self.0.clone())
    }

    /// Stop serving and take back the image, once the owner has closed the
    /// character device. The device must already be stopped, so that the kernel
    /// has aborted the fetches of its queue.
    pub fn release(self) -> anyhow::Result<Image> {
        let (reply, replied) = std::sync::mpsc::channel();
        () = self.0.send(Command::Release(reply))?;

        replied.recv().map_err(|_| {
            anyhow::anyhow!("device {} was torn down without its image", self.0.dev_id)
        })
    }
}

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
            ring: ring()?,
            waker,
            wake_buf: Box::new([0; 8]),
            cdev,
            image,
            capture,
            horizon,
            policy,
            inflight: InFlight::default(),
            slots: (0..queue_depth).map(|_| Slot::Idle).collect(),
            backlog: Backlog::new(),
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
                        self.slots[tag as usize] = Slot::Idle;
                    }
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => return Some(()),
                Err(std::sync::mpsc::TryRecvError::Disconnected) => return None,
            }
        }
    }
}
