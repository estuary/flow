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

use crate::capture::Capture;
use crate::horizon::Policy;
use crate::image::Image;
use crate::inflight::InFlight;
use crate::proto::Chunk;
use crate::ublk::{self, sys};
use crate::wake::Waker;

/// A disk holds at most one operation per queue tag, plus its wake. The backlog
/// absorbs any overflow. Each disk has a ring of its own, so this leaves headroom
/// over [`ublk::QUEUE_DEPTH`] without being lavish about it.
const RING_ENTRIES: u32 = 128;

/// Stack of an owner thread. Its frames are a reap and a submission. The platform
/// default would reserve far more address space than one uses, for every disk on
/// the host.
const STACK_BYTES: usize = 256 * 1024;

/// Kernel workers the shared pool may run, as `[bounded, unbounded]`.
///
/// The kernel derives its own values from the CPU count and `RLIMIT_NPROC`, so
/// they differ between hosts of different sizes. These values are fixed instead.
/// They bound the whole process rather than one disk.
const IOWQ_MAX_WORKERS: [u32; 2] = [128, 128];

/// Image bytes one snapshot batch reads back. A caller holds a batch until it is
/// appended, so this bounds what publishing a fresh disk's filesystem costs,
/// however large that filesystem is.
const SNAPSHOT_BATCH_BYTES: usize = 8 << 20;

type Backlog = std::collections::VecDeque<io_uring::squeue::Entry>;

/// A disk for an owner to serve. Its device must already have its parameters
/// set, and must not be started until [`spawn`] returns.
pub struct Serve {
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
    pub horizon: Policy,
    pub metrics: crate::metrics::Device,
}

/// Cuts and ends one disk's service.
pub struct Handle(Commands);

/// Asks a disk to read its image back as chunks, and can do nothing else to it.
///
/// It is a handle of its own because the journal writer holds one for the length
/// of a session, while the session itself holds the [`Handle`].
#[derive(Clone)]
pub struct Snapshotter(Commands);

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

type Batch = (Vec<Vec<Chunk>>, Option<u32>);

enum Command {
    CloseAdmission(tokio::sync::oneshot::Sender<()>),
    ResumeAdmission,
    Snapshot(u32, tokio::sync::oneshot::Sender<std::io::Result<Batch>>),
    OpenHorizon(u64, tokio::sync::oneshot::Sender<Option<u32>>),
    HorizonPending(tokio::sync::oneshot::Sender<u32>),
    CloseHorizon,
    Release(std::sync::mpsc::Sender<Image>),
}

/// Serve `serve` from a thread of its own. This returns once every tag of the
/// queue has a fetch in flight. The caller then starts the device.
pub fn spawn(serve: Serve) -> anyhow::Result<Handle> {
    let (dev_id, waker) = (serve.dev_id, serve.waker.clone());
    let (commands, received) = std::sync::mpsc::channel();
    let (armed, is_armed) = std::sync::mpsc::channel();

    _ = std::thread::Builder::new()
        .name(format!("disk-{dev_id}"))
        .stack_size(STACK_BYTES)
        .spawn(move || {
            match Owner::new(serve).and_then(|mut owner| {
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
    /// This is the point-in-time cut of a publication. A mutation is captured
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

    pub fn snapshotter(&self) -> Snapshotter {
        Snapshotter(self.0.clone())
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

impl Snapshotter {
    /// Read one batch of the disk's image back as the chunks which reproduce
    /// it, beginning at block `from`, per [`Image::snapshot`]. Also reports the
    /// block a following batch resumes at.
    ///
    /// A mutation may be applied between the request and the read, so the
    /// snapshot may already hold one. A caller therefore appends the snapshot
    /// ahead of every mutation captured since the mount. A mutation the snapshot
    /// already holds is simply applied again.
    pub async fn snapshot(&self, from: u32) -> anyhow::Result<Batch> {
        let (reply, replied) = tokio::sync::oneshot::channel();
        () = self.0.send(Command::Snapshot(from, reply))?;

        replied
            .await
            .map_err(|_| self.0.stopped())?
            .map_err(|err| anyhow::anyhow!("snapshotting device {}: {err}", self.0.dev_id))
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
    // What a recovered disk was rebuilt holding, before it serves anything.
    owner.report();

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
    policy: Policy,
    metrics: crate::metrics::Device,
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
    /// Whether mutations may be captured. A publication's cut closes this.
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

/// What an owner holds for one device request tag.
#[derive(Default)]
struct Slot {
    /// Blocks the request covers.
    range: std::ops::Range<u32>,
    /// Buffer being filled. It holds image content for a read, and incoming data
    /// for a write.
    buf: Vec<u8>,
    /// A write's data, once fetched. The chunks are slices of it, and the image
    /// write reads from it.
    data: bytes::Bytes,
    /// Chunks the capture channel has not accepted yet.
    chunks: Vec<Chunk>,
}

/// Which operation of a request a completion belongs to.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
enum Step {
    Wake = 0,
    Fetch = 1,
    ImageRead = 2,
    DeviceWrite = 3,
    DeviceRead = 4,
    ImageWrite = 5,
    ImagePunch = 6,
}

fn user_data(tag: u16, step: Step) -> u64 {
    (tag as u64) << 8 | step as u64
}

fn parse_user_data(user_data: u64) -> (u16, Step) {
    let step = match user_data as u8 {
        0 => Step::Wake,
        1 => Step::Fetch,
        2 => Step::ImageRead,
        3 => Step::DeviceWrite,
        4 => Step::DeviceRead,
        5 => Step::ImageWrite,
        6 => Step::ImagePunch,
        other => panic!("completion carries unknown step {other}"),
    };
    ((user_data >> 8) as u16, step)
}

impl Owner {
    fn new(serve: Serve) -> anyhow::Result<Self> {
        let Serve {
            dev_id,
            cdev,
            image,
            capture,
            waker,
            queue_depth,
            horizon,
            metrics,
        } = serve;

        Ok(Self {
            descs: ublk::IoDescs::map(&cdev, ublk::QUEUE_ID, queue_depth)?,
            dev_id,
            ring: ring()?,
            waker,
            wake_buf: Box::new([0; 8]),
            cdev,
            image,
            capture,
            policy: horizon,
            metrics,
            inflight: InFlight::default(),
            slots: (0..queue_depth).map(|_| Slot::default()).collect(),
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

    /// Arm the wake, and put a fetch in flight for every tag.
    ///
    /// This must run on the thread which will serve the disk. `ublk` binds the
    /// queue to whichever thread issues its first fetch.
    fn arm(&mut self) -> std::io::Result<()> {
        self.arm_wake();

        for tag in 0..self.slots.len() as u16 {
            let entry = self.io_command(tag, sys::UBLK_U_IO_FETCH_REQ, 0);
            self.submit(entry);
        }
        while !self.backlog.is_empty() {
            self.flush();
            self.ring.submit()?;
        }
        Ok(())
    }

    /// Take every queued command, or `None` if every handle is gone.
    fn drain_commands(&mut self, commands: &std::sync::mpsc::Receiver<Command>) -> Option<()> {
        loop {
            match commands.try_recv() {
                Ok(Command::CloseAdmission(quiet)) => {
                    self.admitting = false;
                    self.quiet = Some(quiet);
                    self.report_quiet();

                    if let Some(horizon) = self.image.horizon() {
                        horizon.cut();
                    }
                    // Counting bits scans the whole bitmap. The disk therefore
                    // reports its footprint at each cut, and not as each
                    // mutation lands.
                    self.report();
                }
                Ok(Command::ResumeAdmission) => {
                    self.admitting = true;
                    self.retry_parked();
                }
                Ok(Command::Snapshot(from, reply)) => {
                    // Only the owner reads its bitmap, so this runs here rather
                    // than on the task which asked.
                    let run_blocks = ublk::MAX_IO_BUF_BYTES / crate::BLOCK_SIZE;
                    _ = reply.send(self.image.snapshot(from, run_blocks, SNAPSHOT_BATCH_BYTES));
                }
                Ok(Command::OpenHorizon(range, reply)) => {
                    let allocated =
                        self.image.allocated().count_ones() as u64 * crate::BLOCK_SIZE as u64;

                    let opened = self
                        .policy
                        .opens(range, allocated)
                        .then(|| self.image.open_horizon());

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
                Ok(Command::HorizonPending(reply)) => _ = reply.send(self.image.horizon_pending()),
                Ok(Command::CloseHorizon) => {
                    self.image.close_horizon();
                    self.metrics.horizon_pending.set(0.0);
                }
                Ok(Command::Release(reply)) => {
                    self.release = Some(reply);

                    // A request whose chunks the capture channel never accepted
                    // changed nothing, and the stopped device has already
                    // errored it. A request whose chunks were accepted is still
                    // waiting on the image, and it must still apply.
                    for tag in std::mem::take(&mut self.parked) {
                        self.slots[tag as usize] = Slot::default();
                    }
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => return Some(()),
                Err(std::sync::mpsc::TryRecvError::Disconnected) => return None,
            }
        }
    }

    /// Report what the disk holds and what its horizon still owes. Both are
    /// counts over a bitmap.
    fn report(&mut self) {
        let allocated = self.image.allocated().count_ones() as u64;

        self.metrics
            .allocated
            .set(allocated * crate::BLOCK_SIZE as u64);
        self.metrics
            .horizon_pending
            .set(self.image.horizon_pending() as f64);
    }

    /// Answer the cut once admission is closed and everything admitted has been
    /// applied.
    fn report_quiet(&mut self) {
        if self.admitted != 0 {
            return;
        }
        if let Some(quiet) = self.quiet.take() {
            _ = quiet.send(());
        }
    }

    /// Move as many backlogged submissions into the ring as fit.
    fn flush(&mut self) {
        let Self { ring, backlog, .. } = self;
        let mut submission = ring.submission();

        while let Some(entry) = backlog.front() {
            // SAFETY: every buffer an entry addresses belongs to a slot or to
            // this owner, and neither is dropped while that entry is
            // outstanding.
            if unsafe { submission.push(entry) }.is_err() {
                break;
            }
            backlog.pop_front();
        }
    }

    /// Arm a read of the waker's eventfd. A command, or freed capture capacity,
    /// interrupts this owner's wait on its ring through that read.
    fn arm_wake(&mut self) {
        let entry = io_uring::opcode::Read::new(
            io_uring::types::Fd(self.waker.as_raw_fd()),
            self.wake_buf.as_mut_ptr(),
            self.wake_buf.len() as u32,
        )
        .build()
        .user_data(user_data(0, Step::Wake));

        self.backlog.push_back(entry);
    }

    fn reap(&mut self) {
        self.reaped.clear();
        self.reaped.extend(
            self.ring
                .completion()
                .map(|cqe| (cqe.user_data(), cqe.result())),
        );

        for index in 0..self.reaped.len() {
            let (user_data, result) = self.reaped[index];
            let (tag, step) = parse_user_data(user_data);

            if let Step::Wake = step {
                self.arm_wake();
                self.retry_parked();
                continue;
            }
            self.pending -= 1;
            self.advance(tag, step, result);
        }
    }

    fn submit(&mut self, entry: io_uring::squeue::Entry) {
        self.pending += 1;
        self.backlog.push_back(entry);
    }

    /// Advance the request at `tag`, whose `step` completed with `result`.
    fn advance(&mut self, tag: u16, step: Step, result: i32) {
        match step {
            Step::Wake => unreachable!("a wake is handled by the reap"),

            // A negative fetch tells the owner that the kernel has aborted the
            // queue. The kernel does that when the device stops.
            Step::Fetch if result < 0 => self.stopping = true,
            Step::Fetch => self.begin(tag),

            Step::ImageRead => {
                let bytes = self.slots[tag as usize].buf.len();

                match transferred(result, bytes) {
                    Err(err) => self.fail(tag, "reading the image", err),
                    Ok(()) => {
                        let entry = self.write_device(tag);
                        self.submit(entry);
                    }
                }
            }
            Step::DeviceWrite => {
                let bytes = self.slots[tag as usize].buf.len();

                match transferred(result, bytes) {
                    Err(err) => self.fail(tag, "handing read data to the device", err),
                    Ok(()) => self.complete(tag, bytes as i32),
                }
            }
            Step::DeviceRead => {
                let bytes = self.slots[tag as usize].buf.len();

                match transferred(result, bytes) {
                    Err(err) => self.fail(tag, "taking write data from the device", err),
                    Ok(()) => {
                        let slot = &mut self.slots[tag as usize];
                        let data = bytes::Bytes::from(std::mem::take(&mut slot.buf));
                        let block = slot.range.start;
                        slot.data = data.clone();

                        let chunks = crate::chunk::encode_write(block, &data);
                        self.offer(tag, chunks);
                    }
                }
            }
            Step::ImageWrite | Step::ImagePunch => self.finish_mutation(tag, step, result),
        }
    }

    /// Decode the request the kernel handed back at `tag` and take its first
    /// step.
    fn begin(&mut self, tag: u16) {
        let desc = self.descs.get(tag);
        let block_size = crate::BLOCK_SIZE as u64;

        let offset = desc.start_sector * sys::SECTOR_SIZE;
        let bytes = sys::io_desc_sectors(&desc) as u64 * sys::SECTOR_SIZE;

        // The device's logical block size is the tracking block size. The block
        // layer therefore cannot issue a request which straddles a block.
        assert!(
            offset.is_multiple_of(block_size) && bytes.is_multiple_of(block_size),
            "device request of {bytes} bytes at {offset} is not {block_size}-aligned",
        );
        let range = (offset / block_size) as u32..((offset + bytes) / block_size) as u32;
        assert!(
            range.end <= self.image.blocks(),
            "device request covers blocks {range:?}, beyond the device",
        );
        self.slots[tag as usize].range = range.clone();

        match sys::io_desc_op(&desc) {
            sys::UBLK_IO_OP_READ => {
                self.slots[tag as usize].buf = vec![0; bytes as usize];
                let entry = self.read_image(tag);
                self.submit(entry);
            }
            sys::UBLK_IO_OP_WRITE => {
                self.slots[tag as usize].buf = vec![0; bytes as usize];
                let entry = self.read_device(tag);
                self.submit(entry);
            }
            // Both deallocate, per `chunk::encode_punch`.
            sys::UBLK_IO_OP_DISCARD | sys::UBLK_IO_OP_WRITE_ZEROES => {
                let chunks = vec![crate::chunk::encode_punch(
                    range.start,
                    range.end - range.start,
                )];
                self.offer(tag, chunks);
            }
            // The device advertises no volatile write cache, so a flush should
            // never arrive. This daemon serves no other kind of block request.
            op => {
                tracing::warn!(dev_id = self.dev_id, op, "unsupported device request");
                self.complete(tag, -libc::EOPNOTSUPP);
            }
        }
    }

    /// Hand `chunks` to the capture channel. A mutation is captured before it is
    /// applied, so journal order is application order. A mutation waits here when
    /// the channel is full. It is never dropped or refused.
    ///
    /// A closed admission parks a mutation exactly as a full channel does, which
    /// places it after the cut.
    fn offer(&mut self, tag: u16, chunks: Vec<Chunk>) {
        let changed = crate::chunk::data_bytes(&chunks);

        let offered = match self.admitting {
            true => self.capture.offer(chunks),
            false => Err(chunks),
        };

        match offered {
            Ok(()) => self.admit(tag, changed),
            Err(chunks) => {
                self.slots[tag as usize].chunks = chunks;
                self.parked.push_back(tag);

                self.metrics.stalls.increment(1);
                self.metrics.parked.set(self.parked.len() as f64);
            }
        }
    }

    /// Take the mutation at `tag`, whose chunks the capture channel has
    /// accepted.
    ///
    /// A mutation publishes the blocks it covers, so it discharges them from any
    /// open horizon. The `changed` bytes it carries earn the budget a copy spends.
    fn admit(&mut self, tag: u16, changed: u64) {
        let range = self.slots[tag as usize].range.clone();

        if let Some(horizon) = self.image.horizon() {
            horizon.published(range);
            horizon.changed(changed);
        }
        self.admitted += 1;
        self.begin_mutation(tag);
    }

    /// Spend this delta's copy budget on the open horizon, interleaving
    /// compaction with the traffic paying for it.
    ///
    /// A copy is selected, read, and offered without yielding, so no mutation
    /// can land between its read and its offer.
    fn compact(&mut self) {
        if !self.admitting || self.image.horizon().is_none() {
            return;
        }
        let run_blocks = ublk::MAX_IO_BUF_BYTES / crate::BLOCK_SIZE;

        while self.capture.has_room() {
            let chunks = match self.image.copy_horizon(&self.policy, run_blocks) {
                Ok(Some(chunks)) => chunks,
                Ok(None) => return,
                Err(err) => {
                    tracing::error!(dev_id = self.dev_id, ?err, "failed to copy a horizon run");
                    return;
                }
            };
            let Ok(()) = self.capture.offer(chunks) else {
                unreachable!("the capture channel had room for a horizon copy")
            };
        }
    }

    /// Re-offer the chunks of every request parked on capture capacity or on a
    /// closed admission. This keeps arrival order, so neither backpressure nor a
    /// cut reorders two mutations.
    fn retry_parked(&mut self) {
        while self.admitting {
            let Some(&tag) = self.parked.front() else {
                return;
            };
            let chunks = std::mem::take(&mut self.slots[tag as usize].chunks);
            let changed = crate::chunk::data_bytes(&chunks);

            match self.capture.offer(chunks) {
                Ok(()) => {
                    self.parked.pop_front();
                    self.metrics.parked.set(self.parked.len() as f64);
                    self.admit(tag, changed);
                }
                Err(chunks) => {
                    self.slots[tag as usize].chunks = chunks;
                    return;
                }
            }
        }
    }

    fn begin_mutation(&mut self, tag: u16) {
        let range = self.slots[tag as usize].range.clone();

        if self.inflight.begin(tag, range) {
            self.mutate(tag);
        }
    }

    /// Submit the image write or punch of `tag`.
    fn mutate(&mut self, tag: u16) {
        let fd = io_uring::types::Fd(std::os::fd::AsRawFd::as_raw_fd(self.image.file()));
        let slot = &self.slots[tag as usize];
        let offset = self.image.offset(slot.range.start);

        // A punch is the request which carries no data.
        let entry = if slot.data.is_empty() {
            let bytes = (slot.range.end - slot.range.start) as u64 * crate::BLOCK_SIZE as u64;

            io_uring::opcode::Fallocate::new(fd, bytes)
                .offset(offset)
                .mode(crate::image::PUNCH_MODE)
                .build()
                .user_data(user_data(tag, Step::ImagePunch))
        } else {
            io_uring::opcode::Write::new(fd, slot.data.as_ptr(), slot.data.len() as u32)
                .offset(offset)
                .build()
                .user_data(user_data(tag, Step::ImageWrite))
        };
        self.submit(entry);
    }

    fn finish_mutation(&mut self, tag: u16, step: Step, result: i32) {
        let slot = &self.slots[tag as usize];
        let range = slot.range.clone();

        let expected = match step {
            Step::ImageWrite => slot.data.len(),
            _ => 0,
        };
        let outcome = match transferred(result, expected) {
            Ok(()) if step == Step::ImageWrite => {
                self.image.allocate(range);
                expected as i32
            }
            Ok(()) => {
                self.image.deallocate(range);
                0
            }
            // A failed image write, which in practice means ENOSPC, errors only
            // its own request. Ext4's default `errors=remount-ro` then contains
            // the failure to this one disk.
            Err(err) => {
                tracing::error!(dev_id = self.dev_id, ?err, "image mutation failed");
                -libc::EIO
            }
        };

        self.admitted -= 1;

        for released in self.inflight.end(tag) {
            self.mutate(released);
        }
        self.complete(tag, outcome);
    }

    /// Complete `tag` back to the kernel and re-arm its fetch. `result` is the
    /// bytes the request transferred, or a negative errno. The kernel reads a
    /// zero-byte read as an I/O error.
    fn complete(&mut self, tag: u16, result: i32) {
        self.slots[tag as usize] = Slot::default();

        // A stopped device has already errored every request it had outstanding.
        if self.stopping {
            return;
        }
        let entry = self.io_command(tag, sys::UBLK_U_IO_COMMIT_AND_FETCH_REQ, result);
        self.submit(entry);
    }

    fn fail(&mut self, tag: u16, what: &str, err: std::io::Error) {
        tracing::error!(dev_id = self.dev_id, tag, ?err, "{what} failed");
        self.complete(tag, -libc::EIO);
    }

    fn read_image(&mut self, tag: u16) -> io_uring::squeue::Entry {
        let fd = io_uring::types::Fd(std::os::fd::AsRawFd::as_raw_fd(self.image.file()));
        let offset = self.image.offset(self.slots[tag as usize].range.start);

        let slot = &mut self.slots[tag as usize];
        let (buf, len) = (slot.buf.as_mut_ptr(), slot.buf.len() as u32);

        io_uring::opcode::Read::new(fd, buf, len)
            .offset(offset)
            .build()
            .user_data(user_data(tag, Step::ImageRead))
    }

    /// Hand a read's image content to the character device. This is how request
    /// data moves under `UBLK_F_USER_COPY`.
    fn write_device(&mut self, tag: u16) -> io_uring::squeue::Entry {
        let fd = io_uring::types::Fd(std::os::fd::AsRawFd::as_raw_fd(&self.cdev));
        let offset = sys::io_buf_offset(ublk::QUEUE_ID, tag);

        let slot = &self.slots[tag as usize];
        let (buf, len) = (slot.buf.as_ptr(), slot.buf.len() as u32);

        io_uring::opcode::Write::new(fd, buf, len)
            .offset(offset)
            .build()
            .user_data(user_data(tag, Step::DeviceWrite))
    }

    /// Take a write's incoming data from the character device.
    fn read_device(&mut self, tag: u16) -> io_uring::squeue::Entry {
        let fd = io_uring::types::Fd(std::os::fd::AsRawFd::as_raw_fd(&self.cdev));
        let offset = sys::io_buf_offset(ublk::QUEUE_ID, tag);

        let slot = &mut self.slots[tag as usize];
        let (buf, len) = (slot.buf.as_mut_ptr(), slot.buf.len() as u32);

        io_uring::opcode::Read::new(fd, buf, len)
            .offset(offset)
            .build()
            .user_data(user_data(tag, Step::DeviceRead))
    }

    fn io_command(&self, tag: u16, cmd_op: u32, result: i32) -> io_uring::squeue::Entry {
        let command = sys::io_cmd(ublk::QUEUE_ID, tag, result);
        // SAFETY: `UblksrvIoCmd` is `repr(C)` and its 16 bytes are fully occupied
        // by its fields, so the copy reads no padding.
        let bytes = unsafe { sys::cmd_bytes::<_, 16>(&command) };

        io_uring::opcode::UringCmd16::new(
            io_uring::types::Fd(std::os::fd::AsRawFd::as_raw_fd(&self.cdev)),
            cmd_op,
        )
        .cmd(bytes)
        .build()
        .user_data(user_data(tag, Step::Fetch))
    }
}

/// Interpret an `io_uring` result which should have moved `expected` bytes.
fn transferred(result: i32, expected: usize) -> Result<(), std::io::Error> {
    if result < 0 {
        return Err(std::io::Error::from_raw_os_error(-result));
    }
    if result as usize != expected {
        return Err(std::io::Error::other(format!(
            "moved {result} of {expected} bytes",
        )));
    }
    Ok(())
}

/// One disk's ring, sharing the process-wide pool of kernel workers.
///
/// A ring which built its own pool would size it from the host's CPU count and
/// `RLIMIT_NPROC`. A host serving many disks could then back them with thousands
/// of worker threads. Punches reach those workers in ordinary operation, so this
/// is not a rare path.
fn ring() -> anyhow::Result<io_uring::IoUring> {
    let anchor = workers()?;

    io_uring::IoUring::builder()
        .setup_attach_wq(std::os::fd::AsRawFd::as_raw_fd(anchor))
        .build(RING_ENTRIES)
        .map_err(Into::into)
}

/// The ring whose worker pool every disk shares. It is process-wide because the
/// pool is. Nothing ever drops it, so it outlives every ring attached to it.
fn workers() -> anyhow::Result<&'static io_uring::IoUring> {
    static WORKERS: std::sync::OnceLock<io_uring::IoUring> = std::sync::OnceLock::new();

    if let Some(workers) = WORKERS.get() {
        return Ok(workers);
    }
    // Two disks which start at once may each build one of these and discard the
    // loser. That costs one descriptor until the process ends, never an answer.
    let anchor = io_uring::IoUring::new(RING_ENTRIES)?;
    let mut prior = IOWQ_MAX_WORKERS;
    () = anchor.submitter().register_iowq_max_workers(&mut prior)?;

    tracing::debug!(?prior, max = ?IOWQ_MAX_WORKERS, "sized the shared io_uring worker pool");

    Ok(WORKERS.get_or_init(|| anchor))
}

#[cfg(test)]
mod test {
    use super::{Step, parse_user_data, user_data};

    #[test]
    fn test_user_data_round_trips() {
        for (tag, step) in [
            (0, Step::Fetch),
            (31, Step::ImageWrite),
            (u16::MAX, Step::ImagePunch),
        ] {
            assert_eq!(parse_user_data(user_data(tag, step)), (tag, step));
        }
    }

    /// Every disk's ring attaches to the one worker pool. Each ring is built on
    /// the thread which will serve it, and not on the thread which anchored.
    #[test]
    fn test_rings_attach_to_the_shared_worker_pool() {
        for _ in 0..2 {
            if let Err(err) = std::thread::spawn(super::ring).join().unwrap() {
                panic!("a disk's ring could not attach to the shared pool: {err}");
            }
        }
    }
}
