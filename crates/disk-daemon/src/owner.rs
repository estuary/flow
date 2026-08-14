//! One disk's owner: the thread which serves its device, the `io_uring` that
//! thread drives, and the state of every request in flight.
//!
//! A disk is owned by exactly one thread, which is the only mutator of its image
//! and bitmaps, so every decision about a block is serialized without a lock.
//!
//! The thread is not a style choice. `ublk` binds a device's queue to the thread
//! which arms its first fetch, and rejects every later command from any other
//! thread with `EINVAL`, so the ring cannot be driven by a task which moves
//! between runtime workers. The thread blocks in `submit_and_wait`, and a
//! [`Waker`] armed on the ring is what interrupts it when a command arrives or
//! capture capacity frees.
//!
//! An owner never blocks anywhere else. Image and character-device I/O is
//! submitted to the ring and reaped later, and a disk whose capture channel is
//! full parks only the requests which need it.

use crate::capture::Capture;
use crate::image::Image;
use crate::inflight::InFlight;
use crate::proto::Chunk;
use crate::ublk::{self, sys};
use crate::wake::Waker;

/// A disk holds at most one operation per queue tag, plus its wake, and the
/// backlog absorbs any overflow. This is generous against [`ublk::QUEUE_DEPTH`],
/// and it is per disk, so generosity is not free.
const RING_ENTRIES: u32 = 128;

/// Stack of an owner thread. Its frames are a reap and a submission, so the
/// platform default would reserve three orders of magnitude more address space
/// than one uses, times every disk on the host.
const STACK_BYTES: usize = 256 * 1024;

/// Kernel workers the shared pool may run, as `[bounded, unbounded]`.
///
/// The kernel derives its own from the CPU count and `RLIMIT_NPROC`, which makes
/// them differ between hosts of different sizes. These are fixed instead, and
/// they bound the whole process rather than one disk.
const IOWQ_MAX_WORKERS: [u32; 2] = [128, 128];

type Backlog = std::collections::VecDeque<io_uring::squeue::Entry>;

/// A disk for an owner to serve. Its device must already have its parameters
/// set, and must not be started until [`spawn`] returns.
pub struct Serve {
    pub dev_id: u32,
    pub cdev: std::fs::File,
    pub image: Image,
    pub capture: Capture,
    /// Interrupts this owner's wait on its ring. It is the caller's because
    /// `capture` is built around it, and taking a mutation is one of the two
    /// things which must wake an owner.
    pub waker: Waker,
    /// Requests the device may have outstanding, which is its concurrency.
    pub queue_depth: u16,
}

/// Cuts and ends one disk's service.
pub struct Handle(Commands);

/// Asks a disk to read its image back as chunks, and can do nothing else to it.
///
/// It is a handle of its own because the journal writer holds one for the length
/// of a session, while the session itself holds the [`Handle`].
#[derive(Clone)]
pub struct Snapshotter(Commands);

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
    Snapshot(tokio::sync::oneshot::Sender<std::io::Result<Vec<Vec<Chunk>>>>),
    Release(std::sync::mpsc::Sender<Image>),
}

/// Serve `serve` from a thread of its own, returning once every tag of its queue
/// has a fetch in flight. Starting the device is what the kernel waits for next.
pub fn spawn(serve: Serve) -> anyhow::Result<Handle> {
    let (dev_id, waker) = (serve.dev_id, serve.waker.clone());
    let (commands, received) = std::sync::mpsc::channel();
    let (armed, is_armed) = std::sync::mpsc::channel();

    // The ring is built and armed on the thread which will serve it, never
    // here: arming binds the queue to whichever thread issues the first fetch,
    // and every later command must come from that same thread.
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

    // The caller starts the device as soon as this returns, and the kernel
    // waits for every tag to be fetching before it does.
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
    /// Stop admitting mutations, and return once every mutation admitted has
    /// been applied to the image.
    ///
    /// This is the point-in-time cut of a publication: because a mutation is
    /// captured before it is applied, each one is entirely before or after the
    /// cut. Reads continue, and a mutation arriving while admission is closed
    /// waits for [`Handle::resume_admission`] rather than failing.
    pub async fn close_admission(&self) -> anyhow::Result<()> {
        let (quiet, quieted) = tokio::sync::oneshot::channel();
        () = self.0.send(Command::CloseAdmission(quiet))?;

        quieted
            .await
            .map_err(|_| anyhow::anyhow!("device {} stopped being served", self.0.dev_id))
    }

    pub fn resume_admission(&self) -> anyhow::Result<()> {
        self.0.send(Command::ResumeAdmission)
    }

    pub fn snapshotter(&self) -> Snapshotter {
        Snapshotter(self.0.clone())
    }

    /// Stop serving and take back the image, once the owner has closed the
    /// character device. The device must already be stopped, so that the kernel
    /// has aborted the queue's fetches.
    pub fn release(self) -> anyhow::Result<Image> {
        let (reply, replied) = std::sync::mpsc::channel();
        () = self.0.send(Command::Release(reply))?;

        replied.recv().map_err(|_| {
            anyhow::anyhow!("device {} was torn down without its image", self.0.dev_id)
        })
    }
}

impl Snapshotter {
    /// Read the disk's image back as the chunks which reproduce it, per
    /// [`Image::snapshot`].
    ///
    /// A mutation may be applied between this being asked for and the read, so
    /// the snapshot may already reflect one. That is why it is only ever
    /// appended ahead of every mutation captured since the mount: one already
    /// reflected in it is simply applied again.
    pub async fn snapshot(&self) -> anyhow::Result<Vec<Vec<Chunk>>> {
        let (reply, replied) = tokio::sync::oneshot::channel();
        () = self.0.send(Command::Snapshot(reply))?;

        replied
            .await
            .map_err(|_| anyhow::anyhow!("device {} stopped being served", self.0.dev_id))?
            .map_err(|err| anyhow::anyhow!("snapshotting device {}: {err}", self.0.dev_id))
    }
}

impl Commands {
    fn send(&self, command: Command) -> anyhow::Result<()> {
        self.sender
            .send(command)
            .map_err(|_| anyhow::anyhow!("device {} stopped being served", self.dev_id))?;
        self.waker.wake();

        Ok(())
    }
}

fn run(mut owner: Owner, commands: std::sync::mpsc::Receiver<Command>) {
    loop {
        // A disconnect means every handle is gone, so nothing is left to serve
        // this disk for and nothing will ask for its image.
        let Some(()) = owner.drain_commands(&commands) else {
            break;
        };
        if owner.release.is_some() && owner.pending == 0 {
            break;
        }
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

    // Dropping these closes the character device and unmaps its descriptors,
    // which is what lets the kernel delete the device. Dropping the capture
    // channel with them is what tells the journal writer the disk is gone.
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
    inflight: InFlight,
    slots: Vec<Slot>,
    backlog: Backlog,
    /// Completions of one pass, taken before any are handled because handling
    /// one submits more.
    reaped: Vec<(u64, i32)>,
    /// Ring operations outstanding. Buffers and descriptors stay alive until
    /// this reaches zero.
    pending: usize,
    /// Tags whose chunks the capture channel refused, in arrival order.
    parked: std::collections::VecDeque<u16>,
    /// Whether mutations may be captured, which a publication's cut closes.
    admitting: bool,
    /// Mutations captured but not yet applied to the image. The cut is reached
    /// when this is zero.
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
    /// Buffer being filled: image content for a read, incoming data for a
    /// write.
    buf: Vec<u8>,
    /// A write's data, once fetched. The chunks are slices of it and the image
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
    /// This must run on the thread which will serve the disk: `ublk` binds the
    /// queue to whoever issues its first fetch.
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
                }
                Ok(Command::ResumeAdmission) => {
                    self.admitting = true;
                    self.retry_parked();
                }
                Ok(Command::Snapshot(reply)) => {
                    // The owner is the only reader of its bitmap, so this runs
                    // here rather than on the asking task. It reads only the
                    // blocks a formatted filesystem allocated, which is the
                    // single-digit megabytes a `mkfs` writes.
                    let run_blocks = ublk::MAX_IO_BUF_BYTES / self.image.block_size();
                    _ = reply.send(self.image.snapshot(run_blocks));
                }
                Ok(Command::Release(reply)) => {
                    self.release = Some(reply);

                    // A request whose chunks the capture channel never accepted
                    // changed nothing, and the stopped device has already
                    // errored it. One whose chunks were accepted is still
                    // waiting on the image, and must apply.
                    for tag in std::mem::take(&mut self.parked) {
                        self.slots[tag as usize] = Slot::default();
                    }
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => return Some(()),
                Err(std::sync::mpsc::TryRecvError::Disconnected) => return None,
            }
        }
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

    /// Arm a read of the waker's eventfd, which is how a command or freed
    /// capture capacity interrupts this owner's wait on its ring.
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

            // A negative fetch is how an owner learns the kernel has aborted the
            // queue, which it does when the device stops.
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
                let block_size = self.image.block_size();

                match transferred(result, bytes) {
                    Err(err) => self.fail(tag, "taking write data from the device", err),
                    Ok(()) => {
                        let slot = &mut self.slots[tag as usize];
                        let data = bytes::Bytes::from(std::mem::take(&mut slot.buf));
                        let block = slot.range.start;
                        slot.data = data.clone();

                        let chunks = crate::chunk::encode_write(block, &data, block_size);
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
        let block_size = self.image.block_size() as u64;

        let offset = desc.start_sector * sys::SECTOR_SIZE;
        let bytes = sys::io_desc_sectors(&desc) as u64 * sys::SECTOR_SIZE;

        // The device's logical block size is the tracking block size, so the
        // block layer cannot issue a request which straddles a block.
        assert!(
            offset % block_size == 0 && bytes % block_size == 0,
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
            // Both deallocate, because an unallocated block reads as zeroes and
            // staying sparse is what keeps a rebuilt image small.
            sys::UBLK_IO_OP_DISCARD | sys::UBLK_IO_OP_WRITE_ZEROES => {
                let chunks = vec![crate::chunk::encode_punch(
                    range.start,
                    range.end - range.start,
                )];
                self.offer(tag, chunks);
            }
            // The device advertises no volatile write cache, so a flush should
            // never arrive and nothing else is a block request this serves.
            op => {
                tracing::warn!(dev_id = self.dev_id, op, "unsupported device request");
                self.complete(tag, -libc::EOPNOTSUPP);
            }
        }
    }

    /// Hand `chunks` to the capture channel. A mutation is captured before it is
    /// applied, so that journal order is application order, and it waits here
    /// when the channel is full rather than being dropped or refused.
    ///
    /// A closed admission parks a mutation exactly as a full channel does, which
    /// is what places it after the cut.
    fn offer(&mut self, tag: u16, chunks: Vec<Chunk>) {
        let offered = match self.admitting {
            true => self.capture.offer(chunks),
            false => Err(chunks),
        };

        match offered {
            Ok(()) => {
                self.admitted += 1;
                self.begin_mutation(tag);
            }
            Err(chunks) => {
                self.slots[tag as usize].chunks = chunks;
                self.parked.push_back(tag);
            }
        }
    }

    /// Re-offer the chunks of every request parked on capture capacity or on a
    /// closed admission. Order is preserved, so neither backpressure nor a cut
    /// reorders two mutations.
    fn retry_parked(&mut self) {
        while self.admitting {
            let Some(&tag) = self.parked.front() else {
                return;
            };
            let chunks = std::mem::take(&mut self.slots[tag as usize].chunks);

            match self.capture.offer(chunks) {
                Ok(()) => {
                    self.parked.pop_front();
                    self.admitted += 1;
                    self.begin_mutation(tag);
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
            let bytes = (slot.range.end - slot.range.start) as u64 * self.image.block_size() as u64;

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
            // An image write which fails, which in practice means ENOSPC, errors
            // only its own request. Ext4's default `errors=remount-ro` then
            // contains the failure to this one disk.
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
    /// bytes the request transferred, or a negative errno. A read which reports
    /// zero bytes is an I/O error to the kernel.
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

    /// Hand a read's image content to the character device, which is how request
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
/// `RLIMIT_NPROC`, so a host serving many disks could back them with thousands
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
/// pool is, and it outlives every ring attached to it by never being dropped.
fn workers() -> anyhow::Result<&'static io_uring::IoUring> {
    static WORKERS: std::sync::OnceLock<io_uring::IoUring> = std::sync::OnceLock::new();

    if let Some(workers) = WORKERS.get() {
        return Ok(workers);
    }
    // Two disks starting at once may each build one of these and discard the
    // loser, which costs a descriptor until the process ends, never an answer.
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

    /// Every disk's ring attaches to the one worker pool, and each is built on
    /// the thread which will serve it rather than on the one which anchored.
    #[test]
    fn test_rings_attach_to_the_shared_worker_pool() {
        for _ in 0..2 {
            if let Err(err) = std::thread::spawn(super::ring).join().unwrap() {
                panic!("a disk's ring could not attach to the shared pool: {err}");
            }
        }
    }
}
