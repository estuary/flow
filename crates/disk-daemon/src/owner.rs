//! Pooled owner threads, each serving many disks.
//!
//! A disk is owned by exactly one owner: the thread which reaps its ring's
//! completions, and the only mutator of its image and bitmaps. Every decision
//! about a block is therefore serialized without a lock.
//!
//! An owner never blocks. Image and character-device I/O is submitted to its
//! ring and reaped later, so a slow operation on one disk cannot stall another,
//! and a disk whose capture channel is full parks only its own requests.

use crate::capture::Capture;
use crate::image::Image;
use crate::inflight::InFlight;
use crate::proto::Chunk;
use crate::ublk::{self, sys};
use crate::wake::Waker;

/// Deep enough that the fetches, data transfers and image operations of many
/// disks fit without the backlog doing real work.
const RING_ENTRIES: u32 = 512;

type Backlog = std::collections::VecDeque<io_uring::squeue::Entry>;

/// A disk for an owner to serve. Its device must already have its parameters
/// set, and must not be started until [`Handle::serve`] returns.
pub struct Serve {
    pub dev_id: u32,
    pub cdev: std::fs::File,
    pub image: Image,
    pub capture: Capture,
}

/// A pool of owner threads.
///
/// Every disk must be released before the pool is dropped, since an owner
/// serves until its disks are gone.
pub struct Pool {
    handles: Vec<Handle>,
    threads: Vec<std::thread::JoinHandle<()>>,
    next: std::sync::atomic::AtomicUsize,
}

/// Adds and removes the disks of one owner.
#[derive(Clone)]
pub struct Handle {
    commands: std::sync::mpsc::Sender<Command>,
    waker: Waker,
}

impl Pool {
    pub fn new(owners: usize) -> anyhow::Result<Self> {
        assert!(owners != 0, "a pool has at least one owner");

        let mut handles = Vec::with_capacity(owners);
        let mut threads = Vec::with_capacity(owners);

        for index in 0..owners {
            let (handle, thread) = spawn(index)?;
            handles.push(handle);
            threads.push(thread);
        }
        Ok(Self {
            handles,
            threads,
            next: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    /// The owner of the next disk to be created. Ownership is per disk, so one
    /// owner comes to serve many.
    pub fn owner(&self) -> Handle {
        let index = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.handles[index % self.handles.len()].clone()
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        for handle in &self.handles {
            _ = handle.commands.send(Command::Shutdown);
            handle.waker.wake();
        }
        for thread in self.threads.drain(..) {
            _ = thread.join();
        }
    }
}

impl Handle {
    /// Begin serving `serve`, returning once every tag of its queue has a fetch
    /// in flight. Starting the device is what the kernel waits for next.
    pub fn serve(&self, serve: Serve) -> anyhow::Result<()> {
        let dev_id = serve.dev_id;
        self.request(|reply| Command::Serve(Box::new(serve), reply))?
            .map_err(|err| anyhow::anyhow!("owner failed to serve device {dev_id}: {err}"))
    }

    /// Stop serving `dev_id` and return its image, or `None` if this owner never
    /// served it. Its device must already be stopped, so that the kernel has
    /// aborted the queue's fetches.
    pub fn release(&self, dev_id: u32) -> anyhow::Result<Option<Image>> {
        self.request(|reply| Command::Release(dev_id, reply))
    }

    /// Wake target for the capture channels of this owner's disks.
    pub(crate) fn waker(&self) -> Waker {
        self.waker.clone()
    }

    fn request<T>(
        &self,
        command: impl FnOnce(std::sync::mpsc::Sender<T>) -> Command,
    ) -> anyhow::Result<T> {
        let (reply, replied) = std::sync::mpsc::channel();

        self.commands
            .send(command(reply))
            .map_err(|_| anyhow::anyhow!("owner thread has exited"))?;
        self.waker.wake();

        replied
            .recv()
            .map_err(|_| anyhow::anyhow!("owner thread exited without replying"))
    }
}

enum Command {
    Serve(Box<Serve>, std::sync::mpsc::Sender<std::io::Result<()>>),
    Release(u32, std::sync::mpsc::Sender<Option<Image>>),
    Shutdown,
}

fn spawn(index: usize) -> anyhow::Result<(Handle, std::thread::JoinHandle<()>)> {
    let waker = Waker::new()?;
    let (commands, received) = std::sync::mpsc::channel();

    let owner = Owner {
        ring: io_uring::IoUring::new(RING_ENTRIES)?,
        waker: waker.clone(),
        wake_buf: Box::new([0; 8]),
        commands: received,
        disks: std::collections::BTreeMap::new(),
        backlog: Backlog::new(),
        reaped: Vec::new(),
        shutdown: false,
    };
    let thread = std::thread::Builder::new()
        .name(format!("disk-owner-{index}"))
        .spawn(move || run(owner))?;

    Ok((Handle { commands, waker }, thread))
}

struct Owner {
    ring: io_uring::IoUring,
    waker: Waker,
    wake_buf: Box<[u8; 8]>,
    commands: std::sync::mpsc::Receiver<Command>,
    disks: std::collections::BTreeMap<u32, Disk>,
    backlog: Backlog,
    /// Completions of one pass, taken before any are handled because handling
    /// one submits more.
    reaped: Vec<(u64, i32)>,
    shutdown: bool,
}

struct Disk {
    dev_id: u32,
    cdev: std::fs::File,
    descs: ublk::IoDescs,
    image: Image,
    capture: Capture,
    inflight: InFlight,
    slots: Vec<Slot>,
    /// Ring operations this disk has outstanding. Its buffers and descriptors
    /// stay alive until this reaches zero.
    pending: usize,
    /// Tags whose chunks the capture channel refused, in arrival order.
    parked: std::collections::VecDeque<u16>,
    /// The kernel has aborted the queue, so fetches are not re-armed.
    stopping: bool,
    /// Set once the disk is to be released, and replied to when it is quiet.
    release: Option<std::sync::mpsc::Sender<Option<Image>>>,
}

/// What the owner holds for one device request tag.
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

fn user_data(dev_id: u32, tag: u16, step: Step) -> u64 {
    (dev_id as u64) << 24 | (tag as u64) << 8 | step as u64
}

fn parse_user_data(user_data: u64) -> (u32, u16, Step) {
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
    ((user_data >> 24) as u32, (user_data >> 8) as u16, step)
}

fn run(mut owner: Owner) {
    owner.arm_wake();

    loop {
        owner.drain_commands();

        if owner.shutdown && owner.disks.is_empty() {
            break;
        }
        owner.flush();

        match owner.ring.submit_and_wait(1) {
            Ok(_) => (),
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err) => {
                tracing::error!(?err, "owner ring failed, and its disks stop here");
                break;
            }
        }
        owner.reap();
        owner.release_quiet();
    }
}

impl Owner {
    fn drain_commands(&mut self) {
        loop {
            match self.commands.try_recv() {
                Ok(Command::Serve(serve, reply)) => {
                    let result = self.serve(*serve);
                    _ = reply.send(result);
                }
                Ok(Command::Release(dev_id, reply)) => {
                    self.begin_release(dev_id, reply);
                    self.release_quiet();
                }
                Ok(Command::Shutdown) => self.shutdown = true,
                Err(std::sync::mpsc::TryRecvError::Empty) => break,
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    self.shutdown = true;
                    break;
                }
            }
        }
    }

    fn serve(&mut self, serve: Serve) -> std::io::Result<()> {
        let Serve {
            dev_id,
            cdev,
            image,
            capture,
        } = serve;

        let mut disk = Disk {
            dev_id,
            descs: ublk::IoDescs::map(&cdev, ublk::QUEUE_ID, ublk::QUEUE_DEPTH)?,
            cdev,
            image,
            capture,
            inflight: InFlight::default(),
            slots: (0..ublk::QUEUE_DEPTH).map(|_| Slot::default()).collect(),
            pending: 0,
            parked: std::collections::VecDeque::new(),
            stopping: false,
            release: None,
        };
        for tag in 0..ublk::QUEUE_DEPTH {
            let entry = io_command(&disk, tag, sys::UBLK_U_IO_FETCH_REQ, 0);
            disk.submit(&mut self.backlog, entry);
        }
        self.disks.insert(dev_id, disk);

        // The device cannot start until the kernel has every fetch, and the
        // caller starts it as soon as this returns.
        while !self.backlog.is_empty() {
            self.flush();
            self.ring.submit()?;
        }
        Ok(())
    }

    fn begin_release(&mut self, dev_id: u32, reply: std::sync::mpsc::Sender<Option<Image>>) {
        let Some(disk) = self.disks.get_mut(&dev_id) else {
            // The disk was never served, which is the teardown path of a device
            // whose creation failed part way through.
            _ = reply.send(None);
            return;
        };
        disk.release = Some(reply);

        // A request whose chunks the capture channel never accepted changed
        // nothing, and the stopped device has already errored it. One whose
        // chunks were accepted is still waiting on the image, and must apply.
        for tag in std::mem::take(&mut disk.parked) {
            disk.slots[tag as usize] = Slot::default();
        }
    }

    fn release_quiet(&mut self) {
        let quiet: Vec<u32> = self
            .disks
            .iter()
            .filter(|(_, disk)| disk.release.is_some() && disk.pending == 0)
            .map(|(dev_id, _)| *dev_id)
            .collect();

        for dev_id in quiet {
            let Disk { image, release, .. } = self.disks.remove(&dev_id).expect("just listed");
            // Dropping the rest closes the character device and unmaps its
            // descriptors, which is what lets the kernel delete the device.
            _ = release.expect("filtered on").send(Some(image));
        }
    }

    /// Move as many backlogged submissions into the ring as fit.
    fn flush(&mut self) {
        let Self { ring, backlog, .. } = self;
        let mut submission = ring.submission();

        while let Some(entry) = backlog.front() {
            // SAFETY: every buffer an entry addresses belongs to a slot, to a
            // disk, or to `wake_buf`, none of which are dropped while that
            // entry is outstanding.
            if unsafe { submission.push(entry) }.is_err() {
                break;
            }
            backlog.pop_front();
        }
    }

    fn arm_wake(&mut self) {
        let entry = io_uring::opcode::Read::new(
            io_uring::types::Fd(self.waker.as_raw_fd()),
            self.wake_buf.as_mut_ptr(),
            self.wake_buf.len() as u32,
        )
        .build()
        .user_data(user_data(0, 0, Step::Wake));

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
            let (dev_id, tag, step) = parse_user_data(user_data);

            if let Step::Wake = step {
                self.arm_wake();
                self.retry_parked();
                continue;
            }
            let Self { disks, backlog, .. } = self;
            let Some(disk) = disks.get_mut(&dev_id) else {
                continue;
            };
            disk.pending -= 1;
            advance(disk, tag, step, result, backlog);
        }
    }

    /// Re-offer the chunks of every request parked on capture capacity. Order
    /// is preserved, so backpressure never reorders two mutations.
    fn retry_parked(&mut self) {
        let Self { disks, backlog, .. } = self;

        for disk in disks.values_mut() {
            while let Some(&tag) = disk.parked.front() {
                let chunks = std::mem::take(&mut disk.slots[tag as usize].chunks);

                match disk.capture.offer(chunks) {
                    Ok(()) => {
                        disk.parked.pop_front();
                        begin_mutation(disk, tag, backlog);
                    }
                    Err(chunks) => {
                        disk.slots[tag as usize].chunks = chunks;
                        break;
                    }
                }
            }
        }
    }
}

impl Disk {
    fn submit(&mut self, backlog: &mut Backlog, entry: io_uring::squeue::Entry) {
        self.pending += 1;
        backlog.push_back(entry);
    }
}

/// Advance the request at `tag`, whose `step` completed with `result`.
fn advance(disk: &mut Disk, tag: u16, step: Step, result: i32, backlog: &mut Backlog) {
    match step {
        Step::Wake => unreachable!("wake completions carry no disk"),

        // A negative fetch is how an owner learns the kernel has aborted the
        // queue, which it does when the device stops.
        Step::Fetch if result < 0 => disk.stopping = true,
        Step::Fetch => begin(disk, tag, backlog),

        Step::ImageRead => {
            let bytes = disk.slots[tag as usize].buf.len();

            match transferred(result, bytes) {
                Err(err) => fail(disk, tag, "reading the image", err, backlog),
                Ok(()) => {
                    let entry = write_device(disk, tag);
                    disk.submit(backlog, entry);
                }
            }
        }
        Step::DeviceWrite => {
            let bytes = disk.slots[tag as usize].buf.len();

            match transferred(result, bytes) {
                Err(err) => fail(disk, tag, "handing read data to the device", err, backlog),
                Ok(()) => complete(disk, tag, bytes as i32, backlog),
            }
        }
        Step::DeviceRead => {
            let bytes = disk.slots[tag as usize].buf.len();
            let block_size = disk.image.block_size();

            match transferred(result, bytes) {
                Err(err) => fail(disk, tag, "taking write data from the device", err, backlog),
                Ok(()) => {
                    let slot = &mut disk.slots[tag as usize];
                    let data = bytes::Bytes::from(std::mem::take(&mut slot.buf));
                    let block = slot.range.start;
                    slot.data = data.clone();

                    let chunks = crate::chunk::encode_write(block, &data, block_size);
                    offer(disk, tag, chunks, backlog);
                }
            }
        }
        Step::ImageWrite | Step::ImagePunch => finish_mutation(disk, tag, step, result, backlog),
    }
}

/// Decode the request the kernel handed back at `tag` and take its first step.
fn begin(disk: &mut Disk, tag: u16, backlog: &mut Backlog) {
    let desc = disk.descs.get(tag);
    let block_size = disk.image.block_size() as u64;

    let offset = desc.start_sector * sys::SECTOR_SIZE;
    let bytes = sys::io_desc_sectors(&desc) as u64 * sys::SECTOR_SIZE;

    // The device's logical block size is the tracking block size, so the block
    // layer cannot issue a request which straddles a block.
    assert!(
        offset % block_size == 0 && bytes % block_size == 0,
        "device request of {bytes} bytes at {offset} is not {block_size}-aligned",
    );
    let range = (offset / block_size) as u32..((offset + bytes) / block_size) as u32;
    assert!(
        range.end <= disk.image.blocks(),
        "device request covers blocks {range:?}, beyond the device",
    );
    disk.slots[tag as usize].range = range.clone();

    match sys::io_desc_op(&desc) {
        sys::UBLK_IO_OP_READ => {
            disk.slots[tag as usize].buf = vec![0; bytes as usize];
            let entry = read_image(disk, tag);
            disk.submit(backlog, entry);
        }
        sys::UBLK_IO_OP_WRITE => {
            disk.slots[tag as usize].buf = vec![0; bytes as usize];
            let entry = read_device(disk, tag);
            disk.submit(backlog, entry);
        }
        // Both deallocate, because an unallocated block reads as zeroes and
        // staying sparse is what keeps a rebuilt image small.
        sys::UBLK_IO_OP_DISCARD | sys::UBLK_IO_OP_WRITE_ZEROES => {
            let chunks = vec![crate::chunk::encode_punch(
                range.start,
                range.end - range.start,
            )];
            offer(disk, tag, chunks, backlog);
        }
        // The device advertises no volatile write cache, so a flush should
        // never arrive and nothing else is a block request this serves.
        op => {
            tracing::warn!(dev_id = disk.dev_id, op, "unsupported device request");
            complete(disk, tag, -libc::EOPNOTSUPP, backlog);
        }
    }
}

/// Hand `chunks` to the capture channel. A mutation is captured before it is
/// applied, so that journal order is application order, and it waits here when
/// the channel is full rather than being dropped or refused.
fn offer(disk: &mut Disk, tag: u16, chunks: Vec<Chunk>, backlog: &mut Backlog) {
    match disk.capture.offer(chunks) {
        Ok(()) => begin_mutation(disk, tag, backlog),
        Err(chunks) => {
            disk.slots[tag as usize].chunks = chunks;
            disk.parked.push_back(tag);
        }
    }
}

fn begin_mutation(disk: &mut Disk, tag: u16, backlog: &mut Backlog) {
    let range = disk.slots[tag as usize].range.clone();

    if disk.inflight.begin(tag, range) {
        mutate(disk, tag, backlog);
    }
}

/// Submit the image write or punch of `tag`.
fn mutate(disk: &mut Disk, tag: u16, backlog: &mut Backlog) {
    let fd = io_uring::types::Fd(std::os::fd::AsRawFd::as_raw_fd(disk.image.file()));
    let slot = &disk.slots[tag as usize];
    let offset = disk.image.offset(slot.range.start);

    // A punch is the request which carries no data.
    let entry = if slot.data.is_empty() {
        let bytes = (slot.range.end - slot.range.start) as u64 * disk.image.block_size() as u64;

        io_uring::opcode::Fallocate::new(fd, bytes)
            .offset(offset)
            .mode(crate::image::PUNCH_MODE)
            .build()
            .user_data(user_data(disk.dev_id, tag, Step::ImagePunch))
    } else {
        io_uring::opcode::Write::new(fd, slot.data.as_ptr(), slot.data.len() as u32)
            .offset(offset)
            .build()
            .user_data(user_data(disk.dev_id, tag, Step::ImageWrite))
    };
    disk.submit(backlog, entry);
}

fn finish_mutation(disk: &mut Disk, tag: u16, step: Step, result: i32, backlog: &mut Backlog) {
    let slot = &disk.slots[tag as usize];
    let range = slot.range.clone();

    let expected = match step {
        Step::ImageWrite => slot.data.len(),
        _ => 0,
    };
    let outcome = match transferred(result, expected) {
        Ok(()) if step == Step::ImageWrite => {
            disk.image.allocate(range);
            expected as i32
        }
        Ok(()) => {
            disk.image.deallocate(range);
            0
        }
        // An image write which fails, which in practice means ENOSPC, errors
        // only its own request. Ext4's default `errors=remount-ro` then
        // contains the failure to this one disk.
        Err(err) => {
            tracing::error!(dev_id = disk.dev_id, ?err, "image mutation failed");
            -libc::EIO
        }
    };

    for released in disk.inflight.end(tag) {
        mutate(disk, released, backlog);
    }
    complete(disk, tag, outcome, backlog);
}

/// Complete `tag` back to the kernel and re-arm its fetch. `result` is the
/// bytes the request transferred, or a negative errno. A read which reports
/// zero bytes is an I/O error to the kernel.
fn complete(disk: &mut Disk, tag: u16, result: i32, backlog: &mut Backlog) {
    disk.slots[tag as usize] = Slot::default();

    // A stopped device has already errored every request it had outstanding.
    if disk.stopping {
        return;
    }
    let entry = io_command(disk, tag, sys::UBLK_U_IO_COMMIT_AND_FETCH_REQ, result);
    disk.submit(backlog, entry);
}

fn fail(disk: &mut Disk, tag: u16, what: &str, err: std::io::Error, backlog: &mut Backlog) {
    tracing::error!(dev_id = disk.dev_id, tag, ?err, "{what} failed");
    complete(disk, tag, -libc::EIO, backlog);
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

fn read_image(disk: &mut Disk, tag: u16) -> io_uring::squeue::Entry {
    let fd = io_uring::types::Fd(std::os::fd::AsRawFd::as_raw_fd(disk.image.file()));
    let offset = disk.image.offset(disk.slots[tag as usize].range.start);
    let dev_id = disk.dev_id;

    let slot = &mut disk.slots[tag as usize];
    let (buf, len) = (slot.buf.as_mut_ptr(), slot.buf.len() as u32);

    io_uring::opcode::Read::new(fd, buf, len)
        .offset(offset)
        .build()
        .user_data(user_data(dev_id, tag, Step::ImageRead))
}

/// Hand a read's image content to the character device, which is how request
/// data moves under `UBLK_F_USER_COPY`.
fn write_device(disk: &mut Disk, tag: u16) -> io_uring::squeue::Entry {
    let fd = io_uring::types::Fd(std::os::fd::AsRawFd::as_raw_fd(&disk.cdev));
    let offset = sys::io_buf_offset(ublk::QUEUE_ID, tag);
    let dev_id = disk.dev_id;

    let slot = &disk.slots[tag as usize];
    let (buf, len) = (slot.buf.as_ptr(), slot.buf.len() as u32);

    io_uring::opcode::Write::new(fd, buf, len)
        .offset(offset)
        .build()
        .user_data(user_data(dev_id, tag, Step::DeviceWrite))
}

/// Take a write's incoming data from the character device.
fn read_device(disk: &mut Disk, tag: u16) -> io_uring::squeue::Entry {
    let fd = io_uring::types::Fd(std::os::fd::AsRawFd::as_raw_fd(&disk.cdev));
    let offset = sys::io_buf_offset(ublk::QUEUE_ID, tag);
    let dev_id = disk.dev_id;

    let slot = &mut disk.slots[tag as usize];
    let (buf, len) = (slot.buf.as_mut_ptr(), slot.buf.len() as u32);

    io_uring::opcode::Read::new(fd, buf, len)
        .offset(offset)
        .build()
        .user_data(user_data(dev_id, tag, Step::DeviceRead))
}

fn io_command(disk: &Disk, tag: u16, cmd_op: u32, result: i32) -> io_uring::squeue::Entry {
    let command = sys::io_cmd(ublk::QUEUE_ID, tag, result);
    // SAFETY: `UblksrvIoCmd` is `repr(C)` and its 16 bytes are fully occupied by
    // its fields, so the copy reads no padding.
    let bytes = unsafe { sys::cmd_bytes::<_, 16>(&command) };

    io_uring::opcode::UringCmd16::new(
        io_uring::types::Fd(std::os::fd::AsRawFd::as_raw_fd(&disk.cdev)),
        cmd_op,
    )
    .cmd(bytes)
    .build()
    .user_data(user_data(disk.dev_id, tag, Step::Fetch))
}

#[cfg(test)]
mod test {
    use super::{Step, parse_user_data, user_data};

    #[test]
    fn test_user_data_round_trips() {
        for (dev_id, tag, step) in [
            (0, 0, Step::Fetch),
            (7, 31, Step::ImageWrite),
            (u32::MAX, u16::MAX, Step::ImagePunch),
        ] {
            assert_eq!(
                parse_user_data(user_data(dev_id, tag, step)),
                (dev_id, tag, step)
            );
        }
    }
}
