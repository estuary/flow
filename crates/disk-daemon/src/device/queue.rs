//! The `ublk` queue one owner serves, over the `io_uring` which carries it.
//!
//! The ring carries the queue's fetch and commit commands, and the poll which wakes
//! the owner. The queue also moves each request's data through the character device,
//! as blocking calls: a write's as its request is handed over, and a read's as it
//! completes. The owner and its backend therefore take a [`Request`] and answer it
//! with a [`Reply`], both plain data. What the ring buys is one wait for everything
//! which can wake the owner, and commits and fetches which travel in batches.
//!
//! A tag is the kernel's while a fetch, or a commit which re-arms one, is queued or
//! in flight. It is the owner's from the fetch's completion until the owner
//! completes its request. The kernel aborts every tag's fetch once the device has
//! stopped. [`Tags`] tracks which side holds each tag, so a request completed
//! twice, or one never handed over, panics where the owner completes it. The
//! kernel could not catch every such commit: one which reaches it after it has
//! handed the tag's next request over commits that request instead.
//!
//! None of it needs `io_uring`'s worker threads: the driver holds the fetches, and
//! the wake is polled. An operation handed to one would run in a pool which belongs
//! to the owner's thread, at the kernel's default size, and which nothing caps
//! across disks.

use crate::ublk::{self, sys};
use crate::wake::Waker;

/// One disk's `ublk` queue. Only its owner's thread touches it.
pub(super) struct Queue {
    dev_id: u32,
    /// Size of the device, which bounds every request it hands over.
    blocks: u32,
    ring: io_uring::IoUring,
    tags: Tags,
    /// Tags handed over by the last wait. Kept across waits, so a wait allocates
    /// nothing for them.
    fetched: Vec<u16>,
    waker: Waker,
    cdev: std::fs::File,
    descs: ublk::IoDescs,
}

/// Which side of the queue holds each of its tags. It changes only as a batch of
/// completions arrives, or as the owner completes a request.
struct Tags {
    /// Names the device in the panic of a completion the kernel refused.
    dev_id: u32,
    held: Vec<Tag>,
}

/// Which side of the queue holds a tag.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Tag {
    /// A fetch, or a commit which re-arms one, is queued or in flight.
    Fetching,
    /// The kernel handed over a request, which the owner has yet to complete.
    Owned,
    /// The kernel aborted the tag's fetch, and nothing re-arms it.
    Aborted,
}

impl Queue {
    /// A queue of `depth` tags over the character device `cdev` of a device of
    /// `blocks`, which `waker` interrupts. It returns with the wake armed and a fetch
    /// in flight for every tag.
    ///
    /// This must run on the thread which will serve the disk. `ublk` binds the
    /// queue to whichever thread issues its first fetch.
    pub fn new(
        dev_id: u32,
        cdev: std::fs::File,
        waker: Waker,
        depth: u16,
        blocks: u32,
    ) -> std::io::Result<Self> {
        // A tag has one operation queued or in flight only while the kernel holds
        // it, and the wake has at most one. A ring of that size therefore always
        // has room for the next operation, however much a submission left queued,
        // and its completion queue, which the kernel makes twice as large, cannot
        // overflow.
        let entries = (depth as u32 + 1).next_power_of_two();

        let mut queue = Self {
            dev_id,
            blocks,
            ring: io_uring::IoUring::new(entries)?,
            tags: Tags::new(dev_id, depth),
            fetched: Vec::new(),
            waker,
            descs: ublk::IoDescs::map(&cdev, ublk::QUEUE_ID, depth)?,
            cdev,
        };
        queue.push(queue.poll_wake());

        for tag in 0..depth {
            queue.push(queue.io_command(tag, sys::UBLK_U_IO_FETCH_REQ, 0));
        }
        // A submission may take fewer entries than are queued, and leave the rest
        // for the next one.
        while !queue.ring.submission().is_empty() {
            queue.ring.submit()?;
        }
        Ok(queue)
    }

    /// Whether the device has stopped. The kernel aborts every tag's fetch once
    /// it has, which is only after every request has completed.
    pub fn stopped(&self) -> bool {
        self.tags.stopped()
    }

    /// Submit what is queued and wait for anything to complete. Append each request
    /// the kernel has handed over to `requests`, with a write's data taken from the
    /// device already. A write whose data cannot be taken fails here, and the owner
    /// never sees it.
    ///
    /// A wake completes here too, and interrupts the wait. It is drained and
    /// re-armed, and otherwise ignored: whatever it announced, the owner looks for
    /// itself.
    pub fn wait(&mut self, requests: &mut Vec<(u16, Request)>) -> std::io::Result<()> {
        match self.ring.submit_and_wait(1) {
            Err(err) if err.kind() != std::io::ErrorKind::Interrupted => return Err(err),
            _ => (),
        }
        let completions = self
            .ring
            .completion()
            .map(|cqe| (cqe.user_data(), cqe.result()));
        let woken = self.tags.on_completions(completions, &mut self.fetched);

        // The drain must come before this returns, because the owner looks for
        // what a wake announced before it next waits. Drained any later, a wake
        // which landed after the owner looked would be lost.
        if woken {
            () = self.waker.drain();
            self.push(self.poll_wake());
        }

        let mut fetched = std::mem::take(&mut self.fetched);
        for tag in fetched.drain(..) {
            if let Some(request) = self.take_request(tag) {
                requests.push((tag, request));
            }
        }
        self.fetched = fetched;

        Ok(())
    }

    /// Complete the request at `tag` with `reply`, and re-arm its fetch. A read's
    /// data is handed to the device first, and a read whose data cannot be handed
    /// over fails instead.
    pub fn complete(&mut self, tag: u16, reply: Reply) {
        let result = match reply {
            Reply::Done(bytes) => bytes as i32,
            Reply::Failed(errno) => -errno,
            Reply::Data(buf) => {
                match std::os::unix::fs::FileExt::write_all_at(&self.cdev, &buf, data_offset(tag)) {
                    Ok(()) => buf.len() as i32,
                    Err(err) => return self.fail(tag, "handing read data to the device", err),
                }
            }
        };
        self.commit(tag, result)
    }

    /// The request the kernel handed over at `tag`, as data. `None` where a write's
    /// data cannot be taken from the device, which fails that request.
    fn take_request(&mut self, tag: u16) -> Option<Request> {
        let request = match decode(&self.descs.get(tag), self.blocks) {
            Op::Read(range) => Request::Read(range),
            Op::Write(range) => {
                let mut data = vec![0; range.len() * crate::BLOCK_SIZE as usize];

                if let Err(err) = std::os::unix::fs::FileExt::read_exact_at(
                    &self.cdev,
                    &mut data,
                    data_offset(tag),
                ) {
                    () = self.fail(tag, "taking write data from the device", err);
                    return None;
                }
                Request::Write {
                    start: range.start,
                    data: data.into(),
                }
            }
            Op::Punch(range) => Request::Punch(range),
            Op::Unsupported(op) => Request::Unsupported(op),
        };
        Some(request)
    }

    /// Fail the request at `tag` for want of its data, and only it. Nothing it did
    /// was recorded.
    fn fail(&mut self, tag: u16, what: &str, err: std::io::Error) {
        tracing::error!(
            dev_id = self.dev_id,
            tag,
            what,
            ?err,
            "device request failed"
        );
        self.commit(tag, -libc::EIO)
    }

    /// Hand `tag` back to the kernel and re-arm its fetch. `result` is the bytes the
    /// request transferred, or a negative errno. The kernel reads a zero-byte read as
    /// an I/O error.
    fn commit(&mut self, tag: u16, result: i32) {
        () = self.tags.complete(tag);
        self.push(self.io_command(tag, sys::UBLK_U_IO_COMMIT_AND_FETCH_REQ, result));
    }

    /// Poll the waker's eventfd, through which a command, or room freed in the
    /// recording channel, interrupts the owner's wait.
    fn poll_wake(&self) -> io_uring::squeue::Entry {
        io_uring::opcode::PollAdd::new(
            io_uring::types::Fd(self.waker.as_raw_fd()),
            libc::POLLIN as u32,
        )
        .build()
        .user_data(user_data(0, Step::Wake))
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

    /// Queue `entry` for the next submission.
    fn push(&mut self, entry: io_uring::squeue::Entry) {
        // SAFETY: no entry addresses memory. A command's bytes travel inside the
        // entry, and a poll carries none.
        unsafe { self.ring.submission().push(&entry) }
            .expect("the ring has room for an operation per tag, and the wake");
    }
}

impl Tags {
    /// `depth` tags, each with a fetch in flight.
    fn new(dev_id: u32, depth: u16) -> Self {
        Self {
            dev_id,
            held: vec![Tag::Fetching; depth as usize],
        }
    }

    fn stopped(&self) -> bool {
        self.held.iter().all(|tag| *tag == Tag::Aborted)
    }

    /// Apply a batch of completions, each a `user_data` and its `result`. Append
    /// each tag the kernel handed over to `fetched`, and return whether the wake
    /// was among them.
    fn on_completions(
        &mut self,
        completions: impl IntoIterator<Item = (u64, i32)>,
        fetched: &mut Vec<u16>,
    ) -> bool {
        let mut woken = false;

        for (user_data, result) in completions {
            match decode_completion(self.dev_id, user_data, result) {
                Completion::Wake => woken = true,
                Completion::Fetched(tag) => {
                    () = self.advance(tag, Tag::Fetching, Tag::Owned);
                    fetched.push(tag);
                }
                Completion::Aborted(tag) => self.advance(tag, Tag::Fetching, Tag::Aborted),
            }
        }
        woken
    }

    /// The owner completes the request at `tag`, which re-arms its fetch.
    fn complete(&mut self, tag: u16) {
        self.advance(tag, Tag::Owned, Tag::Fetching)
    }

    /// Move `tag` from `from` to `to`. A tag found anywhere else is a request the
    /// owner completed twice, or one the kernel never handed over.
    fn advance(&mut self, tag: u16, from: Tag, to: Tag) {
        let held = &mut self.held[tag as usize];

        assert_eq!(*held, from, "tag {tag} cannot move to {to:?}");
        *held = to;
    }
}

/// One device request, as the queue hands it over.
#[derive(Debug, PartialEq)]
pub(super) enum Request {
    Read(std::ops::Range<u32>),
    /// A write, whose data the queue has taken from the device.
    Write {
        start: u32,
        data: bytes::Bytes,
    },
    /// A discard or write-zeroes. Both deallocate, per `chunk::encode_punch`.
    Punch(std::ops::Range<u32>),
    /// The device advertises no volatile write cache, so a flush should never
    /// arrive. This daemon serves no other kind of block request.
    Unsupported(u8),
}

/// How a request is answered.
#[derive(Debug, PartialEq)]
pub(super) enum Reply {
    /// The request transferred this many bytes.
    Done(u32),
    /// A read's data, which the queue hands to the device as it completes the read.
    Data(Vec<u8>),
    /// The request failed with this errno.
    Failed(i32),
}

/// One device request, as the kernel described it when it handed over its tag.
#[derive(Debug, PartialEq)]
enum Op {
    Read(std::ops::Range<u32>),
    Write(std::ops::Range<u32>),
    Punch(std::ops::Range<u32>),
    Unsupported(u8),
}

/// What one completion reports.
#[derive(Debug, PartialEq)]
enum Completion {
    /// The waker's eventfd is readable.
    Wake,
    /// The kernel handed over a request at this tag.
    Fetched(u16),
    /// The kernel aborted this tag's fetch.
    Aborted(u16),
}

/// Which operation a completion belongs to.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
enum Step {
    Wake = 0,
    Fetch = 1,
}

fn user_data(tag: u16, step: Step) -> u64 {
    (tag as u64) << 8 | step as u64
}

fn parse_user_data(user_data: u64) -> (u16, Step) {
    let step = match user_data as u8 {
        0 => Step::Wake,
        1 => Step::Fetch,
        other => panic!("completion carries unknown step {other}"),
    };
    ((user_data >> 8) as u16, step)
}

/// Decode a completion of device `dev_id`'s ring. Anything but a wake, a request,
/// or an abort is the kernel refusing an operation this queue built.
fn decode_completion(dev_id: u32, user_data: u64, result: i32) -> Completion {
    let (tag, step) = parse_user_data(user_data);

    match step {
        Step::Wake if result >= 0 => Completion::Wake,
        Step::Fetch if result == sys::UBLK_IO_RES_OK => Completion::Fetched(tag),
        Step::Fetch if result == sys::UBLK_IO_RES_ABORT => Completion::Aborted(tag),
        _ if result < 0 => panic!(
            "device {dev_id} refused the {step:?} of tag {tag}: {}",
            std::io::Error::from_raw_os_error(-result),
        ),
        _ => panic!("device {dev_id} completed the {step:?} of tag {tag} with {result}"),
    }
}

fn decode(desc: &sys::UblksrvIoDesc, blocks: u32) -> Op {
    let sectors_per_block = crate::BLOCK_SIZE as u64 / sys::SECTOR_SIZE;
    let (start_sector, sectors) = (desc.start_sector, sys::io_desc_sectors(desc) as u64);

    // The device's logical block size is the tracking block size. The block layer
    // therefore cannot issue a request which straddles a block.
    assert!(
        start_sector.is_multiple_of(sectors_per_block) && sectors.is_multiple_of(sectors_per_block),
        "device request of {sectors} sectors at sector {start_sector} is not block-aligned",
    );
    // Bounded before narrowing, so a request past the device's end cannot wrap
    // back onto it.
    let start = start_sector / sectors_per_block;
    let end = start + sectors / sectors_per_block;
    assert!(
        end <= blocks as u64,
        "device request covers blocks {start}..{end}, beyond the device",
    );
    let range = start as u32..end as u32;

    match sys::io_desc_op(desc) {
        sys::UBLK_IO_OP_READ => Op::Read(range),
        sys::UBLK_IO_OP_WRITE => Op::Write(range),
        sys::UBLK_IO_OP_DISCARD | sys::UBLK_IO_OP_WRITE_ZEROES => Op::Punch(range),
        op => Op::Unsupported(op),
    }
}

/// Under `UBLK_F_USER_COPY`, a request's data moves through the character device,
/// at an offset which names the queue and tag.
fn data_offset(tag: u16) -> u64 {
    sys::io_buf_offset(ublk::QUEUE_ID, tag)
}

#[cfg(test)]
mod test {
    use super::{
        Completion, Op, Step, Tags, decode, decode_completion, parse_user_data, user_data,
    };
    use crate::ublk::sys;

    #[test]
    fn test_user_data_round_trips() {
        for (tag, step) in [(0, Step::Wake), (15, Step::Fetch), (u16::MAX, Step::Fetch)] {
            assert_eq!(parse_user_data(user_data(tag, step)), (tag, step));
        }
    }

    #[test]
    fn test_a_completion_decodes_to_what_it_reports() {
        let decoded = [
            (user_data(0, Step::Wake), libc::POLLIN as i32),
            (user_data(3, Step::Fetch), sys::UBLK_IO_RES_OK),
            (user_data(7, Step::Fetch), sys::UBLK_IO_RES_ABORT),
        ]
        .map(|(user_data, result)| decode_completion(0, user_data, result));

        assert_eq!(
            decoded,
            [
                Completion::Wake,
                Completion::Fetched(3),
                Completion::Aborted(7)
            ]
        );
    }

    #[test]
    #[should_panic(expected = "device 0 refused the Fetch of tag 3")]
    fn test_a_refused_command_panics() {
        decode_completion(0, user_data(3, Step::Fetch), -libc::EINVAL);
    }

    /// A failed wake which were re-armed regardless would fail again at once, and
    /// spin the owner.
    #[test]
    #[should_panic(expected = "device 0 refused the Wake of tag 0")]
    fn test_a_failed_wake_panics() {
        decode_completion(0, user_data(0, Step::Wake), -libc::EBADF);
    }

    fn woken() -> (u64, i32) {
        (user_data(0, Step::Wake), libc::POLLIN as i32)
    }

    fn fetched(tag: u16) -> (u64, i32) {
        (user_data(tag, Step::Fetch), sys::UBLK_IO_RES_OK)
    }

    fn aborted(tag: u16) -> (u64, i32) {
        (user_data(tag, Step::Fetch), sys::UBLK_IO_RES_ABORT)
    }

    /// Render what `on_completions` returned, or `-` for a completion by the owner,
    /// and the tags after it.
    fn line(what: &str, returned: Option<(Vec<u16>, bool)>, tags: &Tags) -> String {
        let returned = match returned {
            Some((fetched, woken)) => format!("fetched {fetched:?} woken {woken}"),
            None => "-".to_string(),
        };
        format!(
            "{what:<24}{returned:<28}{:?} stopped {}",
            tags.held,
            tags.stopped()
        )
    }

    fn on_completions(tags: &mut Tags, what: &str, batch: &[(u64, i32)]) -> String {
        let mut fetched = Vec::new();
        let woken = tags.on_completions(batch.iter().copied(), &mut fetched);
        line(what, Some((fetched, woken)), tags)
    }

    fn complete(tags: &mut Tags, tag: u16) -> String {
        () = tags.complete(tag);
        line(&format!("complete {tag}"), None, tags)
    }

    /// A tag moves between the kernel and the owner with each request, and the
    /// queue stops only once the kernel has aborted every tag's fetch. It aborts a
    /// fetch only after the owner has completed that tag's request.
    #[test]
    fn test_a_tag_follows_its_requests_until_the_abort() {
        let mut tags = Tags::new(0, 2);

        let trace = [
            on_completions(
                &mut tags,
                "wake and two requests",
                &[woken(), fetched(0), fetched(1)],
            ),
            complete(&mut tags, 0),
            on_completions(&mut tags, "abort of tag 0", &[aborted(0)]),
            complete(&mut tags, 1),
            on_completions(&mut tags, "abort of tag 1", &[aborted(1)]),
        ]
        .join("\n");

        insta::assert_snapshot!(trace, @"
        wake and two requests   fetched [0, 1] woken true   [Owned, Owned] stopped false
        complete 0              -                           [Fetching, Owned] stopped false
        abort of tag 0          fetched [] woken false      [Aborted, Owned] stopped false
        complete 1              -                           [Aborted, Fetching] stopped false
        abort of tag 1          fetched [] woken false      [Aborted, Aborted] stopped true
        ");
    }

    #[test]
    #[should_panic(expected = "tag 1 cannot move to Fetching")]
    fn test_completing_a_request_twice_panics() {
        let mut tags = Tags::new(0, 2);
        _ = tags.on_completions([fetched(1)], &mut Vec::new());

        tags.complete(1);
        tags.complete(1);
    }

    #[test]
    #[should_panic(expected = "tag 0 cannot move to Owned")]
    fn test_a_request_at_a_tag_the_owner_holds_panics() {
        let mut tags = Tags::new(0, 1);
        _ = tags.on_completions([fetched(0), fetched(0)], &mut Vec::new());
    }

    #[test]
    #[should_panic(expected = "tag 0 cannot move to Aborted")]
    fn test_an_abort_of_a_tag_the_owner_holds_panics() {
        let mut tags = Tags::new(0, 1);
        _ = tags.on_completions([fetched(0), aborted(0)], &mut Vec::new());
    }

    fn desc(op: u8, block: u64, blocks: u32) -> sys::UblksrvIoDesc {
        let sectors_per_block = crate::BLOCK_SIZE as u64 / sys::SECTOR_SIZE;

        let mut desc = sys::UblksrvIoDesc {
            op_flags: op as u32,
            start_sector: block * sectors_per_block,
            ..Default::default()
        };
        desc.__bindgen_anon_1.nr_sectors = blocks * sectors_per_block as u32;
        desc
    }

    #[test]
    fn test_a_descriptor_decodes_to_the_blocks_it_covers() {
        let decoded = [
            (sys::UBLK_IO_OP_READ, 0, 1),
            (sys::UBLK_IO_OP_WRITE, 3, 2),
            (sys::UBLK_IO_OP_DISCARD, 8, 8),
            (sys::UBLK_IO_OP_WRITE_ZEROES, 15, 1),
            // A flush, which covers nothing.
            (2, 0, 0),
        ]
        .map(|(op, block, blocks)| decode(&desc(op, block, blocks), 16));

        assert_eq!(
            decoded,
            [
                Op::Read(0..1),
                Op::Write(3..5),
                Op::Punch(8..16),
                Op::Punch(15..16),
                Op::Unsupported(2),
            ]
        );
    }

    /// Narrowed to 32 bits first, this request would wrap onto block 0.
    #[test]
    #[should_panic(expected = "beyond the device")]
    fn test_a_request_beyond_the_device_panics() {
        decode(&desc(sys::UBLK_IO_OP_READ, 1 << 32, 1), 16);
    }
}
