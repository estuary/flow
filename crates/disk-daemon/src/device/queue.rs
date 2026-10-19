use super::wake;
use crate::ublk::{self, sys};

/// One disk's `ublk` queue. Only its owner's thread touches it.
pub(super) struct Queue {
    /// Kernel device ID.
    dev_id: u32,
    /// Carries only ublk commands and the waker's poll.
    ring: io_uring::IoUring,
    /// Ownership of each request tag.
    tags: Tags,
    /// Tags handed over by the last wait.
    fetched: Vec<u16>,
    /// Eventfd used to wake the owner.
    waker: wake::Waker,
    /// Character device used for request data copies.
    cdev: std::fs::File,
    /// Kernel request descriptors, indexed by tag.
    descs: ublk::IoDescs,
}

impl Queue {
    /// Create a queue with the wake armed and a fetch in flight for every tag.
    pub fn new(
        dev_id: u32,
        cdev: std::fs::File,
        waker: wake::Waker,
        depth: u16,
    ) -> std::io::Result<Self> {
        // Each tag and the wake have at most one operation queued or in flight.
        // This sizes both the submission queue and its larger completion queue
        // to avoid overflow, even after a partial submission.
        let entries = (depth as u32 + 1).next_power_of_two();

        let mut queue = Self {
            dev_id,
            ring: io_uring::IoUring::new(entries)?,
            tags: Tags::new(dev_id, depth),
            fetched: Vec::new(),
            waker,
            descs: ublk::IoDescs::map(&cdev, ublk::QUEUE_ID, depth)?,
            cdev,
        };
        // Arm the wake, so a command can interrupt the first wait.
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

    /// Whether the device has stopped, which the kernel reports by aborting every
    /// tag's fetch once every request has completed.
    pub fn stopped(&self) -> bool {
        self.tags.stopped()
    }

    /// Wait for completions and append fetched requests.
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

        // Drain before the owner checks for work. Draining afterward could
        // lose a wake that arrived between that check and the drain.
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
                let offset = sys::io_buf_offset(ublk::QUEUE_ID, tag);

                match std::os::unix::fs::FileExt::write_all_at(&self.cdev, &buf, offset) {
                    Ok(()) => buf.len() as i32,
                    Err(err) => {
                        return self.fail_copy(tag, "handing read data to the device", err);
                    }
                }
            }
        };
        self.commit(tag, result)
    }

    /// The request the kernel handed over at `tag`, as data. `None` where a write's
    /// data cannot be taken from the device, which fails that request.
    fn take_request(&mut self, tag: u16) -> Option<Request> {
        let request = match decode(&self.descs.get(tag)) {
            Op::Read(range) => Request::Read(range),
            Op::Write(range) => {
                // Under `UBLK_F_USER_COPY`, a request's data moves through the character
                // device, at an offset which names the queue and tag.
                let mut data = vec![0; range.len() * crate::BLOCK_SIZE as usize];
                let offset = sys::io_buf_offset(ublk::QUEUE_ID, tag);

                if let Err(err) =
                    std::os::unix::fs::FileExt::read_exact_at(&self.cdev, &mut data, offset)
                {
                    () = self.fail_copy(tag, "taking write data from the device", err);
                    return None;
                }
                Request::Write {
                    start: range.start,
                    data: data.into(),
                }
            }
            Op::Punch(range) => Request::Punch(range),
        };
        Some(request)
    }

    /// Fail the request at `tag` with EIO, where its data could not be copied
    /// through the character device.
    fn fail_copy(&mut self, tag: u16, what: &str, err: std::io::Error) {
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
    /// request transferred, or a negative errno.
    fn commit(&mut self, tag: u16, result: i32) {
        () = self.tags.advance(tag, Tag::Owned, Tag::Fetching);
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

    fn push(&mut self, entry: io_uring::squeue::Entry) {
        // SAFETY: no entry addresses memory. A command's bytes travel inside the
        // entry, and a poll carries none.
        unsafe { self.ring.submission().push(&entry) }
            .expect("the ring has room for an operation per tag, and the wake");
    }
}

/// One device request, as the queue hands it over.
#[derive(Debug, PartialEq)]
pub(super) enum Request {
    /// A read of these blocks, which the target serves straight from the image.
    Read(std::ops::Range<u32>),
    /// A write, whose data the queue has taken from the device.
    Write { start: u32, data: bytes::Bytes },
    /// A discard or write-zeroes. Both deallocate, per `chunk::encode_punch`.
    Punch(std::ops::Range<u32>),
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

/// Track which side holds each tag. A duplicate commit can complete the wrong
/// request after the kernel reuses a tag, so check transitions locally.
struct Tags {
    /// Names the device in the panic of a completion the kernel refused.
    dev_id: u32,
    /// Which side holds each tag, indexed by tag.
    held: Vec<Tag>,
}

impl Tags {
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

    /// Move `tag` from `from` to `to`. A tag found anywhere else is a request the
    /// owner completed twice, or one the kernel never handed over.
    fn advance(&mut self, tag: u16, from: Tag, to: Tag) {
        let held = &mut self.held[tag as usize];

        assert_eq!(*held, from, "tag {tag} cannot move to {to:?}");
        *held = to;
    }
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

/// Decode a completion of device `dev_id`'s ring. Anything but a wake, a request,
/// or an abort is the kernel refusing an operation this queue built.
fn decode_completion(dev_id: u32, user_data: u64, result: i32) -> Completion {
    let tag = (user_data >> 8) as u16;
    let step = match user_data as u8 {
        0 => Step::Wake,
        1 => Step::Fetch,
        other => panic!("completion carries unknown step {other}"),
    };

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

/// One device request, as the kernel described it when it handed over its tag.
#[derive(Debug, PartialEq)]
enum Op {
    Read(std::ops::Range<u32>),
    Write(std::ops::Range<u32>),
    Punch(std::ops::Range<u32>),
}

fn decode(desc: &sys::UblksrvIoDesc) -> Op {
    let sectors_per_block = crate::BLOCK_SIZE as u64 / sys::SECTOR_SIZE;
    let (start_sector, sectors) = (desc.start_sector, sys::io_desc_sectors(desc) as u64);

    // The device's logical block size is the tracking block size. The block layer
    // therefore cannot issue a request which straddles a block.
    assert!(
        start_sector.is_multiple_of(sectors_per_block) && sectors.is_multiple_of(sectors_per_block),
        "device request of {sectors} sectors at sector {start_sector} is not block-aligned",
    );
    // The block layer bounds every request by the device's size, a `u32` count of
    // blocks.
    let start = start_sector / sectors_per_block;
    let end = start + sectors / sectors_per_block;
    assert!(
        end <= u32::MAX as u64,
        "device request covers blocks {start}..{end}, beyond any device",
    );
    let range = start as u32..end as u32;

    match sys::io_desc_op(desc) {
        sys::UBLK_IO_OP_READ => Op::Read(range),
        sys::UBLK_IO_OP_WRITE => Op::Write(range),
        sys::UBLK_IO_OP_DISCARD | sys::UBLK_IO_OP_WRITE_ZEROES => Op::Punch(range),
        op => panic!("device request has op {op}, which this device does not serve"),
    }
}

#[cfg(test)]
mod test {
    use super::{Step, Tag, Tags, decode, decode_completion, user_data};
    use crate::ublk::sys;

    /// Run `f`, rendering what it returned or the first line of its panic.
    fn outcome(f: impl FnOnce() -> String) -> String {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
            Ok(rendered) => rendered,
            Err(panic) => {
                let message = (panic.downcast_ref::<String>().map(String::as_str))
                    .or_else(|| panic.downcast_ref::<&str>().copied())
                    .unwrap_or_default();
                format!("panicked: {}", message.lines().next().unwrap_or_default())
            }
        }
    }

    #[test]
    fn test_a_completion_decodes_to_what_it_reports() {
        let mut out = Vec::new();

        for (tag, step, result, named) in [
            (0, Step::Wake, libc::POLLIN as i32, "POLLIN"),
            (3, Step::Fetch, sys::UBLK_IO_RES_OK, "UBLK_IO_RES_OK"),
            (
                u16::MAX,
                Step::Fetch,
                sys::UBLK_IO_RES_ABORT,
                "UBLK_IO_RES_ABORT",
            ),
            (3, Step::Fetch, -libc::EINVAL, "-EINVAL"),
            // Re-arming a failed wake would make the owner spin.
            (0, Step::Wake, -libc::EBADF, "-EBADF"),
        ] {
            let decoded =
                outcome(|| format!("{:?}", decode_completion(0, user_data(tag, step), result)));
            // Derived `Debug` of a unit variant ignores width, so pad the rendering.
            let step = format!("{step:?}");
            out.push(format!("{step:<6}tag {tag:<6}{named:<19}{decoded}"));
        }
        insta::assert_snapshot!(out.join("\n"), @"
        Wake  tag 0     POLLIN             Wake
        Fetch tag 3     UBLK_IO_RES_OK     Fetched(3)
        Fetch tag 65535 UBLK_IO_RES_ABORT  Aborted(65535)
        Fetch tag 3     -EINVAL            panicked: device 0 refused the Fetch of tag 3: Invalid argument (os error 22)
        Wake  tag 0     -EBADF             panicked: device 0 refused the Wake of tag 0: Bad file descriptor (os error 9)
        ");
    }

    /// What happens to a queue's tags, as the kernel and the owner act on them.
    #[derive(Debug, Clone, Copy)]
    enum Event {
        /// The kernel completes the poll of the wake.
        Wake,
        /// The kernel hands over a request at this tag.
        Fetched(u16),
        /// The kernel aborts this tag's fetch.
        Aborted(u16),
        /// The owner completes the request at this tag.
        Complete(u16),
    }

    /// A tag moves between the kernel and the owner with each request, and the
    /// queue stops only once the kernel has aborted every tag's fetch. A tag found
    /// on the wrong side is a request completed twice, or one never handed over.
    #[test]
    fn test_a_tag_follows_its_requests_until_the_abort() {
        use Event::*;
        let mut out = Vec::new();

        for (case, events) in [
            (
                "until the abort",
                &[
                    Wake,
                    Fetched(0),
                    Fetched(1),
                    Complete(0),
                    Aborted(0),
                    Complete(1),
                    Aborted(1),
                ][..],
            ),
            ("completed twice", &[Fetched(1), Complete(1), Complete(1)]),
            ("fetched while owned", &[Fetched(0), Fetched(0)]),
            ("aborted while owned", &[Fetched(0), Aborted(0)]),
        ] {
            let mut tags = Tags::new(0, 2);

            for &event in events {
                let rendered = outcome(|| {
                    let completion = match event {
                        Wake => (user_data(0, Step::Wake), libc::POLLIN as i32),
                        Fetched(tag) => (user_data(tag, Step::Fetch), sys::UBLK_IO_RES_OK),
                        Aborted(tag) => (user_data(tag, Step::Fetch), sys::UBLK_IO_RES_ABORT),
                        Complete(tag) => {
                            () = tags.advance(tag, Tag::Owned, Tag::Fetching);
                            return format!("{:?} stopped {}", tags.held, tags.stopped());
                        }
                    };
                    let mut fetched = Vec::new();
                    let woken = tags.on_completions([completion], &mut fetched);

                    format!(
                        "{:?} stopped {}, fetched {fetched:?} woken {woken}",
                        tags.held,
                        tags.stopped()
                    )
                });
                let event = format!("{event:?}");
                out.push(format!("{case:<20}{event:<12}{rendered}"));

                if rendered.starts_with("panicked") {
                    break;
                }
            }
        }
        insta::assert_snapshot!(out.join("\n"), @"
        until the abort     Wake        [Fetching, Fetching] stopped false, fetched [] woken true
        until the abort     Fetched(0)  [Owned, Fetching] stopped false, fetched [0] woken false
        until the abort     Fetched(1)  [Owned, Owned] stopped false, fetched [1] woken false
        until the abort     Complete(0) [Fetching, Owned] stopped false
        until the abort     Aborted(0)  [Aborted, Owned] stopped false, fetched [] woken false
        until the abort     Complete(1) [Aborted, Fetching] stopped false
        until the abort     Aborted(1)  [Aborted, Aborted] stopped true, fetched [] woken false
        completed twice     Fetched(1)  [Fetching, Owned] stopped false, fetched [1] woken false
        completed twice     Complete(1) [Fetching, Fetching] stopped false
        completed twice     Complete(1) panicked: assertion `left == right` failed: tag 1 cannot move to Fetching
        fetched while owned Fetched(0)  [Owned, Fetching] stopped false, fetched [0] woken false
        fetched while owned Fetched(0)  panicked: assertion `left == right` failed: tag 0 cannot move to Owned
        aborted while owned Fetched(0)  [Owned, Fetching] stopped false, fetched [0] woken false
        aborted while owned Aborted(0)  panicked: assertion `left == right` failed: tag 0 cannot move to Aborted
        ");
    }

    #[test]
    fn test_a_descriptor_decodes_to_the_blocks_it_covers() {
        let sectors_per_block = crate::BLOCK_SIZE as u64 / sys::SECTOR_SIZE;
        let mut out = Vec::new();

        for (op, named, block, blocks) in [
            (sys::UBLK_IO_OP_READ, "READ", 0, 1),
            (sys::UBLK_IO_OP_WRITE, "WRITE", 3, 2),
            (sys::UBLK_IO_OP_DISCARD, "DISCARD", 8, 8),
            (sys::UBLK_IO_OP_WRITE_ZEROES, "WRITE_ZEROES", 15, 1),
            // A flush, which the device never advertises a cache for.
            (2, "FLUSH", 0, 0),
            // Narrowed to 32 bits first, this request would wrap onto block 0.
            (sys::UBLK_IO_OP_READ, "READ", 1 << 32, 1),
        ] {
            let mut desc = sys::UblksrvIoDesc {
                op_flags: op as u32,
                start_sector: block * sectors_per_block,
                ..Default::default()
            };
            desc.__bindgen_anon_1.nr_sectors = blocks * sectors_per_block as u32;

            let decoded = outcome(|| format!("{:?}", decode(&desc)));
            out.push(format!(
                "{named:<14}block {block:<12}count {blocks:<4}{decoded}"
            ));
        }
        insta::assert_snapshot!(out.join("\n"), @"
        READ          block 0           count 1   Read(0..1)
        WRITE         block 3           count 2   Write(3..5)
        DISCARD       block 8           count 8   Punch(8..16)
        WRITE_ZEROES  block 15          count 1   Punch(15..16)
        FLUSH         block 0           count 0   panicked: device request has op 2, which this device does not serve
        READ          block 4294967296  count 1   panicked: device request covers blocks 4294967296..4294967297, beyond any device
        ");
    }
}
