//! The `io_uring` one owner drives, and the encoding by which a completion names
//! the request and step it belongs to.
//!
//! Every operation an owner issues goes through the backlog, so a full submission
//! queue costs a later trip around the loop rather than a dropped operation. Buffers
//! addressed by an outstanding entry stay pinned until `reap` sees their completion;
//! `pending` counts them, and the thread does not release the image until it is zero.

use super::Owner;
use crate::ublk::sys;

/// A disk holds at most one operation per queue tag, plus its wake. The backlog
/// absorbs any overflow. Each disk has a ring of its own, so this leaves headroom
/// over [`crate::ublk::QUEUE_DEPTH`] without being lavish about it.
const RING_ENTRIES: u32 = 128;

/// Kernel workers the shared pool may run, as `[bounded, unbounded]`.
///
/// The kernel derives its own values from the CPU count and `RLIMIT_NPROC`, so
/// they differ between hosts of different sizes. These values are fixed instead.
/// They bound the whole process rather than one disk.
const IOWQ_MAX_WORKERS: [u32; 2] = [128, 128];

pub(super) type Backlog = std::collections::VecDeque<io_uring::squeue::Entry>;

/// Which operation of a request a completion belongs to.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub(super) enum Step {
    Wake = 0,
    Fetch = 1,
    ImageRead = 2,
    DeviceWrite = 3,
    DeviceRead = 4,
    ImageWrite = 5,
    ImagePunch = 6,
}

pub(super) fn user_data(tag: u16, step: Step) -> u64 {
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
    /// Arm the wake, and put a fetch in flight for every tag.
    ///
    /// This must run on the thread which will serve the disk. `ublk` binds the
    /// queue to whichever thread issues its first fetch.
    pub(super) fn arm(&mut self) -> std::io::Result<()> {
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

    /// Move as many backlogged submissions into the ring as fit.
    pub(super) fn flush(&mut self) {
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

    pub(super) fn reap(&mut self) {
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

    pub(super) fn submit(&mut self, entry: io_uring::squeue::Entry) {
        self.pending += 1;
        self.backlog.push_back(entry);
    }
}

/// Interpret an `io_uring` result which should have moved `expected` bytes.
pub(super) fn transferred(result: i32, expected: usize) -> Result<(), std::io::Error> {
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
pub(super) fn ring() -> anyhow::Result<io_uring::IoUring> {
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
    // Two disks which start at once may each build one of these. The loser is
    // dropped, and both see the same pool.
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
