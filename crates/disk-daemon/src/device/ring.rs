//! The `io_uring` one owner drives, and the encoding by which a completion names
//! the request and step it belongs to.
//!
//! The ring carries the queue's fetch and commit commands, and the read which wakes
//! the owner. Everything else a request does, from its data copies through the
//! character device to its image I/O, is a blocking call on the owner's thread, per
//! `request.rs`. What the ring buys is one wait for everything which can wake the
//! owner, and commits and fetches which travel in batches.
//!
//! Every operation an owner issues goes through the backlog, so a full submission
//! queue costs a later trip around the loop rather than a dropped operation. Each
//! tag has one fetch in flight whenever the owner is not holding its request, and
//! the kernel aborts every one of them once the device has stopped.
//!
//! None of it needs `io_uring`'s worker threads: the driver holds the fetches, and
//! the wake is polled. An operation handed to one would run in a pool which belongs
//! to the owner's thread, at the kernel's default size, and which nothing caps
//! across disks.

use super::Owner;
use crate::ublk::sys;

/// A disk holds at most one operation per queue tag, plus its wake. The backlog
/// absorbs any overflow. Each disk has a ring of its own, so this leaves headroom
/// over [`crate::ublk::QUEUE_DEPTH`] without being lavish about it.
pub(super) const RING_ENTRIES: u32 = 128;

pub(super) type Backlog = std::collections::VecDeque<io_uring::squeue::Entry>;

/// Which operation of a request a completion belongs to.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub(super) enum Step {
    Wake = 0,
    Fetch = 1,
}

pub(super) fn user_data(tag: u16, step: Step) -> u64 {
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

impl Owner {
    /// Arm the wake, and put a fetch in flight for every tag.
    ///
    /// This must run on the thread which will serve the disk. `ublk` binds the
    /// queue to whichever thread issues its first fetch.
    pub(super) fn arm(&mut self) -> std::io::Result<()> {
        self.arm_wake();

        for tag in 0..self.slots.len() as u16 {
            let entry = self.io_command(tag, sys::UBLK_U_IO_FETCH_REQ, 0);
            self.backlog.push_back(entry);
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
            self.advance(tag, step, result);
        }
    }
}

#[cfg(test)]
mod test {
    use super::{Step, parse_user_data, user_data};

    #[test]
    fn test_user_data_round_trips() {
        for (tag, step) in [(0, Step::Wake), (15, Step::Fetch), (u16::MAX, Step::Fetch)] {
            assert_eq!(parse_user_data(user_data(tag, step)), (tag, step));
        }
    }
}
