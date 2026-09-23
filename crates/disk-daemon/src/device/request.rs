//! The path one device request takes, from the fetch which hands its tag over to
//! the completion which hands the tag back.
//!
//! A read is served from the image and then copied to the character device. A write
//! takes its data from the character device, is captured, and is then applied to the
//! image. A discard or write-zeroes carries no data at all: it is captured as a punch
//! and applied as one. `complete` returns the tag to the kernel and re-arms its fetch.
//!
//! Every step between the fetch and the commit is a blocking call on the owner's
//! thread: the copies through the character device, and the image's reads and writes
//! alike. Only the fetch and the commit ride the ring, which batches them. A mutation
//! is applied the moment the capture channel accepts it, so the image takes mutations
//! in exactly the order the journal does, with nothing to track in between. The
//! price is that a slow call into the host filesystem holds up every other request of
//! the disk while it runs, which the crate README weighs.

use super::Owner;
use super::ring::{Step, user_data};
use crate::proto::Chunk;
use crate::ublk::{self, sys};

/// What an owner holds for one device request tag.
///
/// Serving a request runs to completion on the owner's thread, so a tag holds
/// nothing while that happens. It holds a mutation only while the capture channel
/// refuses it. No ring operation ever addresses a slot, so one may be replaced at
/// any time.
pub(super) enum Slot {
    /// No mutation waits at this tag.
    Idle,
    /// A mutation the capture channel refused, because it was full or because a
    /// prepare had closed admission. It is offered again in arrival order.
    Parked {
        range: std::ops::Range<u32>,
        /// The write's data, and empty for a punch, which carries none.
        data: bytes::Bytes,
        /// Chunks the capture channel has not accepted yet.
        chunks: Vec<Chunk>,
    },
}

impl Owner {
    /// Advance the request at `tag`, whose `step` completed with `result`.
    pub(super) fn advance(&mut self, tag: u16, step: Step, result: i32) {
        match step {
            Step::Wake => unreachable!("a wake is handled by the reap"),

            // A negative fetch tells the owner that the kernel has aborted the
            // queue. The kernel does that when the device stops.
            Step::Fetch if result < 0 => self.stopping = true,
            Step::Fetch => self.serve(tag),
        }
    }

    /// Serve the request the kernel handed over at `tag`.
    fn serve(&mut self, tag: u16) {
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
        // Under `UBLK_F_USER_COPY`, a request's data moves through the character
        // device, at an offset which names the queue and tag.
        let data_offset = sys::io_buf_offset(ublk::QUEUE_ID, tag);

        match sys::io_desc_op(&desc) {
            sys::UBLK_IO_OP_READ => {
                let mut buf = vec![0; bytes as usize];

                if let Err(err) = self.image.read_at(range.start, &mut buf) {
                    return self.fail(tag, "reading the image", err);
                }
                match std::os::unix::fs::FileExt::write_all_at(&self.cdev, &buf, data_offset) {
                    Ok(()) => self.complete(tag, bytes as i32),
                    Err(err) => self.fail(tag, "handing read data to the device", err),
                }
            }
            sys::UBLK_IO_OP_WRITE => {
                let mut buf = vec![0; bytes as usize];

                if let Err(err) =
                    std::os::unix::fs::FileExt::read_exact_at(&self.cdev, &mut buf, data_offset)
                {
                    return self.fail(tag, "taking write data from the device", err);
                }
                let data = bytes::Bytes::from(buf);
                let chunks = crate::chunk::encode_write(range.start, &data);

                self.offer(tag, range, data, chunks);
            }
            // Both deallocate, per `chunk::encode_punch`. A punch carries no data,
            // which is how `apply` tells the two apart.
            sys::UBLK_IO_OP_DISCARD | sys::UBLK_IO_OP_WRITE_ZEROES => {
                let chunks = vec![crate::chunk::encode_punch(
                    range.start,
                    range.end - range.start,
                )];
                self.offer(tag, range, bytes::Bytes::new(), chunks);
            }
            // The device advertises no volatile write cache, so a flush should
            // never arrive. This daemon serves no other kind of block request.
            op => {
                tracing::warn!(dev_id = self.dev_id, op, "unsupported device request");
                self.complete(tag, -libc::EOPNOTSUPP);
            }
        }
    }

    /// Apply the mutation at `tag`, which the capture channel has just accepted, to
    /// the image, and complete its request.
    ///
    /// This is the one place a mutation reaches the image. Because it runs as soon as
    /// the mutation is captured, two overlapping mutations land in the image in the
    /// order they were captured, whatever order their requests arrived in.
    pub(super) fn apply(&mut self, tag: u16, range: std::ops::Range<u32>, data: bytes::Bytes) {
        // A punch is the mutation which carries no data.
        let (applied, result) = match data.is_empty() {
            true => (self.image.punch(range.start, range.end - range.start), 0),
            false => (self.image.write_at(range.start, &data), data.len() as i32),
        };

        match applied {
            Ok(()) => self.complete(tag, result),
            // In practice this is the host filesystem out of space. The capture
            // channel holds the mutation already, so the delta now open holds what
            // the image lacks. The request fails, and so does the next cut, which
            // ends the tenure before that delta can commit. A replay drops a delta
            // which never commits, and the image goes with the tenure.
            Err(err) => {
                () = self
                    .fail_disk(anyhow::Error::new(err).context("applying a mutation to the image"));
                self.complete(tag, -libc::EIO);
            }
        }
    }

    /// Complete `tag` back to the kernel and re-arm its fetch. `result` is the
    /// bytes the request transferred, or a negative errno. The kernel reads a
    /// zero-byte read as an I/O error.
    fn complete(&mut self, tag: u16, result: i32) {
        self.slots[tag as usize] = Slot::Idle;

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

    /// Fail every later cut of this disk with `err`. Only the first failure is
    /// kept, because the tenure ends at the next cut whichever it reports.
    pub(super) fn fail_disk(&mut self, err: anyhow::Error) {
        if self.failed.is_some() {
            return;
        }
        tracing::error!(
            dev_id = self.dev_id,
            ?err,
            "a disk failed, so its next cut will"
        );
        self.failed = Some(err);
    }

    pub(super) fn io_command(&self, tag: u16, cmd_op: u32, result: i32) -> io_uring::squeue::Entry {
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
