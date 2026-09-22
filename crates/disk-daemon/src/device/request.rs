//! The path one device request takes, from the fetch which hands its tag over to
//! the completion which hands the tag back.
//!
//! A read is served from the image and then copied to the character device. A write
//! takes its data from the character device, is captured, and is then applied to the
//! image. A discard or write-zeroes carries no data at all: it is captured as a punch
//! and applied as one. Whichever it is, the tag's [`Slot`] holds what the next step
//! needs, and `complete` returns it to the kernel and re-arms its fetch.

use super::Owner;
use super::ring::{Step, transferred, user_data};
use crate::proto::Chunk;
use crate::ublk::{self, sys};

/// What an owner holds for one device request tag, and which phase of that
/// request's path the tag is on.
///
/// The phases are one value rather than a bag of fields because the pinning
/// discipline turns on them: a buffer which an outstanding SQE addresses must not
/// be dropped before that SQE's completion is reaped. A slot therefore returns to
/// [`Slot::Idle`] in exactly two places. [`Owner::complete`] runs on a completion,
/// which is after the last of a request's operations. The `Release` command resets
/// the *parked* tags, whose mutations the capture channel refused before any image
/// operation was submitted for them.
///
/// Moving a slot from one phase to the next is safe wherever its buffer moves with
/// it. `Vec` and `Bytes` are handles: moving one leaves its heap allocation, which
/// is what an SQE addresses, exactly where it was.
pub(super) enum Slot {
    /// No request holds this tag.
    Idle,
    /// A device read. `buf` takes the image content, and then the same buffer is
    /// handed to the character device.
    Reading {
        range: std::ops::Range<u32>,
        buf: Vec<u8>,
    },
    /// A device write, whose data is being taken from the character device into
    /// `buf`.
    Receiving {
        range: std::ops::Range<u32>,
        buf: Vec<u8>,
    },
    /// A mutation the capture channel refused, because it was full or because a
    /// prepare had closed admission. It is offered again in arrival order.
    Parked {
        range: std::ops::Range<u32>,
        /// The write's data, and empty for a punch, which carries none.
        data: bytes::Bytes,
        /// Chunks the capture channel has not accepted yet.
        chunks: Vec<Chunk>,
    },
    /// A captured mutation, waiting on an overlapping mutation in flight or on the
    /// image write or punch which applies it.
    Admitted {
        range: std::ops::Range<u32>,
        /// The write's data, which the image write reads from, and empty for a
        /// punch.
        data: bytes::Bytes,
    },
}

impl Slot {
    /// Blocks the request at this tag covers.
    pub(super) fn range(&self) -> std::ops::Range<u32> {
        match self {
            Slot::Idle => panic!("an idle tag holds no request"),
            Slot::Reading { range, .. }
            | Slot::Receiving { range, .. }
            | Slot::Parked { range, .. }
            | Slot::Admitted { range, .. } => range.clone(),
        }
    }
}

impl Owner {
    /// Advance the request at `tag`, whose `step` completed with `result`.
    pub(super) fn advance(&mut self, tag: u16, step: Step, result: i32) {
        match step {
            Step::Wake => unreachable!("a wake is handled by the reap"),

            // A negative fetch tells the owner that the kernel has aborted the
            // queue. The kernel does that when the device stops.
            Step::Fetch if result < 0 => self.stopping = true,
            Step::Fetch => self.begin(tag),

            Step::ImageRead => {
                let Slot::Reading { buf, .. } = &self.slots[tag as usize] else {
                    panic!("only a reading tag reads the image");
                };

                match transferred(result, buf.len()) {
                    Err(err) => self.fail(tag, "reading the image", err),
                    Ok(()) => {
                        let entry = self.write_device(tag);
                        self.submit(entry);
                    }
                }
            }
            Step::DeviceWrite => {
                let Slot::Reading { buf, .. } = &self.slots[tag as usize] else {
                    panic!("only a reading tag hands data to the device");
                };
                let bytes = buf.len();

                match transferred(result, bytes) {
                    Err(err) => self.fail(tag, "handing read data to the device", err),
                    Ok(()) => self.complete(tag, bytes as i32),
                }
            }
            Step::DeviceRead => {
                let Slot::Receiving { range, buf } = &mut self.slots[tag as usize] else {
                    panic!("only a receiving tag takes data from the device");
                };

                match transferred(result, buf.len()) {
                    Err(err) => self.fail(tag, "taking write data from the device", err),
                    Ok(()) => {
                        let range = range.clone();
                        let data = bytes::Bytes::from(std::mem::take(buf));
                        let chunks = crate::chunk::encode_write(range.start, &data);

                        self.offer(tag, range, data, chunks);
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
        match sys::io_desc_op(&desc) {
            sys::UBLK_IO_OP_READ => {
                self.slots[tag as usize] = Slot::Reading {
                    range,
                    buf: vec![0; bytes as usize],
                };
                let entry = self.read_image(tag);
                self.submit(entry);
            }
            sys::UBLK_IO_OP_WRITE => {
                self.slots[tag as usize] = Slot::Receiving {
                    range,
                    buf: vec![0; bytes as usize],
                };
                let entry = self.read_device(tag);
                self.submit(entry);
            }
            // Both deallocate, per `chunk::encode_punch`. A punch carries no data,
            // which is how `mutate` tells the two apart.
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

    /// Submit the image write or punch of `tag`.
    pub(super) fn mutate(&mut self, tag: u16) {
        let fd = io_uring::types::Fd(std::os::fd::AsRawFd::as_raw_fd(self.image.file()));

        let Slot::Admitted { range, data } = &self.slots[tag as usize] else {
            panic!("only an admitted tag has a mutation to apply");
        };
        let offset = self.image.offset(range.start);

        // A punch is the request which carries no data.
        let entry = if data.is_empty() {
            let bytes = (range.end - range.start) as u64 * crate::BLOCK_SIZE as u64;

            io_uring::opcode::Fallocate::new(fd, bytes)
                .offset(offset)
                .mode(crate::image::PUNCH_MODE)
                .build()
                .user_data(user_data(tag, Step::ImagePunch))
        } else {
            io_uring::opcode::Write::new(fd, data.as_ptr(), data.len() as u32)
                .offset(offset)
                .build()
                .user_data(user_data(tag, Step::ImageWrite))
        };
        self.submit(entry);
    }

    fn finish_mutation(&mut self, tag: u16, step: Step, result: i32) {
        let Slot::Admitted { range, data } = &self.slots[tag as usize] else {
            panic!("only an admitted tag completes an image mutation");
        };
        let range = range.clone();

        let expected = match step {
            Step::ImageWrite => data.len(),
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

    fn read_image(&mut self, tag: u16) -> io_uring::squeue::Entry {
        let fd = io_uring::types::Fd(std::os::fd::AsRawFd::as_raw_fd(self.image.file()));
        let offset = self.image.offset(self.slots[tag as usize].range().start);

        let Slot::Reading { buf, .. } = &mut self.slots[tag as usize] else {
            panic!("only a reading tag reads the image");
        };
        let (buf, len) = (buf.as_mut_ptr(), buf.len() as u32);

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

        let Slot::Reading { buf, .. } = &self.slots[tag as usize] else {
            panic!("only a reading tag hands data to the device");
        };
        let (buf, len) = (buf.as_ptr(), buf.len() as u32);

        io_uring::opcode::Write::new(fd, buf, len)
            .offset(offset)
            .build()
            .user_data(user_data(tag, Step::DeviceWrite))
    }

    /// Take a write's incoming data from the character device.
    fn read_device(&mut self, tag: u16) -> io_uring::squeue::Entry {
        let fd = io_uring::types::Fd(std::os::fd::AsRawFd::as_raw_fd(&self.cdev));
        let offset = sys::io_buf_offset(ublk::QUEUE_ID, tag);

        let Slot::Receiving { buf, .. } = &mut self.slots[tag as usize] else {
            panic!("only a receiving tag takes data from the device");
        };
        let (buf, len) = (buf.as_mut_ptr(), buf.len() as u32);

        io_uring::opcode::Read::new(fd, buf, len)
            .offset(offset)
            .build()
            .user_data(user_data(tag, Step::DeviceRead))
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
