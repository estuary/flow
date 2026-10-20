use super::ublk::{self, sys};
use anyhow::Context;

/// One device request.
pub(super) enum Request<'a> {
    /// Fill `buf` with the blocks from `start`.
    Read { start: u32, buf: &'a mut [u8] },
    /// A write of `data` from block `start`, taken from the device.
    Write { start: u32, data: bytes::Bytes },
    /// A discard or write-zeroes.
    Punch(std::ops::Range<u32>),
}

/// One disk's queue.
pub(super) struct Queue {
    /// Kernel device ID.
    dev_id: u32,
    /// Carries only ublk commands, each of whose `user_data` is its tag.
    ring: io_uring::IoUring,
    /// Character device used for request data copies.
    cdev: std::fs::File,
    /// Kernel request descriptors, indexed by tag.
    descriptors: ublk::IoDescs,
    /// Number of tags in the queue.
    depth: u16,
    /// Buffer serving reads. Sized to the largest request.
    read_buf: Vec<u8>,
}

impl Queue {
    /// Initialize a new `Queue`.
    pub fn new(dev_id: u32, cdev: std::fs::File, depth: u16) -> std::io::Result<Self> {
        let mut queue = Self {
            dev_id,
            // Room for one command per tag, the most this queue ever has queued
            // or in flight. io_uring sizes its rings in powers of two, and
            // makes its completion ring twice the size of its submission ring.
            ring: io_uring::IoUring::new((depth as u32).next_power_of_two())?,
            descriptors: ublk::IoDescs::map(&cdev, ublk::QUEUE_ID, depth)?,
            cdev,
            depth,
            read_buf: vec![0; ublk::MAX_IO_BUF_BYTES as usize],
        };

        for tag in 0..depth {
            queue.push_command(tag, sys::UBLK_U_IO_FETCH_REQ, 0);
        }
        // A submission may take fewer entries than are queued, so run until all
        // entries are submitted.
        while !queue.ring.submission().is_empty() {
            queue.ring.submit()?;
        }
        Ok(queue)
    }

    /// Serve every request through `handle`, until the kernel stops the device.
    pub fn serve(
        mut self,
        mut handle: impl FnMut(Request) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        // The kernel aborts every tag once the device has stopped and every
        // request has completed.
        let mut aborted = 0;
        while aborted < self.depth {
            // An interrupt is not a failure: this turn handles whatever has
            // completed (possibly nothing) and the next one waits again.
            if let Err(err) = self.ring.submit_and_wait(1)
                && err.kind() != std::io::ErrorKind::Interrupted
            {
                return Err(anyhow::Error::new(err).context("waiting for device requests"));
            }

            // Each completion is a `(tag, result)` tuple:
            //
            // - `tag` comes from `user_data()`, which we set to the tag when we
            // queued the command. It's the kernel's request slot this
            // completion is about.
            // - `result` is set by the ublk driver: `UBLK_IO_RES_OK` when the
            // tag now holds a request, or `UBLK_IO_RES_ABORT` once the device
            // stops. Anything else is the kernel refusing a command we built.
            //
            // Further down, the tag's descriptor is decoded into an operation.
            // It's read from memory the kernel shares with us, which we mmap
            // from the ublk character device (`/dev/ublkcN`). This is what ends
            // up saying what we actually need to do to satisfy the request.
            let completions: Vec<(u16, i32)> = self
                .ring
                .completion()
                .map(|cqe| (cqe.user_data() as u16, cqe.result()))
                .collect();

            for (tag, result) in completions {
                match result {
                    sys::UBLK_IO_RES_OK => (),
                    sys::UBLK_IO_RES_ABORT => {
                        aborted += 1;
                        continue;
                    }
                    result => panic!(
                        "device {} refused the command of tag {tag}: {}",
                        self.dev_id,
                        std::io::Error::from_raw_os_error(-result),
                    ),
                }

                // Offset in the character device for this tag's data, which is
                // where to point a `pread` or `pwrite`. For a write, read the
                // character device at that offset and write that data to our
                // image; and for a read, write it at that offset after reading
                // it from our image. The read/write thing seems kind of
                // backward at first but make sense when you think about it.
                let offset = sys::io_buf_offset(ublk::QUEUE_ID, tag);

                // Read this tag's request descriptor from the descriptor array,
                // which is mmap'd from the character device. It says what to do
                // (read, write, or punch), how many sectors, and where on the
                // image they start.
                let desc = self.descriptors.get(tag);

                let transferred = match Op::decode(&desc) {
                    Op::Read(range) => {
                        // Read from the image and write what was read to the
                        // character device.
                        let len = range.len() * crate::BLOCK_SIZE as usize;
                        () = handle(Request::Read {
                            start: range.start,
                            buf: &mut self.read_buf[..len],
                        })?;

                        std::os::unix::fs::FileExt::write_all_at(
                            &self.cdev,
                            &self.read_buf[..len],
                            offset,
                        )
                        .map(|()| len as u32)
                        .context("handing read data to the device")
                    }
                    Op::Write(range) => {
                        // Read from the character device and write what was
                        // read to the image.
                        let len = range.len() * crate::BLOCK_SIZE as usize;
                        let mut data = vec![0; len];

                        match std::os::unix::fs::FileExt::read_exact_at(
                            &self.cdev, &mut data, offset,
                        ) {
                            Ok(()) => {
                                () = handle(Request::Write {
                                    start: range.start,
                                    data: data.into(),
                                })?;
                                Ok(len as u32)
                            }
                            Err(err) => Err(anyhow::Error::new(err)
                                .context("taking write data from the device")),
                        }
                    }
                    Op::Punch(range) => {
                        // Nothing to read or write; just hole punch the image.
                        () = handle(Request::Punch(range))?;
                        Ok(0)
                    }
                };

                let result = match transferred {
                    Ok(bytes) => bytes as i32,
                    Err(err) => {
                        tracing::error!(tag, ?err, "device request failed");
                        -libc::EIO
                    }
                };

                // Answer the requst on this tag and arm it for its next
                // request.
                self.push_command(tag, sys::UBLK_U_IO_COMMIT_AND_FETCH_REQ, result);
            }
        }
        Ok(())
    }

    fn push_command(&mut self, tag: u16, cmd_op: u32, result: i32) {
        let command = sys::io_cmd(ublk::QUEUE_ID, tag, result);
        // SAFETY: `UblksrvIoCmd` is `repr(C)` and its 16 bytes are fully
        // occupied by its fields, so the copy reads no padding.
        let bytes = unsafe { sys::cmd_bytes::<_, 16>(&command) };

        let entry = io_uring::opcode::UringCmd16::new(
            io_uring::types::Fd(std::os::fd::AsRawFd::as_raw_fd(&self.cdev)),
            cmd_op,
        )
        .cmd(bytes)
        .build()
        .user_data(tag as u64);

        // SAFETY: no entry addresses memory. A command's bytes travel inside
        // the entry.
        unsafe { self.ring.submission().push(&entry) }
            .expect("the ring has room for an operation per tag");
    }
}

#[derive(Debug, PartialEq)]
enum Op {
    Read(std::ops::Range<u32>),
    Write(std::ops::Range<u32>),
    Punch(std::ops::Range<u32>),
}

impl Op {
    fn decode(desc: &sys::UblksrvIoDesc) -> Self {
        // Determine the staring and end block of the data, relative to the
        // `/dev/ublkbN` block device, which our image mirrors.
        let sectors_per_block = crate::BLOCK_SIZE as u64 / sys::SECTOR_SIZE;
        let start = desc.start_sector / sectors_per_block;
        let end = start + sys::io_desc_sectors(desc) as u64 / sectors_per_block;
        let range = start as u32..end as u32;

        // Map to the ublk operation.
        match sys::io_desc_op(desc) {
            sys::UBLK_IO_OP_READ => Op::Read(range),
            sys::UBLK_IO_OP_WRITE => Op::Write(range),
            sys::UBLK_IO_OP_DISCARD | sys::UBLK_IO_OP_WRITE_ZEROES => Op::Punch(range),
            op => panic!("device request has op {op}, which this device does not serve"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::Op;
    use crate::device::ublk::sys;

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
        ] {
            let mut desc = sys::UblksrvIoDesc {
                op_flags: op as u32,
                start_sector: block * sectors_per_block,
                ..Default::default()
            };
            desc.__bindgen_anon_1.nr_sectors = blocks * sectors_per_block as u32;

            let decoded = outcome(|| format!("{:?}", Op::decode(&desc)));
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
        ");
    }
}
