use super::chunk;
use super::target;
use super::ublk;
use anyhow::Context;

/// Serve device `dev_id` through its character device `cdev`, from the rebuilt
/// `target`, sending its live chunks to `served_tx`.
pub(super) fn spawn(
    dev_id: u32,
    cdev: std::fs::File,
    target: target::Target,
    served_tx: tokio::sync::mpsc::Sender<super::LiveChunk>,
) -> anyhow::Result<std::thread::JoinHandle<anyhow::Result<()>>> {
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();

    let thread = std::thread::Builder::new()
        .name("disk-owner".to_string())
        .spawn(move || run(dev_id, cdev, target, served_tx, ready_tx))
        .with_context(|| format!("spawning the owner of device {dev_id}"))?;

    if let Err(std::sync::mpsc::RecvError) = ready_rx.recv() {
        return Err(match thread.join() {
            Ok(Err(err)) => err,
            Ok(Ok(())) => panic!("the owner of device {dev_id} returned without serving it"),
            Err(_panic) => anyhow::anyhow!("the owner of device {dev_id} panicked"),
        });
    }
    Ok(thread)
}

/// The owner thread: arm the queue, then serve requests from `target` until the
/// kernel stops the device, or the image fails.
fn run(
    dev_id: u32,
    cdev: std::fs::File,
    mut target: target::Target,
    served_tx: tokio::sync::mpsc::Sender<super::LiveChunk>,
    ready_tx: std::sync::mpsc::Sender<()>,
) -> anyhow::Result<()> {
    // Names the device in every event the owner logs.
    let _span = tracing::info_span!("device", dev_id).entered();

    // Mark this thread part of the disk's I/O path, so its allocations never
    // wait on I/O and its writes are throttled only by the image's host device.
    // Otherwise it could block on writeback of dirty pages which only it can
    // clean.
    () = rustix::process::configure_io_flusher_behavior(true)
        .context("marking a disk's owner an I/O flusher, which requires CAP_SYS_RESOURCE")?;

    // Carries only ublk commands. Room for one command per tag, the most this
    // queue ever has queued or in flight. io_uring sizes its rings in powers of
    // two, and makes its completion ring twice the size of its submission ring.
    let mut ring = io_uring::IoUring::new((ublk::QUEUE_DEPTH as u32).next_power_of_two())
        .with_context(|| format!("creating the ring of device {dev_id}"))?;

    // Kernel request descriptors, indexed by tag.
    let descriptors = ublk::IoDescs::map(&cdev, ublk::QUEUE_ID, ublk::QUEUE_DEPTH)
        .with_context(|| format!("mapping the request descriptors of device {dev_id}"))?;

    for tag in 0..ublk::QUEUE_DEPTH {
        push_command(&mut ring, &cdev, tag, ublk::UBLK_U_IO_FETCH_REQ, 0);
    }
    // A submission may take fewer entries than are queued, so run until all
    // entries are submitted.
    while !ring.submission().is_empty() {
        _ = ring
            .submit()
            .with_context(|| format!("arming the queue of device {dev_id}"))?;
    }
    _ = ready_tx.send(());

    // Buffer serving reads. Sized to the largest request.
    let mut read_buf = vec![0; ublk::MAX_IO_BUF_BYTES as usize];

    // The kernel aborts every tag once the device has stopped and every request
    // has completed.
    let mut aborted = 0;
    while aborted < ublk::QUEUE_DEPTH {
        // An interrupt is not a failure: this turn handles whatever has
        // completed (possibly nothing) and the next one waits again.
        if let Err(err) = ring.submit_and_wait(1)
            && err.kind() != std::io::ErrorKind::Interrupted
        {
            return Err(anyhow::Error::new(err).context("waiting for device requests"));
        }

        // `push_command` sets each command's `user_data` to its tag.
        let completions: Vec<(u16, i32)> = ring
            .completion()
            .map(|cqe| (cqe.user_data() as u16, cqe.result()))
            .collect();

        for (tag, result) in completions {
            match result {
                ublk::UBLK_IO_RES_OK => (),
                ublk::UBLK_IO_RES_ABORT => {
                    aborted += 1;
                    continue;
                }
                result => panic!(
                    "device {dev_id} refused the command of tag {tag}: {}",
                    std::io::Error::from_raw_os_error(-result),
                ),
            }

            let offset = ublk::io_buf_offset(ublk::QUEUE_ID, tag);
            let desc = descriptors.get(tag);

            let (chunk, transferred) = match Op::decode(&desc) {
                Op::Read(range) => {
                    // Read from the image and write what was read to the
                    // character device.
                    let len = range.len() * crate::BLOCK_SIZE as usize;
                    () = target.read(range.start, &mut read_buf[..len])?;

                    let copied =
                        std::os::unix::fs::FileExt::write_all_at(&cdev, &read_buf[..len], offset)
                            .map(|()| len as u32)
                            .context("handing read data to the device");
                    (None, copied)
                }
                Op::Write(range) => {
                    // Read from the character device and write what was read to
                    // the image.
                    let len = range.len() * crate::BLOCK_SIZE as usize;
                    let mut data = vec![0; len];

                    match std::os::unix::fs::FileExt::read_exact_at(&cdev, &mut data, offset) {
                        Ok(()) => (
                            Some(chunk::encode_write(range.start, &data.into())),
                            Ok(len as u32),
                        ),
                        Err(err) => (
                            None,
                            Err(anyhow::Error::new(err)
                                .context("taking write data from the device")),
                        ),
                    }
                }
                Op::Punch(range) => {
                    // Nothing to read or write; just hole punch the image.
                    (
                        Some(chunk::encode_punch(range.start, range.len() as u32)),
                        Ok(0),
                    )
                }
            };

            if let Some(chunk) = chunk {
                // Sent before the request is answered, so a commit's syncfs
                // waits for its writes to reach the journal writer.
                for live in target.serve(chunk)? {
                    // Fails only once the writer is gone, as at teardown.
                    _ = served_tx.blocking_send(live);
                }
            }

            let result = match transferred {
                Ok(bytes) => bytes as i32,
                Err(err) => {
                    tracing::error!(tag, ?err, "device request failed");
                    -libc::EIO
                }
            };

            // Answer the request on this tag and arm it for its next
            // request.
            push_command(
                &mut ring,
                &cdev,
                tag,
                ublk::UBLK_U_IO_COMMIT_AND_FETCH_REQ,
                result,
            );
        }
    }
    Ok(())
}

fn push_command(
    ring: &mut io_uring::IoUring,
    cdev: &std::fs::File,
    tag: u16,
    cmd_op: u32,
    result: i32,
) {
    let bytes = ublk::io_cmd(ublk::QUEUE_ID, tag, result);

    let entry = io_uring::opcode::UringCmd16::new(
        io_uring::types::Fd(std::os::fd::AsRawFd::as_raw_fd(cdev)),
        cmd_op,
    )
    .cmd(bytes)
    .build()
    .user_data(tag as u64);

    // SAFETY: no entry addresses memory. A command's bytes travel inside the
    // entry.
    unsafe { ring.submission().push(&entry) }.expect("the ring has room for an operation per tag");
}

#[derive(Debug, PartialEq)]
enum Op {
    Read(std::ops::Range<u32>),
    Write(std::ops::Range<u32>),
    Punch(std::ops::Range<u32>),
}

impl Op {
    fn decode(desc: &ublk::UblksrvIoDesc) -> Self {
        // Determine the starting and end block of the data, relative to the
        // `/dev/ublkbN` block device, which our image mirrors.
        let sectors_per_block = crate::BLOCK_SIZE as u64 / ublk::SECTOR_SIZE;
        let start = desc.start_sector / sectors_per_block;
        let end = start + ublk::io_desc_sectors(desc) as u64 / sectors_per_block;
        let range = start as u32..end as u32;

        // Map to the ublk operation.
        match ublk::io_desc_op(desc) {
            ublk::UBLK_IO_OP_READ => Op::Read(range),
            ublk::UBLK_IO_OP_WRITE => Op::Write(range),
            ublk::UBLK_IO_OP_DISCARD | ublk::UBLK_IO_OP_WRITE_ZEROES => Op::Punch(range),
            op => panic!("device request has op {op}, which this device does not serve"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::Op;
    use crate::device::ublk;

    #[test]
    fn test_a_descriptor_decodes_to_the_blocks_it_covers() {
        let sectors_per_block = crate::BLOCK_SIZE as u64 / ublk::SECTOR_SIZE;

        // (operation and any flags, first block, blocks, decoded)
        let cases = [
            (ublk::UBLK_IO_OP_READ as u32, 0, 1, Op::Read(0..1)),
            (ublk::UBLK_IO_OP_WRITE as u32, 3, 2, Op::Write(3..5)),
            (ublk::UBLK_IO_OP_DISCARD as u32, 8, 8, Op::Punch(8..16)),
            (
                ublk::UBLK_IO_OP_WRITE_ZEROES as u32,
                15,
                1,
                Op::Punch(15..16),
            ),
            // A metadata read, as ext4 issues: UBLK_IO_F_META sits above the op.
            (ublk::UBLK_IO_OP_READ as u32 | 1 << 11, 2, 1, Op::Read(2..3)),
        ];
        for (op_flags, block, blocks, decoded) in cases {
            let mut desc = ublk::UblksrvIoDesc {
                op_flags,
                start_sector: block * sectors_per_block,
                ..Default::default()
            };
            desc.__bindgen_anon_1.nr_sectors = blocks * sectors_per_block as u32;

            assert_eq!(Op::decode(&desc), decoded);
        }
    }
}
