use super::chunk;
use super::image;
use super::ublk;
use anyhow::Context;

/// Serve device `dev_id` through its character device `cdev`, from the rebuilt
/// `image`, sending its live chunks to `live_tx`.
pub(super) fn spawn(
    dev_id: u32,
    cdev: std::fs::File,
    image: image::Image,
    live_tx: tokio::sync::mpsc::Sender<anyhow::Result<crate::LiveChunk>>,
) -> anyhow::Result<std::thread::JoinHandle<anyhow::Result<()>>> {
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();

    let thread = std::thread::Builder::new()
        .name("disk-owner".to_string())
        .spawn(move || run(dev_id, cdev, image, live_tx, ready_tx))
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

/// The owner thread: arm the queue, then serve requests from `image` until the
/// kernel stops the device, or the image or a data copy fails.
fn run(
    dev_id: u32,
    cdev: std::fs::File,
    mut image: image::Image,
    live_tx: tokio::sync::mpsc::Sender<anyhow::Result<crate::LiveChunk>>,
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

    // Carries only ublk commands. The queue's one tag has at most one command
    // queued or in flight.
    let mut ring = io_uring::IoUring::new(1)
        .with_context(|| format!("creating the ring of device {dev_id}"))?;

    let descriptors = ublk::IoDescs::map(&cdev)
        .with_context(|| format!("mapping the request descriptors of device {dev_id}"))?;

    push(&mut ring, ublk::fetch(&cdev));
    // A submission may take fewer entries than are queued, so run until all
    // entries are submitted.
    while !ring.submission().is_empty() {
        _ = ring
            .submit()
            .with_context(|| format!("arming the queue of device {dev_id}"))?;
    }
    _ = ready_tx.send(());

    match serve(dev_id, &cdev, &mut ring, &descriptors, &mut image, &live_tx) {
        Ok(()) => Ok(()),
        Err(err) => match live_tx.blocking_send(Err(err)) {
            Ok(()) => Ok(()),
            Err(tokio::sync::mpsc::error::SendError(Err(err))) => Err(err),
            Err(tokio::sync::mpsc::error::SendError(Ok(_))) => {
                unreachable!("only the failure was sent")
            }
        },
    }
}

/// Serve requests from `image` until the kernel stops the device, or the image
/// or a data copy fails.
fn serve(
    dev_id: u32,
    cdev: &std::fs::File,
    ring: &mut io_uring::IoUring,
    descriptors: &ublk::IoDescs,
    image: &mut image::Image,
    live_tx: &tokio::sync::mpsc::Sender<anyhow::Result<crate::LiveChunk>>,
) -> anyhow::Result<()> {
    // Buffer serving reads. Sized to the largest request.
    let mut read_buf = vec![0; ublk::MAX_IO_BUF_BYTES as usize];

    loop {
        // Submit the answer that the last turn queued, and wait for a request.
        // A signal can end the wait before anything completes. The kernel then
        // returns EINTR or, if this call also submitted the answer, its count as
        // though it had waited. Neither is a failure: this turn handles whatever
        // has completed, possibly nothing, and the next turn waits again.
        if let Err(err) = ring.submit_and_wait(1)
            && err.kind() != std::io::ErrorKind::Interrupted
        {
            return Err(anyhow::Error::new(err).context("waiting for device requests"));
        }

        let Some(result) = ring.completion().next().map(|cqe| cqe.result()) else {
            continue;
        };
        match result {
            ublk::UBLK_IO_RES_OK => (),
            ublk::UBLK_IO_RES_ABORT => return Ok(()),
            result => panic!(
                "device {dev_id} refused a command: {}",
                std::io::Error::from_raw_os_error(-result),
            ),
        }

        let offset = ublk::UBLKSRV_IO_BUF_OFFSET;
        let (chunk, transferred) = match descriptors.get() {
            ublk::Op::Read(range) => {
                // Read from the image and write what was read to the
                // character device.
                let len = range.len() * crate::BLOCK_SIZE as usize;
                () = image.read(range.start, &mut read_buf[..len])?;
                () = std::os::unix::fs::FileExt::write_all_at(cdev, &read_buf[..len], offset)
                    .context("handing read data to the device")?;

                (None, len as i32)
            }
            ublk::Op::Write(range) => {
                // Read from the character device and write what was read to
                // the image.
                let len = range.len() * crate::BLOCK_SIZE as usize;
                let mut data = vec![0; len];
                () = std::os::unix::fs::FileExt::read_exact_at(cdev, &mut data, offset)
                    .context("taking write data from the device")?;

                (
                    Some(chunk::encode_write(range.start, data.into())),
                    len as i32,
                )
            }
            ublk::Op::Punch(range) => {
                // Nothing to read or write; just hole punch the image.
                (
                    Some(chunk::encode_punch(range.start, range.len() as u32)),
                    0,
                )
            }
        };

        if let Some(chunk) = chunk {
            // Sent before the request is answered, so a commit's syncfs
            // waits for its writes to reach the attachment.
            for live in image.serve(chunk)? {
                // Fails only once the attachment drops its receiver, as at
                // teardown.
                _ = live_tx.blocking_send(Ok(live));
            }
        }

        push(ring, ublk::commit_and_fetch(cdev, transferred));
    }
}

fn push(ring: &mut io_uring::IoUring, entry: io_uring::squeue::Entry) {
    // SAFETY: no entry addresses memory. A command's bytes travel inside the
    // entry.
    unsafe { ring.submission().push(&entry) }.expect("the ring has room for the tag's command");
}
