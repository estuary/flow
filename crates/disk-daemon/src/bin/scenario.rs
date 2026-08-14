//! Privileged scenarios, run as `sudo -n disk-daemon-scenario <name>` by the
//! crate's tests.
//!
//! Serving a `ublk` device and mounting a filesystem both need real
//! `CAP_SYS_ADMIN`, and cargo must not run as root or the target directory stops
//! being the user's. So the privilege lives in this child process instead. It
//! prints one JSON object of observations on stdout, which the test asserts
//! against, and logs to stderr.
//!
//! `tests/daemon.rs` drives the daemon binary itself, so what ships is what is
//! tested there. These scenarios use the crate as a library instead, which is
//! how they reach what the session protocol does not offer: a queue depth
//! shallow enough to force backpressure, and the digests, allocated counts and
//! extent lists which show that a replayed image matches in holes as well as in
//! bytes.

use disk_daemon::bitmap::Bitmap;
use disk_daemon::capture::Captured;
use disk_daemon::chunk;
use disk_daemon::disk::Disk;
use disk_daemon::image::Image;
use disk_daemon::proto::Chunk;
use disk_daemon::ublk::Control;

/// 128 MiB, which `mkfs.ext4` accepts comfortably and which keeps a scenario to
/// a few seconds.
const BLOCKS: u32 = 32768;
const BLOCK_SIZE: u32 = 4096;

/// Deep enough that no ordinary scenario meets it. The backpressure scenario
/// sets its own.

const MOUNT_OPTIONS: &str = "noatime,nodev,nosuid,noexec,discard";

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_target(false)
        .init();

    let name = std::env::args().nth(1).unwrap_or_default();
    let report = match name.as_str() {
        "lifecycle" => lifecycle(),
        "ext4" => ext4(),
        "discard" => discard(),
        "backpressure" => backpressure(),
        "reap" => reap(),
        other => anyhow::bail!("unknown scenario {other:?}"),
    }?;

    println!("{report}");
    Ok(())
}

/// Create a device, serve it, read from it, and tear it down.
fn lifecycle() -> anyhow::Result<serde_json::Value> {
    let scenario = Scenario::new()?;
    let (mut disk, captured) = scenario.disk(disk_daemon::ublk::QUEUE_DEPTH)?;
    let collector = collect(captured);

    let dev_id = disk.dev_id();
    let block_path = disk.block_path();

    // A fresh device is all holes, so it reads as zeroes.
    let mut head = vec![0xff; BLOCK_SIZE as usize];
    let device = std::fs::File::open(&block_path)?;
    std::os::unix::fs::FileExt::read_exact_at(&device, &mut head, 0)?;
    drop(device);

    let served = serde_json::json!({
        "char": disk_daemon::ublk::char_path(dev_id).exists(),
        "block": block_path.exists(),
        "sys_block": sys_block_path(dev_id).exists(),
    });
    let image = disk.stop()?.expect("the disk was live");
    let mutations = collector.join().expect("collector panicked");

    Ok(serde_json::json!({
        "dir": scenario.dir,
        "dev_id": dev_id,
        "ublks_max": scenario.control.ublks_max(),
        "reads_zeroes": head.iter().all(|&b| b == 0),
        "served": served,
        "torn_down": serde_json::json!({
            "char": disk_daemon::ublk::char_path(dev_id).exists(),
            "block": block_path.exists(),
            "sys_block": sys_block_path(dev_id).exists(),
        }),
        // Reads and a partition scan mutate nothing.
        "mutations": mutations.len(),
        "image": describe(image.file(), image.allocated())?,
    }))
}

/// Format the served device, mount it, write and re-read files across a
/// remount, then replay the captured stream into a second image.
fn ext4() -> anyhow::Result<serde_json::Value> {
    let scenario = Scenario::new()?;
    let (mut disk, captured) = scenario.disk(disk_daemon::ublk::QUEUE_DEPTH)?;
    let collector = collect(captured);

    let block_path = disk.block_path();
    format(&block_path)?;

    let files: Vec<(&str, Vec<u8>)> = vec![
        ("small", pattern(0x11, 11)),
        ("one-block", pattern(0x22, BLOCK_SIZE as usize)),
        ("all-zeroes", vec![0; 3 * BLOCK_SIZE as usize]),
        ("large", pattern(0x33, 3 * 1024 * 1024 + 17)),
    ];

    let mut mount = Mount::new(&block_path, &scenario.dir.join("mnt"))?;
    for (name, content) in &files {
        let file = std::fs::File::create(mount.path.join(name))?;
        std::os::unix::fs::FileExt::write_all_at(&file, content, 0)?;
        file.sync_all()?;
    }
    mount.unmount()?;

    // Remounting is what makes the read-back come from the device rather than
    // the page cache, and it runs ext4's own journal replay.
    let mut mount = Mount::new(&block_path, &scenario.dir.join("mnt"))?;
    let mut mismatched = Vec::new();

    for (name, content) in &files {
        if std::fs::read(mount.path.join(name))? != *content {
            mismatched.push(*name);
        }
    }
    mount.unmount()?;

    let image = disk.stop()?.expect("the disk was live");
    let mutations = collector.join().expect("collector panicked");

    let (replayed, allocated) = replay(&scenario.dir, &mutations)?;

    Ok(serde_json::json!({
        "dir": scenario.dir,
        "mount": mount.path,
        "files": files.iter().map(|(name, content)| (*name, content.len())).collect::<Vec<_>>(),
        "mismatched": mismatched,
        "mutations": mutations.len(),
        "chunks": mutations.iter().map(Vec::len).sum::<usize>(),
        "image": describe(image.file(), image.allocated())?,
        "replay": describe(replayed.file(), &allocated)?,
    }))
}

/// Free space through the mount and observe the discards as punches which clear
/// allocated bits.
fn discard() -> anyhow::Result<serde_json::Value> {
    const FILE_BLOCKS: u32 = 4096;

    let scenario = Scenario::new()?;
    let (mut disk, captured) = scenario.disk(disk_daemon::ublk::QUEUE_DEPTH)?;
    let collector = collect(captured);

    let block_path = disk.block_path();
    format(&block_path)?;

    let mut mount = Mount::new(&block_path, &scenario.dir.join("mnt"))?;
    let path = mount.path.join("filler");

    let file = std::fs::File::create(&path)?;
    std::os::unix::fs::FileExt::write_all_at(
        &file,
        &pattern(0x5a, (FILE_BLOCKS * BLOCK_SIZE) as usize),
        0,
    )?;
    file.sync_all()?;
    drop(file);

    std::fs::remove_file(&path)?;
    // `-o discard` issues discards as ext4 commits the deletion, and `fstrim`
    // then covers everything else the filesystem considers free.
    run(&mut std::process::Command::new("sync"))?;
    run(std::process::Command::new("fstrim").arg(&mount.path))?;

    mount.unmount()?;
    let image = disk.stop()?.expect("the disk was live");
    let mutations = collector.join().expect("collector panicked");

    // Replay tracks the allocated set as it moves, so the peak is what the
    // filler occupied and the final value is what the discards left.
    let (replayed, allocated) = replay(&scenario.dir, &mutations)?;
    let mut peak_allocated = 0;
    let mut peak_bits = Bitmap::new(BLOCKS);
    let peak_image = Image::create(&scenario.dir, BLOCKS, BLOCK_SIZE)?;

    for chunk in mutations.iter().flatten() {
        chunk::apply(chunk, BLOCK_SIZE, peak_image.file(), &mut peak_bits)?;
        peak_allocated = std::cmp::max(peak_allocated, peak_bits.count_ones());
    }
    let punches: Vec<&Chunk> = mutations
        .iter()
        .flatten()
        .filter(|chunk| {
            matches!(
                chunk.content,
                Some(disk_daemon::proto::chunk::Content::Punch(_))
            )
        })
        .collect();

    Ok(serde_json::json!({
        "dir": scenario.dir,
        "file_blocks": FILE_BLOCKS,
        "punch_chunks": punches.len(),
        "punched_blocks": punches
            .iter()
            .map(|chunk| chunk::covered_blocks(chunk, BLOCK_SIZE).len())
            .sum::<usize>(),
        "peak_allocated": peak_allocated,
        "image": describe(image.file(), image.allocated())?,
        "replay": describe(replayed.file(), &allocated)?,
    }))
}

/// Stall the capture sink and observe that writes park rather than fail.
fn backpressure() -> anyhow::Result<serde_json::Value> {
    // The capture channel holds one mutation per queue slot, so a shallow
    // queue is a shallow channel.
    const QUEUE_DEPTH: u16 = 2;
    const WRITES: usize = 16;
    const STALL: std::time::Duration = std::time::Duration::from_millis(500);

    let scenario = Scenario::new()?;
    let (mut disk, captured) = scenario.disk(QUEUE_DEPTH)?;

    let done = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let failed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let writer = {
        let (block_path, done, failed) = (disk.block_path(), done.clone(), failed.clone());

        // O_DIRECT so each write is one device request which does not return
        // until the device completes it.
        std::thread::spawn(move || -> anyhow::Result<()> {
            let mut options = std::fs::OpenOptions::new();
            options.write(true);
            std::os::unix::fs::OpenOptionsExt::custom_flags(&mut options, libc::O_DIRECT);
            let device = options.open(&block_path)?;

            let block = aligned_block(0x7e);

            for index in 0..WRITES {
                let offset = index as u64 * BLOCK_SIZE as u64;

                match std::os::unix::fs::FileExt::write_all_at(&device, &block, offset) {
                    Ok(()) => _ = done.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                    Err(err) => {
                        tracing::error!(?err, offset, "device write failed");
                        failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }
            Ok(())
        })
    };

    std::thread::sleep(STALL);
    let during_stall = done.load(std::sync::atomic::Ordering::Relaxed);

    // Taking from the channel is what frees the parked writes.
    let collector = collect(captured);
    writer.join().expect("writer panicked")?;

    let after = done.load(std::sync::atomic::Ordering::Relaxed);
    let image = disk.stop()?.expect("the disk was live");
    let mutations = collector.join().expect("collector panicked");

    let (replayed, allocated) = replay(&scenario.dir, &mutations)?;

    Ok(serde_json::json!({
        "dir": scenario.dir,
        "capacity": QUEUE_DEPTH,
        "writes": WRITES,
        "during_stall": during_stall,
        "completed": after,
        "failed": failed.load(std::sync::atomic::Ordering::Relaxed),
        "mutations": mutations.len(),
        "image": describe(image.file(), image.allocated())?,
        "replay": describe(replayed.file(), &allocated)?,
    }))
}

/// Delete every device whose block device the kernel has already removed,
/// which is what a server killed outright leaves behind.
///
/// Reaping is the test suite's to do, not a daemon's: whether a device with no
/// server may be deleted is a decision about the whole host, and these tests
/// are the host's only ublk user.
fn reap() -> anyhow::Result<serde_json::Value> {
    let control = Control::open()?;
    let mut deleted = Vec::new();

    for entry in std::fs::read_dir("/dev")? {
        let name = entry?.file_name().to_string_lossy().into_owned();

        let Some(dev_id) = name.strip_prefix("ublkc") else {
            continue;
        };
        let dev_id: u32 = dev_id.parse()?;

        anyhow::ensure!(
            !sys_block_path(dev_id).exists(),
            "device {dev_id} is still serving a block device",
        );
        () = control.del_dev(dev_id)?;
        deleted.push(dev_id);
    }

    Ok(serde_json::json!({ "deleted": deleted }))
}

/// The working directory and control device of one scenario.
struct Scenario {
    dir: std::path::PathBuf,
    control: std::sync::Arc<Control>,
}

impl Scenario {
    fn new() -> anyhow::Result<Self> {
        let dir = std::env::temp_dir().join(format!("disk-daemon-scenario.{}", std::process::id()));
        std::fs::create_dir_all(&dir)?;

        Ok(Self {
            dir,
            control: std::sync::Arc::new(Control::open()?),
        })
    }

    fn disk(&self, queue_depth: u16) -> anyhow::Result<(Disk, Captured)> {
        let image = Image::create(&self.dir, BLOCKS, BLOCK_SIZE)?;

        Disk::create(&self.control, image, queue_depth)
    }
}

impl Drop for Scenario {
    fn drop(&mut self) {
        _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// A filesystem this scenario mounted. Unmounted on drop, so a failing scenario
/// leaves no mount behind.
struct Mount {
    path: std::path::PathBuf,
    mounted: bool,
}

impl Mount {
    fn new(device: &std::path::Path, path: &std::path::Path) -> anyhow::Result<Self> {
        std::fs::create_dir_all(path)?;

        run(std::process::Command::new("mount")
            .args(["-t", "ext4", "-o", MOUNT_OPTIONS])
            .arg(device)
            .arg(path))?;

        Ok(Self {
            path: path.to_path_buf(),
            mounted: true,
        })
    }

    fn unmount(&mut self) -> anyhow::Result<()> {
        if !std::mem::take(&mut self.mounted) {
            return Ok(());
        }
        run(std::process::Command::new("umount").arg(&self.path))
    }
}

impl Drop for Mount {
    fn drop(&mut self) {
        if let Err(err) = self.unmount() {
            tracing::error!(?err, path = ?self.path, "failed to unmount");
        }
    }
}

/// `assume_storage_prezeroed` leaves the unused inode tables and the internal
/// journal as holes, so they never enter the allocated bitmap. `nodiscard`
/// keeps the format from discarding the whole device first.
fn format(device: &std::path::Path) -> anyhow::Result<()> {
    run(std::process::Command::new("mkfs.ext4")
        .args(["-F", "-b"])
        .arg(BLOCK_SIZE.to_string())
        .args(["-m", "0", "-E", "nodiscard,assume_storage_prezeroed=1"])
        .arg(device))
}

fn run(command: &mut std::process::Command) -> anyhow::Result<()> {
    let output = command
        .output()
        .map_err(|err| anyhow::anyhow!("running {command:?}: {err}"))?;

    anyhow::ensure!(
        output.status.success(),
        "{command:?} failed ({}): {}{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    Ok(())
}

/// Take every mutation of a disk until its owner releases it.
fn collect(captured: Captured) -> std::thread::JoinHandle<Vec<Vec<Chunk>>> {
    std::thread::spawn(move || {
        let mut mutations = Vec::new();
        while let Some(chunks) = captured.blocking_recv() {
            mutations.push(chunks);
        }
        mutations
    })
}

/// Apply a captured stream to a fresh image, which is what a recovering session
/// does.
fn replay(dir: &std::path::Path, mutations: &[Vec<Chunk>]) -> anyhow::Result<(Image, Bitmap)> {
    let image = Image::create(dir, BLOCKS, BLOCK_SIZE)?;
    let mut allocated = Bitmap::new(BLOCKS);

    for chunk in mutations.iter().flatten() {
        chunk::apply(chunk, BLOCK_SIZE, image.file(), &mut allocated)?;
    }
    Ok((image, allocated))
}

/// Content, tracked allocation, and filesystem allocation of an image. Two
/// images agree on all three when one is a faithful replay of the other.
fn describe(file: &std::fs::File, allocated: &Bitmap) -> anyhow::Result<serde_json::Value> {
    // Delayed allocation means the extents are only settled once written back.
    file.sync_all()?;
    let extents = data_extents(file)?;

    Ok(serde_json::json!({
        "digest": digest(file)?,
        "allocated": allocated.count_ones(),
        "extents": extents.len(),
        "extents_digest": sha256(format!("{extents:?}").as_bytes()),
    }))
}

fn digest(file: &std::fs::File) -> anyhow::Result<String> {
    let mut hasher = <sha2::Sha256 as sha2::Digest>::new();
    let mut buf = vec![0u8; 1 << 20];

    let len = file.metadata()?.len();
    let mut offset = 0;

    while offset < len {
        let take = std::cmp::min(buf.len() as u64, len - offset) as usize;
        std::os::unix::fs::FileExt::read_exact_at(file, &mut buf[..take], offset)?;

        sha2::Digest::update(&mut hasher, &buf[..take]);
        offset += take as u64;
    }
    Ok(hex(&sha2::Digest::finalize(hasher)))
}

fn sha256(bytes: &[u8]) -> String {
    hex(&<sha2::Sha256 as sha2::Digest>::digest(bytes))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Byte ranges the host filesystem reports as allocated.
fn data_extents(file: &std::fs::File) -> anyhow::Result<Vec<(u64, u64)>> {
    let fd = std::os::fd::AsRawFd::as_raw_fd(file);
    let size = file.metadata()?.len() as i64;

    let mut extents = Vec::new();
    let mut cursor = 0;

    while cursor < size {
        // SAFETY: `file` holds the descriptor open across both calls.
        let start = unsafe { libc::lseek(fd, cursor, libc::SEEK_DATA) };
        if start < 0 {
            break; // ENXIO: no data at or after `cursor`.
        }
        let end = unsafe { libc::lseek(fd, start, libc::SEEK_HOLE) };
        anyhow::ensure!(end > start, "SEEK_HOLE must advance past SEEK_DATA");

        extents.push((start as u64, end as u64));
        cursor = end;
    }
    Ok(extents)
}

fn sys_block_path(dev_id: u32) -> std::path::PathBuf {
    format!("/sys/block/ublkb{dev_id}").into()
}

/// File content in which every third block is entirely zero, so that trailing
/// zero trimming and empty-data chunks both occur.
fn pattern(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|index| {
            if (index / BLOCK_SIZE as usize) % 3 == 2 {
                0
            } else {
                seed.wrapping_add((index % 251) as u8)
            }
        })
        .collect()
}

/// One block of content, aligned as `O_DIRECT` requires.
fn aligned_block(fill: u8) -> &'static [u8] {
    let backing = vec![fill; 2 * BLOCK_SIZE as usize].leak();
    let offset = backing.as_ptr().align_offset(BLOCK_SIZE as usize);

    &backing[offset..offset + BLOCK_SIZE as usize]
}
