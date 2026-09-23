//! Serving a real `ublk` device from a test.
//!
//! Serving a device and mounting a filesystem need `CAP_SYS_ADMIN`, and cargo must not
//! run as root, or the target directory stops being the user's. The privilege
//! therefore lives in a child process, and that child is this test binary again,
//! running the one case it was told to. [`privileged_test`] arranges that, so a case's
//! assertions are ordinary assertions which happen to run as root.
//!
//! These tests are in the default run. A nextest test group serializes them, because
//! they contend on the host-wide control device.

use crate::BLOCK_SIZE;
use crate::bitmap::Bitmap;
use crate::capture::Captured;
use crate::chunk;
use crate::device::Device;
use crate::horizon::Policy;
use crate::image::Image;
use crate::proto::Chunk;
use crate::ublk::Control;

/// 128 MiB of blocks. `mkfs.ext4` accepts that size comfortably, and it keeps a case
/// to a few seconds.
pub const BLOCKS: u32 = 32768;

/// Compaction no case reaches unless it exercises compaction on purpose. These are the
/// shipped ratios, with a minimum no case's journal range approaches.
pub const NO_COMPACTION: Policy = Policy {
    open_ratio: 2.0,
    copy_ratio: 0.5,
    minimum_bytes: 1 << 40,
};

/// Set on the privileged child, which runs a case body rather than spawning one.
const MARKER: &str = "DISK_DAEMON_PRIVILEGED";

/// Define a `#[test]` whose body runs as root, in a directory the body names.
///
/// The child is selected by test name, and the macro derives that name from the module
/// it is invoked in rather than letting a case repeat it. A case which is renamed or
/// moved therefore keeps working.
macro_rules! privileged_test {
    ($(#[$meta:meta])* fn $name:ident($dir:ident) $body:block) => {
        $(#[$meta])*
        #[test]
        fn $name() {
            $crate::test_support::device::privileged(
                module_path!(),
                stringify!($name),
                |$dir| $body,
            )
        }
    };
}

pub(crate) use privileged_test;

/// Run `body` as root, by re-executing this test binary under `sudo -n`.
///
/// The child runs the one case named by `module_path` and `name`, whose assertions are
/// then the verdict this returns. `body` receives the directory to work in.
pub fn privileged(module_path: &str, name: &str, body: impl FnOnce(&std::path::Path)) {
    // The parent chooses that directory and names it to the child, rather than both
    // deriving one: `sudo` resets the environment, so the child's `TMPDIR` need not be
    // the parent's.
    if let Some(dir) = std::env::var_os(MARKER) {
        return body(std::path::Path::new(&dir));
    }

    let dir = std::env::temp_dir().join(format!("disk-daemon-device.{name}"));

    // A test is named by its path below the crate root, which is what `--exact` takes.
    let module = module_path
        .split_once("::")
        .map(|(_crate_name, path)| path)
        .unwrap_or(module_path);

    let case = format!("{module}::{name}");

    let output = std::process::Command::new("sudo")
        .args(["-n", "env", &format!("{MARKER}={}", dir.display())])
        .arg(std::env::current_exe().expect("this test binary has a path"))
        .args(["--exact", &case, "--nocapture"])
        .output()
        .expect("spawning sudo");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "{case} failed as root ({}):\n{stdout}{stderr}",
        output.status,
    );
    // A filter which matches nothing also exits zero, which would pass a case without
    // ever running it.
    assert!(
        stdout.contains("1 passed"),
        "{case} did not run as root:\n{stdout}{stderr}",
    );
}

/// The working directory and control device of one case.
pub struct Scenario {
    pub dir: std::path::PathBuf,
    pub control: std::sync::Arc<Control>,
}

impl Scenario {
    pub fn new(dir: &std::path::Path) -> Self {
        std::fs::create_dir_all(dir).unwrap();

        Self {
            dir: dir.to_path_buf(),
            control: std::sync::Arc::new(Control::open().unwrap()),
        }
    }

    /// Create and serve a disk over a fresh image of [`BLOCKS`] blocks.
    ///
    /// No case here replays, so no horizon is ever resumed: a case which is about
    /// compaction opens one through the disk's own `Compactor`.
    pub fn disk(&self, queue_depth: u16, policy: Policy) -> (Device, Captured) {
        let image = Image::create(&self.dir, BLOCKS).unwrap();

        Device::create(&self.control, image, queue_depth, None, policy).unwrap()
    }
}

impl Drop for Scenario {
    fn drop(&mut self) {
        _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// A filesystem a case mounted. `Drop` unmounts it, so a failing case leaves no mount
/// behind.
pub struct Mount {
    pub path: std::path::PathBuf,
    mounted: bool,
}

impl Mount {
    pub fn new(device: &std::path::Path, path: &std::path::Path) -> Self {
        Self::mount("ext4", crate::filesystem::MOUNT_OPTIONS, device, path)
    }

    /// A tmpfs of `bytes`, which a case fills to run a disk's host out of space.
    pub fn tmpfs(path: &std::path::Path, bytes: u64) -> Self {
        Self::mount("tmpfs", &format!("size={bytes}"), "tmpfs".as_ref(), path)
    }

    fn mount(kind: &str, options: &str, source: &std::path::Path, path: &std::path::Path) -> Self {
        std::fs::create_dir_all(path).unwrap();

        run(std::process::Command::new("mount")
            .args(["-t", kind, "-o", options])
            .arg(source)
            .arg(path))
        .unwrap();

        Self {
            path: path.to_path_buf(),
            mounted: true,
        }
    }

    pub fn unmount(&mut self) -> anyhow::Result<()> {
        if !std::mem::take(&mut self.mounted) {
            return Ok(());
        }
        run(std::process::Command::new("umount").arg(&self.path))
    }
}

impl Drop for Mount {
    fn drop(&mut self) {
        // A panic here while another unwinds would abort the child, losing the failure
        // it was reporting. The leak check of the parent fails the case.
        if let Err(err) = self.unmount() {
            tracing::error!(?err, path = ?self.path, "failed to unmount");
        }
    }
}

/// Take every mutation of a disk until its owner exits.
///
/// A case drives its device from synchronous code and joins this handle, so the
/// thread carries a runtime of its own to await the channel on.
pub fn collect(mut captured: Captured) -> std::thread::JoinHandle<Vec<Vec<Chunk>>> {
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("building a runtime to collect on");

        runtime.block_on(async move {
            let mut mutations = Vec::new();
            while let Some(chunks) = captured.recv().await {
                mutations.push(chunks);
            }
            mutations
        })
    })
}

/// Apply a captured stream to a fresh image, as a recovering tenure does.
pub fn replay(dir: &std::path::Path, mutations: &[Vec<Chunk>]) -> (Image, Bitmap) {
    let image = Image::create(dir, BLOCKS).unwrap();
    let mut allocated = Bitmap::new(BLOCKS);

    for chunk in mutations.iter().flatten() {
        () = chunk::apply(chunk, image.file(), &mut allocated).unwrap();
    }
    (image, allocated)
}

/// The captured stream, replayed onto a second image, reproduces the image served byte
/// for byte and block for block. This is why the local image is disposable.
///
/// Holes are compared as well as bytes. A replay which matched every byte but
/// allocated differently would have cost the disk its sparseness.
pub fn assert_replays_identically(served: &Image, replayed: &Image, allocated: &Bitmap) {
    assert!(
        served.allocated().count_ones() > 0,
        "the case allocated nothing",
    );
    assert_eq!(
        served.allocated(),
        allocated,
        "the replay tracked other blocks",
    );

    // Delayed allocation settles the extents only once they are written back.
    served.file().sync_all().unwrap();
    replayed.file().sync_all().unwrap();

    assert_eq!(
        data_extents(served.file()),
        data_extents(replayed.file()),
        "the replay allocated other ranges of the host filesystem",
    );
    assert_eq!(
        first_difference(served.file(), replayed.file()),
        None,
        "the replay differs from the image served",
    );
}

/// Byte ranges the host filesystem reports as allocated.
fn data_extents(file: &std::fs::File) -> Vec<(u64, u64)> {
    let fd = std::os::fd::AsRawFd::as_raw_fd(file);
    let size = file.metadata().unwrap().len() as i64;

    let mut extents = Vec::new();
    let mut cursor = 0;

    while cursor < size {
        // SAFETY: `file` holds the descriptor open across both calls.
        let start = unsafe { libc::lseek(fd, cursor, libc::SEEK_DATA) };
        if start < 0 {
            break; // ENXIO: no data at or after `cursor`.
        }
        let end = unsafe { libc::lseek(fd, start, libc::SEEK_HOLE) };
        assert!(end > start, "SEEK_HOLE must advance past SEEK_DATA");

        extents.push((start as u64, end as u64));
        cursor = end;
    }
    extents
}

/// Offset at which two images first differ, which names where a replay went wrong
/// instead of dumping either image.
fn first_difference(left: &std::fs::File, right: &std::fs::File) -> Option<u64> {
    let len = left.metadata().unwrap().len();
    assert_eq!(
        len,
        right.metadata().unwrap().len(),
        "the images differ in size",
    );

    let mut buffers = [vec![0u8; 1 << 20], vec![0u8; 1 << 20]];
    let mut offset = 0;

    while offset < len {
        let take = std::cmp::min(buffers[0].len() as u64, len - offset) as usize;

        for (file, buf) in [left, right].into_iter().zip(buffers.iter_mut()) {
            () = std::os::unix::fs::FileExt::read_exact_at(file, &mut buf[..take], offset).unwrap();
        }
        let [left_buf, right_buf] = &buffers;

        if let Some(index) = (0..take).find(|&index| left_buf[index] != right_buf[index]) {
            return Some(offset + index as u64);
        }
        offset += take as u64;
    }
    None
}

pub fn write_blocks(device: &std::fs::File, block: u32, data: &[u8]) {
    std::os::unix::fs::FileExt::write_all_at(device, data, block as u64 * BLOCK_SIZE as u64)
        .unwrap()
}

/// Format a device exactly as the daemon does, by running the daemon's own
/// invocation of `mkfs`.
///
/// `async_process::Command` is `std::process::Command`, so a case which drives its
/// device synchronously can run it without a runtime.
pub fn format(device: &std::path::Path) {
    run(&mut crate::filesystem::mkfs(device, None)).unwrap()
}

pub fn run(command: &mut std::process::Command) -> anyhow::Result<()> {
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

/// File content in which every third block is entirely zero. Both trailing-zero
/// trimming and empty-data chunks then occur.
pub fn pattern(seed: u8, len: usize) -> Vec<u8> {
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
pub fn aligned_block(fill: u8) -> &'static [u8] {
    let block = aligned_buffer(1);
    block.fill(fill);

    block
}

/// A buffer of `blocks` blocks, aligned as `O_DIRECT` requires. A caller refills it
/// rather than allocating one buffer per write.
pub fn aligned_buffer(blocks: u32) -> &'static mut [u8] {
    let backing = vec![0u8; (blocks as usize + 1) * BLOCK_SIZE as usize].leak();
    let offset = backing.as_ptr().align_offset(BLOCK_SIZE as usize);

    &mut backing[offset..offset + blocks as usize * BLOCK_SIZE as usize]
}

/// Open a device for writes which each become exactly one device request.
///
/// `O_DIRECT` is what makes the accounting a case does the disk's own, rather than a
/// page cache's, and what makes a write block until the device completes it.
pub fn open_direct(device: &std::path::Path) -> std::fs::File {
    let mut options = std::fs::OpenOptions::new();
    options.write(true);
    std::os::unix::fs::OpenOptionsExt::custom_flags(&mut options, libc::O_DIRECT);

    options
        .open(device)
        .unwrap_or_else(|err| panic!("opening {device:?} with O_DIRECT: {err}"))
}
