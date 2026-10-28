/// An ext4 filesystem which a case mounts on a disk's device, as a client would.
///
/// Dropping it detaches a filesystem still mounted, so even a failed case leaves no
/// mount behind.
pub struct Mounted {
    /// The mount point, until the filesystem is unmounted.
    path: Option<std::path::PathBuf>,
}

impl Mounted {
    /// Format `device`, which nothing has written, then mount it under `dir`.
    ///
    /// The format leaves the zeroes of a fresh device unwritten, so ext4 has no
    /// inode tables to zero after it mounts.
    pub fn format(dir: &std::path::Path, device: &std::path::Path) -> Self {
        () = run(std::process::Command::new("mkfs.ext4")
            .args(["-q", "-F", "-b", "4096"])
            .args(["-E", "nodiscard,assume_storage_prezeroed=1"])
            .arg(device));

        Self::mount(dir, device)
    }

    /// Mount the filesystem on `device` under `dir`.
    pub fn mount(dir: &std::path::Path, device: &std::path::Path) -> Self {
        static MOUNTS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

        // Devices reuse numbers, so the counter keeps each mount point distinct.
        let path = dir.join(format!(
            "mount-{}",
            MOUNTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap_or_else(|err| panic!("creating {path:?}: {err}"));

        // `discard` returns a deleted file's blocks to the device's image, and
        // `noatime` keeps reads from writing.
        () = run(std::process::Command::new("mount")
            .args(["-t", "ext4", "-o", "noatime,discard"])
            .arg(device)
            .arg(&path));

        Self { path: Some(path) }
    }

    pub fn path(&self) -> &std::path::Path {
        self.path.as_deref().expect("mounted")
    }

    /// Flush the filesystem to its device, as a client does before it prepares.
    pub fn sync(&self) {
        let root = std::fs::File::open(self.path()).unwrap();
        () =
            rustix::fs::syncfs(root).unwrap_or_else(|err| panic!("syncing {:?}: {err}", self.path));
    }

    /// Unmount, as a client does before it closes, and remove the mount point.
    pub fn unmount(mut self) {
        let path = self.path.take().expect("mounted");

        () = run(std::process::Command::new("umount").arg(&path));
        () = std::fs::remove_dir(&path).unwrap_or_else(|err| panic!("removing {path:?}: {err}"));
    }
}

impl Drop for Mounted {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            _ = std::process::Command::new("umount")
                .arg("--lazy")
                .arg(&path)
                .status();
            _ = std::fs::remove_dir(&path);
        }
    }
}

/// Run `command` to completion, and require that it succeed.
fn run(command: &mut std::process::Command) {
    let output = command
        .output()
        .unwrap_or_else(|err| panic!("spawning {command:?}: {err}"));

    assert!(
        output.status.success(),
        "{command:?} failed ({}): {}{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}
