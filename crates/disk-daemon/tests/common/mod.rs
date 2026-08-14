//! What the privileged test binaries share: the prerequisites they need, and
//! the leak checks they end with.

/// Fail with what to do about it, rather than skipping, so that a machine which
/// cannot serve a device says so.
pub fn check_prerequisites() {
    assert!(
        std::path::Path::new("/sys/module/ublk_drv").exists(),
        "ublk_drv is not loaded, so no block device can be served. \
         Load it with `sudo modprobe ublk_drv`.",
    );
    assert!(
        std::path::Path::new("/dev/ublk-control").exists(),
        "/dev/ublk-control is absent though ublk_drv is loaded, so this kernel's \
         module was built without the control device.",
    );

    let sudo = std::process::Command::new("sudo")
        .args(["-n", "true"])
        .output()
        .expect("spawning sudo");

    assert!(
        sudo.status.success(),
        "passwordless sudo is required, because these tests serve real ublk \
         devices from `sudo -n` child processes: {}",
        String::from_utf8_lossy(&sudo.stderr),
    );
}

/// No device node and no `/sys/block` entry outlives the test which made it.
/// A nextest test group serializes these binaries, so what they see is theirs.
pub fn assert_no_leaked_devices() {
    let nodes = entries("/dev", |name| {
        name.starts_with("ublk") && name != "ublk-control"
    });
    assert!(nodes.is_empty(), "leaked device nodes: {nodes:?}");

    let blocks = entries("/sys/block", |name| name.starts_with("ublkb"));
    assert!(blocks.is_empty(), "leaked block devices: {blocks:?}");
}

/// No mount of `dir` or of a served device outlives the test which made it.
pub fn assert_no_mounts_under(dir: &str) {
    let mounts = std::fs::read_to_string("/proc/mounts").unwrap();

    for line in mounts.lines() {
        assert!(
            !line.contains(dir) && !line.contains("/dev/ublkb"),
            "leaked mount: {line}",
        );
    }
}

fn entries(dir: &str, keep: impl Fn(&str) -> bool) -> Vec<String> {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|entry| Some(entry.ok()?.file_name().to_string_lossy().into_owned()))
        .filter(|name| keep(name))
        .collect()
}
