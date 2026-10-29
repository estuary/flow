use super::ublk;

/// The time between passes, after the first pass at start.
const INTERVAL: std::time::Duration = std::time::Duration::from_secs(5 * 60);

/// Reap orphans on a detached thread: once now, and then every `INTERVAL`. An
/// orphan is a device of this crate whose server died. The kernel stopped it, but
/// did not delete it. To reap an orphan, detach its mounts and delete it. Remove
/// its mount point if it is in `mount_dir`.
pub fn reap(mount_dir: std::path::PathBuf) -> std::io::Result<()> {
    std::thread::Builder::new()
        .name("disk-reaper".to_string())
        .spawn(move || {
            loop {
                let dev_ids: Vec<u32> = match std::fs::read_dir("/sys/class/ublk-char") {
                    Ok(entries) => entries
                        .filter_map(|entry| {
                            entry
                                .ok()?
                                .file_name()
                                .to_str()?
                                .strip_prefix("ublkc")?
                                .parse()
                                .ok()
                        })
                        .collect(),
                    Err(err) => {
                        tracing::warn!(?err, "listing ublk devices");
                        Vec::new()
                    }
                };

                for dev_id in dev_ids {
                    let mut control = match ublk::Control::open(dev_id) {
                        Ok(control) => control,
                        Err(err) => {
                            tracing::warn!(dev_id, ?err, "opening the control of a ublk device");
                            continue;
                        }
                    };
                    match control.orphaned() {
                        Ok(true) => (),
                        Ok(false) => continue,
                        Err(err) => {
                            tracing::warn!(dev_id, ?err, "reading a ublk device");
                            continue;
                        }
                    }
                    tracing::info!(dev_id, "reaping an orphan");
                    let block_path = control.block_path();

                    let mounted = super::mounted(&block_path).unwrap_or_else(|err| {
                        tracing::warn!(?err, "reading mounts");
                        false
                    });
                    if mounted {
                        // Detach each mount of the device in this mount namespace.
                        match std::process::Command::new("umount")
                            .args(["--lazy", "--all-targets"])
                            .arg(&block_path)
                            .output()
                        {
                            Ok(output) if output.status.success() => (),
                            Ok(output) => tracing::warn!(
                                dev_id,
                                stderr = %String::from_utf8_lossy(&output.stderr),
                                "detaching the mounts of an orphan",
                            ),
                            Err(err) => tracing::warn!(dev_id, ?err, "running umount"),
                        }
                    }
                    // A mount point names its device.
                    let name = block_path.file_name().expect("a block device has a name");
                    _ = std::fs::remove_dir(mount_dir.join(name));

                    // Before Linux 6.11, a delete waits while a mount in another
                    // namespace holds the device. Later orphans wait until then.
                    match control.delete() {
                        Ok(()) => tracing::info!(dev_id, "reaped an orphan"),
                        Err(err) => tracing::warn!(dev_id, ?err, "deleting an orphan"),
                    }
                }

                std::thread::sleep(INTERVAL);
            }
        })
        .map(drop)
}
