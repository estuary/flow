use super::ublk;

/// The time between passes, after the first pass at start.
const INTERVAL: std::time::Duration = std::time::Duration::from_secs(5 * 60);

/// Reap orphans on a detached thread: once now, and then every `INTERVAL`. An
/// orphan is a stopped device of this crate whose number no `ublk::Control`
/// holds. To reap an orphan, lock its number and delete it.
pub fn reap() -> std::io::Result<()> {
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
                    let mut control = match ublk::Control::lock(dev_id) {
                        Ok(Some(control)) => control,
                        Ok(None) => continue,
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
