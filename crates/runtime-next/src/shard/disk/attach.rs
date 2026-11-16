use super::DISK_SIZE;
use anyhow::Context;
use std::os::unix::fs::PermissionsExt;

/// Features of every disk's ext4 filesystem: e2fsprogs 1.47's defaults for
/// ext4, pinned so that reactors with other versions make the same filesystem.
const FEATURES: &str = "none,sparse_super,large_file,filetype,resize_inode,dir_index,ext_attr,has_journal,extent,huge_file,flex_bg,metadata_csum,64bit,dir_nlink,extra_isize";

/// A disk attached through the disk daemon, and mounted.
pub struct Attached {
    /// The daemon's attachment, which serves the disk's device.
    disk: disk_daemon::client::Disk,
    /// Name of the disk's journal.
    journal: String,
    /// Root of the mounted filesystem.
    root: std::path::PathBuf,
}

impl Attached {
    /// Attach the disk of `journal` with its committed `ack_intent`, format it
    /// if new, and mount it.
    pub async fn new(
        uds_path: &std::path::Path,
        journal: &str,
        ack_intent: Option<bytes::Bytes>,
    ) -> anyhow::Result<Self> {
        let client = disk_daemon::client::Client::connect(uds_path)
            .await
            .with_context(|| format!("connecting to the disk daemon at {uds_path:?}"))?;

        let has_intent = ack_intent.is_some();
        let disk_daemon::client::Promoted {
            mut disk,
            device_path,
            blank,
        } = client
            .open(
                disk_daemon::proto::Open {
                    journal: journal.to_string(),
                    device_size: DISK_SIZE,
                },
                ack_intent.unwrap_or_default(),
            )
            .await
            .with_context(|| format!("attaching disk {journal}"))?;

        match (has_intent, blank) {
            (false, true) => () = format(&device_path).await?,
            (true, false) => (),
            (true, true) => anyhow::bail!(
                "disk {journal} has a committed ACK intent, but its device is blank: reset the task to start it with an empty disk"
            ),
            (false, false) => anyhow::bail!(
                "disk {journal}'s device has a filesystem, but no ACK intent was committed for it: reset the task to start it with an empty disk"
            ),
        }
        let root = disk
            .mount(
                "ext4",
                "nodev,nosuid,noexec,nosymfollow,errors=remount-ro,noatime,discard",
            )
            .await
            .with_context(|| format!("mounting disk {journal}"))?;

        Ok(Self {
            disk,
            journal: journal.to_string(),
            root,
        })
    }

    /// The directory a connector sees at `$CONNECTOR_MOUNT/persist`.
    pub fn persist_dir(&self) -> std::path::PathBuf {
        self.root.join("persist")
    }

    /// Confirm the last acknowledged commit, then make the disk's changes
    /// durable. Return their ACK intent, keyed by the disk's journal.
    pub async fn prepare(
        &mut self,
    ) -> anyhow::Result<std::collections::BTreeMap<String, bytes::Bytes>> {
        let intent = self
            .disk
            .prepare()
            .await
            .context("preparing the disk's delta")?;

        Ok([(self.journal.clone(), intent)].into())
    }

    /// Commit the prepared delta. Never waits: the next `prepare` or `close`
    /// confirms the commit and reports a failure.
    pub fn acknowledge(&mut self) {
        self.disk.acknowledge();
    }

    /// Close the attachment, which unmounts the disk and stops its device.
    pub async fn close(self) -> anyhow::Result<()> {
        self.disk
            .close()
            .await
            .context("closing the disk's attachment")
    }
}

async fn format(device: &std::path::Path) -> anyhow::Result<()> {
    // `mkfs -d` copies this template into the new filesystem. So the disk starts
    // with an empty `persist/`, which any uid can write. The connector sees it at
    // `$CONNECTOR_MOUNT/persist`.
    let template = tempfile::tempdir().context("creating a disk template")?;
    let persist = template.path().join("persist");
    () = std::fs::create_dir(&persist).context("creating the template's persist/")?;
    () = std::fs::set_permissions(&persist, std::fs::Permissions::from_mode(0o1777))
        .context("opening the template's persist/")?;

    let mut mkfs = std::process::Command::new("mkfs.ext4");
    mkfs.args(["-q", "-b", "4096", "-m", "0", "-O", FEATURES])
        .args(["-E", "nodiscard,assume_storage_prezeroed=1"])
        .arg("-d")
        .arg(template.path())
        .arg(device)
        // A prompt reads EOF, and so declines.
        .stdin(std::process::Stdio::null());

    let output = tokio::task::spawn_blocking(move || mkfs.output())
        .await
        .unwrap()
        .context("running mkfs.ext4")?;
    if !output.status.success() {
        anyhow::bail!(
            "mkfs.ext4 of {device:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}
