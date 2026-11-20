// Only Linux attaches disks. Elsewhere, `unsupported.rs` stands in.
#[cfg_attr(not(target_os = "linux"), path = "unsupported.rs")]
mod attach;
pub use attach::Attached;

use anyhow::Context;

/// Size of every disk's device.
pub const DISK_SIZE: u64 = 10 * 1024 * 1024 * 1024; // 10 GiB.

#[derive(Clone, Debug, PartialEq)]
pub enum Disks {
    /// A journal-backed disk from the daemon.
    Persistent {
        uds_path: Option<std::path::PathBuf>,
    },
    /// An empty directory, which lives only as long as its connector.
    Ephemeral,
}

impl Disks {
    /// Return what the shard of `labeling` gives its connector at
    /// `$CONNECTOR_MOUNT/persist`. The first call which needs a disk attaches
    /// it and keeps it in `attached`. Later calls reuse it.
    pub async fn for_shard(
        &self,
        labeling: &ops::proto::ShardLabeling,
        disk_template: Option<&proto_gazette::broker::JournalSpec>,
        disk_ack_intents: &std::collections::BTreeMap<String, bytes::Bytes>,
        attached: &mut Option<Attached>,
    ) -> anyhow::Result<Option<connector::proto::request::start::Volume>> {
        use connector::proto::request::start;

        let range = labeling.range.as_ref().context("missing range")?;
        // Only shard zero has a disk for now.
        let Some(template) = disk_template.filter(|_| labels::shard::is_zero(range)) else {
            return Ok(None);
        };
        let uds_path = match self {
            Disks::Ephemeral => {
                return Ok(Some(start::Volume::Ephemeral(start::Ephemeral {})));
            }
            Disks::Persistent { uds_path: None } => {
                anyhow::bail!("this reactor has no disk daemon (FLOW_DISK_DAEMON_UDS_PATH)")
            }
            Disks::Persistent {
                uds_path: Some(uds_path),
            } => uds_path,
        };

        if attached.is_none() {
            let journal = labels::shard::disk_journal_name(&template.name, range);
            let ack_intent = disk_ack_intents.get(&journal).cloned();
            *attached = Some(Attached::new(uds_path, &journal, ack_intent).await?);
        }
        // The daemon names the mount in a proto string, so it's always UTF-8.
        let persist_dir = attached.as_ref().unwrap().persist_dir();
        Ok(Some(start::Volume::HostDir(
            persist_dir.to_string_lossy().into_owned(),
        )))
    }
}
