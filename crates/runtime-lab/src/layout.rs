//! On-disk layout of a run, shared by the controller and its host processes.
//!
//! Two roots, deliberately separate:
//! - The **run directory** `<experiment>/runs/<run-id>/` is the run's record:
//!   manifest, events, samples, stats, and logs. It's small, and it names
//!   customer data, so it must never be within a git work tree.
//! - The **data directory** `<data-root>/runs/<run-id>/` holds bulk, disposable
//!   state: shuffle logs, temp files, shard-zero RocksDBs, journals files, and
//!   the broker's etcd and fragments. It belongs on a local SSD.
//!
//! Unix sockets live under `$XDG_RUNTIME_DIR`, as socket paths are limited to
//! 108 bytes.

use anyhow::Context;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Written by each host process, once it's serving, to `<name>.ready.json`
/// of its host directory (by `write_json_atomic`).
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Ready {
    pub pid: u32,
    /// Loopback port of the process's service-kit admin surface (`/metrics`).
    pub admin_port: u16,
    /// Sidecars only: the gRPC endpoint of the Leader and shuffle services.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
}

/// The run manifest, `<run-dir>/manifest.json`: everything an operator (or a
/// script) needs to find and act upon the processes, cgroups, and directories
/// of a run. It's the contract of scripted runs; see WORKFLOW.md.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Manifest {
    pub run_id: String,
    pub topology: PathBuf,
    pub run_dir: PathBuf,
    pub data_dir: PathBuf,
    /// Root of the run's cgroup tree.
    pub cgroup: PathBuf,
    /// Run directory of the `--resume`d run, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resumed: Option<PathBuf>,
    pub controller: ManifestProcess,
    /// The run's gazette broker, to which every task's writes go.
    pub broker: ManifestProcess,
    /// gRPC endpoint of the broker.
    pub broker_endpoint: String,
    /// The broker's single etcd node.
    pub etcd: ManifestProcess,
    /// Root of the broker's file:// fragment store.
    pub fragments_dir: PathBuf,
    pub hosts: BTreeMap<String, ManifestHost>,
    pub tasks: BTreeMap<String, ManifestTask>,
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManifestProcess {
    pub pid: u32,
    pub cgroup: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admin_port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log: Option<PathBuf>,
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManifestHost {
    pub cgroup: PathBuf,
    /// Absent when no shard is placed on the host.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reactor: Option<ManifestProcess>,
    pub sidecar: ManifestProcess,
    /// Sidecar gRPC endpoint.
    pub endpoint: String,
    /// cgroup which the host's connectors join as they start.
    pub connectors_cgroup: PathBuf,
    pub dir: PathBuf,
    pub data_dir: PathBuf,
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManifestTask {
    pub kind: String,
    /// Where shard zero's state started: `fresh`, `snapshot:<name>`, or
    /// `resumed:<run-id>`.
    pub start: String,
    /// Shard zero's RocksDB, which the run reopens across sessions and which a
    /// `--resume` run reopens again.
    pub rocksdb_dir: PathBuf,
    /// Partition specs of the collections the task writes, rewritten each
    /// sample, which a `--resume` run (or a snapshot) restores.
    #[serde(default)]
    pub journals_file: PathBuf,
    pub shards: Vec<ManifestShard>,
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManifestShard {
    pub index: usize,
    pub label: String,
    pub id: String,
    pub host: String,
    pub key_begin: String,
    pub key_end: String,
    pub r_clock_begin: String,
    pub r_clock_end: String,
    pub socket: PathBuf,
    pub shuffle_dir: PathBuf,
}

impl Manifest {
    pub fn write(&self, run_dir: &Path) -> anyhow::Result<()> {
        write_json_atomic(&run_dir.join("manifest.json"), self)
    }

    pub fn load(run_dir: &Path) -> anyhow::Result<Self> {
        let path = run_dir.join("manifest.json");
        let content =
            std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
        serde_json::from_slice(&content).with_context(|| format!("parsing {}", path.display()))
    }
}

/// Write `value` as pretty JSON to `path`, by write-then-rename so that a
/// reader polling for `path` never sees a partial file.
pub fn write_json_atomic(path: &Path, value: &impl serde::Serialize) -> anyhow::Result<()> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);

    std::fs::write(&tmp, serde_json::to_vec_pretty(value)?)
        .with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, path).with_context(|| format!("renaming to {}", path.display()))
}

/// Append-only NDJSON event log of a run, `<run-dir>/events.ndjson`.
/// Measures are aligned against it by timestamp.
#[derive(Clone)]
pub struct Events(std::sync::Arc<std::sync::Mutex<std::fs::File>>);

impl Events {
    pub fn open(run_dir: &Path) -> anyhow::Result<Self> {
        let path = run_dir.join("events.ndjson");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("opening {}", path.display()))?;
        Ok(Self(std::sync::Arc::new(std::sync::Mutex::new(file))))
    }

    /// Record `event` with its `fields`, and mirror it to the controller log.
    pub fn record(&self, event: &str, fields: serde_json::Value) {
        use std::io::Write;

        tracing::info!(event, %fields, "event");
        let mut line = serde_json::json!({ "ts": now(), "event": event });
        if let (Some(line), serde_json::Value::Object(fields)) = (line.as_object_mut(), fields) {
            line.extend(fields);
        }
        let mut line = serde_json::to_vec(&line).unwrap();
        line.push(b'\n');
        // An event log that can't be written is a lost record, not a failure
        // of the experiment: it's logged, and the run continues.
        if let Err(err) = self.0.lock().unwrap().write_all(&line) {
            tracing::error!(%err, "failed to write event");
        }
    }
}

/// Current wall-clock time, as RFC 3339 with milliseconds (the format of every
/// timestamp in a run directory).
pub fn now() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// Fail if `path` (or its nearest existing ancestor) is within a git work tree.
/// Run and data directories name customer tenants and collections, and must
/// never be committed; see WORKFLOW.md.
pub fn ensure_outside_git(path: &Path) -> anyhow::Result<()> {
    let mut probe = path;
    while !probe.exists() {
        probe = probe.parent().context("path has no existing ancestor")?;
    }
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(probe)
        .args(["rev-parse", "--is-inside-work-tree"])
        .stderr(std::process::Stdio::null())
        .output();

    match output {
        Ok(output) if String::from_utf8_lossy(&output.stdout).trim() == "true" => {
            anyhow::bail!(
                "{} is within a git work tree. Experiments name customer data and must live outside of any repository (see crates/runtime-lab/WORKFLOW.md)",
                path.display()
            )
        }
        // Not a work tree, or no `git` at all.
        _ => Ok(()),
    }
}

/// Classify the storage backing `path`, returning a warning if it's not a
/// local SSD. Shuffle log throughput on a network block device (GCP
/// Persistent Disk / Hyperdisk, AWS EBS) is a small fraction of a local SSD's,
/// which silently becomes the bottleneck of an experiment.
pub fn storage_warning(path: &Path) -> Option<String> {
    let output = std::process::Command::new("findmnt")
        .args(["-no", "SOURCE,FSTYPE", "--target"])
        .arg(path)
        .output()
        .ok()?;
    let output = String::from_utf8_lossy(&output.stdout);
    let mut fields = output.split_whitespace();
    let (source, fstype) = (fields.next()?, fields.next()?);

    if fstype == "tmpfs" {
        return Some(format!(
            "{} is on tmpfs: shuffle logs and RocksDBs consume host memory, outside of the lab's cgroup accounting of connectors and hosts",
            path.display()
        ));
    }
    let device = source.strip_prefix("/dev/")?;
    let models = block_models(device);
    if models.is_empty() {
        return None; // Unknown: nothing useful to say.
    }
    let network = models.iter().any(|model| {
        model.ends_with("-pd")
            || model.contains("PersistentDisk")
            || model.contains("Elastic Block Store")
    });
    network.then(|| {
        format!(
            "{} is on a network block device ({}), not a local SSD. Shuffle log throughput will be far lower than on a local SSD, and may become the experiment's bottleneck. Pass --data-root on a local SSD",
            path.display(),
            models.join(", "),
        )
    })
}

/// Models of the physical devices underlying block `device`, following
/// partitions to their disk and RAID arrays to their members.
fn block_models(device: &str) -> Vec<String> {
    let sys = Path::new("/sys/class/block").join(device);
    let Ok(resolved) = std::fs::canonicalize(&sys) else {
        return Vec::new();
    };
    // A partition's parent directory is its disk.
    let disk = if resolved.join("partition").exists() {
        resolved.parent().map(Path::to_path_buf).unwrap_or(resolved)
    } else {
        resolved
    };

    if let Ok(members) = std::fs::read_dir(disk.join("slaves")) {
        let members: Vec<String> = members
            .filter_map(Result::ok)
            .flat_map(|member| block_models(&member.file_name().to_string_lossy()))
            .collect();
        if !members.is_empty() {
            return members;
        }
    }
    std::fs::read_to_string(disk.join("device/model"))
        .map(|model| vec![model.trim().to_string()])
        .unwrap_or_default()
}
