//! cgroup v2 tree management: nodes are plain directories, limits are plain
//! interface files, and processes are started directly into their leaf.
//!
//! The controller runs within a systemd scope having `Delegate=yes`, which it
//! owns outright: it moves itself into a `controller/` leaf and creates each
//! host's subtree beside it (see README.md).

use anyhow::Context;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub const MOUNT: &str = "/sys/fs/cgroup";

/// Controllers the lab enables through its tree. `cpuset` and `io` are not
/// delegated to users by default; see `scripts/setup-cgroups.sh`.
pub const CONTROLLERS: &[&str] = &["cpu", "cpuset", "io", "memory", "pids"];

/// The cgroup directory of the current process.
pub fn current() -> anyhow::Result<PathBuf> {
    let content =
        std::fs::read_to_string("/proc/self/cgroup").context("reading /proc/self/cgroup")?;
    // cgroup v2 has a single "0::/path" line.
    let path = content
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .context("no cgroup v2 entry in /proc/self/cgroup (is this a cgroup v2 host?)")?;
    Ok(Path::new(MOUNT).join(path.trim_start_matches('/')))
}

/// Move the current process from `root` into a `controller` leaf beneath it,
/// then enable every available lab controller for `root`'s children.
/// cgroup v2 forbids a node with member processes from delegating controllers
/// to its children, which is why the move comes first.
///
/// Returns the lab controllers which are unavailable.
pub fn take_root(root: &Path) -> anyhow::Result<Vec<&'static str>> {
    let leaf = root.join("controller");
    mkdir(&leaf)?;
    write(&leaf, "cgroup.procs", "0")?;
    enable_controllers(root)
}

/// Enable every available lab controller for `node`'s children, returning
/// those which are unavailable.
pub fn enable_controllers(node: &Path) -> anyhow::Result<Vec<&'static str>> {
    let available = read(node, "cgroup.controllers")?;
    let available: Vec<&str> = available.split_whitespace().collect();

    let (enable, missing): (Vec<&'static str>, Vec<&'static str>) = CONTROLLERS
        .iter()
        .partition(|controller| available.contains(controller));

    if !enable.is_empty() {
        let value = enable
            .iter()
            .map(|c| format!("+{c}"))
            .collect::<Vec<_>>()
            .join(" ");
        write(node, "cgroup.subtree_control", &value)?;
    }
    Ok(missing)
}

/// Create node `path` (if needed) and write each of its interface `files`.
pub fn create(path: &Path, files: &BTreeMap<String, String>) -> anyhow::Result<()> {
    mkdir(path)?;
    for (file, value) in files {
        write(path, file, value)?;
    }
    Ok(())
}

pub fn mkdir(path: &Path) -> anyhow::Result<()> {
    match std::fs::create_dir(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(err) => Err(err).with_context(|| format!("creating cgroup {}", path.display())),
    }
}

pub fn write(node: &Path, file: &str, value: &str) -> anyhow::Result<()> {
    let path = node.join(file);
    std::fs::write(&path, value).with_context(|| format!("writing {value:?} to {}", path.display()))
}

pub fn read(node: &Path, file: &str) -> anyhow::Result<String> {
    let path = node.join(file);
    std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))
}

/// Arrange for `cmd` to start within cgroup `leaf`, and to be SIGKILLed if its
/// parent (the controller) dies. The child joins `leaf` between fork and exec,
/// so it never runs anywhere else.
pub fn spawn_into(
    cmd: &mut tokio::process::Command,
    leaf: &Path,
) -> anyhow::Result<tokio::process::Child> {
    use std::os::fd::AsRawFd;

    // Opened before fork: only async-signal-safe calls may follow it.
    let procs = std::fs::OpenOptions::new()
        .write(true)
        .open(leaf.join("cgroup.procs"))
        .with_context(|| format!("opening {}/cgroup.procs", leaf.display()))?;
    let fd = procs.as_raw_fd();

    // Safety: the closure calls only async-signal-safe functions.
    unsafe {
        cmd.pre_exec(move || {
            if libc::write(fd, b"0".as_ptr() as *const libc::c_void, 1) != 1 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = cmd
        .spawn()
        .with_context(|| format!("spawning into {}", leaf.display()))?;
    std::mem::drop(procs);
    Ok(child)
}

/// Sample a node's accounting files, as a JSON object. Absent files (for a
/// controller which isn't enabled) are skipped.
pub fn sample(node: &Path) -> serde_json::Value {
    let mut out = serde_json::Map::new();

    if let Ok(content) = read(node, "cpu.stat") {
        out.insert("cpu".to_string(), flat_keyed(&content));
    }
    for file in ["memory.current", "memory.peak", "memory.swap.current"] {
        if let Ok(content) = read(node, file) {
            if let Ok(value) = content.trim().parse::<u64>() {
                out.insert(file.to_string(), value.into());
            }
        }
    }
    if let Ok(content) = read(node, "memory.stat") {
        let stat = flat_keyed(&content);
        let keep: serde_json::Map<_, _> = ["anon", "file", "file_dirty", "file_writeback", "shmem"]
            .iter()
            .filter_map(|k| stat.get(*k).map(|v| (k.to_string(), v.clone())))
            .collect();
        out.insert("memory".to_string(), keep.into());
    }
    if let Ok(content) = read(node, "io.stat") {
        out.insert("io".to_string(), nested_keyed(&content));
    }
    for (file, key) in [
        ("cpu.pressure", "cpuPressure"),
        ("io.pressure", "ioPressure"),
        ("memory.pressure", "memoryPressure"),
    ] {
        if let Ok(content) = read(node, file) {
            out.insert(key.to_string(), nested_keyed(&content));
        }
    }
    out.into()
}

/// Parse `key value` lines, as in `cpu.stat` and `memory.stat`.
fn flat_keyed(content: &str) -> serde_json::Value {
    content
        .lines()
        .filter_map(|line| {
            let (key, value) = line.split_once(' ')?;
            Some((key.to_string(), parse_number(value)))
        })
        .collect::<serde_json::Map<_, _>>()
        .into()
}

/// Parse `name k=v k=v` lines, as in `io.stat` (keyed by device) and the
/// pressure files (keyed by `some` / `full`).
fn nested_keyed(content: &str) -> serde_json::Value {
    content
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let name = fields.next()?;
            let values = fields
                .filter_map(|kv| kv.split_once('='))
                .map(|(k, v)| (k.to_string(), parse_number(v)))
                .collect::<serde_json::Map<_, _>>();
            Some((name.to_string(), values.into()))
        })
        .collect::<serde_json::Map<_, _>>()
        .into()
}

fn parse_number(value: &str) -> serde_json::Value {
    let value = value.trim();
    if let Ok(n) = value.parse::<u64>() {
        n.into()
    } else if let Ok(f) = value.parse::<f64>() {
        f.into()
    } else {
        value.into()
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn parse_interface_files() {
        insta::assert_json_snapshot!(serde_json::json!({
            "cpu": super::flat_keyed("usage_usec 1234\nnr_throttled 5\nthrottled_usec 678\n"),
            "io": super::nested_keyed("259:0 rbytes=10 wbytes=20 rios=1 wios=2 dbytes=0 dios=0\n"),
            "pressure": super::nested_keyed("some avg10=1.50 avg60=0.00 avg300=0.00 total=99\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=7\n"),
        }));
    }
}
