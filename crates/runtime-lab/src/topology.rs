//! The declarative topology of an experiment: hosts, and the placement of each
//! task's shards onto them. See TOPOLOGY.md for the schema and its rationale.

use anyhow::Context;
use std::collections::BTreeMap;

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Topology {
    /// Catalog source (a Flow YAML file), relative to the topology file.
    pub catalog: String,
    /// Hosts of the topology, each a cgroup subtree running one reactor and
    /// one sidecar process. Names become cgroup and directory names.
    pub hosts: BTreeMap<String, Host>,
    /// Tasks to run, keyed by catalog name.
    pub tasks: BTreeMap<String, Task>,
    /// The run's gazette broker, to which every task's writes go.
    #[serde(default)]
    pub broker: Process,
    /// The run's single etcd node, of the broker.
    #[serde(default)]
    pub etcd: Process,
    /// Append rate limit, in bytes per second, of every journal that lab tasks
    /// write, overriding the production rate of their partition templates.
    /// Zero is unlimited, as in gazette's own `max_append_rate`.
    #[serde(default)]
    pub max_append_rate: Option<i64>,
}

#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Host {
    /// Raw cgroup interface files of the host node (e.g. `cpu.max`).
    #[serde(default)]
    pub cgroup: BTreeMap<String, String>,
    #[serde(default)]
    pub reactor: Process,
    #[serde(default)]
    pub sidecar: Process,
    #[serde(default)]
    pub connectors: Node,
}

#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Process {
    /// Raw cgroup interface files of this process's node.
    #[serde(default)]
    pub cgroup: BTreeMap<String, String>,
    /// Extra environment of the process.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Extra arguments of the process's subcommand.
    #[serde(default)]
    pub args: Vec<String>,
}

#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Node {
    /// Raw cgroup interface files of this node.
    #[serde(default)]
    pub cgroup: BTreeMap<String, String>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Task {
    /// Host of each shard, in shard order. Its length is the shard count, which
    /// is `rclockSplits` times the number of key splits. Shards are ordered
    /// key-major: key split `k` and r-clock split `r` is shard `k * rclockSplits + r`.
    pub shards: Vec<String>,
    /// Number of r-clock splits of each key range.
    #[serde(default = "one")]
    pub rclock_splits: u32,
    /// Named snapshot from which shard zero's RocksDB is initialized.
    /// A bare name resolves under `<data-root>/snapshots/`, and a path
    /// (containing a `/`) relative to the topology file.
    #[serde(default)]
    pub snapshot: Option<String>,
}

fn one() -> u32 {
    1
}

impl Topology {
    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        let content =
            std::fs::read(path).with_context(|| format!("reading topology {}", path.display()))?;
        let this: Self = serde_yaml::from_slice(&content)
            .with_context(|| format!("parsing topology {}", path.display()))?;
        this.validate()?;
        Ok(this)
    }

    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.hosts.is_empty(), "topology has no hosts");
        anyhow::ensure!(!self.tasks.is_empty(), "topology has no tasks");

        for name in self.hosts.keys() {
            anyhow::ensure!(
                !name.is_empty()
                    && name
                        .chars()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'),
                "host name {name:?} must be non-empty [a-z0-9-]"
            );
            // Reserved for the controller's own cgroup, and the broker's and etcd's.
            anyhow::ensure!(
                !["controller", "broker", "etcd"].contains(&name.as_str()),
                "host name {name:?} is reserved"
            );
            // Host process threads are named `<host>-sidecar`, `<host>-reactor`,
            // and `<host>-t<task>-s<shard>` (see `ShardPlan::label`). Linux
            // silently truncates thread names past 15 bytes, which would make
            // them ambiguous in `top -H` and in thread samples.
            anyhow::ensure!(
                format!("{name}-sidecar").len() <= THREAD_NAME_MAX,
                "host name {name:?} is too long: at most {} bytes, so that its thread names fit",
                THREAD_NAME_MAX - "-sidecar".len(),
            );
        }
        if let Some(rate) = self.max_append_rate {
            anyhow::ensure!(rate >= 0, "maxAppendRate {rate} must be non-negative");
        }
        for (name, task) in &self.tasks {
            anyhow::ensure!(!task.shards.is_empty(), "task {name} has no shards");
            anyhow::ensure!(
                task.rclock_splits != 0 && task.shards.len() % task.rclock_splits as usize == 0,
                "task {name} has {} shards, which isn't a multiple of rclockSplits {}",
                task.shards.len(),
                task.rclock_splits,
            );
            for host in &task.shards {
                anyhow::ensure!(
                    self.hosts.contains_key(host),
                    "task {name} places a shard on undefined host {host:?}"
                );
            }
        }
        for plan in plan_shards(self) {
            anyhow::ensure!(
                plan.label.len() <= THREAD_NAME_MAX,
                "shard label {:?} exceeds {THREAD_NAME_MAX} bytes: use a shorter host name",
                plan.label,
            );
        }
        Ok(())
    }
}

/// Linux's limit on a thread name, excluding its NUL terminator.
pub const THREAD_NAME_MAX: usize = 15;

/// A shard's placement and identity within the run.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ShardPlan {
    /// Task catalog name.
    pub task: String,
    /// Index of the task within the topology (sorted by name).
    pub task_index: usize,
    /// Index of the shard within its task.
    pub shard_index: usize,
    /// Host which runs the shard.
    pub host: String,
    /// Label naming the shard's runtime threads and files, as
    /// `<host>-t<task index>-s<shard index>`.
    pub label: String,
}

/// Flatten the topology into its shards, in (task, shard) order.
pub fn plan_shards(topology: &Topology) -> Vec<ShardPlan> {
    topology
        .tasks
        .iter()
        .enumerate()
        .flat_map(|(task_index, (task, spec))| {
            spec.shards
                .iter()
                .enumerate()
                .map(move |(shard_index, host)| ShardPlan {
                    task: task.clone(),
                    task_index,
                    shard_index,
                    host: host.clone(),
                    label: format!("{host}-t{task_index}-s{shard_index:03}"),
                })
        })
        .collect()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_and_plan() {
        let fixture = r#"
catalog: catalog.flow.yaml
hosts:
  h1:
    reactor:
      cgroup: { cpu.max: "50000 100000" }
      env: { FLOW_RUNTIME_WORKER_THREADS: "1" }
  h2:
    sidecar:
      args: ["--worker-threads=2"]
tasks:
  acmeCo/lab/sink:
    shards: [h1, h2, h1, h2]
    rclockSplits: 2
  acmeCo/lab/identity:
    shards: [h2]
    snapshot: warm
broker:
  cgroup: { cpuset.cpus: "6" }
maxAppendRate: 0
"#;
        let topology: Topology = serde_yaml::from_str(fixture).unwrap();
        topology.validate().unwrap();
        assert_eq!(topology.max_append_rate, Some(0));
        assert_eq!(topology.broker.cgroup["cpuset.cpus"], "6");
        insta::assert_json_snapshot!(plan_shards(&topology));
    }

    #[test]
    fn rejects_bad_topologies() {
        for (fixture, expect) in [
            (
                "catalog: c\nhosts: {h1: {}}\ntasks: {a/b: {shards: [h9]}}",
                "undefined host",
            ),
            (
                "catalog: c\nhosts: {h1: {}}\ntasks: {a/b: {shards: [h1, h1, h1], rclockSplits: 2}}",
                "multiple of rclockSplits",
            ),
            (
                "catalog: c\nhosts: {H_1: {}}\ntasks: {a/b: {shards: [H_1]}}",
                "must be non-empty",
            ),
            (
                "catalog: c\nhosts: {host-one: {}}\ntasks: {a/b: {shards: [host-one]}}",
                "is too long",
            ),
            (
                "catalog: c\nhosts: {broker: {}}\ntasks: {a/b: {shards: [broker]}}",
                "is reserved",
            ),
        ] {
            let topology: Topology = serde_yaml::from_str(fixture).unwrap();
            let err = topology.validate().unwrap_err().to_string();
            assert!(err.contains(expect), "{err}");
        }
    }
}
