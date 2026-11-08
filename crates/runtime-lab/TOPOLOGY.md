# Topology

A topology file declares an experiment's hosts, and the placement of each
task's shards onto them. It lives in the experiment directory, and the
catalog path it names is relative to it.

```yaml
catalog: catalog.flow.yaml

hosts:
  h1:
    cgroup: { cpu.max: "200000 100000" }        # the host node: two cores' bandwidth
    reactor:
      cgroup: { cpuset.cpus: "0-1" }            # raw cgroup interface files
      env: { FLOW_RUNTIME_WORKER_THREADS: "1" } # environment of the process
      args: []                                  # extra arguments of `runtime-lab reactor`
    sidecar:
      cgroup: { cpuset.cpus: "0-1", memory.high: "4G" }
      args: ["--worker-threads=2", "--shuffle-disk-limit=4294967296"]
    connectors:
      cgroup: { cpuset.cpus: "0-1" }
  h2: {}                                        # an unconstrained host

tasks:
  acmeCo/lab/sink:                  # a capture, materialization, or derivation of the catalog
    shards: [h1, h2, h1, h2]        # the host of each shard, in shard order
    rclockSplits: 2                 # r-clock splits of each key range (default 1)
    snapshot: warm                  # initialize shard zero's RocksDB from a snapshot

broker:                             # the run's gazette broker
  cgroup: { cpuset.cpus: "6" }
  args: ["--log.level=warn"]        # extra arguments of `gazette serve`
etcd: {}                            # the broker's etcd node
maxAppendRate: 0                    # bytes/s of every journal tasks write; 0 is unlimited
```

## Fields

- **`hosts.<name>`** — a host, named `[a-z0-9-]+` of at most 7 bytes, so that
  its thread names (`h1-sidecar`, `h1-t0-s000`) fit Linux's 15-byte limit.
  Each host runs one sidecar, and one reactor if any shard is placed on it.
- **`cgroup`** — raw cgroup v2 interface files, written as the node is created,
  before its process starts. They're the same files an operator or script
  writes during a run, so there's one vocabulary for starting limits and live
  changes: `cpu.max`, `cpu.weight`, `cpuset.cpus`, `memory.high`,
  `memory.max`, `io.max`, `io.weight`, and so on. A host node's files bound
  all of its children together.
- **`reactor.env` / `sidecar.env`**, **`reactor.args` / `sidecar.args`** —
  plain environment and arguments. No special fields: run `runtime-lab
  sidecar --help` for sidecar arguments.
- **`tasks.<name>.shards`** — its length is the task's shard count, which is
  `rclockSplits` × the number of key splits. Shards are split evenly, and
  ordered key-major: key split *k*, r-clock split *r* is shard
  *k* × `rclockSplits` + *r*. Shard zero hosts the task's Leader on its host's
  sidecar. A capture has exactly one shard, as runtime-next's capture sessions
  are single-shard and leaderless.
- **`broker` / `etcd`** — process blocks (`cgroup`, `env`, `args`) of the
  run's gazette broker and its etcd node, which are nodes at the run's root
  (`broker/`, `etcd/`), apart from every host. `args` are appended to
  `gazette serve` and `etcd`.
- **`maxAppendRate`** — the append rate limit, in bytes per second, of every
  partition the run's tasks write. Unset, partitions keep production's rate
  (4MiB/s), which back-pressures a fast task and drives automatic splits.
  `0` is unlimited: no journal flow control at all. The broker's own
  `--broker.max-append-rate` (in `broker.args`) can only lower it.
- **`tasks.<name>.snapshot`** — a named snapshot, under
  `<data-root>/snapshots/<name>`, or a path (containing a `/`) relative to
  the topology file. It's copied into the run, and
  never modified. Without one, shard zero starts empty. `--resume` overrides
  it (see WORKFLOW.md).

## Joints worth adjusting

Common knobs of a reproduction, by where they live:

| Joint | Where |
| --- | --- |
| Shard count, placement, co-location, noisy neighbors | `tasks.*.shards`, several tasks on shared hosts |
| R-clock scale-out (read-only derivation transforms) | `rclockSplits` |
| CPU bandwidth or cores of a host, or of one process | `cgroup: {cpu.max, cpuset.cpus, cpu.weight}` at a host or child node |
| Memory pressure | `memory.high` (reclaims and throttles above it), `memory.max` (OOM-kills: a failure) |
| Disk bandwidth or IOPS | `io.max` (`"<major>:<minor> rbps=… wbps=… riops=… wiops=…"`) |
| Tokio worker threads per shard | reactor `env: {FLOW_RUNTIME_WORKER_THREADS}` |
| Journal flow control, and so partition splits | `maxAppendRate` |
| Broker CPU or IO, kept apart from hosts | `broker: {cgroup: ...}` |
| Sidecar worker threads | sidecar `args: ["--worker-threads=N"]` |
| Shuffle disk limit | sidecar `--shuffle-disk-limit`, or the task's `shards.shuffleDiskLimit` |
| Transaction durations | the task's `shards.minTxnDuration` / `maxTxnDuration` in the catalog |
| Read priorities, read delays, `notBefore` / `notAfter` | the catalog's bindings / transforms |
| Connector behavior | fork a reference connector (`src/bin/`), and name it in the catalog |
| Starting state | `snapshot`, or `--resume` |
| Logging | `RUST_LOG` of the controller, inherited by every process |

Two cautions:

- `available_parallelism` (and so a sidecar's default worker count) honors a
  cpuset and a `cpu.max` quota applied *before* the process starts, and not
  later changes. Set starting limits in the topology, rather than immediately
  after the run starts.
- `cpuset.cpus` of sibling hosts may overlap. Hosts that share CPUs contend
  for them, which may be exactly what's intended.
