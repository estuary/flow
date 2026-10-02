# runtime-lab

A reproduction lab for `runtime-next` and `shuffle`. Take a behavior observed
in production — a stall, a throughput shortfall, a load imbalance — and put it
into a controlled, single-VM environment: reproduce it, then prototype, evaluate,
and verify a fix.

A run reads **real collections** (the effects that matter often live in the
source data), through a user's token, and drives the **production code paths**
of runtime-next and shuffle. Its writes go to the run's own gazette broker,
never to production: tasks use no-op reference connectors that replicate the
protocol effects of a real connector, and publish through production's
publisher, so partition creation, journal flow control, and automatic
partition splits behave as in production.
Hosts are cgroup subtrees, so load imbalances are imposed from outside the
processes, with ordinary cgroup files.

- **[WORKFLOW.md](WORKFLOW.md)** — how to use the lab: setup, the reproduction
  loop, observing and intervening, scripted runs, snapshots, and policy.
  Start here.
- **[TOPOLOGY.md](TOPOLOGY.md)** — the topology file, and the joints an
  experiment is likely to adjust.
- **[GLOSSARY.md](GLOSSARY.md)** — the lab's terms and measures, defined once.

## Architecture

```
systemd user scope runtime-lab-<run-id>   (Delegate=yes: the run's cgroup root)
├─ controller/          controller process: builds the catalog, drives sessions, samples
├─ etcd/                the run's single etcd node, of the broker
├─ broker/              the run's gazette broker, to which every task's writes go
├─ h1/                  a host
│  ├─ reactor/          reactor process: one Shard service + tokio runtime per shard
│  ├─ sidecar/          sidecar process: Leader + shuffle services
│  └─ connectors/       the host's connector processes (they join it as they start)
└─ h2/ ...
```

The controller is the Go controller's analog: it dials each shard's Unix
socket and sends `SessionLoop`, `Join`, `Task`, and `Stop`, exactly as
`go/runtime/materialize_v2.go` does. A capture's session is of a single shard,
which has no Leader (`capture_v2.go`). A reactor is a Go reactor's per-shard
`TaskService`s minus Go. A sidecar is `runtime-sidecar`, authorized by a user
token instead of data-plane keys. Shards and sidecars talk over loopback gRPC,
as they do across machines in production. Shards and Leaders publish to the
run's broker over loopback gRPC, through `runtime_next::JournalPublisherFactory`.

## Layout

```
src/
├── main.rs          # multi-call binary: `controller`, `reactor`, `sidecar`
├── controller/
│   ├── mod.rs       # run lifecycle: scope, cgroups, build, hosts, fail-stop teardown
│   ├── session.rs   # per-task session driver over Shard streams (Capture / Materialize / Derive)
│   ├── broker.rs    # the run's gazette + etcd: stats tails, journals files, fragment reclamation
│   └── sampler.rs   # samples/{cgroups,metrics,threads}.ndjson
├── reactor.rs       # a host's shards
├── sidecar.rs       # a host's Leader and shuffle services
├── topology.rs      # topology schema and shard placement
├── catalog.rs       # in-memory catalog build (flowctl's live-catalog resolution)
├── auth.rs          # FLOW_AUTH_TOKEN / flowctl profile credentials
├── cgroup.rs        # cgroup tree, spawning into a leaf, accounting samples
├── layout.rs        # run directory layout, manifest, events, git + storage guards
├── connector.rs     # reference connectors' serving loop, which joins the connectors cgroup
└── bin/
    ├── materialize-sink.rs   # reference materialization: a pure sink
    ├── derive-identity.rs    # reference derivation: an identity transform
    └── capture-fake-postgres/  # reference capture: a fake of source-postgres (no database)
scripts/
├── setup-cgroups.sh      # one-time: delegate cpuset + io cgroup controllers
├── report.py             # a run directory's headline measures
├── labrun.py             # helpers for scripted runs
└── example_cap_cpu.py    # a scripted run: cap a node mid-run, then release it
examples/                 # a reference (not run as is): fictitious acmeCo/ catalog and topologies
```

## Non-obvious details

- **The controller re-executes itself** under `systemd-run --user --scope -p
  Delegate=yes`, which gives it a cgroup subtree it owns outright. It then
  moves itself into `controller/`, because cgroup v2 forbids a node with member
  processes from delegating controllers to children.
- **Processes start in their leaf cgroup** (between fork and exec), and die
  with the controller (`PR_SET_PDEATHSIG`). Connectors are children of the
  reactor, and move themselves into `connectors/` as their first act (within
  `connector::serve`), so their CPU never counts against the reactor.
- **Shard zero's RocksDB outlives its stream.** A shard removes the RocksDB
  path it's given as its stream ends: production's recovery log is the durable
  state there, and the directory is disposable. Here the directory stands in
  for one recovered by log playback, which a snapshot captures or a later run
  reopens. So the controller gives shard zero a symlink to it
  (`<data-dir>/rocksdb/t<N>.link`), and `std::fs::remove_dir_all` removes the
  symlink, never what it points to.
- **The run's broker is real gazette** (`gazette serve`), file-only, without
  auth keys (so without AuthZ), and with replication bounded to one. Its
  fragments are reclaimed as they persist, except for `ops/` journals:
  nothing reads the lab's collections. The controller tails each task's ops
  stats journal (`ops/lab/stats/...`, set as the shard labeling's stats
  journal, as activation does) into `stats.ndjson`. Because Join labeling
  carries it, a task's built spec needs no ops collections.
- **Partitions are run state.** Each task's journals file (`journals/t<N>.json`
  of the data directory) holds the partition specs of the collections it
  writes, rewritten every sample. A `--resume` or a snapshot restores them, so
  shard zero's recovered ACK intents name journals which exist, and a task
  resumes with the partitions its splits left. A restored partition takes the
  run's append rate.
- **Allocation is jemalloc**, as within production's reactor (where shards are
  served through `bindings`). Allocation-heavy paths, like combiner arenas
  churned per-transaction, behave very differently under glibc malloc.
- **Fail-stop, no restarts.** An unexpected exit, stream error, or sampling
  failure ends the run non-zero, recorded in `events.ndjson`. A failure an experiment didn't
  intend can't go unnoticed. An experiment which wants restarts scripts them.
- **No control API.** A running experiment is controlled through its
  manifest's cgroup paths and PIDs; scripts are Python (see WORKFLOW.md).
- **No new instrumentation of the systems under test.** The baseline measures
  use only existing metrics, the Leader's stats documents, and kernel
  accounting. Experiment-specific instrumentation belongs on the experiment's
  own branch.
- **What isn't modeled:** recovery-log recording (and so its cost), the
  CGO / UDS hop into Go, the Go runtime sharing the reactor process, data-plane
  AuthN, broker replication and cloud fragment storage, and readers of the
  lab's own collections.
