# Workflow

How a user and an agent use the lab together. The user brings an effect seen
in production, and credentials. The agent owns an experiment: it forms a
hypothesis, reproduces the effect, and then prototypes, evaluates, and
verifies a change.

## The loop

1. **Describe the effect.** What was observed, on which task, and with what
   shape: shard count, bindings, source collections, transaction settings,
   connector behavior, and host load. The user may give the agent the task's
   spec, paste it, or just describe it.
2. **Hypothesize** a mechanism, and what a reproduction would show in the
   [measures](GLOSSARY.md#measures).
3. **Reproduce.** Write the experiment's catalog and topology, then run
   until the measures show the effect. Remove things until only what's needed
   for the effect remains: that isolates it.
4. **Prototype** a change to runtime-next, shuffle, or a connector, on a branch.
5. **Evaluate and verify:** run the same reproduction, from the same starting
   state, with and without the change, and compare measures.

Record hypotheses, runs, and findings in the experiment directory
(`notes.md` works well). A later session can then pick up where this one
left off.

## Policy: customer data

An experiment names customer tenants, collections, and journals in its catalog,
topology, scripts, notes, logs, and measures. **None of it may ever be written
into a git-tracked file**: not source, tests, fixtures, docs, or commit
messages. That rule is absolute. So:

- Experiments live outside of any repository, for example `~/flow-lab/<name>/`.
  The controller refuses to write a run directory (or data directory) inside
  a git work tree. That's a guard against accidents, not a license: the rule
  applies to everything an agent writes.
- When a finding needs a checked-in example (a test, or a repro in an
  issue), use a fictitious tenant like `acmeCo/` and invented names that keep
  the relevant shape (name length, nesting depth, key structure).
- The examples in `examples/` read `demo/wikipedia/recentchange`, which is a
  public Estuary demo collection.

## Setup (once per VM)

1. **Delegate cgroup controllers.** Systemd delegates `cpu memory pids` to users
   by default. The lab also needs `cpuset` and `io`. Check whether this VM is
   already set up, and set it up if not:

   ```bash
   crates/runtime-lab/scripts/setup-cgroups.sh --check || crates/runtime-lab/scripts/setup-cgroups.sh
   ```

   It uses sudo, and doesn't require a logout. The controller warns (in its
   log, and in `events.ndjson`) when a controller is missing.
2. **Use a local SSD for data.** Shuffle logs, RocksDBs, and snapshots go to
   the **data root** (`--data-root`, or `RUNTIME_LAB_DATA_ROOT`). Put it on a
   local SSD: on a network block device (GCP Persistent Disk / Hyperdisk, AWS
   EBS) shuffle-log throughput is a fraction of a local SSD's, and silently
   becomes the experiment's bottleneck. The controller warns when the data
   root is on a network disk or tmpfs. If the VM has no local SSD, tell the
   user, since it changes what can be concluded.

   ```bash
   lsblk -o NAME,SIZE,MODEL,MOUNTPOINT   # e.g. GCP local SSDs are "nvme_card*"; "-pd" is a network disk
   ```
3. **Build** (release builds are the only meaningful ones for measurement):

   ```bash
   mise exec -- cargo build --release -p runtime-lab
   ```

   This builds `runtime-lab` and the reference connectors side by side in
   `$CARGO_TARGET_DIR/release`. `CARGO_TARGET_DIR` is set only under `mise`,
   within the repository, while experiments live outside of it, so find it
   once and export what later commands need:

   ```bash
   TARGET=$(cd <repo> && mise exec -- bash -c 'echo $CARGO_TARGET_DIR')
   export PATH=$TARGET/release:$PATH                    # `runtime-lab` for commands below
   export RUNTIME_LAB_BIN=$TARGET/release/runtime-lab   # for scripts (labrun.py)
   ```

   The controller puts its own directory on `PATH` for the processes it
   starts, so `local:` connector commands resolve.
4. **Provide gazette and etcd**, which every run starts as its broker. Build
   `gazette` into `$GOBIN`, and point the controller at both (or put them on
   `PATH`):

   ```bash
   (cd <repo> && mise run build:gazette)
   export RUNTIME_LAB_GAZETTE=$(cd <repo> && mise exec -- bash -c 'echo $GOBIN')/gazette
   export RUNTIME_LAB_ETCD=$(cd <repo> && mise which etcd)
   ```

## Credentials

The user provides either a token or a flowctl profile:

- `FLOW_AUTH_TOKEN`: a JWT access token (which expires, typically within an
  hour), or a base64 refresh token (as `flowctl auth token` prints). Prefer
  a refresh token for runs longer than a few minutes.
- `--profile <name>`: a flowctl profile (`~/.config/flowctl/<name>.json`),
  read and never written.

`FLOW_AUTH_TOKEN` wins when both are set, as it does for flowctl. The refresh
token must be *multi-use*, because every sidecar refreshes independently. A
single-use token is rejected up front. Credentials are never written into a
run directory; don't put them in notes or scripts either.

The token must be able to **read** the source collections. Journals are read
directly, and nothing is written to production: every write goes to the run's
own broker.

## An experiment

```
~/flow-lab/<experiment>/
├── catalog.flow.yaml      # the tasks under test (and derived collections)
├── topology.yaml          # hosts and placement (TOPOLOGY.md)
├── notes.md               # hypotheses, runs, findings
├── *.py                   # scripted runs
└── runs/<run-id>/         # one per run (below)
<data-root>/
├── runs/<run-id>/         # bulk data: shuffle logs, temp files, shard-zero RocksDBs,
│                          #   journals files, the broker's etcd and fragments
└── snapshots/<name>/      # named snapshots: rocksdb/ and journals.json
```

Use `examples/` as a reference for creating an experiment's catalog and topology.

Commands below run from the experiment directory, and name the lab's scripts
by their path in the repository (`<repo>/crates/runtime-lab/scripts/`).

The catalog is ordinary Estuary YAML, written and changed by hand. Build and
publication IDs are above every live one. Collections it names resolve live
through the control plane, and its tasks use `local:` connectors, typically
the reference connectors:

- **`materialize-sink`**: loads nothing, and discards every Store.
- **`derive-identity`**: publishes each source document unchanged.
- **`capture-fake-postgres`**: a fake of source-postgres, which simulates a
  database having declared tables (or a canned set), and emits its synthetic
  backfill and WAL as fast as the runtime reads it: ascending key-ordered
  backfill chunks, each with intermixed WAL updates, and checkpoints.
  Documents are generated from the bound collection's top-level projections,
  and are deterministic in the configuration's `seed`.

All are cheap, speak the protobuf codec (`local: {protobuf: true}`), and are
meant to be forked. Copy one into `src/bin/<name>.rs` (or a directory,
`src/bin/<name>/`), change it (each lists its natural points of change),
rebuild, and name it in the catalog. Keep its
`runtime_lab::connector::serve` call (or `enter_cgroup`, for a connector with
its own serving loop), which joins the host's connectors cgroup. Match a
production connector's protocol behavior where the effect depends on it:
Loaded responses, slow commits, acknowledgement timing, or connector state.

A capture has exactly one shard, and writes its collections to the run's
broker, where nothing reads them: a lab capture's output can't be the source
of another lab task. Its partitions are throttled at production's append
rate unless the topology's `maxAppendRate` says otherwise, so a saturating
capture backs up behind journal flow control, and splits its partitions,
as in production.

## Running

```bash
cd ~/flow-lab/example
runtime-lab controller topology.yaml --profile <name> --data-root /mnt/disks/local-ssd/flow-lab \
  --duration 5m --label baseline
```

Its run ID (`<UTC time>-<label>`) names the run directory, `runs/<run-id>/`:
it's in the controller's first log line (`runStarted`), or `ls -t runs | head -1`.
The controller builds the catalog (any error is fatal), lays out the hosts,
starts their processes, and drives every task's sessions until `--duration`
elapses or it receives SIGINT or SIGTERM. Then it stops every session, which
may take a transaction's duration, and tears down. It exits zero only on a
clean stop.

**It is fail-stop.** A host process which exits, a shard stream which
fails, or sampling which fails (say, a full disk), ends the run immediately
and non-zero, with the cause in
`events.ndjson` (`runFailed`) and the host logs. No process or stream is ever
restarted. When a failure wasn't intended, it's a finding. When a stall is
intended, the system shows it as it is: a wedge that holds still is easier
to inspect than one that keeps restarting.

A *session* which stops itself cleanly is different, and isn't a failure: as
in production, the next session starts at the next revision
(`sessionStopped` with `requested: false`, then `sessionStarted`). Shards
stop sessions for reasons such as shedding pinned shuffle segments, or
completing an idempotent-recovery replay. The cause is logged by the shard
(`stopping session` in the reactor log), and the report counts such stops per
task. A task which keeps restarting its sessions without advancing its
committed source clock is stuck, even though no failure is recorded.

### A run directory

```
runs/<run-id>/
├── manifest.json         # pids, cgroups, ports, sockets, directories: the run's address book
├── topology.yaml         # the topology, as run
├── events.ndjson         # the run's timeline: sessions, stops, failures, script actions
├── stats.ndjson          # every task's per-transaction stats documents (and ACKs),
│                         #   tailed from its ops stats journal
├── broker.log, etcd.log  # the broker's and etcd's logs
├── samples/
│   ├── cgroups.ndjson    # each node's cpu.stat, memory, io.stat, and pressure (--sample-interval)
│   ├── metrics.ndjson    # each process's runtime_*, shuffle_*, gazette_* metrics, and the broker's
│   ├── journals.ndjson   # the broker's journals: partitions by collection, and their revisions
│   └── threads.ndjson    # each process's CPU ticks by thread name
└── hosts/<host>/
    └── reactor.log, sidecar.log   # process logs (RUST_LOG), including connector logs
```

### Observing

- **Report:** `<repo>/crates/runtime-lab/scripts/report.py <run-dir>` prints the headline measures over a
  window which skips start-up (`--skip`, default 30s; `--until` bounds it).
  It also prints **phases** split at each script event (so an intervention's
  before, during, and after are measured apart), and a **timeline** of
  `--bucket` intervals. Committed throughput is counted at each committed
  transaction's close, so it's lumpy in buckets or phases which span few
  transactions.
  All offsets are from the run's start, as in the event log. `--json` emits
  everything for further processing. It works on a live run too. Copy it into
  the experiment and extend it freely.
- **Live dashboards:** each process's service-kit admin surface is at
  `http://127.0.0.1:<adminPort>/` (ports are in the manifest). It shows
  in-flight handlers with their phases and event tracks, per-handler trace
  levels, and `/metrics`. The same is available as JSON:
  `/debug/handlers.json` (every handler), and
  `/debug/handlers/<id>/detail.json` (one handler's fields and event tracks).
  Event tracks are short rings, so to keep their history, poll them into the
  run directory from a script. The dashboards are often the most direct view
  of a wedged session: its phase, unresolved hints, and frozen checkpoint.
- **Kernel tools** attach by PID or cgroup: `pidstat -t -p <pid>`,
  `perf top -p <pid>`, `perf record -a -G <cgroup path relative to /sys/fs/cgroup>`,
  `systemd-cgtop`, `bpftrace`, `top -H`. Thread names carry the host: each
  shard's runtime (`h2-t1-s002`), the sidecar (`h2-sidecar`), and each
  process's main thread (`h2-reactor`, `h2-sidecar`).

### Reading a stall

A task is **stalled** when its committed source clock stops advancing (the
report's timeline `clock` column) while it's behind its source. The
surrounding measures distinguish kinds of stall:

- **Busy** (hosts' CPU high): re-reading, or restarting sessions
  (`selfStopped`) without progress.
- **Idle** (hosts' CPU near zero): a wedge, typically waiting on something
  which can't happen. Shuffle log back-pressure (disk backlog at its limit)
  parks Slices without counting as *stalled reads*, which only count journal
  reads that are parked while behind.
- **Source quiet**: when tailing (lag near zero), no commits can simply mean
  the source isn't being written. Check that reads are tailing
  (`shuffle_slice_tailing_reads`) and that the Leader's bytes behind
  (`runtime_leader_behind_bytes`) is near zero, before calling it a stall.

service-kit drops a metric which hasn't been updated for 10 minutes, so during
a long stall the gauges of stuck components disappear from `metrics.ndjson`.
Their absence is itself a signal.

### Intervening

Act on the run directly, using the manifest:

```bash
M=runs/<run-id>/manifest.json
CG=$(jq -r '.hosts.h2.reactor.cgroup' $M)
echo "50000 100000" > $CG/cpu.max          # cap h2's reactor to half a core
echo "max 100000"   > $CG/cpu.max          # and release it
kill -KILL $(jq -r '.hosts.h1.sidecar.pid' $M)   # inject a fault (the run fails-stop)
```

Prefer this over adding controller code. When you intervene by hand, note it
(and its time) in `notes.md`, or use a script, which records it in the event log.

## Scripted runs

A sequence of events which must repeat exactly is a Python script.
`scripts/labrun.py` has the helpers: start a controller with a known run ID,
wait for its manifest, address nodes and processes by name, act, and record
each action in `events.ndjson`.

```python
import sys; sys.path.insert(0, "<repo>/crates/runtime-lab/scripts")
import labrun

with labrun.start("topology.yaml", label="cap", duration="4m", profile="me",
                  data_root="/mnt/disks/local-ssd/flow-lab") as run:
    run.sleep_until(60)                               # seconds since the run was ready
    run.set_cgroup("h2/reactor", "cpu.max", "50000 100000")
    run.sleep_until(120)
    run.set_cgroup("h2/reactor", "cpu.max", "max 100000")
```

`scripts/example_cap_cpu.py` is a complete example, ending with a report
(`--node` defaults to the first host's reactor).

A `labrun.Run` offers: `sleep_until(seconds)`, `elapsed()`, `stop()` (a clean
stop, as SIGINT), `wait()`; `cgroup(node)`, `pid(process)`, `shards(task)`,
and `manifest` for addressing; and `set_cgroup(node, file, value)`,
`signal(process, sig=SIGKILL)`, and `event(name, **fields)` as actions.
Nodes are `h1`, `h1/reactor`, `h1/sidecar`, `h1/connectors`, `broker`, and
`etcd`, and processes are `h1/reactor`, `h1/sidecar`, `broker`, and `etcd`.
A host on which no shard is placed has no reactor (the manifest omits it).
Conditions ("once source skew passes 30s") are ordinary code over the files
of the run directory. A script which needs to know something the manifest
doesn't say should get it added to the manifest, instead of guessing paths.

## State: snapshots, resume, and faults

Shard zero's RocksDB holds the task's committed state: its read frontier,
connector state, and anything else that production recovers from the recovery
log. In the lab it's a directory, which every session of a run reopens. A
task's **journals file** holds the partition specs of the collections it
writes, such as the partitions its splits created. It's the rest of the
task's state: shard zero's recovered ACK intents name those journals, which a
later run must restore.

- **Fresh:** without a `snapshot`, a run starts empty and reads from each
  binding's `notBefore` (or the start of the collection).
- **Snapshot:** `snapshot: <name>` copies `<data-root>/snapshots/<name>/rocksdb`
  into the run, and restores its `journals.json`. Every run from one snapshot starts from exactly the same state, so
  before-and-after comparisons compare like with like.
- **Taking one:** a snapshot is a copy of *one task's* shard-zero RocksDB and
  journals file (`.tasks.<task>.rocksdbDir` and `.journalsFile` of the
  manifest), taken while it isn't running, for example after the run stops.
  Take one of each task you'll start from, and name each in its task's
  `snapshot:` field:

  ```bash
  mkdir -p <data-root>/snapshots
  jq -r '.tasks | to_entries[] | "\(.key) \(.value.rocksdbDir) \(.value.journalsFile)"' runs/<run-id>/manifest.json |
    while read task dir journals; do
      snap="<data-root>/snapshots/${task##*/}-warm"
      mkdir -p "$snap" && cp -a "$dir" "$snap/rocksdb" && cp "$journals" "$snap/journals.json"
    done
  ```

  (This names `acmeCo/lab/sink`'s snapshot `sink-warm`.)
- **Resume:** `--resume <run-dir>` (a path, relative to the current directory)
  restores a prior run's journals files, and reopens its shard-zero RocksDBs *in place*, as a restarted
  production shard recovers its log: the task continues from what that run
  committed, including after it failed. Because it's in place, the resumed run
  advances that state, and a second resume of the same run starts from where
  the first left off. To return to a post-fault state more than once, capture
  it as a snapshot before resuming.
- **Which start a run had** is recorded: each task's `taskStart` event, and its
  manifest `.tasks.<task>.start`, say `fresh`, `snapshot:<name>`, or
  `resumed:<run-id>`. To confirm a run continued from its state, compare the
  source clock and bytes behind of its *first* commit with the *last* commit
  of the run which produced the state: they should be close. (For a resumed
  run the report prints both.) A fresh run's first commit instead has the
  most bytes behind, and a source clock somewhat past `notBefore`, since its
  first transaction can span hours of source data.
- **Faults:** kill a host process (`SIGKILL`; `run.signal("h1/sidecar")` in a
  script) or inject an error at a precise point of the protocol (on an
  experiment branch). The run fail-stops, and
  its state is left as the fault left it. A `--resume` run then exercises
  recovery: startup reconciliation, idempotent replay of a hinted
  transaction, and gapped reads. Restart loops are scripts over these pieces.

## Changing the system under test

- Change runtime-next, shuffle, or a connector on a branch, and rebuild.
  Runs record the topology but not the build, so note which commit a run used.
- Instrumentation which an experiment needs (a metric, an event, a log line)
  is added on the experiment's branch. The baseline adds none to the systems
  under test, and new production instrumentation needs its own production
  motivation.
- If the lab itself needs a new capability, prefer the smallest joint: a
  topology field, a manifest field, a script helper, or a report measure.

## Fidelity

What the lab models: the production code of runtime-next shards and Leaders,
and of shuffle Sessions, Slices, and Logs; their process boundaries (the
controller ⇄ shard socket, loopback gRPC between sidecars); per-shard tokio
runtimes; real journal reads of real data; journal writes through
to a real gazette broker, with its flow control and automatic partition splits;
cgroup-imposed resource limits; and a connector's protocol behavior.

Journal reads go to production brokers and cloud storage, whose latency and
bandwidth vary. Judge measures across repeated runs, not single ones.

What it doesn't:

- Recovery-log recording, and its cost on shard zero.
- The Go reactor: CGO, its UDS hop, and the Go runtime sharing the process.
- Data-plane AuthN, and TLS.