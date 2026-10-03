# Glossary

Terms of the lab, and of the measures in `scripts/report.py`. When an
experiment introduces a measure, define it here, in these terms.

## Topology

- **Run** — one invocation of the controller, from start to stop or failure.
  A run is identified by its **run ID**, and recorded in its **run directory**.
- **Experiment** — a directory (outside of any repository) holding a topology,
  a catalog, scripts, notes, and the runs made from them.
- **Host** — a simulated machine: a cgroup subtree with a **reactor** process,
  a **sidecar** process, and a **connectors** node.
- **Node** — any cgroup of a run's tree: a host (`h1`) or one of its children
  (`h1/reactor`, `h1/sidecar`, `h1/connectors`), or `controller`, `broker`,
  or `etcd`.
- **Broker** — the run's own gazette broker (and its etcd), to which every
  task's writes go: collection partitions, ops stats, and ACK intents.
- **Shard** — as in production: a key-range × r-clock-range split of a task.
  Shard zero hosts the task's Leader (on its host's sidecar) and its RocksDB.
  A shard's **label** (`h2-t1-s002`: host, task index, shard index) names
  its tokio runtime threads and files.
- **Session** — as in production: one `Join` of every shard of a task, through
  to every shard's `Stopped`. A run's sessions are numbered by **revision**.
- **Fail-stop** — any unexpected failure ends the whole run, with no restart. A
  session which stops itself cleanly is followed by the next, and is no failure.

## Time

- **Source clock** — the wall-clock time at which a source document was
  written by its producer, as recorded by the `uuid::Clock` of its document
  UUID. It's the time axis of the source data, distinct from the wall-clock
  time of the run.
- **Source skew** — for one task and one cohort, the maximum minus the minimum,
  across the task's shards, of the source clock of the latest document each
  shard's Slice has read (`shuffle_slice_last_source_published_at_time_seconds`).
  Measured in seconds of source clock. It measures how unevenly shards progress
  through the source data, which is how shards which are coupled by
  transactions diverge. Only shards whose Slice reads a journal report a
  clock. Clocks are comparable only within a **cohort** (bindings of equal
  priority and read delay), as other cohorts diverge by design.
- **Source lag** — the run's wall-clock time minus the minimum source clock of
  a task and cohort: how far behind the source's present the slowest reader is.
  It's large during a backfill of historical data, by design. Reads begin at
  fragment boundaries, and documents before a binding's `notBefore` are read
  (and advance the clock) but not processed, so lag can briefly exceed the
  age of `notBefore`.

## Measures

- **Committed throughput** — source documents (and their bytes) per second of
  run time, from the Leader's per-transaction stats documents, counted at
  the *close* of a transaction which committed (its commit follows the
  close, by the commit's duration). Documents read into a transaction which
  never commits aren't counted: a Leader writes stats before it commits, so
  a task's last transaction of a failed or live run, which may not have
  committed, is reported apart. Reading rates (`shuffle_slice_bytes_read`)
  are something else.
- **Transaction cadence** — transaction durations (a stats document's
  `openSecondsTotal`) and commits per minute, of transactions which read
  something. **Empty transactions** (common as a fresh session starts) are
  counted apart.
- **left / right / out** — per-binding document and byte counts of a
  transaction, as in production stats. For a materialization: *left* is
  loaded from the endpoint, *right* is read from the source, and *out* is
  stored to the endpoint, after combining by key within the transaction (so
  *out* is less than *right* when a transaction reads a key more than once).
  For a derivation: *left* doesn't apply, *right* is
  read from the source (a transform's `input`), and *out* is written to the
  derived collection, after combining. For a capture: *right* is captured
  from the connector, and *out* is published to the collection, after
  combining (so *out* over *right* is what combining saved).
- **Shuffle pressure** — a shard's shuffle log **disk backlog** (bytes written
  but not yet reclaimed, `shuffle_log_disk_backlog_bytes`) and its **stalled
  reads** (journal reads which are behind yet parked,
  `shuffle_slice_stalled_reads`). A stalled read head-of-line blocks its Slice.
  A Slice parked by a Log's disk back-pressure is *not* counted as stalled.
- **Append backpressure** — per journal, bytes appended per second
  (`gazette_append`, by every appending process), and the share of them which
  the broker's flow control delayed (`gazette_append_delayed`, prorated by
  chunk count). A share which stays high is what drives a partition split,
  and splits show as the collection's **partitions** growing over the window
  (`samples/journals.ndjson`, and `partitionsChanged` events).
- **Stall** — a task whose committed source clock stops advancing while it's
  behind its source (see WORKFLOW.md, "Reading a stall").
- **CPU cores** — a node's CPU time per second of run time (`cpu.stat`
  `usage_usec`).
- **Throttling** — a node's CPU bandwidth-limit throttling (`cpu.stat`):
  throttled seconds per second of run time, and the fraction of *enforced*
  periods in which it was throttled. Periods are enforced only while a quota
  is set, so that fraction describes the capped time alone. Compare throttled
  seconds across windows instead.
- **Pressure** — a node's PSI `some` stall time (`cpu.pressure`, `io.pressure`,
  `memory.pressure`), as a fraction of run time: how often at least one of its
  tasks was waiting for the resource.
- **CPU by thread** — CPU cores by thread name within a process. It attributes
  a reactor's CPU to its shards' runtimes.

## Scale-out

A task's shards are allocated equal slices of work, but run on unequal hosts
with varying load. Shards progress in lockstep through the source, so by design
a task can go no faster than its slowest shard. Faster shards are back-pressured,
and should yield their excess CPU to other work on their host. These measures
separate that designed-in cost of unequal hosts from overhead we can remove.
They apply while a task is behind its source (as in a backfill): a caught-up
task is bound by its input rate instead.

- **Shard capacity** — the committed throughput one shard sustains alone on its
  host, under that host's limits and background load. It must be measured
  apart from the multi-shard run (for example, by single-shard runs under each
  host's limits), because within that run a shard's rate already reflects
  coupling to the others.
- **Pacing shard** — the shard of a task with the lowest shard capacity. It sets
  the pace of every other shard.
- **Lockstep ceiling** — the task's shard count times the pacing shard's
  capacity: the task's maximum committed throughput, by design. It's the
  baseline for judging linear scale-out, rather than the sum of shard
  capacities, whose difference is the designed-in cost of unequal hosts.
- **Scale-out efficiency** — committed throughput over the lockstep ceiling. At
  1.0, shards cost nothing to coordinate beyond waiting on the pacing shard, and
  any shortfall is overhead of the runtime.
- **Slack** — a shard's capacity minus the rate it actually runs at: headroom on
  its host which back-pressure holds it back from using.
- **Hold-back overhead** — the CPU per committed byte of a back-pressured shard,
  in excess of the pacing shard's: the cost of waiting (polling, wake-ups,
  context switches). Ideally zero, so that a shard's slack returns to its host.
