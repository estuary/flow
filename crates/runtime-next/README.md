# runtime-next

Rust task runtime that replaces the Go-driven transaction loop. For
derivations and materializations, a per-task **Shuffle Leader** coordinates
multi-shard transactions; the Go runtime becomes a thin shim for shard
lifecycle and ops logs. Captures use a simpler model with independent
per-shard transaction loops.

Shards also auto-split collection partition journals that stay append-rate
throttled, adding write throughput without operator action.

This crate hosts both sides of the runtime-v2 protocol:

- **`Shard`** — per-shard, controller-facing gRPC service. One instance per
  assigned shard, embedded in the Go reactor process via CGO over a per-shard
  Unix domain socket.
- **`Leader`** — sidecar gRPC service. One sidecar per reactor machine
  (systemd-supervised, lifetime-bound to the reactor), serving every task
  whose shard zero is assigned to a reactor on that machine.

"Controller" throughout this crate is whatever drives a shard's lifecycle:
the Go runtime in production, an in-process driver such as `flowctl
preview`, or a unit-test harness. The crate is agnostic to which.

## Architecture

```
Reactor machine
  ├─ reactor process(es) (Go + Rust via CGO)
  │    ├─ Go: Etcd watch, shard lifecycle, ops logs (OUTSIDE_TXN)
  │    └─ Per-shard TaskService (this crate, via CGO):
  │         ├─ Connector driving, combining, publishing CONTINUE_TXN docs
  │         ├─ In-memory state (connector state, checkpoints, max-keys)
  │         ├─ Derive/materialize: state persisted via Leader's Persist/Persisted
  │         │    (RocksDB + Go Recorder on the shard hosting the recovery log)
  │         └─ Capture: per-shard RocksDB with Go Recorder
  │
  └─ runtime sidecar process (Rust, one per machine)
       ├─ Shuffle Leader service (this crate, per-task via Join)
       ├─ Shuffle service (`crates/shuffle`, Session/Slice/Log RPCs)
       └─ Listens on the fixed sidecar port, shared fleet-wide
```

The Gazette consumer framework's transaction lifecycle is **bypassed
entirely**: `StartReadingMessages` drains without producing messages, so
`BeginTxn`/`ConsumeMessage`/`FinalizeTxn`/`StartCommit` are never invoked.
The framework still manages assignment, Etcd state, and recovery log setup;
all document processing and commit sequencing happen here, via the Shuffle
Leader protocol (derive/materialize) or per-shard transaction loop (capture).

For derive/materialize, only **shard zero** hosts a recovery log. Non-zero
shards have `ShardSpec.recovery_log_prefix = ""`, spin up instantly, and
acquire state through the Leader protocol — eliminating per-shard recovery
logs and simplifying crash recovery. The sidecar and per-shard TaskServices
communicate solely by gRPC; no shared memory.

## Layout

```
src/
├── lib.rs             # crate root and Accumulator
├── task_service.rs    # CGO entry point: binds UDS, serves the Shard and
│                       #   Connector services; installs a ServiceRouter
├── publish.rs         # Publisher / PublisherFactory traits + JournalPublisher
│                       #   (journal-IO) impls; leader & shard are monomorphized over
│                       #   the factory (preview installs its own from flowctl, so this
│                       #   crate is preview-agnostic); RecordingPublisher for tests.
├── logger.rs          # Logger / LoggerFactory traits: the task's log + event stream
│                       #   (connector log sink + structured Events — persist / applied /
│                       #   spec-update / inferred-schema — which flatten to logs via
│                       #   LogEvent::to_log). Production shards install FnLoggerFactory
│                       #   (→ task-log file); leaders & tests install TracingLogger
├── patches.rs         # wire format for connector-state patch streams
│
├── leader/            # sidecar Leader service
│   ├── service.rs       # gRPC entry, per-task Join rendezvous
│   ├── join.rs          # protocol primitives for joining shards into a session
│   ├── close_policy.rs  # when a transaction closes (min / max txn duration)
│   ├── frontier_mapping.rs  # consumer.Checkpoint <-> shuffle::Frontier
│   ├── shuffle.rs       # ShuffleSession / ShuffleSessionFactory traits + ShuffleServiceFactory
│   │                     #   (journal-reading Session) impl; leader is monomorphized over the
│   │                     #   factory (preview installs its own fixture replay from flowctl)
│   ├── capture/
│   │   ├── fsm.rs           # head/tail state machines for capture transactions
│   │   └── task.rs          # Task + Binding + Target: the leader's data model
│   ├── derive/
│   │   ├── handler.rs       # gRPC stream handler, dispatches to startup/actor
│   │   ├── startup.rs       # Recover / Open / Apply / Recovered phase
│   │   ├── fsm.rs           # pipelined HeadFSM / TailFSM state machines
│   │   ├── actor.rs         # event loop driving open / commit / acknowledge
│   │   └── task.rs          # Task: the leader's data model
│   └── materialize/
│       ├── handler.rs       # gRPC stream handler, dispatches to startup/actor
│       ├── startup.rs       # Recover / Open / Apply / Recovered phase
│       ├── fsm.rs           # pipelined HeadFSM / TailFSM state machines
│       ├── actor.rs         # event loop driving open / commit / acknowledge / trigger
│       ├── triggers.rs      # webhook trigger delivery
│       ├── sync_schedule.rs # compiled sync-schedule evaluator (commit pacing)
│       └── task.rs          # Task: the leader's data model
│
└── shard/             # per-shard controller-facing service
    ├── service.rs       # gRPC entry, dispatches by task type
    ├── connector.rs     # client of the `connector` crate: route, start, and read
    │                     #   its stream, sinking interleaved connector logs
    ├── recovery.rs      # Persist <-> RocksDB WriteBatch encode/decode + scan-time FC: pruning
    ├── rocksdb.rs       # single Persist application path
    ├── split_policy.rs  # append-rate throttling which drives partition splits
    ├── capture/
    │   ├── handler.rs       # startup, apply/open, recovery scan, publisher setup;
    │   │                     #   stows inferred shapes by collection across sessions
    │   ├── actor.rs         # independent per-shard capture transaction loop
    │   └── drain.rs         # combiner drain: publish documents, widen inference
    ├── derive/
    │   ├── handler.rs       # gRPC stream handler
    │   ├── startup.rs       # join leader, scan RocksDB, open connector
    │   ├── scan.rs          # frontier scan: source documents out as C:Read
    │   ├── actor.rs         # per-shard transaction loop
    │   ├── drain.rs         # output combiner drain: publish derived documents
    │   └── task.rs          # Task + Transform + Source: the shard's data model
    └── materialize/
        ├── handler.rs       # gRPC stream handler
        ├── startup.rs       # join leader, scan RocksDB, open connector
        ├── scan.rs          # frontier scan: source documents into the combiner,
        │                     #   unseen keys out as C:Load
        ├── actor.rs         # per-shard transaction loop
        ├── drain.rs         # combiner drain: C:Store to the connector
        ├── boundaries.rs    # per-binding backfill-truncation boundaries
        └── task.rs          # Task + Binding + Source: the shard's data model
```

Within a task type, `mod.rs` declares submodules and the session's `Metrics`,
and `task.rs` owns the task's data model: a `Task`, its per-binding struct
(`Transform` for a derivation, `Binding` otherwise), and the per-collection
`Source` / `Target` those bindings group onto. Derive and materialize are
deliberately parallel at every one of these paths, so a difference between the
two files at the same path is meant to be a real difference between the task
types.

## Key entry points

- **`TaskService::new`** (`task_service.rs`) — CGO constructor invoked by Go
  on shard assignment. Wires the data-plane environment (FQDN, control API,
  signing and verification keys), constructs a `connector::Service` and the
  `shard::Service` which routes to it, and serves both over a per-shard Unix
  domain socket.
- **`leader::Service::new`** (`leader/service.rs`) — sidecar process builds
  one of these and registers it on the sidecar port alongside `shuffle::Service`.
- **`shard::Service`** (`shard/service.rs`) — implements the controller-facing
  `Shard` trait. Each bidi stream terminates *both* the controller-bound
  protocol and the leader-bound protocol, translating between them and the
  connector RPC.

The only messages that flow controller → runtime-next → leader unmodified are
`Stop` and `CloseNow`, and the only one flowing leader → runtime-next →
controller unmodified is `Synced`.

## Protocol

`go/protocols/runtime/runtime.proto` defines `Leader` and `Shard` RPCs. Both
carry the same `Derive` / `Materialize` message types; field semantics are
documented inline in the proto.

## Invariants

- **No dependency on `runtime`.** The legacy `runtime` crate may depend on
  `runtime-next`, never the reverse. Files shared between the two live
  physically in `runtime/` and are pulled in via `#[path]`. See the comment
  at the top of `lib.rs`.
- **Shard-local processing is identical for all shards.** Shard zero is
  special only at session startup (forwards `Task` to the leader; receives
  Apply/Persist). The per-transaction loop has no `if shard_zero` branches —
  the leader decides what each shard does and shards follow uniformly.
- **All shards participate in every transaction**, even idle ones — they
  send empty deltas and respond immediately. Shard topology is fail-stop:
  any shard drop aborts the session and tears down all surviving shards.
  The Gazette allocator reassigns; the next session re-joins from PRIMARY.
- **Migration guards on non-zero shards.** Non-zero shards still open a
  (typically empty, tempdir-backed) RocksDB and run the same `scan` path on
  session start. Recovery is expected to error if a non-zero shard observes
  unexpected committed state, or if its connector reports a non-empty
  runtime checkpoint at `Opened` — both indicate stale per-shard state from
  before consolidation.
- **`shard/rocksdb.rs` is the single Persist application path.** Capture
  reuses it by synthesizing `Persist` messages locally rather than receiving
  them from a leader.
- **A shard owns the storage its `SessionLoop` names.** A
  `RocksDBDescriptor.rocksdb_path` transfers the directory to the serve loop,
  which removes it on every exit path — clean stop, session error, and failed
  open alike — always after the RocksDB is torn down
  (`shard::rocksdb::OwnedDir`). An absent descriptor gets a tempdir the shard
  makes and owns identically.
- **Only the `spawn_*` adapters send `Err` on a response stream, and only after
  `serve` returned.** Errors are ALWAYS passed up the stack rather than sent
  into channels. So a response stream's `Err` is the whole loop's outcome, and a
  controller which drops its request stream and reads through to `Err` or EOF
  sees the shard's last word. See `shard/service.rs`.
- **A `Stop` may outrun the session it addresses.** The controller sends
  `Stop` when it observes its task term cancelled, and it selects over that
  cancellation and our `Stopped` concurrently — so the two cross on the wire
  whenever a session ends at the same moment the term does. A publish does
  exactly that to every shard at once, and an idempotent-replay session that
  stops itself can coincide with one. Each session loop therefore absorbs a
  `Stop` received while awaiting a `Join`: the session it addressed is over,
  and its `Stopped` is already on its way to the controller.
  Failing the stream instead strands the shard, since the stream is created
  once per replica (`NewStore`) and reused by every later session — each of
  which would then fail its `Join`, recoverable only by unassigning.

## Coexistence with `runtime`

This crate ships **deployed inert** alongside the existing `runtime` crate;
both coexist on the same reactor. Per-task feature flags on shard labels
select which runtime serves a given task — all shards of a task use the
same runtime. The runtime sidecar runs uniformly on every reactor machine
regardless of which tasks are assigned; old-runtime tasks simply don't talk
to it. Rollback for any task is a feature-flag flip.

Once a task has stably cut over, the per-task `drop-runtime-v1-rollback`
shard-label flag tells the leader to stop maintaining the legacy V1
`consumer.Checkpoint`; the leader deletes the persisted one during startup
(see below), forfeiting rollback in exchange for shedding compatibility state.

## Startup checkpoint reconciliation

The legacy V1 `consumer.Checkpoint` holds a *complete* committed frontier,
whereas the V2 `FC:` keys are written per-transaction as *deltas*. So at a
V1→V2 cutover the recovered `FC:` keys are not yet a sound recovery baseline.
`leader::materialize::startup` reconciles this synchronously: after the
connector `Open`/`Opened` exchange, when the final status of the recovered V1
checkpoint and any remote-authoritative connector checkpoint is known, it
issues a single cleanup `Persist` to shard zero. If a checkpoint was
*authoritative* (its mapped frontier replaced the recovered one), the cleanup
clears all `FC:` keys and rewrites the complete baseline; if
`drop-runtime-v1-rollback` is set, it also deletes the legacy `checkpoint`
key. An authoritative (unmarked) checkpoint implies no V2 transaction has
committed, so clearing `FC:` loses no V2 state. The transaction loop then
only ever writes `FC:` deltas.

## Committed-frontier pruning

The startup scan also drops ancient, closed, far-behind `FC:` producers
(`shard::recovery::prune_committed_frontier`), bounding the frontier a shard
carries forever. This deliberately relaxes the Frontier invariant, and both
horizons it uses are load-bearing: `FRONTIER_PRUNE_BYTE_HORIZON` is local, but
the *clock* horizon is `shuffle::PRODUCER_STALENESS_HORIZON`, shared with the
session's `shuffle::Completed`. Pruning forgets a producer, so only the
session's matching horizon rule can still discharge a later backfill's causal
hints naming it. Read both doc comments before touching either.

## Idempotent recovery (materialize)

A leader whose startup scan finds a hinted-but-uncommitted transaction opens
its session in *idempotent replay*: it must re-establish exactly that
transaction, because its effects may have been partially released to the
connector. Such a session does the replay and nothing else, driving a replay
to transaction commit and then stopping. A next session is an ordinary one,
completing the recovery's post-commit work and resuming regular processing.

Exiting is what makes the replay's disk usage bounded in the face of bindings
with differing priorities. The shuffle Session mirrors this split, stopping
journal reads once they read through hinted spans. This prevents over-read
from consuming disk quota, and prevents starving lower-priority bindings.
See `crates/shuffle/README.md` and issue #3246.

## Targets and Sources

Bindings don't carry collection-derived state; they *reference* a per-collection
struct which does. A capture binding references a `Target` it writes, and a
materialize binding or derive transform references a `Source` it reads. Schema
validators, publisher targets, and inferred write-shapes are built once per
Target / Source, so a task fanning many bindings onto few collections pays
per collection.

The two key differently:

- **Capture `Target`s key on `partition_template_name`** — journal identity — in
  both spec forms. `Task::new` requires that bindings sharing a name also carry
  equal `CollectionSpec` values.
- **Materialize / derive / shuffle `Source`s key on the declared
  `collection_index`** — value identity — never on collection name, because a
  materialization's `group_by` can give two bindings of one named collection
  differing read schemas. An inline-form binding carries no index and is its
  own Source.

## Schema inference (capture)

Inference is keyed by *collection*, not binding: every binding of a Target
widens that Target's one shape, capped and logged once. Any binding's
`SourcedSchema` therefore ratchets its whole collection to
`SOURCED_SCHEMA_COMPLEXITY_LIMIT`.

Shapes live only in memory but accumulate across the many connector sessions of
a shard, so between sessions they're stowed under the Target's
`partition_template_name` — stable where Target *indices* are not — and restored
into the next session's layout. A collection's generation is folded into its
template name, so a collection reset also restarts inference.

## Backfill truncation (capture)

A capture `BackfillBegin` publishes an isolated, document-free marker
transaction and stamps `estuary.dev/truncated-at` on every partition of the
target collection — the boundary materializations classify against (below).

When other active bindings of the task also write those journals
(`Target::fan_in`), the head FSM suppresses the Begin: the message still
isolates its transaction, but no boundary clock, marker intent, `truncated-at`
label, or `ActiveBackfillChange::Begin` is built. The backfill itself proceeds
regardless — the connector re-captures, and documents merge on key.

## Backfill truncation (materialize)

When a source collection is backfill-truncated, documents a materialization
sourced before the truncation boundary are superseded and must not combine
with — or reduce forward into — documents at or above it. The shard actor
(`shard/materialize/boundaries.rs`) tracks per-binding the latest observed
truncation boundary (`Begin`) clock; boundaries classify ingress rather than
tagging combiner entries.

The combiner (`doc::combine`) marks superseded entries with a one-bit **STALE**
flag. A stale entry is never validated or reduced; on drain it is discarded,
transferring only its `front()` existence onto the first fresh entry of a
shared `(binding, key)` — so a truncated row's destination presence is
preserved while its value is not. Staleness reaches an entry three ways, all
collapsing to the same flag:

- **`truncate(binding)`** at the moment a boundary is learned reclassifies
  everything the accumulator already holds: the live MemTable drops the
  binding's pre-boundary source documents outright (they carry no existence)
  and flags its Loaded fronts stale in place, while every spill segment already
  written is fenced by a per-binding **ordinal cutoff** (`cutoffs[binding]` =
  segment count). At drain, an entry in a segment below its binding's cutoff is
  stamped stale.
- **`add_stale_front`** flags a Loaded row classified stale on arrival.
- The **persisted flag** rides the spill entry header, so an entry flagged in
  memory and then spilled (under memory pressure, into a segment at or above the
  cutoff) is still stale at drain — staleness is `persisted-flag OR ordinal
  fence`.

The split is **exhaustive** because `observe_begin` has a single call site — at
L:Load receipt, before the scan — so every combiner add is unambiguously either
*before* it (reclassified by `truncate`) or *after* it (self-classifying at
ingress): the scan drops pre-boundary source documents by their shuffle clock,
and Loaded rows split into fresh fronts vs. stale fronts by their embedded
document-UUID clock. Loaded rows classify by the document UUID (not message
timing) because staleness is a property of when the row was last *stored*: a row
can load stale many transactions after the one that truncated its binding.

Cutoffs are **accumulator-local** and die with the accumulator — a recycled
(drained) accumulator starts with zeroed cutoffs at the same moment its spill
file is truncated to length 0, so segment ordinals and their fences restart
together. The per-binding boundary **clocks**, by contrast, are the persistent
runtime state that outlives any single accumulator.

Consequences and requirements:

- **Once a binding has observed a `Begin`, its Loaded rows must expose a
  parseable document UUID** (at the binding's configured pointer, typically
  `/_meta/uuid`) so each row can be classified against the boundary; a missing
  or malformed UUID then fails the transaction. A binding that has never
  truncated has no boundary, so its Loaded rows are fresh regardless of clock
  and need no UUID — this spares the many pre-existing materializations,
  unrelated to truncation, whose rows carry none. Delta-update bindings never
  load and are unaffected.
- **Boundaries must be visible before the documents they fence.** A `Begin`
  rides eagerly on unresolved shuffle peeks (see `crates/shuffle`), so the shard
  applies it before scanning any document at or above its clock, and each
  document then classifies against the current boundary. This assumes a single
  writer per truncating collection; concurrent writers are undefined.
- **Markers are latest-state, not an event log.** The leader keeps
  session-cumulative per-binding `Begin`/`Complete` maps; each connector
  `Flush` projects the transaction's latest observed clocks. An eager
  (unresolved-peek) `Begin` is used only to stamp outgoing `Load` frontiers
  for shard classification — it never enters transaction extents, the
  connector `Flush`, or durable `Persist` state until its causal hints
  resolve and it rides a fully-resolved frontier.
- **No persisted combiner state.** Neither the STALE flags nor the segment
  cutoffs survive a session: cutoffs are accumulator-local and reset when the
  accumulator is recycled. The per-binding boundary clocks live in the shard
  session, and on recovery the shard reconstructs them from the leader's
  cumulative `Begin` (committed ∪ hinted) delivered on the first `L:Load`.

## Sync schedules (materialize)

A materialization model may carry a `syncSchedule` (type and validation in
`models::sync_schedule`) pacing how often transactions commit: a required
`baseInterval`, plus any number of non-overlapping local-time `windows`, each
with its own interval. The leader compiles the schedule once at task startup
(`leader/materialize/sync_schedule.rs`) — compilation re-runs validation, so
evaluation cannot fail — and computes the next permitted commit instant on an
epoch-relative grid per regime, with deterministic jitter seeded from the
task's tenant: a tenant's materializations — which typically share destination
resources like a warehouse — commit at coinciding instants, waking the
destination once, while unrelated tenants spread apart. A fire crossing regime
transitions clamps to the first one where a faster regime takes over.

Enforcement rides the close policy's min/max transaction durations:
`fsm::compute_open_duration` modulates the open-duration band per evaluation
(the band collapses onto the commit instant while holding), so
`close_policy::evaluate` has no schedule awareness. The combiner usage
ceilings still force early commits — a backfill drains under memory pressure
with no caught-up detection — and `CloseNow` bypasses a hold, so spec updates
restart promptly. The first transaction of a leader session is never held.

## Sync-now (materialize)

Sync-now forces an immediate commit of a materialization's open transaction —
collapsing any sync-schedule hold — and resolves once that transaction is
fully acknowledged (committed and queryable in the destination). This crate
contributes only the *barrier*, not the RPC: `TaskControl.SyncNow` is served
by the reactor front door (`go/runtime/task_control.go`), and the waiting is
done by the Go controller (`materializeAppV2.syncNow`).

The barrier is `CloseNow` and `Synced`, documented in `runtime.proto`. The
Actor counts `Tail::Done` transitions of its session (`acknowledged_count`),
the transactions still ahead of that count (`pending_count` —
`fsm::pending_transactions`, an open Head transaction plus a Tail which
hasn't reached Done), and the highest `CloseNow.seq` it has received
(`close_request_seq`). It broadcasts `Synced` to its shards whenever any of
the three changes after the session's first `CloseNow`, and each shard
relays it to its controller unmodified.

A controller sends `CloseNow` with sequence S, waits for a `Synced` reporting
`close_request_seq >= S`, and then awaits `acknowledged_count` reaching that
same message's `acknowledged + pending`. Waiting for its own echo is what
makes the barrier exact: counts take a moment to travel out, so the ones a
controller already holds can predate a transaction which has since opened.

So the Actor holds no waiters, no timers, and no notion of a caller: repeated
`CloseNow` is idempotent (the FSM's existing `close_requested` input, cleared
whenever no transaction is open), and a session which exits simply stops
reporting — its counts restart from zero in the next one, which is why a
controller must discard a target recorded under a session that ended.

## Status

- `leader::materialize` / `shard::materialize` and `leader::derive` /
  `shard::derive` are implemented.
- `shard::capture` is implemented as an independent per-shard transaction
  loop with local RocksDB persistence.
- All three task types are wired into the Go runtime behind the
  `estuary.dev/flag/enable-runtime-v2` shard label (`go/runtime/{capture,
  materialize,derive}_v2.go`); without the flag the legacy runtime is used.
  derive-sqlite threads its recorded SQLite VFS to the connector and runs on an
  ephemeral shard-zero RocksDB (SQLite is authoritative).
