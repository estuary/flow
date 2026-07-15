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
├── lib.rs             # crate root, shared helpers (Verify, Accumulator)
├── task_service.rs    # CGO entry point: binds UDS, serves Shard service
├── publish.rs         # Publisher / PublisherFactory traits + JournalPublisher
│                       #   (journal-IO) impls; leader & shard are monomorphized over
│                       #   the factory (preview installs its own from flowctl, so this
│                       #   crate is preview-agnostic)
├── logger.rs          # Logger / LoggerFactory traits: the task's log + event stream
│                       #   (connector log sink + structured Events — persist / applied /
│                       #   inferred-schema / container lifecycle — which flatten to logs
│                       #   via LogEvent::to_log). Production shards install FnLoggerFactory
│                       #   (→ task-log file); leaders & tests install TracingLogger
├── patches.rs         # wire format for connector-state patch streams
│
├── leader/            # sidecar Leader service
│   ├── service.rs       # gRPC entry, per-task Join rendezvous
│   ├── join.rs          # protocol primitives for joining shards into a session
│   ├── shuffle.rs       # ShuffleSession / ShuffleSessionFactory traits + ShuffleServiceFactory
│   │                     #   (journal-reading Session) impl; leader is monomorphized over the
│   │                     #   factory (preview installs its own fixture replay from flowctl)
│   └── materialize/
│       ├── handler.rs       # gRPC stream handler, dispatches to startup/actor
│       ├── startup.rs       # Recover / Open / Apply / Recovered phase
│       ├── fsm.rs           # pipelined HeadFSM / TailFSM state machines
│       ├── actor.rs         # event loop driving open / commit / acknowledge / trigger
│       ├── frontier_mapping.rs  # consumer.Checkpoint <-> shuffle::Frontier
│       ├── triggers.rs      # webhook trigger delivery
│       └── task.rs          # per-task state held by the leader actor
│
└── shard/             # per-shard controller-facing service
    ├── service.rs       # gRPC entry, dispatches by task type
    ├── recovery.rs      # Persist <-> RocksDB WriteBatch encode/decode + scan-time FC: pruning
    ├── rocksdb.rs       # single Persist application path
    ├── capture/
    │   ├── handler.rs       # startup, apply/open, recovery scan, publisher setup
    │   ├── connector.rs     # capture connector RPC bridging
    │   ├── actor.rs         # independent per-shard capture transaction loop
    │   ├── fsm.rs           # head/tail state machines for capture transactions
    │   └── task.rs          # per-capture task and binding shape
    └── materialize/
        ├── handler.rs       # gRPC stream handler
        ├── startup.rs       # join leader, scan RocksDB, open connector
        ├── scan.rs          # in-memory state recovery from RocksDB
        ├── connector.rs     # connector RPC bridging
        ├── actor.rs         # per-shard transaction loop
        └── drain.rs         # graceful drain on Stop / CloseNow
```

## Key entry points

- **`TaskService::new`** (`task_service.rs`) — CGO constructor invoked by Go
  on shard assignment. Wires the data-plane environment (FQDN, control API,
  signing key), constructs a `shard::Service`, and serves it over a per-shard
  Unix domain socket.
- **`leader::Service::new`** (`leader/service.rs`) — sidecar process builds
  one of these and registers it on the sidecar port alongside `shuffle::Service`.
- **`shard::Service`** (`shard/service.rs`) — implements the controller-facing
  `Shard` trait. Each bidi stream terminates *both* the controller-bound
  protocol and the leader-bound protocol, translating between them and the
  connector RPC.

The only messages that flow controller → runtime-next → leader unmodified are
`Stop` and `CloseNow`.

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
