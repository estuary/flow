# Runtime V2

## Objectives

Replace the Go-driven transaction loop with a Rust-driven architecture.
For derivations and materializations, a **Shuffle Leader** coordinates
multi-shard transactions, the **shuffle** crate replaces the Go shuffle,
and **journal publishing** moves from Go to Rust. Captures follow a
simpler model with independent per-shard transaction loops. The Go
runtime becomes a thin shim for shard lifecycle and ops logs.

Why:
- **Multi-shard coordination** (derive/materialize): The Go shuffle has
  known scaling limitations (per-shard×journal RPCs, in-memory staging,
  no coordinated checkpoints). The Shuffle Leader + Rust shuffle solve
  these architecturally.
- **Consolidated state** (derive/materialize): Scale-out shards persist
  through shard zero's recovery log, eliminating per-shard recovery logs
  and simplifying crash recovery.
- **Fewer moving parts**: Transaction lifecycle, publishing, stats, and
  shuffle all move to Rust. Go retains only Etcd lifecycle and ops logs.

There are no multi-shard tasks in production today. Multi-shard
coordination (Shuffle Leader, shard-zero consolidation) will be
validated first with test workloads and introduced gradually
alongside production single-shard migration.

## Architecture

```
Reactor machine
  ├─ reactor process(es) (Go + Rust via CGO)
  │    ├─ Go: Etcd watch, shard lifecycle, ops logs (OUTSIDE_TXN)
  │    └─ Per-shard TaskService (Rust, via CGO):
  │         ├─ Connector driving, combining, publishing CONTINUE_TXN docs
  │         ├─ In-memory state (connector state, checkpoints, max-keys)
  │         ├─ Derive/materialize: state persisted via Leader's Persist/Persisted
  │         │    (RocksDB + Go Recorder on the shard hosting the recovery log)
  │         └─ Capture: per-shard RocksDB with Go Recorder
  │
  └─ shuffle sidecar process (Rust, one per machine, systemd-supervised)
       ├─ Shuffle Leader service (per-task, via join pattern)
       ├─ Shuffle service (Session/Slice/Log RPCs)
       └─ Listens on the fixed shuffle port (same across the fleet)
```

Three layers interact:

1. **Go runtime** (`go/runtime/`) observes Etcd for shard lifecycle.
   On assignment, it spawns a CGO TaskService for runtime-next and
   sends a `RocksDBDescriptor`. On term changes (spec updates in
   Etcd), it cancels the term context for graceful restart. It writes
   ops logs as `OUTSIDE_TXN` documents. Feature flags on shard labels
   determine whether to call runtime-next or the existing runtime
   crate.

   The Gazette consumer framework's transaction lifecycle is
   **bypassed entirely**: `StartReadingMessages` drains without
   producing messages, so `BeginTxn`/`ConsumeMessage`/`FinalizeTxn`/
   `StartCommit` are never invoked. The framework still manages
   assignment, Etcd state, and recovery log setup; all document
   processing and commit sequencing happen in Rust via the Shuffle
   Leader protocol (or, for captures, a per-shard transaction loop).

2. **Per-shard `TaskService`** (`crates/runtime-next/`) runs in the
   reactor process via CGO — one per shard, created and torn down by
   `taskBase` in response to shard assignments. Hosts the `Shard`
   gRPC service over a per-shard UDS. Task-scoped logs cross back to
   Go via the existing pipe-FD mechanism.

   - For derive/materialize: all mutable state (connector state,
     checkpoints, max-keys) is held in memory; state changes flow
     through the Shuffle Leader protocol and land in shard zero's
     RocksDB.
   - For captures: each shard manages its own state independently,
     persisting to its own RocksDB via its own recovery log.

3. **Shuffle sidecar process** (pure Rust), one per reactor machine,
   supervised by systemd with the same lifetime as the reactor
   process(es). Hosts two gRPC services for *all* tasks on the machine:

   - **Shuffle Leader** (`crates/runtime-next/src/leader/`): accepts
     shard streams for tasks whose shard zero is assigned to a reactor
     on this machine.
   - **Shuffle** (`crates/shuffle/`, Session/Slice/Log RPCs): accepts
     shuffle RPCs from any reactor participating in a task.

   Both listen on the **shuffle port**: a fixed CLI argument, shared
   fleet-wide. A reactor reaches any peer sidecar by replacing the
   port of that peer's `ProcessSpec.endpoint`. One sidecar serves
   multiple co-located reactor processes on the same machine (used
   by local stacks).

   Sidecar tracing goes to the sidecar's own stderr as application
   logs, not task logs. Captures don't use the Shuffle Leader or
   shuffle services, but the sidecar runs uniformly regardless.

   *Why a sidecar rather than in-process via CGO.* The shuffle stack
   and per-shard TaskService already communicate solely by gRPC with
   no shared memory, so the process boundary aligns with an existing
   logical one. Separation buys an independent monitoring domain and
   a smaller reactor Rust + CGO surface. A sidecar crash fail-stops
   every joined session on the machine.

The sidecar and per-shard TaskServices communicate only via the
Shuffle Leader protocol and the shuffle Session/Slice/Log RPCs over
gRPC — never via shared memory.

## Where to start reading

Leader and shard implementations both live in `crates/runtime-next/`.

- **Runtime protocol** — `go/protocols/runtime/runtime.proto` defines
  the bidirectional `Leader` (sidecar) and `Shard` (per-shard,
  controller-facing) RPCs. Both carry the same message types (`Derive`,
  `Materialize`); messages and their fields are documented inline.
- **Sidecar Shuffle Leader** — `crates/runtime-next/src/leader/`.
  - `leader/service.rs`, `leader/materialize/handler.rs`: gRPC entry
    points and per-task Join rendezvous.
  - `leader/materialize/startup.rs`: Recover/Open/Apply/Recovered phase.
  - `leader/materialize/fsm.rs`, `leader/materialize/actor.rs`:
    pipelined HeadFSM/TailFSM driving open / commit / acknowledge /
    trigger.
  - `shard/recovery.rs`: encode/decode of `Persist` payloads to RocksDB
    `WriteBatch` operations and recovery iteration to in-memory state.
    Single source of truth for the on-disk key layout (`FH:`, `FC:`,
    `AI:`, `MK-v2:`, `connector-state`, `trigger-params`, `last-applied`).
  - `patches.rs`: wire format for connector state patch streams shared
    by leader-side state reduction and shard-side persistence.
  - `leader/materialize/frontier_mapping.rs`: bi-directional mapping between
    `consumer.Checkpoint` and `shuffle::Frontier`.
  - `triggers.rs`, `publish.rs`: webhook trigger delivery and
    leader-side journal publishing of stats / ACK intents.
- **Per-shard TaskService** — `crates/runtime-next/src/`.
  - `task_service.rs`, `handler.rs`: CGO entry point and `Shard` gRPC
    handler.
  - `shard/materialize/`: per-shard transaction loop, connector RPC
    bridging, and (on shard zero) RocksDB persistence.
  - `shard/rocksdb.rs`: the single Persist application code path; capture
    will reuse it by synthesizing Persist messages locally.

## Key invariants

These are load-bearing rules the implementation enforces and that any
new code must continue to honor:

- **Crate dependency direction.** Leader and shard implementations both
  live in `runtime-next`. The legacy `runtime` crate does not depend on
  `runtime-next`, and `runtime-next` MUST NOT depend on `runtime`.
  `runtime` is being minimally changed to trivially ensure we don't break it.
  Prefer a little copying to a little dependency.

- **Shard-local processing is identical for all shards.** Shard zero
  is special only in that it hosts the recovery log and receives
  leader-directed Apply/Persist operations. Shard-zero conditional
  branches are confined to startup (forwarding `Task` to the leader,
  which only one shard may do) and to receiving Apply/Persist; the
  per-transaction loop has no "am I shard zero?" tests — the leader
  decides what each shard does and shards follow uniformly.

- **All shards participate in every transaction**, even idle ones —
  they send empty deltas and respond immediately. Shard topology is
  fail-stop: any shard drop aborts the session and tears down all
  surviving shards. The Gazette allocator reassigns; the next session
  re-joins from PRIMARY.

- **Non-zero derive/materialize shards have no recovery log.** The
  consumer framework supports this via `ShardSpec.recovery_log_prefix
  = ""`; non-zero shards spin up instantly and acquire state via the
  leader protocol. Shard zero's recovery dominates session startup
  latency.

  Non-zero shards still open a (typically empty, tempdir-backed)
  RocksDB and run the same `scan` path on session start. This is
  intentional: during migration a non-zero shard may inherit
  pre-existing on-disk state, and the recovery scan must surface that
  rather than silently ignore it. Recovery is expected to error if a
  non-zero shard observes unexpected committed state.

  The same migration guard applies to connector-reported runtime
  checkpoints at `Opened`: non-zero shards are expected to report no
  checkpoint state. A non-zero checkpoint is treated as evidence that
  the task still has per-shard committed state, and startup fails
  explicitly instead of proceeding with an unsafe consolidation.

## Capture architecture

Capture shards operate **independently** — each shard has its own
recovery log, RocksDB, connector instance, and publisher. Capture
connector state represents per-shard cursors (e.g., CDC LSNs) with
no cross-shard coordination, so shards must make independent progress.
A capture shard failure affects only that shard.

Capture is unimplemented in `runtime-next` at the time of writing.
The intent is to reuse `runtime-next`'s single Persist application
path by synthesizing `Persist` messages locally inside the capture
shard, rather than receiving them from a leader.

## Migration strategy

The complete runtime-next — Shuffle Leader, Rust shuffle, Rust
publishing, and per-task-type transaction loops — is built and
**deployed inert**. The new code ships alongside the existing runtime
with no tasks using it. Task migration is then controlled via
per-task feature flags on shard labels, with per-task rollback if
issues arise.

This avoids partial implementations that would require migrating
tasks twice (to a partial runtime, then again to the final one).
Risk is managed through rollout pacing: tasks activate into the
complete runtime in stages of increasing blast radius.

### Rollout sequence

1. **Low-value single-shard captures**: exercises the Rust transaction
   loop and publishing with the simplest task type.
2. **Single-shard derive/materialize**: adds the Rust shuffle and
   leader protocol at N=1: one shard joins with itself, all RPCs local.
   All production derive/materialize tasks are single-shard today.
3. **Multi-shard derive/materialize**: full leader coordination at
   N>1. No multi-shard tasks exist in production today; they will
   be introduced with test workloads first.
4. **Higher-value tasks and full migration**: as confidence grows,
   progressively migrate remaining tasks. Go shuffle deleted after
   full migration.

### Rollout mechanics

- **Per-task**: feature flags on shard labels select old vs new runtime.
  All shards of a task use the same runtime.
- **Coexistence**: old-runtime and new-runtime tasks run on the same
  reactor. The shuffle sidecar runs uniformly on every reactor
  machine regardless of which tasks are assigned; old-runtime tasks
  simply don't talk to it. The only change to the existing `runtime`
  crate is Frontier-aware rollback (the migration swap on startup,
  using `runtime_next::leader::materialize::frontier_mapping`).
- **Rollback**: switching a task's feature flag back to the old runtime
  is a per-task operation. No global rollback needed.
