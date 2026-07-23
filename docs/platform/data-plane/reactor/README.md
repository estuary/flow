# Reactor

The task-executing side of a [data plane](../). Where brokers serve journals,
reactors run [tasks](../../tasks/) — driving their connectors, shuffling their
inputs, and committing their progress. This node covers how a reactor runs a
shard: the runtime it embeds, the shuffle subsystem that feeds it, how it
commits checkpoints, and how it exposes a connector to the network.

## Glossary

**Runtime**:
The task-execution logic a reactor embeds — the capture, derive, and
materialize transaction loops. Two generations coexist (**V1** and **V2**),
selected per task.
_Avoid_: engine, worker

**Consumer framework**:
Gazette's machinery that assigns shards to reactor members and manages their
Etcd state and recovery logs. The runtime drives transactions itself rather
than through the framework's transaction lifecycle.

**Runtime sidecar**:
A per-machine process, one per reactor host, that serves the shuffle subsystem
(the Shuffle Leader and Shuffle services) for the tasks assigned to reactors on
that machine.
_Avoid_: agent, daemon

**Shuffle Leader**:
The per-task coordinator (V2) that sequences a single distributed transaction
across all of a task's shards.
_Avoid_: master, primary

**Shard zero**:
The distinguished first shard of a task. For derivations and materializations
it hosts the task's sole recovery log and anchors the Shuffle Leader rendezvous;
every other shard spins up statelessly.

**Checkpoint**:
A task's durable record of committed progress — how far it has consumed its
inputs and the connector state that goes with it. What a reassigned shard
resumes from.
_Avoid_: offset, watermark

**Connector networking**:
Exposing a running connector's port at a hostname, so external systems reach it
through the data plane's TLS frontend.

## What a reactor runs

A running task is partitioned into [shards](../#glossary), and Gazette's
**consumer framework** assigns each shard to a reactor member. The framework
handles assignment, Etcd state, and recovery-log setup — but the reactor does
_not_ use its transaction lifecycle. Instead each shard embeds a **runtime**
that drives the capture, derive, or materialize loop directly: pulling or
pushing documents through the task's [connector](../../connectors/), combining
them, publishing to journals, and committing checkpoints.

## Two runtime generations

Two runtimes coexist on every reactor, and a per-task shard-label flag
(`estuary.dev/flag/enable-runtime-v2`) selects which one serves a given task —
all shards of a task use the same generation, and switching is a flag flip.

- **V1** runs each shard as an independent transaction loop with its own
  recovery log, coordinated through the Go layer.
- **V2** makes a task's shards transact _together_. A per-task **Shuffle
  Leader** sequences one distributed transaction spanning every shard, and only
  **shard zero** carries a recovery log; non-zero shards hold no persistent
  state of their own, spin up instantly, and acquire what they need from the
  leader. All shards — even idle ones — participate in every transaction, and
  the topology is fail-stop: if any shard drops, the session aborts and re-forms
  from a fresh assignment. Captures are simpler even under V2, keeping
  independent per-shard loops rather than a shared transaction.

## Shuffle

Every shard reads its inputs through **shuffle**: a read of the source journals
that routes each document to the shard responsible for its key, so a given key
is always processed by the same shard even as shards split or move. Shuffle
serves derivation transforms, materialization bindings, and ad-hoc collection
reads.

The subsystem runs in the **runtime sidecar** — one process per reactor machine,
shared by all its reactors — and is built from three layered gRPC services:

- **Session** — one per task, opened by shard zero; discovers the source
  journals and aggregates progress into checkpoints.
- **Slice** — one per shard; reads its share of journals, sequences and
  validates documents, and routes each to the shard that owns its key.
- **Log** — one per shard; merges the documents routed to it, in processing
  order, into an on-disk log the shard then consumes.

Documents are staged to disk rather than held in memory, so reads run far ahead
of processing under back-pressure, and steady-state reading never replays —
replay is confined to restart recovery, where a gapped producer's skipped span
is recovered by a single bounded historical read.

## Checkpoint mechanics

A **checkpoint** is what makes a task's progress durable and its processing
effectively exactly-once: it records how far each input has been consumed,
together with the connector state at that point, so a reassigned shard resumes
precisely where its predecessor stopped.

The two runtimes record checkpoints differently. V1 persists a single
_complete_ committed frontier per shard. V2 instead writes per-transaction
_deltas_ to shard zero's recovery log, coordinated across all shards so recovery
is idempotent. Because a V1 checkpoint is complete and V2's are deltas, a task
switching from V1 to V2 reconciles once at startup — rewriting a complete
baseline from the recovered V1 checkpoint before the delta stream takes over.

## Connector networking

Some connectors need to be reachable from outside — to receive webhooks, or to
serve a port. The reactor machine runs a TLS **frontend** that makes that
possible without exposing the reactor itself. It matches each incoming
connection on its TLS server name (SNI), resolves that name to a specific task
shard and port through the shard's `hostname` and `expose-port` labels, and then
either TCP-proxies straight through to a public port on the connector or serves
the connection as an authorizing HTTP/2 reverse proxy for a private one.
Unmatched connections pass through untouched.

## Where this lives

- `go/runtime` — the reactor (Flow consumer): shard lifecycle, Etcd watch, ops,
  and the V1/V2 selection (`{capture,derive,materialize}_v2.go`)
- `crates/runtime` — the V1 task runtime, plus connector container and sidecar
  management (`container.rs`, `image_connector.rs`)
- `crates/runtime-next` — the V2 runtime: the per-shard `Shard` and per-machine
  `Leader` services, and the V1→V2 checkpoint reconciliation
- `crates/runtime-sidecar` — the per-machine sidecar process and its listeners
- `crates/shuffle` — the V2 shuffle subsystem (Session/Slice/Log); `go/shuffle` —
  the legacy shuffle
- `go/network` — the connector-networking frontend (SNI matching and proxying)
- `go/protocols/runtime/runtime.proto` — the `Leader` and `Shard` RPCs
