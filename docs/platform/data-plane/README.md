# Data Plane

## Glossary

**Data plane**:
The runtime surface that executes tasks and serves collection data. A deployment
of brokers, reactors, and Etcd, identified by a fully-qualified domain name; the
platform runs one or more.
_Avoid_: cluster, region, deployment

**Gazette**:
The broker-and-consumer system the data plane is built on: brokers serve
journals, and its _consumer framework_ assigns and runs shards over them.

**Broker**:
A Gazette server that serves journals — brokering their appends and reads and
coordinating their fragments.
_Avoid_: server, node

**Journal**:
An append-only stream of bytes, the durable log that backs a collection. The
unit a broker serves. This is the canonical home of the journal concept;
[collections](../collections/) reference it.
_Avoid_: log, stream, topic, partition

**Fragment**:
A contiguous span of a journal persisted as a single object in cloud storage.
Brokers coordinate fragments; clients fetch their bytes directly from storage.
_Avoid_: segment, chunk, object

**Reactor**:
The runtime process that executes tasks on Gazette's consumer framework, running
each task as shards and driving their connectors as sidecars.
_Avoid_: worker, consumer, node

**Shard**:
The unit of a task's partitioned, independently-assigned execution — owning a
range of the shuffle key-space and r-clock-space it processes. This is the
canonical home of the shard concept; [tasks](../tasks/) reference it.
_Avoid_: partition, worker, task instance

**Etcd**:
The coordination store holding a data plane's local state — shard and journal
specs, member assignments, and labels — the source of truth for both brokers and
reactors.

**Recovery log**:
A journal recording a shard's local (RocksDB) state so a reassigned shard
recovers exactly where its predecessor left off.

**Shuffle**:
The mechanism by which a shard reads its input journals, routing each document to
the shard responsible for its key so tasks scale across shards.
_Avoid_: repartition, reshuffle

**Ops journal**:
A journal into which reactors publish a task's operational logs or stats,
readable like any other collection.

**Dekaf**:
A data-plane service that presents collections as Kafka topics, letting Kafka
consumers read collection data.

## What a data plane is

A data plane is where the platform actually runs. Where the
[control plane](../control-plane/) holds the [catalog](../catalog/) and decides
what should run, a data plane executes it: it runs [tasks](../tasks/) and serves
the [collection](../collections/) data they read and write. It is a deployment
of three cooperating parts — **Gazette brokers**, **reactors**, and **Etcd** —
plus the cloud storage that holds collection data durably.

The platform runs **more than one** data plane. Each is identified by a
fully-qualified domain name (its FQDN), and every task and journal is _homed_ in
exactly one of them. The control plane resolves a catalog name to the data plane
that owns it and hands back that plane's broker and reactor addresses. A data
plane's identity also anchors the tokens it trusts (see
[below](#relation-to-the-control-plane)), and a task or collection can be
_migrated_ from one data plane to another.

## Brokers and journals

A **journal** is an append-only stream of bytes — the durable log that backs a
collection. **Brokers** serve journals: they order appends, replicate the live
tail across broker members for durability, and coordinate each journal's
fragments.

A **fragment** is a contiguous span of a journal persisted as one object in
**cloud storage** (the bucket named by the collection's storage mapping). This
split is what lets a journal be effectively unbounded and cheap: brokers hold
only the live tail, while historical content lives as fragments in the bucket.
Reading clients fetch fragment bytes **directly** from storage rather than
through a broker, so replaying history doesn't load the brokers.

## Reactors and shards

A **reactor** executes tasks on Gazette's **consumer framework**. A running task
is partitioned into **shards**, and the framework assigns each shard to a reactor
member. A shard owns a range of the task's key-space and r-clock-space; together
the shards of a task cover the whole space, and a task scales by splitting a
shard's range in two.

Each shard reads its inputs through **shuffle** — a read of the source journals
that routes every document to the shard responsible for its key, so that a given
key is always processed by the same shard even as shards split and move. A
shard's local computation state lives in RocksDB and is mirrored to a **recovery
log** (itself a journal), so when a shard is reassigned to another member it
recovers exactly where its predecessor left off.

A reactor also drives each task's [connectors](../connectors/), running them as
**sidecar** containers alongside the shard and speaking the capture or
materialize protocol to them. A per-machine **runtime sidecar** process hosts the
shared shuffle machinery for the reactors on that machine.

How a reactor actually runs a shard — the two runtime generations, the shuffle
subsystem, checkpoint mechanics, and connector networking — is
[reactor/](./reactor/).

## Etcd and assignment

**Etcd** is the data plane's coordination store and the source of truth for its
local state: the specs of its shards and journals, which member each is currently
assigned to, and the **labels** that carry a shard's coordinates (its key and
r-clock ranges, task name and type, catalog build, and operational flags).
Gazette's allocator watches Etcd and continuously assigns shards and journal
primaries across the available members, re-balancing as members come and go and
as shards split — the mechanism behind a data plane's fault tolerance and
elasticity.

## Reading collection data out

Because collection data is just journals, anything holding a read capability can
consume it directly. **Dekaf** is the data plane's compatibility surface for that:
it presents collections as **Kafka topics**, so existing Kafka consumers read
collection data without speaking Gazette's protocol.

## Operational telemetry

A task's own operations are collection data too. Reactors publish each task's
**logs** and **stats** into dedicated **ops journals**, which are read back
exactly like any other collection — this is how a user observes what their tasks
are doing.

## Relation to the control plane

The control plane never runs tasks; it _activates_ them into a data plane (see
the [catalog handoff](../catalog/#handoff-to-the-data-plane)) and then leaves the
data plane to run them. Access is **token-based**: the control plane mints
short-lived JWTs scoped to a namespace prefix and a capability, signed such that
they verify only against the intended data plane's identity. A caller presents a
capability — to lead a task, to shuffle-read from it, or to read a collection —
and the data plane authorizes the request against its own FQDN. Reactors in turn
call back to the control plane's API for the catalog builds and configuration
they need to run.

## Where this lives

- `crates/gazette` — Rust client for brokers, journals, shards, and fragments
  (the brokers and consumer framework themselves are upstream
  `go.gazette.dev/core`)
- `crates/runtime`, `crates/runtime-next` — the in-reactor task runtime (capture,
  derive, materialize transaction logic)
- `crates/runtime-sidecar` — the per-machine sidecar process and its services
- `crates/shuffle`, `go/shuffle` — shuffled reads between tasks
- `crates/dekaf` — the Kafka-compatible read surface
- `crates/ops` — the logs and stats published into ops journals
- `go/runtime` — the reactor (Flow consumer) built on Gazette's consumer framework
- `go/labels/labels.go` — the `estuary.dev/*` label vocabulary Etcd coordination
  keys on
- `crates/models/src/authorizations.rs` — the control-plane authorization handshake
