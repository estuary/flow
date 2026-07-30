# Consistency testing for materialization connectors

Why the suite in `crates/materialize-consistency` is shaped as it is: the
reasoning, the alternatives that were rejected, and the places where implementing
it changed the plan. The suite's own README is the roadmap; this is the record.

## The problem

A materialization connector is expected to uphold exactly-once delivery, and
before this suite there was no mechanical way to find out whether it does.

- The `Apply`-drains-pending-work contract was tested by hand-calling RPCs
  in-process, with the runtime simulated by the test.
- Fencing was tested by installing fence rows and checking that a stale nonce is
  rejected — the mechanism in isolation, never a real race.
- The integration harness drove materializations through a real runtime, but shard
  count was fixed at process start, and there was no way to crash a connector at a
  chosen point in a transaction or to run two instances concurrently.

So the invariants that matter most were the ones nobody could test. The Snowpipe
Streaming v2 write path (estuary/connectors#4828) is the motivating case: it
resumes from Snowflake's committed offset token, which is correct only if an
interrupted transaction is replayed identically, and that property was verified
manually in production (estuary/connectors#4933).

The problem is sharpened by agents. An agent cannot reason its way to confidence
about a distributed invariant, so the most dangerous class of connector change —
anything touching `Store`, `StartCommit`, `Acknowledge`, checkpointing, or fencing
— was also the class with the least automated protection.

## Decisions

### Verify against a real runtime, never a model

Every scenario runs a real materialization task on a real Flow data plane. A
harness that synthesized protocol messages itself would be a second implementation
of the runtime's semantics, and would certify compliance against that second
implementation. This way the suite stays correct as the runtime changes.

The shim's zombie is the single exception, and a bounded one: the messages the
zombie processes are real runtime messages, and only their *scheduling* is the
shim's. Everything else the shim does is observation and perturbation of a stream
it did not author.

### Target the V2 runtime exclusively

V2 is where connectors are heading, where the coordinator/shard-zero state
scatter-gather exists, and where idempotent transaction replay exists — which the
document-counter class depends on.

Shard splits do apply to V2. The split workflow lives at the shared gazette
consumer layer and runs before the v1/v2 dispatch, so it is available to both, and
`flowctl raw split-shards` already refuses a task that lacks the
`enable-runtime-v2` flag. This was a correction made while designing: the
shard-reconfiguration scenarios are testable now rather than blocked on runtime
work.

### Connectors run as binaries via `local:`, not as images

This is what makes the loop fast enough for an agent to use — a change is `go
build` plus re-run, not an image build and push — and it makes the shim nearly
free, since a `local:` endpoint is an arbitrary argv with its own environment
variables.

### An interposer shim, configured entirely through the environment

The catalog's `local:` command names the shim, with the real connector as its
argument. Two decisions were left open by the spec and are resolved here:

**The control channel is environment variables, not a dial-out server.** Every
specified scenario is expressible as "perturb the Nth occurrence of a protocol
event", which the shim decides on its own. A control server is needed only if a
scenario must be triggered from *outside* the connector's protocol stream, and none
of the current scenarios is.

**Fault state is a marker file in the run directory.** This is load-bearing rather
than incidental. A crash fault kills the connector; the runtime restarts it; the
replacement process reaches the same trigger. Without cross-process state it would
crash again, forever, leaving the shard permanently down — and a destination
nothing wrote to is trivially consistent, so the scenario would *pass*. The marker
is created with `create_new`, so the claim is atomic between a live instance and
its zombie.

### The workload

Two collections over the same document contract:

- `merged`, keyed `[/id]`, with `sum` on the balance delta.
- `log`, keyed `[/id, /seq]`, append-only.

The generalisation that drove this: **an exactly-once violation is only detectable
when destination state depends on how many times a document was applied.** Under
last-write-wins merge semantics, applying a document twice is invisible. So the
workload must combine a summed counter with an append-only binding, or it cannot
see the thing it exists to detect. The delta binding is load-bearing rather than
optional — the Snowpipe Streaming v2 path handles only delta-updates bindings.

**Deviation: two captures, not one capture with two bindings.** The spec assumed
one capture feeding both collections via its `collections` list. But `source-soak`
routes each document to `id % len(bindings)`, so one capture with two bindings
*partitions* accounts between the two collections rather than writing both to each.
Conservation would then hold only over the union of the two destinations, and
standard-versus-delta agreement would be uncheckable, since the two collections
would describe disjoint accounts. Two single-binding captures give each collection
a self-conserving population, so every invariant is checkable per binding and a
failure localizes. The capture connector is still reused unmodified.

**Reproducibility is a property of the journal, not of a seeded generator.** The
capture and the materialization are separate tasks joined only by the collection's
journals. Once a document is written it is durable and immutable, and interrupting
the materialization neither touches the capture nor rewrites the collection. Every
crash-and-replay scenario replays byte-identical input by construction. (This was
the second correction made while designing: a seeded generator was thought
necessary and is not.)

### Transaction boundaries are approximate, by choice

The runtime's close policy has document- and byte-count ranges, but they are not
threaded through from the spec; only minimum and maximum transaction duration are.
So transaction size is shaped by those durations plus a rate-paced capture, giving
roughly `rate × min duration`. The shim cannot help: it can stall a response to
*extend* a transaction, but it has no way to make one close.

Hence the rule that constrains every scenario: **keyed on protocol events the shim
observes, never on document identity.** This costs nothing because verification is
invariant-based rather than snapshot-based.

### Invariants are checked at quiescence

Not a convenience. Some legitimate patterns make rows visible before the Flow
transaction commits — the document-counter class appends during `Store` — so a
mid-flight destination read would report a violation where none exists.

**Deviation: a run quiesces by disabling its own captures.** The spec had the
workload published once per stack, which leaves the capture running forever and
gives a scenario no quiescent moment to read. Publishing the workload per run
instead bounds the data volume, makes quiescence reachable, and is *more* isolated,
not less. It respects the rule that matters — a scenario touches only its own
tasks, never anything stack-wide.

### Recovering a crashed shard is part of the scenario

A crash fault is only half of crash-and-replay. The other half turned out to need
work the spec did not anticipate: **a Gazette shard whose processing loop fails is
marked FAILED and stays that way** — the allocator will not reschedule it. Every
crash scenario would otherwise wait out its deadline against a shard that never
came back, and report a *pass*, because a destination nothing wrote to is trivially
consistent.

In production something eventually re-activates the task, and `activate` unassigns
every shard it upserts, so failures clear as a side effect of a publication. The
suite cannot use that: republishing the materialization bumps its version, drives
an `Apply`, and opens a new session — perturbing the task under test at the moment
the run has stopped perturbing it on purpose.

So `flowctl raw unassign-shards` was added beside its sibling `split-shards`, where
the data-plane auth it needs already lives, and the runner calls it until the task
is committing again. It does only the unassigning, changing no specification.

Two consequences worth knowing:

- **Shard administration needs `estuary_support/`.** Both splitting and unassigning
  authorize at Admin capability, which the control plane grants only to a user
  holding that role — correctly, since these are support-level operations. The
  `ci:consistency` task grants it to the test user and warms the agent's
  authorization snapshot, rather than widening what `local:test-tenant` hands every
  test.
- **The runner requires progress after the fault.** Recovery is not assumed from
  the unassign returning: the scenario waits for further committed transactions,
  which is what distinguishes a connector that recovered from one that merely
  stopped.

### The expectation is read from the collection

The oracle each document carries makes a destination row self-checking, and that
catches duplication and gaps arithmetically. It does not catch a *tail-truncated*
materialization, which is internally consistent: every delivered prefix agrees with
its own oracle, and conservation still holds because a transaction boundary
contains whole capture checkpoints, so both legs of every transfer are present.

So the harness reads the collection itself with `flowctl collections read` and
compares. That expectation is authoritative and the connector under test had no
hand in it, which is what makes "never loses data" a real check rather than a
hopeful one.

### Destination reads go through the connector binary

Retrieving all rows of a materialized resource is already a required method of the
shared materializer interface, implemented once for every SQL destination. A
harness in the flow repository cannot call Go code across the repository boundary,
so `materialize-boilerplate` exposes what it already has as a `read` subcommand
emitting newline-delimited JSON, and the reference connector implements the same
subcommand. One code path serves both.

### Compliance model: default-strict with justified exemptions

Every connector is held to every invariant. Anything weaker is an explicit
`Exemption` naming the invariant and carrying a written justification — a required
constructor argument rather than an optional field, so one cannot be added without
stating why.

The rejected alternative was having each connector declare its class and running
only that class's invariants. It fails for a specific reason: the cheapest way to
make a failing test pass becomes downgrading the claim, and a connector that
silently regresses to a weaker class gets reclassified and passes. Default-strict
inverts that pressure, and the set of exemptions becomes a map of where the fleet
is actually weak.

An exemption records a reviewed property, not a defect. The Snowpipe Streaming v2
deviation is the model case: rows become visible before the Flow transaction
commits, and rows appended by a transaction that never commits persist — recovery
skips rather than re-sends them, so a *committed* transaction is still
exactly-once. That must be declared, so the checker's silence about it is a
decision somebody made rather than an accident.

### The suite proves itself

A consistency suite that passes vacuously is worse than no suite, because it
manufactures confidence that agents and reviewers act on. So the reference
connector's defects are switchable by configuration, and every scenario names the
defect it must catch. `tests/scenarios.rs` runs each scenario twice and asserts the
outcome flips; `scenarios.rs` has unit tests asserting that every scenario is
paired and every defect is reached by some scenario.

The runner adds two guards of its own, both of which turn a silent vacuous pass
into a failure:

- A scenario whose fault never fired fails, rather than passing on an unperturbed
  run.
- After the fault, the task must commit further transactions before the run
  proceeds. A crash that left the shard permanently down would otherwise look
  like a pass.

### One seam

The entire harness is tested at its highest point: the scenario runner driving the
reference connector. Everything beneath — shim framing, trace parsing, invariant
checkers, split driving, task publication, destination reads — is covered
transitively.

There are deliberately no unit seams below the runner, with one exception: the
invariant checkers are pure functions over documents and are unit-tested in
`invariants.rs`, because their failure mode is *silent blindness* rather than a
loud error, and a checker that has gone blind is exactly what the negative runs
exist to catch. Fragmenting coverage further is precisely how a suite ends up with
green units and a blind end-to-end result.

These are assertion-shaped tests, a departure from this repository's snapshot
convention. The oracle makes the correct answer computable rather than recorded, so
a snapshot would add a stale artifact without adding information.

## Deviations from the spec, collected

| Spec said | Implementation does | Why |
| --- | --- | --- |
| One capture, several collections | Two single-binding captures | `source-soak` partitions documents across bindings, which would make conservation and standard/delta agreement uncheckable per collection |
| Workload published once per stack | Workload published per run | The spec's model leaves no quiescent moment to read, and per-run is strictly more isolated |
| Link `gazette`/`labels` to drive splits | Shell out to `flowctl raw split-shards` | That subcommand already exists, already refuses non-V2 tasks, and returns a status rather than text to parse. The spec's concern was parsing CLI text, which does not apply |
| Shard joins covered | Not implemented | There is no `join-shards` counterpart to `split-shards`, and the data-plane auth it needs is private to `flowctl`. Belongs there as a sibling subcommand, not as a second copy here |

## Deferred

**The `Apply`-drains-pending-work scenario.** The connector implements the drain,
and `Apply` is idempotent, but no scenario exercises it — because the precondition
cannot currently be arranged. Staged work is pending only between a transaction's
recovery-log commit and its `Acknowledge`, and a crash there is repaired by the
restart's own first `Acknowledge` long before the run's next publication drives an
`Apply`. Setting it up needs the materialization *stopped* while holding committed
staged work, then restarted with a changed spec — three publications and a shard
disable, where the run currently does one publication of the captures alone. Worth
doing; not worth a scenario that silently fails to establish what it claims to
test.

**A destination genuinely behind its checkpoint.** The document-counter class
refuses this state rather than guessing at it, which is the right behaviour and is
implemented — and unexercised. No fault the shim can inject produces it: it needs
the destination tampered with from outside the connector's protocol stream, which
is the one thing the shim deliberately cannot do. A scenario would need a harness
hook that mutates the destination between sessions.

**Shard joins.** The one specified scenario family that is not implemented.
`activate::task_changes` already computes deletions when handed fewer desired
shards than exist, so a join is expressible — but it needs an authorized shard
client, and `flowctl::dataplane::user_task_admin` is private. The right move is a
`flowctl raw join-shards` beside `split-shards`, after which the harness gains a
`join_shards` flag mirroring `split_shards`.

**Multi-shard coordination scenarios.** Coordinator-crashes-with-peers-alive and
its converse need a task that is multi-shard from the start plus a fault targeted
at one shard's process. The shim currently applies its rules per process, so both
shards would match the same rule; targeting needs the rule to be scoped by key
range, which the trace already records.

**CI gating.** Connectors CI downloads released Flow binaries and has no flow
checkout, and publishing a catalog needs the control plane. Whether building flow
in CI is worth it depends on real per-connector runtime, which could not be
estimated honestly before the harness existed. A deferral, not a rejection.

**Reactor-level zombies.** Running two reactors and partitioning the owner from
etcd would exercise the runtime's fencing and the connector's together at maximum
fidelity, but it is lease-timing-dependent and needs privileged host manipulation.
The shim-based zombie is deterministic and targets the connector's own fencing,
which is what a connector-compliance suite should judge — and the split-derived
zombie picks up the runtime half for free, since the split workflow fences the
source shard's primary off its recovery log and then unassigns it.

**Connectors predating the shared materializer interface** — webhook, slack, sns,
pubsub, kafka, pinecone, sheets, and the csv/parquet file materializations — have
no destination-reading method and are out of scope until they migrate. Most are
at-least-once by design and would be exempt regardless.

## Rules for future scenario authors

Stated so they survive this document:

1. Scenarios are keyed on protocol events, not document identity.
2. Assertions happen at quiescence, not mid-flight.
3. Scenarios never touch stack-wide state.
4. No scenario is finished without a paired defect it provably catches.
