# Consistency testing for materialization connectors

Why the suite in `crates/materialize-consistency` is shaped as it is: the
reasoning, the alternatives that were rejected, and the places where implementing
it changed the plan. The suite's own README is the roadmap; this is the record.

## The problem

A materialization connector is expected to uphold exactly-once delivery, and
before this suite there was no mechanical way to find out whether it does.

- The idempotent-apply contract — re-running a recovered checkpoint's staged work —
  was tested by hand-calling RPCs in-process, with the runtime simulated by the test.
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

**A fired fault is recorded as a file, so it fires only once.** A crash fault kills
the connector and the runtime starts a fresh one, which would reach the same trigger
and crash again — forever. The file outlives the process, so the replacement knows the
fault has already fired. It is created with `create_new`, which makes the claim atomic
when two processes race for it.

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
crash-and-replay scenario replays byte-identical input by construction.

### Transaction boundaries are approximate, by choice

The runtime's close policy has document- and byte-count ranges, but they are not
threaded through from the spec; only minimum and maximum transaction duration are.
So transaction size is shaped by those durations plus a rate-paced capture, giving
roughly `rate × min duration`. The shim cannot help: it can stall a response to
*extend* a transaction, but it has no way to make one close.

Hence the rule that constrains every scenario: **keyed on protocol events the shim
observes, never on document identity.** This costs nothing because verification is
invariant-based rather than snapshot-based.

### Invariants are checked once the task is idle

Not a convenience. Some legitimate patterns make rows visible before the Flow
transaction commits — the document-counter class appends during `Store` — so a
mid-flight destination read would report a violation where none exists.

**Deviation: a run goes idle by disabling its own captures.** The spec had the
workload published once per stack, which leaves the capture running forever so a
scenario never has a settled moment to read. Publishing the workload per run
instead bounds the data volume, lets the task go idle, and is *more* isolated,
not less. It respects the rule that matters — a scenario touches only its own
tasks, never anything stack-wide.

### Shard surgery runs through scripts, not new `flowctl` subcommands

Splitting is already a `flowctl` subcommand. Unassigning a shard and joining a task's
shards are not, and they are local test affordances rather than things an operator needs
from the CLI, so the suite ships them as scripts under `scripts/` that drive `gazctl`.
`flowctl raw gazctl-env` supplies the data-plane addresses and tokens, so no authorization
logic is duplicated.

A join widens the surviving shard's key range to cover its partner's and marks the partner
deleted. Two properties make it safe, and both are why the *lower* shard of each pair
survives:

- **The survivor keeps its identity.** A shard's ID derives from its range *begin*, and
  merging into the lower shard leaves that unchanged — so it keeps its ID, its recovery
  log, and its accumulated state. Only its `end` widens.
- **No key is ever unowned.** Shard upserts land before shard deletions, so the survivor
  owns the widened range before its partner goes away.

A join is refused unless the pair is genuinely adjacent on exactly one axis — two shards
from the same split. A gap would silently drop the keys inside it and an overlap would
deliver them twice, so guessing is worse than failing.

The asymmetry with a split is real and is recorded in the scenario: a split child inherits
its checkpoint from the range that contained it, but two ranges collapsing into one leave
no single range that contained the result, so a join falls back to the recovery log. The
`join-after-split` scenario therefore asserts only on the destination, never on which
checkpoint the connector chose.

### Monotonicity is not a membership-change-safe invariant

**A membership change preserves exactly-once delivery of the *set*, but not delivery
*order* at the sink.**

A split child resumes from its inherited checkpoint and may deliver a sequence that
the departing parent had already raced past, so an id's rows can land out of order
while remaining exactly one row per document. The suite observes exactly that shape: no
loss, no duplicates, conservation intact and oracle agreement intact, alongside a crop of
monotonicity complaints.

So the reconfiguration scenarios declare a monotonicity exemption, with the
set-based checks explicitly *not* exempt. Those four carry the exactly-once claim,
and they are the ones a split has to keep.

This is the compliance model earning its keep in the direction it was designed for:
the weaker property is declared and justified in one place, and the connector is
still held to everything else. The document-counter class needed the same exemption
for a different reason — rows of an uncommitted transaction stay visible until
recovery skips past them — which is a hint that sink ordering is simply not a
property the runtime offers whenever sessions can overlap.

### Only shard zero may propose a runtime checkpoint

Under V2 the non-zero shards of a leaderful task are **stateless** — they have no
recovery log and acquire everything through the leader protocol. The leader
therefore *refuses* an `Opened` from a non-zero shard that carries a runtime
checkpoint, and the whole task fails with `expected Opened` during its fan-in.

So however authoritative a destination is for the *data*, a connector must gate its
checkpoint on being shard zero. The reference connector's `remoteAuthoritative`
class returned its stored checkpoint from every shard, which worked perfectly on a
single-shard task and took the task down the moment it was split. That is exactly
the class of latent defect the suite exists to surface, and it surfaced here in the
suite's own reference connector before any production one.

### The reference destination is genuinely shared, and that has teeth

SQLite was chosen so that "commits during `StartCommit`" and "commits during
`Store`" are really different behaviours, and so that a zombie's stale write is
refused by the *destination* rather than by a check the connector performs on
itself. That means one file is written by several processes at once: two shards of a
split task, or a live instance and its zombie.

Getting that wrong cost three attempts, all recorded in the history because the
wrong ones are instructive:

1. `PRAGMA journal_mode = WAL` needs a brief exclusive lock and SQLite fails it
   outright rather than consulting the busy handler. Set the mode only when the file
   is not already in it — it is a durable property, so the process that creates the
   file sets it uncontended.
2. A busy timeout does not cover a **deferred** transaction upgrading from read to
   write: in WAL mode that returns `SQLITE_BUSY_SNAPSHOT` immediately. Every
   read-then-write transaction must be `BEGIN IMMEDIATE`.
3. Only then does the busy timeout do what it looks like it does.

The lesson that generalises past SQLite: **a scenario author debugging a shard
failure should read the task's connector logs before believing where the error
points.** All three of these presented as `Materialize error (expected leader
message) from leader / receiving Opened fan-in` — a leader-protocol failure two
layers above the actual fault, which was a connector exiting during `Open`.

### Recovering a crashed shard is part of the scenario

A crash fault is only half of crash-and-replay. A Gazette shard whose processing loop
fails is marked FAILED and stays that way, so recovery means unassigning it, which the
suite does with a script rather than by republishing the task — a republish would bump the
materialization's version and open a new session, perturbing the task under test at the
moment the run has stopped perturbing it on purpose.

One consequence is worth knowing:

- **The runner requires progress after the fault.** Recovery is not assumed from
  the unassign returning: the scenario waits for further committed transactions,
  which is what distinguishes a connector that recovered from one that merely
  stopped.

### The expectation is read from the collection

Every document carries an oracle, so a destination row can be checked against itself.
That catches duplicates and gaps. It cannot catch a materialization that simply *stops
early*, because everything it did deliver is correct — the rows that arrived agree with
their oracles, and the sums still balance.

So the harness reads the collection and compares against that instead.

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

### The counted channel

The document-counter class emulates Snowpipe Streaming v2.

**The offset belongs to the destination.** One channel is opened per (binding, shard);
several channels of one binding append to the same destination table, each with its own
offset. As each write lands the *destination* increments that offset, atomically with
accepting the row. The connector keeps the offset in its checkpoint too — committed with
the recovery log — and on restart compares the two: `skip = destination − checkpointed`
tells it how many documents of the replayed transaction the destination already holds. A
destination *behind* the checkpoint is impossible and is refused rather than guessed at.

It also has to be **delta-updates only**. An offset counts rows the destination accepted,
which says nothing about an upsert; Snowpipe v2 supports only delta bindings, so the
reference class refuses a merge binding rather than emulating something no such connector
does. Scenarios therefore choose their binding set, and a subject without a standard
binding is still held to per-document cardinality, running-sum-against-oracle and
monotonicity — the sharpest checks here.

### Two known runtime limitations

Two independent gaps each block one scenario. They are unrelated, and closing either leaves
the other's scenario failing — so neither marker should be removed on the strength of the
other being fixed.

#### Gap 1 — a prepared transaction must outlive a membership change

The runtime does not yet provide a capability that
[discussion 2581](https://github.com/estuary/flow/discussions/2581) names as a requirement
for materialization scale-out:

> Idempotent runtime transactions that respect shard splits: Any transactions that have
> been started with a given task shard split must be replayed with that same shard split
> before a shard scale up / down is applied.

Put the other way: a change in the number of shards should only become active once any
prepared transaction has been fully processed.

This reaches the **counted channel**, because that class writes during `Store`. The rows of
a prepared-but-uncommitted transaction are already in the destination when the split lands
and cannot be taken back; the children open fresh channels at offset zero and append the
replayed input a second time. Scaling down is the mirror image — a survivor reads one
departing channel's counter, skips too few, and duplicates.

`counter-split-during-commit` is marked `blocked_on_runtime` for this.

#### Gap 2 — `Acknowledge` is not ordered across shards against the next transaction's loads

A coordinating connector has only one shard apply staged work — the arrangement
`materialize-databricks` uses, so that two shards never contend for a binding's table. That
makes the shard which *loads* a key and the shard which *applies* it different processes,
and nothing orders them.

The only ordering primitive a connector has is `LoadIterator::WaitForAcknowledged`, and it
waits on that shard's own acknowledgement, knowing nothing of its peers'. The window is
structural rather than incidental: the leader emits `Action::Load` on its *extend* path, and
`tail_done` gates only `may_close`, never `may_extend`. The leader's own test says as much —
*"Head opens txn 2 via a fresh ready Frontier — pipelined with Tail."*

So a non-primary shard can load a key before the primary has applied the previous
transaction's staged value for it, reduce onto that stale base, and write a merged value
which loses the earlier contribution. The failure is confined to merged bindings:
append-only bindings are handed each document once and never read one back.

Closing it needs the runtime to tell a shard that *all* shards have acknowledged, so
`WaitForAcknowledged` can mean what it already claims — that a connector may then issue
loads without violating read-committed semantics. This has been raised with the data-plane
team.

`split-during-commit` is marked `blocked_on_runtime` for this.

**One property worth carrying elsewhere.** Keying a channel by the shard's whole range
rather than by `key_begin` alone converts the scaling-up failure from silent data loss into
duplication: a new child never inherits an offset that isn't its own, so it cannot
over-skip. Duplication is detectable at the destination; a lost prefix is not.

## Deferred

**A destination genuinely behind its checkpoint.** The document-counter class
refuses this state rather than guessing at it, which is the right behaviour and is
implemented — and unexercised. No fault the shim can inject produces it: it needs
the destination tampered with from outside the connector's protocol stream, which
is the one thing the shim deliberately cannot do. A scenario would need a harness
hook that mutates the destination between sessions.

**Multi-shard coordination scenarios.** Coordinator-crashes-with-peers-alive and
its converse need a task that is multi-shard from the start plus a fault targeted
at one shard's process. The shim currently applies its rules per process, so both
shards would match the same rule; targeting needs the rule to be scoped by key
range, which the trace already records.

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
2. Assertions happen once the task is idle, not mid-flight.
3. Scenarios never touch stack-wide state.
4. No scenario is finished without a paired defect it provably catches.

## Crashing a split shard

Two scenarios crash a shard that a split produced, and they are deliberately separate
because the two shards fail differently. `counter-crash-in-split-leader` crashes the child
that is also shard zero — it owns half the keyspace and holds the recovery log.
`counter-crash-in-split-non-leader` crashes a non-zero child, which in a V2 task is
stateless: no recovery log, its state arriving by leader broadcast, so it is rebuilt from
nothing rather than replayed from a log.

Three properties shape how these scenarios are written, and each is a constraint rather
than a preference.

**A split alone perturbs nothing a counted channel can get wrong.** It lands at a
transaction boundary, so nothing is replayed, so no channel has anything to skip — and
skipping is the whole of the class's behaviour. Both scenarios therefore inject a crash as
well: without a replay, a defect about skipping wrongly has no opportunity to misbehave,
and the scenario would pass in both halves while establishing nothing.

**A fault cannot be aimed after a membership change by occurrence count.** `arm_after`
counts a session's own committed transactions, and a split child's count starts at zero, so
any threshold low enough for a child to reach is one the pre-split parent reaches first — the
fault lands before the split rather than after it, and a crash there kills the shard while
the split is still being applied, so the split never lands at all. `ShardTarget` selects the
shard by its *range* instead, which is a property the session has at `Open` rather than one
it accumulates.

**Either shard's death fails the whole task, not just its own shard.** Whichever one dies,
the survivor reports `expected leader message ... unexpected EOF`. Unassigning the failed
shards is not a reliable remedy for that — it restores the task about two runs in three — so
`harness::recover` escalates after a third of its budget to republishing the task disabled
and then enabled, which tears the shards down and rebuilds them from the recovery log. A
restart rather than a reschedule, and what an operator would do.

Whether a V2 task *should* need a republish to survive a connector crash in a split shard is
a question about the runtime rather than about a connector. The suite measures it either way.

### Any split scenario passes through that window

Every scenario that splits passes through the same windows whether it aims at them or not:
the harness cannot ask for a split at a transaction boundary, the workload commits every one
to two seconds, and a split takes seconds to apply, so a transaction is nearly always in
flight when one lands.

The unmarked splitting scenarios are left unmarked rather than declared expected failures,
because they pass whenever the race falls the other way, which is most of the time. Two
signatures are worth recognising, because each is a runtime gap rather than a connector
defect. Duplicates with no losses, in a counted-channel scenario, is Gap 1. A merged binding
whose value disagrees with its own delivered rows — in both directions, with the total not
conserved, while the append-only bindings are exact — is Gap 2.
