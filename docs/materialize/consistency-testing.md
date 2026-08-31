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

The suite observes exactly that shape, repeatably: no loss, no duplicates, conservation
intact and oracle agreement intact, alongside a crop of monotonicity complaints — 9 to 33 per
run on the reconfiguration scenarios, and 40 to 54 on the counted channel's, measured after the
two monotonicity checkers were made to score a regression the same way.

**The mechanism is not understood, and this document used to claim it was.** The natural
explanation — a split child delivering a sequence the departing parent had already raced past —
does not survive reading the code: on the delta paths these classes use, a parent write past a
child's resume point is either refused by the fence or lands as a duplicate, and duplicates are
*not* exempt, so such a run would fail on `NoDuplicates` regardless. Something else reorders
delivery, and naming it is open work.

So the reconfiguration scenarios declare a monotonicity exemption, with the set-based checks
explicitly *not* exempt. Those four carry the exactly-once claim, and they are the ones a split
has to keep — which is what makes an unexplained ordering deviation tolerable: whatever causes
it, it demonstrably does not cost or duplicate a document.

This is the compliance model earning its keep, and also showing its limit. The weaker property
is declared in one place and everything else is still held — but a justification is only as good
as its reasoning, and this one is now honest about having none. Two exemptions were deleted
during review for describing mechanisms that could not fire *and* suppressing nothing; these
suppress a great deal, so they stay, with the observation recorded instead of a story.

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
suite does with a script. Unassigning is *preferred* over republishing, because a republish
bumps the materialization's version and opens a new session, perturbing the task under test
at the moment the run has stopped perturbing it on purpose. But it is preferred, not
absolute: unassigning restores a split task only about two runs in three, so after a third
of its budget `recover` escalates to exactly that republish rather than fail the run. See "Crashing a split
shard" below.

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

So the harness reads the collection itself with `flowctl collections read` and
compares. That expectation is authoritative and the connector under test had no
hand in it, which is what makes "never loses data" a real check rather than a
hopeful one.

### Destination reads go through connector code, not through a client of our own

Verification must read what actually landed, and the protocol offers no way to: `Load`
answers only the keys the runtime asks about, and only for bindings that are not
delta-updates. Reaching into the destination directly would mean the harness carrying a
client for every endpoint it might be pointed at, which is the thing it exists not to do.

Retrieving all rows of a resource is already a required method of the shared materializer
interface — `Materializer.SnapshotTestResource` — as is removing one, `DeleteResource`. The
harness is Rust and the connectors are Go, so reaching them needs a process boundary.

**Two designs were built here, and the second is better.** The first added `read` and
`drop-resource` subcommands to `materialize-boilerplate`, which every connector then carried.
That was wrong twice over: a production CLI should not grow surface for a test harness, and
the subcommands were a *second implementation* of methods that already existed, so the
integration tests and the harness had two accounts of what a resource holds.

The second, and what is here now, is `tests/materialize/testctl` in the connectors
repository: a program outside the connector that calls those same two methods. It needs the
connector's package to be importable — `package connector` with `func main` under
`cmd/connector`, which `materialize-iceberg` already did and `materialize-databricks` was
converted to — and it means no connector grows a subcommand for this suite.

So there are deliberately **two** read paths, and `stack::ReadVia` makes the choice explicit
rather than implicit in which arguments happen to be set:

- the reference connector's own `read` subcommand, which is fine because it lives in this
  repository, nothing else runs it, and it is Rust that `testctl` cannot drive;
- `testctl` for a real subject.

Removing a resource is `testctl` only. The reference connector's destination is a file inside
the run directory, deleted with it.

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

Unit seams below the runner are the exception rather than the rule, and each one earns
its place by having a failure mode the end-to-end runs cannot show. The invariant checkers
are pure functions whose failure mode is *silent blindness* rather than a loud error. The
shim's framing, the store's SQL, the subject's schema parsing and the harness's own
tabulation are all likewise unit-tested, because each can be wrong in a way that makes a
scenario pass. What is deliberately absent is a unit seam that would let a scenario be
*replaced* by unit coverage — that is how a suite ends up with green units and a blind
end-to-end result.

These are assertion-shaped tests, a departure from this repository's snapshot
convention. The oracle makes the correct answer computable rather than recorded, so
a snapshot would add a stale artifact without adding information.

### The four classes

The reference connector implements each independently of any real connector, so the harness
cannot bake in one vendor's assumptions, and so the document-counter class is executable
before any production connector adopts it.

| Class | Commits during | Authority | Fenced by |
| --- | --- | --- | --- |
| `remoteAuthoritative` | `StartCommit` | destination checkpoint | nonce table |
| `postCommitApply` | `Acknowledge`, from durable staging | recovery log | — |
| `documentCounter` | `Store`, appending to a counted channel | destination count | — |
| `atLeastOnce` | `Store` | recovery log | — |

Three details of `postCommitApply` are load-bearing and follow `materialize-databricks`
rather than being invented here. Its checkpoint carries the *statements* which apply a staged
batch, keyed by binding — not a pointer to work the destination is asked to rediscover,
because leftover staging cannot say whether its transaction committed or was abandoned. Only
the **primary** shard runs them, learning of its peers' staged work from the aggregated state
patches the runtime delivers with `Acknowledge`, so two shards never contend for one
binding's table. That describes steady state only: a replay session sends no `Acknowledge` for
the transaction it is recovering, so committed-but-unapplied work reaches the connector in
`Open.state_json` rather than as a re-delivered patch — which is the path
`split-after-commit-before-apply` exercises. The reference connector reads both; only this
sentence used to describe one.

`Apply` is the third detail: `Apply` deliberately drains nothing — not because it cannot, since
`Apply.state_json` exists and runtime-next populates it, but because draining there would add a
second reconciliation path exercised only by some runtimes, where `Acknowledge` is the path
every transaction already takes. And the load is deferred until `Flush`,
which is what makes `split-during-commit` pass rather than the expected failure it once was —
see "Why a coordinating connector must not read at `Load`" below.

Where the reference diverges from `materialize-databricks` deliberately, `Session::primary`
in `reference/mod.rs` says so at the site.

### The counted channel

The document-counter class emulates the *production* Snowpipe Streaming v2 design — not
the snowflake streaming path in the connectors repository, which stages blobs during
`Store` and registers them at `Acknowledge`, so its rows are not visible before the
transaction commits and its recovery is blob-sequenced rather than counted.

**The offset belongs to the destination.** One channel is opened per (binding, shard);
several channels of one binding append to the same destination table, each with its own
offset. As each write lands the *destination* increments that offset, atomically with
accepting the row. The connector keeps the offset in its checkpoint too — committed with
the recovery log — and on restart compares the two: `skip = destination − checkpointed`
tells it how many documents of the replayed transaction the destination already holds. A
destination *behind* the checkpoint is impossible and is refused rather than guessed at.

It also has to be **delta-updates only**. An offset counts rows the destination accepted,
which says nothing about an upsert; Snowpipe v2 supports only delta bindings, so the
reference class is never *given* a merge binding rather than emulating something no such
connector does — the scenario decides the binding set, and nothing in the connector refuses
one. Scenarios therefore choose their binding set, and a subject without a standard
binding is still held to per-document cardinality, running-sum-against-oracle and
monotonicity — the sharpest checks here.

### A known runtime limitation: a prepared transaction must outlive a membership change

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

It does **not** reach a class that only stages during `Store`. Nothing of a prepared
transaction is in the destination when the split lands, so the children have nothing to
append twice; they inherit the staged work and a merge that runs again is a no-op.

**The failure is intermittent, and the reason is worth knowing before reading a result.** The
hazard needs the runtime to hand the range over *mid*-transaction, and it usually does not — it
finishes the transaction it is in and hands over at a quiet point, which is a committed
transaction and no hazard. So the run either lands in the window or does not, and when it lands
it lands narrowly: a caught run delivered 2072 log rows against 2070 documents — two rows twice,
both of them documents the expectation holds.

Forcing the overlap does not work, and two attempts to do so are recorded in the scenario because
both made it *pass*: issuing the split only once the stall had begun, and lengthening the stall.
Given a shard that will hold still, the runtime takes the quiet point. Asking it to hand over at a
moment of the harness's choosing is asking for the guarantee under test, so the overlap is left
unsynchronized on purpose. Read a passing run as evidence about that run and nothing more —
`split-during-commit` reaches the same destination state deterministically by crashing instead, so
coverage does not rest on this race.

`split-lands-on-prepared-transaction` carries this as a `blocked_on_runtime` gap scoped to
`DocumentCounter`, and runs for every exactly-once class. For the counted channel it is the
suite's one expected failure. For the others it is expected to pass, and that is the more
useful half: a scenario that only ever failed would leave open whether the perturbation is
survivable at all, whereas one that passes for a staging class and fails for a counted one
locates the gap in the runtime rather than in the ask.

### Why a coordinating connector must not read at `Load`

Post-commit-apply has one shard apply staged work on behalf of its peers, which means the
shard that *loads* a key and the shard that *applies* it are different processes. Nothing
in the protocol orders them directly: the leader emits `Action::Load` on its *extend* path,
and `tail_done` gates only `may_close`, so a transaction's load phase can begin while the
previous transaction is still being acknowledged.

`Flush` is what closes that window. It is sent only once the Tail reaches `Done`, which
requires every shard's `Acknowledged` — so a connector that stages load keys as `Load`
requests arrive and reads the destination only when `Flush` comes has, by construction,
waited for the applying shard to finish.

This is not a workaround; it is what every connector of every class in the fleet already
does. `materialize-databricks`, `-snowflake` and `-bigquery` write keys to a staging file
inside the `it.Next()` loop and join afterwards; `materialize-postgres` queues them into a
temp table and joins afterwards. The boilerplate makes the guarantee explicit at that exact
point, calling `WaitForAcknowledged` when no loads remain — *"Block for clients which stage
loads during the loop and query on our return"* — and panics if a `Loaded` response is
written before it.

Reading the destination per `Load` request instead is the one arrangement that breaks, and
it breaks only for merged bindings: a base missing the applying shard's work is reduced
onto, and the difference is lost. `split-during-commit` is the scenario that catches it.

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

**A task that is multi-shard from the start.** Every multi-shard scenario reaches that
state by splitting a single-shard task, never by starting with two. Targeting a fault at one
shard is no longer the obstacle — `ShardTarget` scopes a rule by key range, and
`crash-in-split-leader` and `crash-in-split-non-leader` use it — but a task born multi-shard
would exercise a different startup path, with no inherited state for either shard.

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

**Deletions, entirely.** The workload only ever inserts and updates: `source-soak` emits no
tombstones, so nothing in the suite exercises a delete. Every question about them is therefore
open — whether a replayed delete is idempotent, whether a document deleted before a crash stays
deleted after one, whether a delete and a later insert of the same key survive reordering. This is
the largest single gap by surface area, and closing it needs a workload change rather than a
scenario.

**A wedged split is ambiguous, by construction.** For `split-during-commit` and
`split-after-commit-before-apply`, mutually-fencing children *are* the paired defect's signature —
and for `crash-in-split-leader` and `crash-in-split-non-leader`, whose defects duplicate rather
than wedge, the same wedge would be environmental. Both reach the post-split recovery gate before
their fault fires, so neither reading can be chosen positionally, and the gate is left unmarked:
an environmental wedge in one of the latter two scores as a defect caught. Distinguishing them
needs evidence the split actually landed — the trace carries it, in the narrowed ranges of the
children's `Opened` events — which is a check worth writing when something makes it matter.

**Protocol surfaces no scenario perturbs.** Each is reachable in principle and none is
covered, so they are listed rather than left to be rediscovered:

- **A crash during `Apply`.** Every scenario's fault lands in the transaction loop, so a
  connector interrupted midway through creating or altering its tables is never tested — and
  `Apply` is where a connector is least likely to be idempotent, since it is written as though it
  runs once.
- **A backfill counter bump, and a binding disabled and re-enabled.** Both change what a
  binding means between sessions while its resource stays put, and both are ordinary user
  actions. Neither appears in any scenario's catalog.
- **A second crash during a replay.** Every crash scenario fires once, so recovery itself is
  never interrupted. The fired-marker that makes a fault one-shot is what stands in the way; a
  rule would need to distinguish "the nth occurrence in this process" from "in this run".

**Mid-history loss on the merged path, which the arithmetic cannot name as loss.** The merged
bindings detect divergence arithmetically — a reduced balance against the oracle that names it, and
a delta history that must accumulate to the same figure — and arithmetic over a signed quantity
cannot say which way it went. It is tempting to read a total below the collection's as loss and one
above it as duplication, which would take merged-path loss out from behind the exemptions that
license duplication, and it does not hold: `balanceDelta` is mixed-sign within an account, so
omitting a subset moves the total by whichever sign that subset carries. One measured account
showed both directions at once — rows missing, total *below* the collection's at the account level
and *above* the oracle at an intermediate sequence.

Two consequences. A shortfall confined to a merged binding is reported, but as an oracle
disagreement, so a scenario exempting that invariant for duplication also absorbs it. And losing
two documents of one account whose deltas cancel
is invisible there at all.

What *is* sound on these bindings is sequence coverage, because a sequence only advances: both
merged checkers hold an account's delivered rows to reaching the collection's latest sequence, and
file a shortfall as loss. That catches a missing tail and not a missing middle. The log binding holds a row per document and settles it exactly, which is why
every scenario has one and why a subject without delta-updates support is refused outright. What
remains uncovered is a connector that loses on the *merged* path only, with a cancelling
coincidence. Two fixes were considered and both cost more than the hole: a summed `docs: 1` on
every event makes the count exact but changes the soak fixture this suite deliberately reuses
unmodified, and reducing `set` with the `set` strategy to check it against `oracle.set` compares
an order-dependent value, which around a membership change would need a reordering exemption as
broad as the one monotonicity already carries — trading an exact check for a suppressed one.

## Rules for future scenario authors

Stated so they survive this document:

1. Scenarios are keyed on protocol events, not document identity.
2. Assertions happen once the task is idle, not mid-flight.
3. Scenarios never touch stack-wide state.
4. No scenario is finished without a paired defect it provably catches.

## Crashing a split shard

Two scenarios crash a shard that a split produced, and they are deliberately separate
because the two shards fail differently. `crash-in-split-leader` crashes the child
that is also shard zero — it owns half the keyspace and holds the recovery log.
`crash-in-split-non-leader` crashes a non-zero child, which in a V2 task is
stateless: no recovery log, its state arriving by leader broadcast, so it is rebuilt from
nothing rather than replayed from a log.

Three properties shape how these scenarios are written, and each is a constraint rather
than a preference.

**A split alone is too weak a perturbation to rely on.** Not because it lands at a transaction
boundary — it does not, as "Any split scenario passes through that window" below explains — but
because whether it creates the replay these defects need is a race. So a split-only scenario
passes in both halves often enough to establish nothing, and both scenarios inject a crash as
well, which makes the replay certain rather than incidental.

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
because they pass whenever the race falls the other way, which is most of the time. That is
a deliberate asymmetry with `RuntimeGap`, and an uncomfortable one: a `RuntimeGap` scenario
is *required* to fail, so nobody can quietly stop looking at it, whereas these are permitted
to fail without anything recording that they did. If they turn out to fail often enough to
be noise rather than signal, the fix is a `RuntimeGap` for the same underlying capability
rather than a retry. Two
signatures are worth recognising. Duplicates with no losses in a counted-channel scenario is
the runtime limitation above. A merged binding whose value disagrees with its own delivered
rows — in both directions, with the total not conserved, while the append-only bindings are
exact — is a connector reading its destination before `Flush`.
