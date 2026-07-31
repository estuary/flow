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

### Joining shards needed a new subcommand

Splitting was already a `flowctl` subcommand. Joining was not, and the spec assumed
the harness would drive both by linking `gazette` and `labels` directly. It drives
neither: the data-plane admin authorization both need lives inside `flowctl` and is
not exported, so a second copy here would be an untested duplicate of the
fiddliest part of the platform's auth.

So `activate` gained `map_shards_to_join`, the inverse of its existing
`map_shard_to_split`, and `flowctl raw join-shards` sits beside `split-shards`.
The mechanism is the one an operator would use by hand with `gazctl shards
list -o yaml` / `apply`: widen the surviving shard's key range to cover its
partner's, and mark the partner deleted.

Two properties make it safe, and both are why the *lower* shard of each pair is the
survivor:

- **The survivor keeps its identity.** A shard's ID derives from its range
  *begin*, and merging into the lower shard leaves that unchanged — so it keeps its
  ID, its recovery log, and its accumulated state. Only its `end` widens.
- **No key is ever unowned.** `activate::apply_changes` already orders changes so
  that shard upserts land before shard deletions, and those before journal
  deletions. The survivor therefore owns the widened range before its partner
  goes away.

A join is refused unless the pair is genuinely adjacent on exactly one axis — two
shards from the same split. A gap would silently drop the keys inside it and an
overlap would deliver them twice, so guessing is worse than failing.

The asymmetry with a split is real and is recorded in the scenario: a split child
inherits its checkpoint from the range that contained it, but two ranges collapsing
into one leave no single range that contained the result, so a join falls back to
the recovery log. The `join-after-split` scenario therefore asserts only on the
destination, never on which checkpoint the connector chose.

### Monotonicity is not a membership-change-safe invariant

The second finding from the shard-split scenarios, and the one that shaped the
invariant set: **a membership change preserves exactly-once delivery of the *set*,
but not delivery *order* at the sink.**

A split child resumes from its inherited checkpoint and may deliver a sequence that
the departing parent had already raced past, so an id's rows can land out of order
while remaining exactly one row per document. The suite observed precisely that:
1513 documents, no loss, no duplicates, conservation intact, oracle agreement
intact — and 78 monotonicity complaints.

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

The first thing the shard-split scenarios found, once they could run at all, is a
V2 contract rule that a destination-authoritative connector has to respect and
which nothing had previously forced anyone to discover:

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

It is also a good argument for the shard-reconfiguration scenarios being part of the
default set rather than an optional extra: nothing about a single-shard run can
reveal it.

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
| Link `gazette`/`labels` to drive splits | Shell out to `flowctl raw split-shards`, and a new `join-shards` | Splitting already existed as a subcommand that refuses non-V2 tasks and returns a status rather than text to parse. Joining did not exist, and was added beside it rather than reimplemented here — see below |

## Findings from the reconfiguration scenarios

The shard-split and join scenarios were the last to work, and getting them to run
surfaced more than they were written to check. One lead remains open at the bottom.

**`split-during-commit` — one root cause found and fixed, one still open.**

### Found: a `Load` must see staged writes, including an ancestor's

The proof is one account from a failing run's `evidence.json`. Same collection, same
transactions, two bindings:

| binding | value | uses `Load`? |
| --- | --- | --- |
| merged delta | 57 → 48 → … → **321**, matching the oracle at every step | no |
| merged standard | **152** (expected 321), `seq` fully current | yes |

Nothing was lost — the sequence is up to date. Only the reduced *sum* is wrong, and only
on the binding the runtime computes by loading, reducing and storing. Nothing but a
stale `Load` base produces that.

The post-commit-apply class stages during `Store` and applies during `Acknowledge`;
between those points its rows are durable and invisible, and `load()` consulted only the
destination table. **A connector must answer `Load` consistently with everything it has
been asked to `Store` in a committed transaction, applied or not.**

Why it needs a split: the runtime re-uses documents it cached from prior transactions,
so a long-lived session rarely issues a real `Load` for a key it just stored — the bug
is unreachable. Split children start with cold caches. Why it is intermittent: a Load
must land inside the staged-but-unapplied window, which this scenario's 4-second
`StartCommit` stall widens.

Three scopes were measured before the right one:

| staged visibility | clean half | paired defect |
| --- | --- | --- |
| own shard only | 0 of 5 passed | caught |
| every shard | passed | not reliably caught |
| ranges containing mine | 1500–1800 documents upheld | not reliably caught |

Own-shard-only fails because a split child's inherited keys have their staged rows under
the *parent's* wider range. Containment admits ancestors and excludes siblings, which is
the correct rule — sibling ranges never contain one another.

### Found: apply every committed-but-unapplied transaction, not just the newest

`acknowledge` applied only `committed_txn`, the newest, while `discard_staged_after`
removes only transactions *after* it. A transaction staged and log-committed but never
acknowledged, with a newer one behind it, is therefore neither applied nor discarded —
it leaks permanently. A split fences the parent mid-flight, which is exactly how two of
them pile up.

Counting located it where reasoning had not. The shim's trace records documents Stored
per binding, and comparing that against the destination gave, over three failing runs:

| Stored | delivered | in the collection |
| --- | --- | --- |
| 640 | 582 | 600 |
| 790 | 738 | 760 |
| 780 | 724 | 740 |

The connector was handed *more* than the collection holds — a split replays input — and
applied fewer. That eliminated the runtime, the harness and the expectation in one step,
and it is the single most useful measurement in this whole investigation: **compare what
the connector was asked to store against what the destination holds before theorising
about either side.**

This is a real bug and the fix is correct by inspection — `Apply` already looped over
`staged_txns` for exactly this reason, and a unit test pins it. But **it did not close the
log shortfall.** One run afterwards delivered 730 of 730 with no duplicates; two of the
next four were still 18 short (`log="752/770"`, healthy and quiescent). Against a pre-fix
rate of three failures in five, that is indistinguishable.

So: a genuine defect removed, and the symptom it was expected to explain still present.
Whatever strands those documents is either a second path into the same staging leak or
something else, and the open item below is the likelier candidate.

### The counted channel, and why it is the class that survives a split

The document-counter class emulates Snowpipe Streaming v2, and getting it faithful took a
correction worth recording, because the whole point of the class turns on it.

**The offset belongs to the destination.** One channel is opened per (binding, shard);
several channels of one binding append to the same destination table, each with its own
offset. As each write lands the *destination* increments that offset, atomically with
accepting the row. The connector keeps the offset in its checkpoint too — committed with
the recovery log — and on restart compares the two: `skip = destination − checkpointed`
tells it how many documents of the replayed transaction the destination already holds. A
destination *behind* the checkpoint is impossible and is refused rather than guessed at.

The reference connector originally reported a connector-side *mirror* of that offset,
incremented as it wrote, instead of reading the destination back. That is wrong in itself —
a second copy of the only number that matters — and wrong in a way this class cannot
tolerate, since the mirror's drift is invisible in exactly the case the design exists for:
a process dying between the destination accepting a row and the connector noting it.

It also has to be **delta-updates only**. An offset counts rows the destination accepted,
which says nothing about an upsert; Snowpipe v2 supports only delta bindings, so the
reference class refuses a merge binding rather than emulating something no such connector
does. Scenarios therefore choose their binding set, and a subject without a standard
binding is still held to per-document cardinality, running-sum-against-oracle and
monotonicity — the sharpest checks here.

**Why this is the class that survives a membership change.** A counted channel resumes by
asking the destination how far it got, so a newly created shard needs no inherited state:
a fresh channel simply starts at offset zero. Compare the post-commit-apply dead end
below — a child inheriting staged work cannot tell whether its own resume point precedes
it, so it must either duplicate or lose. The counted channel never asks that question.
`counter-survives-a-split` verifies it directly, and it is the scenario that most closely
mirrors what the Snowpipe path actually relies on in production.

### A known runtime limitation: a prepared transaction must outlive a membership change

Two scenarios fail for a reason that is **not** a connector defect and not a harness
defect. It is a known limitation of the current runtime, and fixing it is out of scope for
this suite. The scenarios are kept, red, because they are the regression detector for it.

The rule the runtime does not yet enforce:

> Once a transaction is prepared, that same transaction **and the same shard split** must
> be used to finish it, through to the commit of the driver checkpoint. A change in the
> number of shards should only become active after any prior prepared transaction has been
> fully processed.

Without that guarantee, a split or join landing between "prepared" and "checkpoint
committed" corrupts any strategy that reconciles against a per-shard destination counter:

**Scaling down** — `[0, 5)` and `[5, 10)` become `[0, 10)`. A prepared transaction holds 7
keys per range, 14 total; each shard writes its 7, so each channel counter reads 7. The
task scales down and crashes before the driver checkpoint commits. The single new shard
reads a counter of 7 and a checkpointed 0, so it skips 7 of the coming transaction rather
than 14 — **duplicates**.

**Scaling up** — `[0, 10)` becomes `[0, 5)` and `[5, 10)`. The prepared transaction holds
14 keys; `[0, 10)` writes all 14. The task splits before the checkpoint commits. A `[0, 5)`
shard that resolves "the channel whose range starts at 0" sees 14, skips 14, and **misses
data**; `[5, 10)` opens a new channel and **duplicates** its first 7.

**What the suite observes, and one difference worth keeping.** The crash-then-split run
shows 40 duplicates, every one exactly ×2, and *nothing missing* — one transaction's worth
of appends landed twice. The scaling-up analysis predicts one child missing data and the
other duplicating; this connector only duplicates, because its channels are keyed by the
whole range `(key_begin, key_end)` rather than by `key_begin` alone. A new child therefore
never inherits an offset that isn't its own, so it never over-skips, so it cannot silently
lose data — it can only duplicate.

That is worth preserving beyond this suite: **keying a channel by the shard's full range
converts the scaling-up failure from silent data loss into duplication.** Duplication is
detectable at the destination; loss of a prefix is not. It does not fix the limitation, but
it makes the limitation's consequences the safer of the two.

**What passes anyway.** A split with no transaction in flight is survived cleanly: the
counted-channel class passed three consecutive runs across a split of 1196, 1458 and 1498
documents when no appends were pending. The limitation bites only in the window the
mitigation names.

### The post-commit-apply limit: staged work cannot be inherited across a split

Range-pair keying (below) removed the ambiguity between an ancestor and a sibling, and
produced the first fully-correct run — the clean build upheld every invariant over 1913
documents while the defective build was caught with 129 violations. But four runs in five
still fail, now by *duplicating* rather than losing, and the evidence says why.

Every failing account has repeated `(id, seq)` rows in the merged delta binding, and the
running sum diverges from the oracle **exactly at the first repeat**:

| account | repeated seqs | first divergence | final oracle |
| --- | --- | --- | --- |
| 2 | 4, 6 | seq 4, off by −34 | correct |
| 3 | 3, 5 | seq 3, off by −59 | correct |
| 4 | 1, 3 | seq 1, off by +74 | correct |
| 5 | 2, 3 | seq 2, off by −110 | correct |

Accounts that pass have no repeats. Final oracle and final sequence are right everywhere,
so nothing is lost — early transactions are applied twice. The append-only binding agrees:
783 delivered against 779 expected, three duplicates, nothing missing.

The cause is the repair itself, and it is not a keying problem. When a split child finds an
ancestor's staged-but-unapplied transaction it faces a question it cannot answer: **is my
own resume point before or after that transaction?**

- If after, the work is committed and unapplied, and applying it is the only way to avoid
  losing it — the leak fixed above.
- If before, the runtime is about to replay that same input, and applying it duplicates.

A shard's resume point is not something the connector is told. For shard zero it follows
its own recovery log, but a *non-zero* V2 shard is stateless — no recovery log, its
progress arriving through the leader — so a child's starting point can legitimately predate
its ancestor's last committed transaction. Applying risks duplication; discarding risks
loss; the connector has no third option.

**The conclusion is about the class, not the reference connector.** Post-commit-apply
staging is only safe across a membership change if re-application is idempotent *per
document* rather than per transaction — which a keyed, merged destination gets for free and
an append-only one cannot have without a deduplication key. That is precisely why the
Snowpipe Streaming v2 path uses a *counted channel* — the document-counter class, which
resumes by asking the destination how far it got — rather than post-commit staging. This
scenario has, the long way round, derived the reason that design exists.

So `split-during-commit` against the post-commit-apply class is testing something that
class cannot do. The options are to pair the scenario with a connector class that can
(document-counter), or to keep it as a declared exemption with this reasoning attached.
Either way it should not be silently made green.

### Open: the paired defect no longer bites

`IgnoreKeyRange` makes both shards claim the *identical* full range, so containment
admits them to each other's staging and repairs part of the damage the defect exists to
cause. The scenario needs a defect containment cannot repair — `DropDocuments` is the
obvious candidate — but re-pairing it while the clean half still fails would only hide
the open bug above.

**`join-after-split` — the task does not resume committing after the join.** The
join itself applies correctly (verified by hand: two shards collapse to one covering
the full range, and the survivor keeps its ID). What is unresolved is recovery
afterwards: the runner's unassign-until-progress loop times out. A join deletes a
shard while the survivor widens, so the likely candidates are the departing shard's
assignment lingering, or the survivor needing to re-open before it will commit under
its new range.

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
