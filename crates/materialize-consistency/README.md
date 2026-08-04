# materialize-consistency

A consistency test suite for materialization connectors. It runs a connector as a
real task on a real Flow runtime, deliberately breaks it at precise points, and
checks that the destination still holds exactly the right data.

Companion design document: `docs/materialize/consistency-testing.md` — the
reasoning, the rejected alternatives, and the deviations from the spec that
implementation forced.

```bash
mise run ci:consistency                      # the whole suite
mise run ci:consistency --filter zombie      # one scenario (a nextest filter,
                                             #   so `/a|b/` for a regex)
mise run ci:consistency --debug              # debug logging, output uncaptured
```

Needs a running local stack (`mise run local:stack`) and nothing else: the
reference connector materializes into SQLite, so scenario development costs no
credentials and no cloud spend.

## The shape of it

```
                  publishes                    ┌──────────────┐
   scenario  ──────────────────►  local stack  │ two captures │  the soak
   runner                                      │ two          │  workload,
      │                                        │ collections  │  unmodified
      │  reads the collections                 └──────┬───────┘
      │  (the expectation)                            │
      │                                        ┌──────▼───────┐
      │                                        │ materializa- │
      │                                        │ tion under   │
      │                          ┌─────────────┤ test         │
      │                          │  local:     └──────────────┘
      │                    ┌─────▼──────┐
      │        trace ◄─────┤    shim    ├─────► connector process
      │        faults ─────►            ├─────► zombie process
      │                    └─────┬──────┘
      │  reads the destination   │
      ▼  through the connector   ▼
   invariant checkers ◄──── destination
```

## Key types and entry points

| Where | What |
| --- | --- |
| `scenarios.rs` | The scenario table: what each verifies, its faults, and the defect it must catch. Start here. |
| `protocol.rs` | The shim↔harness contract: `Trigger`, `Action`, `FaultRule`, `TraceEvent`, and the run-directory layout. |
| `shim.rs` | The interposer. Decodes the protocol stream in flight, traces it, injects faults, drives the zombie. |
| `reference/mod.rs` | The reference materialization: four connector classes and seven switchable defects. |
| `reference/store.rs` | Its destination — SQLite, with a real fence table and real staging. |
| `invariants.rs` | The checkers, as pure functions over documents. Unit-tested here. |
| `harness/mod.rs` | The runner: publish, perturb, quiesce, verify, clean up. |
| `harness/catalog.rs` | The catalog a run publishes, and why the workload is shaped as it is. |
| `harness/stack.rs` | Everything needed from the stack, all through `flowctl`. |
| `tests/scenarios.rs` | The suite's one seam: every scenario, run clean and then defective. |

`FaultRule` carries a `ShardTarget` — `Any`, `SplitLeader` or `SplitNonLeader` —
because occurrence counts cannot express "after the membership change": a split child's
count starts at zero, so any threshold it can reach the pre-split parent reaches first,
and the fault lands mid-split instead. A `Scenario` may also carry a `RuntimeGap`, which
makes it an *expected failure for the classes the gap exposes*: it runs and fails with its
violation count, which is the measurement of the gap, while a class the gap does not reach
must pass it normally. The marker is removed when the gap closes.

## Running it against a real connector

The reference connector exists to prove the harness. Any connector can be the subject
instead, named through the environment:

```bash
FLOW_CONSISTENCY_SUBJECT=/path/to/materialize-yourthing \
FLOW_CONSISTENCY_SUBJECT_CONFIG=/path/to/config.json \
FLOW_CONSISTENCY_SUBJECT_CLASS=postCommitApply \
  mise run ci:consistency
```

All three variables are required together; setting some alone is an error rather than a
silent fall back to the reference connector.

**The class is declared rather than discovered**, because how a connector divides
durability with the runtime is a property of its implementation that `spec` does not report.

It decides which scenarios run, but it excludes far less than you might expect: a fault a
connector must survive is rarely a property of its class, so nearly every scenario runs
against nearly every class. See `Scenario::applies_to`. Only two things are excluded — an
at-least-once subject skips the exactly-once scenarios, which it never claimed to uphold,
and `zombie-at-start-commit` runs for `remoteAuthoritative` alone, because the harness orders
the two racing instances by their `Open` fences and a class that does not fence gives it
nothing to order them by.

Note what is *not* excluded: `split-lands-on-prepared-transaction` runs for every
exactly-once class even though the counted channel cannot pass it. A gap that stops one class
is recorded as a `RuntimeGap` naming that class, so the scenario still runs for the others and
its passing there is the evidence that the gap is the runtime's rather than an impossible ask.

A skipped scenario prints `not-applicable` and still counts as a passing test, so read
those lines to see what was and was not verified. Declaring the wrong class does not
produce a false pass: the scenarios that run then measure guarantees the subject never
made, and fail.

**The subject must be a built binary**, not a container image — the shim `exec`s it. Cross
compiling is often blocked by cgo dependencies, so build it where the stack runs.

**The config is the connector's own endpoint configuration**, JSON or YAML. Every connector
in the connectors repository keeps one for its integration tests, usually
`materialize-$name/testdata/config.local.yaml`. Those are sops-encrypted, and decrypting
them is two steps, not one: `sops -d` recovers the values, and the `encrypted_suffix`
declared in the file's own sops block has to be stripped from every key — the same thing
Flow's `unseal` crate does (`crates/unseal/src/lib.rs`). A config still carrying
`personal_access_token_sops` will be rejected by the connector's strict parse.

**The resource configuration is discovered, not written.** The harness calls `spec` on the
subject and reads which property names the table (`x-collection-name`) and which flags
delta updates (`x-delta-updates`), so it works for a connector spelling them `table` and
`delta_updates` as well as one spelling them anything else.

**It runs once, not twice.** The clean/defective pairing exists to show the harness can tell
a good subject from a bad one, which needs a subject whose defects are switchable. A real
connector has no defective build to compare against, so the second pass is skipped.

**The subject must be able to read its destination back.** Verification reads what actually
landed rather than trusting the connector's account of it, via the `read` subcommand
`materialize-boilerplate` exposes; a SQL connector gets it by implementing `sql.RowReader`
in one line over `sql.StdReadRows`. A connector that does not implement it cannot be
verified by this harness.

That subcommand is **not yet merged** — it is estuary/connectors#4981 — and only
`materialize-databricks` and `materialize-sqlite` implement `sql.RowReader` today. So "runs
against any connector" is the design, and "runs against any connector implementing
`sql.RowReader`" is the present tense.

**A scenario that splits or joins shards needs the subject configured for multi-shard
operation.** Where that is behind a feature flag the harness cannot know its name, and a
connector run multi-shard without it will fail in ways that look like defects but are not:
`materialize-databricks` gates its coordinator behaviour on `advanced.feature_flags:
scale_out`, off by default, and without it two shards contend over one table. Set whatever
the connector requires in the config you pass.

**Timing scales with the subject.** A remote destination commits in tens of seconds where
the reference connector commits in milliseconds, so a named subject gets longer
transactions and proportionately longer gates (`Workload::remote`). Expect a few minutes
per scenario rather than tens of seconds.

**Monotonicity is exempted** for such a subject: the order rows come back from a table is
not guaranteed to be the order they were stored in, so there is no delivery order to check.
The set-based invariants carry the exactly-once claim.

## Reading a failure

A failing scenario is not necessarily a failing connector. These are the gates a run
passes through, and what each one's failure means.

**Recovery.** `harness::recover` is the gate after a perturbation: it unassigns the
task's shards — every one of them, not only those marked FAILED — until it commits again, and after a third of its budget escalates to
republishing the task disabled-then-enabled. Nothing in a run waits on shard *status* —
progress over the shim's trace is the measure instead, because a crashed shard is what most
scenarios inject and a shard reported primary may still be doing nothing.

**Completion.** The two collections need different measures, because they are keyed
differently. `log` is keyed `[/id, /seq]`, so every document is its own row and a row count
is exact. `merged` is keyed `[/id]` and reduced, so the runtime delivers one row per key per
*transaction* and its row count is always below the document count; completion there is
per-account — every account must reach its highest expected `seq`.

**A short drain is a shortfall, not a violation.** If the destination stops short, whether
the connector lost those documents or the runner stopped waiting cannot be told apart, so
`drain` fails naming the shortfall rather than handing an incomplete destination to the
checkers. `log 610/610, merged accounts behind 3 of 40` is a shortfall; a violation list is
a verdict.

**Faults arm after the warmup.** The warmup gate has no recovery step, so a crash landing
inside it would wedge the run. A unit test enforces this for every `Crash` rule; other
actions leave the shard running and are exempt.

**The environment.** Two symptoms present as connector faults and are not. `etcdserver:
mvcc: database space exceeded` in the reactor's log means etcd has hit its quota and can no
longer accept shard-status writes, so tasks publish and then never reach primary with
nothing in their own logs to explain it — compact, and defrag to reclaim the disk. And a
crash-looping systemd unit recompiles in `ExecStartPre`, so a restart loop is a compile loop
and can drive load high enough to expire etcd leases.

## The two rules

**Scenarios are keyed on protocol events, never on document identity.** The
runtime's close policy takes document- and byte-count ranges, but they are not
threaded through from the spec — only transaction durations are. So transaction
size is approximately `rate × min duration` and which documents land in which
transaction varies between runs. A fault says "the 4th `Acknowledge`", never "the
document for account 7".

**A scenario without a paired defect is not finished.** Each scenario names a
defect of the reference connector, and `tests/scenarios.rs` runs it twice: clean,
where it must pass, and defective, where it must fail. `scenarios.rs` has unit
tests asserting that every scenario is paired and every defect is reached, so
coverage cannot quietly erode as scenarios are added.

Two more rules that nothing enforces mechanically, so they are review obligations:

- **Assertions happen at quiescence, never mid-flight.** The document-counter
  class appends during `Store`, so a mid-flight read would report a violation
  where none exists.
- **A scenario never touches stack-wide state.** No reactor restarts, no etcd
  surgery. This is what makes one shared stack safe for several agents at once. A
  run stops *its own* captures to reach quiescence, and deletes only its own
  prefix.

## How a violation is detected

Every document of the workload carries an `oracle`: the producer's authoritative
truth for that account after that event. So the right answer is *computable*, and
there are no snapshots here despite the repository's convention — a snapshot would
add a stale artifact without adding information.

The sharpest check needs no external bookkeeping at all. The `merged` collection
reduces `balanceDelta` with `sum`, so a materialized row's delta *is* the
account's balance, while its `oracle.balance` independently states what that
balance should be. Deliver a document twice and the sum runs ahead of the oracle;
lose one and it falls behind.

That alone cannot see a *tail-truncated* materialization, which is internally
consistent — so the expectation is read from the collection itself with `flowctl
collections read`, which the connector under test had no hand in.

## Reproducibility

Nothing is seeded, and nothing needs to be. The capture and the materialization
are separate tasks joined only by the collection's journals: once a document is
written it is durable and immutable, and interrupting the materialization neither
touches the capture nor rewrites the collection. Every crash-and-replay scenario
replays byte-identical input by construction.

## Isolation and cleanup

Each run suffixes every catalog name and puts its destination in its own run
directory under `${FLOW_STACK_DIR}/consistency/`, so concurrent runs never
interact. A run deletes its tasks whether it passed or failed; the run directory
survives a failure, holding the shim's trace and the destination for inspection.

Leftovers from a crashed run are visible as `flowctl catalog list --prefix
test/consistency/` and as directories under `${FLOW_STACK_DIR}/consistency/`.

## Connector classes

The reference connector implements each independently of any real connector, so
the harness cannot bake in one vendor's assumptions, and so the document-counter
class is executable before any production connector adopts it.

| Class | Commits during | Authority | Fenced by |
| --- | --- | --- | --- |
| `remoteAuthoritative` | `StartCommit` | destination checkpoint | nonce table |
| `postCommitApply` | `Acknowledge`, from durable staging | recovery log | — |
| `documentCounter` | `Store`, appending to a counted channel | destination count | nonce table |
| `atLeastOnce` | `Store` | recovery log | — |

Two details of `postCommitApply` are load-bearing and follow `materialize-databricks`
rather than being invented here. Its checkpoint carries the *statements* which apply a
staged batch, keyed by binding — not a pointer to work the destination is asked to
rediscover, because leftover staging cannot say whether its transaction committed or was
abandoned. And only the **primary** shard runs them, learning of its peers' staged work
from the aggregated state patches the runtime delivers with `Acknowledge`, so that two
shards never contend for one binding's table. `Apply` deliberately drains nothing: it is
handed no connector state, so it has no basis for deciding what committed.

Deferring the load until `Flush` is the third, and it is what makes `split-during-commit`
pass rather than the expected failure it once was: `Flush` is emitted only once every
shard's `Acknowledged` has arrived, so a connector that stages load keys as they come in and
reads the destination only at `Flush` has waited for the applying shard by construction. See
`docs/materialize/consistency-testing.md`.

## Compliance model

Default-strict. Every connector is held to every invariant, and anything weaker is
an `Exemption` carrying a written justification. The rejected alternative — each
connector declares its class, and only that class's invariants run — fails because
the cheapest way to make a failing test pass becomes downgrading the claim.
`at-least-once-never-loses` is the model case: it declares exemptions for
duplication and everything downstream of it, and is still held to never losing
data.

## What is not here

- **Captures.** Their invariants are a different set and belong in a sibling
  suite, not forced into these abstractions.
- **CI gating.** Connectors CI has no flow checkout and no control plane. Runs on
  demand on a dev VM until per-connector runtime is known.
- **Real connectors.** The subject is an input — a binary, a config, and a class —
  so onboarding one is adding a `Subject`, not changing the harness. Their
  catalogs live in the connectors repository. Exemptions are declared in Rust beside the
  scenario that needs them, not loaded from a file.
