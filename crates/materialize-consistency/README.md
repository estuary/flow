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
| `harness/stack.rs` | Everything needed from the stack: `flowctl`, plus `gazctl` via `scripts/` for shard surgery. |
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
# In the connectors repository, once:
go build -o /tmp/testctl ./tests/materialize/testctl

FLOW_CONSISTENCY_SUBJECT=/path/to/materialize-yourthing \
FLOW_CONSISTENCY_SUBJECT_CONFIG=/path/to/config.json \
FLOW_CONSISTENCY_SUBJECT_CLASS=postCommitApply \
FLOW_CONSISTENCY_SUBJECT_TOOL=/tmp/testctl \
FLOW_CONSISTENCY_SUBJECT_NAME=materialize-databricks \
  mise run ci:consistency
```

All five are required together; setting some alone is an error rather than a silent fall back
to the reference connector.

**Two artifacts, not one.** The connector binary is what the shim `exec`s and the runtime
drives; `testctl` is separate, and is how verification reads the destination back and how a run
drops the tables it created. `FLOW_CONSISTENCY_SUBJECT_NAME` is the name `testctl` knows the
connector by, which is not derivable from the binary's file name. Why it is a separate program
rather than a connector subcommand is in the design document.

**Tables are named to be sweepable.** Each carries the run id *and*
`_flow_test_<unix>`, the connectors repository's convention, so `testctl -mode sweep` can
clear what a killed run left behind. Dropping by name only removes what the caller knows it
created; sweeping enumerates what is actually there.

**The class is declared rather than discovered**, because how a connector divides
durability with the runtime is a property of its implementation that `spec` does not report.

It decides which scenarios run. Most scenarios run for most classes — a fault a connector
must survive is rarely a property of its class — but three exclusions are worth knowing, and
`Scenario::applies_to` is the authority:

- an at-least-once subject skips every exactly-once scenario, which it never claimed to uphold;
- `zombie-at-start-commit` runs for `remoteAuthoritative` alone, because the shim orders the
  two racing instances by their `Open` fences and a class that does not fence gives it nothing
  to order them by;
- `split-during-commit`, `split-during-store` and `join-after-split` skip `documentCounter`,
  because each lands a membership change on a live transaction and whether that reaches the
  counted channel's exposure is a race — see `MEMBERSHIP_CHANGE_FAIRLY_ASKED`.

So a `documentCounter` subject skips **four** scenarios, not one. Read the run's
`not-applicable` lines rather than counting on this list to stay current.

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

**The subject must declare a delta-updates option.** A connector whose resource schema has no
`x-delta-updates` property is refused, rather than quietly given merge bindings in place of
its delta ones: a duplicate applied to a merge binding is an idempotent upsert and therefore
invisible, so accepting one would leave every scenario passing with the suite's sharpest check
disabled and nothing saying so.

**Which connectors can be a subject** is whatever `testctl` can drive, which is a connector
whose package is importable — `package connector` with `func main` under `cmd/connector`. See
`tests/materialize/testctl/README.md` in the connectors repository for the current list and for
how to add one; a connector still in `package main` needs converting first.

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

**Where to look.** A failing run keeps its directory under `${FLOW_STACK_DIR}/consistency/`,
holding the shim's protocol trace (`trace.jsonl`), the evidence it compared, and — for the
reference connector — the destination itself. Passing runs delete it. Debris from a killed run
shows up as `flowctl catalog list --prefix test/consistency/`, and `mise run ci:consistency`
purges the gazette state such runs leave behind before it starts.

Three messages are worth recognising on sight. `timed out waiting for N committed
transactions` means the task published but is not progressing — usually the environment.
`timed out waiting for K fault(s) to fire; 0 did` means the scenario never reached the point
it exists to perturb, so it proved nothing and is never a pass. `the destination stopped short
of the collections` is the interesting one: the task went quiet with rows missing.

For the failure *signatures* — which symptom means the runtime gap, which means a connector
reading its destination before `Flush` — see `docs/materialize/consistency-testing.md`.

## What is not here

- **Captures.** Their invariants are a different set and belong in a sibling suite.
- **CI gating.** Connectors CI has no flow checkout and no control plane, so this runs on
  demand on a dev VM until per-connector runtime is known.
- **Connector catalogs.** The subject is an input — a binary, a config, a class — so
  onboarding one is configuration, not a change to the harness.

## Where the reasoning lives

This file is a roadmap. `docs/materialize/consistency-testing.md` is the design record, and
the place to look before changing anything here:

- why verification runs against a real runtime rather than a model, and what the shim may and
  may not do
- the workload's shape, and why an exactly-once violation is only detectable when destination
  state depends on how many times a document was applied
- the four connector classes, the counted channel, and where the reference diverges from the
  connectors it models
- the four rules every scenario obeys, two of which nothing can enforce mechanically
- the compliance model: default-strict, with exemptions that must carry a justification
- the runtime gap the suite currently measures, and the failure signatures that identify it
