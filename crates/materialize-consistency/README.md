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
mise run ci:consistency --debug              # with the trace on stderr
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
and the fault lands mid-split instead. A `Scenario` may also carry
`known_limitation`, which makes it an *expected failure*: it runs and fails with its
violation count, which is the measurement of a runtime gap, and the marker is removed
when the gap closes.

## Reading a failure

A failing scenario is not necessarily a failing connector. These are the gates a run
passes through, and what each one's failure means.

**Recovery.** `harness::recover` is the gate after a perturbation: it unassigns FAILED
shards until the task commits again, and after a third of its budget escalates to
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
  catalogs and exemption files live in the connectors repository.
