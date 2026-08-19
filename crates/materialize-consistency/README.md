# materialize-consistency

A test suite that answers one question: **when a materialization connector is
crashed, stalled, raced, or re-sharded at the worst possible moment, does the
destination still hold exactly the right data?**

It tests against a real Flow runtime, not a mock: each scenario publishes a real
capture and materialization to your local stack, injects a fault at a precise
point in the protocol, and then verifies what the destination actually holds.

```bash
mise run local:stack                         # once: a running local stack
mise run ci:consistency                      # the whole suite
mise run ci:consistency --filter zombie      # one scenario
mise run ci:consistency --debug              # connector logs, uncaptured
```

By default it tests a built-in reference connector that writes to SQLite, so it
needs no credentials and no cloud. Any real connector can be tested instead —
see [Testing a real connector](#testing-a-real-connector).

## How it works

```
                  publishes                    ┌──────────────┐
   scenario  ──────────────────►  local stack  │ two captures │
   runner                                      │ two          │
      │                                        │ collections  │
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

- A **shim** sits between the runtime and the connector, posing as the
  connector. It sees every protocol message and can crash the connector, stall
  it, or race it against a frozen stale copy (a "zombie") at an exact protocol
  event — "the 4th `Acknowledge`". Neither Flow nor the connector is modified.
- The workload carries an **oracle** in every document, so the correct final
  state is computed, not asserted from a snapshot. **Invariant checkers** then
  verify the destination: nothing lost, nothing duplicated, every value right.
- Every scenario runs twice against the reference connector: **clean**, where it
  must pass, and against a **paired defect** (a deliberately broken mode of the
  reference connector), where it must fail. A checker that stops catching its
  defect is itself a test failure.

## The scenarios

Each scenario is one fault and one question. Faults are keyed on exact protocol
events: a crash on a *request* trigger (`Store`, `Flush`, `StartCommit`,
`Acknowledge`) fires before the connector receives that message; a crash on a
*response* trigger (`StartedCommit`, `Acknowledged`) fires as the connector
emits it, before the runtime sees it.

| Scenario | Perturbation | What it checks |
| --- | --- | --- |
| `baseline` | none | Catches a miswired harness before it can bless anything else. |
| `crash-between-commits` | crash on `Acknowledged` #5 — just after the connector finished applying a committed transaction | The replayed `Acknowledge` must not apply that transaction twice. |
| `crash-mid-store` | crash before `Store` request #25 (armed after 3 commits) — mid-store, before any `StartCommit` | Half-done work must never be applied on replay. |
| `crash-at-flush` | crash before `Flush` request #4 — loads done, stores not yet begun | No documents lost, no merged values corrupted. |
| `split-during-store` | split the task's shards mid-run, unsynchronized with transaction boundaries; no crash | Every document still lands exactly once across the membership change. |
| `split-during-commit` | crash before `StartCommit` request #4 — so that transaction never commits — then split while the task is down | Staged work that never committed is never applied, and the replay lands exactly once on the new shards. |
| `split-after-commit-before-apply` | crash before `Acknowledge` request #4 — leaving the previous transaction committed but not yet applied — then split while the task is down | The committed staged work is applied exactly once by the new shards. |
| `split-lands-on-prepared-transaction` | crash on `StartedCommit` #4 — the destination has committed what the runtime has not recorded — then split | *Expected failure* for `documentCounter`: measures a known runtime gap ([discussion 2581](https://github.com/estuary/flow/discussions/2581)). |
| `join-after-split` | split the shards, let them settle, then join them back together; no crash | The surviving shard picks up the departed shard's work exactly once. |
| `zombie-at-start-commit` | freeze a second instance at its `Open` (fence taken, nothing more); thaw it after the live one commits twice, so it replays through to `StartCommit` | The stale commit must be refused by the fence. |
| `destination-ahead-of-checkpoint` | crash on `StartedCommit` #4 — rows appended, recovery log not yet committed | Recovery skips exactly what the destination already holds, no more. |
| `recovery-reconciles-with-destination` | crash on `StartedCommit` #5 — same destination-ahead state | Recovery must check what the destination actually holds, not trust its own checkpoint. |
| `crash-in-split-leader` | split, then crash the child holding the recovery log on its `StartedCommit` #2 | The rebuilt shard inherits neither its parent's resume point nor a blank one — each document lands exactly once. |
| `crash-in-split-non-leader` | split, then crash a stateless non-leader child on its `StartedCommit` #2, taking the whole task down | The task comes back, and exactly-once still holds. |
| `at-least-once-never-loses` | crash on `StartedCommit` #4 — before the recovery log commits, forcing a replay | The replay may duplicate, but must never lose data. |

`src/scenarios.rs` is the authority, with the full reasoning on each.

## Connector classes

Connectors achieve exactly-once in different ways, and the subject declares
which way it uses. Most scenarios run for every class; a few only make sense
for some.

| Class | How it stays consistent | Modeled on |
| --- | --- | --- |
| `remoteAuthoritative` | Keeps its checkpoint in the destination, fenced against stale writers | `materialize-postgres` |
| `postCommitApply` | Stages work, applies it only after the runtime commits | `materialize-databricks` |
| `documentCounter` | Counts rows accepted by a streaming channel, skips that many on recovery | Snowflake Snowpipe Streaming v2 |
| `atLeastOnce` | No exactly-once claim; replays may duplicate | — |

## Testing a real connector

Point the suite at a connector binary, its endpoint config, and its class:

```bash
FLOW_CONSISTENCY_SUBJECT=/path/to/materialize-yourthing \
FLOW_CONSISTENCY_SUBJECT_CONFIG=/path/to/config.json \
FLOW_CONSISTENCY_SUBJECT_CLASS=postCommitApply \
FLOW_CONSISTENCY_SUBJECT_TOOL=/path/to/testctl \
FLOW_CONSISTENCY_SUBJECT_NAME=materialize-yourthing \
  mise run ci:consistency
```

The essentials:

- **The subject is a built binary**, not an image, plus `testctl` (from the
  connectors repository's `tests/materialize/testctl`), which reads the
  destination back and drops the tables a run creates.
- **The config is the connector's own endpoint config**, decrypted from its
  checked-in `config.local.yaml` — with `_sops` key suffixes stripped.
- Expect **a few minutes per scenario** against a remote destination, and each
  run cleans up the tables it created.

This documentation is deliberately agent-ready: to run the suite against your
connector, point an agent at [`AGENT_README.md`](AGENT_README.md) — the complete
operating guide, with every requirement and gotcha spelled out (config
decryption, feature flags, which scenarios apply to which class, and how to read
each kind of failure).

## When a scenario fails

A failing scenario is not automatically a failing connector. The run tells you
which it is:

- A **violation list** is a verdict about the connector; the evidence
  (`evidence.json`, the protocol trace) is kept under
  `${FLOW_STACK_DIR}/consistency/`.
- A **shortfall** ("the destination stopped short") or a **fault that never
  fired** means the run proved nothing — usually stack environment or subject
  configuration.
- `split-lands-on-prepared-transaction` is *expected* to fail for the
  `documentCounter` class: it measures a known runtime gap and turns red the
  other way (an "unexpected pass") if the gap ever silently closes.

The "Reading a failure" section of [`AGENT_README.md`](AGENT_README.md) has the
full triage guide.

## More

- [`AGENT_README.md`](AGENT_README.md) — the complete operating guide, written
  for agents (and thorough humans) driving the suite.
- `docs/materialize/consistency-testing.md` — the design record: why it is
  built this way, the rejected alternatives, and the runtime gap it measures.
