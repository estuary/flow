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
  event — "the 4th `Acknowledge`", never "the document for account 7". Neither
  Flow nor the connector is modified.
- The workload carries an **oracle** in every document, so the correct final
  state is computed, not asserted from a snapshot. **Invariant checkers** then
  verify the destination: nothing lost, nothing duplicated, every value right.
- Every scenario runs twice against the reference connector: **clean**, where it
  must pass, and against a **paired defect** (a deliberately broken mode of the
  reference connector), where it must fail. A checker that stops catching its
  defect is itself a test failure.

## The scenarios

Each scenario is one fault and one question. In plain terms:

| Scenario | What it checks |
| --- | --- |
| `baseline` | No fault at all. Catches a miswired harness before it can bless anything else. |
| `crash-between-commits` | Crash just after a transaction is applied. The replay must not apply it twice. |
| `crash-mid-store` | Crash while documents are being written, before commit. Half-done work must never be applied. |
| `crash-at-flush` | Crash between the load and store phases. No documents lost, no merged values corrupted. |
| `split-during-store` | Split the task's shards mid-transaction. Every document still lands exactly once. |
| `split-during-commit` | Crash before the commit, then split. Staged work that never committed is never applied. |
| `split-after-commit-before-apply` | Crash after the commit but before the work is applied, then split. The new shards apply it exactly once. |
| `split-lands-on-prepared-transaction` | A split lands on a transaction already written but not committed. *Expected failure* for one class — it measures a known runtime gap ([discussion 2581](https://github.com/estuary/flow/discussions/2581)). |
| `join-after-split` | Scale back down. The surviving shard picks up the departed shard's work exactly once. |
| `zombie-at-start-commit` | A stale instance thaws and tries to commit superseded work. Fencing must refuse it. |
| `destination-ahead-of-checkpoint` | Crash leaves the destination holding rows the checkpoint doesn't know about. Recovery skips exactly those, no more. |
| `recovery-reconciles-with-destination` | Recovery must check what the destination actually holds, not trust its own checkpoint. |
| `crash-in-split-leader` | Crash the split child that holds the recovery log. Exactly-once still holds. |
| `crash-in-split-non-leader` | Crash a stateless split child, taking the whole task down. It comes back, and exactly-once still holds. |
| `at-least-once-never-loses` | A connector claiming only at-least-once may duplicate — but must never lose data. |

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
- **Multi-shard operation must be enabled** if the connector gates it behind a
  feature flag (`scale_out` for databricks), or the split scenarios fail for
  configuration reasons, not connector reasons.
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
