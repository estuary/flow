# catalog-tests

Runs a catalog's `tests:` on the `runtime-next` runtime, with **no Gazette
broker, no etcd, and no Go consumer**. Collections are an in-memory append-log
(`store::CollectionStore`); derivations are real runtime-next sessions driven
through `runtime-local`.

## Fit within the project

```
flowctl raw test ────────┐
                         ├──> catalog-tests ──> runtime-local ──> runtime-next
control-plane-api ───────┘        (this crate)
  (publication tests)
```

The crate is the V2 replacement for the Go V1 test machinery (`Graph` and
`TestTime` are ports of Go's `PendingStat` / `TestTime`). Its whole external
contract is re-exported from `lib.rs`: `run_tests`, `Options`, `TestResults`,
`TestOutcome`, `TestStatus`, `LogHandler`. Everything else is `pub` only so
`tests/` can white-box it.

Both callers are library callers, so **nothing here writes stdout**: connector
stderr and runtime events go to `Options::log_handler` (a terminal for
`flowctl`, a publication's job logs for the agent), and per-case results come
back as `TestOutcome`s carrying a `scope` — a source URL with a JSON-pointer
fragment locating the exact failing *step*.

**Only derivations run.** A catalog test never executes a capture or a
materialization (`build::for_catalog_test` no-ops both), so ingest steps stand
in for captures and verify steps read collections directly.

## Layout

| Module | Purpose |
|---|---|
| `run` | `run_tests`: start one resident session per enabled derivation, run each case, Reset between them. Owns `LiveDriver`, `Options`, outcome bookkeeping. |
| `scheduler` | The `Driver` trait and `run_test_case`: the step/read/advance loop. Pure scheduling, unit-tested against a recording mock. |
| `graph` | `Graph` + `TestTime`: dataflow topology, pending reads, synthetic time. |
| `clock` | `Clock` (`BTreeMap<journal, offset>`) and its min/max/contains reductions. |
| `session` | `DerivationSession`: one derivation resident for the whole run. |
| `publish` | `TestPublisher`: the `runtime_next::Publisher` seam. Appends to the store, and signals transaction commits. |
| `store` | `CollectionStore`: the journals stand-in, stamping each append with a transaction ordinal. |
| `partitions` | Logical-partition routing: document → journal name + the journal's complete label set. |
| `steps` | The INGEST and VERIFY step bodies (combine + compare). |
| `diff` | The verify comparator and its failure rendering. |

`tests/derive_sqlite.rs` drives `DerivationSession` by hand (no `run_tests`);
`tests/catalog_tests.rs` drives the whole path from inline YAML. Both run
multi-shard over derive-sqlite, which is in-process — no Docker. The one
exception is `tests/fixtures/dying_connector.py`, a `local:` connector (a
subprocess, still no Docker) used to inject startup failures. Multi-shard *image*
derivations are only covered by the examples suite in CI.

## Non-obvious details

**Nothing ever sleeps.** `TestTime` is synthetic nanoseconds with no relation
to wall clock. A transform's `readDelay` becomes a `PendingRead::ready_at`, and
the graph simply *withholds* that read until `run_steps` has nothing else to do
and advances time to it. `LiveDriver::advance` is a deliberate no-op: the
session has no clock, and by the time `read()` is called the delay has already
been honored by ordering. A one-hour delay costs microseconds
(`read_delay_gates_on_synthetic_time`); if a real sleep ever crept in, that test
hangs rather than fails. `Graph::completed_advance` panics if time is advanced
past a pending read.

**Two clock flavors that must never mix.** Write clocks are keyed by plain
journal names; read-through clocks are keyed by `{journal};{read_suffix}`, one
key space per transform, so the same journal read by two transforms advances
independently. The key spaces are disjoint, so a cross-flavor reduction is
silently meaningless rather than wrong-looking. `Graph::project_write` is the
one place a write clock is converted into read-through clocks.

**Self-cycles terminate by `contains_clock`, not by a visit set.**
`project_write` skips any reader whose recorded `read_through` already contains
the projected clock, so a derivation reading itself stops enqueuing once it has
read through its own writes. `derive_sqlite::self_cycle_terminates` is the
regression test; the fixed-point is the only thing keeping the cascade finite.

**A verify is gated by topology, then windowed by clocks.**
`Graph::has_pending_write(collection)` walks *forward* from every pending read
to see whether any derivation reachable from it is named `collection` — it takes
a collection *name* (not a template) precisely because a derivation's name is
its output collection's name. Only once nothing pending can still write does the
step run, comparing the store window `(from, to]` where `from` is the global
write clock captured at the *start of the case*. The store is never cleared, so
this window is the only thing isolating one case's expectations from another's
data.

**An "offset" is a document count, not a byte offset.** `CollectionStore`
journals are `Vec<Vec<u8>>`, so a clock offset indexes documents directly and
the `(from, to]` window is a half-open slice. In `read_collection_window`, a
journal absent from `to` means "read through the head" (the `-1` sentinel), and
absent from `from` means zero.

**Journals are selected by `partition_template.name`, never by collection
name.** Collection names nest — `acmeCo/nest` and `acmeCo/nest/inner` are both
legal — so the parent's name is a prefix of the child's journals. The template
name appends the collection's generation ID, which no sibling or descendant can
reproduce. Selecting on the bare name broke two ways at once: verify of the
parent read the child's documents, and the parent's write clock carried the
child's journals, feeding a transform its own downstream output. Covered by
`graph::tests::nested_collection_names_project_separately` and
`catalog_tests::nested_collection_names_stay_separate`.

**A transaction commit is observed through the `Publisher` seam.** There is no
journal IO, so `TestPublisher::write_intents` is repurposed as the commit
signal — the seam's contract *is* the transaction lifecycle, and the leader's
Tail FSM reaches `WriteIntents` once per transaction after every shard has
drained. Only the *leader's stats-only* publisher signals, discriminated by the
empty `collection_specs` that `PublisherFactory::open` documents for it. One
extra signal fires at session startup when the Tail replays recovered ACK
intents; `DerivationSession::start` consumes it, or the first `read()` would
take it for its own and run a transaction ahead of itself.

**Feeding is per binding, not per collection.** A read's `read_through` journals
carry the transform's checkpoint suffix, which `suffix_to_binding` maps back to
one binding. That is what makes two transforms of the same source with different
read delays advance independently.

**Reset clears connector memory only.** `DerivationSession::reset` sends one
`Reset` per shard (Reset is shard-local; the leader is not in its path) and
awaits each shard's `ResetDone` — safe because the session is quiescent between
reads. It clears derive-sqlite registers / TypeScript module state, and leaves
read frontiers, feed cursors, and collection data untouched. It runs after
*every* case including the last, so no case can observe another's connector
state. See `derive_sqlite::reset_clears_connector_state_but_not_data`.

**Failure bookkeeping is deliberately asymmetric.** A failing case is recorded,
not raised. If the inter-case Reset then fails *after* a failed case, the
session is dead and cannot be revived, so the run stops and the remaining cases
become `TestStatus::NotRun` naming the case that ended the run — rather than
discarding the outcome that actually explains it. A Reset failure after a
*passing* case has no user-attributable cause to fold into and is returned as a
run-level error. Same rule at `shutdown_all`. `run_test_case` also quiesces the
graph on failure, so a case that dies partway cannot leak a delayed read into
the next case's verify window.

**derive-sqlite runs stateless here, which is what makes it multi-shard.**
Sessions send an empty `Task.sqlite_vfs_uri`, so each shard's connector runs a
session-scoped `:memory:` database and reports no checkpoint at Opened. With no
checkpoint the runtime doesn't mark the task remote-authoritative — the
property that would otherwise defer to endpoint state and make the leader
reject any multi-shard topology. See `Database` in `derive-sqlite`'s
`connector.rs` and its `opened_checkpoint_tracks_database_durability` test.

**Redaction runs with an empty salt, everywhere.** `steps` passes an empty salt
to the combiner and `session::start` clears the derivation spec's
`redact_salt`, so an expected digest is `sha256:` plus the plain SHA-256 of the
raw value (`printf hello | sha256sum`). The platform salt is derived from the
task's shard-ID prefix and rotates whenever a task is deleted and re-published,
which would make the expectation unwritable by hand.

**Ingest and verify combine differently, on purpose.** Ingest combines under the
collection's *write* schema with associative reductions, matching the
cardinality a real capture would publish; verify combines under the *read*
schema with full reductions, to one document per key. Comparison is lock-step by
index — legal because `validation` rejects a verify step whose documents are not
in key order (`Error::TestVerifyOrder`) — and each pair goes through `doc::diff`,
which ignores actual properties the expectation doesn't mention (e.g. `_meta`)
and reports *located* differences rather than two whole documents.

**A transform's `notBefore` / `notAfter` are not modeled.** Document clocks
are synthetic, starting at the Unix epoch, so `session::start` clears both
bounds on the shuffle bindings; otherwise the sequencer would compare a real
timestamp against 1970 and filter everything (or nothing). Anchoring clocks at
wall time instead is not an option: the shuffle also gates `adjusted_clock =
clock + readDelay` against `now`, and real clocks would make a `readDelay` a
real sleep.

**Order is guaranteed across transactions, and deliberately not within one.**
Every store append is stamped with a transaction ordinal. `Driver::begin_transaction`
bumps it once per ingest step and once per batch of concurrently-ready reads —
derivations reading at the same synthetic instant are concurrent transactions
in production, so their outputs race and share an ordinal, while a cascade at
the same instant is a later batch and a later ordinal. Both readers of the
store obey it: `DerivationSession::read` sorts the documents it feeds by ordinal
(stably) before assigning clocks, and `CollectionStore::read_collection_window`
returns a verify's documents the same way, so its reduction sees what a
downstream derivation would. Intra-ordinal order is whatever the store's append
order happened to be; nothing may depend on it.

**Output append order across shards is a race, by design.** `publish_doc`
appends as documents are published; the *shuffle* key pins a document to a
shard, and a derivation's output key need not be a function of it. Documents of
one shuffle key reduce in source order, and nothing more is guaranteed — exactly
as competing journal appends are in production.

**The caller installs the rustls `CryptoProvider`.** The runtime-next loopback
stack dials over rustls; `flowctl` and the agent install one at startup, and the
integration tests install it themselves (idempotently) for the same reason.

**Log volume is the task's own choice.** `run::logger_factory` reads the task's
`shards: {logLevel}` from its shard template's `estuary.dev/log-level` label and
gates the sink at that level; nothing raises it later. Bumping a task to `debug`
is how a user gets connector output into `flowctl raw test` or a publication's
job logs.

**`Driver` exists to make scheduling testable.** `scheduler::run_test_case` is
generic over the four IO actions, so `scheduler::tests` snapshots the whole
schedule — read cascade, lazy advances, verify gating — against a mock that only
records calls, with no runtime at all.
