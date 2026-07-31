# catalog-tests

Runs a catalog's `tests:` on the V2 `runtime-next` runtime — **no Gazette broker,
no etcd, no Go consumer, no `flowctl-go`**.

Two callers link it: `flowctl test` (developer-facing) and `control-plane-api`
(publication tests). Neither writes to stdout from here — logs and failures go to
a caller-supplied sink, because one of them is a server streaming into job logs.

## Fit within the project

```
flowctl test ─────────┐
                      ├──> catalog-tests ──> runtime-local ──> runtime-next
control-plane-api ────┘
```

`runtime-local` owns "run a runtime-next task locally with synthetic shards";
this crate owns "what a catalog test *means*". The dependency runs one way only,
and the crate boundary is what enforces it — see `crates/runtime-local/README.md`.

## Layers

| Module | Purpose |
|---|---|
| `clock` | Per-journal offset clocks; `min`/`max`/`contains` reductions. |
| `graph` | The dataflow graph: which reads are pending, which are unblocked at the current synthetic time, and whether a collection may still be written. |
| `action` | The `Driver` trait and the `run_test_case` scheduling loop. |
| `store` | `CollectionStore`, the in-memory append-log stand-in for collection journals. |
| `diff` | The Verify comparator: superset match, scaled-epsilon float compare, UUID masking. |
| `publish` / `logger` / `partitions` | The `runtime-next` host seams, plus logical-partition routing. |
| `runner` | `DerivationRunner`: one derivation resident for a whole run. |
| `steps` | The INGEST and VERIFY steps. |
| `run` | `run_tests`, the entry point. |

`run_tests` is the whole surface most callers need: hand it a `build::Output`'s
validations and an `Options`, get back a per-case outcome list.

`clock`, `graph`, and `action` are a faithful port of V1's `go/testing/`. That
code was small, proven, and load-bearing for semantics no one wants to
re-derive — cascading stats, self-cycles reaching fixed point, read-delay
scheduling — so the port is deliberately literal, down to the unit tests, which
carry the name of the Go test each came from.

## Non-obvious details

**The graph tracks derivations only.** V1 built it as
`NewGraph(nil, collections, nil)`: captures and materializations are excluded
because a catalog test never runs them. Ingest steps stand in for captures, and
verify steps read collections directly.

**Synthetic time only; never wall-clock.** Read delays are honored by *advancing
a counter*, not sleeping. Time advances lazily and by exactly the amount needed to
unblock the next pending read (`pop_ready_stats` returns that delta), so a
transform with a one-hour read delay costs nothing. `completed_advance` panics if
time would pass a pending stat, which would silently skip a read.

**Self-cycles terminate via `contains_clock`.** A derivation reading its own
collection would otherwise enqueue a read for every write forever. `project_write`
skips a reader whose read-through clock already contains the projected write, which
is what makes a self-cycle reach a fixed point.

**Verify gates on a BFS, not on the immediate parent.** `has_pending_write` walks
the graph forward from every pending task, so a verify of a collection at the end
of a multi-hop chain waits for the whole chain to quiesce.

**Offsets are document counts, not byte offsets.** `CollectionStore` is a
document log, so a clock value is "how many documents have been appended". Verify's
`(from, to]` window therefore maps exactly onto a half-open document-index range.

**The float comparison inherits a V1 edge case.** `compare_numbers` scales
`FLT_EPSILON` by magnitude, so values very near zero compare by their string
representation alone (`0` and `0.0` do *not* match). This is V1's behavior and is
kept deliberately, for parity.

**One derivation, one resident session, one transaction per stat.** A
`DerivationRunner` keeps its session open for the entire run, so a connector
container starts at most once per derivation, and each `stat` drives exactly one
transaction and awaits its commit. That last property is load-bearing beyond
performance: it is *why* `Reset` can be shard-local. The runner is quiescent
whenever it sends one, so no leader is needed to find a transaction boundary
common to all shards. See `crates/runtime-next/README.md`.

**The commit signal comes from the publisher, not the log stream.** `write_intents`
is where a transaction's commit is observed, because the `Publisher` seam's
contract *is* the transaction lifecycle. `LogEvent` would be the easier hook and
the wrong one — it is an observability channel, and both it and its variants are
`#[non_exhaustive]`. One consequence to know: the leader's Tail FSM also passes
through `WriteIntents` at session *startup*, replaying recovered ACK intents, so
`DerivationRunner::start` consumes exactly one signal before any stat runs.

## Known coverage gap: single-shard remote-authoritative derivations

`run_tests` gives **image derivations 3 shards** and **SQLite derivations 1**.
V1's publication tests used 3 splits for everything, "to try to catch shuffle
errors", so for SQLite derivations this is a real regression.

The cause: derive-sqlite is remote-authoritative — it reports connector state at
`Opened`, which the leader rejects on any non-zero shard. Fixing runtime-next to
support multi-shard remote-authoritative tasks is out of scope here.

What keeps this acceptable: `examples/` contains derive-typescript derivations, so
3-split routing is still exercised end to end in CI, and the segment writer's
key-routing has direct coverage in `tests/derive_sqlite.rs`
(`multi_shard_segment_routing`).
