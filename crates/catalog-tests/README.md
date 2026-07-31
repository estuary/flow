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
