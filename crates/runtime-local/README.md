# runtime-local

Runs `runtime-next` tasks locally with synthetic shards, and with **no Gazette
broker, etcd, or Go consumer**.

In production, a Go consumer assigns shards and drives each one's session
protocol, Gazette brokers serve the journals that collections are read from and
written to, and etcd holds the shard assignments. This crate stands in for all
three: it hosts `runtime_next::Service` on a loopback port, synthesizes the
`SessionLoop` / `Join` / `Task` envelopes the Go controller would send, and
drives N shards as tokio tasks. Source documents can come from real journals or
be written directly as shuffle-log segments; output documents, stats, and logs go
wherever the caller's seams send them.

## Fit within the project

```
flowctl raw preview-next ──┐
                           ├──> runtime-local ──> runtime-next
catalog-tests ─────────────┘
```

Both consumers need the same thing — "run this task locally, N shards, feeding it
these documents" — and differ only in the three `runtime-next` host seams. So the
crate is generic over all three and knows nothing about either caller.

The layering matters and is enforced by the crate boundary: `catalog-tests`
depends on `runtime-local`, never the reverse. Nothing here may reach for a
catalog-test type. (This crate is *not* `runtime::harness`, which is the V1
runtime's equivalent and dies with V1 preview.)

## Key types and entry points

| Item | Purpose |
|---|---|
| `services::Run` | Per-invocation resources: the loopback tonic server and the shuffle-log directory. `start_capture` for leaderless captures; `start_with_shuffle_leader` otherwise. |
| `Controls<P, L>` | The publisher and logger factories installed on each shard, plus shard zero's optional connector-state seed and final-state request, both carried on its `SessionLoop`. `run_sessions` returns the reported final state. |
| `materialize_driver` / `derive_driver` / `capture_driver` | `run_sessions` drives N shards of one materialization / derivation / capture through a sequence of sessions. |
| `segments` | Writes documents directly as `shuffle::log` segments and builds the checkpoint `Frontier` that makes them visible — a whole transaction at a time, or document-by-document through a `TxnState`; plus the channel-fed `ShuffleSessionFactory` that relays those frontiers. |
| `shards` | The synthetic shard topology — an even split of the `u32` key space, shared by the leader's join shards and the shuffle topology. |

## Non-obvious details

**The shuffle source is the caller's choice.** `start_with_shuffle_leader` takes a
`build_shuffle` callback rather than a boolean, receiving the freshly-bound
`peer_endpoint` and returning a `ShuffleSessionFactory` plus an optional
`shuffle::Service` to co-host. This is what keeps journal *authorization* out of
this crate: reading live journals needs a logged-in user token, which only
`flowctl` has. A segment-fed run returns `None` for the service and never dials
shuffle at all.

**Writing segments bypasses journals, not the consumer.** `segments` writes real
`shuffle::log` blocks and mirrors the live slice actor's behavior exactly —
synthetic document UUIDs, the schema-valid flag from validation, the packed
shuffle key, and key-hash routing to the owning shard. The consumer therefore
reads these documents on precisely the same path it reads live ones. A
consequence: a shard's reader reconstructs segment filenames from *its own*
index, so a multi-shard writer must author each stream under the matching shard
index (`ShardWriter::new`). `ShardWriter::with_segment_threshold` rolls segments
sooner, for a caller which bounds its standing on-disk backlog: sealed segments
are the granularity that backlog is accounted and reclaimed at.

**`TxnState` is the real writer; `write_transaction*` are wrappers.** Documents
are routed and buffered into per-shard blocks as they are pushed (`push_binding`
/ `push_doc`) and each block is appended the moment it meets a size threshold, so
at most one block per shard is resident however large a transaction is.
`finish_txn` appends the remainders and returns the checkpoint `Frontier`. A
caller which does not yet hold a whole transaction — a streaming fixture reading
a FIFO line by line — pushes as it parses, and so bounds its memory by block size
rather than transaction size.

**Clocks are the caller's policy.** `push_binding` takes each document's clock
rather than generating one. Document clocks must increase globally across a run,
or a recovered frontier will re-admit earlier documents — but *how* they advance
differs by caller (preview reproduces the legacy fixture harness's
`3600 * ordinal + index` stamping, which `TxnState::for_txn` + `push_doc`
implement; a test runner paces them against its own synthetic time). Segment LSNs
are the opposite: they restart per session, because the runtime's per-session
`Reader` is ephemeral and unlinks segments as it reads them.

**Per-binding feeding is deliberate.** `push_binding` tags each document with the
specific binding it feeds, not just its collection. Feeding one binding at a time
is what lets a caller honor per-transform read progress — and therefore read
delays — independently. `push_doc` is the collection-tagged wrapper that fans a
document out to every binding of its collection at once.

**Dropping any of this mid-flight is safe.** A dropped driver future EOFs its
request streams, and each shard's serve task unwinds detached — removing its
own RocksDB tempdir and releasing its connector, which self-reaps regardless
via `flow-connector-init`'s dead-man's switch. What a drop forgoes is only the
observation of a completed shutdown.
