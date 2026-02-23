# shuffle

Distributed document shuffling for Flow tasks (derivations, materializations).
Routes source collection documents to the correct task shard based on shuffle
key hashes and r-clock ranges, with coordinated checkpoints across all shards.

See `plans/shuffle-v2-requirements.md` for the full design rationale.

## Architecture

Three gRPC RPCs form a hierarchy. For M members, the system uses M Slice
streams and M&sup2; Queue streams (each Slice opens one Queue to every member):

```
Coordinator
  └─ Session (one, on first member)
       ├─ Slice 0 ──┬── Queue 0  (in-process)
       │             ├── Queue 1  (remote)
       │             └── Queue 2  (remote)
       ├─ Slice 1 ──┬── Queue 0  (remote)
       │             ├── Queue 1  (in-process)
       │             └── Queue 2  (remote)
       └─ Slice 2 ──┬── Queue 0  (remote)
                     ├── Queue 1  (remote)
                     └── Queue 2  (in-process)
```

**Session** — Coordinates the shuffle. Receives journal discoveries from
Slices, assigns reads (StartRead), pulls progress deltas, aggregates them
into a checkpoint pipeline, and serves NextCheckpoint to the Coordinator.

**Slice** — Reads journals, sequences documents (filtering duplicates via
per-producer clock tracking), orders them by priority then adjusted clock,
routes each to the owning Queue by key hash / r-clock, and autonomously
flushes Queues after commits.

**Queue** — Receives documents from all Slices, merges them into a single
ordered stream via a priority heap, and writes to disk. Responds to Flush
once all preceding documents are durable.

## Module layout

```
src/
├── lib.rs              Key hash (HighwayHash), channel helpers, error mapping
├── service.rs          Service: gRPC server, spawn_{session,slice,queue}, peer dialing
├── binding.rs          Binding: per-binding config extracted from task specs
├── frontier.rs         Frontier / JournalFrontier / ProducerFrontier, Drain
│
├── session/
│   ├── handler.rs      serve_session: opens Slice RPCs, reads resume_checkpoint
│   ├── actor.rs        SessionActor: select! loop dispatching Session/Slice messages
│   └── state.rs        Topology (routing, StartRead), CheckpointPipeline (4-stage)
│
├── slice/
│   ├── handler.rs      serve_slice: opens Queue RPCs, builds Topology
│   ├── actor.rs        SliceActor: select! loop over listings, probes, reads, heap
│   ├── state.rs        Topology, FlushState, ProgressState, sequence_document
│   ├── listing.rs      Gazette list::Subscriber → ListingAdded events
│   ├── routing.rs      rotate_clock (UUID → r-clock), route_to_members
│   ├── producer.rs     ProducerState, ProducerMap, build_flush_frontier
│   ├── read.rs         ReadState, Meta, ReadyRead
│   └── heap.rs         ReadyReadHeap: max-heap over (priority DESC, adjusted_clock ASC)
│
└── queue/
    ├── handler.rs      serve_queue: QueueJoin synchronization across Slices
    ├── actor.rs        QueueActor: select! loop merging Enqueues from all Slices
    ├── state.rs        Topology
    └── heap.rs         EnqueueHeap: max-heap matching Slice ordering
```

## Key types

- `Service` — gRPC server entry point. Holds peer channels and QueueJoin state.
- `Binding` — Immutable per-binding shuffle config (collection, key extractors,
  priority, read delay, cohort, partition fields, schema validator).
- `Frontier` — Sorted list of `JournalFrontier` entries, each containing sorted
  `ProducerFrontier` entries. Supports `reduce` (sorted merge), `resolve_hints`
  (causal hint resolution), and `project_unresolved_hints` (recovery projection).
- `Drain` — Streams a `Frontier` as chunked `FrontierChunk` proto messages.
- `CheckpointPipeline` — Four-stage state machine (`progressed` &rarr;
  `unresolved` &rarr; `ready` &rarr; `take_ready`). Gates promotion on causal
  hint resolution and protects the recovery checkpoint from contamination.

## Protocol (wire types)

Defined in `go/protocols/shuffle/shuffle.proto`. Key messages:

- **Session**: `Open` / `Opened`, `NextCheckpoint` / checkpoint chunks,
  resume checkpoint streaming via `FrontierChunk`
- **Slice**: `Open` / `Opened`, `Start`, `StartRead`, `Progress` /
  `Progressed` (frontier delta), `ListingAdded`
- **Queue**: `Open` / `Opened`, `Enqueue` (routed document), `Flush` / `Flushed`
