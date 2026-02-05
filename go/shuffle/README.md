# shuffle

Distributed read coordination for Flow tasks. Orchestrates reading documents from collection journals and routing them to the correct task shards based on shuffle keys.

Used by derivations, materializations, and ad-hoc collection reads.

## Architecture

The shuffle system has two roles that task shards play simultaneously:

```
┌─────────────────────────────────────────────────────────────────┐
│                         Task Shard                              │
│                                                                 │
│  ┌─────────────────────┐         ┌─────────────────────────┐   │
│  │    CLIENT SIDE      │         │      SERVER SIDE        │   │
│  │                     │         │                         │   │
│  │  ReadBuilder        │ gRPC    │  Coordinator            │   │
│  │    └─> read ────────┼────────>│    └─> ring             │   │
│  │  governor           │         │          └─> subscriber │   │
│  │    (orders docs)    │         │    (reads journal,      │   │
│  │                     │         │     fans out to subscribers) │
│  └─────────────────────┘         └─────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

**Client side**: Consumes shuffled documents from coordinator shards, orders them, and delivers to the task.

**Server side**: Reads journals and distributes documents to subscribing shards based on key-range ownership.

Each journal has exactly one coordinator (selected via HRW hashing). Multiple subscribers reading the same journal share a single underlying journal read.

## Client Side

### Entry Points

- `NewReadBuilder()` - Creates a read builder for a task. Initializes journal watches and determines which coordinator handles each journal.

- `StartReadingMessages()` - Main entry point for normal task operation. Launches a goroutine that delivers ordered documents through a channel.

- `StartReplayRead()` - Creates a bounded read of a specific journal range for replay/recovery.

### Key Types

**`ReadBuilder`** (`read.go`) - Builds read instances for each journal partition. Watches for journal changes and triggers drain/restart when topology changes.

**`read`** (`read.go`) - A single gRPC stream to a coordinator, receiving documents for one journal. Implements backpressure with exponential backoff.

**`governor`** (`reader.go`) - Orders documents from multiple concurrent reads. Manages four read states:
- `pending`: reads without ready documents
- `queued`: reads with documents ready (priority heap)
- `gated`: reads waiting for wall-time to catch up (for read delays)
- `active`: journals currently being read

Documents are ordered by priority first, then by adjusted clock (`clock + readDelay`).

## Server Side

### Entry Points

- `NewCoordinator()` - Creates the server-side coordinator. Called by `FlowConsumer`.

- `Coordinator.Subscribe()` - Handles incoming shuffle RPCs. Routes to existing rings or creates new ones.

- `API.Shuffle()` - gRPC endpoint implementation. Resolves coordinator shard and delegates to `Subscribe()`.

### Key Types

**`Coordinator`** (`ring.go`) - Manages rings keyed by `(journal, replay, buildID)`. Routes shuffle requests to the appropriate ring.

**`ring`** (`ring.go`) - Coordinates reads of a single journal across multiple subscribers. Reads documents, extracts shuffle keys, and distributes to subscribers based on key-range ownership.

**`subscriber`** (`subscriber.go`) - Represents a connected consumer. Stores range responsibilities (`KeyBegin/End`, `RClockBegin/End`) and stages responses for delivery.

## Non-Obvious Details

### Backpressure and Deadlock Prevention

`read.sendReadResult()` implements exponential backoff when the response channel is full. If a read can't make progress for ~2 minutes, it's cancelled. This prevents distributed deadlock when shards form circular read dependencies.

### Ring Consolidation

Multiple subscribers reading the same journal share one underlying read. The ring extracts packed keys from documents and routes each document only to subscribers whose key-range matches.

### Shuffle Key Optimization

When shuffle key fields match partition label fields, the system can statically determine the key hash from journal labels alone, skipping journals that can't contain matching documents.

### Memory Arenas

Both `subscriber` staging and `ring` document reading use power-of-2 capacity allocation (4KB min, 1MB max) to reduce allocation churn.
