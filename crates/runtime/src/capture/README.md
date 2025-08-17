# capture

The capture module implements the runtime protocol for ingesting data from external sources into Flow collections. It manages the lifecycle of capture connectors and ensures reliable, exactly-once data ingestion.

## Purpose

Captures bridge external data sources with Flow collections by:
- Running connectors that interface with source systems
- Validating and transforming ingested documents
- Managing checkpoints for resumable, exactly-once processing
- Inferring and evolving document schemas

## Architecture

### Core Types

- **Task**: Capture task configuration
  - `bindings`: Target collections and their configurations
  - `explicit_acknowledgements`: Whether connector requires explicit acks
  - `restart`: Scheduled restart interval for periodic captures
  - `shard_ref`: Task identification within the data plane

- **Binding**: Individual source-to-collection mapping
  - Target collection details (name, generation ID, schema)
  - Document processing (UUID injection, key/partition extraction)
  - State management key for resumption
  - Inferred shape tracking

- **Transaction**: Atomic unit of captured data
  - Accumulated documents and checkpoints
  - Statistics per binding (docs/bytes)
  - Schema inference updates
  - Optimistic combination up to 32MB threshold

### Protocol Flow

1. **Open**: Initialize capture session
   - Load previous state from RocksDB
   - Start connector with configuration
   - Receive connector's capabilities

2. **Capture Loop**:
   - Connector sends documents and checkpoints
   - Runtime validates against schemas
   - Combines documents by key
   - Accumulates into transactions

3. **Commit**: Atomically persist transaction
   - Write combined documents to journals
   - Store checkpoint in consumer shard
   - Update inferred schemas if changed

### Differences from Derive/Materialize

**Unique to Capture**:
- **Source-driven**: Connectors push data vs. reading from journals
- **Schema inference**: Automatically discovers and evolves schemas
- **Backfill handling**: Special state tracking for historical data
- **Restart scheduling**: Periodic re-execution for polling sources
- **Explicit acknowledgements**: Optional connector flow control

**Similarities**:
- Transaction-based processing model
- RocksDB for persistent state
- Connector lifecycle management
- Statistics and monitoring

### Connector Integration

Captures support two connector types:
- **Image**: Docker containers via gRPC
- **Local**: Direct process execution (development)

The connector protocol (`capture.proto`) defines:
- Discovery of available bindings
- Validation of configurations
- Streaming capture with checkpoints
- Apply for connector-side effects

## Key Implementation Details

- **Long-polling**: 5-second timeout balances latency and efficiency
- **Combiner threshold**: 32MB limit for optimistic combination
- **Complexity limits**: Adaptive schema inference (10K for sourced, default otherwise)
- **UUID injection**: Placeholder replaced with actual UUIDs at write time
- **State keys**: Unique per resource for multiple bindings to same collection

## Entry Points

- `serve_capture()`: Main gRPC service handler (serve.rs:32)
- `Task::new()`: Initialize from Open request (task.rs:8)
- `recv_connector_captured()`: Process captured documents (protocol.rs)