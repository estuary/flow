# materialize

The materialize module implements the runtime protocol for maintaining materialized views of Flow collections in external systems. It manages incremental updates to endpoints like databases, ensuring consistency between Flow collections and external storage.

## Purpose

Materializations maintain external views by:
- Reading documents from source collections
- Computing incremental updates (inserts/updates/deletes)
- Applying changes to external endpoints
- Managing transactional consistency and restarts

## Architecture

### Core Types

- **Task**: Materialization task configuration
  - `bindings`: Source collections and their mappings
  - `shard_ref`: Task identification

- **Binding**: Collection-to-endpoint mapping
  - `collection_name`: Source collection
  - `delta_updates`: Enable incremental change processing
  - `key_extractors`: Document key extraction
  - `value_extractors`: Field extraction for endpoint
  - `store_document`: Whether to store full document
  - `state_key`: Binding-specific state tracking

- **Transaction**: Batch of updates
  - `checkpoint`: Consumer progress
  - `stats`: Per-binding (left/right/out) statistics
  - Source clock tracking for ordering

### Protocol Flow

1. **Open**: Initialize materialization session
   - Load previous state from RocksDB
   - Start connector with endpoint config
   - Receive connector-provided checkpoint

2. **Processing Loop**:
   - Read documents from collection journals
   - Load existing keys from endpoint (if needed)
   - Compute changes (combine left/right)
   - Send Store requests to connector

3. **Commit**: Atomically persist changes
   - Connector applies changes to endpoint
   - Store checkpoint and acknowledgements
   - Update runtime statistics

### Differences from Capture/Derive

**Unique to Materialize**:
- **Load-before-store**: Fetches existing values for updates
- **Delta updates**: Tracks document changes explicitly
- **Endpoint transactions**: Coordinates with external systems
- **Fence management**: Ensures idempotency across restarts
- **Key deduplication**: Optimizes loads within transactions

**Similarities with Derive**:
- Reads from collection journals
- Consumer checkpoint management
- Multiple source bindings

**Different from Capture**:
- Consumes rather than produces collections
- Bidirectional connector communication (load/store)
- External system transaction coordination

### Connector Integration

Materializations work with endpoint connectors:
- **Database connectors**: PostgreSQL, MySQL, BigQuery, etc.
- **Object stores**: S3, GCS with Parquet/JSON
- **Streaming systems**: Kafka, Kinesis
- **APIs**: Webhooks, custom endpoints

The protocol (`materialize.proto`) defines:
- Endpoint constraints and capabilities
- Load requests for existing documents
- Store requests for updates
- Transaction boundaries and acknowledgements

## Key Implementation Details

- **LoadKeySet**: Hash-based deduplication using xxHash3
- **Combiner integration**: Merges updates before storing
- **Fence tracking**: Prevents duplicate application on restart
- **Delta optimization**: Minimizes data transfer for updates
- **Journal suffixes**: Separate checkpoints per binding

## Entry Points

- `serve_materialize()`: Main gRPC service handler (serve.rs)
- `Task::new()`: Initialize from Open request (task.rs)
- `load_and_combine()`: Core update logic (protocol.rs)
- `recv_connector_loaded()`: Process loaded documents (protocol.rs)