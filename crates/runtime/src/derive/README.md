# derive

The derive module implements the runtime protocol for derivations - collections that are computed through transformations of other collections. It manages the execution of user-defined derivation logic while maintaining transactional consistency.

## Purpose

Derivations create derived collections by:
- Reading documents from source collections
- Applying user-defined transformations
- Publishing results to the derived collection
- Maintaining exactly-once processing guarantees

## Architecture

### Core Types

- **Task**: Derivation task configuration
  - `collection_name`: Target derived collection
  - `transforms`: Source collections and their transformations
  - `key_extractors`: Key extraction for output documents
  - `document_uuid_ptr`: UUID injection point
  - `write_schema_json`: Output collection schema

- **Transform**: Individual source transformation
  - `collection_name`: Source collection name
  - `name`: Transform identifier
  - `read_schema_json`: Expected source schema

- **Transaction**: Processing batch
  - `checkpoint`: Consumer progress tracking
  - `read_stats`: Per-transform statistics and clocks
  - `publish_stats`: Output document statistics
  - `max_clock`: Latest source document timestamp
  - `updated_inference`: Schema evolution flag

### Protocol Flow

1. **Open**: Initialize derivation session
   - Load previous state from RocksDB
   - Start connector with transform logic
   - Configure source collection readers

2. **Processing Loop**:
   - Read documents from source journals
   - Send to connector for transformation
   - Receive published results
   - Combine output documents by key

3. **Commit**: Atomically persist results
   - Write derived documents to journal
   - Update consumer checkpoint
   - Track source collection progress

### Differences from Capture/Materialize

**Unique to Derive**:
- **Multi-source reads**: Processes from multiple collections
- **Transform registry**: Named transformations per source
- **Clock tracking**: Monitors source document timestamps
- **Shuffle reads**: Optional key-based document routing
- **Lambda invocations**: Supports TypeScript/Deno transforms

**Similarities with Capture**:
- Publishes to collections
- Schema validation and inference
- Document combination by key

**Similarities with Materialize**:
- Reads from collection journals
- Consumer checkpoint management
- Source progress tracking

### Connector Types

Derivations support specialized runtimes:
- **TypeScript (Deno)**: JavaScript/TypeScript transformations
- **SQLite**: SQL-based transformations
- **Image containers**: User-provided transformation logic

The protocol (`derive.proto`) defines:
- Transform configuration
- Document reading by transform
- Publishing derived documents
- Transaction boundaries

## Key Implementation Details

- **Read coordination**: Ensures ordered processing across transforms
- **Combiner integration**: Reduces documents before publishing
- **Clock propagation**: Tracks source timestamps for ordering
- **Schema evolution**: Updates inferred shapes incrementally
- **Shuffle optimization**: Routes documents by key for efficiency

## Entry Points

- `serve_derive()`: Main gRPC service handler (serve.rs)
- `Task::new()`: Initialize from Open request (task.rs)
- `recv_connector_published()`: Process derived documents (protocol.rs)