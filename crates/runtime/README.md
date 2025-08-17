# runtime

The Flow Runtime crate implements the server-side runtime protocols for executing capture, derivation, and materialization tasks within data planes. It provides the core infrastructure for running connectors and managing data flow between components.

## Purpose

The runtime serves as the bridge between:
- User specifications (from the control plane)
- Data plane execution (Gazette brokers and reactors)  
- External connectors (capture sources and materialization endpoints)

## Architecture

### Core Components

- **Runtime Service** (`lib.rs`): Main service orchestrating task execution
  - Configures container networking and local connector support
  - Routes requests to appropriate task-specific modules
  - Handles log forwarding and error propagation

- **Task Modules**: Three parallel implementations sharing common patterns
  - `capture/`: Ingests data from external sources into collections
  - `derive/`: Transforms collections through user-defined logic
  - `materialize/`: Maintains views of collections in external systems

- **Connector Infrastructure**:
  - `image_connector.rs`: Docker container management for connector images
  - `local_connector.rs`: Direct process execution for development
  - `container.rs`: Shared container lifecycle management

- **Storage & State**:
  - `rocksdb.rs`: Persistent state management for checkpoints and schemas
  - `combine.rs`: Document combination and reduction logic

### Common Patterns Across Task Modules

All three task modules (`capture`, `derive`, `materialize`) share:

1. **Protocol Structure**:
   - `mod.rs`: Core types (Task, Binding, Transaction)
   - `protocol.rs`: Request/response handling logic
   - `serve.rs`: gRPC service implementation
   - `connector.rs`: Connector lifecycle management
   - `task.rs`: Task initialization from specifications

2. **Transaction Model**:
   - Long-polling for efficiency (5-second timeout)
   - Atomic checkpoint commits with RocksDB
   - Statistics tracking per binding

3. **Streaming Architecture**:
   - RequestStream/ResponseStream trait abstractions
   - Coroutine-based async processing
   - Backpressure via bounded channels

## Key Types

- `Runtime<L>`: Main service parameterized by log handler
- `RuntimeProtocol`: Enum distinguishing capture/derive/materialize
- Stream traits: `RequestStream`, `ResponseStream` for each module

## Entry Points

- gRPC service implementations in each module's `serve.rs`
- `Runtime::new()` for service initialization
- `TaskService` for runtime task management

## Non-obvious Details

- Uses `CHANNEL_BUFFER=16` for document pipelines - balances memory usage with processing efficiency
- Connector responses include container metadata for debugging
- Maximum message size of 64MB for document processing
- Supports both authenticated (production) and local (development) modes