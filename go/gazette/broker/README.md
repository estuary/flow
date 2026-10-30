# broker

The `broker` package implements Gazette's core journal broker service and gRPC API runtime. It provides the distributed streaming data broker that enables low-latency append and read operations against journals stored across multiple storage backends.

## What it does

- **Journal Server Implementation**: Implements the complete `protocol.JournalServer` gRPC interface (Read, Append, Replicate, List, Apply, ListFragments)
- **Topology Observation**: Observes current journal topology assignments from the `allocator` package via etcd and implements APIs based on those assignments
- **Replication Pipeline**: Orchestrates real-time replication of journal writes across broker replicas for durability and consistency  
- **Fragment Management**: Handles journal fragment lifecycle from spooling through persistence to remote storage (S3, GCS, Azure, filesystem)
- **Request Routing**: Intelligently routes client requests to appropriate broker instances based on journal assignments and server health

## How it fits into the broader codebase

The broker package serves as Gazette's core service layer, positioned between:

- **Client Libraries** (`broker/client/`): Provides the server-side implementation of APIs that clients consume
- **Protocol Definitions** (`broker/protocol/`): Implements the gRPC service contracts and message validation
- **Storage Abstractions** (`broker/fragment/`): Manages the underlying fragment storage and indexing
- **Command-line Tools** (`cmd/gazctl/`, `cmd/gazette/`): Powers the broker server process and management utilities
- **Consumer Framework** (`consumer/`): Provides the journal substrate that consumer applications build upon

This package is what transforms Gazette from a protocol definition into a running distributed system.

## Essential types and usage

### Core Types

- **`Service`**: Top-level broker service that implements `protocol.JournalServer` and coordinates with etcd
- **`replica`**: Runtime instance representing a journal assigned to this broker, managing its fragment index and replication
- **`resolver`**: Maps journal names to responsible broker routes and manages local replica lifecycle  
- **`pipeline`**: Coordinates distributed write replication across broker peers for consistency
- **`appendFSM`**: State machine managing the lifecycle of append operations with flow control and validation

### Key APIs

- **Append API** (`append_api.go`): Handles streaming append operations with atomic commit semantics
- **Read API** (`read_api.go`): Serves streaming read operations from local fragments or via proxy  
- **Replicate API** (`replicate_api.go`): Manages peer-to-peer replication between broker instances
- **List/Apply APIs** (`list_apply_api.go`): Handles journal specification management and discovery

### Subsystems

- **Fragment Storage** (`fragment/`): Abstracts over multiple storage backends with unified operations
- **Protocol Layer** (`protocol/`): Defines gRPC contracts, validation, and core data types
- **Journal Space** (`journalspace/`): Manages allocation keyspace for journal assignments
- **HTTP Gateway** (`http_gateway/`): Provides REST interface alongside native gRPC APIs

## Architecture

**Topology-Driven Operation**: The `allocator` package manages journal topology assignment via etcd. This package observes those assignments and implements the replication protocol accordingly. Each journal is assigned to a primary broker and optional replica brokers based on the configured replication factor.

**Replication Pipeline**: Write operations flow through a multi-phase pipeline:
1. Client connects to any broker 
2. Broker resolves journal to responsible primary
3. Primary coordinates atomic replication across R replicas
4. Fragments are asynchronously persisted to configured storage

**Fragment-Based Storage**: Journals are stored as immutable, content-addressed fragments in blob storage. The broker maintains local indexes mapping byte offsets to fragments for efficient reads.

**Proxy-Capable Reads**: Read operations automatically route to appropriate brokers. Non-primary brokers can proxy reads or serve from local fragment caches.

The architecture enables horizontal scaling, automated failover, and strong consistency guarantees while maintaining the "files in blob storage" abstraction that makes Gazette journals simultaneously a streaming system and a data lake.