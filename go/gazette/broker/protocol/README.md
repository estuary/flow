# broker/protocol

The `broker/protocol` package defines Gazette's core broker datamodel, validation framework, and gRPC APIs. It implements the fundamental types and service contracts that enable clients and broker servers to communicate reliably.

## What it does

- **Protobuf API Definition**: Implements the complete gRPC service interface for journal operations (List, Apply, Read, Append, Replicate, ListFragments)
- **Type Safety & Validation**: Provides strict validation for all protocol messages through the `Validator` interface, ensuring well-formed data without ad-hoc checks
- **Smart Load Balancing**: Includes a gRPC dispatcher that routes requests based on journal topology and server availability
- **Core Types**: Defines essential broker concepts like `Journal`, `Fragment`, `JournalSpec`, and `Route`

## How it fits into the broader codebase

This package serves as the contract layer between:
- Gazette broker servers (`broker/`)
- Client libraries (`broker/client/`)
- External tools (`cmd/gazctl/`)

The protocol is designed for both machine-to-machine communication (broker replication) and client-facing operations.

## Essential types and usage

### Core Types
- **`Journal`**: Unique identifier for a journal (e.g., "company-journals/topic/part-1234")
- **`JournalSpec`**: Complete specification including replication, fragment stores, retention policies
- **`Fragment`**: Immutable data segment with content addressing and integrity checksums
- **`Route`**: Current topology showing which brokers serve a journal and who is primary

### Key Services
- **`Journal` service**: Primary gRPC interface with 6 core RPCs
- **Validation**: All types implement `Validator` for strict correctness guarantees
- **Dispatcher**: Load balancer for intelligent request routing

### Import Convention
```go
import pb "go.gazette.dev/core/broker/protocol"
```

## Architecture

**Client-side Dispatching**: The gRPC dispatcher balancer (`DispatcherGRPCBalancerName`) enables clients to use a single connection while automatically routing requests to appropriate brokers based on journal assignments and server health.

**Validation-First Design**: Every protocol message must pass validation before processing, eliminating the need for repetitive error checking throughout the broker implementation.

**Content Addressing**: Fragments use deterministic naming based on content hashes, enabling safe distributed storage and deduplication.