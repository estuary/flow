# CLAUDE.md

This directory implements Gazette: a low-latency streaming data broker,
and a consumer framework which builds upon it.

Gazette is OSS and maintained by Estuary as a crucial building
block of Estuary's "Flow" platform.

## Layout

- Lives at `go/gazette` of `https://github.com/estuary/flow`,
  with Go import path `github.com/estuary/flow/go/gazette`.
- Is part of the repository's single Go module (the root `go.mod`).
- `https://github.com/gazette/core` (`go.gazette.dev/core`) is a
  secondary mirror. Development happens here.

## Build and Test

Run Go tooling through `mise`, which supplies RocksDB and etcd:
```bash
mise exec -- go build ./go/gazette/...
mise exec -- go test -tags libsqlite3 ./go/gazette/...
mise run build:gazette       # `gazette` and `gazctl` binaries
mise run build:go-protobufs  # after .proto changes
```

Protobufs use `go/gazette` as their include root,
so their names (`broker/protocol/protocol.proto`) are stable
for Flow's protobufs and Rust crates which import them.

## Architecture Overview

### Core Components

1. **Broker Service** (`/broker`)
   - Manages journal read/append operations
   - Handles fragment storage across cloud backends
   - Provides gRPC and HTTP gateway APIs
   - Key packages: `client/`, `fragment/`, `protocol/`
   - [Protobuf](broker/protocol/protocol.proto)

2. **Consumer Framework** (`/consumer`)
   - Framework for building streaming applications
   - Provides exactly-once processing semantics
   - Supports RocksDB and SQLite storage backends
   - Key packages: `recoverylog/`, `store-rocksdb/`, `store-sqlite/`
   - [Protobuf](consumer/protocol/protocol.proto)

3. **Service Discovery**
   - Brokers and consumers use etcd for member discovery and assignments
   - Key packages: `/allocator`, `/keyspace`

4. **Command Line Tools**
   - `gazette`: Main broker binary
   - `gazctl`: CLI for managing journals and shards

### Key Abstractions

- **Journal**: Append-only log backed by cloud storage fragments
- **Fragment**: Contiguous chunk of journal data stored as a cloud object
- **Shard**: Unit of consumer processing with associated state
- **Recovery Log**: Transaction log enabling exactly-once semantics

## External Docs

We maintain a directory of [user-facing docs](docs/),
using Sphinx, Restructured Text, and Read The Docs.

## Development Guidelines

The repository's root AGENTS.md applies. Additionally:

- Test utilities are in `/brokertest` and `/consumertest`,
  and `/etcdtest` provides a real etcd for tests.
- Log using `logrus`.
