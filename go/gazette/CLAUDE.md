# CLAUDE.md

This repo implements Gazette: a low-latency streaming data broker,
and a consumer framework which builds upon it.

Gazette is OSS and maintained by Estuary as a crucial building
block of Estuary's "Flow" platform.

## Layout

- Repo lives at `https://github.com/gazette/core`.
- Has Go import path `go.gazette.dev/core`.

## Build and Test

Standard Go tooling:
```bash
go build ./...
go test ./...
```

Makefile for whole-project build and test:
```bash
make go-build
make go-test-fast
```

Makefile will re-generate protobuf as needed.

### Format and Lint

Standard tooling:
```bash
go fmt ./...
go vet ./...
```

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

## README.md

Every Go module should have a README.md which has essential
context for working within this repository:

- what it does
- how it fits into the broader codebase
- essential types and usage
- brief details on architecture

A README.md is targeted at an expert developer, and is ONLY a roadmap:
it's concise and serves to orient a developer on where to look next,
rather than being comprehensive documentation.

Each README.md must stay up to date.
On making changes, or if you spot an inconsistency, then update the affected README.

When planning, examine READMEs for crucial context.

## Development Guidelines

### Implementation
- DO use `var myVar = ...` for initialization. Do NOT use `myVar := ...`
  The exception is cases like `myVar, err := ...` where `err` cannot be redeclared
- DO write comments that document design rationale, broader context, or non-obvious behavior
- Do NOT write comments which describe the obvious behavior of code.
  For example, don't write `// Get credentials` before a call `getCredentials()`
- Use functional programming over procedural
- Use early-return over nested conditionals

### Testing
- Prefer snapshots over fine-grain assertions
- Test utilities in `/brokertest` and `/consumertest`
- Also `etcdtest` for real etcd in tests

### Errors
- Use structured errors with `fmt.Errorf`
- Wrap errors with context
- Return errors up the stack rather than logging
- If something cannot happen, then panic.
  - Don't waste SLOC with extra error checks.

### Logging
- Use `logrus` package
- Structured logging with key-value pairs
- Avoid verbose logging in hot paths
