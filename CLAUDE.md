# CLAUDE.md

Estuary Flow is a real-time data platform with:
- Control plane: user-facing catalog management APIs
- Data planes: distributed runtime execution
- Connectors: OCI images integrating external systems

This repo lives at `https://github.com/estuary/flow`

## Repository Overview

Flow is built with:
- **Rust** (primary language)
  - Third-party sources under `~/.cargo/registry/src/`
- **Go** - integration glue with the Gazette consumer framework
  - Third-party sources under `~/go/pkg/mod/`
- **Protobuf** - communication between control plane, data planes, and connectors
- **Supabase** - migrations are under `supabase/migrations/`
  - pgTAP tests under `supabase/tests/`
- **Docs** - external user-facing product documentation under `site/` (Docusaurus)

## Essential Commands

### Build

```bash
# Check specific Rust crate
cargo check -p $crate_name

# Build specific Rust crate
cargo build --release --locked -p $crate_name

# Build Go module (use wrapper script for proper CGO flags)
./go.sh build ./go/$module_name

# Regenerate checked-in protobuf (required after .proto changes)
make go-protobufs rust-protobufs

# Project-wide build (SLOW)
make linux-binaries
```

Some Go modules link RocksDB or SQLite.
Use `./go.sh` wrapper to configure proper CGO flags.

### Test

```bash
# Test specific Rust crate
cargo test --release --locked -p $crate_name

# Test specific Go module
./go.sh test ./go/$module_name

# Run pgTAP SQL tests
./supabase/run_sql_tests.sh

# Test all Rust code (SLOW)
make rust-gnu-test

# Test all Go code (SLOW)
make go-test-fast

# E2E tests over derivation examples (SLOW)
make catalog-test

# E2E tests over capture / materialize examples (VERY SLOW)
make end-to-end-test
```

### Development

A development Supabase instance is available:
```bash
# Reset with current migrations as needed
supabase db reset

# Interact directly with dev DB
psql postgresql://postgres:postgres@localhost:5432/postgres -c 'SELECT 1;'
```

## Architecture Overview

### Core Concepts

Users interact with the control plane to manage a catalog of:
- **Captures**: tasks which capture from a user endpoint into target collections
- **Collections**: collections of data with enforced JSON Schema
- **Derivations**: both a collection and a task - the task builds its collection through transformation of other collections
- **Materializations**: tasks which maintain materialized views of source collections in an endpoint
- **Tests**: fixtures of source collection inputs and expected derivation outputs

Collections and tasks have a declarative (JSON/YAML) **model**.
Users refine model changes in **drafts**, which are **published**
to the control plane for verification and testing.
The control plane compiles the user's catalog model into
**built specs** that have extra specifics required by the runtime,
and activates specs into their associated data plane.

### Control-plane components
- **Supabase**: catalog and platform config DB
- **Agent**: APIs and background automation
- **Data-plane controller**: provisions data planes

### Data-plane components
- **Gazette**: brokers serve the journals that back collections
- **Reactors**: runtime written to Gazette consumer framework;
  executes tasks and runs connectors as sidecars over gRPC
- **Etcd**: config for gazette and reactors

### Protocols

- `go/protocols/flow/flow.proto` - core types and built specs
- `go/protocols/capture/capture.proto` - protocol for capture tasks
- `go/protocols/derive/derive.proto` - for derivation tasks
- `go/protocols/materialize/materialize.proto` - for materialization tasks

## README.md

Every crate/module should have a README.md with essential context:
- Purpose and fit within the project
- Key types and entry points
- Brief architecture and non-obvious details

A README.md is ONLY a roadmap for expert developers,
orienting them where to look next.

Keep READMEs current - update with code changes.

## Development Guidelines

### Implementation
- Use `var myVar = ...` in Go. Do NOT use `myVar := ...` (unless required due to shadowing)
- Write comments that document "why" - rationale, broader context, and non-obvious detail
- Do NOT write comments which describe the obvious behavior of code.
  Don't write `// Get credentials` before a call `getCredentials()`
- Prefer functional approaches. Try to avoid mutation.
- Use early-return over nested conditionals

### Testing
- Prefer snapshots over fine-grain assertions (`insta` / `cupaloy`)

### Errors
- Wrap errors with context (`anyhow::Context` / `fmt.Errorf`)
- Return errors up the stack rather than logging
- Panic on impossible states (do NOT add spurious error handling)

### Logging
- Structured logging with context (`tracing` / `logrus`)
- Avoid verbose logging in hot paths
