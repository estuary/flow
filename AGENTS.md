Estuary is a real-time data platform with:
- Control plane: user-facing catalog management APIs
- Data planes: distributed runtime execution
- Connectors: OCI images integrating external systems

This repo lives at `https://github.com/estuary/flow`

## Repository Overview

Estuary is built with:
- **Rust** (primary language)
  - Third-party sources under `~/.cargo/registry/src/`
- **Go** - integration glue with the Gazette consumer framework
  - Third-party sources under `~/go/pkg/mod/`
- **Protobuf** - communication between control plane, data planes, and connectors
- **Supabase** - migrations are under `supabase/migrations/`
  - pgTAP tests under `supabase/tests/`
- **Docs** - external user-facing product documentation under `site/` (Docusaurus)

## Essential Commands

### Build & Test

Use regular `cargo` and `go` tools to build and test crates.

```bash
# libsqlite3 tag is required for `bindings` and `flowctl-go` packages.
go build -tags libsqlite3 ./go/bindings

# Regenerate checked-in protobuf (required after .proto changes)
mise run build:go-protobufs
mise run build:rust-protobufs

# Run pgTAP SQL Tests
mise run ci:sql-tap

# E2E tests over derivation examples (SLOW)
mise run ci:catalog-test

# Start (just) local Supabase.
mise run local:supabase
# Reset with current migrations as needed
supabase db reset
# Interact directly with dev DB ($FLOW_PG_URL is ambient inside the checkout)
psql "$FLOW_PG_URL" -c 'SELECT 1;'

# Start a complete local stack (see local/README.md)
mise run local:stack
# CLI for interacting with the platform (FLOWCTL_PROFILE is ambient; no flag).
cargo run -p flowctl -- --help
# ...or the built binary once the stack is up:
flowctl catalog list

# Run after changing Rust files to ensure consistent formatting
cargo fmt
```

Local-stack commands are **per-stack** and mise is mandatory: each checkout
(primary clone or linked git worktree) runs its own isolated stack, and
`mise/tasks/local/stack-env` makes that stack's variables ambient for everything run
through mise. There is no special/canonical stack and no fixed ports — the
primary clone is just stack `flow`; ports are `base(i) = 10000 + 1000·index`.
Run `mise run local:stack-info` to see this checkout's ports, units, and
ready-to-paste commands. See `local/README.md`.

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

Collections and tasks live in a unified, hierarchical namespace.
`/`-delimited prefixes act as "roles" and are the unit of AuthZ.
Users are granted capabilities to roles (`user_grants` table),
and roles are granted capabilities to other roles (`role_grants`).
A top-level prefix like `acmeCo/` homes an organization and
is called a "tenant".

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
- `go/protocols/capture/capture.proto`
- `go/protocols/derive/derive.proto`
- `go/protocols/materialize/materialize.proto`

## README.md

Every crate/module should have a README.md with essential context:
- Purpose and fit within the project
- Key types and entry points
- Brief architecture and non-obvious details

A README.md is ONLY a roadmap for expert developers,
orienting them where to look next.

Keep READMEs current - update with code changes.

## Development Guidelines

### Customer data
- NEVER write customer data into any git-checked file (source, tests, comments,
  fixtures, snapshots, docs, commit messages). This is absolute.
- This explicitly includes customer task/catalog names (e.g. tenant prefixes and
  collection paths), endpoint configs, credentials, hostnames, and any data
  values sampled from a customer's system.
- When a real-world example is needed — such as reproducing a bug report — use a
  fictitious tenant like `acmeCo/` and invented names that preserve the relevant
  shape (length, nesting depth) without the original identifiers.

### Implementation
- Use `var myVar = ...` in Go. Do NOT use `myVar := ...` (unless required due to shadowing)
- Write comments that document "why" - rationale, broader context, and non-obvious detail
- Do NOT write comments which describe the obvious behavior of code.
  Don't write `// Get credentials` before a call `getCredentials()`
- Use early-return over nested conditionals
- Use at least one level of name qualification for third-party types and functions.
  For example, `axum::Router::new()` instead of `use axum::Router; Router::new()`.
  Types / functions should be unqualified ONLY if they're in the current module.
- Prefer pure functions that take and act over POD states.
  AVOID structures that mix complex state and impl behaviors, where possible.
  The exception is state machines: structs and enums that encapsulate fine-grain
  POD state into higher-order transitions that are easier to reason about.
  DO seek to decompose problems into state machines.
- Avoid routines with trivial bodies that could be inlined into the caller.
  Indirection has cost (hard to read): each routine must buy us something.
- Decompose IO and POD processing into separate routines where possible.
  Routines should gravitate towards IO or processing, and not mix both.

### Testing
- Prefer snapshots over fine-grain assertions (`insta` / `cupaloy`)

### Errors
- Wrap errors with context (`anyhow::Context` / `fmt.Errorf`)
- Return errors up the stack rather than logging
- Panic on impossible states (do NOT add spurious error handling)

### Logging
- Structured logging with context (`tracing` / `logrus`)
- Avoid verbose logging in hot paths
