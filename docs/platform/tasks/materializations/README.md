# Materializations

_Stub — deepen via `/platform-docs` as this concept is built out._

## Glossary

**Materialization**:
A task that maintains a materialized view of source collections in an external endpoint.
_Avoid_: sink, export, sync

## Overview

A materialization continuously pushes documents from source collections into an external system through a connector, maintaining it as an up-to-date view of those collections.

## Where this lives

- `crates/runtime` — materialization task execution
- `crates/dekaf` — Kafka-compatible materialization surface
- `go/protocols/materialize/materialize.proto` — the materialize protocol
