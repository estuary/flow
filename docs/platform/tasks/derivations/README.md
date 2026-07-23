# Derivations

_Stub — deepen via `/platform-docs` as this concept is built out._

## Glossary

**Derivation**:
Both a collection and a task: a task that builds its collection by transforming other collections.
_Avoid_: transform, view, pipeline

## Overview

A derivation is both a collection and the task that builds it. It transforms documents read from source collections through user-defined logic and writes the results into its own collection.

## Where this lives

- `crates/derive`, `crates/derive-sqlite`, `crates/derive-typescript` — derivation runtimes
- `crates/runtime-next`, `crates/shuffle` — task execution and shuffled reads
- `go/protocols/derive/derive.proto` — the derive protocol
