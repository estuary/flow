# Data Plane

_Stub — deepen via `/platform-docs` as this concept is built out._

## Glossary

**Data plane**:
The runtime surface that executes tasks and serves collection data.

**Gazette**:
The broker system whose journals back collections.

**Broker**:
A Gazette server that serves journals.

**Reactor**:
A runtime process that executes tasks on the Gazette consumer framework.

## Overview

A data plane executes tasks and serves the journals backing collections. Gazette brokers serve journals, reactors run tasks on the Gazette consumer framework and run connectors as sidecars, and Etcd holds config for both.

## Where this lives

- `crates/gazette` — broker and journal client
- `crates/runtime`, `crates/runtime-sidecar` — reactor runtime and sidecar
- `crates/shuffle` — shuffled reads between tasks
