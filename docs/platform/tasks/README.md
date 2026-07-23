# Tasks

_Stub — deepen via `/platform-docs` as this concept is built out._

## Glossary

**Task**:
A capture, derivation, or materialization — a process the platform runs in a data plane.
_Avoid_: job, pipeline

**Shard**:
A unit of a task's partitioned, independently-assigned execution. The
[data plane](../data-plane/) owns this primitive; here it is just a task's unit
of parallelism.

**Activation**:
Installing a built spec's task into its data plane so the runtime executes it.

## Overview

A task is a process the platform executes in a data plane. Captures, derivations, and materializations share a lifecycle — published, activated, and run as shards — while each has its own semantics described below.

## Concept map

- [captures/](./captures/) — pull documents from an external endpoint into collections.
- [derivations/](./derivations/) — build a collection by transforming other collections.
- [materializations/](./materializations/) — maintain a materialized view of collections in an external endpoint.
- [tests/](./tests/) — fixtures asserting expected derivation behavior.
