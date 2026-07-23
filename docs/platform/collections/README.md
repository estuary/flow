# Collections

_Stub — deepen via `/platform-docs` as this concept is built out._

## Glossary

**Collection**:
An append-only set of JSON documents with an enforced schema.
_Avoid_: table, topic, stream

**Journal**:
An append-only log, held in cloud storage, that backs a collection's data.

**Storage mapping**:
The cloud storage location a collection's journals are written to.

## Overview

A collection is an append-only set of schematized JSON documents. Its data lives in one or more journals written to the cloud storage bucket named by its storage mapping.

## Where this lives

- `crates/doc`, `crates/json` — document and JSON schema handling
- `crates/models` — collection and storage-mapping spec types
