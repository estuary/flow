# Captures

_Stub — deepen via `/platform-docs` as this concept is built out._

## Glossary

**Capture**:
A task that reads documents from an external endpoint into target collections.
_Avoid_: source, ingest, import

## Overview

A capture connects to an external system through a connector and writes the documents it reads into one or more target collections.

## Where this lives

- `crates/runtime` — capture task execution
- `crates/connector-init` — connector startup
- `go/protocols/capture/capture.proto` — the capture protocol
