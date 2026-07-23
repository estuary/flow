# Connectors

_Stub — deepen via `/platform-docs` as this concept is built out._

## Glossary

**Connector**:
An OCI image integrating an external system.
_Avoid_: driver, plugin, integration

**Sidecar**:
A connector the runtime runs as a subprocess, speaking its protocol over gRPC.

## Overview

A connector is an OCI image that integrates an external system. The runtime runs a connector as a sidecar, driving it over gRPC with the capture or materialize protocol.

## Where this lives

- `crates/connector-init` — connector startup and gRPC bridging
- `crates/dekaf-connector` — the Dekaf connector
- `go/protocols/capture/capture.proto`, `go/protocols/materialize/materialize.proto` — connector protocols
