# Catalog

_Stub — deepen via `/platform-docs` as this concept is built out._

## Glossary

**Catalog**:
The complete set of captures, collections, derivations, materializations, and tests a user manages.

**Draft**:
A set of proposed spec changes a user edits before publishing.
_Avoid_: branch, changeset

**Publication**:
The act of verifying a draft's specs and activating them.
_Avoid_: deploy, release

**Built spec**:
A spec compiled by the control plane, carrying the extra specifics the runtime needs.

## Overview

The catalog is the declarative model users manage. Users refine changes in drafts, publish them to the control plane for verification and testing, and the platform compiles each spec into a built spec it activates into a data plane.

## Where this lives

- `crates/models` — the spec model types
- `crates/agent`, `crates/publisher` — draft and publication handling
- `crates/build`, `crates/validation`, `crates/sources` — compilation into built specs
- `go/protocols/flow/flow.proto` — core flow types and the built-spec protobuf
