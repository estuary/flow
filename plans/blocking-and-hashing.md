# Blocking and Hashing

## Executive Summary

This project introduces publication-time blocking and hashing capabilities to Flow, enabling users to remove sensitive data (blocking) or obfuscate it via SHA-256 hashing before documents are written to journals. Users identify transformed locations through a new JSON Schema annotation of the collection write schema, with hashing salts managed under each capture or derivation.

## Background & Context

### Problem Statement

1. **Data Privacy Compliance**: Organizations need to prevent sensitive data from being persisted in Flow's journals while still capturing and processing the remaining document content.
   - As a data engineer, I need to block PII fields from being stored so that I comply with privacy regulations while maintaining operational data flows.

2. **Data Obfuscation**: Users need to hash sensitive values for analytics and correlation while preventing recovery of original values.
   - As a data analyst, I need to hash customer identifiers so that I can perform analytics without exposing actual customer data.

3. **Selective Field Redaction**: Users need granular control over which document locations are transformed, including nested locations under arrays or recursive structures.
   - As a platform administrator, I need to configure field-level blocking/hashing per collection so that different data sensitivity requirements are met across teams.

### Existing Components

- **HeapNode Document Model:** Mutable document representation used in runtime
  - Implementation: [`crates/doc/src/heap.rs`](../crates/doc/src/heap.rs)
  - Supports in-place mutation for property removal and value replacement
- **Capture Runtime:** Processes captured documents before journal writes
  - Implementation: [`crates/runtime/src/capture/protocol.rs`](../crates/runtime/src/capture/protocol.rs)
  - Handles document ingestion, UUID injection, and memtable additions
- **Derivation Runtime:** Processes derived documents before journal writes
  - Implementation: [`crates/runtime/src/derive/protocol.rs`](../crates/runtime/src/derive/protocol.rs)
  - Handles transform outputs and document publishing

## Architecture

The blocking and hashing feature introduces a JSON Schema annotation `transform` to specify fine-grain field-level data transformations. This annotation works similarly to the existing `reduce` keyword, allowing users to mark locations for removal or hashing (SHA-256 obfuscation) throughout their document structure. Transformations are applied during document combining/draining operations before journal writes. Hashing uses SHA-256 with per-task salts for consistent obfuscation.

As the `transform` annotation is applied at write time (through `schema` or `writeSchema`, not `readSchema`), we will also update auto-discovery behavior of the control plane. Currently, auto-discovery completely overwrites the collection write schema, which would remove any annotations added by the user. To prevent this, we will introduce a new sub-schema of the write schema `flow://connector-schema` with similar behavior to `flow://inferred-schema` of the read schema: during auto-discovery, only this sub-schema definition will be updated, leaving the surrounding schema unmodified.


### Data Model

**JSON Schema Annotation for Treatments**:

Users apply a new JSON Schema `transform` keyword to block or hash locations:

```yaml
collections:
  my/collection:
    schema:
      $defs:
        flow://connector-schema:
          $id: flow://connector-schema
          type: object
          # Connector's schema is inlined here and updated during auto-discovery
          properties:
            user_email:
              type: string
            ssn:
              type: string
            addresses:
              type: array
              items:
                type: object
                properties:
                  street:
                    type: string
      $ref: flow://connector-schema
      # User additions to control data blocking:
      properties:
        user_email:
          transform: sha256
        ssn:
          transform: remove
        addresses:
          items:
            properties:
              street:
                transform: sha256 # Hash nested field in array items
```

**Connector Schema Management**:

To support user-defined treatments while preserving connector schema updates:
- Collections have both `writeSchema` (connector-controlled) and user-controlled schema
- During auto-discovery, the connector's schema is written to `$defs/flow://connector-schema`
- User's schema references this via `$ref: flow://connector-schema`
- Only the `flow://connector-schema` definition is overwritten during updates
- Users can wrap the connector schema with their own treatments and validations

Migration for existing collections without this pattern:
```yaml
# Original connector schema becomes:
$defs:
  flow://connector-schema:
    $id: flow://connector-schema
    # Original schema here
$ref: flow://connector-schema
```

**Treatment Composition and Precedence**:

- Treatments are inherited through schema composition (`$ref`, `allOf`, etc.)
- When multiple schemas define treatments for the same location:
  - `block` takes precedence over `sha256`
  - Explicit treatments override inherited ones
- Treatments apply to all instances of a location, including within arrays and nested objects
- Recursive schemas can apply treatments uniformly across all recursion levels

**CaptureSpec Salt Storage** (in `flow.proto`):
```protobuf
message CaptureSpec {
  // ... existing fields ...

  // Salt for SHA-256 hashing. Generated if not user-provided.
  bytes hashing_salt = 20;
}
```

**DerivationSpec Salt Storage** (in `flow.proto`):
```protobuf
message CollectionSpec.Derivation {
  // ... existing fields ...

  // Salt for SHA-256 hashing. Generated if not user-provided.
  bytes hashing_salt = 20;
}
```

**Capture Model Salt Configuration** (user-facing):
```yaml
captures:
  my/capture:
    # ... existing config ...
    hashingSalt: "user-provided-salt"  # Optional, base64 encoded
```

**Derivation Model Salt Configuration** (user-facing):
```yaml
collections:
  my/derivation:
    derive:
      # ... existing config ...
      hashingSalt: "user-provided-salt"  # Optional, base64 encoded
```


### Control Flow

1. **Configuration Phase** (validation crate):
   - User defines `x-treatment` annotations in collection schemas
   - Validation ensures:
     - Only string/integer types can be hashed (error otherwise)
     - Blocked locations cannot be required fields or keys
     - Treatment annotations are valid enum values ("block" or "sha256")
   - For captures/derivations with hashed fields but no user salt:
     - Generate random salt if no live spec exists
     - Propagate existing salt from live spec if rebuilding (similar to generation IDs)
   - Store salt in built CaptureSpec or Derivation spec

2. **Build Phase** (assemble crate):
   - Parse collection schemas to identify `x-treatment` annotations
   - Build treatment metadata mapping document locations to treatments
   - Store salt (user-provided or generated) in built specs
   - Handle `flow://connector-schema` references for auto-discovery compatibility

3. **Runtime Phase - Document Processing** (`crates/doc/src/combine`):
   - Treatments are applied at three drain points:
     - `MemDrainer::drain_next()` - when draining from memory
     - `SpillDrainer::drain_next()` - when draining from spilled data
     - `MemTable::spill()` - when spilling without re-validation
   - Apply transformations to HeapNode during drain:
     - For blocked fields: Remove properties from HeapNode's BumpVec (maintaining sort order)
     - For SHA-256 fields: Replace values in-place:
       - Strings: SHA-256(salt + value) → hex string
       - Integers: SHA-256(salt + value) → integer via modulo i64::MAX (sign not preserved)
       - Nulls: pass through unchanged
   - Document continues through normal flow (UUID injection, journal writes)

5. **Materialization Phase**:
   - Documents already transformed, no additional action needed
   - Materialized views reflect blocked/hashed state