# Collection Projections in flow-web

This document describes the collection projections functionality exposed through the `flow-web` WASM crate.

## Overview

The `skim_collection_projections` function provides access to Estuary's collection validation and projection derivation logic from web interfaces. It processes a collection's schema, key, and projection definitions to derive the actual projections that would be available for materialization or derivation.

## Function Signature

```typescript
function skim_collection_projections(input: CollectionProjectionsInput): CollectionProjectionsResult
```

## Input Format

```typescript
interface CollectionProjectionsInput {
  collection: string;           // Collection name (e.g., "acmeCo/users")
  model: CollectionDef;         // Complete collection definition
}

interface CollectionDef {
  schema: JSONSchema;           // JSON schema for the collection
  writeSchema?: JSONSchema;     // Optional write schema (if different from read)
  readSchema?: JSONSchema;      // Optional read schema (if different from write)
  key: string[];               // JSON pointer strings for the collection key
  projections: Record<string, string>; // Named projections mapping field names to JSON pointers
  derive?: DerivationDef;      // Optional derivation configuration
  journals?: JournalConfig;    // Optional journal configuration
}
```

## Output Format

```typescript
interface CollectionProjectionsResult {
  projections: Projection[];    // Derived projections ready for use
  errors: string[];             // Validation errors encountered
}

interface Projection {
  ptr: string;                 // JSON pointer to the field location
  field: string;               // Logical field name
  inference: FieldInference;   // Inferred type and constraint information
  isPartitionKey: boolean;     // Whether this field is part of partition key
  isPrimaryKey: boolean;       // Whether this field is part of primary key
}

interface FieldInference {
  types: string[];             // Inferred JSON types (e.g., ["string", "null"])
  string: StringConstraints;   // String-specific constraints
  exists: string;              // Existence requirement ("must", "may", "cannot")
  title?: string;              // Human-readable field title
  description?: string;        // Field description
  default?: any;               // Default value if applicable
}
```

## Key Features

### Schema Processing
- Validates JSON schema structure and references
- Resolves schema references and definitions
- Processes read/write schema variations
- Infers field types and constraints

### Projection Derivation
- Maps logical field names to JSON pointer locations
- Validates projection definitions against schema
- Infers field characteristics (types, nullability, etc.)
- Identifies key and partition key fields

### Error Reporting
- Comprehensive validation error messages
- Schema validation errors
- Key definition problems
- Projection mapping issues

## Usage Example

```javascript
import { skim_collection_projections } from 'flow-web';

const input = {
  collection: "acmeCo/users",
  model: {
    schema: {
      type: "object",
      properties: {
        id: { type: "integer" },
        email: { type: "string", format: "email" },
        name: { type: "string" },
        created_at: { type: "string", format: "date-time" },
        metadata: {
          type: "object",
          properties: {
            source: { type: "string" }
          }
        }
      },
      required: ["id", "email"]
    },
    key: ["/id"],
    projections: {
      "Id": "/id",
      "Email": "/email",
      "Name": "/name",
      "CreatedAt": "/created_at",
      "Source": "/metadata/source"
    }
  }
};

const result = skim_collection_projections(input);

// result.projections contains derived projection objects
// result.errors contains any validation errors
```

## Common Use Cases

### Collection Schema Validation
Validate an edited collection model prior to publication:

```javascript
const result = skim_collection_projections({
  collection: "myCollection",
  model: userDefinedCollection
});

if (result.errors.length > 0) {
  console.error("Collection validation failed:", result.errors);
} else {
  console.log("Collection is valid with projections:", result.projections);
}
```

### UI Field Discovery
Enumerate available projections for materialization field selection:

```javascript
const result = skim_collection_projections(collectionDef);
const available = result.projections.map(p => ({
  name: p.field,
  pointer: p.ptr,
  types: p.inference.types,
  required: p.inference.exists === "must"
}));
```

### Key Validation
Verify that edited collection key pointers are valid and key-able types.

```javascript
const result = skim_collection_projections(collectionDef);
const keyFields = result.projections.filter(p => p.is_primary_key);
if (keyFields.length === 0) {
  console.warn("No key fields found in collection");
}
```

## Error Handling

The function returns JavaScript errors for:
- Invalid collection names
- Malformed JSON schemas
- Invalid key definitions
- Projection mapping errors
- Schema reference resolution failures

All validation errors are collected and returned in the `errors` array with descriptive messages suitable for displaying to users.

## Integration Notes

- This function runs the same logic as the control plane's collection validation
- Results can be used to populate UI forms for materialization configuration
- The derived projections are exactly what would be available for field selection
- For production collections, the control plane performs the actual validation
