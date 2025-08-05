# Field Selection in flow-web

This document describes the field selection functionality exposed through the `flow-web` WASM crate.

## Overview

The `evaluate_field_selection` function provides access to Estuary's improved field selection logic from web interfaces. It evaluates which fields should be materialized based on collection projections, user configuration, connector constraints, and existing materializations.

## Function Signature

```typescript
function evaluate_field_selection(input: FieldSelectionInput): FieldSelectionResult
```

## Input Format

```typescript
interface FieldSelectionInput {
  collectionKey: string[];                    // Collection key JSON pointers (e.g., ["/id", "/timestamp"])
  collectionProjections: Projection[];        // Available fields from the collection
  liveSpec?: BuiltMaterializationBinding;     // Existing materialization (if updating)
  model: MaterializationBinding;              // User's desired configuration
  validated: ValidatedBinding;                // Connector validation results
}
```

## Output Format

```typescript
interface FieldSelectionResult {
  outcomes: FieldOutcome[];                   // Per-field selection details
  selection: FieldSelection;                  // Final materialization configuration
  hasConflicts: boolean;                      // Whether conflicts need resolution
}

interface FieldOutcome {
  field: string;                              // Field name
  select?: SelectOutput;                      // Structured selection reason (if selected)
  reject?: RejectOutput;                      // Structured rejection reason (if rejected)
  isIncompatible?: boolean;                   // True when conflict has ConnectorIncompatible reject reason
}

interface SelectOutput {
  reason: SelectReason;                       // Structured selection reason
  detail: string;                             // Human-readable description
}

interface RejectOutput {
  reason: RejectReason;                       // Structured rejection reason
  detail: string;                             // Human-readable description
}

interface FieldSelection {
  keys: string[];                             // Fields used as primary key
  values: string[];                           // Fields materialized as values
  document: string;                           // Field storing full document (if any)
  fieldConfigJsonMap: Record<string, string>; // Per-field connector configuration
}
```

## Field Selection Reasons

### Selection Reasons (strongest to weakest)
- **GroupByKey**: Part of the materialization group-by key
- **CurrentDocument**: Currently used to store the document
- **UserRequires**: Required by user's field selection
- **ConnectorRequires**: Required by connector
- **PartitionKey**: Collection partition key
- **CurrentValue**: Part of current materialization
- **UserDefined**: User-projected field
- **ConnectorRequiresLocation**: Location-based connector requirement
- **CoreMetadata**: Essential system fields
- **DesiredDepth**: Within user-specified depth

### Rejection Reasons (strongest to weakest)
- **UserExcludes**: Excluded by user's field selection
- **ConnectorForbids**: Forbidden by connector
- **ConnectorIncompatible**: Requires backfill to resolve
- **CollectionOmits**: Field doesn't exist in source
- **ConnectorOmits**: No connector constraint provided
- **DuplicateFold**: Ambiguous folded field name
- **DuplicateLocation**: Location already materialized
- **CoveredLocation**: Location covered by parent field
- **NotSelected**: Doesn't meet selection criteria

## Usage Example

```javascript
import { evaluate_field_selection } from 'flow-web';

const input = {
  collectionKey: ["/id"],
  collectionProjections: [
    {
      ptr: "/id",
      field: "id",
      inference: { types: ["integer"], exists: "must" },
      is_primary_key: true
    },
    {
      ptr: "/name",
      field: "name",
      inference: { types: ["string"], exists: "may" },
      is_primary_key: false
    }
  ],
  model: {
    fields: {
      groupBy: [],
      recommended: { Bool: true },
      require: {},
      exclude: []
    }
  },
  validated: {
    resourcePath: ["users"],
    constraints: {
      "id": { type: "FIELD_REQUIRED", reason: "Primary key" },
      "name": { type: "FIELD_OPTIONAL", reason: "User data" }
    }
  }
};

const result = evaluate_field_selection(input);

// result.outcomes shows why each field was selected/rejected
// result.selection shows the final field configuration
// result.hasConflicts indicates if user action is needed

// Access structured reasons
result.outcomes.forEach(outcome => {
  if (outcome.select) {
    console.log(`${outcome.field} selected:`, outcome.select.reason, "-", outcome.select.detail);
  }
  if (outcome.reject) {
    console.log(`${outcome.field} rejected:`, outcome.reject.reason, "-", outcome.reject.detail);
  }
});

// Check for incompatible conflicts that may require backfill
const incompatibleFields = result.outcomes.filter(o => o.isIncompatible);
if (incompatibleFields.length > 0) {
  console.log("Fields requiring backfill:", incompatibleFields.map(f => f.field));
}
```

## Key Features

### Priority-Based Selection
The system uses a priority-based algorithm where stronger selection reasons override weaker rejection reasons, and vice versa. This ensures that critical fields (like primary keys) are always selected, while user preferences are respected where possible.

### Conflict Detection
When a field has both selection and rejection reasons, it's marked as a conflict. These conflicts are surfaced to users with detailed explanations.

### Group-by Key Handling
The system supports custom group-by keys independent of the collection's primary key. If no group-by is specified, it falls back to the collection's canonical key projections.

### Depth-Based Selection
Users can specify depth limits to control how deeply nested fields are automatically selected, preventing over-materialization of complex document structures.

### Backward Compatibility
The system preserves existing field selections when updating materializations, ensuring that changes don't unexpectedly break running tasks.

### Incompatible Conflict Handling
The `isIncompatible` boolean provides special handling for conflicts where a field is both selected and rejected with a `ConnectorIncompatible` reason. This indicates the field would be included if a backfill were performed to resolve schema incompatibilities.

**When `isIncompatible` is true:**
- The field has both selection and rejection reasons
- The rejection reason is specifically `ConnectorIncompatible`
- The field could be resolved by performing a backfill operation
- The UI should present backfill options to the user

**Example scenarios:**
- SQL column type changed from `INTEGER` to `VARCHAR`
- New `NOT NULL` constraint added to existing nullable field
- Field format requirements that existing data doesn't satisfy

```javascript
// Handle incompatible conflicts in UI
const incompatibleFields = result.outcomes.filter(o => o.isIncompatible);
if (incompatibleFields.length > 0) {
  // Show user that backfill is required for these fields
  // Access structured reason for detailed conflict information
  incompatibleFields.forEach(field => {
    console.log(`Field ${field.field} requires backfill:`);
    console.log(`  Selected because: ${field.select?.detail}`);
    console.log(`  Rejected because: ${field.reject?.detail}`);
  });
  showBackfillPrompt(incompatibleFields);
}
```

## Error Handling

The function returns JavaScript errors for:
- Invalid input JSON structure
- Missing required fields
- Internal evaluation failures

All errors include descriptive messages suitable for displaying to users.
