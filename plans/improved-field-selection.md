# Improved Field Selection

## Executive Summary

This project aims to enhance Estuary's field selection capabilities for materializations by introducing support for custom group-by keys, depth-based field selection, and shifting field selection implementation burden from connectors to the control plane. Currently, field selection is limited to collection key JSON pointers and selects all scalar fields regardless of document hierarchy depth. Additionally, connectors currently handle field validation and folding logic individually. The improvements will allow users to specify custom primary keys via `group_by` fields, control field selection depth to avoid over-materialization of deeply nested data structures, and centralize field validation and folding logic in the control plane for consistency across all connectors.

## Background & Context

### Problem Statement
Field selection is a critical interaction between Estuary users, the control plane, and materialization connectors that determines which document fields are materialized to destinations like SQL tables. The current implementation has several limitations:

1. **Limited Key Selection**: Field selection keys are restricted to those matching collection key JSON pointers
  - As a user, I want to declare a custom group-by that defines the primary key for my materialization, independent of the source collection's key structure
2. **No Depth Control**: All scalar fields are selected by default regardless of nesting depth, leading to overly wide and nested tables
  - As a user, I want to control selection depth so I can materialize only top-level fields (depth 1)
  - As a user, if I require field "parent" then I want field "parent/child" to be omitted from selection, regardless of my selected depth
3. **Connector Duplication**: Each connector implements its own field validation (key requirements, null constraints) and field folding (e.x. lowercasing; removing dashes/underscores/unicode)
  - As a connector developer, I want to inform the control plane of declarative constraints via the materialization protocol, and not implement it myself
4. **More Flexible `flow_document`**: Today "standard" materializations require that the root JSON pointer is materialized as a field.
  - As a user, I'd like to materialize all top-level fields and avoid having the `flow_document` field altogether.
5. **Improved automation**: Field conflicts, such as incompatible SQL types, do not respect `onIncompatibleSchemaChange`
  - As a platform, we'd like to relax current restrictions on collection key changes
  - As a user, I'd like a changed collection key or incompatible field to honor `onIncompatibleSchemaChange`
6. **Backward Compatibility**: We have many running materializations, and need to deploy improvements without upsetting their current selections
  - As a user, if my materialization is currently running with a field, it should continue to unless I explicitly exclude it
  - As a platform, we need to reconcile depth with the existing `recommended` field without breaking existing tasks.

## Technical Details

### Existing Components
- **MaterializationFields Model:** Core data structure defining field inclusion/exclusion behavior
  - Implementation: [`crates/models/src/materializations.rs`](crates/models/src/materializations.rs)
  - Current fields: `include`, `exclude`, `recommended`
- **Field Selection Validation:** Logic for processing field selection rules and generating FieldSelection protocol messages
  - Implementation: [`crates/validation/src/materialization.rs`](crates/validation/src/materialization.rs)
  - Key functions: `walk_materialization_fields`, `walk_materialization_response`
- **FieldSelection Protocol:** Protocol buffer definition for communicating selected fields
  - Implementation: [`go/protocols/flow/flow.proto`](go/protocols/flow/flow.proto)
  - Current structure: `keys`, `values`, `document`, `field_config_json_map`
- **Materialization Constraint Protocol:** Protocol buffer definition for connector field constraints
  - Implementation: [`go/protocols/materialize/materialize.proto`](go/protocols/materialize/materialize.proto)
  - Current structure: `type`, `reason`

### Architecture

Building on these existing components, the improved field selection system introduces enhancements to the data models and validation logic while maintaining compatibility with current implementations.

The improved field selection system restructures how fields are selected, validated, and communicated between the control plane and connectors. This architecture centralizes logic in the control plane while providing connectors with declarative constraint mechanisms.

#### Core Components

**1. Enhanced MaterializationFields Model**

The `MaterializationFields` struct in `models/src/materializations.rs` will be extended with:
- `group_by: Vec<Field>` - Custom primary key fields independent of collection keys
- `recommended: RecommendedDepth` - Depth-based field selection replacing boolean mode
  - `RecommendedDepth::Depth(u32)` for precise depth control (0 = no fields, 1 = top-level only, etc.)
  - `RecommendedDepth::Legacy(bool)` for backward compatibility with existing configurations

**2. Refined Constraint Protocol**

Materialization constraints will be clarified with updated semantics and a new `folded_field` capability:
- `FIELD_REQUIRED` - Fields with special connector meaning (e.g., "text" field for Slack connector)
- `LOCATION_REQUIRED` - JSON pointer locations that must be present (e.g., root document, all top-level fields)
- `FIELD_OPTIONAL` - Fields that may be materialized (absorbs deprecated `LOCATION_RECOMMENDED`)
- `FIELD_FORBIDDEN` - Fields permanently unusable (e.g., unsupported types like JSON null)
- `UNSATISFIABLE` - Fields requiring backfill to resolve conflicts (e.g., incompatible SQL column types)
- `folded_field` - Connector-provided field name transformations for technical limitations

**3. Centralized Field Selection Logic**

The control plane will implement a priority-based field selection algorithm using `Select` and `Reject` enums:

```rust
// Selection reasons ordered by strength (weakest to strongest)
pub enum Select {
    DesiredDepth,           // Within user-specified depth
    CoreMetadata,           // Essential system fields
    ConnectorRequiresLocation,  // Location-based connector requirements
    UserDefined,           // User-projected field
    CurrentValue,          // Currently materialized field
    PartitionKey,          // Collection partition key
    ConnectorRequires,     // Named field requirements
    UserRequires,          // User field requirements
    CurrentDocument,       // Current document storage field
    GroupByKey,            // Part of the group-by key
}

// Rejection reasons ordered by strength (weakest to strongest)
pub enum Reject {
    NotSelected,           // No selection criteria met
    CoveredLocation,       // Location covered by parent field
    DuplicateLocation,     // Location already materialized
    DuplicateFold,         // Ambiguous folded field name
    ConnectorOmits,        // No connector constraint returned with Validated
    CollectionOmits,       // Field not in source collection
    ConnectorUnsatisfiable, // Requires backfill
    ConnectorForbids,      // Permanently forbidden
    UserExcludes,          // User exclusion
}
```

#### Selection Algorithm

For each potential field, the system will:
1. Determine the strongest `Select` reason (if any)
2. Determine the strongest `Reject` reason (if any)
3. Compare strengths to decide: selected, rejected, or conflicted
4. Order selected fields by `Select` strength for priority-based resolution
5. Handle conflicts according to `onIncompatibleSchemaChange` settings

#### Backward Compatibility Strategy

**Implicit Group-by Key**: When no `group_by` is specified, the canonical projections of the collection key are used instead. This represents a technically-breaking change from the current preference for user projections.

**Depth Integration**: The new depth-based selection integrates with existing `recommended` configurations:
- `recommended: true` maps to infinite depth selection
- `recommended: false` maps to depth 0 (no automatic selection)
- Existing field selections are preserved unless explicitly excluded

**Connector Impacts**: Connector constraints are updated in-place with clarified but compatible semantics. The control-plane will update Projection.IsPrimaryKey fields prior to invoking connectors, which lets them identify keyed fields during Validate.

## Risk Mitigation

Given the scope of changes to field selection logic and the potential for breaking changes (particularly around collection key handling), we will implement a parallel "shadow" strategy during rollout.

### Parallel Selection Logic

The new field selection algorithm will live alongside the existing logic, with both running in parallel:

1. **Dual Execution**: Both old and new selection algorithms will execute for every materialization validation
2. **Existing Logic Remains Effective**: The current field selection logic will continue to determine actual field selections
3. **New Logic Runs Shadow Mode**: The improved selection algorithm will run but not affect real materializations
4. **Difference Logging**: Any discrepancies between old and new selections will be logged with detailed rationale

### Impact Analysis Process

When differences are detected between old and new selection logic:

1. **Automatic Logging**: Log the materialization task, binding details, and specific field differences
2. **Categorization**: Classify differences by root cause:
   - Collection key projection handling changes (breaking change impact)
   - Depth-based selection differences
   - New constraint handling
   - Field folding conflicts
3. **Task Review**: Study affected existing tasks to understand impact and necessity
4. **Mitigation Strategy**: For tasks impacted by collection key changes, explicitly configure `group_by` to maintain current keys

### Rollout Strategy

1. **Initial**: Deploy parallel execution with logging
2. **Analysis Period**: Monitor for 1-2 weeks to identify all impacted materializations
3. **Remediation**: Update affected tasks with explicit `group_by` configurations where needed
4. **Cutover**: Switch to new logic as the effective implementation
5. **Cleanup**: Remove old logic after successful transition period

## Implementation Plan

### Phase 1: Core Data Model & Protocol Updates
- [ ] Extend MaterializationFields model with group_by and RecommendedDepth
  - Add `group_by: Vec<Field>` field to MaterializationFields struct
  - Replace `recommended: bool` with `recommended: RecommendedDepth` enum
  - Add backward compatibility serialization/deserialization with `#[serde(alias = "depth")]`
  - [`crates/models/src/materializations.rs`](crates/models/src/materializations.rs)
- [ ] Update materialization constraint protocol
  - Add `folded_field` string field to Constraint message
  - Update constraint type documentation and semantics
  - Deprecate `LOCATION_RECOMMENDED` in favor of `FIELD_OPTIONAL`
  - [`go/protocols/materialize/materialize.proto`](go/protocols/materialize/materialize.proto)

### Phase 2: Implement Selection and Shadow
- [ ] Implement Select and Reject enums
  - Create ordered enum variants with error message implementations
  - Add priority comparison logic for selection strength
  - Include context fields for reasons (e.g., order, config, folded_field)
  - [`crates/validation/src/field_selection.rs`](crates/validation/src/field_selection.rs)
- [ ] Implement new field selection logic
  - Logic to map fields into Select / Reject reasons
  - Implement as pure functions to facilitate snapshot testing with `insta`
  - Logic to gather / group each field into strongest reason
  - Implement depth-based field filtering for RecommendedDepth::Depth
  - Add implicit group-by key from collection keys when not specified
  - Add logic to preserve current field selections during upgrades
  - Handle folded field name conflict detection and resolution
  - [`crates/validation/src/field_selection.rs`](crates/validation/src/field_selection.rs)
- [ ] Integrate new logic in "shadow" mode
  - Selection logic is active but not primary
  - Differences in FieldSelection `keys`/`values`/`document` are logged
  - [`crates/validation/src/materialization.rs`](crates/validation/src/materialization.rs)
- [ ] Expose new logic in `flow-web` crate
  - Add a "selection outcome" type which exposes rationale to JavaScript
  - [`crates/flow-web/`](crates/flow-web/)
- [ ] Update connectors to surface folded fields
  - DO NOT update for new constraint semantics yet
  - Test field name folding for case sensitivity and character limitations

### Phase 3: Cutover
- [ ] Remove legacy field selection in favor of new implementation
  - [`crates/validation/src/materialization.rs`](crates/validation/src/materialization.rs)
- [ ] Integrate onIncompatibleSchemaChange handling
  - Extend conflict resolution to respect schema change policies
  - Add support for collection key changes and field type conflicts
  - Implement backfill requirement detection for UNSATISFIABLE constraints
  - [`crates/validation/src/materialization.rs`](crates/validation/src/materialization.rs)
- [ ] Update connectors to honor updated constraint semantics
- [ ] Add UI support for new field selection options
  - Extend materialization configuration forms with group_by field selection
  - Add depth control to field selection mode
  - Update field selection preview to use WASM routine with rationale

## Testing Plan

### Unit & Integration Tests
- [ ] `insta` snapshot testing of a fixture, covering all Select / Reject reasons, per-field outcomes, and final FieldSelection
- [ ] additional `insta` snapshot tests of perturbations of the fixture, using a JSON patch strategy
