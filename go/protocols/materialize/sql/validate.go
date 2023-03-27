package sql

import (
	"fmt"

	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

// ValidateSelectedFields validates a proposed MaterializationSpec against a set of constraints. If
// any constraints would be violated, then an error is returned.
func ValidateSelectedFields(constraints map[string]*pm.Response_Validated_Constraint, proposed *pf.MaterializationSpec_Binding) error {
	// Track all the location pointers for each included field so that we can verify all the
	// LOCATION_REQUIRED constraints are met.
	var includedPointers = make(map[string]bool)

	// Does each field in the materialization have an allowable constraint?
	var allFields = proposed.FieldSelection.AllFields()
	for _, field := range allFields {
		var projection = proposed.Collection.GetProjection(field)
		if projection == nil {
			return fmt.Errorf("No such projection for field '%s'", field)
		}
		includedPointers[projection.Ptr] = true
		var constraint = constraints[field]
		if constraint.Type.IsForbidden() {
			return fmt.Errorf("The field '%s' may not be materialize because it has constraint: %s with reason: %s", field, constraint.Type, constraint.Reason)
		}
	}

	// Are all of the required fields and locations included?
	for field, constraint := range constraints {
		switch constraint.Type {
		case pm.Response_Validated_Constraint_FIELD_REQUIRED:
			if !sliceContains(field, allFields) {
				return fmt.Errorf("Required field '%s' is missing. It is required because: %s", field, constraint.Reason)
			}
		case pm.Response_Validated_Constraint_LOCATION_REQUIRED:
			var projection = proposed.Collection.GetProjection(field)
			if !includedPointers[projection.Ptr] {
				return fmt.Errorf("The materialization must include a projections of location '%s', but no such projection is included", projection.Ptr)
			}
		}
	}

	return nil
}

// ValidateNewSQLProjections returns a set of constraints for a proposed flow collection for a
// **new** materialization (one that is not running and has never been Applied). Note that this will
// "recommend" all projections of single scalar types, which is what drives the default field
// selection in flowctl.
func ValidateNewSQLProjections(proposed *pf.CollectionSpec, deltaUpdates bool) map[string]*pm.Response_Validated_Constraint {
	var constraints = make(map[string]*pm.Response_Validated_Constraint)
	for _, projection := range proposed.Projections {
		var constraint = new(pm.Response_Validated_Constraint)
		switch {
		case len(projection.Field) > 63:
			constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
			constraint.Reason = "Field names must be less than 63 bytes in length."
		case projection.IsPrimaryKey:
			constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
			constraint.Reason = "All Locations that are part of the collections key are required"
		case projection.IsRootDocumentProjection() && deltaUpdates:
			constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
			constraint.Reason = "The root document should usually be materialized"
		case projection.IsRootDocumentProjection():
			constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
			constraint.Reason = "The root document must be materialized"
		case projection.Inference.IsSingleScalarType():
			constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
			constraint.Reason = "The projection has a single scalar type"

		case projection.Inference.IsSingleType():
			constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
			constraint.Reason = "This field is able to be materialized"
		default:
			// If we got here, then either the field may have multiple types, or the only possible
			// type is "null". In either case, we're not going to allow it. Technically, we could
			// allow the null type to be materialized, but I can't think of a use case where that
			// would be desirable.
			constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
			constraint.Reason = "Cannot materialize this field"
		}
		constraints[projection.Field] = constraint
	}
	return constraints
}

// ValidateMatchesExisting returns a set of constraints to use when there is a new proposed
// CollectionSpec for a materialization that is already running, or has been Applied. The returned
// constraints will explicitly require all fields that are currently materialized, as long as they
// are not unsatisfiable, and forbid any fields that are not currently materialized.
func ValidateMatchesExisting(existing *pf.MaterializationSpec_Binding, proposed *pf.CollectionSpec) map[string]*pm.Response_Validated_Constraint {
	var constraints = make(map[string]*pm.Response_Validated_Constraint)
	for _, field := range existing.FieldSelection.AllFields() {
		var constraint = new(pm.Response_Validated_Constraint)
		var typeError = checkTypeError(field, &existing.Collection, proposed)
		if len(typeError) > 0 {
			constraint.Type = pm.Response_Validated_Constraint_UNSATISFIABLE
			constraint.Reason = typeError
		} else {
			constraint.Type = pm.Response_Validated_Constraint_FIELD_REQUIRED
			constraint.Reason = "This field is part of the current materialization"
		}

		constraints[field] = constraint
	}
	// We'll loop through the proposed projections and forbid any that aren't already in our map.
	// This is done solely so that we can supply a descriptive reason, since any fields we fail to
	// mention are implicitly forbidden.
	for _, proj := range proposed.Projections {
		if _, ok := constraints[proj.Field]; !ok {
			var constraint = new(pm.Response_Validated_Constraint)
			constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
			constraint.Reason = "This field is not included in the existing materialization."
			constraints[proj.Field] = constraint
		}
	}

	return constraints
}

func checkTypeError(field string, existing *pf.CollectionSpec, proposed *pf.CollectionSpec) string {
	// existingProjection is guaranteed to exist since the MaterializationSpec has already been
	// validated.
	var existingProjection = existing.GetProjection(field)
	var proposedProjection = proposed.GetProjection(field)
	if proposedProjection == nil {
		return "The proposed materialization is missing the projection, which is required because it's included in the existing materialization"
	}

	// Ensure that the possible types of the proposed are a subset of the existing possible types.
	// The new projection is allowed to contain fewer types than the original, though, since that
	// will always work with the original database schema.
	for _, pt := range proposedProjection.Inference.Types {
		if !sliceContains(pt, existingProjection.Inference.Types) {
			return fmt.Sprintf("The proposed projection may contain the type '%s', which is not part of the original projection", pt)
		}
	}

	// If the existing projection must exist, then so must the proposed. This is because this field
	// is used to determine whether a column may contain nulls. So if the existing column cannot
	// contain null, then we can't allow the new projection to possible be null. But if the existing
	// column is nullable, then it won't matter if the new one is or not since the column will be
	// unconstrained.
	if existingProjection.Inference.Exists == pf.Inference_MUST &&
		!sliceContains("null", existingProjection.Inference.Types) &&
		proposedProjection.Inference.Exists != pf.Inference_MUST {
		return "The existing projection must exist and be non-null, so the new projection must also exist"
	}
	return ""
}

func sliceContains(expected string, actual []string) bool {
	for _, ty := range actual {
		if ty == expected {
			return true
		}
	}
	return false
}
