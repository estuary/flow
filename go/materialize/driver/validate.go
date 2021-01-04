package driver

import (
	"fmt"

	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

type MaterializationSpec struct {
	Fields         pm.FieldSelection `json:"fields"`
	CollectionSpec pf.CollectionSpec `json:"collectionSpec"`
}

func ValidateSelectedFields(constraints map[string]*pm.Constraint, proposed *MaterializationSpec) error {
	// Track all the location pointers for each included field so that we can verify all the
	// LOCATION_REQUIRED constraints are met.
	var includedPointers = make(map[string]bool)

	// Does each field in the materialization have an allowable constraint?
	var allFields = proposed.Fields.AllFields()
	for _, field := range allFields {
		var projection = proposed.CollectionSpec.GetProjection(field)
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
		case pm.Constraint_FIELD_REQUIRED:
			if !sliceContains(field, allFields) {
				return fmt.Errorf("Required field '%s' is missing. It is required because: %s", field, constraint.Reason)
			}
		case pm.Constraint_LOCATION_REQUIRED:
			var projection = proposed.CollectionSpec.GetProjection(field)
			if !includedPointers[projection.Ptr] {
				return fmt.Errorf("The materialization must include a projections of location '%s', but no such projection is included", projection.Ptr)
			}
		}
	}

	return nil
}

func ValidateNewSqlProjections(proposed *pf.CollectionSpec) map[string]*pm.Constraint {
	var constraints = make(map[string]*pm.Constraint)
	for _, projection := range proposed.Projections {
		var constraint = new(pm.Constraint)
		switch {
		case projection.IsPrimaryKey:
			constraint.Type = pm.Constraint_LOCATION_REQUIRED
			constraint.Reason = "All Locations that are part of the collections key are required"
		case projection.IsRootDocumentProjection():
			constraint.Type = pm.Constraint_LOCATION_REQUIRED
			constraint.Reason = "The root document must be materialized"
		case projection.IsSingleScalarType():
			constraint.Type = pm.Constraint_LOCATION_RECOMMENDED
			constraint.Reason = "The projection has a single scalar type"

		case projection.IsSingleType():
			constraint.Type = pm.Constraint_FIELD_OPTIONAL
			constraint.Reason = "This field is able to be materialized"
		default:
			// If we got here, then either the field may have multiple types, or the only possible
			// type is "null". In either case, we're not going to allow it. Technically, we could
			// allow the null type to be materializaed, but I can't think of a use case where that
			// would be desirable.
			constraint.Type = pm.Constraint_FIELD_FORBIDDEN
			constraint.Reason = "Cannot materialize this field"
		}
		constraints[projection.Field] = constraint
	}
	return constraints
}

func ValidateMatchesExisting(existing *MaterializationSpec, proposed *pf.CollectionSpec) map[string]*pm.Constraint {
	var constraints = make(map[string]*pm.Constraint)
	for _, field := range existing.Fields.AllFields() {
		var constraint = new(pm.Constraint)
		var typeError = checkTypeError(field, &existing.CollectionSpec, proposed)
		if len(typeError) > 0 {
			constraint.Type = pm.Constraint_UNSATISFIABLE
			constraint.Reason = typeError
		} else {
			constraint.Type = pm.Constraint_FIELD_REQUIRED
			constraint.Reason = "This field is part of the current materialization"
		}

		constraints[field] = constraint
	}
	// We'll loop through the proposed projections and forbid any that aren't already in our map.
	// This is done solely so that we can supply a descriptive reason, since any fields we fail to
	// mention are implicitly forbidden.
	for _, proj := range proposed.Projections {
		if _, ok := constraints[proj.Field]; !ok {
			var constraint = new(pm.Constraint)
			constraint.Type = pm.Constraint_FIELD_FORBIDDEN
			constraint.Reason = "This field is not included in the existing materialization."
			constraints[proj.Field] = constraint
		}
	}

	return constraints
}

func checkTypeError(field string, existing *pf.CollectionSpec, proposed *pf.CollectionSpec) string {
	var e = existing.GetProjection(field)
	// The projection will always exist in the existing spec unless we've made a grave programming
	// error or someone has manually modified the database and screwed it up.
	if e == nil {
		// TODO: log something
		return "The materialization spec is invalid. It is missing a projection for this field."
	}

	var p = proposed.GetProjection(field)
	if p == nil {
		return "The proposed materialization is missing the projection, which is required because it's included in the existing materialization"
	}

	// Ensure that the possible types of the proposed are a subset of the existing possible types.
	// The new projection is allowed to contain fewer types than the original, though, since that
	// will always work with the original database schema.
	for _, pt := range p.Inference.Types {
		if !sliceContains(pt, e.Inference.Types) {
			return fmt.Sprintf("The proposed projection may contain the type '%s', which is not part of the original projection", pt)
		}
	}

	// If the existing projection must exist, then so must the proposed. This is because this field
	// is used to determine whether a column may contain nulls. So if the existing column cannot
	// contain null, then we can't allow the new projection to possible be null. But if the existing
	// column is nullable, then it won't matter if the new one is or not since the column will be
	// unconstrained.
	if e.Inference.MustExist && !sliceContains("null", e.Inference.Types) && !p.Inference.MustExist {
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
