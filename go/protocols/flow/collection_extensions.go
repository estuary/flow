package flow

import (
	pb "go.gazette.dev/core/broker/protocol"
)

// GetProjectionByField finds the projection with the given field name, or nil if one does not exist
// TODO: change references to this to use GetProjection instead
func GetProjectionByField(field string, projections []*Projection) *Projection {
	for _, proj := range projections {
		if proj.Field == field {
			return proj
		}
	}
	return nil
}

// GetProjection finds the projection with the given field name, or nil if one does not exist
func (m *CollectionSpec) GetProjection(field string) *Projection {
	return GetProjectionByField(field, m.Projections)
}

// Validate returns an error if the CollectionSpec is invalid.
func (m *CollectionSpec) Validate() error {
	var keyPointers = make(map[string]struct{})

	for i, proj := range m.Projections {
		var err error
		if proj.Field == "" {
			err = pb.NewValidationError("missing field")
		}
		if proj.Inference == nil {
			return pb.NewValidationError("missing inferences")
		}
		if len(proj.Inference.Types) == 0 {
			return pb.NewValidationError("missing inference types")
		}
		if err != nil {
			return pb.ExtendContext(err, "Projections[%d]", i)
		}

		if proj.IsPrimaryKey {
			keyPointers[proj.Ptr] = struct{}{}
		}
	}

	if m.SchemaUri == "" {
		return pb.NewValidationError("missing schema URI")
	}
	if len(m.KeyPtrs) == 0 {
		return pb.NewValidationError("key pointers are empty")
	}
	for _, p := range m.KeyPtrs {
		if _, ok := keyPointers[p]; !ok {
			return pb.NewValidationError("no keyed projection for key pointer %q", p)
		}
	}

	return nil
}

// Validate returns an error if the DerivationSpec is invalid.
func (m *DerivationSpec) Validate() error {
	if err := m.Collection.Validate(); err != nil {
		return pb.ExtendContext(err, "Collection")
	}
	if m.RegisterSchemaUri == "" {
		return pb.NewValidationError("missing RegisterSchemaUri")
	}
	if m.RegisterInitialJson == "" {
		return pb.NewValidationError("missing RegisterInitialJson")
	}
	for i, tf := range m.Transforms {
		if err := tf.Validate(); err != nil {
			return pb.ExtendContext(err, "Transform[%d]", i)
		}
	}

	return nil
}

// Validate returns an error if the Rule is invalid.
func (m *JournalRules_Rule) Validate() error {
	if m.Rule == "" {
		return pb.NewValidationError("missing Rule")
	}
	if err := m.Selector.Validate(); err != nil {
		return pb.ExtendContext(err, "Selector")
	}

	// We cannot validate templates because, by design,
	// they are only partial specifications.

	return nil
}

// Validate returns an error if the Rules are invalid.
func (m *JournalRules) Validate() error {
	for i, r := range m.Rules {
		if err := r.Validate(); err != nil {
			return pb.ExtendContext(err, "Rules[%d]", i)
		}
	}
	return nil
}

// IsSingleType returns true if this projection may only hold a single type besides null For
// example, if the types are ["string", "null"] or just ["string"], then this would return true.
func (projection *Projection) IsSingleType() bool {
	var nTypes = 0
	for _, ty := range projection.Inference.Types {
		if ty != "null" {
			nTypes++
		}
	}
	return nTypes == 1
}

// IsRootDocumentProjection returns true only if this is a projection of the entire document,
// meaning that the json pointer is the empty string.
func (projection *Projection) IsRootDocumentProjection() bool {
	return len(projection.Ptr) == 0
}

// IsSingleScalarType returns true if this projection may hold a single scalar type besides null.
func (projection *Projection) IsSingleScalarType() bool {
	var types = projection.Inference.Types
	var isScalar = false
	var nTypes = 0
	for _, ty := range types {
		switch ty {
		case "null":
		case "integer", "number", "boolean", "string":
			isScalar = true
			nTypes++
		default:
			nTypes++
		}
	}
	return isScalar && nTypes == 1
}
