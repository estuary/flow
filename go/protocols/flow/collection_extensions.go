package flow

import "fmt"

// TODO: change references to this to use GetProjection instead
// GetProjectionByField finds the projection with the given field name, or nil if one does not exist
func GetProjectionByField(field string, projections []*Projection) *Projection {
	for _, proj := range projections {
		if proj.Field == field {
			return proj
		}
	}
	return nil
}

// GetProjection finds the projection with the given field name, or nil if one does not exist
func (spec *CollectionSpec) GetProjection(field string) *Projection {
	return GetProjectionByField(field, spec.Projections)
}

// Validates the CollectionSpec and returns an error if it is invalid. If this returns nil, then the
// spec is valid.
// TODO: this validation could be a lot more thorough. It's just hitting what seems like the most
// likely suspects now.
func (spec *CollectionSpec) Validate() error {
	if len(spec.KeyPtrs) == 0 {
		return fmt.Errorf("collection '%s' has no key pointers", spec.Name)
	}
	var keyPointers = make(map[string]int)
	for _, ptr := range spec.KeyPtrs {
		keyPointers[ptr] = 0
	}
	for _, proj := range spec.Projections {
		if len(proj.Field) == 0 {
			return fmt.Errorf("projection has no field")
		}
		if proj.Inference == nil {
			return fmt.Errorf("projection '%s' is missing inference", proj.Field)
		}
		if len(proj.Inference.Types) == 0 {
			return fmt.Errorf("projection '%s' is missing type information", proj.Field)
		}
		if proj.IsPrimaryKey {
			keyPointers[proj.Ptr]++
		}
	}
	// ensure that the key information is consistent
	for ptr, n := range keyPointers {
		if n == 0 {
			return fmt.Errorf("collection key '%s' has no projections", ptr)
		}
	}

	return nil // gudenuf
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
