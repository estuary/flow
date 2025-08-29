package flow

import (
	"encoding/json"
	"sort"
	"strings"

	pb "go.gazette.dev/core/broker/protocol"
)

// GetProjection finds the projection with the given field name, or nil if one does not exist
func (m *CollectionSpec) GetProjection(field string) *Projection {
	var index = sort.Search(len(m.Projections), func(index int) bool {
		return m.Projections[index].Field >= field
	})
	if index != len(m.Projections) && m.Projections[index].Field == field {
		return &m.Projections[index]
	}
	return nil
}

// GetReadSchemaJson returns the effective JSON schema for collection reads.
func (m *CollectionSpec) GetReadSchemaJson() json.RawMessage {
	if len(m.ReadSchemaJson) != 0 {
		return m.ReadSchemaJson
	}
	return m.WriteSchemaJson
}

// Validate returns an error if the CollectionSpec is invalid.
func (m *CollectionSpec) Validate() error {
	if err := m.Name.Validate(); err != nil {
		return pb.ExtendContext(err, "Collection")
	}

	var keyPointers = make(map[string]struct{})

	for i, proj := range m.Projections {
		var err error

		if proj.Field == "" {
			err = pb.NewValidationError("missing field")
		} else if err2 := proj.Inference.Validate(); err != nil {
			err = err2
		} else if i != 0 && proj.Field <= m.Projections[i-1].Field {
			err = pb.NewValidationError("projections are not in Field order")
		}

		if err != nil {
			return pb.ExtendContext(err, "Projections[%d]", i)
		}

		if proj.IsPrimaryKey {
			keyPointers[proj.Ptr] = struct{}{}
		}
	}

	if len(m.Key) == 0 {
		return pb.NewValidationError("key pointers are empty")
	}
	for _, p := range m.Key {
		if _, ok := keyPointers[p]; !ok {
			return pb.NewValidationError("no keyed projection for key pointer %q", p)
		}
	}
	for i, field := range m.PartitionFields {
		var err error
		if p := m.GetProjection(field); p == nil {
			err = pb.NewValidationError("no projection for field %q", field)
		} else if !p.IsPartitionKey {
			err = pb.NewValidationError("projection is not a partition key")
		}
		if err != nil {
			return pb.ExtendContext(err, "PartitionFields[%d]", i)
		}
	}
	if err := m.PartitionTemplate.Validate(); err != nil {
		return pb.ExtendContext(err, "PartitionTemplate")
	}

	return nil
}

// Validate returns an error if the Derivation is invalid.
func (m *CollectionSpec_Derivation) Validate() error {
	for i, tf := range m.Transforms {
		if err := tf.Validate(); err != nil {
			return pb.ExtendContext(err, "Transform[%d]", i)
		}
	}
	if err := m.ShardTemplate.Validate(); err != nil {
		return pb.ExtendContext(err, "ShardTemplate")
	} else if err := m.RecoveryLogTemplate.Validate(); err != nil {
		return pb.ExtendContext(err, "RecoveryLogTemplate")
	}
	return nil
}

// Validate returns an error if the Transform is invalid.
func (m *CollectionSpec_Derivation_Transform) Validate() error {
	if err := m.Name.Validate(); err != nil {
		return pb.ExtendContext(err, "Name")
	} else if err := m.Collection.Validate(); err != nil {
		return pb.ExtendContext(err, "Collection")
	} else if err := m.PartitionSelector.Validate(); err != nil {
		return pb.ExtendContext(err, "PartitionSelector")
	} else if len(m.LambdaConfigJson) == 0 {
		return pb.ExtendContext(err, "missing LambdaConfigJson")
	}
	return nil
}

// IsRootDocumentProjection returns true only if this is a projection of the entire document,
// meaning that the json pointer is the empty string.
func (projection *Projection) IsRootDocumentProjection() bool {
	return len(projection.Ptr) == 0
}

// IsRootLevelProjection returns true if this projection represents a root-level 
// property of the document
func (projection *Projection) IsRootLevelProjection() bool {
	return strings.Count(projection.Ptr, "/") == 1
}

// IsSingleType returns true if this inference may only hold a single type besides null For
// example, if the types are ["string", "null"] or just ["string"], then this would return true.
func (i *Inference) IsSingleType() bool {
	var nTypes = 0
	for _, ty := range i.Types {
		if ty != JsonTypeNull {
			nTypes++
		}
	}
	return nTypes == 1
}

// IsSingleScalarType returns true if this inference may hold a single scalar type besides null.
func (i *Inference) IsSingleScalarType() bool {
	var isScalar = false
	var nTypes = 0
	for _, ty := range i.Types {
		switch ty {
		case JsonTypeNull:
		case JsonTypeInteger, JsonTypeNumber, JsonTypeBoolean, JsonTypeString:
			isScalar = true
			nTypes++
		default:
			nTypes++
		}
	}
	return isScalar && nTypes == 1
}

// Type_ constants for each type name used in JSON schemas.
const (
	JsonTypeNull    = "null"
	JsonTypeInteger = "integer"
	JsonTypeNumber  = "number"
	JsonTypeBoolean = "boolean"
	JsonTypeString  = "string"
	JsonTypeObject  = "object"
	JsonTypeArray   = "array"
)

// Validate returns an error if the Inference is invalid.
func (i *Inference) Validate() error {
	return nil
}
