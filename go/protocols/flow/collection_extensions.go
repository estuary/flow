package flow

import (
	"encoding/json"
	"sort"

	pb "go.gazette.dev/core/broker/protocol"
)

// A spec-carrying message is in *indirect* form when its LinkedCollections table
// is non-empty: every binding or transform of that message then leaves
// Collection zero-valued and names its collection through CollectionIndex.
// An empty table means every binding inlines its own Collection, as it always
// has. The form is a property of the message as a whole, so validation and
// resolution branch on the table, never on a per-binding presence check
// (gogoproto.nullable=false means "unset" is indistinguishable from a
// zero-valued Collection).

// ValidateLinkedCollections validates each entry of a LinkedCollections table.
// Entries need not be unique or ordered: the control-plane builder emits them
// unique-by-value and ordered on name, but that's a convention rather than an
// invariant of the message.
func ValidateLinkedCollections(linked []CollectionSpec) error {
	for i := range linked {
		if err := linked[i].Validate(); err != nil {
			return pb.ExtendContext(err, "LinkedCollections[%d]", i)
		}
	}
	return nil
}

// ValidateBindingCollection checks that one binding or transform agrees with the
// form of its parent message: in indirect form it must index the table and leave
// its inlined collection zero-valued, and in inline form it must leave
// CollectionIndex unset.
func ValidateBindingCollection(numLinked int, inlinedSize int, index uint32) error {
	if numLinked == 0 {
		if index != 0 {
			return pb.NewValidationError(
				"CollectionIndex is %d but the spec has no LinkedCollections", index)
		}
		return nil
	}
	if inlinedSize != 0 {
		return pb.NewValidationError(
			"Collection is set but the spec is indirected (use CollectionIndex)")
	} else if int(index) >= numLinked {
		return pb.NewValidationError(
			"CollectionIndex %d is out of range (%d LinkedCollections)", index, numLinked)
	}
	return nil
}

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

// TransformCollection returns the source CollectionSpec of a transform of this
// Derivation, resolving it through LinkedCollections if this Derivation is in
// indirect form. It returns nil if the transform cannot be resolved, which
// Validate() rejects. Only transform *sources* are ever indirected; the
// derived collection which owns this Derivation is not.
func (m *CollectionSpec_Derivation) TransformCollection(t *CollectionSpec_Derivation_Transform) *CollectionSpec {
	if len(m.LinkedCollections) == 0 {
		return &t.Collection
	} else if int(t.CollectionIndex) >= len(m.LinkedCollections) {
		return nil
	}
	return &m.LinkedCollections[t.CollectionIndex]
}

// Validate returns an error if the Derivation is invalid.
func (m *CollectionSpec_Derivation) Validate() error {
	if err := ValidateLinkedCollections(m.LinkedCollections); err != nil {
		return err
	}
	for i := range m.Transforms {
		if err := m.Transforms[i].validate(m); err != nil {
			return pb.ExtendContext(err, "Transform[%d]", i)
		}
	}
	// Inactive transforms index the same table as their active peers, so their
	// encoding form must agree. Their other fields are not validated, matching
	// the long-standing behavior of this method.
	for i, tf := range m.InactiveTransforms {
		if err := ValidateBindingCollection(
			len(m.LinkedCollections), tf.Collection.ProtoSize(), tf.CollectionIndex,
		); err != nil {
			return pb.ExtendContext(err, "InactiveTransform[%d]", i)
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
// It validates the transform's inlined Collection, and so is meaningful only
// for a transform of an inline-form Derivation. An indirect-form transform must
// instead be validated through its parent, which resolves its collection.
func (m *CollectionSpec_Derivation_Transform) Validate() error {
	if err := m.Collection.Validate(); err != nil {
		return pb.ExtendContext(err, "Collection")
	}
	return m.validateSelf()
}

// validate checks this transform within the context of its parent Derivation,
// which determines whether the transform inlines its source collection or
// indexes the parent's LinkedCollections.
func (m *CollectionSpec_Derivation_Transform) validate(parent *CollectionSpec_Derivation) error {
	if err := ValidateBindingCollection(
		len(parent.LinkedCollections), m.Collection.ProtoSize(), m.CollectionIndex,
	); err != nil {
		return err
	} else if len(parent.LinkedCollections) == 0 {
		if err := m.Collection.Validate(); err != nil {
			return pb.ExtendContext(err, "Collection")
		}
	}
	return m.validateSelf()
}

func (m *CollectionSpec_Derivation_Transform) validateSelf() error {
	if err := m.Name.Validate(); err != nil {
		return pb.ExtendContext(err, "Name")
	} else if err := m.PartitionSelector.Validate(); err != nil {
		return pb.ExtendContext(err, "PartitionSelector")
	} else if len(m.LambdaConfigJson) == 0 {
		return pb.NewValidationError("missing LambdaConfigJson")
	}
	return nil
}

// IsRootDocumentProjection returns true only if this is a projection of the entire document,
// meaning that the json pointer is the empty string.
func (projection *Projection) IsRootDocumentProjection() bool {
	return len(projection.Ptr) == 0
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
