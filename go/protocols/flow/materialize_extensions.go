package flow

import (
	"bytes"
	"encoding/json"
	"sort"

	pb "go.gazette.dev/core/broker/protocol"
)

// Materialization is a type wrapper for Materialization catalog task names.
type Materialization string

// String returns the Materialization name as a string.
func (m Materialization) String() string { return string(m) }

// Validate returns an error if the Materialization is malformed.
func (m Materialization) Validate() error {
	return pb.ValidateToken(m.String(), pb.TokenSymbols, 1, 512)
}

// AllFields returns the complete set of all the fields as a single string slice. All the keys
// fields will be ordered first, in the same order as they appear in Keys, followed by all the
// Values fields in the same order, with the root document field coming last.
func (fields *FieldSelection) AllFields() []string {
	var all = make([]string, 0, len(fields.Keys)+len(fields.Values)+1)
	all = append(all, fields.Keys...)
	all = append(all, fields.Values...)
	if fields.Document != "" {
		all = append(all, fields.Document)
	}
	return all
}

// Validate returns an error if the FieldSelection is malformed.
func (fields *FieldSelection) Validate() error {
	if !sort.StringsAreSorted(fields.Values) {
		return pb.NewValidationError("Values must be sorted")
	}
	return nil
}

// Equal returns true if this FieldSelection is deeply equal to the other.
func (fields *FieldSelection) Equal(other *FieldSelection) bool {
	if other == nil {
		return fields == nil
	}

	if len(fields.Keys) != len(other.Keys) {
		return false
	}
	for i := range fields.Keys {
		if fields.Keys[i] != other.Keys[i] {
			return false
		}
	}
	if len(fields.Values) != len(other.Values) {
		return false
	}
	for i := range fields.Values {
		if fields.Values[i] != other.Values[i] {
			return false
		}
	}
	if fields.Document != other.Document {
		return false
	}
	if len(fields.FieldConfigJsonMap) != len(other.FieldConfigJsonMap) {
		return false
	}
	for key := range fields.FieldConfigJsonMap {
		if string(fields.FieldConfigJsonMap[key]) != string(other.FieldConfigJsonMap[key]) {
			return false
		}
	}
	return bytes.Equal(fields.XXX_unrecognized, other.XXX_unrecognized)
}

// Validate returns an error if the MaterializationSpec is malformed.
func (m *MaterializationSpec) Validate() error {
	if err := m.Name.Validate(); err != nil {
		return pb.ExtendContext(err, "Materialization")
	} else if _, ok := MaterializationSpec_ConnectorType_name[int32(m.ConnectorType)]; !ok {
		return pb.NewValidationError("unknown ConnectorType %v", m.ConnectorType)
	} else if len(m.ConfigJson) == 0 {
		return pb.NewValidationError("missing ConfigJson")
	}

	for i := range m.Bindings {
		if err := m.Bindings[i].Validate(); err != nil {
			return pb.ExtendContext(err, "Bindings[%d]", i)
		}
	}
	if err := m.ShardTemplate.Validate(); err != nil {
		return pb.ExtendContext(err, "ShardTemplate")
	} else if err := m.RecoveryLogTemplate.Validate(); err != nil {
		return pb.ExtendContext(err, "RecoveryLogTemplate")
	}
	return nil
}

// Validate returns an error if the MaterializationSpec_Binding is malformed.
func (m *MaterializationSpec_Binding) Validate() error {
	if err := m.Collection.Validate(); err != nil {
		return pb.ExtendContext(err, "Collection")
	} else if len(m.ResourceConfigJson) == 0 {
		return pb.NewValidationError("missing ResourceConfigJson")
	} else if err = m.FieldSelection.Validate(); err != nil {
		return pb.ExtendContext(err, "FieldSelection")
	} else if len(m.ResourcePath) == 0 {
		return pb.NewValidationError("missing ResourcePath")
	} else if err = m.PartitionSelector.Validate(); err != nil {
		return pb.ExtendContext(err, "PartitionSelector")
	}

	if m.DeprecatedShuffle != nil {
		if err := m.DeprecatedShuffle.PartitionSelector.Validate(); err != nil {
			return pb.ExtendContext(err, "DeprecatedShuffle.PartitionSelector")
		}
	}

	for i, p := range m.ResourcePath {
		if len(p) == 0 {
			return pb.ExtendContext(
				pb.NewValidationError("missing value"), "ResourcePath[%d]", i)
		}
	}

	// Validate that all fields reference extant projections.
	for _, field := range m.FieldSelection.AllFields() {
		if m.Collection.GetProjection(field) == nil {
			return pb.NewValidationError("the selected field '%s' has no corresponding projection", field)
		}
	}
	return nil
}

// FieldValuePtrs returns the projection pointers of the contianed FieldSelection.Values.
func (m *MaterializationSpec_Binding) FieldValuePtrs() []string {
	var out []string

	for _, field := range m.FieldSelection.Values {
		out = append(out, m.Collection.GetProjection(field).Ptr)
	}
	return out
}

func (m *MaterializationSpec) InvokeConfig() (*json.RawMessage, string) {
	return &m.ConfigJson, m.ConnectorType.String()
}
