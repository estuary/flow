package flow

import (
	"sort"

	pb "go.gazette.dev/core/broker/protocol"
)

// Materialization names a specified catalog materialization.
type Materialization string

// String returns the Materialization name as a string.
func (m Materialization) String() string { return string(m) }

// AllFields returns the complete set of all the fields as a single string slice. All the keys
// fields will be ordered first, in the same order as they appear in Keys, followed by all the
// Values fields in the same order, with the root document field coming last.
func (fields *FieldSelection) AllFields() []string {
	var all = make([]string, 0, len(fields.Keys)+len(fields.Values)+1)
	all = append(all, fields.Keys...)
	all = append(all, fields.Values...)
	return append(all, fields.Document)
}

// Validate returns an error if the FieldSelection is malformed.
func (fields *FieldSelection) Validate() error {
	if !sort.StringsAreSorted(fields.Values) {
		return pb.NewValidationError("Values must be sorted")
	}
	return nil
}

// Validate returns an error if the MaterializationSpec is malformed.
func (m *MaterializationSpec) Validate() error {
	if err := m.Collection.Validate(); err != nil {
		return pb.ExtendContext(err, "Collection")
	} else if _, ok := EndpointType_name[int32(m.EndpointType)]; !ok {
		return pb.NewValidationError("unknown EndpointType %v", m.EndpointType)
	} else if err = m.FieldSelection.Validate(); err != nil {
		return pb.ExtendContext(err, "FieldSelection")
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
func (m *MaterializationSpec) FieldValuePtrs() []string {
	var out []string

	for _, field := range m.FieldSelection.Values {
		out = append(out, m.Collection.GetProjection(field).Ptr)
	}
	return out
}
