package materialize

import (
	"reflect"
)

// IsForbidden returns true if the constraint type forbids inclusion in a materialization. This will
// return true for FIELD_FORBIDDEN and UNSATISFIABLE, and false for any other constraint type.
func (m *Constraint_Type) IsForbidden() bool {
	switch *m {
	case Constraint_FIELD_FORBIDDEN, Constraint_UNSATISFIABLE:
		return true
	default:
		return false
	}
}

// AllFields returns the complete set of all the fields as a single string slice. All the keys
// fields will be ordered first, in the same order as they appear in Keys, followed by all the
// Values fields in the same order, with the root document field coming last.
func (fields *FieldSelection) AllFields() []string {
	var allFields = make([]string, 0, len(fields.Keys)+len(fields.Values)+1)
	allFields = append(allFields, fields.Keys...)
	allFields = append(allFields, fields.Values...)
	return append(allFields, fields.Document)
}

// Equal returns true if the FieldSelections both represent the same selection of fields in the same
// order.
func (f FieldSelection) Equal(other *FieldSelection) bool {
	return reflect.DeepEqual(f.Keys, other.Keys) &&
		reflect.DeepEqual(f.Values, other.Values) &&
		f.Document == other.Document
}
