package materialize

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

// ExplicitZeroCheckpoint is a zero-valued message encoding,
// implemented as a trivial encoding of the max-value 2^29-1 protobuf
// tag with boolean true. See TransactionResponse_Opened.FlowCheckpoint.
var ExplicitZeroCheckpoint = []byte{0xf8, 0xff, 0xff, 0xff, 0xf, 0x1}
