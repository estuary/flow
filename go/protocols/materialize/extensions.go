package materialize

import (
	"bytes"

	"github.com/gogo/protobuf/jsonpb"
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

// ExplicitZeroCheckpoint is a zero-valued message encoding,
// implemented as a trivial encoding of the max-value 2^29-1 protobuf
// tag with boolean true. See TransactionResponse_Opened.FlowCheckpoint.
var ExplicitZeroCheckpoint = []byte{0xf8, 0xff, 0xff, 0xff, 0xf, 0x1}

func (m *ValidateRequest) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	var err = (&jsonpb.Marshaler{}).Marshal(&b, m)
	return b.Bytes(), err
}

func (m *ValidateRequest) UnmarshalJSON(b []byte) error {
	return jsonpb.Unmarshal(bytes.NewReader(b), m)
}

func (m *ValidateResponse) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	var err = (&jsonpb.Marshaler{}).Marshal(&b, m)
	return b.Bytes(), err
}

func (m *ValidateResponse) UnmarshalJSON(b []byte) error {
	return jsonpb.Unmarshal(bytes.NewReader(b), m)
}

func (m *ApplyRequest) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	var err = (&jsonpb.Marshaler{}).Marshal(&b, m)
	return b.Bytes(), err
}

func (m *ApplyRequest) UnmarshalJSON(b []byte) error {
	return jsonpb.Unmarshal(bytes.NewReader(b), m)
}

func (m *ApplyResponse) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	var err = (&jsonpb.Marshaler{}).Marshal(&b, m)
	return b.Bytes(), err
}

func (m *ApplyResponse) UnmarshalJSON(b []byte) error {
	return jsonpb.Unmarshal(bytes.NewReader(b), m)
}

func (m *TransactionRequest_Open) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	var err = (&jsonpb.Marshaler{}).Marshal(&b, m)
	return b.Bytes(), err
}

func (m *TransactionRequest_Open) UnmarshalJSON(b []byte) error {
	return jsonpb.Unmarshal(bytes.NewReader(b), m)
}

func (m *TransactionResponse_Opened) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	var err = (&jsonpb.Marshaler{}).Marshal(&b, m)
	return b.Bytes(), err
}

func (m *TransactionResponse_Opened) UnmarshalJSON(b []byte) error {
	return jsonpb.Unmarshal(bytes.NewReader(b), m)
}
