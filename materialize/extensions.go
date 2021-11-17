package materialize

import (
	"bytes"

	pf "github.com/estuary/protocols/flow"
	"github.com/gogo/protobuf/jsonpb"
	pb "go.gazette.dev/core/broker/protocol"
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

// Validate returns an error if the SpecRequest isn't well-formed.
func (m *SpecRequest) Validate() error {
	if _, ok := pf.EndpointType_name[int32(m.EndpointType)]; !ok {
		return pb.NewValidationError("unknown EndpointType %v", m.EndpointType)
	}
	return nil
}

// Validate returns an error if the SpecResponse isn't well-formed.
func (m *SpecResponse) Validate() error {
	if len(m.EndpointSpecSchemaJson) == 0 {
		return pb.NewValidationError("missing EndpointSpecSchemaJson")
	} else if len(m.ResourceSpecSchemaJson) == 0 {
		return pb.NewValidationError("missing ResourceSpecSchemaJson")
	} else if m.DocumentationUrl == "" {
		return pb.NewValidationError("missing DocumentationUrl")
	}
	return nil
}

// Validate returns an error if the ValidateRequest isn't well-formed.
func (m *ValidateRequest) Validate() error {
	if err := m.Materialization.Validate(); err != nil {
		return pb.ExtendContext(err, "Materialization")
	} else if _, ok := pf.EndpointType_name[int32(m.EndpointType)]; !ok {
		return pb.NewValidationError("unknown EndpointType %v", m.EndpointType)
	} else if len(m.EndpointSpecJson) == 0 {
		return pb.NewValidationError("missing EndpointSpecJson")
	}

	for i := range m.Bindings {
		if err := m.Bindings[i].Validate(); err != nil {
			return pb.ExtendContext(err, "Bindings[%d]", i)
		}
	}
	return nil
}

// Validate returns an error if the ValidateRequest_Binding isn't well-formed.
func (m *ValidateRequest_Binding) Validate() error {
	if err := m.Collection.Validate(); err != nil {
		return pb.ExtendContext(err, "Collection")
	} else if len(m.ResourceSpecJson) == 0 {
		return pb.NewValidationError("missing EndpointSpecJson")
	}
	return nil
}

func (m *ValidateRequest) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	var err = (&jsonpb.Marshaler{}).Marshal(&b, m)
	return b.Bytes(), err
}

func (m *ValidateRequest) UnmarshalJSON(b []byte) error {
	return jsonpb.Unmarshal(bytes.NewReader(b), m)
}

// Validate returns an error if the ValidateResponse isn't well-formed.
func (m *ValidateResponse) Validate() error {
	for i := range m.Bindings {
		if err := m.Bindings[i].Validate(); err != nil {
			return pb.ExtendContext(err, "Bindings[%d]", i)
		}
	}
	return nil
}

// Validate returns an error if the ValidateResponse_Binding isn't well-formed.
func (m *ValidateResponse_Binding) Validate() error {
	for field, constraint := range m.Constraints {
		if _, ok := Constraint_Type_name[int32(constraint.Type)]; !ok {
			return pb.ExtendContext(
				pb.NewValidationError("unknown Constraint Type %v", constraint),
				"Constraints[%s]", field)
		}
	}
	if len(m.ResourcePath) == 0 {
		return pb.NewValidationError("missing ResourcePath")
	}
	for i, p := range m.ResourcePath {
		if len(p) == 0 {
			return pb.ExtendContext(
				pb.NewValidationError("missing value"), "ResourcePath[%d]", i)
		}
	}
	return nil
}

func (m *ValidateResponse) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	var err = (&jsonpb.Marshaler{}).Marshal(&b, m)
	return b.Bytes(), err
}

func (m *ValidateResponse) UnmarshalJSON(b []byte) error {
	return jsonpb.Unmarshal(bytes.NewReader(b), m)
}

// Validate returns an error if the ApplyRequest is malformed.
func (m *ApplyRequest) Validate() error {
	if err := m.Materialization.Validate(); err != nil {
		return pb.ExtendContext(err, "Materialization")
	}

	// DryRun cannot have a validation error.
	return nil
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

// Validate returns an error if the message is not well-formed.
func (m *TransactionRequest) Validate() error {
	var count int
	if m.Open != nil {
		if err := m.Open.Validate(); err != nil {
			return pb.ExtendContext(err, "Open")
		}
		count += 1
	}
	if m.Load != nil {
		if err := m.Load.Validate(); err != nil {
			return pb.ExtendContext(err, "Load")
		}
		count += 1
	}
	if m.Prepare != nil {
		if err := m.Prepare.Validate(); err != nil {
			return pb.ExtendContext(err, "Prepare")
		}
		count += 1
	}
	if m.Store != nil {
		if err := m.Store.Validate(); err != nil {
			return pb.ExtendContext(err, "Store")
		}
		count += 1
	}
	if m.Commit != nil {
		if err := m.Commit.Validate(); err != nil {
			return pb.ExtendContext(err, "Commit")
		}
		count += 1
	}
	if m.Acknowledge != nil {
		if err := m.Acknowledge.Validate(); err != nil {
			return pb.ExtendContext(err, "Acknowledge")
		}
		count += 1
	}

	if count != 1 {
		return pb.NewValidationError("expected one of Open, Load, Prepare, Store, Commit, Acknowledge")
	}
	return nil
}

// Validate returns an error if the message is not well-formed.
func (m *TransactionRequest_Open) Validate() error {
	if err := m.Materialization.Validate(); err != nil {
		return pb.ExtendContext(err, "Materialization")
	} else if m.Version == "" {
		return pb.NewValidationError("expected Version")
	} else if m.KeyBegin > m.KeyEnd {
		return pb.NewValidationError("invalid KeyBegin / KeyEnd range")
	}
	// DriverCheckpointJson may be empty.
	return nil
}

// Validate returns an error if the message is not well-formed.
func (m *TransactionRequest_Load) Validate() error {
	if len(m.PackedKeys) == 0 {
		return pb.NewValidationError("expected PackedKeys")
	}
	return nil
}

// Validate returns an error if the message is not well-formed.
func (m *TransactionRequest_Prepare) Validate() error {
	if len(m.FlowCheckpoint) == 0 {
		return pb.NewValidationError("expected FlowCheckpoint")
	}
	return nil
}

// Validate returns an error if the message is not well-formed.
func (m *TransactionRequest_Store) Validate() error {
	if ll := len(m.DocsJson); ll == 0 {
		return pb.NewValidationError("expected DocsJson")
	} else if lr := len(m.PackedKeys); ll != lr {
		return pb.NewValidationError("expected PackedKeys length to match DocsJson: %d vs %d", ll, lr)
	} else if lr = len(m.PackedValues); ll != lr {
		return pb.NewValidationError("expected PackedValues length to match DocsJson: %d vs %d", ll, lr)
	} else if lr := len(m.Exists); ll != lr {
		return pb.NewValidationError("expected Exists length to match DocsJson: %d vs %d", ll, lr)
	}
	return nil
}

// Validate is a no-op.
func (m *TransactionRequest_Commit) Validate() error {
	return nil
}

// Validate is a no-op.
func (m *TransactionRequest_Acknowledge) Validate() error {
	return nil
}

// Validate returns an error if the message is not well-formed.
func (m *TransactionResponse) Validate() error {
	var count int
	if m.Opened != nil {
		if err := m.Opened.Validate(); err != nil {
			return pb.ExtendContext(err, "Opened")
		}
		count += 1
	}
	if m.Loaded != nil {
		if err := m.Loaded.Validate(); err != nil {
			return pb.ExtendContext(err, "Loaded")
		}
		count += 1
	}
	if m.Prepared != nil {
		if err := m.Prepared.Validate(); err != nil {
			return pb.ExtendContext(err, "Prepared")
		}
		count += 1
	}
	if m.DriverCommitted != nil {
		if err := m.DriverCommitted.Validate(); err != nil {
			return pb.ExtendContext(err, "DriverCommitted")
		}
		count += 1
	}
	if m.Acknowledged != nil {
		if err := m.Acknowledged.Validate(); err != nil {
			return pb.ExtendContext(err, "Acknowledged")
		}
		count += 1
	}

	if count != 1 {
		return pb.NewValidationError("expected one of Opened, Loaded, Prepared, DriverCommitted, Acknowledged")
	}
	return nil
}

// Validate returns an error if the message is not well-formed.
func (m *TransactionResponse_Opened) Validate() error {
	// FlowCheckpoint may be empty.
	return nil
}

// Validate returns an error if the message is not well-formed.
func (m *TransactionResponse_Loaded) Validate() error {
	if len(m.DocsJson) == 0 {
		return pb.NewValidationError("expected DocsJson")
	}
	return nil
}

// Validate returns an error if the message is not well-formed.
func (m *TransactionResponse_DriverCommitted) Validate() error {
	return nil
}

// Validate returns an error if the message is not well-formed.
func (m *TransactionResponse_Acknowledged) Validate() error {
	return nil
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
