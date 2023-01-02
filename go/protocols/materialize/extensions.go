package materialize

import (
	"encoding/json"

	pf "github.com/estuary/flow/go/protocols/flow"
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

func (m *SpecRequest) GetEndpointType() pf.EndpointType {
	return m.EndpointType
}
func (m *SpecRequest) GetEndpointSpecPtr() *json.RawMessage {
	return &m.EndpointSpecJson
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

func (m *ValidateRequest) GetEndpointType() pf.EndpointType {
	return m.EndpointType
}
func (m *ValidateRequest) GetEndpointSpecPtr() *json.RawMessage {
	return &m.EndpointSpecJson
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
		if constraint == nil {
			return pb.ExtendContext(
				pb.NewValidationError("Constraint is missing"), "Constraints[%s]", field)
		} else if _, ok := Constraint_Type_name[int32(constraint.Type)]; !ok {
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

// Validate returns an error if the ApplyRequest is malformed.
func (m *ApplyRequest) Validate() error {
	if err := m.Materialization.Validate(); err != nil {
		return pb.ExtendContext(err, "Materialization")
	}

	// DryRun cannot have a validation error.
	return nil
}

func (m *ApplyRequest) GetEndpointType() pf.EndpointType {
	return m.Materialization.EndpointType
}
func (m *ApplyRequest) GetEndpointSpecPtr() *json.RawMessage {
	return &m.Materialization.EndpointSpecJson
}

func (m *ApplyResponse) Validate() error {
	// No validations to do.
	return nil
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
	if m.Flush != nil {
		if err := m.Flush.Validate(); err != nil {
			return pb.ExtendContext(err, "Flush")
		}
		count += 1
	}
	if m.Store != nil {
		if err := m.Store.Validate(); err != nil {
			return pb.ExtendContext(err, "Store")
		}
		count += 1
	}
	if m.StartCommit != nil {
		if err := m.StartCommit.Validate(); err != nil {
			return pb.ExtendContext(err, "StartCommit")
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

// Validate returns an error if the message is malformed.
func (m *TransactionRequest_Flush) Validate() error {
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

// Validate returns an error if the message is malformed.
func (m *TransactionRequest_StartCommit) Validate() error {
	if len(m.RuntimeCheckpoint) == 0 {
		return pb.NewValidationError("expected RuntimeCheckpoint")
	}
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
	if m.Flushed != nil {
		if err := m.Flushed.Validate(); err != nil {
			return pb.ExtendContext(err, "Flushed")
		}
		count += 1
	}
	if m.StartedCommit != nil {
		if err := m.StartedCommit.Validate(); err != nil {
			return pb.ExtendContext(err, "StartedCommit")
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
func (m *TransactionResponse_StartedCommit) Validate() error {
	return nil
}

// Validate returns an error if the message is not well-formed.
func (m *TransactionResponse_Acknowledged) Validate() error {
	return nil
}
