package materialize

import (
	"encoding/json"

	pf "github.com/estuary/flow/go/protocols/flow"
	pb "go.gazette.dev/core/broker/protocol"
)

// IsForbidden returns true if the constraint type forbids inclusion in a materialization. This will
// return true for FIELD_FORBIDDEN, INCOMPATIBLE, and UNSATISFIABLE (deprecated alias for INCOMPATIBLE),
// and false for any other constraint type.
func (m *Response_Validated_Constraint_Type) IsForbidden() bool {
	switch *m {
	case Response_Validated_Constraint_FIELD_FORBIDDEN, Response_Validated_Constraint_INCOMPATIBLE, Response_Validated_Constraint_UNSATISFIABLE:
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
func (m *Request_Spec) Validate() error {
	// ConnectorType and ConfigJson are optional.
	return nil
}

// Validate returns an error if the SpecResponse isn't well-formed.
func (m *Response_Spec) Validate() error {
	if m.Protocol != 3032023 {
		return pb.NewValidationError("wrong Protocol (%d, should be 3032023)", m.Protocol)
	} else if len(m.ConfigSchemaJson) == 0 {
		return pb.NewValidationError("missing ConfigSchemaJson")
	} else if len(m.ResourceConfigSchemaJson) == 0 {
		return pb.NewValidationError("missing ResourceConfigSchemaJson")
	} else if m.DocumentationUrl == "" {
		return pb.NewValidationError("missing DocumentationUrl")
	}
	return nil
}

// Validate returns an error if the ValidateRequest isn't well-formed.
func (m *Request_Validate) Validate() error {
	if err := m.Name.Validate(); err != nil {
		return pb.ExtendContext(err, "Name")
	} else if _, ok := pf.MaterializationSpec_ConnectorType_name[int32(m.ConnectorType)]; !ok {
		return pb.NewValidationError("unknown EndpointType %v", m.ConnectorType)
	} else if len(m.ConfigJson) == 0 {
		return pb.NewValidationError("missing ConfigJson")
	}

	for i := range m.Bindings {
		if err := m.Bindings[i].Validate(); err != nil {
			return pb.ExtendContext(err, "Bindings[%d]", i)
		}
	}
	return nil
}

// Validate returns an error if the ValidateRequest_Binding isn't well-formed.
func (m *Request_Validate_Binding) Validate() error {
	if err := m.Collection.Validate(); err != nil {
		return pb.ExtendContext(err, "Collection")
	} else if len(m.ResourceConfigJson) == 0 {
		return pb.NewValidationError("missing ResourceConfigJson")
	}
	return nil
}

// Validate returns an error if the ValidateResponse isn't well-formed.
func (m *Response_Validated) Validate() error {
	for i := range m.Bindings {
		if err := m.Bindings[i].Validate(); err != nil {
			return pb.ExtendContext(err, "Bindings[%d]", i)
		}
	}
	return nil
}

// Validate returns an error if the ValidateResponse_Binding isn't well-formed.
func (m *Response_Validated_Binding) Validate() error {
	for field, constraint := range m.Constraints {
		if constraint == nil {
			return pb.ExtendContext(
				pb.NewValidationError("Constraint is missing"), "Constraints[%s]", field)
		} else if _, ok := Response_Validated_Constraint_Type_name[int32(constraint.Type)]; !ok {
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
func (m *Request_Apply) Validate() error {
	if err := m.Materialization.Validate(); err != nil {
		return pb.ExtendContext(err, "Materialization")
	}

	// DryRun cannot have a validation error.
	return nil
}

func (m *Response_Applied) Validate() error {
	// No validations to do.
	return nil
}

// Validate returns an error if the message is not well-formed.
func (m *Request_Open) Validate() error {
	if err := m.Materialization.Validate(); err != nil {
		return pb.ExtendContext(err, "Materialization")
	} else if m.Version == "" {
		return pb.NewValidationError("expected Version")
	} else if err = m.Range.Validate(); err != nil {
		return pb.ExtendContext(err, "Range")
	}
	// StateJson may be empty.
	return nil
}

// Validate returns an error if the message is not well-formed.
func (m *Request_Load) Validate() error {
	if len(m.KeyPacked) == 0 {
		return pb.NewValidationError("expected KeyPacked")
	}
	// KeyJson is not checked yet.
	return nil
}

// Validate returns an error if the message is malformed.
func (m *Request_Flush) Validate() error {
	return nil
}

// Validate returns an error if the message is not well-formed.
func (m *Request_Store) Validate() error {
	if ll := len(m.DocJson); ll == 0 {
		return pb.NewValidationError("expected DocJson")
	} else if len(m.KeyPacked) == 0 {
		return pb.NewValidationError("expected KeyPacked")
	} else if len(m.ValuesPacked) == 0 {
		return pb.NewValidationError("expected ValuesPacked")
	}
	// KeyJson and ValuesJson are not checked yet.
	return nil
}

// Validate returns an error if the message is malformed.
func (m *Request_StartCommit) Validate() error {
	// TODO(johnny): More checkpoint validation.
	// Make sure ack intents are restricted to valid journals.
	/*
		if err := m.RuntimeCheckpoint.Validate(); err != nil {
			return pb.ExtendContext("RuntimeCheckpoint", err)
		}
	*/
	return nil
}

// Validate is a no-op.
func (m *Request_Acknowledge) Validate() error {
	return nil
}

// Validate returns an error if the message is not well-formed.
func (m *Response_Opened) Validate() error {
	// FlowCheckpoint may be empty.
	return nil
}

// Validate returns an error if the message is not well-formed.
func (m *Response_Loaded) Validate() error {
	if len(m.DocJson) == 0 {
		return pb.NewValidationError("expected DocJson")
	}
	return nil
}

func (m *Response_Flushed) Validate() error {
	return nil
}

// Validate returns an error if the message is not well-formed.
func (m *Response_StartedCommit) Validate() error {
	return nil
}

// Validate returns an error if the message is not well-formed.
func (m *Response_Acknowledged) Validate() error {
	return nil
}

// Validate returns an error if the message is not well-formed.
func (m *Request) Validate_() error {
	var count int
	if m.Spec != nil {
		if err := m.Spec.Validate(); err != nil {
			return pb.ExtendContext(err, "Spec")
		}
		count += 1
	}
	if m.Validate != nil {
		if err := m.Validate.Validate(); err != nil {
			return pb.ExtendContext(err, "Validate")
		}
		count += 1
	}
	if m.Apply != nil {
		if err := m.Apply.Validate(); err != nil {
			return pb.ExtendContext(err, "Apply")
		}
		count += 1
	}
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
		return pb.NewValidationError("expected one of Spec, Validate, Apply, Open, Load, Prepare, Store, Commit, or Acknowledge")
	}
	return nil
}

// Validate returns an error if the message is not well-formed.
func (m *Response) Validate() error {
	var count int
	if m.Spec != nil {
		if err := m.Spec.Validate(); err != nil {
			return pb.ExtendContext(err, "Spec")
		}
		count += 1
	}
	if m.Validated != nil {
		if err := m.Validated.Validate(); err != nil {
			return pb.ExtendContext(err, "Validated")
		}
		count += 1
	}
	if m.Applied != nil {
		if err := m.Applied.Validate(); err != nil {
			return pb.ExtendContext(err, "Applied")
		}
		count += 1
	}
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
		return pb.NewValidationError("expected one of Spec, Validated, Applied, Opened, Loaded, Flushed, StartedCommit, or Acknowledged")
	}
	return nil
}

func (m *Request) InvokeConfig() (*json.RawMessage, string) {
	switch {
	case m.Spec != nil:
		return &m.Spec.ConfigJson, m.Spec.ConnectorType.String()
	case m.Validate != nil:
		return &m.Validate.ConfigJson, m.Validate.ConnectorType.String()
	case m.Apply != nil:
		return &m.Apply.Materialization.ConfigJson, m.Apply.Materialization.ConnectorType.String()
	case m.Open != nil:
		return &m.Open.Materialization.ConfigJson, m.Open.Materialization.ConnectorType.String()
	default:
		panic("invalid request")
	}
}
