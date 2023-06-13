package capture

import (
	"encoding/json"

	pf "github.com/estuary/flow/go/protocols/flow"
	pb "go.gazette.dev/core/broker/protocol"
)

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

func (m *Request_Discover) Validate() error {
	if _, ok := pf.CaptureSpec_ConnectorType_name[int32(m.ConnectorType)]; !ok {
		return pb.NewValidationError("unknown ConnectorType %v", m.ConnectorType)
	} else if len(m.ConfigJson) == 0 {
		return pb.NewValidationError("missing ConfigJson")
	}
	return nil
}

func (m *Response_Discovered) Validate() error {
	for i, b := range m.Bindings {
		if err := b.Validate(); err != nil {
			return pb.NewValidationError("Bindings[%d]: %w", i, err)
		}
	}
	return nil
}

func (m *Response_Discovered_Binding) Validate() error {
	if m.RecommendedName == "" {
		return pb.NewValidationError("missing RecommendedName")
	} else if len(m.DocumentSchemaJson) == 0 {
		return pb.NewValidationError("missing DocumentSchemaJson")
	} else if len(m.ResourceConfigJson) == 0 {
		return pb.NewValidationError("missing ResourceConfigJson")
	}
	return nil
}

// Validate returns an error if the ValidateRequest isn't well-formed.
func (m *Request_Validate) Validate() error {
	if err := m.Name.Validate(); err != nil {
		return pb.ExtendContext(err, "Capture")
	} else if _, ok := pf.CaptureSpec_ConnectorType_name[int32(m.ConnectorType)]; !ok {
		return pb.NewValidationError("unknown ConnectorType %v", m.ConnectorType)
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
	if err := m.Capture.Validate(); err != nil {
		return pb.ExtendContext(err, "Capture")
	}

	// DryRun cannot have a validation error.
	return nil
}

func (m *Response_Applied) Validate() error {
	// No validations to do.
	return nil
}

// Validate returns an error if the Documents isn't well-formed.
func (m *Response_Captured) Validate() error {
	if len(m.DocJson) == 0 {
		return pb.NewValidationError("expected DocJson")
	}
	return nil
}

// Validate returns an error if the Acknowledge isn't well-formed.
func (m *Request_Acknowledge) Validate() error {
	return nil // No-op.
}

// Validate returns an error if the PullRequest_Open isn't well-formed.
func (m *Request_Open) Validate() error {
	if err := m.Capture.Validate(); err != nil {
		return pb.ExtendContext(err, "Capture")
	} else if err := m.Range.Validate(); err != nil {
		return pb.ExtendContext(err, "Range")
	}
	// StateJson may be empty.
	return nil
}

func (m *Response_Checkpoint) Validate() error {
	if m.State != nil {
		if err := m.State.Validate(); err != nil {
			return pb.ExtendContext(err, "State")
		}
	}
	return nil
}

// Validate is currently a no-op.
func (m *Response_Opened) Validate() error {
	return nil // Opened has no fields.
}

// Validate returns an error if the Request isn't well-formed.
func (m *Request) Validate_() error {
	var count int
	if m.Spec != nil {
		if err := m.Spec.Validate(); err != nil {
			return pb.ExtendContext(err, "Spec")
		}
		count += 1
	}
	if m.Discover != nil {
		if err := m.Discover.Validate(); err != nil {
			return pb.ExtendContext(err, "Discover")
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
	if m.Acknowledge != nil {
		if err := m.Acknowledge.Validate(); err != nil {
			return pb.ExtendContext(err, "Acknowledge")
		}
		count += 1
	}

	if count != 1 {
		return pb.NewValidationError("expected one of Spec, Discover, Validate, Apply, Open, or Acknowledge")
	}
	return nil
}

// Validate returns an error if the Response isn't well-formed.
func (m *Response) Validate() error {
	var count int
	if m.Spec != nil {
		if err := m.Spec.Validate(); err != nil {
			return pb.ExtendContext(err, "Spec")
		}
		count += 1
	}
	if m.Discovered != nil {
		if err := m.Discovered.Validate(); err != nil {
			return pb.ExtendContext(err, "Discovered")
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
	if m.Captured != nil {
		if err := m.Captured.Validate(); err != nil {
			return pb.ExtendContext(err, "Captured")
		}
		count += 1
	}
	if m.Checkpoint != nil {
		if err := m.Checkpoint.Validate(); err != nil {
			return pb.ExtendContext(err, "Checkpoint")
		}
		count += 1
	}

	if count != 1 {
		return pb.NewValidationError("expected one of Spec, Discovered, Validated, Applied, Opened, Captured, or Checkpoint")
	}
	return nil
}

func (m *Request) InvokeConfig() (*json.RawMessage, string) {
	switch {
	case m.Spec != nil:
		return &m.Spec.ConfigJson, m.Spec.ConnectorType.String()
	case m.Discover != nil:
		return &m.Discover.ConfigJson, m.Discover.ConnectorType.String()
	case m.Validate != nil:
		return &m.Validate.ConfigJson, m.Validate.ConnectorType.String()
	case m.Apply != nil:
		return &m.Apply.Capture.ConfigJson, m.Apply.Capture.ConnectorType.String()
	case m.Open != nil:
		return &m.Open.Capture.ConfigJson, m.Open.Capture.ConnectorType.String()
	default:
		panic("invalid request")
	}
}
