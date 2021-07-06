package capture

import (
	pf "github.com/estuary/flow/go/protocols/flow"
	pb "go.gazette.dev/core/broker/protocol"
)

func (m *SpecRequest) Validate() error {
	if _, ok := pf.EndpointType_name[int32(m.EndpointType)]; !ok {
		return pb.NewValidationError("unknown EndpointType %v", m.EndpointType)
	} else if len(m.EndpointSpecJson) == 0 {
		return pb.NewValidationError("missing EndpointSpecJson")
	}
	return nil
}

func (m *DiscoverRequest) Validate() error {
	if _, ok := pf.EndpointType_name[int32(m.EndpointType)]; !ok {
		return pb.NewValidationError("unknown EndpointType %v", m.EndpointType)
	} else if len(m.EndpointSpecJson) == 0 {
		return pb.NewValidationError("missing EndpointSpecJson")
	}
	return nil
}

func (m *DiscoverResponse) Validate() error {
	for i, b := range m.Bindings {
		if err := b.Validate(); err != nil {
			return pb.NewValidationError("Bindings[%d]: %w", i, err)
		}
	}
	return nil
}

func (m *DiscoverResponse_Binding) Validate() error {
	if m.RecommendedName == "" {
		return pb.NewValidationError("missing RecommendedName")
	} else if len(m.DocumentSchemaJson) == 0 {
		return pb.NewValidationError("missing DocumentSchemaJson")
	} else if len(m.ResourceSpecJson) == 0 {
		return pb.NewValidationError("missing ResourceSpecJson")
	}
	return nil
}

// Validate returns an error if the ValidateRequest isn't well-formed.
func (m *ValidateRequest) Validate() error {
	if err := m.Capture.Validate(); err != nil {
		return pb.ExtendContext(err, "Capture")
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

// Validate returns an error if the CaptureRequest isn't well-formed.
func (m *CaptureRequest) Validate() error {
	if err := m.Capture.Validate(); err != nil {
		return pb.ExtendContext(err, "Capture")
	} else if m.KeyEnd < m.KeyBegin {
		return pb.NewValidationError("invalid key range (KeyEnd < KeyBegin)")
	}

	// DriverCheckpointJson may be empty if no checkpoint has been recorded yet.

	return nil
}
