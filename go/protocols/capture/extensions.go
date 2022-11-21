package capture

import (
	"encoding/json"

	pf "github.com/estuary/flow/go/protocols/flow"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer/protocol"
)

func (m *SpecRequest) Validate() error {
	if _, ok := pf.EndpointType_name[int32(m.EndpointType)]; !ok {
		return pb.NewValidationError("unknown EndpointType %v", m.EndpointType)
	} else if len(m.EndpointSpecJson) == 0 {
		return pb.NewValidationError("missing EndpointSpecJson")
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

func (m *DiscoverRequest) Validate() error {
	if _, ok := pf.EndpointType_name[int32(m.EndpointType)]; !ok {
		return pb.NewValidationError("unknown EndpointType %v", m.EndpointType)
	} else if len(m.EndpointSpecJson) == 0 {
		return pb.NewValidationError("missing EndpointSpecJson")
	}
	return nil
}

func (m *DiscoverRequest) GetEndpointType() pf.EndpointType {
	return m.EndpointType
}
func (m *DiscoverRequest) GetEndpointSpecPtr() *json.RawMessage {
	return &m.EndpointSpecJson
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
	if err := m.RecommendedName.Validate(); err != nil {
		return pb.ExtendContext(err, "RecommendedName")
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
	if err := m.Capture.Validate(); err != nil {
		return pb.ExtendContext(err, "Capture")
	}

	// DryRun cannot have a validation error.
	return nil
}

func (m *ApplyRequest) GetEndpointType() pf.EndpointType {
	return m.Capture.EndpointType
}
func (m *ApplyRequest) GetEndpointSpecPtr() *json.RawMessage {
	return &m.Capture.EndpointSpecJson
}

func (m *ApplyResponse) Validate() error {
	// No validations to do.
	return nil
}

// Validate returns an error if the Documents isn't well-formed.
func (m *Documents) Validate() error {
	if len(m.DocsJson) == 0 {
		return pb.NewValidationError("expected DocsJson")
	}
	return nil
}

// Validate returns an error if the Acknowledge isn't well-formed.
func (m *Acknowledge) Validate() error {
	return nil // No-op.
}

// Validate returns an error if the PullRequest isn't well-formed.
func (m *PullRequest) Validate() error {
	var count int
	if m.Open != nil {
		if err := m.Open.Validate(); err != nil {
			return pb.ExtendContext(err, "Opened")
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
		return pb.NewValidationError("expected one of Open, Acknowledge")
	}
	return nil
}

// Validate returns an error if the PullRequest_Open isn't well-formed.
func (m *PullRequest_Open) Validate() error {
	if err := m.Capture.Validate(); err != nil {
		return pb.ExtendContext(err, "Capture")
	} else if m.KeyEnd < m.KeyBegin {
		return pb.NewValidationError("invalid key range (KeyEnd < KeyBegin)")
	}
	// DriverCheckpointJson may be empty.
	// Tail has no validations.
	return nil
}

// Validate returns an error if the PullResponse isn't well-formed.
func (m *PullResponse) Validate() error {
	var count int
	if m.Opened != nil {
		if err := m.Opened.Validate(); err != nil {
			return pb.ExtendContext(err, "Opened")
		}
		count += 1
	}
	if m.Documents != nil {
		if err := m.Documents.Validate(); err != nil {
			return pb.ExtendContext(err, "Documents")
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
		return pb.NewValidationError("expected one of Opened, Documents, Checkpoint")
	}
	return nil
}

// Validate is currently a no-op.
func (m *PullResponse_Opened) Validate() error {
	return nil // Opened has no fields.
}

// Validate returns an error if the PushRequest isn't well-formed.
func (m *PushRequest) Validate() error {
	var count int
	if m.Open != nil {
		if err := m.Open.Validate(); err != nil {
			return pb.ExtendContext(err, "Open")
		}
		count += 1
	}
	if m.Documents != nil {
		if err := m.Documents.Validate(); err != nil {
			return pb.ExtendContext(err, "Documents")
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
		return pb.NewValidationError("expected one of Open, Documents, Checkpoint")
	}
	return nil
}

// Validate returns an error if the PushRequest_Open isn't well-formed.
func (m *PushRequest_Open) Validate() error {
	if m.Header != nil {
		if err := m.Header.Validate(); err != nil {
			return pb.ExtendContext(err, "Header")
		}
	}
	if err := m.Capture.Validate(); err != nil {
		return pb.ExtendContext(err, "Capture")
	}
	return nil
}

// Validate returns an error if the PushResponse isn't well-formed.
func (m *PushResponse) Validate() error {
	var count int
	if m.Opened != nil {
		if err := m.Opened.Validate(); err != nil {
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
		return pb.NewValidationError("expected one of Opened, Acknowledge")
	}
	return nil
}

// Validate returns an error if the PushResponse_Opened isn't well-formed.
func (m *PushResponse_Opened) Validate() error {
	if err := m.Status.Validate(); err != nil {
		return pb.ExtendContext(err, "Status")
	} else if err := m.Header.Validate(); err != nil {
		return pb.ExtendContext(err, "Header")
	}

	if m.Status != protocol.Status_OK {
		// Capture, KeyBegin, KeyEnd, and DriverCheckpointJson are unset.
	} else if m.Capture == nil {
		return pb.NewValidationError("missing Capture")
	} else if err := m.Capture.Validate(); err != nil {
		return pb.ExtendContext(err, "Capture")
	} else if m.KeyEnd < m.KeyBegin {
		return pb.NewValidationError("invalid key range (KeyEnd < KeyBegin)")
	}
	// DriverCheckpointJson may be empty.

	return nil
}
