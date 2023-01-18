package flow

import (
	"encoding/json"

	pb "go.gazette.dev/core/broker/protocol"
)

// Capture is a type wrapper for Capture catalog task names.
type Capture string

// String returns the Capture name as a string.
func (c Capture) String() string { return string(c) }

// Validate returns an error if the Capture is malformed.
func (c Capture) Validate() error {
	return pb.ValidateToken(c.String(), pb.TokenSymbols, 1, 512)
}

// Validate returns an error if the CaptureSpec is malformed.
func (m *CaptureSpec) Validate() error {
	if err := m.Capture.Validate(); err != nil {
		return pb.ExtendContext(err, "Capture")
	} else if _, ok := EndpointType_name[int32(m.EndpointType)]; !ok {
		return pb.NewValidationError("unknown EndpointType %v", m.EndpointType)
	} else if len(m.EndpointSpecJson) == 0 {
		return pb.NewValidationError("missing EndpointSpecJson")
	}

	for i := range m.Bindings {
		if err := m.Bindings[i].Validate(); err != nil {
			return pb.ExtendContext(err, "Bindings[%d]", i)
		}
	}
	if err := m.ShardTemplate.Validate(); err != nil {
		return pb.ExtendContext(err, "ShardTemplate")
	} else if err := m.RecoveryLogTemplate.Validate(); err != nil {
		return pb.ExtendContext(err, "RecoveryLogTemplate")
	}
	return nil
}

func (m *CaptureSpec) GetEndpointType() EndpointType {
	return m.EndpointType
}
func (m *CaptureSpec) GetEndpointSpecPtr() *json.RawMessage {
	return &m.EndpointSpecJson
}

// Validate returns an error if the CaptureSpec_Binding is malformed.
func (m *CaptureSpec_Binding) Validate() error {
	if err := m.Collection.Validate(); err != nil {
		return pb.ExtendContext(err, "Collection")
	} else if len(m.ResourceSpecJson) == 0 {
		return pb.NewValidationError("missing EndpointSpecJson")
	} else if len(m.ResourcePath) == 0 {
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
