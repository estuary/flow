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

// BindingCollection returns the CollectionSpec of a binding of this CaptureSpec,
// resolving it through LinkedCollections if this spec is in indirect form.
// It returns nil if the binding cannot be resolved, which Validate() rejects.
func (m *CaptureSpec) BindingCollection(b *CaptureSpec_Binding) *CollectionSpec {
	if len(m.LinkedCollections) == 0 {
		return &b.Collection
	} else if int(b.CollectionIndex) >= len(m.LinkedCollections) {
		return nil
	}
	return &m.LinkedCollections[b.CollectionIndex]
}

// Validate returns an error if the CaptureSpec is malformed.
func (m *CaptureSpec) Validate() error {
	if err := m.Name.Validate(); err != nil {
		return pb.ExtendContext(err, "Name")
	} else if _, ok := CaptureSpec_ConnectorType_name[int32(m.ConnectorType)]; !ok {
		return pb.NewValidationError("unknown ConnectorType %v", m.ConnectorType)
	} else if len(m.ConfigJson) == 0 {
		return pb.NewValidationError("missing ConfigJson")
	}

	if err := ValidateLinkedCollections(m.LinkedCollections); err != nil {
		return err
	}
	for i := range m.Bindings {
		if err := m.Bindings[i].validate(m); err != nil {
			return pb.ExtendContext(err, "Bindings[%d]", i)
		}
	}
	// Inactive bindings index the same table as their active peers, so their
	// encoding form must agree. Their other fields are not validated, matching
	// the long-standing behavior of this method.
	for i, b := range m.InactiveBindings {
		if err := ValidateBindingCollection(
			len(m.LinkedCollections), b.Collection.ProtoSize(), b.CollectionIndex,
		); err != nil {
			return pb.ExtendContext(err, "InactiveBindings[%d]", i)
		}
	}
	if err := m.ShardTemplate.Validate(); err != nil {
		return pb.ExtendContext(err, "ShardTemplate")
	} else if err := m.RecoveryLogTemplate.Validate(); err != nil {
		return pb.ExtendContext(err, "RecoveryLogTemplate")
	}
	return nil
}

// Validate returns an error if the CaptureSpec_Binding is malformed.
// It validates the binding's inlined Collection, and so is meaningful only for
// a binding of an inline-form CaptureSpec. An indirect-form binding must instead
// be validated through its parent, which resolves its collection.
func (m *CaptureSpec_Binding) Validate() error {
	if err := m.Collection.Validate(); err != nil {
		return pb.ExtendContext(err, "Collection")
	}
	return m.validateResource()
}

// validate checks this binding within the context of its parent spec, which
// determines whether the binding inlines its collection or indexes the parent's
// LinkedCollections.
func (m *CaptureSpec_Binding) validate(parent *CaptureSpec) error {
	if err := ValidateBindingCollection(
		len(parent.LinkedCollections), m.Collection.ProtoSize(), m.CollectionIndex,
	); err != nil {
		return err
	} else if len(parent.LinkedCollections) == 0 {
		if err := m.Collection.Validate(); err != nil {
			return pb.ExtendContext(err, "Collection")
		}
	}
	return m.validateResource()
}

func (m *CaptureSpec_Binding) validateResource() error {
	if len(m.ResourceConfigJson) == 0 {
		return pb.NewValidationError("missing ResourceConfigJson")
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

func (m *CaptureSpec) InvokeConfig() (*json.RawMessage, string) {
	return &m.ConfigJson, m.ConnectorType.String()
}
