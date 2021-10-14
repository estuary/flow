package flow

import (
	bytes "bytes"
	"encoding/json"

	pb "go.gazette.dev/core/broker/protocol"
)

// Validate returns an error if the CatalogTask is invalid.
func (m *CatalogTask) Validate() error {
	if m.CommonsId == "" {
		return pb.NewValidationError("missing CommonsId")
	}

	var n = 0
	if m.Capture != nil {
		if err := m.Capture.Validate(); err != nil {
			return pb.ExtendContext(err, "Capture")
		}
		n++
	}
	if m.Ingestion != nil {
		if err := m.Ingestion.Validate(); err != nil {
			return pb.ExtendContext(err, "Ingestion")
		}
		n++
	}
	if m.Derivation != nil {
		if err := m.Derivation.Validate(); err != nil {
			return pb.ExtendContext(err, "Derivation")
		}
		n++
	}
	if m.Materialization != nil {
		if err := m.Materialization.Validate(); err != nil {
			return pb.ExtendContext(err, "Materialization")
		}
		n++
	}

	if n != 1 {
		return pb.NewValidationError(
			"expected exactly one of Capture, Ingestion, Derivation, or Materializations")
	}

	return nil
}

// Name returns the stable, long-lived name of this CatalogTask.
func (m *CatalogTask) Name() string {
	if m.Capture != nil {
		return m.Capture.Capture.String()
	} else if m.Ingestion != nil {
		return m.Ingestion.Collection.String()
	} else if m.Derivation != nil {
		return m.Derivation.Collection.Collection.String()
	} else if m.Materialization != nil {
		return m.Materialization.Materialization.String()
	} else {
		panic("invalid CatalogTask")
	}
}

// Shuffles returns the []*Shuffles of this CatalogTask.
func (m *CatalogTask) Shuffles() []*Shuffle {
	// Captures have no shuffles.

	if m.Derivation != nil {
		var shuffles = make([]*Shuffle, len(m.Derivation.Transforms))
		for i := range m.Derivation.Transforms {
			shuffles[i] = &m.Derivation.Transforms[i].Shuffle
		}
		return shuffles
	}

	if m.Materialization != nil {
		var shuffles = make([]*Shuffle, len(m.Materialization.Bindings))
		for i := range m.Materialization.Bindings {
			shuffles[i] = &m.Materialization.Bindings[i].Shuffle
		}
		return shuffles
	}

	return nil
}

// Validate returns an error if the Commons is invalid.
func (m *CatalogCommons) Validate() error {
	if m.CommonsId == "" {
		return pb.NewValidationError("missing CommonsId")
	}
	return nil
}

// UnmarshalStrict unmarshals |doc| into |m|, using a strict decoding
// of the document which prohibits unknown fields.
// If decoding is successful, then |m| is also validated.
func UnmarshalStrict(doc json.RawMessage, into pb.Validator) error {
	var d = json.NewDecoder(bytes.NewReader(doc))
	d.DisallowUnknownFields()

	if err := d.Decode(into); err != nil {
		return err
	}
	return into.Validate()
}
