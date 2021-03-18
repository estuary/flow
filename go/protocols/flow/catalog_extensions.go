package flow

import (
	pb "go.gazette.dev/core/broker/protocol"
)

// Validate returns an error if the Rule is invalid.
func (m *JournalRules_Rule) Validate() error {
	if m.Rule == "" {
		return pb.NewValidationError("missing Rule")
	}
	if err := m.Selector.Validate(); err != nil {
		return pb.ExtendContext(err, "Selector")
	}

	// We cannot validate templates because, by design,
	// they are only partial specifications.

	return nil
}

// Validate returns an error if the Rules are invalid.
func (m *JournalRules) Validate() error {
	for i, r := range m.Rules {
		if err := r.Validate(); err != nil {
			return pb.ExtendContext(err, "Rules[%d]", i)
		}
	}
	return nil
}

// Validate returns an error if the Rule is invalid.
func (m *ShardRules_Rule) Validate() error {
	if m.Rule == "" {
		return pb.NewValidationError("missing Rule")
	}
	if err := m.Selector.Validate(); err != nil {
		return pb.ExtendContext(err, "Selector")
	}

	// We cannot validate templates because, by design,
	// they are only partial specifications.

	return nil
}

// Validate returns an error if the Rules are invalid.
func (m *ShardRules) Validate() error {
	for i, r := range m.Rules {
		if err := r.Validate(); err != nil {
			return pb.ExtendContext(err, "Rules[%d]", i)
		}
	}
	return nil
}

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
		return m.Capture.Capture
	} else if m.Ingestion != nil {
		return m.Ingestion.Collection.String()
	} else if m.Derivation != nil {
		return m.Derivation.Collection.Collection.String()
	} else if m.Materialization != nil {
		return m.Materialization.Materialization
	} else {
		panic("invalid CatalogTask")
	}
}

// Validate returns an error if the Commons is invalid.
func (m *CatalogCommons) Validate() error {
	if m.CommonsId == "" {
		return pb.NewValidationError("missing CommonsId")
	} else if err := m.JournalRules.Validate(); err != nil {
		return pb.ExtendContext(err, "JournalRules")
	} else if err := m.ShardRules.Validate(); err != nil {
		return pb.ExtendContext(err, "ShardRules")
	}
	return nil
}
