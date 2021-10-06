package flow

import pb "go.gazette.dev/core/broker/protocol"

// Validate returns a validation error of the SplitRequest.
func (m *SplitRequest) Validate() error {
	if err := m.Shard.Validate(); err != nil {
		return pb.ExtendContext(err, "Journal")
	} else if m.SplitOnKey == m.SplitOnRclock {
		return pb.NewValidationError("expected one of SplitOnKey or SplitOnRclock")
	}
	return nil
}

// Validate returns a validation error of the SplitResponse.
func (m *SplitResponse) Validate() error {
	if err := m.Header.Validate(); err != nil {
		return pb.ExtendContext(err, "Header")
	} else if err = m.Status.Validate(); err != nil {
		return pb.ExtendContext(err, "Status")
	}

	if (m.ParentRange == nil) != (m.LhsRange == nil) ||
		(m.ParentRange == nil) != (m.RhsRange == nil) {
		return pb.NewValidationError("expected Parent/Lhs/RhsRange to all be set or not set")
	}

	if m.ParentRange != nil {
		if err := m.ParentRange.Validate(); err != nil {
			return pb.ExtendContext(err, "ParentRange")
		} else if err := m.LhsRange.Validate(); err != nil {
			return pb.ExtendContext(err, "LhsRange")
		} else if err := m.RhsRange.Validate(); err != nil {
			return pb.ExtendContext(err, "RhsRange")
		}
	}

	return nil
}
