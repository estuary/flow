package protocol

import (
	pb "go.gazette.dev/core/broker/protocol"
)

// Validate returns an error if the StatRequest is not well-formed.
func (m *StatRequest) Validate() error {
	if m.Header != nil {
		if err := m.Header.Validate(); err != nil {
			return pb.ExtendContext(err, "Header")
		}
	}
	if err := m.Shard.Validate(); err != nil {
		return pb.ExtendContext(err, "Shard")
	} else if err = pb.Offsets(m.ReadThrough).Validate(); err != nil {
		return pb.ExtendContext(err, "ReadThrough")
	}
	return nil
}

// Validate returns an error if the StatResponse is not well-formed.
func (m *StatResponse) Validate() error {
	if err := m.Status.Validate(); err != nil {
		return pb.ExtendContext(err, "Status")
	} else if err = m.Header.Validate(); err != nil {
		return pb.ExtendContext(err, "Header")
	} else if err = pb.Offsets(m.ReadThrough).Validate(); err != nil {
		return pb.ExtendContext(err, "ReadThrough")
	} else if err = pb.Offsets(m.PublishAt).Validate(); err != nil {
		return pb.ExtendContext(err, "PublishAt")
	}
	return nil
}

// Validate returns an error if the ListRequest is not well-formed.
func (m *ListRequest) Validate() error {
	if err := m.Selector.Validate(); err != nil {
		return pb.ExtendContext(err, "Selector")
	}
	return nil
}

// Validate returns an error if the ListResponse is not well-formed.
func (m *ListResponse) Validate() error {
	if err := m.Status.Validate(); err != nil {
		return pb.ExtendContext(err, "Status")
	} else if err = m.Header.Validate(); err != nil {
		return pb.ExtendContext(err, "Header")
	}
	for i, shard := range m.Shards {
		if err := shard.Validate(); err != nil {
			return pb.ExtendContext(err, "Shards[%d]", i)
		}
	}
	return nil
}

// Validate returns an error if the ListResponse_Shard is not well-formed.
func (m *ListResponse_Shard) Validate() error {
	if err := m.Spec.Validate(); err != nil {
		return pb.ExtendContext(err, "Spec")
	} else if m.ModRevision <= 0 {
		return pb.NewValidationError("invalid ModRevision (%d; expected > 0)", m.ModRevision)
	} else if err = m.Route.Validate(); err != nil {
		return pb.ExtendContext(err, "Route")
	} else if l1, l2 := len(m.Route.Members), len(m.Status); l1 != l2 {
		return pb.NewValidationError("length of Route.Members and Status are not equal (%d vs %d)", l1, l2)
	}
	for i, status := range m.Status {
		if err := status.Validate(); err != nil {
			return pb.ExtendContext(err, "Status[%d]", i)
		}
	}
	return nil
}

// Validate returns an error if the ApplyRequest is not well-formed.
func (m *ApplyRequest) Validate() error {
	for i, change := range m.Changes {
		if err := change.Validate(); err != nil {
			return pb.ExtendContext(err, "Changes[%d]", i)
		}
	}
	return nil
}

// Validate returns an error if the ApplyRequest_Change is not well-formed.
func (m *ApplyRequest_Change) Validate() error {
	if m.Upsert != nil {
		if m.Delete != "" {
			return pb.NewValidationError("both Upsert and Delete are set (expected exactly one)")
		} else if err := m.Upsert.Validate(); err != nil {
			return pb.ExtendContext(err, "Upsert")
		} else if m.ExpectModRevision < 0 {
			return pb.NewValidationError("invalid ExpectModRevision (%d; expected >= 0)", m.ExpectModRevision)
		}
	} else if m.Delete != "" {
		if err := m.Delete.Validate(); err != nil {
			return pb.ExtendContext(err, "Delete")
		} else if m.ExpectModRevision <= 0 {
			return pb.NewValidationError("invalid ExpectModRevision (%d; expected > 0)", m.ExpectModRevision)
		}
	} else {
		return pb.NewValidationError("neither Upsert nor Delete are set (expected exactly one)")
	}
	return nil
}

// Validate returns an error if the ApplyResponse is not well-formed.
func (m *ApplyResponse) Validate() error {
	if err := m.Status.Validate(); err != nil {
		return pb.ExtendContext(err, "Status")
	} else if err = m.Header.Validate(); err != nil {
		return pb.ExtendContext(err, "Header")
	}
	return nil
}

// Validate returns an error if the HintsRequest is not well-formed.
func (m *GetHintsRequest) Validate() error {
	if err := m.Shard.Validate(); err != nil {
		return pb.ExtendContext(err, "Shard")
	}
	return nil
}

// Validate returns an error if the HintsResponse is not well-formed.
func (m *GetHintsResponse) Validate() error {
	if err := m.Status.Validate(); err != nil {
		return pb.ExtendContext(err, "Status")
	}

	if err := m.PrimaryHints.Validate(); err != nil {
		return pb.ExtendContext(err, "primary hints")
	}
	for _, hints := range m.BackupHints {
		if err := hints.Validate(); err != nil {
			return pb.ExtendContext(err, "backup hints")
		}
	}
	return nil
}

// Validate returns an error if the GetHintsResponse_ResponseHints is not well-formed.
func (m GetHintsResponse_ResponseHints) Validate() error {
	if m.Hints != nil {
		return m.Hints.Validate()
	}
	return nil
}
