package protocol

import (
	fmt "fmt"

	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
)

// ShardID returns the ShardID of the member at the given index,
// which must be less than the number of ring Members.
func (m *Ring) ShardID(index int) pc.ShardID {
	if index >= len(m.Members) {
		panic("ring index is too large")
	}
	return pc.ShardID(fmt.Sprintf("%s-%03d", m.Name, index))
}

// Validate returns a validation error of the Ring.
func (m *Ring) Validate() error {
	if m.Name == "" {
		return pb.NewValidationError("expected Name")
	} else if len(m.Members) == 0 {
		return pb.NewValidationError("expected at least one Member")
	}
	for i, p := range m.Members {
		if err := p.Validate(); err != nil {
			return pb.ExtendContext(err, "Members[%d]", i)
		}
	}
	return nil
}

// Validate returns a validation error of the Ring_Member.
func (m *Ring_Member) Validate() error {
	if m.MinMsgClock != 0 && m.MaxMsgClock != 0 && m.MinMsgClock > m.MaxMsgClock {
		return pb.NewValidationError("invalid min/max clocks (min clock %d > max %d)",
			m.MinMsgClock, m.MaxMsgClock)
	}
	return nil
}

// Validate returns a validation error of the ShuffleConfig.
func (m *ShuffleConfig) Validate() error {
	if err := m.Journal.Validate(); err != nil {
		return pb.ExtendContext(err, "Journal")
	} else if err = m.Ring.Validate(); err != nil {
		return pb.ExtendContext(err, "Ring")
	} else if m.Coordinator >= uint32(len(m.Ring.Members)) {
		pb.NewValidationError("invalid Coordinator < len(Members) (%d vs %d)", m.Coordinator, len(m.Ring.Members))
	}
	for i, s := range m.Shuffles {
		if err := s.Validate(); err != nil {
			return pb.ExtendContext(err, "Shuffles[%d]", i)
		}
	}
	return nil
}

// CoordinatorShard returns the ShardID which acts as coordinator of this ShuffleConfig.
func (m *ShuffleConfig) CoordinatorShard() pc.ShardID {
	return m.Ring.ShardID(int(m.Coordinator))
}

// Validate returns a validation error of the ShuffleConfig_Shuffle.
func (m *ShuffleConfig_Shuffle) Validate() error {
	if len(m.ShuffleKeyPtr) == 0 {
		return pb.NewValidationError("expected at least one ShuffleKeyPtr")
	} else if (m.ChooseFrom == 0 && m.BroadcastTo == 0) || (m.ChooseFrom != 0 && m.BroadcastTo != 0) {
		return pb.NewValidationError("expected one of ChooseFrom or BroadcastTo to be non-zero")
	}
	return nil
}
