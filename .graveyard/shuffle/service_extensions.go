package shuffle

import pb "go.gazette.dev/core/broker/protocol"

func (m *Config) Validate() error {
	if len(m.Processors) == 0 {
		return pb.NewValidationError("expected at least one Processor")
	}
	for i, p := range m.Processors {
		if err := p.Validate(); err != nil {
			return pb.ExtendContext(err, "Processors[%d]", i)
		} else if i == 0 && !p.Equal(&Config_Processor{}) {
			return pb.NewValidationError("Processors[0] cannot have clock bounds (%s)", &p)
		}
	}
	if (m.ChooseFrom == 0 && m.BroadcastTo == 0) || (m.ChooseFrom != 0 && m.BroadcastTo != 0) {
		return pb.NewValidationError("expected one of ChooseFrom or BroadcastTo to be non-zero")
	}
	return nil
}

func (m *Config_Processor) Validate() error {
	if m.MinMsgClock != 0 && m.MaxMsgClock != 0 && m.MinMsgClock > m.MaxMsgClock {
		return pb.NewValidationError("invalid min/max clocks (min clock %d > max %d)",
			m.MinMsgClock, m.MaxMsgClock)
	}
	return nil
}
