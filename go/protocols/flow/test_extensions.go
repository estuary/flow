package flow

import pb "go.gazette.dev/core/broker/protocol"

// Validate returns an error if the TestSpec is invalid
func (m *TestSpec) Validate() error {
	if m.Name == "" {
		return pb.NewValidationError("missing Name")
	}
	for i, step := range m.Steps {
		if err := step.Validate(); err != nil {
			return pb.ExtendContext(err, "Steps[%d]", i)
		}
	}
	return nil
}

// Validate returns an error if the TestSpec_Step is invalid.
func (m *TestSpec_Step) Validate() error {
	if err := m.StepType.Validate(); err != nil {
		return pb.ExtendContext(err, "StepType")
	} else if err := m.Collection.Validate(); err != nil {
		return pb.ExtendContext(err, "Collection")
	} else if err = m.Partitions.Validate(); err != nil {
		return pb.ExtendContext(err, "Partitions")
	}
	return nil
}

// Validate returns an error if the step Type is invalid.
func (m TestSpec_Step_Type) Validate() error {
	if _, ok := TestSpec_Step_Type_name[int32(m)]; !ok {
		return pb.NewValidationError("unknown step type (%d)", m)
	}
	return nil
}
