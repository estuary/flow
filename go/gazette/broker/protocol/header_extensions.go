package protocol

// Validate returns an error if the Header is not well-formed.
func (m Header) Validate() error {
	if m.ProcessId != (ProcessSpec_ID{}) {
		if err := m.ProcessId.Validate(); err != nil {
			return ExtendContext(err, "ProcessId")
		}
	}
	if err := m.Route.Validate(); err != nil {
		return ExtendContext(err, "Route")
	} else if err = m.Etcd.Validate(); err != nil {
		return ExtendContext(err, "Etcd")
	}
	return nil
}

// Validate returns an error if the Header_Etcd is not well-formed.
func (m Header_Etcd) Validate() error {
	if m.ClusterId == 0 {
		return NewValidationError("invalid ClusterId (expected != 0)")
	} else if m.MemberId == 0 {
		return NewValidationError("invalid MemberId (expected != 0)")
	} else if m.Revision <= 0 {
		return NewValidationError("invalid Revision (%d; expected 0 < revision)", m.Revision)
	} else if m.RaftTerm == 0 {
		return NewValidationError("invalid RaftTerm (expected != 0)")
	}
	return nil
}
