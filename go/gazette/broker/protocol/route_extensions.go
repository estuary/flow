package protocol

// Copy returns a deep copy of the Route.
func (m Route) Copy() Route {
	return Route{
		Members:   append([]ProcessSpec_ID(nil), m.Members...),
		Primary:   m.Primary,
		Endpoints: append([]Endpoint(nil), m.Endpoints...),
	}
}

// Validate returns an error if the Route is not well-formed.
func (m Route) Validate() error {
	for i, b := range m.Members {
		if err := b.Validate(); err != nil {
			return ExtendContext(err, "Members[%d]", i)
		}
		if i != 0 && !m.Members[i-1].Less(b) {
			return NewValidationError("Members not in unique, sorted order (index %d; %+v <= %+v)",
				i, m.Members[i-1], m.Members[i])
		}
	}

	if m.Primary < -1 || m.Primary >= int32(len(m.Members)) {
		return NewValidationError("invalid Primary (%+v; expected -1 <= Primary < %d)",
			m.Primary, len(m.Members))
	}

	if l := len(m.Endpoints); l != 0 && l != len(m.Members) {
		return NewValidationError("len(Endpoints) != 0, and != len(Members) (%d vs %d)",
			l, len(m.Members))
	}

	for i, ep := range m.Endpoints {
		if ep == "" {
			continue
		} else if err := ep.Validate(); err != nil {
			return ExtendContext(err, "Endpoints[%d]", i)
		}
	}
	return nil
}

// Equivalent returns true if the Routes have equivalent broker Names, Zones,
// and current Primary. It does not compare broker Endpoints.
func (m Route) Equivalent(other *Route) bool {
	if other == nil {
		return false
	} else if m.Primary != other.Primary {
		return false
	} else if len(m.Members) != len(other.Members) {
		return false
	}
	for i := range m.Members {
		if m.Members[i] != other.Members[i] {
			return false
		}
	}
	return true
}

// MarshalString returns the marshaled encoding of the Route as a string.
func (m Route) MarshalString() string {
	var d, err = m.Marshal()
	if err != nil {
		panic(err.Error()) // Cannot happen, as we use no custom marshalling.
	}
	return string(d)
}
