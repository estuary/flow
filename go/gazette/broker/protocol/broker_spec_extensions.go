package protocol

// Validate returns an error if the ProcessSpec_ID is not well-formed.
func (m ProcessSpec_ID) Validate() error {
	if err := ValidateToken(m.Zone, TokenSymbols, minZoneLen, maxZoneLen); err != nil {
		return ExtendContext(err, "Zone")
	} else if err := ValidateToken(m.Suffix, TokenSymbols, minBrokerSuffixLen, maxBrokerSuffixLen); err != nil {
		return ExtendContext(err, "Suffix")
	}
	return nil
}

// Less returns whether the ProcessSpec_ID is less than the argument
// BrokerSpec_ID, under (Zone, Suffix) ordering.
func (m ProcessSpec_ID) Less(other ProcessSpec_ID) bool {
	if m.Zone != other.Zone {
		return m.Zone < other.Zone
	}
	return m.Suffix < other.Suffix
}

// Validate returns an error if the ProcessSpec is not well-formed.
func (m *ProcessSpec) Validate() error {
	if err := m.Id.Validate(); err != nil {
		return ExtendContext(err, "Id")
	} else if err = m.Endpoint.Validate(); err != nil {
		return ExtendContext(err, "Endpoint")
	}
	return nil
}

// Validate returns an error if the BrokerSpec is not well-formed.
func (m *BrokerSpec) Validate() error {
	if err := m.ProcessSpec.Validate(); err != nil {
		return err
	} else if m.JournalLimit > maxBrokerJournalLimit {
		return NewValidationError("invalid JournalLimit (%d; expected 0 <= JournalLimit <= %d)",
			m.JournalLimit, maxBrokerJournalLimit)
	}
	return nil
}

// MarshalString returns the marshaled encoding of the JournalSpec as a string.
func (m *BrokerSpec) MarshalString() string {
	var d, err = m.Marshal()
	if err != nil {
		panic(err.Error()) // Cannot happen, as we use no custom marshalling.
	}
	return string(d)
}

// v3_allocator.MemberValue implementation.
func (m *BrokerSpec) ItemLimit() int { return int(m.JournalLimit) }

// IsExiting returns true if this broker has been signaled to exit.
func (m *BrokerSpec) IsExiting() bool { return m.Exiting }

// SetExiting marks this BrokerSpec as exiting.
// TODO(whb): Zero'ing JournalLimit is for backward compatibility; remove once
// deployment is complete.
func (m *BrokerSpec) SetExiting() { m.Exiting = true; m.JournalLimit = 0 }

const (
	minZoneLen            = 1
	maxZoneLen            = 32
	minBrokerSuffixLen    = 4
	maxBrokerSuffixLen    = 128
	maxBrokerJournalLimit = 1 << 17
)
