package protocol

import (
	bytes "bytes"

	pb "go.gazette.dev/core/broker/protocol"
)

// Collection names a specified catalog collection.
type Collection string

// String returns the Collection name as a string.
func (c Collection) String() string { return string(c) }

// Transform names a specified catalog transformation.
type Transform string

// String returns the Tranform name as a string.
func (t Transform) String() string { return string(t) }

// Validate returns a validation error of the JournalShuffle.
func (m *JournalShuffle) Validate() error {
	if err := m.Journal.Validate(); err != nil {
		return pb.ExtendContext(err, "Journal")
	} else if err = m.Coordinator.Validate(); err != nil {
		return pb.ExtendContext(err, "Coordinator")
	} else if err = m.Shuffle.Validate(); err != nil {
		return pb.ExtendContext(err, "Shuffle")
	}

	return nil
}

// Validate returns a validation error of the RangeSpec.
func (m RangeSpec) Validate() error {
	if bytes.Compare(m.KeyBegin, m.KeyEnd) != -1 {
		return pb.NewValidationError("expected KeyBegin < KeyEnd (%v vs %v)", m.KeyBegin, m.KeyEnd)
	} else if m.RClockBegin >= m.RClockEnd {
		return pb.NewValidationError("expected RClockBegin < RClockEnd (%v vs %v)", m.RClockBegin, m.RClockEnd)
	}
	return nil
}

// Validate returns a validation error of the Shuffle.
func (m *Shuffle) Validate() error {
	if len(m.Transform) == 0 {
		return pb.NewValidationError("expected Transform")
	} else if len(m.ShuffleKeyPtr) == 0 {
		return pb.NewValidationError("expected at least one ShuffleKeyPtr")
	} else if _, ok := Shuffle_Hash_name[int32(m.Hash)]; !ok {
		return pb.NewValidationError("unknown Hash (%v)", m.Hash)
	}
	return nil
}

// Validate returns a validation error of the ShuffleRequest.
func (m *ShuffleRequest) Validate() error {
	if m.Resolution != nil {
		if err := m.Resolution.Validate(); err != nil {
			return pb.ExtendContext(err, "Resolution")
		}
	}
	if err := m.Shuffle.Validate(); err != nil {
		return pb.ExtendContext(err, "Shuffle")
	} else if err = m.Range.Validate(); err != nil {
		return pb.ExtendContext(err, "Range")
	} else if m.Offset < 0 {
		return pb.NewValidationError("invalid Offset (%d; expected 0 <= Offset <= MaxInt64)", m.Offset)
	} else if m.EndOffset < 0 || m.EndOffset != 0 && m.EndOffset < m.Offset {
		return pb.NewValidationError("invalid EndOffset (%d; expected 0 or Offset <= EndOffset)", m.EndOffset)
	}

	return nil
}

// AppendValue into this Field. Requires the Arenas in which Value
// bytes currently reside, and the Arena into which they should be copied.
func (m *Field) AppendValue(from, to *Arena, field Field_Value) {
	switch field.Kind {
	case Field_Value_OBJECT, Field_Value_ARRAY, Field_Value_STRING:
		field.Bytes = to.Add(from.Bytes(field.Bytes))
	}
	m.Values = append(m.Values, field)
}
