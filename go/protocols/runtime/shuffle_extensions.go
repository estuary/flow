package runtime

import (
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
)

// IndexedShuffleResponse is an implementation of message.Message which
// indexes a specific document within a ShuffleResponse.
type IndexedShuffleResponse struct {
	ShuffleResponse
	Index int
	// Index of the Transform or Binding on whose behalf this document was read.
	ShuffleIndex int
}

var _ message.Message = IndexedShuffleResponse{}

// GetUUID fetches the UUID of the Document.
func (sd IndexedShuffleResponse) GetUUID() message.UUID { return sd.UuidParts[sd.Index].Pack() }

// SetUUID panics if called.
func (sd IndexedShuffleResponse) SetUUID(uuid message.UUID) { panic("not implemented") }

// NewAcknowledgement panics if called.
func (sd IndexedShuffleResponse) NewAcknowledgement(pb.Journal) message.Message {
	panic("not implemented")
}

// Tailing returns whether the ShuffleResponse is at the tail of the journal's available content.
func (m *ShuffleResponse) Tailing() bool {
	return m != nil && m.ReadThrough == m.WriteHead
}

// Validate returns a validation error of the ShuffleRequest.
func (m *ShuffleRequest) Validate() error {
	if err := m.Journal.Validate(); err != nil {
		return pb.ExtendContext(err, "Journal")
	} else if m.Offset < 0 {
		return pb.NewValidationError("invalid Offset (%d; expected 0 <= Offset <= MaxInt64)", m.Offset)
	} else if m.EndOffset < 0 || m.EndOffset != 0 && m.EndOffset < m.Offset {
		return pb.NewValidationError("invalid EndOffset (%d; expected 0 or Offset <= EndOffset)", m.EndOffset)
	} else if err = m.Range.Validate(); err != nil {
		return pb.ExtendContext(err, "Range")
	} else if err = m.Coordinator.Validate(); err != nil {
		return pb.ExtendContext(err, "Coordinator")
	} else if m.Resolution == nil {
		return pb.NewValidationError("missing Resolution")
	} else if err = m.Resolution.Validate(); err != nil {
		return pb.ExtendContext(err, "Resolution")
	} else if m.BuildId == "" {
		return pb.NewValidationError("missing BuildId")
	}

	if m.Derivation != nil {
		if err := m.Derivation.Validate(); err != nil {
			return pb.ExtendContext(err, "Derivation")
		} else if l := len(m.Derivation.Derivation.Transforms); int(m.ShuffleIndex) > l {
			return pb.NewValidationError("invalid ShuffleIndex (%d; expected 0 <= ShuffleIndex <= %d transforms)", m.ShuffleIndex, l)
		}
	} else if m.Materialization != nil {
		if err := m.Materialization.Validate(); err != nil {
			return pb.ExtendContext(err, "Materialization")
		} else if l := len(m.Materialization.Bindings); int(m.ShuffleIndex) > l {
			return pb.NewValidationError("invalid ShuffleIndex (%d; expected 0 <= ShuffleIndex <= %d bindings)", m.ShuffleIndex, l)
		}
	} else {
		return pb.NewValidationError("missing Derivation or Materialization")
	}
	return nil
}

// Validate returns a validation error of the ShuffleResponse.
func (m *ShuffleResponse) Validate() error {
	if err := m.Status.Validate(); err != nil {
		return pb.ExtendContext(err, "Status")
	} else if m.Header == nil {
		return pb.NewValidationError("missing Header")
	} else if err = m.Header.Validate(); err != nil {
		return pb.ExtendContext(err, "Header")
	} else if m.ReadThrough < 0 {
		return pb.NewValidationError("invalid ReadThrough (%d; expected 0 <= ReadThrough <= MaxInt64)", m.ReadThrough)
	} else if m.WriteHead < m.ReadThrough {
		return pb.NewValidationError("invalid WriteHead (%d; expected WriteHead >= ReadThrough)", m.WriteHead)
	}

	var docs = len(m.Docs)

	if docs != 0 && m.TerminalError != "" {
		return pb.NewValidationError(
			"terminal error response should not have docs (%d docs, terminal error is %q)",
			docs, m.TerminalError)
	} else if l := len(m.Offsets); l != docs*2 {
		return pb.NewValidationError("wrong number of Offsets (%d; expected %d)", l, docs*2)
	} else if l := len(m.UuidParts); l != docs {
		return pb.NewValidationError("wrong number of UuidParts (%d; expected %d)", l, docs)
	} else if l := len(m.PackedKey); l != docs {
		return pb.NewValidationError("wrong number of PackedKey (%d; expected %d)", l, docs)
	}

	return nil
}
