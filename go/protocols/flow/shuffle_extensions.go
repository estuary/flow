package flow

import (
	"fmt"
	"math"

	pb "go.gazette.dev/core/broker/protocol"
)

// Collection names a specified catalog collection.
type Collection string

// String returns the Collection name as a string.
func (c Collection) String() string { return string(c) }

// Validate returns an error if the Collection is malformed.
func (c Collection) Validate() error {
	return pb.ValidateToken(c.String(), pb.TokenSymbols, 1, 512)
}

// Transform names a specified catalog transformation.
type Transform string

// String returns the Tranform name as a string.
func (t Transform) String() string { return string(t) }

// Validate returns an error if the Collection is malformed.
func (t Transform) Validate() error {
	return pb.ValidateToken(t.String(), pb.TokenSymbols, 1, 512)
}

// Validate returns a validation error of the JournalShuffle.
func (m *JournalShuffle) Validate() error {
	if err := m.Journal.Validate(); err != nil {
		return pb.ExtendContext(err, "Journal")
	} else if err = m.Coordinator.Validate(); err != nil {
		return pb.ExtendContext(err, "Coordinator")
	} else if err = m.Shuffle.Validate(); err != nil {
		return pb.ExtendContext(err, "Shuffle")
	} else if m.BuildId == "" {
		return pb.NewValidationError("missing BuildId")
	}

	return nil
}

// NewFullRange returns a RangeSpec covering the full key and r-clock range.
func NewFullRange() RangeSpec {
	return RangeSpec{
		KeyBegin:    0,
		KeyEnd:      math.MaxUint32,
		RClockBegin: 0,
		RClockEnd:   math.MaxUint32,
	}
}

// Validate returns a validation error of the RangeSpec.
func (m *RangeSpec) Validate() error {
	if m.KeyBegin > m.KeyEnd {
		return pb.NewValidationError("expected KeyBegin <= KeyEnd (%08x vs %08x)", m.KeyBegin, m.KeyEnd)
	} else if m.RClockBegin > m.RClockEnd {
		return pb.NewValidationError("expected RClockBegin <= RClockEnd (%08x vs %08x)", m.RClockBegin, m.RClockEnd)
	}
	return nil
}

// Less returns true if this RangeSpec orders before the argument RangeSpec.
// RangeSpecs are ordered first on key range, and if key range is exactly
// equal, then on r-clock range.
func (m *RangeSpec) Less(r *RangeSpec) bool {
	// If lhs & rhs share the exact same key range, then they order
	// with respect to their RClock range.
	if m.KeyBegin == r.KeyBegin && m.KeyEnd == r.KeyEnd {
		if m.RClockBegin < r.RClockBegin && m.RClockEnd < r.RClockBegin {
			return true
		}
	}
	return m.KeyBegin < r.KeyBegin && m.KeyEnd < r.KeyBegin
}

// Equal returns true if this RangeSpec exactly equals the other.
func (m *RangeSpec) Equal(r *RangeSpec) bool {
	return m.KeyBegin == r.KeyBegin &&
		m.KeyEnd == r.KeyEnd &&
		m.RClockBegin == r.RClockBegin &&
		m.RClockEnd == r.RClockEnd
}

// String returns the RangeSpec in a compact, human-readable text encoding that
// embeds RangeSpec ordering in its natural lexicographic representation.
func (m RangeSpec) String() string {
	return fmt.Sprintf("key:%08x-%08x;r-clock:%08x-%08x",
		m.KeyBegin, m.KeyEnd, m.RClockBegin, m.RClockEnd)
}

// Validate returns a validation error of the Shuffle.
func (m *Shuffle) Validate() error {
	if m.GroupName == "" {
		return pb.NewValidationError("missing GroupName")
	}
	if err := m.SourceCollection.Validate(); err != nil {
		return pb.ExtendContext(err, "SourceCollection")
	}
	if err := m.SourcePartitions.Validate(); err != nil {
		return pb.ExtendContext(err, "SourcePartitions")
	}
	if m.SourceUuidPtr == "" {
		return pb.NewValidationError("missing SourceUuidPtr")
	}
	if m.SourceSchemaUri == "" {
		return pb.NewValidationError("missing SourceSchemaUri")
	}
	if (len(m.ShuffleKeyPtr) == 0) == (m.ShuffleLambda == nil) {
		return pb.NewValidationError("expected one of ShuffleKeyPtr or ShuffleLambda")
	}
	if m.ShuffleLambda != nil {
		if err := m.ShuffleLambda.Validate(); err != nil {
			return pb.ExtendContext(err, "ShuffleLambda")
		}
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

// Validate returns an error if the LambdaSpec is invalid.
func (m *LambdaSpec) Validate() error {
	var cnt int
	if m.Remote != "" {
		cnt++
	}
	if m.Typescript != "" {
		cnt++
	}
	if cnt != 1 {
		return pb.NewValidationError("expected exactly one lambda type")
	}
	return nil
}

// Validate returns an error if the TransformSpec is invalid.
func (m *TransformSpec) Validate() error {
	if err := m.Derivation.Validate(); err != nil {
		return pb.ExtendContext(err, "Derivation")
	}
	if err := m.Transform.Validate(); err != nil {
		return pb.ExtendContext(err, "Transform")
	}
	if err := m.Shuffle.Validate(); err != nil {
		return pb.ExtendContext(err, "Shuffle")
	}
	if m.UpdateLambda != nil {
		if err := m.UpdateLambda.Validate(); err != nil {
			return pb.ExtendContext(err, "UpdateLambda")
		}
	}
	if m.PublishLambda != nil {
		if err := m.PublishLambda.Validate(); err != nil {
			return pb.ExtendContext(err, "PublishLambda")
		}
	}
	return nil
}
