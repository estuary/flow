package protocol

import (
	"bytes"
	"fmt"
	"net/url"
	"strconv"
	"unsafe"

	"github.com/jgraettinger/cockroach-encoding/encoding"
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
	if len(m.ShuffleKeyPtr) == 0 {
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

// ValueFromInterface attempts to convert an interface{} into a Field_Value.
func ValueFromInterface(arena *Arena, v interface{}) (Field_Value, error) {
	if v == nil {
		return Field_Value{Kind: Field_Value_NULL}, nil
	}

	switch vv := v.(type) {
	case int64:
		return Field_Value{Kind: Field_Value_SIGNED, Signed: vv}, nil
	case uint64:
		return Field_Value{Kind: Field_Value_UNSIGNED, Unsigned: vv}, nil
	case float64:
		return Field_Value{Kind: Field_Value_DOUBLE, Double: vv}, nil
	case bool:
		if vv {
			return Field_Value{Kind: Field_Value_TRUE}, nil
		}
		return Field_Value{Kind: Field_Value_FALSE}, nil
	case string:
		return Field_Value{Kind: Field_Value_STRING, Bytes: arena.Add([]byte(vv))}, nil
	}
	return Field_Value{}, fmt.Errorf("couldn't convert from interface %#v", v)
}

// ToInterface converts this Field_Value into a dynamic interface{}.
func (m *Field_Value) ToInterface(arena Arena) interface{} {
	switch m.Kind {
	case Field_Value_NULL:
		return nil
	case Field_Value_TRUE:
		return true
	case Field_Value_FALSE:
		return false
	case Field_Value_UNSIGNED:
		return m.Unsigned
	case Field_Value_SIGNED:
		return m.Signed
	case Field_Value_DOUBLE:
		return m.Double
	case Field_Value_STRING:
		return string(arena.Bytes(m.Bytes))
	case Field_Value_OBJECT, Field_Value_ARRAY:
		return arena.Bytes(m.Bytes)
	default:
		panic("invalid value Kind")
	}
}

// EncodePartition encodes this Value into a string representation suited
// use as a partition discriminant within a Journal name.
// Not all Value types are supported, by design -- only key-able types.
func (m *Field_Value) EncodePartition(b []byte, arena Arena) []byte {
	switch m.Kind {
	case Field_Value_NULL:
		return append(b, "null"...)
	case Field_Value_TRUE:
		return append(b, "true"...)
	case Field_Value_FALSE:
		return append(b, "false"...)
	case Field_Value_UNSIGNED:
		return strconv.AppendUint(b, m.Unsigned, 10)
	case Field_Value_SIGNED:
		return strconv.AppendInt(b, m.Signed, 10)
	case Field_Value_DOUBLE:
		return strconv.AppendFloat(b, m.Double, 'g', -1, 64)
	case Field_Value_STRING, Field_Value_OBJECT, Field_Value_ARRAY:
		var bb = arena.Bytes(m.Bytes)
		var s = *(*string)(unsafe.Pointer(&bb))
		return append(b, url.PathEscape(s)...)
	default:
		panic("invalid value Kind")
	}
}

// EncodePacked encodes this Value into an order-preserving, embedded []byte encoding.
func (m *Field_Value) EncodePacked(b []byte, arena Arena) []byte {
	switch m.Kind {
	case Field_Value_NULL:
		return encoding.EncodeNullAscending(b)
	case Field_Value_TRUE:
		return encoding.EncodeTrueAscending(b)
	case Field_Value_FALSE:
		return encoding.EncodeFalseAscending(b)
	case Field_Value_UNSIGNED:
		return encoding.EncodeUvarintAscending(b, m.Unsigned)
	case Field_Value_SIGNED:
		return encoding.EncodeVarintAscending(b, m.Signed)
	case Field_Value_DOUBLE:
		return encoding.EncodeFloatAscending(b, m.Double)
	case Field_Value_STRING, Field_Value_OBJECT, Field_Value_ARRAY:
		return encoding.EncodeBytesAscending(b, arena.Bytes(m.Bytes))
	default:
		panic("invalid value Kind")
	}
}
