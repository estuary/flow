package flow

import (
	"encoding/binary"

	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
)

// Arena is a packed memory region into which byte content and strings are written.
type Arena []byte

// Add to the Arena, returning their indexed Slice.
func (a *Arena) Add(b []byte) Slice {
	var out = Slice{Begin: uint32(len(*a))}
	*a = append(*a, b...)
	out.End = uint32(len(*a))
	return out
}

// AddAll to the Arena, returning a slice of indexed Slices.
func (a *Arena) AddAll(b ...[]byte) []Slice {
	var out = make([]Slice, 0, len(b))
	for _, bb := range b {
		out = append(out, a.Add(bb))
	}
	return out
}

// Bytes returns the portion of the Arena indexed by Slice as []byte.
func (a *Arena) Bytes(s Slice) []byte { return (*a)[s.Begin:s.End] }

// AllBytes returns all []bytes indexed by the given Slices.
func (a *Arena) AllBytes(s ...Slice) [][]byte {
	var out = make([][]byte, 0, len(s))
	for _, ss := range s {
		out = append(out, a.Bytes(ss))
	}
	return out
}

// NewUUIDParts returns a decomposition of |uuid| into its UUIDParts.
func NewUUIDParts(uuid message.UUID) UUIDParts {
	var tmp [8]byte
	var producer = message.GetProducerID(uuid)
	copy(tmp[:6], producer[:])
	binary.BigEndian.PutUint16(tmp[6:8], uint16(message.GetFlags(uuid)))

	return UUIDParts{
		Node:  binary.BigEndian.Uint64(tmp[:]),
		Clock: message.GetClock(uuid),
	}
}

// Pack this UUIDParts into a message.UUID.
func (parts *UUIDParts) Pack() message.UUID {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], parts.Node)
	var producerID message.ProducerID
	copy(producerID[:], tmp[:6])

	return message.BuildUUID(
		producerID,
		parts.Clock,
		message.Flags(parts.Node),
	)
}

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

var (
	// DocumentUUIDPlaceholder is a unique 36-byte sequence which is used to mark
	// the location within a document serialization which holds the document UUID.
	// This "magic" value is defined here, and also in crates/derive/src/combiner.rs.
	// We never write this value anywhere; it's a temporary placeholder generated
	// within combined documents returned by Rust, that's then immediately replaced
	// with a properly sequenced UUID by flow.Mapper prior to publishing.
	DocumentUUIDPlaceholder = []byte("DocUUIDPlaceholder-329Bb50aa48EAa9ef")
)
