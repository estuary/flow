package protocol

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
		ProducerAndFlags: binary.BigEndian.Uint64(tmp[:]),
		Clock:            message.GetClock(uuid),
	}
}

// Pack this UUIDParts into a message.UUID.
func (parts *UUIDParts) Pack() message.UUID {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], parts.ProducerAndFlags)
	var producerID message.ProducerID
	copy(producerID[:], tmp[:6])

	return message.BuildUUID(
		producerID,
		parts.Clock,
		message.Flags(parts.ProducerAndFlags),
	)
}

// IndexedShuffleResponse is an implementation of message.Message which
// indexes a specific document within a ShuffleResponse.
type IndexedShuffleResponse struct {
	*ShuffleResponse
	Index int
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

/*

// TODO(johnny): Right now, I'm expecting to have a DeriveMessage which, like
// ShuffleMessage, indexes a given document of a DeriveResponse. These
// implementations need to be re-worked for it.

// SetUUID replaces the placeholder UUID string, which must exist, with the UUID.
func (d *Document) SetUUID(uuid message.UUID) {
	d.UuidParts = NewUUIDParts(uuid)

	// Require that the current content has a placeholder.
	// Replace it with the string-form UUID.
	var str = uuid.String()
	var ind = bytes.Index(d.Content, DocumentUUIDPlaceholder)
	if ind == -1 {
		panic("document UUID placeholder not found!")
	}
	copy(d.Content[ind:ind+36], str[0:36])
}

// NewAcknowledgement builds and returns an initialized RawJSON message having
// a placeholder UUID.
func (d *Document) NewAcknowledgement(pb.Journal) message.Message {
	return &Document{
		ContentType: Document_JSON,
		Content:     []byte(DocumentAckJSONTemplate),
	}
}

// MarshalJSONTo marshals a Document message with a following newline.
func (d *Document) MarshalJSONTo(bw *bufio.Writer) (int, error) {
	if d.ContentType != Document_JSON {
		panic("unexpected ContentType")
	}
	return bw.Write(d.Content)
}

// UnmarshalJSON sets the Document's Content with a copy of |data|,
// and the JSON ContentType.
func (d *Document) UnmarshalJSON(data []byte) error {
	d.ContentType = Document_JSON
	d.Content = append(d.Content[:0], data...)
	return nil
}
*/

var (
	// DocumentUUIDPointer is a JSON pointer of the location of the document UUID.
	DocumentUUIDPointer = "/_meta/uuid"
	// DocumentUUIDPlaceholder is a unique 36-byte sequence which is used to mark
	// the location within a document serialization which holds the document UUID.
	DocumentUUIDPlaceholder = []byte("DocUUIDPlaceholder-329Bb50aa48EAa9ef")
	// DocumentAckJSONTemplate is a JSON-encoded document template which serves
	// as a Gazette consumer transaction acknowledgement.
	DocumentAckJSONTemplate = `{"_meta":{"uuid":"` + string(DocumentUUIDPlaceholder) + `","ack":true}}` + "\n"
)
