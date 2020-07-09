package protocol

import (
	"bufio"
	"bytes"
	"encoding/binary"

	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
)

var _ message.Message = new(Document)

// Pack these UUIDParts into a message.UUID.
func (parts *UUIDParts) Pack() message.UUID {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], parts.ProducerAndFlags)
	var producerID message.ProducerID
	copy(producerID[:], tmp[:6])

	return message.BuildUUID(
		producerID,
		parts.Clock,
		message.Flags(parts.ProducerAndFlags),
	)
}

// Less returns true if this Document_Shuffle orders before |other|,
// under the Shuffle's (RingIndex, TransformId) ordering.
func (s Document_Shuffle) Less(other Document_Shuffle) bool {
	if s.RingIndex != other.RingIndex {
		return s.RingIndex < other.RingIndex
	}
	return s.TransformId < other.TransformId
}

// GetUUID fetches the UUID of the Document.
func (d *Document) GetUUID() message.UUID { return d.UuidParts.Pack() }

// SetUUID replaces the placeholder UUID string, which must exist, with the UUID.
func (d *Document) SetUUID(uuid message.UUID) {
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
	d.Content = data
	return nil
}

var (
	// DocumentUUIDPointer is a JSON pointer of the location of the document UUID.
	DocumentUUIDPointer = "/_meta/uuid"
	// DocumentUUIDPlaceholder is a unique 36-byte sequence which is used to mark
	// the location within a document serialization which holds the document UUID.
	DocumentUUIDPlaceholder = []byte("DocUUIDPlaceholder-329Bb50aa48EAa9ef")
	// DocumentAckJSONTemplate is a JSON-encoded document template which serves
	// as a Gazette consumer transaction acknowledgement.
	DocumentAckJSONTemplate = `{"_meta":{"uuid":"` + string(DocumentUUIDPlaceholder) + `","ack": true}}"`
)
