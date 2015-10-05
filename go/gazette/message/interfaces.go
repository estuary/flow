package message

import (
	"github.com/pippio/gazette/journal"
)

// Marshallable is a type capable of sizing & serializing itself to a []byte.
type Marshallable interface {
	// Returns the serialized byte size of the marshalled message.
	Size() int
	// Serializes the message to |buffer| (which must have len >= Size()),
	// returning the number of bytes written and any error.
	MarshalTo(buffer []byte) (n int, err error)
}

// Unmarshallable is a type capable of deserializing itself from a []byte.
type Unmarshallable interface {
	// De-serializes the message from |buffer| (which must contain a message exactly),
	// returning any error.
	Unmarshal(buffer []byte) error
}

// Fixupable is a type capable of being "fixed up" after Unmarshalling.
// This provides an opportunity to apply migrations or initialization after
// a code-generated Unmarshal() implementation has completed.
type Fixupable interface {
	Fixup() error
}

type Message struct {
	// Journal & offset of the message.
	Mark journal.Mark
	// Message value.
	Value Unmarshallable
}
