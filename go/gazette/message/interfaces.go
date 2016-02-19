package message

import (
	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/topic"
)

// Fixupable is a type capable of being "fixed up" after Unmarshalling.
// This provides an opportunity to apply migrations or initialization after
// a code-generated Unmarshal() implementation has completed.
type Fixupable interface {
	Fixup() error
}

type Message struct {
	// Topic of the message.
	Topic *topic.Description
	// Journal & offset of the message.
	Mark journal.Mark
	// Message value.
	Value topic.Unmarshallable
}
