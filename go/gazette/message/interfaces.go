// Package message defines a Message interface and Envelope type, and provides
// a Framing interface and implementations. It additionally defines function
// types and related routines for mapping a Message to a suitable journal.
package message

import (
	"bufio"
	"fmt"

	"go.gazette.dev/core/broker/protocol"
)

// Message is a user-defined, serializable type.
type Message interface{}

// Envelope combines a Message with its Journal, Fragment and byte offset.
type Envelope struct {
	Message
	Fragment    *protocol.Fragment
	JournalSpec *protocol.JournalSpec
	NextOffset  int64 // Offset of the next Message within the Journal.
}

// Framing specifies the serialization used to encode Messages within a topic.
type Framing interface {
	// ContentType of the Framing.
	ContentType() string
	// Marshal a Message to a bufio.Writer. Marshal may assume the Message has
	// passed validation, if implemented for the message type. It may ignore
	// any error returned by the provided Writer.
	Marshal(Message, *bufio.Writer) error
	// Unpack reads and returns a complete framed message from the Reader,
	// including any applicable message header or suffix. It returns an error of
	// the underlying Reader, or of a framing corruption. The returned []byte may
	// be invalidated by a subsequent use of the Reader or another Unpack call.
	Unpack(*bufio.Reader) ([]byte, error)
	// Unmarshals Message from the supplied frame, previously produced by Unpack.
	// It returns a Message-level decoding error, which does not invalidate the
	// framing or Reader (eg, further frames may be unpacked).
	Unmarshal([]byte, Message) error
}

// Fixupable is an optional Message type capable of being "fixed up" after
// decoding. This provides an opportunity to apply custom migrations or
// initialization after a generic or code-generated unmarshal has completed.
type Fixupable interface {
	Fixup() error
}

// MappingFunc maps a Message to a responsible journal. Gazette imposes no formal
// requirement on exactly how that mapping is performed, or the nature of the
// mapped journal.
//
// By convention, users will group a number of like journals together into a
// topic, with each journal playing the role of a partition of the topic. Such
// partitions can be easily distinguished through a JournalSpec Label such as
// "topic=my/topic/name". Note that "partition" and "topic" are useful
// terminology, but play no formal role and have no explicit implementation
// within Gazette (aside from their expression via Labels and LabelSelectors).
//
// A Mapper implementation would typically:
//  1) Apply domain knowledge to introspect the Message and determine a topic.
//  2) Query the broker List RPC to determine current partitions of the topic,
//     caching and periodically refreshing List results as needed.
//  3) Use a modulo or rendezvous hash mapping to select among partitions.
type MappingFunc func(msg Message) (protocol.Journal, Framing, error)

// MappingKeyFunc extracts an appropriate mapping key from the Message, optionally
// using the provided temporary buffer, and returns it.
type MappingKeyFunc func(Message, []byte) []byte

// PartitionsFunc returns a ListResponse of journal partitions from which a
// MappingFunc may select. The returned instance pointer may change across
// invocations, but a returned ListResponse may not be modified. PartitionsFunc
// should seek to preserve pointer equality of result instances when no
// substantive change has occurred. See also: client.PolledList.
type PartitionsFunc func() *protocol.ListResponse

// ErrEmptyListResponse is returned by a MappingFunc which received an empty
// ListResponse from a PartitionsFunc.
var ErrEmptyListResponse = fmt.Errorf("empty ListResponse")
