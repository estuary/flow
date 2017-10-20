package topic

import (
	"bufio"
	"fmt"
	"hash/fnv"

	"github.com/LiveRamp/gazette/journal"
)

// Message is a discrete piece of topic content.
type Message interface{}

// Framing specifies the serialization used to encode Messages within a topic.
type Framing interface {
	// Encode appends the serialization of |msg| onto buffer |b|,
	// returning the resulting buffer.
	Encode(msg Message, b []byte) ([]byte, error)
	// Unpack reads and returns a complete framed message from the Reader,
	// including any applicable message header or suffix. It returns an error of
	// the underlying Reader, or of a framing corruption. The returned []byte may
	// be invalidated by a subsequent use of the Reader or Extract call.
	Unpack(*bufio.Reader) ([]byte, error)
	// Unmarshals Message from the supplied frame, previously produced by Extract.
	// It returns a Message-level decoding error, which does not invalidate the
	// framing or Reader (eg, further frames may be extracted).
	Unmarshal([]byte, Message) error
}

// Fixupable is an optional Message type capable of being "fixed up" after
// decoding. This provides an opportunity to apply migrations or
// initialization after a code-generated decode implementation has completed.
type Fixupable interface {
	Fixup() error
}

// Description details the required properties of a Topic implementation.
type Description struct {
	// Name of the Topic. Topics are typically named and arranged in a directory
	// structure using forward-slash delimiters, and the topic Name is a prefix
	// for all its Partition names. This is a convention only and is not enforced,
	// and some use cases may motivate exceptions to the rule.
	Name string
	// Partitions returns Journal partitions of this Topic. The returned Journals
	// may be held constant, or may change across invocations (eg, in response to
	// a dynamic topic watch). Partitions is called frequently, and care should
	// be taken to avoid excessive allocation. However, a returned slice may not
	// be mutated (shallow equality must be sufficient to detect changes). The
	// returned Journals may be empty.
	Partitions func() []journal.Name `json:"-"`
	// MappedPartition returns the journal.Name which Message maps to, under
	// Topic routing. It is not required that the returned Journal be a member
	// of Partitions: MappedPartition may be used to implicitly create new
	// Journals as required.
	MappedPartition func(Message) journal.Name `json:"-"`

	// Builds or obtains a zero-valued instance of the topic message type.
	GetMessage func() Message `json:"-"`
	// If non-nil, returns a used instance of the message type. This is
	// typically used for pooling of message instances.
	PutMessage func(Message) `json:"-"`
	// Serialization used for Topic messages.
	Framing
}

// Partition pairs a Gazette Journal with the topic it implements.
type Partition struct {
	Topic   *Description
	Journal journal.Name
}

// Envelope combines a Message with its topic and specific journal Mark.
type Envelope struct {
	// Topic of the message.
	Topic *Description
	// Journal & offset of the message.
	journal.Mark
	// Message value.
	Message
}

// Returns the Partition of the message Envelope.
func (e *Envelope) Partition() Partition {
	return Partition{Topic: e.Topic, Journal: e.Mark.Journal}
}

// EnumeratePartitions returns a closure suitable for use as
// Description.Partitions, which defines |partitions| sorted partitions
// prefixed with |name| and having a "part-123" suffix.
func EnumeratePartitions(name string, partitions int) func() []journal.Name {
	var result []journal.Name

	for i := 0; i < partitions; i++ {
		result = append(result, journal.Name(fmt.Sprintf("%s/part-%03d", name, i)))
	}
	return func() []journal.Name {
		return result
	}
}

// ModuloPartitionMapping returns a closure which maps a Message into a stable
// member of |partitions| using modulo arithmetic. It requires a |routingKey|
// function, which extracts and encodes a key from Message,
// returning the result of appending it to the argument []byte.
func ModuloPartitionMapping(partitions func() []journal.Name,
	routingKey func(Message, []byte) []byte) func(Message) journal.Name {

	return func(msg Message) journal.Name {
		var tmp [32]byte

		var h = fnv.New32a()
		h.Write(routingKey(msg, tmp[:0]))

		var parts = partitions()
		return parts[int(h.Sum32())%len(parts)]
	}
}
