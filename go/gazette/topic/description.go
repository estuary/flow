package topic

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"time"

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

type Description struct {
	Name string
	// Number of partitions this topic utilizes. |Partitions| should generally
	// be a power-of-two.
	Partitions int
	// If non-nil, |RoutingKey| returns a stable routing key for |message|.
	// If not set, a random partition is selected.
	RoutingKey func(message interface{}) string `json:"-"`
	// Builds or obtains a zero-valued instance of the topic message type.
	GetMessage func() Unmarshallable `json:"-"`
	// If non-nil, returns a used instance of the message type. This is
	// typically used for pooling of message instances.
	PutMessage func(Unmarshallable) `json:"-"`
	// How far back from the present we must keep the data around.
	RetentionDuration time.Duration
}

func (d *Description) Journal(partition int) journal.Name {
	return journal.Name(fmt.Sprintf("%s/part-%03d", d.Name, partition))
}

func (d *Description) RoutedJournal(message interface{}) journal.Name {
	var key string
	if d.RoutingKey != nil {
		key = d.RoutingKey(message)
	}
	// Map to a partition by routing key, if available. Otherwise choose
	// one at random.
	var partition int

	if key != "" {
		h := fnv.New32a()
		h.Write([]byte(key))
		partition = int(h.Sum32()) % d.Partitions
	} else {
		partition = rand.Int() % d.Partitions
	}
	return d.Journal(partition)
}

// A Sink publishes a Marshallable message to a topic, optionally blocking.
// TODO(johnny): Deprecated. Will be removed with V2 consumers.
type Sink interface {
	Put(msg Marshallable, block bool) error
}

// A Publisher is a journal.Writer, which also knows how to publish
// a Marshallable message to a topic.
type Publisher interface {
	journal.Writer

	Publish(msg Marshallable, to *Description) error
}
