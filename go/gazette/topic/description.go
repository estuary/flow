package topic

import (
	"fmt"
	"hash/fnv"
	"math/rand"

	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/message"
)

type Description struct {
	Name string
	// Number of partitions this topic utilizes. |Partitions| should generally
	// be a power-of-two.
	Partitions int
	// If non-nil, |RoutingKey| returns a stable routing key for |message|.
	// If not set, a random partition is selected.
	RoutingKey func(message interface{}) string
	// Builds or obtains a zero-valued instance of the topic message type.
	GetMessage func() message.Unmarshallable
	// If non-nil, returns a used instance of the message type. This is
	// typically used for pooling of message instances.
	PutMessage func(message.Unmarshallable)
}

func (d *Description) Journal(partition int) journal.Name {
	// TODO(johnny): This is horribly inefficient.
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
