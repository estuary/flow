package topic

import (
	"fmt"
	"hash/fnv"
	"math/rand"
)

type TopicDescription struct {
	Name       string
	Partitions int

	// Returns the serialized byte-length of |message|.
	Size func(message interface{}) int

	// Serializes |message| to []byte, returning error.
	MarshalTo func(message interface{}, to []byte) error

	// Extracts a serialized message from []byte, returning error.
	Unmarshal func(buffer []byte) (interface{}, error)

	// Extracts a routing key to be used for the message.
	RoutingKey func(message interface{}) string
}

func (d *TopicDescription) Journal(partition int) string {
	// TODO(johnny): This is horribly inefficient.
	return fmt.Sprintf("%s/part-%03d", d.Name, partition)
}

func (d *TopicDescription) RoutedJournal(message interface{}) string {
	key := d.RoutingKey(message)

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
