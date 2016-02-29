package consumer

import (
	"sync"

	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/message"
	"github.com/pippio/gazette/topic"
)

// All messages emitted under a shard should be issued via |Publish|.
// Eventually, this method will track partitions written to under the
// current transaction (for later confirmation), and will also ensure that
// messages are appropriately tagged and sequenced.
type publisher struct {
	journal.Writer
}

func (p publisher) Publish(msg topic.Marshallable, to *topic.Description) error {
	buffer := publishBufferPool.Get().([]byte)

	err := message.Frame(msg, &buffer)
	if err == nil {
		_, err = p.Writer.Write(to.RoutedJournal(msg), buffer)
	}
	publishBufferPool.Put(buffer)
	return err
}

var publishBufferPool = sync.Pool{
	New: func() interface{} { return make([]byte, 4096) },
}
