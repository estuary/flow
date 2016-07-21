package message

import (
	"sync"

	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/topic"
)

// Implementation of topic.Publisher, which frames, routes, and writes
// the message to the appropriate Journal.
type SimplePublisher struct {
	journal.Writer
}

func (p SimplePublisher) Publish(msg topic.Marshallable, to *topic.Description) error {
	buffer := publishBufferPool.Get().([]byte)

	err := Frame(msg, &buffer)
	if err == nil {
		_, err = p.Writer.Write(to.RoutedJournal(msg), buffer)
	}
	publishBufferPool.Put(buffer)
	return err
}

var publishBufferPool = sync.Pool{
	New: func() interface{} { return make([]byte, 4096) },
}
