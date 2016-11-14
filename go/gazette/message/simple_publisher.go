package message

import (
	"sync"

	log "github.com/Sirupsen/logrus"

	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/topic"
)

// Implementation of topic.Publisher which optionally validates, frames,
// routes, and writes messages to the appropriate Journal.
type SimplePublisher struct {
	journal.Writer
}

func (p SimplePublisher) Publish(msg topic.Marshallable, to *topic.Description) error {
	if v, ok := msg.(interface {
		Validate() error
	}); ok {
		if err := v.Validate(); err != nil {
			// TODO(johnny): Temporarily log. In the future, return error.
			log.WithField("err", err).Error("event validation failed")
		}
	}
	var buffer = publishBufferPool.Get().([]byte)

	var err = Frame(msg, &buffer)
	if err == nil {
		_, err = p.Writer.Write(to.RoutedJournal(msg), buffer)
	}
	publishBufferPool.Put(buffer)
	return err
}

var publishBufferPool = sync.Pool{
	New: func() interface{} { return make([]byte, 4096) },
}
