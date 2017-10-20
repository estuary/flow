package topic

import (
	"sync"

	"github.com/LiveRamp/gazette/journal"
)

// A Publisher publishes Messages to a Topic.
type Publisher struct {
	journal.Writer
}

func NewPublisher(w journal.Writer) *Publisher {
	return &Publisher{Writer: w}
}

// Publish frames |msg|, routes it to the appropriate Topic partition, and
// writes the resulting encoding. If |msg| implements `Validate() error`,
// the message is Validated prior to framing, and any validation error returned.
func (p Publisher) Publish(msg Message, to *Description) error {
	// Enforce optional Message validation.
	if v, ok := msg.(interface {
		Validate() error
	}); ok {
		if err := v.Validate(); err != nil {
			return err
		}
	}

	if buffer, err := to.Framing.Encode(msg, publishBufferPool.Get().([]byte)); err != nil {
		return err
	} else if _, err = p.Writer.Write(to.MappedPartition(msg), buffer); err != nil {
		return err
	} else {
		publishBufferPool.Put(buffer[:0])
	}
	return nil
}

var publishBufferPool = sync.Pool{
	New: func() interface{} { return make([]byte, 0, 4096) },
}
