package message

import (
	"sync"
	"sync/atomic"

	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/topic"
)

var sinkBufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 4096)
	},
}

const DefaultAutoFlushRate = 10000

type Sink struct {
	Topic  *topic.Description
	Writer journal.Writer

	// If non-zero, one of every |AutoFlushRate| Put()'s will be blocking
	// regardless of the |block| parameter. This effectively rate-limits
	// processors producing large amounts of data, allowing them to (in turn)
	// apply back-pressure to upstream topic consumption.
	AutoFlushRate    int64
	autoFlushCounter int64
}

func NewSink(topic *topic.Description, writer journal.Writer) Sink {
	return Sink{Topic: topic, Writer: writer, AutoFlushRate: DefaultAutoFlushRate}
}

func (s *Sink) Put(msg topic.Marshallable, block bool) error {
	var err error
	var done *journal.AsyncAppend

	buffer := sinkBufferPool.Get().([]byte)

	err = Frame(msg, &buffer)
	if err == nil {
		done, err = s.Writer.Write(s.Topic.RoutedJournal(msg), buffer)
	}
	sinkBufferPool.Put(buffer)

	if err != nil {
		return err
	}
	if block || (s.AutoFlushRate != 0 &&
		atomic.AddInt64(&s.autoFlushCounter, 1)%s.AutoFlushRate == 0) {
		<-done.Ready
	}
	return nil
}
