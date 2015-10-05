package topic

import (
	"sync"
	"sync/atomic"

	"github.com/pippio/api-server/gazette"
)

var sinkBufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 4096)
	},
}

const DefaultAutoFlushRate = 10000

type Sink struct {
	Topic  *TopicDescription
	Client *gazette.WriteClient

	// If non-zero, one of every |AutoFlushRate| Put()'s will be blocking
	// regardless of the |block| parameter. This effectively rate-limits
	// processors producing large amounts of data, allowing them to (in turn)
	// apply back-pressure to upstream topic consumption.
	AutoFlushRate    int64
	autoFlushCounter int64
}

func NewSink(topic *TopicDescription, client *gazette.WriteClient) Sink {
	return Sink{Topic: topic, Client: client, AutoFlushRate: DefaultAutoFlushRate}
}

func (s *Sink) Put(message interface{}, block bool) error {
	var err error
	var done chan struct{}

	buffer := sinkBufferPool.Get().([]byte)

	err = frame(s.Topic, message, &buffer)
	if err == nil {
		done, err = s.Client.Write(s.Topic.RoutedJournal(message), buffer)
	}
	sinkBufferPool.Put(buffer)

	if err != nil {
		return err
	}
	if block || (s.AutoFlushRate != 0 &&
		atomic.AddInt64(&s.autoFlushCounter, 1)%s.AutoFlushRate == 0) {
		<-done
	}
	return nil
}
