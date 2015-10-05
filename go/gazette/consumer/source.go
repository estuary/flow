package topic

import (
	log "github.com/Sirupsen/logrus"
	"github.com/pippio/api-server/discovery"
	"sync"
	"time"
)

const (
	PumpTimes = 10
)

type Source struct {
	topic  *TopicDescription
	opener JournalOpener

	name        string
	etcd        discovery.EtcdService
	coordinator *ConsumerCoordinator

	messages chan JournalMessage

	producers map[string]*JournalProducer
	consumed  map[string]int64

	mu sync.Mutex
}

func NewSource(consumerName string, topic *TopicDescription,
	etcd discovery.EtcdService, opener JournalOpener) (*Source, error) {
	var err error

	c := &Source{
		name:      consumerName,
		topic:     topic,
		etcd:      etcd,
		opener:    opener,
		messages:  make(chan JournalMessage, 100),
		producers: make(map[string]*JournalProducer),
		consumed:  make(map[string]int64),
	}
	if c.coordinator, err = NewConsumerCoordinator(c); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Source) Stop() error {
	if err := c.coordinator.Cancel(); err != nil {
		return err
	}
	// TODO(johnny): Actually block until drained.
	time.Sleep(3 * time.Second)
	return nil
}

func (c *Source) Name() string                { return c.name }
func (c *Source) Topic() *TopicDescription    { return c.topic }
func (c *Source) Etcd() discovery.EtcdService { return c.etcd }

func (c *Source) StartConsuming(journal string, fromOffset int64) {
	log.WithFields(log.Fields{"journal": journal, "offset": fromOffset}).
		Info("StartConsuming")

	c.mu.Lock()
	c.consumed[journal] = fromOffset

	producer := NewJournalProducer(c.topic, c.opener, journal)
	c.producers[journal] = producer
	c.mu.Unlock()

	producer.StartProducingInto(fromOffset, c.messages)
	for i := 0; i < PumpTimes; i++ {
		producer.Pump()
	}
}

func (c *Source) StopConsuming(journal string) int64 {
	log.WithFields(log.Fields{"journal": journal}).
		Info("StopConsuming")

	c.mu.Lock()
	lastOffset := c.consumed[journal]
	delete(c.consumed, journal)

	c.producers[journal].Cancel()
	delete(c.producers, journal)
	c.mu.Unlock()

	return lastOffset
}

func (c *Source) ConsumingJournals() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make([]string, 0, len(c.consumed))
	for journal, _ := range c.consumed {
		out = append(out, journal)
	}
	return out
}

func (c *Source) ConsumedOffset(journal string) (int64, bool) {
	c.mu.Lock()
	offset, ok := c.consumed[journal]
	c.mu.Unlock()

	return offset, ok
}

func (c *Source) Next() JournalMessage {
	return <-c.messages
}

func (c *Source) Acknowledge(msg JournalMessage) {
	c.mu.Lock()
	producer, ok := c.producers[msg.Journal]
	if ok {
		producer.Pump()
		c.consumed[msg.Journal] = msg.NextOffset
	}
	c.mu.Unlock()
}
