package consumer

import (
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/pippio/api-server/discovery"
	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/message"
	"github.com/pippio/gazette/topic"
)

const (
	PumpTimes = 10
)

type Source struct {
	topic  *topic.Description
	getter journal.Getter

	name        string
	etcd        discovery.EtcdService
	coordinator *ConsumerCoordinator

	messages chan message.Message

	producers map[journal.Name]*message.Producer
	consumed  map[journal.Name]int64

	mu sync.Mutex
}

func NewSource(consumerName string, topic *topic.Description,
	etcd discovery.EtcdService, getter journal.Getter) (*Source, error) {
	var err error

	c := &Source{
		name:      consumerName,
		topic:     topic,
		etcd:      etcd,
		getter:    getter,
		messages:  make(chan message.Message, 100),
		producers: make(map[journal.Name]*message.Producer),
		consumed:  make(map[journal.Name]int64),
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
func (c *Source) Topic() *topic.Description   { return c.topic }
func (c *Source) Etcd() discovery.EtcdService { return c.etcd }

func (c *Source) StartConsuming(mark journal.Mark) {
	log.WithFields(log.Fields{"mark": mark}).Info("StartConsuming")

	c.mu.Lock()
	c.consumed[mark.Journal] = mark.Offset

	producer := message.NewProducer(c.getter, c.topic.GetMessage)
	c.producers[mark.Journal] = producer
	c.mu.Unlock()

	producer.StartProducingInto(mark, c.messages)
	for i := 0; i < PumpTimes; i++ {
		producer.Pump()
	}
}

func (c *Source) StopConsuming(name journal.Name) int64 {
	log.WithFields(log.Fields{"journal": name}).Info("StopConsuming")

	c.mu.Lock()
	lastOffset := c.consumed[name]
	delete(c.consumed, name)

	c.producers[name].Cancel()
	delete(c.producers, name)
	c.mu.Unlock()

	return lastOffset
}

func (c *Source) ConsumingJournals() []journal.Name {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make([]journal.Name, 0, len(c.consumed))
	for journal, _ := range c.consumed {
		out = append(out, journal)
	}
	return out
}

func (c *Source) ConsumedOffset(name journal.Name) (int64, bool) {
	c.mu.Lock()
	offset, ok := c.consumed[name]
	c.mu.Unlock()

	return offset, ok
}

func (c *Source) Next() message.Message {
	return <-c.messages
}

func (c *Source) Acknowledge(msg message.Message) {
	c.mu.Lock()
	producer, ok := c.producers[msg.Mark.Journal]
	if ok {
		producer.Pump()
		c.consumed[msg.Mark.Journal] = msg.Mark.Offset
	}
	c.mu.Unlock()
}
