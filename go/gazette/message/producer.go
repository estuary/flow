package message

import (
	log "github.com/Sirupsen/logrus"

	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/topic"
)

// Producer produces unmarshalled messages from a journal.
// TODO(johnny): This is deprecated / will be eliminated with V1 consumers.
type Producer struct {
	getter journal.Getter
	topic  *topic.Description

	signal chan struct{}
}

func NewProducer(getter journal.Getter, topic *topic.Description) *Producer {
	return &Producer{
		getter: getter,
		topic:  topic,
		signal: make(chan struct{}),
	}
}

func (p *Producer) StartProducingInto(mark journal.Mark, sink chan<- Message) {
	sp := &simpleProducer{
		mark:   mark,
		getter: p.getter,
		topic:  p.topic,
		sink:   sink,
		signal: p.signal,
	}
	go sp.loop()
}

func (p *Producer) Pump() {
	// TODO(johnny): Implement regulated producer for better journal leveling.
}

func (p *Producer) Cancel() {
	close(p.signal)
}

// Composable type which opens, reads, decodes, and pushes messages into a sink.
type simpleProducer struct {
	mark   journal.Mark
	getter journal.Getter
	topic  *topic.Description
	// Sink into which decoded messages are written. Not closed on exit.
	sink chan<- Message
	// Messaged to signal intent to exit. Closed when simpleProducer exits.
	signal chan struct{}
}

func (p *simpleProducer) loop() {
	rr := journal.NewRetryReader(p.mark, p.getter)
	decoder := NewDecoder(rr)

	defer func() {
		if err := rr.Close(); err != nil {
			log.WithField("err", err).Warn("failed to Close RetryReader")
		}
	}()

	for done := false; !done; {
		select {
		case <-p.signal:
			done = true
			continue
		default:
			// Pass.
		}
		msg := p.topic.GetMessage()

		if err := decoder.Decode(msg); err != nil {
			log.WithFields(log.Fields{"mark": p.mark, "err": err}).Error("message decode")
		}

		select {
		case p.sink <- Message{p.topic, rr.Mark, msg}:
			p.mark = rr.Mark
		case <-p.signal:
			done = true
		}
	}
}
