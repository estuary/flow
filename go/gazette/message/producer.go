package message

import (
	"io"
	"net"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/pippio/api-server/varz"
	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/topic"
)

const kProducerErrorTimeout = time.Second * 5

// Producer produces unmarshalled messages from a journal.
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
	// Current reader and decoder.
	var rawReader io.ReadCloser
	var markReader *journal.MarkedReader
	var decoder Decoder

	// Always close |rawReader| on exit.
	defer func() {
		if rawReader != nil {
			if err := rawReader.Close(); err != nil {
				log.WithField("err", err).Warn("failed to Close rawReader")
			}
		}
	}()

	// |cooloff| is used to slow the producer loop on an error.
	// Initialize it as a closed channel (which selects immediately).
	cooloffCh := make(chan struct{})
	close(cooloffCh)

	triggerCooloff := func() {
		// Reset |cooloffCh| as a open channel (which blocks), and arrange to
		// close it again (unblocking the loop) after |kProducerErrorTimeout|.
		cooloffCh = make(chan struct{})
		time.AfterFunc(kProducerErrorTimeout, func() { close(cooloffCh) })
	}

	for done := false; !done; {
		select {
		case <-p.signal:
			done = true
			continue
		case <-cooloffCh:
		}

		// Ensure an open reader & decoder.
		if rawReader == nil {
			args := journal.ReadArgs{
				Journal:  p.mark.Journal,
				Offset:   p.mark.Offset,
				Blocking: true,
			}
			var result journal.ReadResult
			result, rawReader = p.getter.Get(args)

			if result.Error != nil {
				log.WithFields(log.Fields{"args": args, "err": result.Error}).
					Warn("failed to open reader")
				triggerCooloff()
				continue
			}

			p.mark.Offset = result.Offset
			markReader = journal.NewMarkedReader(p.mark, rawReader)
			decoder = NewDecoder(markReader)
		}

		msg := p.topic.GetMessage()
		err := decoder.Decode(msg)

		if err != nil {
			if _, isNetErr := err.(net.Error); isNetErr {
				varz.ObtainCount("gazette", "producer", "NetError").Add(1)
				triggerCooloff()
			} else if err != io.EOF && err != io.ErrUnexpectedEOF {
				log.WithFields(log.Fields{"mark": p.mark, "err": err}).Error("message decode")
				triggerCooloff()
			}

			// Desync is a special case, in that message decoding did actually
			// succeed despite the error.
			if err != ErrDesyncDetected {
				rawReader.Close() // |rawReader| is invalidated and must be re-opened.
				rawReader = nil
				markReader = nil
				continue
			}
		}

		select {
		case p.sink <- Message{p.topic, p.mark, msg}:
			p.mark = markReader.Mark
		case <-p.signal:
			done = true
		}
	}
}
