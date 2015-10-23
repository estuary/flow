package message

import (
	"io"
	"net"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/pippio/api-server/varz"
	"github.com/pippio/gazette/journal"
)

const (
	kProducerErrorTimeout = time.Second * 5

	// This should be tuned to the maximum size of the production window.
	kProducerPumpWindow = 100
)

// Producer produces unmarshalled messages from a journal.
type Producer struct {
	getter journal.Getter
	newMsg func() Unmarshallable

	pump chan struct{}
}

func NewProducer(getter journal.Getter, newMsg func() Unmarshallable) *Producer {
	return &Producer{
		getter: getter,
		newMsg: newMsg,
		pump:   make(chan struct{}, kProducerPumpWindow),
	}
}

func (p *Producer) StartProducingInto(mark journal.Mark, sink chan<- Message) {
	go p.loop(mark, sink)
}

func (p *Producer) Pump() {
	p.pump <- struct{}{}
}

func (p *Producer) Cancel() {
	close(p.pump)
}

func (p *Producer) loop(mark journal.Mark, sink chan<- Message) {
	// Messages which can be sent, before we must block on a pump.
	var window int

	var err error
	var reader io.ReadCloser
	var markReader *journal.MarkedReader
	var decoder Decoder

	for done := false; !done; {
		select {
		case _, ok := <-p.pump:
			if ok {
				window += 1
			} else {
				done = true
			}
			continue
		default: // Non-blocking.
		}

		// Ensure an open reader & decoder.
		if reader == nil {
			args := journal.ReadArgs{
				Journal:  mark.Journal,
				Offset:   mark.Offset,
				Blocking: true,
			}
			var result journal.ReadResult
			result, reader = p.getter.Get(args)

			if result.Error != nil {
				log.WithFields(log.Fields{"args": args, "err": result.Error}).
					Warn("failed to open reader")
				time.Sleep(kProducerErrorTimeout)
				continue
			}

			mark.Offset = result.Offset
			markReader = journal.NewMarkedReader(mark, reader)
			decoder = NewDecoder(markReader)
		}

		msg := p.newMsg()
		err = decoder.Decode(msg)

		if err != nil {
			_, isNetErr := err.(net.Error)

			if isNetErr {
				varz.ObtainCount("gazette", "producer", "NetError").Add(1)
				time.Sleep(kProducerErrorTimeout)
			} else if err != io.EOF && err != io.ErrUnexpectedEOF {
				log.WithFields(log.Fields{"mark": mark, "err": err}).Error("message decode")
				time.Sleep(kProducerErrorTimeout)
			}

			// Desync is a special case, in that message decoding did actually
			// succeed despite the error.
			if err != ErrDesyncDetected {
				reader.Close() // |reader| is invalidated and must be re-opened.
				reader = nil
				markReader = nil
				continue
			}
		}

		if window == 0 {
			// Blocking read of |pump| until close, or window opens.
			_, ok := <-p.pump
			if ok {
				window += 1
			} else {
				done = true
				continue
			}
		}
		window -= 1

		sink <- Message{mark, msg}
		mark = markReader.Mark
	}
}
