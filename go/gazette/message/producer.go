package message

import (
	"io"
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
// TODO(johnny): This could use further refactoring, cleanup & tests.
type Producer struct {
	opener journal.Opener
	newMsg func() Unmarshallable

	pump chan struct{}
}

func NewProducer(opener journal.Opener, newMsg func() Unmarshallable) *Producer {
	return &Producer{
		opener: opener,
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
			reader, err = p.opener.OpenJournalAt(mark)

			if err != nil {
				log.WithFields(log.Fields{"mark": mark, "err": err}).
					Error("failed to open reader")
				time.Sleep(kProducerErrorTimeout)
				continue
			}
			decoder = NewDecoder(journal.NewMarkedReader(&mark, reader))
		}

		msg := p.newMsg()
		err = decoder.Decode(msg)

		if err != nil {
			if err == io.ErrUnexpectedEOF {
				varz.ObtainCount("gazette", "producer", "UnexpectedEOF").Add(1)
				time.Sleep(kProducerErrorTimeout)
			} else if err != io.EOF {
				log.WithFields(log.Fields{"mark": mark, "err": err}).Error("message decode")
				time.Sleep(kProducerErrorTimeout)
			}
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				reader.Close() // |reader| is invalidated and must be re-opened.
				reader = nil
			}
			if err != ErrDesyncDetected {
				// Desync is a special case, in that message decoding did actually
				// succeed despite the error.
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
	}
}
