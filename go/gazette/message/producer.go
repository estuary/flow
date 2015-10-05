package topic

import (
	log "github.com/Sirupsen/logrus"
	"io"
	"net/http"
	"time"
)

const (
	kJournalProducerErrorTimeout = time.Second * 5

	// This should be tuned to the maximum size of the production window.
	kJournalProducerPumpWindow = 100
)

type JournalProducer struct {
	topic   *TopicDescription
	journal string
	opener  JournalOpener

	pump chan struct{}
}

type JournalMessage struct {
	Journal string
	Message interface{}

	NextOffset int64
}

type JournalOpener interface {
	OpenJournalAt(journal string, offset int64) (io.ReadCloser, error)
	HeadJournalAt(journal string, offset int64) (*http.Response, error)
}

func NewJournalProducer(topic *TopicDescription, opener JournalOpener,
	journal string) *JournalProducer {

	return &JournalProducer{
		topic:   topic,
		journal: journal,
		opener:  opener,
		pump:    make(chan struct{}, kJournalProducerPumpWindow),
	}
}

func (p *JournalProducer) StartProducingInto(offset int64,
	messages chan<- JournalMessage) *JournalProducer {
	go p.loop(offset, messages)
	return p
}

func (p *JournalProducer) Pump() {
	p.pump <- struct{}{}
}

func (p *JournalProducer) Cancel() {
	close(p.pump)
}

func (p *JournalProducer) loop(offset int64, messages chan<- JournalMessage) {
	var scanner *MessageScanner

	// Messages which can be sent, before we must block on a pump.
	var window int

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

		if scanner == nil {
			if reader, err := p.opener.OpenJournalAt(p.journal, offset); err != nil {
				log.WithFields(log.Fields{"journal": p.journal, "offset": offset,
					"err": err}).Error("failed to open reader")
				time.Sleep(kJournalProducerErrorTimeout)
				continue
			} else {
				scanner = NewMessageScanner(p.topic, reader, &offset)
			}
		}

		if err, recoverable := scanner.Next(); err != nil && !recoverable {
			if err != io.EOF {
				log.WithFields(log.Fields{"journal": p.journal, "offset": offset,
					"err": err}).Warn("error while scanning message")
				time.Sleep(kJournalProducerErrorTimeout)
			}
			scanner.Close()
			scanner = nil
			continue
		} else if err != nil {
			log.WithFields(log.Fields{"journal": p.journal, "offset": offset,
				"err": err}).Error("recoverable error while scanning message")

			time.Sleep(kJournalProducerErrorTimeout)
			if scanner.Message == nil {
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

		messages <- JournalMessage{
			Journal:    p.journal,
			Message:    scanner.Message,
			NextOffset: offset,
		}
	}
}
