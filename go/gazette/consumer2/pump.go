package consumer

import (
	log "github.com/Sirupsen/logrus"

	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/message"
	"github.com/pippio/gazette/topic"
)

type pump struct {
	getter   journal.Getter
	sink     chan<- message.Message
	cancelCh <-chan struct{}
}

func newPump(get journal.Getter, sink chan<- message.Message, cancel <-chan struct{}) *pump {
	return &pump{getter: get, sink: sink, cancelCh: cancel}
}

func (p *pump) pump(topic *topic.Description, mark journal.Mark) {
	log.WithField("mark", mark).Info("now consuming from journal")

	rr := journal.NewRetryReader(mark, p.getter)
	defer func() {
		if err := rr.Close(); err != nil {
			log.WithField("err", err).Warn("failed to Close RetryReader")
		}
	}()

	decoder := message.NewDecoder(rr)

	for {
		msg := topic.GetMessage()

		if err := decoder.Decode(msg); err != nil {
			log.WithFields(log.Fields{"mark": rr.Mark, "err": err}).Error("message decode")
			continue
		}

		select {
		case p.sink <- message.Message{topic, rr.Mark, msg}:
		case <-p.cancelCh:
			return
		}
	}
}
