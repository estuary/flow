package consumer

import (
	"bufio"

	log "github.com/Sirupsen/logrus"

	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/topic"
)

type pump struct {
	getter   journal.Getter
	sink     chan<- topic.Envelope
	cancelCh <-chan struct{}
}

func newPump(get journal.Getter, sink chan<- topic.Envelope, cancel <-chan struct{}) *pump {
	return &pump{getter: get, sink: sink, cancelCh: cancel}
}

func (p *pump) pump(desc *topic.Description, mark journal.Mark) {
	log.WithField("mark", mark).Info("now consuming from journal")

	var rr = journal.NewRetryReader(mark, p.getter)
	defer func() {
		if err := rr.Close(); err != nil {
			log.WithField("err", err).Warn("failed to Close RetryReader")
		}
	}()

	var br = bufio.NewReader(rr)

	for {
		var frame, err = desc.Framing.Unpack(br)
		if err != nil {
			log.WithFields(log.Fields{"mark": rr.AdjustedMark(br), "err": err}).Error("unpacking frame")
			continue
		}

		var msg = desc.GetMessage()
		if err := desc.Framing.Unmarshal(frame, msg); err == topic.ErrDesyncDetected {
			// Only WARN level log for desync.
			// See https://jira.liveramp.com/browse/PUB-1777 for detail.
			log.WithFields(log.Fields{"mark": rr.Mark, "err": err}).Warn("message decode")
			continue
		} else if err != nil {
			log.WithFields(log.Fields{"mark": rr.AdjustedMark(br), "err": err}).Error("message decode")
			continue
		}

		select {
		case p.sink <- topic.Envelope{desc, rr.AdjustedMark(br), msg}:
		case <-p.cancelCh:
			return
		}
	}
}
