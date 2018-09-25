package fragment

import (
	"context"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type Persister struct {
	qA, qB, qC []Spool
	mu         sync.Mutex
	doneCh     chan struct{}
}

// NewPersister returns an empty, initialized Persister.
func NewPersister() *Persister {
	return &Persister{doneCh: make(chan struct{})}
}

func (p *Persister) SpoolComplete(spool Spool, primary bool) {
	if spool.ContentLength() == 0 || spool.BackingStore == "" {
		// Cannot persist this Spool.
	} else if primary {
		// Attempt to immediately persist the Spool.
		go func() {
			if err := Persist(context.Background(), spool); err != nil {
				log.WithField("err", err).Warn("failed to persist Spool")
				p.queue(spool)
			}
		}()
	} else {
		p.queue(spool)
	}
	return
}

func (p *Persister) Finish() {
	p.doneCh <- struct{}{}
	<-p.doneCh
}

func (p *Persister) queue(spool Spool) {
	defer p.mu.Unlock()
	p.mu.Lock()

	p.qC = append(p.qC, spool)
}

func (p *Persister) Serve() {
	for done, exiting := false, false; !done; {

		if !exiting {
			select {
			case <-time.After(time.Minute):
			case <-p.doneCh:
				exiting = true
			}
		}

		for _, spool := range p.qA {
			if err := Persist(context.Background(), spool); err != nil {
				log.WithField("err", err).Warn("failed to persist Spool")
				p.queue(spool)
			}
		}

		// Rotate queues.
		p.mu.Lock()
		p.qA, p.qB, p.qC = p.qB, p.qC, p.qA[:0]

		if exiting && len(p.qA) == 0 && len(p.qB) == 0 {
			done = true
		}
		p.mu.Unlock()
	}
	close(p.doneCh)
}
