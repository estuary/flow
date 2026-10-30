package fragment

import (
	"context"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/keyspace"
)

const (
	persistInterval       = time.Minute
	maxConcurrentPersists = 32
)

// Persister asynchronously persists completed Fragments to their backing pb.FragmentStore.
type Persister struct {
	qA, qB, qC []Spool
	mu         sync.Mutex
	doneCh     chan struct{}
	sem        chan struct{}
	ks         *keyspace.KeySpace
	ticker     *time.Ticker
	persistFn  func(context.Context, Spool, *pb.JournalSpec, bool) error
}

// NewPersister returns an empty, initialized Persister.
func NewPersister(ks *keyspace.KeySpace) *Persister {
	return &Persister{
		doneCh:    make(chan struct{}),
		sem:       make(chan struct{}, maxConcurrentPersists),
		ks:        ks,
		persistFn: Persist,
	}
}

func (p *Persister) SpoolComplete(spool Spool, primary bool) {
	// TODO(johnny): Turn this into a panic() assertion, as it's an invariant
	// of SpoolObserver that's upheld in spool.go.
	if spool.ContentLength() == 0 {
		return // No-op.
	}

	if primary {
		go func() {
			p.sem <- struct{}{}
			defer func() { <-p.sem }()
			p.attemptPersist(spool, false /* not exiting */)
		}()
	} else {
		p.queue(spool)
	}
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
	if p.ticker == nil {
		p.ticker = time.NewTicker(persistInterval)
	}
	for done, exiting := false, false; !done; {
		if !exiting {
			select {
			case <-p.ticker.C:
			case <-p.doneCh:
				exiting = true
				p.ticker.Stop()
			}
		}

		for _, spool := range p.qA {
			p.attemptPersist(spool, exiting)
		}

		// Rotate queues.
		p.mu.Lock()
		p.qA, p.qB, p.qC = p.qB, p.qC, nil

		if exiting && len(p.qA) == 0 && len(p.qB) == 0 {
			done = true
		}
		p.mu.Unlock()
	}
	close(p.doneCh)
}

// attemptPersist persist valid spools, dropping spools with no content or no configured
// backing store. If persistFn fails the spool will be requeued and be attempted again.
func (p *Persister) attemptPersist(spool Spool, exiting bool) {
	// TODO(johnny): Remove; it's a contract invariant (except for unit tests).

	if spool.ContentLength() == 0 {
		// Persisting an empty Spool is a no-op.
		return
	}
	// Attach the current BackingStore of the Fragment's JournalSpec.
	p.ks.Mu.RLock()
	var item, ok = allocator.LookupItem(p.ks, spool.Journal.String())
	p.ks.Mu.RUnlock()

	if !ok {
		log.WithFields(log.Fields{
			"journal": spool.Journal,
			"name":    spool.ContentName(),
		}).Info("dropping Spool (JournalSpec was removed)")

		spoolPersistedTotal.Inc()
		return
	}

	var spec = item.ItemValue.(*pb.JournalSpec)
	if err := p.persistFn(context.Background(), spool, spec, exiting); err != nil {
		log.WithFields(log.Fields{
			"journal": spool.Journal,
			"name":    spool.ContentName(),
			"err":     err,
		}).Warn("failed to persist Spool (will retry)")
		p.queue(spool)
	} else {
		spoolPersistedTotal.Inc()
	}
}
