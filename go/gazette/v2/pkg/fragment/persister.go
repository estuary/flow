package fragment

import (
	"context"
	"sync"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/allocator"
	"github.com/LiveRamp/gazette/v2/pkg/keyspace"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	log "github.com/sirupsen/logrus"
)

const (
	persistInterval = time.Minute
)

type Persister struct {
	qA, qB, qC []Spool
	mu         sync.Mutex
	doneCh     chan struct{}
	ks         *keyspace.KeySpace
	ticker     *time.Ticker
	persistFn  func(ctx context.Context, spool Spool) error
}

// NewPersister returns an empty, initialized Persister.
func NewPersister(ks *keyspace.KeySpace) *Persister {
	return &Persister{
		doneCh:    make(chan struct{}),
		ks:        ks,
		persistFn: Persist,
		ticker:    time.NewTicker(persistInterval),
	}
}

func (p *Persister) SpoolComplete(spool Spool, primary bool) {
	if primary {
		// Attempt to immediately persist the Spool.
		go p.attemptPersist(spool)
	} else if spool.ContentLength() != 0 {
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
			case <-p.ticker.C:
			case <-p.doneCh:
				exiting = true
				p.ticker.Stop()
			}
		}

		for _, spool := range p.qA {
			p.attemptPersist(spool)
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

// attemptPersist persist valid spools, dropping spools with no content or no configured
// backing store. If persistFn fails the spool will be requeued and be attmepted again.
func (p *Persister) attemptPersist(spool Spool) {
	if spool.ContentLength() == 0 {
		// Persisting an empty Spool is a no-op.
		return
	}
	// Attach the current BackingStore of the Fragment's JournalSpec.
	if item, ok := allocator.LookupItem(p.ks, spool.Journal.String()); ok {
		var spec = item.ItemValue.(*pb.JournalSpec)
		// Journal spec has no configured store, drop this fragment.
		if len(spec.Fragment.Stores) == 0 {
			return
		}
		spool.BackingStore = spec.Fragment.Stores[0]
	} else {
		log.WithFields(log.Fields{
			"journal": spool.Journal,
			"name":    spool.ContentName(),
		}).Warn("dropping Spool (JournalSpec was removed)")
		return
	}

	if err := p.persistFn(context.Background(), spool); err != nil {
		log.WithFields(log.Fields{
			"journal": spool.Journal,
			"name":    spool.ContentName(),
			"err":     err,
		}).Warn("failed to persist Spool (will retry)")
		p.queue(spool)
	}
}
