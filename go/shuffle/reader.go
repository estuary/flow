package shuffle

import (
	"container/heap"
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/estuary/flow/go/flow"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/keyspace"
	"go.gazette.dev/core/message"
)

type governor struct {
	rb *ReadBuilder
	// Resolved Timepoint of the present time.Time, which is updated as
	// each Timepoint.Next resolves.
	tp *flow.Timepoint
	// Wall-time clock which is updated with every |ticker| tick.
	wallTime message.Clock
	// MustPoll notes that poll() must run to completion before the next
	// Message may be returned by Next().
	mustPoll bool
	// Ongoing *reads having no ready Documents.
	pending map[*read]struct{}
	// *reads with Documents ready to emit, as a priority heap.
	queued readHeap
	// *reads with Documents having adjusted Clocks beyond |walltime|,
	// which must wait for a future tick in order to be processed.
	gated []*read
	// Journals having an active *read.
	active map[pb.Journal]*read
	// Offsets of journals which are not actively being read.
	idle map[pb.Journal]pb.Offset
	// Channel signaled by readers when a new ShuffleResponse has
	// been sent on the *read's channel. Used to wake poll() when
	// blocking for more data.
	readReadyCh chan struct{}
	// Channel which is signaled with each update of journals.
	journalsUpdateCh chan struct{}
}

// StartReadingMessages begins reading shuffled, ordered messages into the channel, from the given Checkpoint.
func StartReadingMessages(ctx context.Context, rb *ReadBuilder, cp pc.Checkpoint, tp *flow.Timepoint, ch chan<- consumer.EnvelopeOrError) {
	var offsets = make(pb.Offsets)
	for journal, meta := range cp.Sources {
		offsets[journal] = meta.ReadThrough
	}

	var g = &governor{
		rb:               rb,
		tp:               tp,
		mustPoll:         false,
		pending:          make(map[*read]struct{}),
		active:           make(map[pb.Journal]*read),
		idle:             offsets,
		readReadyCh:      make(chan struct{}, 1),
		journalsUpdateCh: make(chan struct{}, 1),
	}
	g.wallTime.Update(time.Now())

	// Spawn a loop which invokes Next() and passes the result to the output |ch|.
	go func() {
		var out consumer.EnvelopeOrError
		for out.Error == nil {
			out.Envelope, out.Error = g.Next(ctx)
			ch <- out
		}
	}()
	// Spawn a loop which wakes |journalsUpdateCh| on each revision update.
	go signalKeySpaceUpdates(ctx, rb.journals, g.journalsUpdateCh)

	return
}

func (g *governor) Next(ctx context.Context) (message.Envelope, error) {
	for {
		if g.mustPoll || len(g.queued) == 0 {
			if err := g.poll(ctx); err == errPollAgain {
				g.mustPoll = true
				continue
			} else if err != nil {
				return message.Envelope{}, err
			} else {
				g.mustPoll = false // poll() completed.
			}
		}

		// An invariant after polling is that all *read instances with
		// an available document have been queued, and only Tailing
		// *read instances without a ready document remain in |pending|.

		// Pop the next ordered document to process.
		var r = heap.Pop(&g.queued).(*read)

		// If this *read adjusts document clocks, and the adjusted clock runs
		// ahead of effective wall-clock time, then we must gate the document
		// until wall-time catches up with its adjusted clock.
		if a := r.resp.UuidParts[r.resp.Index].Clock + r.readDelay; r.readDelay != 0 && a > g.wallTime {

			// TODO(johnny): Leaving for now until we have more testing of this feature.
			log.WithFields(log.Fields{
				"journal":   r.req.Shuffle.Journal,
				"tailing":   r.resp.Tailing(),
				"remaining": len(r.resp.DocsJson),
				"wallTime":  g.wallTime.Time(),
				"want":      a.Time(),
				"actual":    time.Now(),
			}).Info("GATE")

			g.gated = append(g.gated, r)
			continue
		}

		var env = r.dequeue()

		if r.resp.Index != len(r.resp.DocsJson) {
			// Next document is available without polling.
			heap.Push(&g.queued, r)
		} else {
			g.pending[r] = struct{}{}
			g.mustPoll = true
		}
		return env, nil
	}
}

// errPollAgain is returned by poll() if another re-entrant call
// must be made to finish the polling operation.
var errPollAgain = fmt.Errorf("not ready; poll again")

// poll for more data, a journal change, a time increment,
// or for cancellation. poll() returns errPollAgain if it made
// progress but another call to poll() is required. It returns
// nil iff all *reads have been polled, and all non-tailing
// *reads have at least one document queued.
func (g *governor) poll(ctx context.Context) error {
	// Walk all *reads not having a ready ShuffleResponse,
	// polling if (or blocking until) one is available.
	for r := range g.pending {

		var result readResult
		var ok bool

		if r.resp.Tailing() {
			// Reader is tailing the journal. Poll without blocking,
			// as we may wait an unbounded amount of time for more
			// data to be written to the journal.
			select {
			case result, ok = <-r.pollCh:
				// Fall through.
			case <-ctx.Done():
				return ctx.Err()
			case <-g.journalsUpdateCh:
				return g.onConverge(ctx)
			case <-g.tp.Ready():
				return g.onTick()
			default:
				continue // Don't block.
			}
		} else {
			// If we're not yet tailing the journal, block for the next response.
			select {
			case result, ok = <-r.pollCh:
				// Fall through.
			case <-g.journalsUpdateCh:
				return g.onConverge(ctx)
			case <-g.tp.Ready():
				return g.onTick()
			}
		}

		// We've polled a new result for this *read instance.

		if err := r.onRead(result); err != nil {
			// If an error occurred, the response wasn't updated and we'll return
			// errPollAgain below, because there are no ready documents.
			// We expect the read pump will close it's channel after this error.
			// Log the error now, and fall through to poll again, which will remove
			// and then re-start this read.
			r.log().WithField("err", err).Warn("shuffled read failed (will retry)")
		}

		if !ok {
			// This *read was cancelled and its channel has now drained.
			delete(g.pending, r)
			delete(g.active, r.req.Shuffle.Journal)
			// Perserve the journal offset for a future read.
			g.idle[r.req.Shuffle.Journal] = r.req.Offset
			// Converge again, as we may want to start a new read for this journal
			// (i.e., if we drained this read because the coordinating shard has changed).
			return g.onConverge(ctx)
		} else if r.resp.TerminalError != "" {
			return fmt.Errorf(r.resp.TerminalError)
		} else if len(r.resp.DocsJson) == 0 {
			// Re-enter to poll this *read instance again. In particular,
			// we *must* perform another blocking read if !Tailing.
			return errPollAgain
		} else {
			delete(g.pending, r)
			heap.Push(&g.queued, r)
		}
	}

	// We've polled all pending *reads, and as a post-condition, know that
	// a this point all *read instances with available data (i.e., !Tailing)
	// have at least one document queued.
	if len(g.queued) != 0 {
		// This is the once place we return err == nil.
		// In all other control paths, we return errPollAgain to poll() again,
		// or a terminal error (including context cancellation).
		return nil
	}

	// If we /still/ have no queued *reads, we must block until woken.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-g.journalsUpdateCh:
		return g.onConverge(ctx)
	case <-g.tp.Ready():
		return g.onTick()
	case <-g.readReadyCh:
		return errPollAgain
	}
}

func (g *governor) onTick() error {
	// Re-add all gated reads to |queued|, to be re-evaluated
	// against the updated |wallTime|, and poll() again.
	for _, r := range g.gated {
		heap.Push(&g.queued, r)
	}
	g.gated = g.gated[:0]

	// Adjust |tick| by the clock delta (if any) attached to the *consumer.Service.
	var delta = atomic.LoadInt64((*int64)(&g.rb.service.PublishClockDelta))
	g.wallTime.Update(g.tp.Time.Add(time.Duration(delta)))

	// Start awaiting next *Timepoint.
	g.tp = g.tp.Next
	// Ticks interrupt a current poll(), so we always poll again.
	return errPollAgain
}

func (g *governor) onConverge(ctx context.Context) error {
	var added, drain, err = g.rb.buildReads(g.active, g.idle)
	if err != nil {
		return fmt.Errorf("failed to build reads: %w", err)
	}

	for _, r := range added {
		if err := g.rb.start(ctx, r); err != nil {
			return fmt.Errorf("failed to start read: %w", err)
		}
		r.pollCh = make(chan readResult, 2)
		go r.pump(g.readReadyCh)

		g.active[r.spec.Name] = r
		delete(g.idle, r.spec.Name)

		// Mark that we must poll a response from this *read.
		g.pending[r] = struct{}{}
	}

	for _, r := range drain {
		r.log().Debug("read is no longer active; draining")
		r.cancel()
	}

	// Converge interrupts a current poll(), so we always poll again.
	return errPollAgain
}

func signalKeySpaceUpdates(ctx context.Context, ks *keyspace.KeySpace, ch chan<- struct{}) {
	var revision int64 = -1
	for {
		ks.Mu.RLock()
		var err = ks.WaitForRevision(ctx, revision+1)
		revision = ks.Header.Revision
		ks.Mu.RUnlock()

		if err != nil {
			return
		}

		select {
		case ch <- struct{}{}:
		case <-ctx.Done():
			return
		}
	}
}
