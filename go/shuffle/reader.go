package shuffle

import (
	"container/heap"
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/estuary/flow/go/flow"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
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
}

// StartReadingMessages begins reading shuffled, ordered messages into the channel, from the given Checkpoint.
func StartReadingMessages(ctx context.Context, rb *ReadBuilder, cp pc.Checkpoint, tp *flow.Timepoint, ch chan<- consumer.EnvelopeOrError) {
	var offsets = make(pb.Offsets)
	for journal, meta := range cp.Sources {
		offsets[journal] = meta.ReadThrough
	}

	var g = &governor{
		rb:          rb,
		tp:          tp,
		mustPoll:    false,
		pending:     make(map[*read]struct{}),
		active:      make(map[pb.Journal]*read),
		idle:        offsets,
		readReadyCh: make(chan struct{}, 1),
	}
	g.wallTime.Update(time.Now())

	// Spawn a loop which invokes Next() and passes the result to the output |ch|.
	go func() {
		var out consumer.EnvelopeOrError
		defer close(ch)

		if out.Error = g.onConverge(ctx); out.Error != errPollAgain {
			ch <- out
			return
		}
		for {
			out.Envelope, out.Error = g.next(ctx)

			if out.Error == nil {
				ch <- out
			} else {
				if out.Error != errDrained {
					ch <- out
				}
				return
			}
		}
	}()
}

// StartReplayRead builds and starts a read of the given journal and offset range.
func StartReplayRead(ctx context.Context, rb *ReadBuilder, journal pb.Journal, begin, end pb.Offset) message.Iterator {
	var r *read

	return message.IteratorFunc(func() (env message.Envelope, err error) {
		var attempt int
		for {
			if r != nil {
				// Fall through to keep using |r|.
			} else if r, err = rb.buildReplayRead(journal, begin, end); err != nil {
				return message.Envelope{}, err
			} else {
				r.start(ctx, rb.service.Resolver.Resolve,
					pf.NewShufflerClient(rb.service.Loopback), nil)
			}

			if env, err = r.next(); err == nil {
				return env, err
			} else if r.resp.TerminalError != "" {
				return message.Envelope{}, fmt.Errorf(r.resp.TerminalError)
			} else if err == io.EOF {
				return message.Envelope{}, err // Read through |end| offset.
			}

			// Other errors indicate a broken stream, but may be retried.

			// Stream is broken, but may be retried.
			r.log().WithFields(log.Fields{
				"err":     err,
				"attempt": attempt,
			}).Warn("failed to receive shuffled replay read (will retry)")

			switch attempt {
			case 0, 1: // Don't wait.
			default:
				time.Sleep(5 * time.Second)
			}
			attempt++

			begin, r = r.req.Offset, nil
		}
	})
}

// next returns the next message.Envelope in the read sequence,
// or an EOF if none remain, or another encountered error.
// The supplied Context -- associated with the owning Shard -- is used
// with started reads.
func (g *governor) next(ctx context.Context) (message.Envelope, error) {
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
		var readTime = r.resp.UuidParts[r.resp.Index].Clock + r.readDelay

		if r.readDelay != 0 && readTime > g.wallTime {
			log.WithFields(log.Fields{
				"journal":   r.req.Shuffle.Journal,
				"tailing":   r.resp.Tailing(),
				"readyDocs": len(r.resp.DocsJson),
				"wallTime":  g.wallTime,
				"readTime":  readTime,
			}).Debug("gated reads of journal")

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

// errDrained is returned by poll() if the ReadBuilder and all reads have drained.
var errDrained = fmt.Errorf("drained")

// poll for more data, a journal change, a time increment,
// or for cancellation. poll() returns errPollAgain if it made
// progress but another call to poll() is required. It returns
// nil iff all *reads have been polled, and all non-tailing
// *reads have at least one document queued.
func (g *governor) poll(ctx context.Context) error {
	var mustWait bool

	// Walk all *reads not having a ready ShuffleResponse,
	// polling each without blocking to see if one is now available.
	for r := range g.pending {

		var result readResult
		var ok bool

		select {
		case <-ctx.Done():
			return ctx.Err()
		case result, ok = <-r.ch:
			// Fall through.
		case <-g.rb.journals.Update():
			return g.onConverge(ctx)
		case <-g.rb.drainCh:
			return g.onConverge(ctx)
		case <-g.tp.Ready():
			return g.onTick()
		default:
			if !r.resp.Tailing() {
				// We know that more data is already available for this reader
				// and it should be forthcoming. We must block for its next read
				// before we may poll as ready, to ensure that its documents are
				// ordered correctly with respect to other documents we may have
				// already queued from other readers.
				mustWait = true
			}
			continue
		}

		// This *read polled as ready: we now evaluate its outcome.

		if err := r.onRead(result); err != nil {
			// If an error occurred, there are no ready documents and we expect
			// the read pump will close it's channel after this error.
			// Return errPollAgain to read that close, which will go on to remove
			// and possibly restart this failed *read.
			r.log().WithField("err", err).Warn("shuffled read failed (will retry)")
			return errPollAgain
		} else if !ok {
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
		} else if len(r.resp.DocsJson) == 0 && r.resp.Tailing() {
			// This is an empty read which informed us the reader is now tailing.
			// Leave it in pending.
		} else if len(r.resp.DocsJson) == 0 {
			return fmt.Errorf("unexpected non-tailing empty ShuffleResponse")
		} else {
			// Successful read. Queue it for consumption.
			delete(g.pending, r)
			heap.Push(&g.queued, r)
		}
	}

	// If all reads are ready and we have at least one non-empty
	// response queued then we should return it now.
	// This is the *one* place we return err == nil.
	// In all other control paths, we return errPollAgain to poll() again,
	// or a terminal error (including context cancellation).
	if !mustWait && len(g.queued) != 0 {
		return nil
	}

	if g.rb.drainCh == nil && len(g.active) == 0 {
		// We've completed draining all reads.
		return errDrained
	}

	// If we /still/ have no queued *reads, we must block until woken.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-g.rb.journals.Update():
		return g.onConverge(ctx)
	case <-g.rb.drainCh:
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
		r.start(ctx, g.rb.service.Resolver.Resolve,
			pf.NewShufflerClient(g.rb.service.Loopback), g.readReadyCh)

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
