package shuffle

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/flow"
	pf "github.com/estuary/protocols/flow"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
)

// Coordinator collects a set of rings servicing ongoing shuffle reads,
// and matches new ShuffleConfigs to a new or existing ring.
type Coordinator struct {
	catalog flow.Catalog
	ctx     context.Context
	mu      sync.Mutex
	rings   map[*ring]struct{}
	rjc     pb.RoutedJournalClient
}

// NewCoordinator returns a new *Coordinator using the given clients.
func NewCoordinator(ctx context.Context, rjc pb.RoutedJournalClient, catalog flow.Catalog) *Coordinator {
	return &Coordinator{
		catalog: catalog,
		ctx:     ctx,
		rings:   make(map[*ring]struct{}),
		rjc:     rjc,
	}
}

// Subscribe to a coordinated read under the given ShuffleRequest.
// ShuffleResponses are sent to the provided callback until it completes,
// a TerminalError is sent, or another error such as cancellation occurs.
func (c *Coordinator) Subscribe(
	ctx context.Context,
	request pf.ShuffleRequest,
	callback func(*pf.ShuffleResponse, error) error,
) {
	var err = c.subscribe(subscriber{
		ctx:            ctx,
		ShuffleRequest: request,
		callback:       callback,
	})

	if err != nil {
		callback(nil, err)
	}
}

func (c *Coordinator) subscribe(sub subscriber) error {
	// Await a future commons revision before we attempt to resolve it.
	c.catalog.KeySpace.Mu.RLock()
	var err = c.catalog.KeySpace.WaitForRevision(sub.ctx, sub.Shuffle.CommonsRevision)
	c.catalog.KeySpace.Mu.RUnlock()

	if err != nil {
		return err
	}

	// Load commons runtime instances *before* we obtain the Coordinator Mutex.
	// This is fast if the commons has been used already.
	commons, _, err := c.catalog.GetCommons(sub.Shuffle.CommonsId)
	if err != nil {
		return fmt.Errorf("resolving commons %q: %w", sub.Shuffle.CommonsId, err)
	}
	schemaIndex, err := commons.SchemaIndex()
	if err != nil {
		return fmt.Errorf("building schema index: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for ring := range c.rings {
		if ring.shuffle.Equal(sub.Shuffle) {
			select {
			case ring.subscriberCh <- sub:
				return nil
			case <-ring.ctx.Done():
				// ring.serve() may not be reading ring.subscriberCh after cancellation.
				// Loop again.
			}
		}
	}

	// We must create a new ring.
	var maybeValidateSchemaURI string
	if sub.Shuffle.ValidateSchemaAtRead {
		maybeValidateSchemaURI = sub.Shuffle.SourceSchemaUri
	}

	var ex = bindings.NewExtractor()
	if ex.Configure(
		sub.Shuffle.SourceUuidPtr,
		sub.Shuffle.ShuffleKeyPtr,
		maybeValidateSchemaURI,
		schemaIndex,
	); err != nil {
		return fmt.Errorf("building document extractor: %w", err)
	}
	var ctx, cancel = context.WithCancel(c.ctx)

	var ring = &ring{
		coordinator:  c,
		ctx:          ctx,
		cancel:       cancel,
		subscriberCh: make(chan subscriber, 1),
		shuffle:      sub.Shuffle,
	}
	c.rings[ring] = struct{}{}

	ring.log().Debug("starting shuffle ring service")
	go ring.serve(ex)

	ring.subscriberCh <- sub
	return nil
}

// Ring coordinates a read over a single journal on behalf of a
// set of subscribers.
type ring struct {
	coordinator *Coordinator
	ctx         context.Context
	cancel      context.CancelFunc

	subscriberCh chan subscriber
	readChans    []chan *pf.ShuffleResponse

	shuffle pf.JournalShuffle
	subscribers
}

func (r *ring) onSubscribe(sub subscriber) {
	r.log().WithFields(log.Fields{
		"range":       &sub.Range,
		"offset":      sub.Offset,
		"endOffset":   sub.EndOffset,
		"subscribers": len(r.subscribers),
	}).Debug("adding shuffle ring subscriber")

	// Prune before adding to ensure we remove a now-cancelled
	// parent range before adding a replacement child range.
	r.subscribers.prune()

	var rr = r.subscribers.add(sub)
	if rr == nil {
		return // This subscriber doesn't require starting a new read.
	}

	var readCh = make(chan *pf.ShuffleResponse, 1)
	r.readChans = append(r.readChans, readCh)

	if len(r.readChans) == 1 && rr.EndOffset != 0 {
		panic("top-most read cannot have EndOffset")
	}
	go readDocuments(r.ctx, r.coordinator.rjc, *rr, readCh)

	log.WithFields(log.Fields{
		"journal":    r.shuffle.Journal,
		"subscriber": &sub.Range,
		"offset":     rr.Offset,
		"endOffset":  rr.EndOffset,
		"reads":      len(r.readChans),
	}).Debug("started journal read")
}

func (r *ring) onRead(staged *pf.ShuffleResponse, ok bool, ex *bindings.Extractor) {
	if !ok {
		// Reader at the top of the read stack has exited.
		r.readChans = r.readChans[:len(r.readChans)-1]

		log.WithFields(log.Fields{
			"journal": r.shuffle.Journal,
			"reads":   len(r.readChans),
		}).Debug("completed catch-up journal read")
		return
	}

	if len(staged.DocsJson) != 0 {
		// Extract from staged documents.
		for _, d := range staged.DocsJson {
			ex.Document(staged.Arena.Bytes(d))
		}
		var uuids, fields, err = ex.Extract()
		r.onExtract(staged, uuids, fields, err)
	}

	// Stage responses for subscribers, and send.
	r.subscribers.stageResponses(staged)
	r.subscribers.sendResponses()

	// If no active subscribers remain, then cancel this ring.
	if len(r.subscribers) == 0 {
		r.cancel()
	}
}

func (r *ring) onExtract(staged *pf.ShuffleResponse, uuids []pf.UUIDParts, packedKeys [][]byte, err error) {
	if err != nil {
		if staged.TerminalError == "" {
			staged.TerminalError = err.Error()
		}
		r.log().WithFields(log.Fields{
			"err":         err,
			"readThrough": staged.ReadThrough,
			"writeHead":   staged.WriteHead,
		}).Error("failed to extract from documents")
		return
	}

	staged.PackedKey = make([]pf.Slice, len(packedKeys))
	for i, packed := range packedKeys {
		staged.PackedKey[i] = staged.Arena.Add(packed)
	}

	staged.UuidParts = uuids
}

func (r *ring) serve(ex *bindings.Extractor) {
loop:
	for {
		var readCh chan *pf.ShuffleResponse
		if l := len(r.readChans); l != 0 {
			readCh = r.readChans[l-1]
		}

		select {
		case sub := <-r.subscriberCh:
			r.onSubscribe(sub)
		case resp, ok := <-readCh:
			r.onRead(resp, ok, ex)
		case <-r.ctx.Done():
			break loop
		}
	}

	// De-link this ring from its coordinator.
	r.coordinator.mu.Lock()
	delete(r.coordinator.rings, r)
	r.coordinator.mu.Unlock()

	// Drain any remaining subscribers.
	close(r.subscriberCh)
	for sub := range r.subscriberCh {
		r.subscribers = append(r.subscribers, sub)
	}
	for _, sub := range r.subscribers {
		sub.callback(nil, r.ctx.Err())
	}

	r.log().Debug("shuffle ring service exiting")
}

func (r *ring) log() *log.Entry {
	return log.WithFields(log.Fields{
		"journal":     r.shuffle.Journal,
		"coordinator": r.shuffle.Coordinator,
		"replay":      r.shuffle.Replay,
	})
}

// readDocuments pumps reads from a journal into the provided channel,
// which must have a buffer of size one. Documents are merged into a
// channel-buffered ShuffleResponse (up to a limit).
func readDocuments(
	ctx context.Context,
	rjc pb.RoutedJournalClient,
	req pb.ReadRequest,
	ch chan *pf.ShuffleResponse,
) {
	defer close(ch)

	// Start reading in non-blocking mode. This ensures we'll minimally send an opening
	// ShuffleResponse, which informs the client of whether we're tailing the journal
	// (and further responses may block).
	req.Block = false
	req.DoNotProxy = !rjc.IsNoopRouter()

	var rr = client.NewRetryReader(ctx, rjc, req)
	var br = bufio.NewReader(rr)
	var offset = rr.AdjustedOffset(br)

	// Size of the Arena and DocsJson of the ShuffleResponse last written to |ch|.
	// These are used to plan capacity of future ShuffleResponse allocations.
	var lastArena, lastDocs = 0, 0

	for {
		var line, err = message.UnpackLine(br)

		switch err {
		case nil:
			// We read a line.
		case io.EOF:
			return // Reached EndOffset, all done!
		case context.Canceled:
			return // All done.
		case io.ErrNoProgress:
			// bufio.Reader generates these when a read is restarted multiple
			// times with no actual bytes read (e.x. because the journal is idle).
			// It's safe to ignore.
			line, err = nil, nil
		case client.ErrOffsetJump:
			// Occurs when fragments are removed from the middle of the journal.
			log.WithFields(log.Fields{
				"journal": rr.Journal,
				"from":    offset,
				"to":      rr.AdjustedOffset(br),
			}).Warn("source journal offset jump")

			line, err, offset = nil, nil, rr.AdjustedOffset(br)
		default:
			if errors.Cause(err) == client.ErrOffsetNotYetAvailable {
				// Non-blocking read cannot make further progress.
				// Continue reading, now with blocking reads.
				line, err, rr.Reader.Request.Block = nil, nil, true
			}
			// Other possible |err| types will be passed through as a
			// ShuffleResponse.TerminalError, sent to |ch|.
		}

		// Attempt to pop an extend-able ShuffleResponse, or allocate a new one.
		var out *pf.ShuffleResponse
		select {
		case out = <-ch:
		default:
			out = new(pf.ShuffleResponse)
		}

		// Would |line| cause a re-allocation of |out| ?
		if out.Arena == nil ||
			line == nil ||
			(len(out.Arena)+len(line) <= cap(out.Arena) && len(out.DocsJson)+1 <= cap(out.DocsJson)) {
			// It wouldn't, as |out| hasn't been allocated in the first place,
			// or it can be extended without re-allocation.
		} else {
			// It would. Put |out| back. This cannot block, since channel buffer
			// N=1, we dequeued above, and we're the only writer.
			ch <- out

			// Push an empty ShuffleResponse. This may block, applying back pressure
			// until the prior |out| is picked up by the channel reader.
			select {
			case ch <- &pf.ShuffleResponse{
				ReadThrough: out.ReadThrough,
				WriteHead:   out.WriteHead,
			}:
			case <-ctx.Done():
				return
			}

			// Pop it again, for us to extend. This cannot block but we may not
			// pop it before the channel reader does, and so will need to re-allocate.
			select {
			case out = <-ch:
			default:
				out = new(pf.ShuffleResponse)
			}

			// Record that we would have _liked_ to have been able to extend |out|.
			// This causes future allocations to "round up" more capacity.
			lastArena += len(line)
			lastDocs++
		}

		// Do we need to allocate capacity in |out| ?
		if out.Arena == nil && line != nil {
			var arenaCap = roundUpPow2(max(lastArena, len(line)), arenaCapMin, arenaCapMax)
			var docsCap = roundUpPow2(lastDocs, docsCapMin, docsCapMax)

			out.Arena = make([]byte, 0, arenaCap)
			out.DocsJson = make([]pf.Slice, 0, docsCap)
			out.Offsets = make([]int64, 0, 2*docsCap)
		}

		if line != nil {
			out.DocsJson = append(out.DocsJson, out.Arena.Add(line))
			out.Offsets = append(out.Offsets, offset)
			offset = rr.AdjustedOffset(br)
			out.Offsets = append(out.Offsets, offset)
		}

		if err != nil {
			out.TerminalError = err.Error()
		}

		out.ReadThrough = offset
		out.WriteHead = rr.Reader.Response.WriteHead
		lastArena, lastDocs = len(out.Arena), len(out.DocsJson)

		// Place back onto channel (cannot block).
		ch <- out

		if err != nil {
			return
		}
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
