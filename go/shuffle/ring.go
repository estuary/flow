package shuffle

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sync"

	pf "github.com/estuary/flow/go/protocol"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
	"google.golang.org/grpc/status"
)

// Coordinator collects a set of rings servicing ongoing shuffle reads,
// and matches new ShuffleConfigs to a new or existing ring.
type coordinator struct {
	ctx context.Context
	rjc pb.RoutedJournalClient
	ec  pf.ExtractClient

	rings map[*ring]struct{}
	mu    sync.Mutex
}

func newCoordinator(ctx context.Context, rjc pb.RoutedJournalClient, ec pf.ExtractClient) *coordinator {
	return &coordinator{
		ctx:   ctx,
		rjc:   rjc,
		ec:    ec,
		rings: make(map[*ring]struct{}),
	}
}

func (c *coordinator) findOrCreateRing(cfg pf.ShuffleConfig) *ring {
	c.mu.Lock()
	defer c.mu.Unlock()

	for ring := range c.rings {
		if ring.ctx.Err() != nil {
			// Prune completed ring from the collection.
			delete(c.rings, ring)
		} else if ring.cfg.Equal(cfg) {
			// Return a matched, existing ring.
			return ring
		}
	}

	// We must create a new ring.
	var ctx, cancel = context.WithCancel(c.ctx)

	var ring = &ring{
		coordinator:  c,
		ctx:          ctx,
		cancel:       cancel,
		subscriberCh: make(chan subscriber, 1),
		rendezvous:   newRendezvous(cfg),
		subscribers:  make(subscribers, len(cfg.Ring.Members)),
	}
	c.rings[ring] = struct{}{}

	ring.log().Info("starting shuffle ring service")
	go ring.serve()

	return ring
}

// Ring coordinates a read over a single journal on behalf of a
// set of subscribers.
type ring struct {
	*coordinator
	ctx    context.Context
	cancel context.CancelFunc

	subscriberCh chan subscriber
	readChans    []chan pf.ShuffleResponse

	rendezvous
	subscribers
}

func (r *ring) onSubscribe(sub subscriber) {
	r.log().WithFields(log.Fields{
		"ringIndex": sub.request.RingIndex,
		"offset":    sub.request.Offset,
		"endOffset": sub.request.EndOffset,
	}).Info("adding shuffle ring subscriber")

	var rr = r.subscribers.add(sub)
	if rr == nil {
		return // This subscriber doesn't require starting a new read.
	}

	var readCh = make(chan pf.ShuffleResponse, 1)
	r.readChans = append(r.readChans, readCh)

	if len(r.readChans) == 1 && rr.EndOffset != 0 {
		panic("top-most read cannot have EndOffset")
	}
	go readDocuments(r.ctx, r.coordinator.rjc, *rr, readCh)
}

func (r *ring) onRead(staged pf.ShuffleResponse, ok bool) {
	if !ok {
		// Reader at the top of the read stack has exited.
		r.readChans = r.readChans[:len(r.readChans)-1]
		return
	}
	// Pass the request Tranform through to the response.
	staged.Transform = r.cfg.Shuffle.Transform

	var readThrough pb.Offset
	if l := len(staged.End); l != 0 {
		// Extract from staged documents.
		var extract, err = r.coordinator.ec.Extract(r.ctx, r.buildExtractRequest(&staged))
		r.onExtract(&staged, extract, err)

		readThrough = staged.End[l-1]
	}

	// Stage responses for subscribers, and send. If no active subscribers
	// remain then cancel this ring.
	r.subscribers.stageResponses(&staged, &r.rendezvous)
	if r.subscribers.sendResponses(readThrough) == 0 {
		r.cancel()
	}
}

func (r *ring) buildExtractRequest(staged *pf.ShuffleResponse) *pf.ExtractRequest {
	return &pf.ExtractRequest{
		Arena:       staged.Arena,
		ContentType: staged.ContentType,
		Content:     staged.Content,
		UuidPtr:     pf.DocumentUUIDPointer,
		HashPtrs:    r.cfg.Shuffle.ShuffleKeyPtr,
		FieldPtrs:   r.cfg.Shuffle.ShuffleKeyPtr,
	}
}

func (r *ring) onExtract(staged *pf.ShuffleResponse, extract *pf.ExtractResponse, err error) {
	if err != nil {
		var description string
		if s, ok := status.FromError(err); ok {
			description = fmt.Sprintf("flow-worker: %s: %s", s.Code(), s.Message())
		} else {
			description = err.Error()
		}

		if staged.TerminalError == "" {
			staged.TerminalError = description
		}
		log.WithField("err", err).Error("failed to extract hashes")
		return
	}

	staged.UuidParts = extract.UuidParts
	staged.ShuffleHashesLow = extract.HashesLow
	staged.ShuffleHashesHigh = extract.HashesHigh
	staged.ShuffleKey = extract.Fields

	// Re-write field values, updating the Arena that's used.
	for k, v := range staged.ShuffleKey {
		staged.ShuffleKey[k].Values = v.Values[:0]

		for _, vv := range v.Values {
			staged.ShuffleKey[k].AppendValue(&extract.Arena, &staged.Arena, vv)
		}
	}
}

func (r *ring) serve() {
	for len(r.readChans) != 0 || r.ctx.Err() == nil {
		var readCh chan pf.ShuffleResponse
		if l := len(r.readChans); l != 0 {
			readCh = r.readChans[l-1]
		}

		select {
		case sub := <-r.subscriberCh:
			r.onSubscribe(sub)
		case resp, ok := <-readCh:
			r.onRead(resp, ok)
		}
	}
	r.subscribers.sendEOF()
	r.log().Info("shuffle ring service exiting")
}

func (r *ring) log() *log.Entry {
	return log.WithFields(log.Fields{
		"journal":     r.cfg.Journal,
		"coordinator": r.cfg.Coordinator,
		"transform":   r.cfg.Shuffle.Transform,
		"ring":        r.cfg.Ring.Name,
	})
}

// readDocuments pumps reads from a journal into the provided channel,
// which must have a buffer of size one. Documents are merged into a
// channel-buffered ShuffleResponse (up to a limit).
func readDocuments(
	ctx context.Context,
	rjc pb.RoutedJournalClient,
	req pb.ReadRequest,
	ch chan pf.ShuffleResponse,
) {
	defer close(ch)

	// Start reading in non-blocking mode. This ensures we'll minimally send an opening
	// ShuffleResponse, which informs the client of whether we're tailing the journal
	// (and further responses may block).
	req.Block = false
	req.DoNotProxy = !rjc.IsNoopRouter()

	var rr = client.NewRetryReader(ctx, rjc, req)
	var br = bufio.NewReader(rr)

	// Pop attempts to dequeue a pending ShuffleResponse that we can extend.
	// Or, it returns a new one if none is buffered.
	var popPending = func() (out pf.ShuffleResponse) {
		select {
		case out = <-ch:
		default:
		}
		return
	}

	var buffer = make([]byte, 0, 1024)
	var offset = rr.AdjustedOffset(br)

	// TODO(johnny): Use journal ContentType label / wire this up better.
	var contentType = pf.ContentType_JSON

	for {
		var line, err = message.UnpackLine(br)

		switch err {
		case io.EOF:
			return // Reached EndOffset, all done!
		case context.Canceled:
			return
		case io.ErrNoProgress:
			continue // Returned by bufio.Reader sometimes. Ignore.
		case client.ErrOffsetJump:
			// Occurs when fragments are removed from the middle of the journal.
			log.WithFields(log.Fields{
				"journal": rr.Journal,
				"from":    offset,
				"to":      rr.AdjustedOffset(br),
			}).Warn("source journal offset jump")
			offset = rr.AdjustedOffset(br)
			continue
		}

		var out = popPending()

		if l := len(out.End); l != 0 && (out.End[l-1]-out.Begin[0]) >= responseSizeThreshold {
			// |out| is too large for us to extend. Put it back. This cannot block,
			// since buffer N=1, we dequeued above, and we're the only writer.
			ch <- out

			// Push an empty ShuffleResponse. This may block, applying back pressure
			// until the prior |out| is picked up by the channel reader.
			select {
			case ch <- pf.ShuffleResponse{
				ReadThrough: out.ReadThrough,
				WriteHead:   out.WriteHead,
			}:
			case <-ctx.Done():
				return
			}
			// Pop it again, for us to extend.
			out = popPending()
		}

		if err == nil {
			line = append(buffer, line...)
			buffer = line[len(line):]
			out.Content = append(out.Content, out.Arena.Add(line))
			out.ContentType = contentType

			out.Begin = append(out.Begin, offset)
			offset = rr.AdjustedOffset(br)
			out.End = append(out.End, offset)

			out.ReadThrough = offset
			out.WriteHead = rr.Reader.Response.WriteHead
		} else if errors.Cause(err) == client.ErrOffsetNotYetAvailable {
			// Continue reading, now with blocking reads.
			err, rr.Reader.Request.Block = nil, true

			out.ReadThrough = offset
			out.WriteHead = rr.Reader.Response.WriteHead
		} else /* err != nil */ {
			out.TerminalError = err.Error()
		}

		// Place back onto channel (cannot block).
		ch <- out

		if err != nil {
			return
		}
	}
}

const responseSizeThreshold int64 = 1 << 16 // 65KB.
