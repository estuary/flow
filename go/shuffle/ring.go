package shuffle

import (
	"bufio"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"sync"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/jgraettinger/cockroach-encoding/encoding"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
	"google.golang.org/grpc/status"
)

// Coordinator collects a set of rings servicing ongoing shuffle reads,
// and matches new ShuffleConfigs to a new or existing ring.
type Coordinator struct {
	ctx context.Context
	rjc pb.RoutedJournalClient
	ec  pf.ExtractClient

	rings map[*ring]struct{}
	mu    sync.Mutex
}

// NewCoordinator returns a new *Coordinator using the given clients.
func NewCoordinator(ctx context.Context, rjc pb.RoutedJournalClient, ec pf.ExtractClient) *Coordinator {
	return &Coordinator{
		ctx:   ctx,
		rjc:   rjc,
		ec:    ec,
		rings: make(map[*ring]struct{}),
	}
}

func (c *Coordinator) subscribe(sub subscriber) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for ring := range c.rings {
		if ring.shuffle.Equal(sub.Shuffle) {
			ring.subscriberCh <- sub
			return
		}
	}

	// We must create a new ring.
	var ctx, cancel = context.WithCancel(c.ctx)

	var ring = &ring{
		coordinator:  c,
		ctx:          ctx,
		cancel:       cancel,
		subscriberCh: make(chan subscriber, 1),
		shuffle:      sub.Shuffle,
	}
	c.rings[ring] = struct{}{}

	ring.log().Info("starting shuffle ring service")
	go ring.serve()

	ring.subscriberCh <- sub
}

// Ring coordinates a read over a single journal on behalf of a
// set of subscribers.
type ring struct {
	coordinator *Coordinator
	ctx         context.Context
	cancel      context.CancelFunc

	subscriberCh chan subscriber
	readChans    []chan pf.ShuffleResponse

	shuffle pf.JournalShuffle
	subscribers
}

func (r *ring) onSubscribe(sub subscriber) {
	r.log().WithFields(log.Fields{
		"range":       &sub.Range,
		"offset":      sub.Offset,
		"endOffset":   sub.EndOffset,
		"subscribers": len(r.subscribers),
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

	if l := len(staged.End); l != 0 {
		// Extract from staged documents.
		var extract, err = r.coordinator.ec.Extract(r.ctx, r.buildExtractRequest(&staged))
		r.onExtract(&staged, extract, err)
	}

	// Stage responses for subscribers, and send.
	r.subscribers.stageResponses(&staged)
	r.subscribers.sendResponses()

	// If no active subscribers remain, then cancel this ring.
	if len(r.subscribers) == 0 {
		r.cancel()
	}
}

func (r *ring) buildExtractRequest(staged *pf.ShuffleResponse) *pf.ExtractRequest {
	return &pf.ExtractRequest{
		Arena:     staged.Arena,
		DocsJson:  staged.DocsJson,
		UuidPtr:   pf.DocumentUUIDPointer,
		FieldPtrs: r.shuffle.ShuffleKeyPtr,
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

	staged.PackedKey = make([]pf.Slice, 0, len(extract.UuidParts))
	var packed []byte

	for doc := range extract.UuidParts {
		packed = packed[:0]

		for _, v := range extract.Fields {
			var vv = v.Values[doc]
			packed = vv.EncodePacked(packed, extract.Arena)

			// Update field Bytes from extract.Arena to staged.Arena|.
			switch vv.Kind {
			case pf.Field_Value_STRING, pf.Field_Value_OBJECT, pf.Field_Value_ARRAY:
				v.Values[doc].Bytes = staged.Arena.Add(extract.Arena.Bytes(vv.Bytes))
			}
		}

		switch r.shuffle.Hash {
		case pf.Shuffle_NONE:
			// No-op.
		case pf.Shuffle_MD5:
			var h = md5.New()
			h.Write(packed)
			packed = encoding.EncodeBytesAscending(packed[:0], h.Sum(nil))
		}

		staged.PackedKey = append(staged.PackedKey, staged.Arena.Add(packed))
	}

	staged.UuidParts = extract.UuidParts
	staged.ShuffleKey = extract.Fields
}

func (r *ring) serve() {
loop:
	for {
		var readCh chan pf.ShuffleResponse
		if l := len(r.readChans); l != 0 {
			readCh = r.readChans[l-1]
		}

		select {
		case sub := <-r.subscriberCh:
			r.onSubscribe(sub)
		case resp, ok := <-readCh:
			r.onRead(resp, ok)
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
		sub.doneCh <- r.ctx.Err()
	}

	r.log().Info("shuffle ring service exiting")
}

func (r *ring) log() *log.Entry {
	return log.WithFields(log.Fields{
		"journal":     r.shuffle.Journal,
		"coordinator": r.shuffle.Coordinator,
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
			out.DocsJson = append(out.DocsJson, out.Arena.Add(line))

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
