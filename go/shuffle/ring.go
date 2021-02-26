package shuffle

import (
	"bufio"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"sync"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
)

// Coordinator collects a set of rings servicing ongoing shuffle reads,
// and matches new ShuffleConfigs to a new or existing ring.
type Coordinator struct {
	ctx context.Context
	rjc pb.RoutedJournalClient

	rings map[*ring]struct{}
	mu    sync.Mutex
}

// NewCoordinator returns a new *Coordinator using the given clients.
func NewCoordinator(ctx context.Context, rjc pb.RoutedJournalClient) *Coordinator {
	return &Coordinator{
		ctx:   ctx,
		rjc:   rjc,
		rings: make(map[*ring]struct{}),
	}
}

func (c *Coordinator) subscribe(sub subscriber) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for ring := range c.rings {
		if ring.shuffle.Equal(sub.Shuffle) {
			ring.subscriberCh <- sub
			return nil
		}
	}

	// We must create a new ring.
	var ex, err = bindings.NewExtractor(sub.Shuffle.SourceUuidPtr, sub.Shuffle.ShuffleKeyPtr)
	if err != nil {
		return fmt.Errorf("failed to start ring extractor: %w", err)
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
	}).Debug("adding shuffle ring subscriber")

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

func (r *ring) onRead(staged pf.ShuffleResponse, ok bool, ex *bindings.Extractor) {
	if !ok {
		// Reader at the top of the read stack has exited.
		r.readChans = r.readChans[:len(r.readChans)-1]
		return
	}

	if len(staged.DocsJson) != 0 {
		// Extract from staged documents.
		for _, d := range staged.DocsJson {
			ex.Document(staged.Arena.Bytes(d))
		}
		var uuids, fields, err = ex.Extract()
		r.onExtract(&staged, uuids, fields, err)
	}

	// Stage responses for subscribers, and send.
	r.subscribers.stageResponses(&staged)
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
		switch r.shuffle.Hash {
		case pf.Shuffle_NONE:
			staged.PackedKey[i] = staged.Arena.Add(packed)

		case pf.Shuffle_MD5:
			var h = md5.New()
			h.Write(packed)
			var sum = h.Sum(nil)

			staged.PackedKey[i] = staged.Arena.Add(tuple.Tuple{sum}.Pack())
		}
	}

	staged.UuidParts = uuids
}

func (r *ring) serve(ex *bindings.Extractor) {
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
		sub.doneCh <- r.ctx.Err()
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
