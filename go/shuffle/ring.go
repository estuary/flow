package shuffle

import (
	"context"
	"io"
	"sync"

	pf "github.com/estuary/flow/go/protocol"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	"go.gazette.dev/core/message"
)

type coordinator struct {
	rjc pb.RoutedJournalClient
	dc  pf.DeriveClient

	rings map[*ring]struct{}
	mu    sync.Mutex
}

// TODO(johnny) This compiles, and is approximately right, but is untested and I'm
// none too sure of the details.
func (c *coordinator) findOrCreateRing(shard consumer.Shard, cfg pf.ShuffleConfig) *ring {
	c.mu.Lock()
	defer c.mu.Unlock()

	for ring := range c.rings {
		if ring.cfg.Equal(cfg) {
			// Return a matched, existing ring.
			return ring
		}
	}

	// We must create a new ring.
	var ctx, cancel = context.WithCancel(shard.Context())

	var ring = &ring{
		coordinator:  c,
		ctx:          ctx,
		cancel:       cancel,
		subscriberCh: make(chan subscriber, 1),
		rendezvous:   newRendezvous(cfg),
		subscribers:  make(subscribers, len(cfg.Ring.Members)),
	}
	c.rings[ring] = struct{}{}
	go ring.serve()

	return ring
}

type ring struct {
	*coordinator
	ctx    context.Context
	cancel context.CancelFunc

	subscriberCh chan subscriber
	readChans    []chan pf.ShuffleResponse

	rendezvous
	staged pf.ShuffleResponse
	subscribers
}

func (r *ring) onSubscribe(sub subscriber) {
	var rr = r.subscribers.add(sub)
	if rr == nil {
		return // This subscriber doesn't require starting a new read.
	}

	var readCh = make(chan pf.ShuffleResponse, 1)
	r.readChans = append(r.readChans, readCh)
	go readDocuments(r.ctx, r.coordinator.rjc, *rr, readCh)
}

func (r *ring) onRead(resp pf.ShuffleResponse, ok bool) {
	if !ok {
		// Reader at the top of the read stack has reached EOF.
		r.readChans = r.readChans[:len(r.readChans)-1]
		return
	}
	r.staged = resp
	r.onExtract(r.coordinator.dc.Extract(r.ctx, r.buildExtractRequest()))
	r.subscribers.stageResponses(r.staged)

	// Do send.
}

func (r *ring) buildExtractRequest() *pf.ExtractRequest {
	var hashes []pf.ExtractRequest_Hash
	for _, shuffle := range r.cfg.Shuffles {
		hashes = append(hashes, pf.ExtractRequest_Hash{Ptrs: shuffle.ShuffleKeyPtr})
	}
	return &pf.ExtractRequest{
		Documents: r.staged.Documents,
		UuidPtr:   pf.DocumentUUIDPointer,
		Hashes:    hashes,
	}
}

func (r *ring) onExtract(extract *pf.ExtractResponse, err error) {
	if err != nil {
		if r.staged.TerminalError == "" {
			r.staged.TerminalError = err.Error()
		}
		log.WithField("err", err).Error("failed to extract hashes")
		return
	}

	for d := range r.staged.Documents {
		var uuid = extract.UuidParts[d]
		r.staged.Documents[d].UuidParts = uuid

		if message.Flags(uuid.ProducerAndFlags) == message.Flag_ACK_TXN {
			// ACK documents have no shuffles, and go to all readers.
			continue
		}
		for h := range extract.Hashes {
			r.staged.Documents[d].Shuffles = r.rendezvous.pick(h,
				extract.Hashes[h].Values[d],
				uuid.Clock,
				r.staged.Documents[d].Shuffles)
		}
	}
}

func (r *ring) serve() {
	for {
		var readCh chan pf.ShuffleResponse
		select {
		case sub := <-r.subscriberCh:
			r.onSubscribe(sub)
		case resp, ok := <-readCh:
			r.onRead(resp, ok)
		}
	}
}

// readDocuments is a function variable for easy mocking in tests.
var readDocuments = func(
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
	var it = message.NewReadUncommittedIter(rr, func(*pb.JournalSpec) (message.Message, error) {
		return new(pf.Document), nil
	})

	// Pop attempts to dequeue a pending ShuffleResponse that we can extend.
	// Or, it returns a new one if none is buffered.
	var popPending = func() (out pf.ShuffleResponse) {
		select {
		case out = <-ch:
		default:
		}
		return
	}

	for {
		var env, err = it.Next()
		var out = popPending()

		if l := len(out.Documents); l != 0 &&
			(out.Documents[l-1].End-out.Documents[0].Begin) >= responseSizeThreshold {
			// |out| is too large for us to extend. Put it back. This cannot block,
			// since buffer N=1, we dequeued above, and we're the only writer.
			ch <- out

			// Push an empty ShuffleResponse. This may block, applying back pressure
			// until the prior |out| is picked up by the channel reader.
			select {
			case ch <- pf.ShuffleResponse{}:
			case <-ctx.Done():
				return
			}
			// Pop it again, for us to extend.
			out = popPending()
		}

		if err == nil {
			var doc = *env.Message.(*pf.Document)
			doc.Begin, doc.End = env.Begin, env.End
			out.Documents = append(out.Documents, doc)
			out.Tailing = doc.End == rr.Reader.Response.WriteHead
		} else if errors.Cause(err) == client.ErrOffsetNotYetAvailable {
			out.Tailing = true
			// Continue reading, now with blocking reads.
			err, rr.Reader.Request.Block = nil, true
		} else if err != io.EOF {
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
