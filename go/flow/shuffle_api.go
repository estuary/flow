package flow

import (
	"context"
	"fmt"
	"io"
	"sync"

	pf "github.com/estuary/flow/go/protocol"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/message"
	"google.golang.org/grpc"
)

type ShuffleAPI struct {
	resolver *consumer.Resolver
}

func (api *ShuffleAPI) Shuffle(req *pf.ShuffleRequest, stream pf.Shuffler_ShuffleServer) error {
	var res, err = api.resolver.Resolve(consumer.ResolveArgs{
		Context:     stream.Context(),
		ShardID:     req.Config.CoordinatorShard(),
		MayProxy:    false,
		ProxyHeader: req.Resolution,
	})
	var resp = pf.ShuffleResponse{
		Status: res.Status,
		Header: &res.Header,
	}

	if err != nil {
		return err
	} else if resp.Status != pc.Status_OK {
		return stream.SendMsg(&resp)
	}
	defer res.Done()

	// Group on ShuffleConfig.

	return nil
}

type shuffleCoordinator struct {
	rjc   pb.RoutedJournalClient
	rings map[*shuffleRing]struct{}
	mu    sync.Mutex
}

type shuffleRing struct {
	deriveClient pf.DeriveClient

	rjc pb.RoutedJournalClient
	// Configuration of this ring.
	cfg pf.ShuffleConfig
	// Rendezvous used to map messages to ring members.
	rendezvous

	subscribers []shuffleSubscriber

	readOffset pb.Offset
	readChans  []chan pf.ShuffleResponse
}

type shuffleSubscriber struct {
	req    pf.ShuffleRequest
	stream grpc.ServerStream
	errCh  chan error
}

func (r *shuffleRing) onSubscribe(ctx context.Context, sub shuffleSubscriber) {
	if r.subscribers[sub.req.RingIndex].stream != nil {
		sub.errCh <- fmt.Errorf("subscriber at ring index %d already exists", sub.req.RingIndex)
		return
	}

	// Identify the current minimum read offset *excepting* |sub|.
	var prevOffset = minSubscriberOffset(r.subscribers)
	r.subscribers[sub.req.RingIndex] = sub

	// If this subscriber has a lower starting offset, or if no current reads exist,
	// then add a read from the requested offset to the current minimum offset at
	// the top of the read stack. We'll read through this range, and then pop stack
	// as this reader EOF's. If there are no current reads, then |prevOffset| is
	// zero and EndOffset will be as well, such that the bottom-stack read will never
	// EOF.
	if sub.req.Offset < prevOffset || len(r.readChans) == 0 {
		// We must start a new reader to serve this subscriber.
		var readCh = make(chan pf.ShuffleResponse, 1)
		r.readChans = append(r.readChans, readCh)

		var req = pb.ReadRequest{
			Journal:    r.cfg.Journal,
			Offset:     sub.req.Offset,
			EndOffset:  prevOffset,
			Block:      true,
			DoNotProxy: r.rjc.IsNoopRouter(),
		}
		go shuffleRead(ctx, r.rjc, req, readCh)
	}
}

func (r *shuffleRing) serve(ctx context.Context, subCh <-chan shuffleSubscriber) (err error) {
	var subscribers = make([]shuffleSubscriber, len(r.cfg.Ring.Members))

	// On return, close all subscriber streams with the returned error.
	defer func() {
		for _, sub := range subscribers {
			if sub.errCh != nil {
				sub.errCh <- err
			}
		}
	}()

	// Members X Documents X Transforms (as an index bit-mask)
	var transforms = make([][]uint64, len(r.cfg.Ring.Members))

	// Stack of live read loops.
	var readOffset pb.Offset
	var readChans []chan pf.ShuffleResponse

	var shuffleResp = new(pf.ShuffleResponse)

	for {
		// Select (only) from the read loop at the top of the stack.
		var readCh chan []pf.Document
		if l := len(readChans); l != 0 {
			readCh = readChans[l-1]
		}

		select {
		case s := <-subCh:

		case docs := <-readCh:
			for m := range subscribers {
				transforms[m] = transforms[m][:0]
			}
			// Mark which documents are to be shuffled to which members.
			for s, shuffle := range r.cfg.Shuffles {
				var extractResp, err = r.deriveClient.Extract(ctx, &pf.ExtractRequest{
					Documents: docs,
					HashKey:   shuffle.ShuffleKeyPtr,
				})
				if err != nil {
					return fmt.Errorf("extracting shuffle %d document hashes: %w", s, err)
				}

				if s == 0 {
					for d := range extractResp.Documents {
						docs[d].UuidParts = extractResp.Documents[d].UuidParts

						for m := 0; m != len(transforms); m++ {
							transforms[m] = append(transforms[m], 0)
						}
					}
				}
				for d, doc := range extractResp.Documents {
					var ranks = r.rendezvous.pick(s, uint32(doc.HashKey), doc.UuidParts.Clock)

					for _, rank := range ranks {
						transforms[rank.ind][d] |= 1 << s
					}
				}
			}

			// Send documents to each subscriber.

			for m := range transforms {
				if subscribers[m].stream == nil {
					continue
				}
				shuffleResp.Documents = shuffleResp.Documents[:0] // Clear for re-use.

				var subOffset = subscribers[m].req.Offset

				for d := range transforms[m] {
					docs[d].TransformIds = docs[d].TransformIds[:0] // Clear for re-use.
					var flags = message.Flags(docs[d].UuidParts.ProducerAndFlags)

					if docs[d].JournalBeginOffset < subOffset {
						continue // Document already sent to this subscriber.
					} else if flags == message.Flag_ACK_TXN {
						shuffleResp.Documents = append(shuffleResp.Documents, docs[d])
						continue // Acks are sent to all members, but have no transforms.
					}
					// Otherwise collect transforms matched to this member,
					// and send the document if at least one matched.
					for s := range r.cfg.Shuffles {
						if transforms[m][d]&(1<<s) != 0 {
							docs[d].TransformIds = append(docs[d].TransformIds, r.cfg.Shuffles[s].Id)
						}
					}
					if len(docs[d].TransformIds) != 0 {
						shuffleResp.Documents = append(shuffleResp.Documents, docs[d])
					}
				}

				var n = len(shuffleResp.Documents)
				if n == 0 {
					// No documents for this subscriber in this iteration.
				} else if err := subscribers[m].stream.SendMsg(shuffleResp); err == nil {
					// Update Offset with high-water mark.
					subscribers[m].req.Offset = shuffleResp.Documents[n-1].JournalEndOffset
				} else {
					if rErr := subscribers[m].stream.RecvMsg(new(pf.ShuffleRequest)); rErr != nil {
						err = rErr // RecvMsg will return a more informative connection error.
					}
					subscribers[m].errCh <- err
					subscribers[m] = shuffleSubscriber{} // Clear.
				}
			}
		}
	}

}

func (c *shuffleCoordinator) subscribe(req pf.ShuffleRequest, stream grpc.ServerStream) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for ring := range c.rings {
		if ring.ctx.Err() != nil {
			delete(c.rings, ring) // Ring has completed. Prune.
		} else if ring.cfg.Equal(&req.Config) {
			c.subscribeRing(ring, req.Offset, stream)
			return
		}
	}

	// Add a new, empty shuffleRing with this request's configuration.
	var ctx, cancelFn = context.WithCancel(context.Background())
	var ring = &shuffleRing{
		cfg:        req.Config,
		ctx:        ctx,
		cancelFn:   cancelFn,
		rendezvous: newRendezvous(req.Config),
		offsets:    make([]int64, len(req.Config.Ring.Members)),
		streams:    make([]grpc.ServerStream, len(req.Config.Ring.Members)),
	}
	// ring.cond = sync.NewCond(&ring.mu)
	// go c.ringRead(ring, req.Offset, -1)

	c.rings[ring] = struct{}{}
	c.subscribeRing(req, stream, ring)
	return
}

var shuffleRead = func(ctx context.Context, rjc pb.RoutedJournalClient, req pb.ReadRequest, ch chan pf.ShuffleResponse) (err error) {
	var rr = client.NewRetryReader(ctx, rjc, req)
	var it = message.NewReadUncommittedIter(rr, func(*pb.JournalSpec) (message.Message, error) {
		return new(pf.Document), nil
	})

	var envelope message.Envelope
	for {
		if envelope, err = it.Next(); err != nil {
			if err != io.EOF {
				ch <- pf.ShuffleResponse{TerminalError: err.Error()}
			}
			close(ch)
			return
		}
		var doc = *envelope.Message.(*pf.Document)
		doc.JournalBeginOffset = envelope.Begin
		doc.JournalEndOffset = envelope.End

		var d pf.ShuffleResponse
		select {
		case d = <-ch:
			// Continue this ShuffleResponse.
		default:
			// Start a new ShuffleResponse.
		}
		d.Documents = append(d.Documents, doc)

		ch <- d
	}
}

/*
func newShuffleRing(ctx context.Context, cfg pf.ShuffleConfig) *shuffleRing {
	var (
		ring = shuffleRing{
			ctx:        ctx,
			cancelFn:   cancelFn,
			cfg:        cfg,
			rendezvous: newRendezvous(cfg),
			offsets:    make([]int64, size),
			streams:    make([]grpc.ServerStream, size),
		}
	)

	return &ring
}

func (r *shuffleRing) subscribe(offset int64, stream grpc.ServerStream) {

}
*/

var _ pf.ShufflerServer = &ShuffleAPI{}

func minSubscriberOffset(s []shuffleSubscriber) pb.Offset {
	var found bool
	var offset pb.Offset

	for _, ss := range s {
		if ss.stream == nil {
			continue
		} else if !found {
			offset = ss.req.Offset
			found = true
		} else if offset > ss.req.Offset {
			offset = ss.req.Offset
		}
	}
	return offset
}
