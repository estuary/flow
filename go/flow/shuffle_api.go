package flow

import (
	"context"
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
	// Coordinator which owns this ring.
	coordinator *shuffleCoordinator
	// Configuration of this ring.
	cfg pf.ShuffleConfig
	// Context used for journal reads of this ring, and which is cancelled
	// when the last ring subscriber goes away.
	ctx      context.Context
	cancelFn context.CancelFunc
	// Rendezvous used to map messages to ring members.
	rendezvous
	// Subscribed client stream of each ring member index.
	// If a given index is nil, the member is not currently subscribed.
	streams []grpc.ServerStream
	// Read-through offsets of each ring member index.
	// If the stream of a given index is nil, then offset is not valid.
	offsets []int64
	// Mutex guards access of streams and offsets, and condition variable
	// coordinates across multiple potential journal reads.
	cond sync.Cond
	mu   sync.Mutex
}

func (c *shuffleCoordinator) subscribe(req pf.ShuffleRequest, stream grpc.ServerStream) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for ring := range c.rings {
		if ring.cfg.Equal(&req.Config) {
			//c.subscribeRing(ring, req.Offset, stream)
			return
		}
	}

	// Add a new, empty shuffleRing with this request's configuration.
	var ring = &shuffleRing{
		cfg:        req.Config,
		rendezvous: newRendezvous(req.Config),
		offsets:    make([]int64, len(req.Config.Ring.Members)),
		streams:    make([]grpc.ServerStream, len(req.Config.Ring.Members)),
	}
	//ring.cond = sync.NewCond(&ring.mu)

	//go c.ringRead(ring, req.Offset, -1)

	c.rings[ring] = struct{}{}

	//c.subscribeRing(req, stream, ring)
	return
}

func (r *shuffleRing) read(from, to pb.Offset) {

	var req = pb.ReadRequest{
		Journal:    r.cfg.Journal,
		Offset:     from,
		EndOffset:  to,
		Block:      true,
		DoNotProxy: r.coordinator.rjc.IsNoopRouter(),
	}
	var rr = client.NewRetryReader(r.ctx, r.coordinator.rjc, req)
	var _ = message.NewReadUncommittedIter(rr, NewRawJSONMessage)

	//var rr = client.NewRetryReader(s.ctx, s.ajc, ), s.svc.App.NewMessage)
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
