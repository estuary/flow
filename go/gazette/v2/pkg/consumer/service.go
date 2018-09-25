package consumer

import (
	"context"

	"github.com/LiveRamp/gazette/v2/pkg/allocator"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/coreos/etcd/clientv3"
	"golang.org/x/net/trace"
	"google.golang.org/grpc"
)

// Service is the top-level runtime concern of a Gazette Consumer process.
// It drives local shard processing in response to allocator.State,
// powers shard resolution, and is also an implementation of ShardServer.
type Service struct {
	// Resolver of Service shards.
	Resolver *Resolver
	// Loopback connection which defaults to the local server, but is wired with
	// a protocol.DispatchBalancer. Consumer applications may use Loopback proxy
	// application-specific RPCs to peer consumer instances, after performing
	// shard resolution.
	Loopback *grpc.ClientConn
	// Journal client for use by consumer applications.
	Journals pb.RoutedJournalClient

	etcd clientv3.KV
}

// NewService constructs a new Service of the Application, driven by allocator.State.
func NewService(app Application, state *allocator.State, rjc pb.RoutedJournalClient, lo *grpc.ClientConn, etcd *clientv3.Client) *Service {
	return &Service{
		Resolver: NewResolver(state, func() *Replica { return NewReplica(app, state.KS, etcd, rjc) }),
		Loopback: lo,
		Journals: rjc,
		etcd:     etcd,
	}
}

// Specs returns the current collection of ShardSpecs.
func (svc *Service) Specs() (out []*ShardSpec) {
	defer svc.Resolver.state.KS.Mu.RUnlock()
	svc.Resolver.state.KS.Mu.RLock()

	for _, kv := range svc.Resolver.state.Items {
		out = append(out, kv.Decoded.(allocator.Item).ItemValue.(*ShardSpec))
	}
	return
}

func addTrace(ctx context.Context, format string, args ...interface{}) {
	if tr, ok := trace.FromContext(ctx); ok {
		tr.LazyPrintf(format, args...)
	}
}
