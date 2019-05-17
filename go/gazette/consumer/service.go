package consumer

import (
	"context"

	"go.etcd.io/etcd/v3/clientv3"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/protocol"
	"golang.org/x/net/trace"
	"google.golang.org/grpc"
)

// Service is the top-level runtime concern of a Gazette Consumer process.
// It drives local shard processing in response to allocator.State,
// powers shard resolution, and is also an implementation of ShardServer.
type Service struct {
	// Resolver of Service shards.
	Resolver *Resolver
	// Distributed allocator state of the service.
	State *allocator.State
	// Loopback connection which defaults to the local server, but is wired with
	// a protocol.DispatchBalancer. Consumer applications may use Loopback to
	// proxy application-specific RPCs to peer consumer instances, after
	// performing shard resolution.
	Loopback *grpc.ClientConn
	// Journal client for use by consumer applications.
	Journals pb.RoutedJournalClient
	// Etcd client for use by consumer applications.
	Etcd *clientv3.Client
}

// NewService constructs a new Service of the Application, driven by allocator.State.
func NewService(app Application, state *allocator.State, rjc pb.RoutedJournalClient, lo *grpc.ClientConn, etcd *clientv3.Client) *Service {
	return &Service{
		Resolver: NewResolver(state, func() *Replica { return NewReplica(app, state.KS, etcd, rjc) }),
		State:    state,
		Loopback: lo,
		Journals: rjc,
		Etcd:     etcd,
	}
}

// Watch the Service KeySpace and serve any local assignments
// reflected therein, until the Context is cancelled or an error occurs.
// Watch shuts down all local replicas prior to return regardless of
// error status.
func (svc *Service) Watch(ctx context.Context) error {
	return svc.Resolver.watch(ctx, svc.Etcd)
}

func addTrace(ctx context.Context, format string, args ...interface{}) {
	if tr, ok := trace.FromContext(ctx); ok {
		tr.LazyPrintf(format, args...)
	}
}
