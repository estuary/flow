package consumer

import (
	"context"
	"time"

	"go.etcd.io/etcd/client/v3"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/keyspace"
	"go.gazette.dev/core/server"
	"go.gazette.dev/core/task"
	"golang.org/x/net/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Service is the top-level runtime entity of a Gazette Consumer process.
// It drives local shard processing in response to allocator.State,
// powers shard resolution, and is also an implementation of ShardServer.
type Service struct {
	// Application served by the Service.
	App Application
	// Resolver of Service shards.
	Resolver *Resolver
	// Distributed allocator state of the service.
	State *allocator.State
	// Loopback connection which defaults to the local server, but is wired with
	// a pb.DispatchBalancer. Consumer applications may use Loopback to proxy
	// application-specific RPCs to peer consumer instance, after performing
	// shard resolution.
	Loopback *grpc.ClientConn
	// Journal client for use by consumer applications.
	Journals pb.RoutedJournalClient
	// Etcd client for use by consumer applications.
	Etcd *clientv3.Client
	// Delta to apply to message.Clocks used by Shards to sequence published
	// messages, with respect to real time. This should almost always be left
	// as zero, but is helpful for test workflows which require fine-grain
	// control over the write timestamps encoded within message UUIDs.
	// Never decrease this value once the Service is running, only increase it,
	// as a decrement will cause Publisher sequencing invariants to be violated.
	// This is an EXPERIMENTAL API.
	PublishClockDelta time.Duration
	// ShardAPI holds function delegates which power the ShardServer API.
	// They're exposed to allow consumer applications to wrap or alter their behavior.
	ShardAPI struct {
		Stat     func(context.Context, *Service, *pc.StatRequest) (*pc.StatResponse, error)
		List     func(context.Context, *Service, *pc.ListRequest) (*pc.ListResponse, error)
		Apply    func(context.Context, *Service, *pc.ApplyRequest) (*pc.ApplyResponse, error)
		GetHints func(context.Context, *Service, *pc.GetHintsRequest) (*pc.GetHintsResponse, error)
	}

	// stoppingCh is closed when the Service is in the process of shutting down.
	stoppingCh chan struct{}
}

// NewService constructs a new Service of the Application, driven by allocator.State.
func NewService(app Application, state *allocator.State, rjc pb.RoutedJournalClient, lo *grpc.ClientConn, etcd *clientv3.Client) *Service {
	var svc = &Service{
		App:        app,
		State:      state,
		Loopback:   lo,
		Journals:   rjc,
		Etcd:       etcd,
		stoppingCh: make(chan struct{}),
	}
	svc.Resolver = NewResolver(state, func(item keyspace.KeyValue) *shard { return newShard(svc, item) })

	// Default implementations of the ShardServer API.
	svc.ShardAPI.Stat = ShardStat
	svc.ShardAPI.List = ShardList
	svc.ShardAPI.Apply = ShardApply
	svc.ShardAPI.GetHints = ShardGetHints
	return svc
}

// QueueTasks to watch the Service KeySpace and serve any local assignments
// reflected therein, until the Context is cancelled or an error occurs.
// All local shards are gracefully stopped prior to return, even when exiting
// due to an error.
func (svc *Service) QueueTasks(tasks *task.Group, server *server.Server) {
	var watchCtx, watchCancel = context.WithCancel(context.Background())

	// Watch the Service KeySpace and manage local shard shards reflecting
	// the assignments of this consumer. Upon task completion, all shards
	// have been fully torn down.
	tasks.Queue("service.Watch", func() error {
		return svc.Resolver.watch(watchCtx, svc.Etcd)
	})

	// server.GracefulStop stops the server on task.Group cancellation,
	// after which the service.Watch is also cancelled.
	tasks.Queue("service.GracefulStop", func() error {
		<-tasks.Context().Done()

		// Signal the application that long-lived RPCs should stop,
		// so that our gRPC server may gracefully drain all ongoing RPCs.
		close(svc.stoppingCh)
		// Similarly, ensure all local shards are stopped. Under nominal
		// shutdown the allocator would already assure this, but if we're in the
		// process of crashing (eg due to Etcd partition) there may be remaining
		// local shards. Stopping them also cancels any related RPCs.
		svc.Resolver.stopServingLocalShards()

		server.BoundedGracefulStop()

		// Now that we're assured no current or future RPCs can be waiting
		// on a future KeySpace revision, instruct Watch to exit and block
		// until it does so.
		watchCancel()
		svc.Resolver.wg.Wait()

		// All shards (and any peer connections they may have held) have
		// fully torn down. Now we can tear down the loopback.
		var err = server.GRPCLoopback.Close()
		if status.Code(err) == codes.Canceled {
			err = nil // Loopback already closed. Not an error.
		}
		return err
	})
}

// Stopping returns a channel which signals when the Service is in the process
// of shutting down. Consumer applications with long-lived RPCs should use
// this signal to begin graceful cleanup of outstanding RPCs.
func (svc *Service) Stopping() <-chan struct{} { return svc.stoppingCh }

func addTrace(ctx context.Context, format string, args ...interface{}) {
	if tr, ok := trace.FromContext(ctx); ok {
		tr.LazyPrintf(format, args...)
	}
}

// Stat calls its ShardAPI delegate.
func (svc *Service) Stat(ctx context.Context, req *pc.StatRequest) (*pc.StatResponse, error) {
	return svc.ShardAPI.Stat(ctx, svc, req)
}

// List calls its ShardAPI delegate.
func (svc *Service) List(ctx context.Context, req *pc.ListRequest) (*pc.ListResponse, error) {
	return svc.ShardAPI.List(ctx, svc, req)
}

// Apply calls its ShardAPI delegate.
func (svc *Service) Apply(ctx context.Context, req *pc.ApplyRequest) (*pc.ApplyResponse, error) {
	return svc.ShardAPI.Apply(ctx, svc, req)
}

// GetHints calls its ShardAPI delegate.
func (svc *Service) GetHints(ctx context.Context, req *pc.GetHintsRequest) (*pc.GetHintsResponse, error) {
	return svc.ShardAPI.GetHints(ctx, svc, req)
}

// Service implements the ShardServer interface.
var _ pc.ShardServer = (*Service)(nil)
