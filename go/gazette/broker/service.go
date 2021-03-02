package broker

import (
	"context"

	"go.etcd.io/etcd/client/v3"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/server"
	"go.gazette.dev/core/task"
	"golang.org/x/net/trace"
)

// Service is the top-level runtime concern of a Gazette Broker process. It
// drives local journal handling in response to allocator.State, powers
// journal resolution, and is also an implementation of protocol.JournalServer.
type Service struct {
	jc       pb.JournalClient
	etcd     *clientv3.Client
	resolver *resolver

	// stopProxyReadsCh is closed when the Service is beginning shutdown.
	// All other RPCs are allowed to gracefully complete as per usual, but
	// because proxy reads can be very long lived, we must inject an EOF
	// to ensure timely Service shutdown.
	stopProxyReadsCh chan struct{}
}

// NewService constructs a new broker Service, driven by allocator.State.
func NewService(state *allocator.State, jc pb.JournalClient, etcd *clientv3.Client) *Service {
	var svc = &Service{
		jc:               jc,
		etcd:             etcd,
		stopProxyReadsCh: make(chan struct{}),
	}

	svc.resolver = newResolver(state, func(journal pb.Journal) *replica {
		var rep = newReplica(journal)
		go fragmentRefreshDaemon(state.KS, rep)
		go pulseDaemon(svc, rep)
		return rep
	})
	return svc
}

// QueueTasks of the Service to watch its KeySpace and serve local replicas.
func (svc *Service) QueueTasks(tasks *task.Group, server *server.Server, finishFn func()) {
	var watchCtx, watchCancel = context.WithCancel(context.Background())

	// Watch the Service KeySpace and manage local replicas reflecting
	// the assignments of this broker. Upon task completion, all replicas
	// have been fully torn down.
	tasks.Queue("service.Watch", func() error {
		return svc.resolver.watch(watchCtx, svc.etcd)
	})

	// server.GracefulStop stops the server on task.Group cancellation,
	// after which the service.Watch is also cancelled.
	tasks.Queue("service.GracefulStop", func() error {
		<-tasks.Context().Done()

		// Signal that proxy reads should stop, so that our gRPC server may
		// gracefully stop, and then drain all ongoing RPCs.
		close(svc.stopProxyReadsCh)
		// Similarly, ensure all local replicas are stopped. Under nominal
		// shutdown the allocator would already assure this, but if we're in the
		// process of crashing (eg due to Etcd partition) there may be remaining
		// local replicas. Stopping them also cancels any related RPCs.
		svc.resolver.stopServingLocalReplicas()

		server.BoundedGracefulStop()

		// Now that we're assured no current or future RPCs can be waiting
		// on a future KeySpace revision, instruct Watch to exit and block
		// until it does so.
		watchCancel()
		svc.resolver.wg.Wait()

		// TODO(johnny): hack to support persister stop.
		if finishFn != nil {
			finishFn()
		}
		// All replicas (and their replication pipelines) have fully torn
		// down. Now we can tear down the loopback.
		return server.GRPCLoopback.Close()
	})
}

// Route an item using the Service resolver. Route implements the
// protocol.DispatchRouter interface, and enables usages of
// protocol.WithDispatchItemRoute (eg, `client` & `http_gateway` packages) to
// resolve items via the Service resolver.
func (svc *Service) Route(ctx context.Context, item string) pb.Route {
	var res, err = svc.resolver.resolve(resolveArgs{
		ctx:      ctx,
		journal:  pb.Journal(item),
		mayProxy: true,
	})
	if err == errResolverStopped {
		return pb.Route{Primary: -1} // We're shutting down.
	} else if err != nil {
		// Otherwise cannot err because we use neither minEtcdRevision nor proxyHeader.
		panic(err)
	}
	// If Status != OK, Route will be zero-valued, which directs dispatcher
	// to use the default service address (localhost), which will then re-run
	// resolution and generate a proper error message for the client.
	return res.Route
}

// UpdateRoute is a no-op implementation of protocol.DispatchRouter.
func (svc *Service) UpdateRoute(string, *pb.Route) {} // No-op.
// IsNoopRouter returns false.
func (svc *Service) IsNoopRouter() bool { return false }

func addTrace(ctx context.Context, format string, args ...interface{}) {
	if tr, ok := trace.FromContext(ctx); ok {
		tr.LazyPrintf(format, args...)
	}
}

func instrumentJournalServerRPC(op string, err *error, res **resolution) func() {
	journalServerStarted.WithLabelValues(op).Inc()

	return func() {
		var status = "ok"
		if *err != nil {
			status = "<error>"
		} else if res != nil && *res != nil && (*res).status != pb.Status_OK {
			status = (*res).status.String()
		}
		journalServerCompleted.WithLabelValues(op, status).Inc()
	}
}
