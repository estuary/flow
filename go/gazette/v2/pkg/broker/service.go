package broker

import (
	"context"

	"github.com/LiveRamp/gazette/v2/pkg/allocator"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/coreos/etcd/clientv3"
	"golang.org/x/net/trace"
)

// Service is the top-level runtime concern of a Gazette Broker process. It
// drives local journal handling in response to allocator.State, powers
// journal resolution, and is also an implementation of protocol.JournalServer.
type Service struct {
	jc       pb.JournalClient
	etcd     clientv3.KV
	resolver *resolver
}

// NewService constructs a new broker Service, driven by allocator.State.
func NewService(state *allocator.State, jc pb.JournalClient, etcd clientv3.KV) *Service {
	var svc = &Service{jc: jc, etcd: etcd}

	svc.resolver = newResolver(state, func(journal pb.Journal) *replica {
		var rep = newReplica(journal)
		go maintenanceLoop(rep, state.KS, pb.NewRoutedJournalClient(jc, svc), etcd)
		return rep
	})
	return svc
}

func addTrace(ctx context.Context, format string, args ...interface{}) {
	if tr, ok := trace.FromContext(ctx); ok {
		tr.LazyPrintf(format, args...)
	}
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
	if err != nil {
		panic(err) // Cannot err because we use neither minEtcdRevision nor proxyHeader.
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
