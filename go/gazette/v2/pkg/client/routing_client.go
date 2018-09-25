package client

import (
	"context"

	"github.com/hashicorp/golang-lru"
	"google.golang.org/grpc"

	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

// The RouteUpdater interface is implemented by the BrokerClient returned by
// NewRoutingClient. It should be called by users of the client upon receiving
// a Route update, or (if Route is nil) to purge an existing entry.
type RouteUpdater interface {
	UpdateRoute(pb.Journal, *pb.Route)
}

// WithJournalHint attaches a hint to the Context which a routing client uses
// to appropriately route Append requests.
func WithJournalHint(ctx context.Context, journal pb.Journal) context.Context {
	return context.WithValue(ctx, journalHint, journal)
}

// NewRoutingClient returns a BrokerClient which directly routes Read and
// Append requests to best responsible brokers, given the availability
// zone of the client and a local cache of the current broker topology.
// When direct routing isn't possible, the provided |client| is used instead.
func NewRoutingClient(client pb.BrokerClient, zone string, dialer Dialer, cacheSize int) (pb.BrokerClient, error) {
	var cache, err = lru.New(cacheSize)
	if err != nil {
		return nil, err
	}
	return &routingClient{
		BrokerClient: client,
		zone:         zone,
		routes:       cache,
		dialer:       dialer,
	}, nil
}

type routingClient struct {
	pb.BrokerClient            // Default client used where a route is not available.
	zone            string     // Preferred zone of read RPCs.
	routes          *lru.Cache // Cached routes.
	dialer          Dialer     // Dialer to cached route endpoints.
}

func (rc *routingClient) Read(ctx context.Context, in *pb.ReadRequest, opts ...grpc.CallOption) (pb.Broker_ReadClient, error) {
	var iRoute, ok = rc.routes.Get(in.Journal)
	if !ok {
		return rc.BrokerClient.Read(ctx, in, opts...)
	}

	var route = iRoute.(pb.Route)
	var ind = route.SelectReplica(pb.ProcessSpec_ID{Zone: rc.zone})

	var client, err = rc.dialer.Dial(ctx, route.Members[ind], route)
	if err != nil {
		return nil, err
	}
	return pb.NewBrokerClient(client).Read(ctx, in, opts...)
}

func (rc *routingClient) Append(ctx context.Context, opts ...grpc.CallOption) (pb.Broker_AppendClient, error) {
	var journal pb.Journal

	if v := ctx.Value(journalHint); v == nil {
		panic("expected WithJournalHint to be attached to the Context of Append RPC call")
	} else {
		journal = v.(pb.Journal)
	}

	var iRoute, ok = rc.routes.Get(journal)
	if !ok {
		return rc.BrokerClient.Append(ctx, opts...)
	}

	var route = iRoute.(pb.Route)
	var client, err = rc.dialer.Dial(ctx, route.Members[route.Primary], route)
	if err != nil {
		return nil, err
	}
	return pb.NewBrokerClient(client).Append(ctx, opts...)
}

func (rc *routingClient) List(ctx context.Context, in *pb.ListRequest, opts ...grpc.CallOption) (*pb.ListResponse, error) {
	// Delegate to BrokerClient, but update cached routes using response journals.
	var r, err = rc.BrokerClient.List(ctx, in, opts...)

	if err == nil {
		for _, j := range r.Journals {
			rc.UpdateRoute(j.Spec.Name, &j.Route)
		}
	}
	return r, err
}

func (rc *routingClient) UpdateRoute(journal pb.Journal, route *pb.Route) {
	// Only cache non-empty Routes with an assigned primary broker. Presumptively,
	// Routes not meeting this criteria will be updated shortly anyway.
	if route == nil || len(route.Members) == 0 || route.Primary == -1 {
		rc.routes.Remove(journal)
	} else {
		rc.routes.Add(journal, *route)
	}
}

var journalHint = &struct{}{}
