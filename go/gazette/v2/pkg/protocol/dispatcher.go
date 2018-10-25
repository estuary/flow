package protocol

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"
)

// DispatcherGRPCBalancerName is the client-side dispatcher's registered gRPC
// balancer. To utilize client-side dispatching, the service endpoint should be
// dialed with grpc.WithBalancerName(protocol.DispatcherGRPCBalancerName).
const DispatcherGRPCBalancerName = "protocolDispatcher"

// RegisterGRPCDispatcher registers the dispatcher balancer with gRPC. It should
// be called once at program startup. The supplied |localZone| is used to prefer
// intra-zone (over inter-zone) members where able.
func RegisterGRPCDispatcher(localZone string) {
	balancer.Register(dispatcherBuilder{zone: localZone})
}

// WithDispatchRoute attaches a Route and optional ProcessSpec_ID to a Context
// passed to a gRPC RPC call. If ProcessSpec_ID is non-zero valued, the RPC is
// dispatched to the specified member. Otherwise, the RPC is dispatched to a
// Route member, preferring:
//  * A member not having a currently-broken network connection (eg, due to
//    a stale Route or network split).
//  * A member which is in the same zone as the caller (potentially reducing
//    network traffic costs.
//  * A member having a Ready connection (potentially reducing latency).
func WithDispatchRoute(ctx context.Context, rt Route, id ProcessSpec_ID) context.Context {
	return context.WithValue(ctx, dispatchRouteCtxKey{}, dispatchRoute{route: rt, id: id})
}

// WithDispatchDefault attaches a Route and ProcessSpec_ID which indicate
// that the RPC should be dispatched to the default service address.
func WithDispatchDefault(ctx context.Context) context.Context {
	return WithDispatchRoute(ctx, Route{Primary: -1}, ProcessSpec_ID{})
}

// WithDispatchItemRoute uses the DispatchRouter to resolve |item| to a Route
// and ProcessSpec_ID, which are in-turn attached to the Context and returned
// for dispatcher's use.
func WithDispatchItemRoute(ctx context.Context, dr DispatchRouter, item string, requirePrimary bool) context.Context {
	var rt = dr.Route(ctx, item)
	var id ProcessSpec_ID

	if requirePrimary && rt.Primary != -1 {
		id = rt.Members[rt.Primary]
	}
	return context.WithValue(ctx, dispatchRouteCtxKey{},
		dispatchRoute{route: rt, id: id, item: item, DispatchRouter: dr})
}

// DispatchRouter routes item to Routes, and observes item Routes.
type DispatchRouter interface {
	// Route an |item| to a Route, which may be empty if the Route is unknown.
	Route(ctx context.Context, item string) Route
	// UpdateRoute for |item|. A nil |route| is treated as an invalidation.
	UpdateRoute(item string, route *Route)
	// IsNoopRouter returns true if Route is a no-op.
	IsNoopRouter() bool
}

// NoopDispatchRouter is a DispatchRouter which doesn't route.
type NoopDispatchRouter struct{}

func (NoopDispatchRouter) Route(context.Context, string) Route { return Route{Primary: -1} }
func (NoopDispatchRouter) UpdateRoute(string, *Route)          {}
func (NoopDispatchRouter) IsNoopRouter() bool                  { return true }

// dispatcher manages the lifetime of SubConns to individual Endpoints, dialing
// Endpoints when needed and shutting them down when they are no longer used.
// SubConns creation and selection is driven by the Routes and ProcessSpec_IDs
// attached to RPC call Contexts via WithDispatchRoute or WithDispatchItemRoute.
type dispatcher struct {
	cc   balancer.ClientConn
	zone string

	idConn    map[ProcessSpec_ID]markedSubConn
	connID    map[balancer.SubConn]ProcessSpec_ID
	connState map[balancer.SubConn]connectivity.State

	sweepDoneCh chan struct{}
	sweepMark   uint8
	sweepTicker *time.Ticker

	mu sync.Mutex
}

// markedSubConn tracks the last mark associated with a SubConn.
// SubConns not used for a complete sweep interval are closed.
type markedSubConn struct {
	subConn balancer.SubConn
	mark    uint8
}

// Pick implements the Picker interface, used by gRPC to select a ready SubConn.
func (d *dispatcher) Pick(ctx context.Context, opts balancer.PickOptions) (balancer.SubConn, func(balancer.DoneInfo), error) {
	var dr, ok = ctx.Value(dispatchRouteCtxKey{}).(dispatchRoute)
	if !ok {
		panic("expected dispatchRoute on Context; check for missing WithDispatchRoute ?")
	}

	defer d.mu.Unlock()
	d.mu.Lock()

	// If |id| is not set, select our highest-preference member.
	if dr.id == (ProcessSpec_ID{}) {
		for _, id := range dr.route.Members {
			if d.less(id, dr.id) {
				dr.id = id
			}
		}
	}

	msc, ok := d.idConn[dr.id]
	if !ok {
		// Initiate a new SubConn to the ProcessSpec_ID.
		var err error
		if msc.subConn, err = d.cc.NewSubConn(
			[]resolver.Address{{Addr: d.idToAddr(dr.route, dr.id), Type: resolver.Backend}},
			balancer.NewSubConnOptions{}); err != nil {
			return nil, nil, err
		}

		msc.mark = d.sweepMark
		d.idConn[dr.id] = msc
		d.connID[msc.subConn] = dr.id
		d.connState[msc.subConn] = connectivity.Idle

		msc.subConn.Connect()
	}

	// Update the mark of this markedSubConn to keep it alive.
	if msc.mark != d.sweepMark {
		msc.mark = d.sweepMark
		d.idConn[dr.id] = msc
	}

	switch state := d.connState[msc.subConn]; state {
	case connectivity.Idle, connectivity.Connecting:
		// gRPC will block until connection becomes ready.
		return nil, nil, balancer.ErrNoSubConnAvailable
	case connectivity.TransientFailure:
		if dr.DispatchRouter != nil {
			dr.DispatchRouter.UpdateRoute(dr.item, nil) // Invalidate.
		}
		// gRPC will fail-fast RPCs having grpc.FailFast (the default), and block others.
		return nil, nil, balancer.ErrTransientFailure
	case connectivity.Ready:
		return msc.subConn, makeDoneClosure(dr), nil
	default:
		panic(state) // Unexpected connectivity.State.
	}
}

// HandleSubConnStateChange is notified by gRPC to track SubConn connectivity states.
func (d *dispatcher) HandleSubConnStateChange(sc balancer.SubConn, state connectivity.State) {
	d.mu.Lock()
	var id, ok = d.connID[sc]
	if !ok {
		panic("unexpected SubConn")
	}
	d.connState[sc] = state

	if state == connectivity.Shutdown {
		delete(d.idConn, id)
		delete(d.connID, sc)
		delete(d.connState, sc)
	}
	d.mu.Unlock()

	// Notify gRPC that block requests may now be able to proceed.
	d.cc.UpdateBalancerState(connectivity.Ready, d)
}

// HandleResolvedAddrs is notified by gRPC of changes in the DNS resolution of
// the service address. We don't actually care about these, instead using the
// dialed service address directly when a dispatched Route isn't available. If
// that address is, for example, a headless DNS balancer, the `net` package
// implements its own resolution and selection of an appropriate A record.
func (d *dispatcher) HandleResolvedAddrs(addrs []resolver.Address, err error) {}

// Close is notified by gRPC of a parent grpc.ClientConn closure,
// and terminates the period sweep channel.
func (d *dispatcher) Close() { close(d.sweepDoneCh) }

// less defines an ordering over ProcessSpec_ID preferences used by dispatcher.
func (d *dispatcher) less(lhs, rhs ProcessSpec_ID) bool {
	// Always prefer a defined ProcessSpec_ID over the zero-valued one
	// (which is interpreted as "use the default service address".
	if lhs != rhs && (rhs == ProcessSpec_ID{}) {
		return true
	}

	// Then prefer a non-failed transport over a failed one. Note that state
	// orders on Idle => Connecting => Ready => TransientFailure, and |lState|
	// & |rState| will default to Idle if IDs are not actually in |idConn|.
	var lState = d.connState[d.idConn[lhs].subConn]
	var rState = d.connState[d.idConn[rhs].subConn]
	var lOK = lState < connectivity.TransientFailure
	var rOK = rState < connectivity.TransientFailure

	if lOK != rOK {
		return lOK
	}

	// Then prefer a same-zone member over a cross-zone one.
	lOK = lhs.Zone == d.zone
	rOK = rhs.Zone == d.zone

	if lOK != rOK {
		return lOK
	}

	// Then prefer to use a Ready connection over building a new one.
	return lState > rState
}

// idToAddr returns a suitable address for the ID.
func (d *dispatcher) idToAddr(rt Route, id ProcessSpec_ID) string {
	if id == (ProcessSpec_ID{}) {
		return d.cc.Target() // Use the default service address.
	}
	for i := range rt.Members {
		if rt.Members[i] == id {
			return rt.Endpoints[i].URL().Host
		}
	}
	panic("ProcessSpec_ID must be in Route.Members")
}

// sweep removes any SubConns not having their mark updated in the time between calls.
func (d *dispatcher) sweep() {
	var toSweep []balancer.SubConn

	d.mu.Lock()
	for _, msc := range d.idConn {
		if msc.mark != d.sweepMark {
			toSweep = append(toSweep, msc.subConn)
		}
	}
	d.sweepMark++ // Update for next iteration.
	d.mu.Unlock()

	for _, sc := range toSweep {
		// RemoveSubConn begins SubConn shutdown. We expect to see a
		// HandleSubConnStateChange with connectivity.Shutdown, at which
		// point we'll de-index it.
		d.cc.RemoveSubConn(sc)
	}
}

// servePeriodicSweeps invokes sweep() every ticker fire.
func (d *dispatcher) servePeriodicSweeps() {
	for {
		select {
		case <-d.sweepDoneCh:
			d.sweepTicker.Stop()
			return
		case <-d.sweepTicker.C:
			d.sweep()
		}
	}
}

// makeDoneClosure builds a closure which calls |invalidate| if the RPC ended
// in an Unavailable error, which gRPC uses to signal various transport errors.
func makeDoneClosure(dr dispatchRoute) func(balancer.DoneInfo) {
	if dr.DispatchRouter == nil {
		return nil
	}
	return func(info balancer.DoneInfo) {
		if info.Err == nil {
			return
		} else if s, ok := status.FromError(info.Err); ok && s.Code() == codes.Unavailable {
			dr.DispatchRouter.UpdateRoute(dr.item, nil) // Invalidate.
		}
	}
}

// dispatcherBuilder implements balancer.Builder, and builds dispatcher instances.
type dispatcherBuilder struct{ zone string }

func (db dispatcherBuilder) Name() string { return DispatcherGRPCBalancerName }

func (db dispatcherBuilder) Build(cc balancer.ClientConn, opts balancer.BuildOptions) balancer.Balancer {
	var d = &dispatcher{
		cc:   cc,
		zone: db.zone,

		idConn:    make(map[ProcessSpec_ID]markedSubConn),
		connID:    make(map[balancer.SubConn]ProcessSpec_ID),
		connState: make(map[balancer.SubConn]connectivity.State),

		sweepDoneCh: make(chan struct{}),
		sweepMark:   1,
		sweepTicker: time.NewTicker(dispatchSweepInterval),
	}
	go d.servePeriodicSweeps()
	d.cc.UpdateBalancerState(connectivity.Ready, d) // Signal as ready for RPCs.
	return d
}

type (
	// dispatchRoute is attached to Contexts by WithDispatchRoute, for dispatcher.Pick to inspect.
	dispatchRoute struct {
		route Route
		id    ProcessSpec_ID
		item  string
		DispatchRouter
	}
	// dispatchRouteCtxKey keys dispatchRoute values attached to Contexts.
	dispatchRouteCtxKey struct{}
)

var dispatchSweepInterval = time.Second * 30
