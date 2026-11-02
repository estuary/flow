package protocol

import (
	"context"
	"fmt"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/experimental/stats"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"
	gc "gopkg.in/check.v1"
)

type DispatcherSuite struct{}

func (s *DispatcherSuite) TestContextAdapters(c *gc.C) {
	// WithDispatchRoute attaches Route & ID.
	var ctx = WithDispatchRoute(context.Background(), buildRouteFixture(), ProcessSpec_ID{Zone: "remote", Suffix: "primary"})
	c.Check(ctx.Value(dispatchRouteCtxKey{}), gc.DeepEquals, dispatchRoute{
		route: buildRouteFixture(),
		id:    ProcessSpec_ID{Zone: "remote", Suffix: "primary"},
	})

	var mr = new(mockRouter)

	// WithDispatchItemRoute invokes Route of a DispatchRouter. If primary isn't required, ID is left unspecified.
	ctx = WithDispatchItemRoute(ctx, mr, "item/to/route", false)
	c.Check(ctx.Value(dispatchRouteCtxKey{}), gc.DeepEquals, dispatchRoute{
		route:          buildRouteFixture(),
		item:           "item/to/route",
		DispatchRouter: mr,
	})

	// If primary is required and one is available, it becomes the dispatch ID.
	ctx = WithDispatchItemRoute(ctx, mr, "item/to/route", true)
	c.Check(ctx.Value(dispatchRouteCtxKey{}), gc.DeepEquals, dispatchRoute{
		route:          buildRouteFixture(),
		id:             ProcessSpec_ID{Zone: "remote", Suffix: "primary"},
		item:           "item/to/route",
		DispatchRouter: mr,
	})
}

func (s *DispatcherSuite) TestDispatchCases(c *gc.C) {
	var cc = newMockClientConn()
	var disp = dispatcherBuilder{zone: "local"}.Build(cc, balancer.BuildOptions{}).(*dispatcher)
	cc.disp = disp
	close(disp.sweepDoneCh) // Disable async sweeping.

	// Case: Called without a dispatchRoute. Expect it panics.
	c.Check(func() {
		disp.Pick(balancer.PickInfo{Ctx: context.Background()})
	}, gc.PanicMatches, `expected dispatchRoute on Context; check for missing WithDispatchRoute \?`)

	var ctx = WithDispatchRoute(context.Background(), Route{Primary: -1}, ProcessSpec_ID{})

	// Case: Called from initial state with an empty dispatchRoute. Expect a new
	// SubConn to the default service address is started.
	var _, err = disp.Pick(balancer.PickInfo{Ctx: ctx})
	c.Check(err, gc.Equals, balancer.ErrNoSubConnAvailable)
	c.Check(cc.created, gc.DeepEquals, []mockSubConn{{Name: "default.addr:80", disp: disp}})
	cc.created = nil

	// Case: Default connection transitions to Ready. Expect it's now returned.
	mockSubConn{Name: "default.addr:80", disp: disp}.UpdateState(balancer.SubConnState{ConnectivityState: connectivity.Ready})

	result, err := disp.Pick(balancer.PickInfo{Ctx: ctx})
	c.Check(err, gc.IsNil)
	c.Check(result.Done, gc.IsNil)
	c.Check(result.SubConn.(*testSubConnWrapper).name, gc.Equals, "default.addr:80")

	// Case: Specific remote peer is dispatched to.
	ctx = WithDispatchRoute(context.Background(),
		buildRouteFixture(), ProcessSpec_ID{Zone: "remote", Suffix: "primary"})

	result, err = disp.Pick(balancer.PickInfo{Ctx: ctx})
	c.Check(err, gc.Equals, balancer.ErrNoSubConnAvailable)
	c.Check(cc.created, gc.DeepEquals, []mockSubConn{{Name: "remote.addr:80", disp: disp}})
	cc.created = nil

	mockSubConn{Name: "remote.addr:80", disp: disp}.UpdateState(balancer.SubConnState{ConnectivityState: connectivity.Ready})

	result, err = disp.Pick(balancer.PickInfo{Ctx: ctx})
	c.Check(err, gc.IsNil)
	c.Check(result.Done, gc.IsNil)
	c.Check(result.SubConn.(*testSubConnWrapper).name, gc.Equals, "remote.addr:80")

	// Case: Route allows for multiple members. A local one is now dialed.
	ctx = WithDispatchRoute(context.Background(), buildRouteFixture(), ProcessSpec_ID{})

	_, err = disp.Pick(balancer.PickInfo{Ctx: ctx})
	c.Check(err, gc.Equals, balancer.ErrNoSubConnAvailable)
	c.Check(cc.created, gc.DeepEquals, []mockSubConn{{Name: "local.addr:80", disp: disp}})
	cc.created = nil

	mockSubConn{Name: "local.addr:80", disp: disp}.UpdateState(balancer.SubConnState{ConnectivityState: connectivity.Ready})

	result, err = disp.Pick(balancer.PickInfo{Ctx: ctx})
	c.Check(err, gc.IsNil)
	c.Check(result.Done, gc.IsNil)
	c.Check(result.SubConn.(*testSubConnWrapper).name, gc.Equals, "local.addr:80")

	// Case: One local addr is marked as failed. Another is dialed.
	mockSubConn{Name: "local.addr:80", disp: disp}.UpdateState(balancer.SubConnState{ConnectivityState: connectivity.TransientFailure})

	_, err = disp.Pick(balancer.PickInfo{Ctx: ctx})
	c.Check(err, gc.Equals, balancer.ErrNoSubConnAvailable)
	c.Check(cc.created, gc.DeepEquals, []mockSubConn{{Name: "local.otherAddr:80", disp: disp}})
	cc.created = nil

	mockSubConn{Name: "local.otherAddr:80", disp: disp}.UpdateState(balancer.SubConnState{ConnectivityState: connectivity.Ready})

	result, err = disp.Pick(balancer.PickInfo{Ctx: ctx})
	c.Check(err, gc.IsNil)
	c.Check(result.Done, gc.IsNil)
	c.Check(result.SubConn.(*testSubConnWrapper).name, gc.Equals, "local.otherAddr:80")

	// Case: otherAddr is also failed. Expect that an error is returned,
	// rather than dispatch to remote addr. (Eg we prefer to wait for a
	// local replica to recover or the route to change, vs using a remote
	// endpoint which incurs more networking cost).
	mockSubConn{Name: "local.otherAddr:80", disp: disp}.UpdateState(balancer.SubConnState{ConnectivityState: connectivity.TransientFailure})

	_, err = disp.Pick(balancer.PickInfo{Ctx: ctx})
	c.Check(err, gc.Equals, balancer.ErrTransientFailure)

	// Case: local.addr is Ready again. However, primary is required and has failed.
	mockSubConn{Name: "local.addr:80", disp: disp}.UpdateState(balancer.SubConnState{ConnectivityState: connectivity.Ready})
	mockSubConn{Name: "remote.addr:80", disp: disp}.UpdateState(balancer.SubConnState{ConnectivityState: connectivity.TransientFailure})

	ctx = WithDispatchRoute(context.Background(),
		buildRouteFixture(), ProcessSpec_ID{Zone: "remote", Suffix: "primary"})

	_, err = disp.Pick(balancer.PickInfo{Ctx: ctx})
	c.Check(err, gc.Equals, balancer.ErrTransientFailure)

	// Case: DispatchRouter is used with an invalidation channel. As primary
	// is required and is currently failed, an immediate invalidation occurs.
	var mr = new(mockRouter)

	ctx = WithDispatchItemRoute(ctx, mr, "item/one", true)
	_, err = disp.Pick(balancer.PickInfo{Ctx: ctx})
	c.Check(err, gc.Equals, balancer.ErrTransientFailure)

	c.Check(mr.invalidated, gc.Equals, "item/one")
	mr.invalidated = ""

	// Case: Primary isn't required. An invalidation closure is returned.
	ctx = WithDispatchItemRoute(ctx, mr, "item/two", false)
	result, err = disp.Pick(balancer.PickInfo{Ctx: ctx})
	c.Check(err, gc.IsNil)
	c.Check(result.Done, gc.NotNil)
	c.Check(result.SubConn.(*testSubConnWrapper).name, gc.Equals, "local.addr:80")

	// Closure callback with an Unavailable error (only) will trigger an invalidation.
	result.Done(balancer.DoneInfo{Err: nil})
	c.Check(mr.invalidated, gc.Equals, "")
	result.Done(balancer.DoneInfo{Err: fmt.Errorf("other error")})
	c.Check(mr.invalidated, gc.Equals, "")
	result.Done(balancer.DoneInfo{Err: status.Error(codes.Unavailable, "foo")})
	c.Check(mr.invalidated, gc.Equals, "item/two")
}

func (s *DispatcherSuite) TestDispatchMarkAndSweep(c *gc.C) {
	var cc = newMockClientConn()
	var disp = dispatcherBuilder{zone: "local"}.Build(cc, balancer.BuildOptions{}).(*dispatcher)
	cc.disp = disp
	defer disp.Close()

	var err error
	var localID = ProcessSpec_ID{Zone: "local", Suffix: "replica"}
	var remoteID = ProcessSpec_ID{Zone: "remote", Suffix: "primary"}
	var localCtx = WithDispatchRoute(context.Background(), buildRouteFixture(), localID)
	var remoteCtx = WithDispatchRoute(context.Background(), buildRouteFixture(), remoteID)

	_, err = disp.Pick(balancer.PickInfo{Ctx: remoteCtx})
	c.Check(err, gc.Equals, balancer.ErrNoSubConnAvailable)
	_, err = disp.Pick(balancer.PickInfo{Ctx: localCtx})
	c.Check(err, gc.Equals, balancer.ErrNoSubConnAvailable)

	c.Check(cc.created, gc.DeepEquals, []mockSubConn{{Name: "remote.addr:80", disp: disp}, {Name: "local.addr:80", disp: disp}})
	cc.created = nil

	mockSubConn{Name: "remote.addr:80", disp: disp}.UpdateState(balancer.SubConnState{ConnectivityState: connectivity.Ready})
	mockSubConn{Name: "local.addr:80", disp: disp}.UpdateState(balancer.SubConnState{ConnectivityState: connectivity.Connecting})

	disp.sweep()
	c.Check(cc.removed, gc.IsNil)

	// Pick both connections.
	_, err = disp.Pick(balancer.PickInfo{Ctx: remoteCtx})
	c.Check(err, gc.IsNil)
	_, err = disp.Pick(balancer.PickInfo{Ctx: localCtx})
	c.Check(err, gc.Equals, balancer.ErrNoSubConnAvailable)

	// Expect the mark of both were updated to the current sweepMark.
	c.Check(disp.idConn[ProcessSpec_ID{Zone: "remote", Suffix: "primary"}].mark, gc.Equals, disp.sweepMark)
	c.Check(disp.idConn[ProcessSpec_ID{Zone: "local", Suffix: "replica"}].mark, gc.Equals, disp.sweepMark)

	disp.sweep()
	c.Check(cc.removed, gc.IsNil) // Expect sweep does nothing.

	// Pick only the remote SubConn this round.
	_, err = disp.Pick(balancer.PickInfo{Ctx: remoteCtx})
	c.Check(err, gc.IsNil)

	// This time, expect that local.addr is swept.
	disp.sweep()
	c.Check(cc.removed, gc.DeepEquals, []mockSubConn{{Name: "local.addr:80", disp: disp}})
	cc.removed = nil
	mockSubConn{Name: "local.addr:80", disp: disp}.UpdateState(balancer.SubConnState{ConnectivityState: connectivity.Shutdown})

	disp.sweep() // Now remote.addr is swept.
	c.Check(cc.removed, gc.DeepEquals, []mockSubConn{{Name: "remote.addr:80", disp: disp}})
	cc.removed = nil
	mockSubConn{Name: "remote.addr:80", disp: disp}.UpdateState(balancer.SubConnState{ConnectivityState: connectivity.Shutdown})

	// No connections remain.
	c.Check(disp.idConn, gc.HasLen, 0)
	c.Check(disp.connID, gc.HasLen, 0)
	c.Check(disp.connState, gc.HasLen, 0)

	// Pick local.addr again. It is re-dialed.
	_, err = disp.Pick(balancer.PickInfo{Ctx: localCtx})
	c.Check(err, gc.Equals, balancer.ErrNoSubConnAvailable)

	c.Check(cc.created, gc.DeepEquals, []mockSubConn{{Name: "local.addr:80", disp: disp}})
	cc.created = nil

	mockSubConn{Name: "local.addr:80", disp: disp}.UpdateState(balancer.SubConnState{ConnectivityState: connectivity.Ready})
	_, err = disp.Pick(balancer.PickInfo{Ctx: localCtx})
	c.Check(err, gc.IsNil)
}

// testSubConnWrapper wraps a test SubConn to track operations
type testSubConnWrapper struct {
	balancer.SubConn
	name string
	disp *dispatcher
}

// mockSubConn represents a test SubConn for comparisons and state updates
type mockSubConn struct {
	Name string
	disp *dispatcher
}

// mockClientConn implements balancer.ClientConn for testing
type mockClientConn struct {
	balancer.ClientConn
	err      error
	created  []mockSubConn
	removed  []mockSubConn
	disp     *dispatcher
	subConns map[string]*testSubConnWrapper
	target   string
}

func newMockClientConn() *mockClientConn {
	return &mockClientConn{
		subConns: make(map[string]*testSubConnWrapper),
		target:   "default.addr:80", // Default target for tests
	}
}

func (c *mockClientConn) NewSubConn(a []resolver.Address, opts balancer.NewSubConnOptions) (balancer.SubConn, error) {
	if c.err != nil {
		return nil, c.err
	}
	
	name := a[0].Addr
	sc := &testSubConnWrapper{
		name: name,
		disp: c.disp,
	}
	
	c.subConns[name] = sc
	c.created = append(c.created, mockSubConn{Name: name, disp: c.disp})
	
	// StateListener is handled by the gRPC framework
	
	return sc, nil
}

func (c *mockClientConn) UpdateState(state balancer.State) {}

func (c *mockClientConn) ResolveNow(resolver.ResolveNowOptions) {}

func (c *mockClientConn) Target() string { return c.target }

func (c *mockClientConn) RemoveSubConn(sc balancer.SubConn) {
	if tsc, ok := sc.(*testSubConnWrapper); ok {
		c.removed = append(c.removed, mockSubConn{Name: tsc.name, disp: tsc.disp})
		delete(c.subConns, tsc.name)
	}
}

func (c *mockClientConn) MetricsRecorder() stats.MetricsRecorder { return nil }

// Additional fields for testSubConnWrapper
var _ balancer.SubConn = (*testSubConnWrapper)(nil)

func (s *testSubConnWrapper) UpdateAddresses([]resolver.Address) { panic("deprecated") }

func (s *testSubConnWrapper) UpdateState(state balancer.SubConnState) {
	if s.disp != nil {
		s.disp.updateSubConnState(s, state)
	}
}

func (s *testSubConnWrapper) Connect() {}

func (s *testSubConnWrapper) GetOrBuildProducer(balancer.ProducerBuilder) (balancer.Producer, func()) {
	return nil, func() {}
}

func (s *testSubConnWrapper) Shutdown() {
	if cc, ok := s.disp.cc.(*mockClientConn); ok {
		cc.removed = append(cc.removed, mockSubConn{Name: s.name, disp: s.disp})
	}
}

func (s *testSubConnWrapper) RegisterHealthListener(func(balancer.SubConnState)) {}

// Helper to create mockSubConn for UpdateState calls
func (m mockSubConn) UpdateState(state balancer.SubConnState) {
	if cc, ok := m.disp.cc.(*mockClientConn); ok {
		if sc, found := cc.subConns[m.Name]; found {
			sc.UpdateState(state)
		}
	}
}

type mockRouter struct{ invalidated string }

func (r *mockRouter) Route(_ context.Context, _ string) Route { return buildRouteFixture() }
func (r *mockRouter) IsNoopRouter() bool                      { return false }

func (r *mockRouter) UpdateRoute(item string, rt *Route) {
	if rt != nil {
		panic("dispatcher should only call UpdateRoute to invalidate a Route")
	}
	r.invalidated = item
}

func buildRouteFixture() Route {
	return Route{
		Primary: 0,
		Members: []ProcessSpec_ID{
			{Zone: "remote", Suffix: "primary"},
			{Zone: "local", Suffix: "replica"},
			{Zone: "local", Suffix: "other-replica"},
		},
		Endpoints: []Endpoint{"http://remote.addr:80", "http://local.addr:80", "http://local.otherAddr:80"},
	}
}

var _ = gc.Suite(&DispatcherSuite{})
