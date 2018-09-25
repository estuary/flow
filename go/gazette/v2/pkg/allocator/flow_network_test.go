package v3_allocator

import (
	"context"

	gc "github.com/go-check/check"

	pr "github.com/LiveRamp/gazette/pkg/v3.allocator/push_relabel"
)

type FlowNetworkSuite struct{}

func (s *FlowNetworkSuite) TestFlowOverSimpleFixture(c *gc.C) {
	var client = etcdCluster.RandClient()
	var ctx = context.Background()
	buildAllocKeySpaceFixture(c, ctx, client)

	var ks = NewAllocatorKeySpace("/root", testAllocDecoder{})
	var state = NewObservedState(ks, MemberKey(ks, "us-west", "baz"))
	c.Check(ks.Load(ctx, client, 0), gc.IsNil)

	var fn flowNetwork
	fn.init(state)

	c.Assert(fn.items, gc.HasLen, 2)
	c.Assert(fn.zoneItems, gc.HasLen, 4)
	c.Assert(fn.members, gc.HasLen, 3)

	var (
		S    = &fn.source
		I1   = &fn.items[0]
		I2   = &fn.items[1]
		I1E  = &fn.zoneItems[0]
		I1W  = &fn.zoneItems[1]
		I2E  = &fn.zoneItems[2]
		I2W  = &fn.zoneItems[3]
		MBar = &fn.members[0]
		MFoo = &fn.members[1]
		MBaz = &fn.members[2]
		T    = &fn.sink
	)

	// Verify Source Arcs.
	verifyNode(c, S, &pr.Node{ID: 0, Height: 11, Arcs: []pr.Arc{
		{Capacity: 2, Priority: 2, Target: I1},
		{Capacity: 1, Priority: 2, Target: I2},
	}})
	// Verify Item Nodes & Arcs.
	verifyNode(c, I1, &pr.Node{ID: 0, Height: 3, Arcs: []pr.Arc{
		{Capacity: 1, Priority: 2, Target: I1E},
		{Capacity: 1, Priority: 2, Target: I1W},
	}})
	verifyNode(c, I2, &pr.Node{ID: 1, Height: 3, Arcs: []pr.Arc{
		{Capacity: 1, Priority: 2, Target: I2E},
		{Capacity: 1, Priority: 2, Target: I2W},
	}})
	// Verify ZoneItem Nodes & Arcs.
	verifyNode(c, I1E, &pr.Node{ID: 0, Height: 2, Arcs: []pr.Arc{
		{Capacity: 1, Priority: 2, Target: MFoo},
		{Capacity: 1, Priority: 0, Target: MBar},
	}})
	verifyNode(c, I1W, &pr.Node{ID: 1, Height: 2, Arcs: []pr.Arc{
		{Capacity: 1, Priority: 2, Target: MBaz},
	}})
	verifyNode(c, I2E, &pr.Node{ID: 2, Height: 2, Arcs: []pr.Arc{
		{Capacity: 1, Priority: 2, Target: MBar},
		{Capacity: 1, Priority: 0, Target: MFoo},
	}})
	verifyNode(c, I2W, &pr.Node{ID: 3, Height: 2, Arcs: []pr.Arc{
		{Capacity: 1, Priority: 2, Target: MBaz},
	}})
	// Verify Member Arcs. Capacities are scaled down 50% (rounded up) from the
	// Member.ItemLimit, as there are 6 member slots for 3 item slots.
	verifyNode(c, MBar, &pr.Node{ID: 0, Height: 1, Arcs: []pr.Arc{
		{Capacity: 1, Priority: 2, Target: T},
	}})
	verifyNode(c, MFoo, &pr.Node{ID: 1, Height: 1, Arcs: []pr.Arc{
		{Capacity: 1, Priority: 2, Target: T},
	}})
	verifyNode(c, MBaz, &pr.Node{ID: 2, Height: 1, Arcs: []pr.Arc{
		{Capacity: 2, Priority: 2, Target: T},
	}})

	// Solve the flow network for maximum flow.
	pr.FindMaxFlow(&fn.source, &fn.sink)

	// Expect "item-1"'s max-flow Assignments are unchanged from the fixture.
	c.Check(extractItemFlow(state, &fn, 0, nil), gc.DeepEquals, []Assignment{
		{ItemID: "item-1", MemberZone: "us-east", MemberSuffix: "foo"},
		{ItemID: "item-1", MemberZone: "us-west", MemberSuffix: "baz"},
	})
	// "item-2" was over-replicated. Expect one Assignment is unchanged, and the
	// other removed. Since key order, graph construction, and push/relabel itself
	// are all deterministic, Member us-west/baz will always be removed.
	c.Check(extractItemFlow(state, &fn, 1, nil), gc.DeepEquals, []Assignment{
		{ItemID: "item-two", MemberZone: "us-east", MemberSuffix: "bar"},
	})
}

func verifyNode(c *gc.C, node, expect *pr.Node) {
	c.Check(node.ID, gc.Equals, expect.ID)
	c.Check(node.Height, gc.Equals, expect.Height)

	for i, e := range expect.Arcs {
		c.Check(node.Arcs[i].Priority, gc.Equals, e.Priority)
		c.Check(node.Arcs[i].Capacity, gc.Equals, e.Capacity)
		c.Check(node.Arcs[i].Target, gc.Equals, e.Target)
	}
	// Verify any additional arcs are residuals.
	for _, a := range node.Arcs[len(expect.Arcs):] {
		c.Check(a.Capacity, gc.Equals, int32(0))
	}
}

var _ = gc.Suite(&FlowNetworkSuite{})
