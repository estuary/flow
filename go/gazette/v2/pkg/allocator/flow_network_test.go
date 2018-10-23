package allocator

import (
	"context"

	pr "github.com/LiveRamp/gazette/v2/pkg/allocator/push_relabel"
	"github.com/LiveRamp/gazette/v2/pkg/etcdtest"
	gc "github.com/go-check/check"
)

type FlowNetworkSuite struct{}

func (s *FlowNetworkSuite) TestZoneScalingFactors(c *gc.C) {
	var cases = []struct {
		itemCount int
		itemSlots int
		expect    int
		zoneSlots []int
	}{
		// Two unbalanced zones, fully assignable.
		{itemCount: 10, itemSlots: 30, expect: 30, zoneSlots: []int{80, 30}},
		// Three unbalanced zones, all items assigned to fixed slots.
		{itemCount: 10, itemSlots: 30, expect: 30, zoneSlots: []int{80, 20, 12}},
		// Three unbalanced zones; one is smaller than |itemCount|.
		{itemCount: 10, itemSlots: 30, expect: 30, zoneSlots: []int{80, 20, 9}},
		{itemCount: 10, itemSlots: 30, expect: 30, zoneSlots: []int{80, 20, 1}},
		// Three unbalanced zones; two are smaller than |itemCount|.
		{itemCount: 10, itemSlots: 30, expect: 30, zoneSlots: []int{80, 6, 5}},
		{itemCount: 10, itemSlots: 30, expect: 30, zoneSlots: []int{80, 6, 1}},
		// Two balanced zones.
		{itemCount: 10, itemSlots: 30, expect: 30, zoneSlots: []int{50, 50}},
		// Single zone.
		{itemCount: 10, itemSlots: 30, expect: 30, zoneSlots: []int{50}},
		// Irregular sizes.
		{itemCount: 10, itemSlots: 31, expect: 31, zoneSlots: []int{21, 19, 9}},
		{itemCount: 10, itemSlots: 31, expect: 31, zoneSlots: []int{999, 12, 11}},
		// Juuust enough member capacity.
		{itemCount: 19, itemSlots: 40, expect: 40, zoneSlots: []int{21, 20}},
		// Not enough member capacity.
		{itemCount: 19, itemSlots: 40, expect: 39, zoneSlots: []int{19, 20}},
		{itemCount: 10, itemSlots: 40, expect: 39, zoneSlots: []int{19, 20}},
		{itemCount: 10, itemSlots: 40, expect: 39, zoneSlots: []int{39}},
		{itemCount: 10, itemSlots: 30, expect: 12, zoneSlots: []int{5, 4, 3}},
	}
	for _, tc := range cases {
		var num, denom = zoneScalingFactors(tc.itemCount, tc.itemSlots, tc.zoneSlots)

		var total int
		for zone, zs := range tc.zoneSlots {
			// Sum across zoneSlots. Effectively multiply each by it's zoneSlots
			// by instead dividing |denom| by zoneSlots (after removing this
			// component, expect |denom| is constant across zoneSlots).
			total += num[zone]

			c.Check(denom[zone]/zs, gc.Equals, denom[0]/tc.zoneSlots[0])
			c.Log(zone, ": ", float64(num[zone])/float64(denom[zone]), " => ", float64(num[zone]*zs)/float64(denom[zone]))
		}
		c.Check(total/(denom[0]/tc.zoneSlots[0]), gc.Equals, tc.expect)
	}
}

func (s *FlowNetworkSuite) TestFlowOverSimpleFixture(c *gc.C) {
	var client, ctx = etcdtest.TestClient(), context.Background()
	defer etcdtest.Cleanup()
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
	// Verify Member Arcs. Capacities are scaled down 2/3 (rounded up) from
	// the Member.ItemLimit (2 fixed slots per zone, with 3 zone slots).
	verifyNode(c, MBar, &pr.Node{ID: 0, Height: 1, Arcs: []pr.Arc{
		{Capacity: 1, Priority: 2, Target: T},
	}})
	verifyNode(c, MFoo, &pr.Node{ID: 1, Height: 1, Arcs: []pr.Arc{
		{Capacity: 2, Priority: 1, Target: T},
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
