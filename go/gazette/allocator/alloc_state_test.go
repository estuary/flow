package allocator

import (
	"context"
	"math"
	"testing"

	"go.gazette.dev/core/etcdtest"
	gc "gopkg.in/check.v1"
)

type AllocStateSuite struct{}

func (s *AllocStateSuite) TestExtractOverFixture(c *gc.C) {
	var client, ctx = etcdtest.TestClient(), context.Background()
	defer etcdtest.Cleanup()
	buildAllocKeySpaceFixture(c, ctx, client)

	var ks = NewAllocatorKeySpace("/root", testAllocDecoder{})

	// Examine multiple states derived from the KeySpace fixture, each
	// pivoted around a different Member key.
	var states = []*State{
		NewObservedState(ks, MemberKey(ks, "us-west", "baz"), isConsistent),
		NewObservedState(ks, MemberKey(ks, "us-east", "bar"), isConsistent),
		NewObservedState(ks, MemberKey(ks, "us-east", "foo"), isConsistent),
		NewObservedState(ks, MemberKey(ks, "does-not", "exist"), isConsistent),
	}
	c.Check(ks.Load(ctx, client, 0), gc.IsNil)

	// Examine fields which are invariant to the pivoted member key.
	for _, s := range states {
		// Expect |KS| was partitioned on entity type.
		c.Check(s.KS, gc.Equals, ks)
		c.Check(s.Assignments, gc.HasLen, 6)
		c.Check(s.Items, gc.HasLen, 2)
		c.Check(s.Members, gc.HasLen, 3)

		// Expect ordered Zones and slot counts were extracted.
		c.Check(s.Zones, gc.DeepEquals, []string{"us-east", "us-west"})
		c.Check(s.ZoneSlots, gc.DeepEquals, []int{3, 3})
		c.Check(s.MemberSlots, gc.Equals, 6)
		c.Check(s.ItemSlots, gc.Equals, 3)
		c.Check(s.NetworkHash, gc.Equals, uint64(0x175a17d95541fa12))

		// Member counts were sized and initialized with current Assignment counts.
		// Expect counts for Assignments with missing Items were omitted.
		c.Check(s.MemberTotalCount, gc.DeepEquals, []int{1, 1, 2})
		c.Check(s.MemberPrimaryCount, gc.DeepEquals, []int{1, 0, 1})
	}

	// Examine each state for fields influenced by the pivoted member key
	c.Check(states[0].LocalKey, gc.Equals, "/root/members/us-west#baz")
	c.Check(states[0].LocalMemberInd, gc.Equals, 2)
	c.Check(states[0].LocalItems, gc.DeepEquals, []LocalItem{
		// /root/assign/item-1/us-west/baz/0
		{Item: states[0].Items[0], Assignments: states[0].Assignments[0:2], Index: 1},
		// /root/assign/item-two/us-west/baz/1
		{Item: states[0].Items[1], Assignments: states[0].Assignments[3:6], Index: 2},
		// Note /root/assign/item-missing/us-west/baz/0 is omitted (because the Item is missing).
	})

	c.Check(states[1].LocalKey, gc.Equals, "/root/members/us-east#bar")
	c.Check(states[1].LocalMemberInd, gc.Equals, 0)
	c.Check(states[1].LocalItems, gc.DeepEquals, []LocalItem{
		// /root/assign/item-two/us-east/bar/0
		{Item: states[1].Items[1], Assignments: states[1].Assignments[3:6], Index: 1},
	})

	c.Check(states[2].LocalKey, gc.Equals, "/root/members/us-east#foo")
	c.Check(states[2].LocalMemberInd, gc.Equals, 1)
	c.Check(states[2].LocalItems, gc.DeepEquals, []LocalItem{
		// /root/assign/item-1/us-east/foo/1
		{Item: states[2].Items[0], Assignments: states[2].Assignments[0:2], Index: 0},
	})

	c.Check(states[3].LocalKey, gc.Equals, "/root/members/does-not#exist")
	c.Check(states[3].LocalMemberInd, gc.Equals, -1)
	c.Check(states[3].LocalItems, gc.IsNil)
}

func (s *AllocStateSuite) TestLeaderSelection(c *gc.C) {
	var client, ctx = etcdtest.TestClient(), context.Background()
	defer etcdtest.Cleanup()
	// Note the fixture adds keys in random order (the leader may differ each run).
	buildAllocKeySpaceFixture(c, ctx, client)

	var ks = NewAllocatorKeySpace("/root", testAllocDecoder{})
	var states = []*State{
		NewObservedState(ks, MemberKey(ks, "us-east", "bar"), isConsistent),
		NewObservedState(ks, MemberKey(ks, "us-east", "foo"), isConsistent),
		NewObservedState(ks, MemberKey(ks, "us-west", "baz"), isConsistent),
	}
	c.Check(ks.Load(ctx, client, 0), gc.IsNil)

	var count int
	for _, state := range states {
		c.Check(state.LocalMemberInd, gc.Not(gc.Equals), -1)

		if state.isLeader() {
			count++
		}
	}
	c.Check(count, gc.Equals, 1) // Expect exactly one Member is leader.
}

func (s *AllocStateSuite) TestExitCondition(c *gc.C) {
	var client, ctx = etcdtest.TestClient(), context.Background()
	buildAllocKeySpaceFixture(c, ctx, client)
	defer etcdtest.Cleanup()

	var _, err = client.Put(ctx, "/root/members/us-east#allowed-to-exit", `{"R": 0, "E": true}`)
	c.Assert(err, gc.IsNil)

	var ks = NewAllocatorKeySpace("/root", testAllocDecoder{})
	var states = []*State{
		NewObservedState(ks, MemberKey(ks, "us-east", "foo"), isConsistent),
		NewObservedState(ks, MemberKey(ks, "us-east", "allowed-to-exit"), isConsistent),
	}
	c.Check(ks.Load(ctx, client, 0), gc.IsNil)

	c.Check(states[0].LocalMemberInd, gc.Equals, 2)
	c.Check(states[0].shouldExit(), gc.Equals, false)

	c.Check(states[1].LocalMemberInd, gc.Equals, 0)
	c.Check(states[1].shouldExit(), gc.Equals, true)

	// While we're at it, expect |NetworkHash| changed with the new member.
	c.Check(states[0].NetworkHash, gc.Equals, uint64(0x554c9f4e9605a7a1))
}

func (s *AllocStateSuite) TestLoadRatio(c *gc.C) {
	var client, ctx = etcdtest.TestClient(), context.Background()
	buildAllocKeySpaceFixture(c, ctx, client)
	defer etcdtest.Cleanup()

	var ks = NewAllocatorKeySpace("/root", testAllocDecoder{})
	var state = NewObservedState(ks, MemberKey(ks, "us-east", "foo"), isConsistent)
	c.Check(ks.Load(ctx, client, 0), gc.IsNil)

	// Verify expected load ratios, computed using these counts. Note Assignments are:
	//   item-1/us-east/foo/1       (Member R: 2)
	// 	 item-1/us-west/baz/0       (R: 3)
	//   item-missing/us-west/baz/0 (R: 3, but missing Items Then not contribute to Member counts)
	//   item-two/missing/member/2  (Missing Member defaults to infinite load ratio)
	// 	 item-two/us-east/bar/0     (R: 1)
	//   item-two/us-west/baz/1     (R: 3)
	for i, f := range []float32{1.0 / 2.0, 2.0 / 3.0, 2.0 / 3.0, math.MaxFloat32, 1.0 / 1.0, 2.0 / 3.0} {
		c.Check(state.memberLoadRatio(state.Assignments[i], state.MemberTotalCount), gc.Equals, f)
	}
	for i, f := range []float32{0, 1.0 / 3.0, 1.0 / 3.0, math.MaxFloat32, 1.0 / 1.0, 1.0 / 3.0} {
		c.Check(state.memberLoadRatio(state.Assignments[i], state.MemberPrimaryCount), gc.Equals, f)
	}
}

func (s *AllocStateSuite) TestLoadRatioWithShedding(c *gc.C) {
	var client, ctx = etcdtest.TestClient(), context.Background()
	defer etcdtest.Cleanup()

	// m1 is exiting with R:5. m2 is not exiting with R:5.
	// 2 items at R:1. MemberSlots=10, ItemSlots=2, excessSlots=8.
	// m1 sheds its full capacity (5), so its effective limit is 0.
	for _, kv := range [][2]string{
		{"/root/items/item-a", `{"R": 1}`},
		{"/root/items/item-b", `{"R": 1}`},

		{"/root/members/zone-a#m1", `{"R": 5, "E": true}`},
		{"/root/members/zone-a#m2", `{"R": 5, "E": false}`},

		{"/root/assign/item-a#zone-a#m1#0", `consistent`},
		{"/root/assign/item-b#zone-a#m2#0", `consistent`},
	} {
		var _, err = client.Put(ctx, kv[0], kv[1])
		c.Assert(err, gc.IsNil)
	}

	var ks = NewAllocatorKeySpace("/root", testAllocDecoder{})
	var state = NewObservedState(ks, MemberKey(ks, "zone-a", "m1"), isConsistent)
	c.Check(ks.Load(ctx, client, 0), gc.IsNil)

	c.Check(state.ShedCapacity[0], gc.Equals, 5) // m1 fully shed.
	c.Check(state.ShedCapacity[1], gc.Equals, 0)

	// m1 has 1 assignment but effective capacity 0: load ratio is +Inf.
	c.Check(state.memberLoadRatio(state.Assignments[0], state.MemberTotalCount), gc.Equals, float32(math.Inf(1)))
	// m2 has 1 assignment and effective capacity 5: load ratio is 1/5.
	c.Check(state.memberLoadRatio(state.Assignments[1], state.MemberTotalCount), gc.Equals, float32(1.0/5.0))
}

func (s *AllocStateSuite) TestShedCapacityAllAtOnce(c *gc.C) {
	var client, ctx = etcdtest.TestClient(), context.Background()
	defer etcdtest.Cleanup()

	// 5 members, capacity 5 each. MemberSlots = 25.
	// 2 items at R:1. ItemSlots = 2. excessSlots = 23.
	// maxReplicationFactor = 1, maxShedding = 4.
	// Both exiting members shed their full capacity.
	for _, kv := range [][2]string{
		{"/root/items/item-a", `{"R": 1}`},
		{"/root/items/item-b", `{"R": 1}`},

		{"/root/members/zone-a#m1", `{"R": 5, "E": true}`},
		{"/root/members/zone-a#m2", `{"R": 5, "E": true}`},
		{"/root/members/zone-a#m3", `{"R": 5, "E": false}`},
		{"/root/members/zone-b#m4", `{"R": 5, "E": false}`},
		{"/root/members/zone-b#m5", `{"R": 5, "E": false}`},

		{"/root/assign/item-a#zone-a#m1#0", `consistent`},
		{"/root/assign/item-b#zone-b#m4#0", `consistent`},
	} {
		var _, err = client.Put(ctx, kv[0], kv[1])
		c.Assert(err, gc.IsNil)
	}

	var ks = NewAllocatorKeySpace("/root", testAllocDecoder{})
	var state = NewObservedState(ks, MemberKey(ks, "zone-a", "m1"), isConsistent)
	c.Check(ks.Load(ctx, client, 0), gc.IsNil)

	c.Check(state.ShedCapacity[0], gc.Equals, 5) // m1 sheds fully.
	c.Check(state.ShedCapacity[1], gc.Equals, 5) // m2 sheds fully.
	c.Check(state.ShedCapacity[2], gc.Equals, 0)
	c.Check(state.ShedCapacity[3], gc.Equals, 0)
	c.Check(state.ShedCapacity[4], gc.Equals, 0)
}

func (s *AllocStateSuite) TestShedCapacityReplicationConstrained(c *gc.C) {
	var client, ctx = etcdtest.TestClient(), context.Background()
	defer etcdtest.Cleanup()

	// 4 members, capacity 10 each. MemberSlots = 40.
	// 3 items: R:3, R:2, R:1. ItemSlots = 6. excessSlots = 34.
	// maxReplicationFactor = 3, maxShedding = 1.
	// m1 and m2 are both exiting, but only the oldest (m1) sheds.
	for _, kv := range [][2]string{
		{"/root/items/item-a", `{"R": 3}`},
		{"/root/items/item-b", `{"R": 2}`},
		{"/root/items/item-c", `{"R": 1}`},

		{"/root/members/zone-a#m1", `{"R": 10, "E": true}`},
		{"/root/members/zone-a#m2", `{"R": 10, "E": true}`},
		{"/root/members/zone-b#m3", `{"R": 10, "E": false}`},
		{"/root/members/zone-b#m4", `{"R": 10, "E": false}`},

		{"/root/assign/item-a#zone-a#m1#0", `consistent`},
		{"/root/assign/item-a#zone-a#m2#1", `consistent`},
		{"/root/assign/item-a#zone-b#m3#2", `consistent`},
		{"/root/assign/item-b#zone-a#m1#0", `consistent`},
		{"/root/assign/item-b#zone-b#m4#1", `consistent`},
		{"/root/assign/item-c#zone-b#m3#0", `consistent`},
	} {
		var _, err = client.Put(ctx, kv[0], kv[1])
		c.Assert(err, gc.IsNil)
	}

	var ks = NewAllocatorKeySpace("/root", testAllocDecoder{})
	var state = NewObservedState(ks, MemberKey(ks, "zone-a", "m1"), isConsistent)
	c.Check(ks.Load(ctx, client, 0), gc.IsNil)

	c.Check(state.ShedCapacity[0], gc.Equals, 10) // m1 sheds fully (oldest).
	c.Check(state.ShedCapacity[1], gc.Equals, 0)  // m2 blocked by maxShedding.
	c.Check(state.ShedCapacity[2], gc.Equals, 0)
	c.Check(state.ShedCapacity[3], gc.Equals, 0)
}

func (s *AllocStateSuite) TestShedCapacityExcessConstrained(c *gc.C) {
	var client, ctx = etcdtest.TestClient(), context.Background()
	defer etcdtest.Cleanup()

	// 4 members, capacity 3 each. MemberSlots = 12.
	// 5 items at R:1. ItemSlots = 5. excessSlots = 7.
	// maxReplicationFactor = 1, maxShedding = 3.
	// 3 exiting members need 9 total shed, but only 7 excess available.
	// m1: sheds 3 (full), m2: sheds 3 (full), m3: sheds 1 (partial).
	for _, kv := range [][2]string{
		{"/root/items/item-a", `{"R": 1}`},
		{"/root/items/item-b", `{"R": 1}`},
		{"/root/items/item-c", `{"R": 1}`},
		{"/root/items/item-d", `{"R": 1}`},
		{"/root/items/item-e", `{"R": 1}`},

		{"/root/members/zone-a#m1", `{"R": 3, "E": true}`},
		{"/root/members/zone-a#m2", `{"R": 3, "E": true}`},
		{"/root/members/zone-b#m3", `{"R": 3, "E": true}`},
		{"/root/members/zone-b#m4", `{"R": 3, "E": false}`},

		{"/root/assign/item-a#zone-a#m1#0", `consistent`},
		{"/root/assign/item-b#zone-a#m1#0", `consistent`},
		{"/root/assign/item-c#zone-a#m2#0", `consistent`},
		{"/root/assign/item-d#zone-b#m3#0", `consistent`},
		{"/root/assign/item-e#zone-b#m4#0", `consistent`},
	} {
		var _, err = client.Put(ctx, kv[0], kv[1])
		c.Assert(err, gc.IsNil)
	}

	var ks = NewAllocatorKeySpace("/root", testAllocDecoder{})
	var state = NewObservedState(ks, MemberKey(ks, "zone-a", "m1"), isConsistent)
	c.Check(ks.Load(ctx, client, 0), gc.IsNil)

	c.Check(state.ShedCapacity[0], gc.Equals, 3) // m1 sheds fully.
	c.Check(state.ShedCapacity[1], gc.Equals, 3) // m2 sheds fully.
	c.Check(state.ShedCapacity[2], gc.Equals, 1) // m3 gets remaining excess.
	c.Check(state.ShedCapacity[3], gc.Equals, 0) // non-exiting.
}

func (s *AllocStateSuite) TestShedCapacityOverloadedCluster(c *gc.C) {
	var client, ctx = etcdtest.TestClient(), context.Background()
	defer etcdtest.Cleanup()

	// 2 members, capacity 2 each. MemberSlots = 4.
	// 5 items at R:1. ItemSlots = 5. excessSlots = -1 (clamped to 0).
	// No shedding is possible because the cluster is overloaded.
	for _, kv := range [][2]string{
		{"/root/items/item-a", `{"R": 1}`},
		{"/root/items/item-b", `{"R": 1}`},
		{"/root/items/item-c", `{"R": 1}`},
		{"/root/items/item-d", `{"R": 1}`},
		{"/root/items/item-e", `{"R": 1}`},

		{"/root/members/zone-a#m1", `{"R": 2, "E": true}`},
		{"/root/members/zone-b#m2", `{"R": 2, "E": false}`},

		{"/root/assign/item-a#zone-a#m1#0", `consistent`},
		{"/root/assign/item-b#zone-a#m1#0", `consistent`},
		{"/root/assign/item-c#zone-b#m2#0", `consistent`},
		{"/root/assign/item-d#zone-b#m2#0", `consistent`},
	} {
		var _, err = client.Put(ctx, kv[0], kv[1])
		c.Assert(err, gc.IsNil)
	}

	var ks = NewAllocatorKeySpace("/root", testAllocDecoder{})
	var state = NewObservedState(ks, MemberKey(ks, "zone-a", "m1"), isConsistent)
	c.Check(ks.Load(ctx, client, 0), gc.IsNil)

	c.Check(state.ShedCapacity[0], gc.Equals, 0) // m1: no excess to shed.
	c.Check(state.ShedCapacity[1], gc.Equals, 0)
}

func (s *AllocStateSuite) TestShedCapacityNoExiting(c *gc.C) {
	var client, ctx = etcdtest.TestClient(), context.Background()
	defer etcdtest.Cleanup()
	buildAllocKeySpaceFixture(c, ctx, client)

	var ks = NewAllocatorKeySpace("/root", testAllocDecoder{})
	var state = NewObservedState(ks, MemberKey(ks, "us-west", "baz"), isConsistent)
	c.Check(ks.Load(ctx, client, 0), gc.IsNil)

	for i := range state.ShedCapacity {
		c.Check(state.ShedCapacity[i], gc.Equals, 0)
	}
}

var _ = gc.Suite(&AllocStateSuite{})

func TestMain(m *testing.M) { etcdtest.TestMainWithEtcd(m) }
