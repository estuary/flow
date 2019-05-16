package allocator

import (
	"context"
	"math"

	"github.com/LiveRamp/gazette/v2/pkg/etcdtest"
	gc "github.com/go-check/check"
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
		NewObservedState(ks, MemberKey(ks, "us-west", "baz")),
		NewObservedState(ks, MemberKey(ks, "us-east", "bar")),
		NewObservedState(ks, MemberKey(ks, "us-east", "foo")),
		NewObservedState(ks, MemberKey(ks, "does-not", "exist")),
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
		NewObservedState(ks, MemberKey(ks, "us-east", "bar")),
		NewObservedState(ks, MemberKey(ks, "us-east", "foo")),
		NewObservedState(ks, MemberKey(ks, "us-west", "baz")),
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

	var _, err = client.Put(ctx, "/root/members/us-east#allowed-to-exit", `{"R": 0}`)
	c.Assert(err, gc.IsNil)

	var ks = NewAllocatorKeySpace("/root", testAllocDecoder{})
	var states = []*State{
		NewObservedState(ks, MemberKey(ks, "us-east", "foo")),
		NewObservedState(ks, MemberKey(ks, "us-east", "allowed-to-exit")),
	}
	c.Check(ks.Load(ctx, client, 0), gc.IsNil)

	c.Check(states[0].LocalMemberInd, gc.Equals, 2)
	c.Check(states[0].shouldExit(), gc.Equals, false)

	c.Check(states[1].LocalMemberInd, gc.Equals, 0)
	c.Check(states[1].shouldExit(), gc.Equals, true)

	// While we're at it, expect |NetworkHash| changed with the new member.
	c.Check(states[0].NetworkHash, gc.Equals, uint64(0xfce0237931d8c200))
}

func (s *AllocStateSuite) TestLoadRatio(c *gc.C) {
	var client, ctx = etcdtest.TestClient(), context.Background()
	buildAllocKeySpaceFixture(c, ctx, client)
	defer etcdtest.Cleanup()

	var ks = NewAllocatorKeySpace("/root", testAllocDecoder{})
	var state = NewObservedState(ks, MemberKey(ks, "us-east", "foo"))
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

var _ = gc.Suite(&AllocStateSuite{})
