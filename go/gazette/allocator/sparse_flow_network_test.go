package allocator

import (
	"context"

	pr "go.gazette.dev/core/allocator/sparse_push_relabel"
	"go.gazette.dev/core/etcdtest"
	"go.gazette.dev/core/keyspace"
	gc "gopkg.in/check.v1"
)

type SparseSuite struct{}

func (s *SparseSuite) TestOverKeySpaceFixture(c *gc.C) {
	var client, ctx = etcdtest.TestClient(), context.Background()
	defer etcdtest.Cleanup()
	buildAllocKeySpaceFixture(c, ctx, client)

	var ks = NewAllocatorKeySpace("/root", testAllocDecoder{})
	var state = NewObservedState(ks, MemberKey(ks, "us-west", "baz"), isConsistent)
	c.Check(ks.Load(ctx, client, 0), gc.IsNil)

	const (
		I1 = pr.SinkID + 1 + iota
		I2
		I1E
		I1W
		I2E
		I2W
		MBar
		MFoo
		MBaz
	)
	var fn = newSparseFlowNetwork(state, state.Items)

	c.Check(fn.firstItemNodeID, gc.Equals, I1)
	c.Check(fn.firstZoneItemNodeID, gc.Equals, I1E)
	c.Check(fn.firstMemberNodeID, gc.Equals, MBar)

	c.Check(fn.zoneItemAssignments, gc.DeepEquals, []keyspace.KeyValues{
		state.Assignments[0:1], // item-1#us-east#foo#1
		state.Assignments[1:2], // item-1#us-west#baz#0
		// item-missing#us-west#baz#0 skipped (not a current item).
		// item-two#missing#member#2 skipped (not a current zone).
		state.Assignments[4:5], // item-two#us-east#bar#0
		state.Assignments[5:6], // item-two#us-west#baz#1
	})
	c.Check(fn.memberSuffixIdxByZone, gc.DeepEquals, []map[string]pr.NodeID{
		{"bar": MBar, "foo": MFoo}, // Zone us-east.
		{"baz": MBaz},              // Zone us-west.
	})
	c.Check(fn.allZoneItemArcsByZone, gc.DeepEquals, [][]pr.Arc{
		{{To: MBar, Capacity: 1}, {To: MFoo, Capacity: 1}}, // us-east.
		{{To: MBaz, Capacity: 1}},                          // us-west.
	})

	var mf = pr.FindMaxFlow(fn)

	// Verify expected Source => Item Arcs.
	verifyArcs(c, fn, mf, pr.SourceID, [][]pr.Arc{{
		{To: I1, Capacity: 2},
		{To: I2, Capacity: 1},
	}})

	// Verify Item => ZoneItem Arcs.
	verifyArcs(c, fn, mf, I1, [][]pr.Arc{
		{
			{To: I1E, Capacity: 1, PushFront: true},
			{To: I1W, Capacity: 1, PushFront: true},
		},
		{
			{To: I1E, Capacity: 1}, // Uniform.
			{To: I1W, Capacity: 1},
		},
		{
			{To: I1E, Capacity: 1}, // R - 1.
			{To: I1W, Capacity: 1},
		},
	})
	verifyArcs(c, fn, mf, I2, [][]pr.Arc{
		{
			{To: I2E, Capacity: 1, PushFront: true},
			{To: I2W, Capacity: 1, PushFront: true},
		},
		{
			{To: I2E, Capacity: 1}, // Uniform.
			{To: I2W, Capacity: 1},
		},
		{
			{To: I2E, Capacity: 1}, // R - 1.
			{To: I2W, Capacity: 1},
		},
	})

	// Verify ZoneItem => Member arcs.
	verifyArcs(c, fn, mf, I1E, [][]pr.Arc{
		{{To: MFoo, Capacity: 1, PushFront: true}},
		{
			{To: MBar, Capacity: 1},
			{To: MFoo, Capacity: 1},
		},
	})
	verifyArcs(c, fn, mf, I2E, [][]pr.Arc{
		{{To: MBar, Capacity: 1, PushFront: true}},
		{
			{To: MBar, Capacity: 1},
			{To: MFoo, Capacity: 1},
		},
	})
	verifyArcs(c, fn, mf, I1W, [][]pr.Arc{
		{{To: MBaz, Capacity: 1, PushFront: true}},
		{{To: MBaz, Capacity: 1}},
	})

	// Verify Member => Sink arc.
	verifyArcs(c, fn, mf, MFoo, [][]pr.Arc{
		{{To: pr.SinkID, Capacity: 1}}, // Scaling: 2 * 3/6 => 1.
	})
	verifyArcs(c, fn, mf, MBar, [][]pr.Arc{
		{{To: pr.SinkID, Capacity: 1}}, // Scaling: 1 * 3/6 => 1.
	})
	verifyArcs(c, fn, mf, MBaz, [][]pr.Arc{
		{{To: pr.SinkID, Capacity: 2}}, // Scaling: 3 * 3/6 => 2.
	})

	c.Check(fn.extractAssignments(mf, nil), gc.DeepEquals, []Assignment{
		// Expect "item-1"'s max-flow Assignments are unchanged from the fixture.
		{ItemID: "item-1", MemberZone: "us-east", MemberSuffix: "foo"},
		{ItemID: "item-1", MemberZone: "us-west", MemberSuffix: "baz"},
		// "item-2" was over-replicated. Expect one Assignment is unchanged,
		// and the other is removed (choice is arbitrary but deterministic).
		{ItemID: "item-two", MemberZone: "us-west", MemberSuffix: "baz"},
	})
}

func (s *SparseSuite) TestMemberScalingOverflow(c *gc.C) {
	var client, ctx = etcdtest.TestClient(), context.Background()
	defer etcdtest.Cleanup()

	for k, v := range map[string]string{
		"/root/items/item-1": `{"R": 2}`,
		"/root/items/item-2": `{"R": 2}`,

		"/root/members/A#one": `{"R": 2}`,
		"/root/members/A#two": `{"R": 6}`,
	} {
		var _, err = client.Put(ctx, k, v)
		c.Assert(err, gc.IsNil)
	}
	var ks = NewAllocatorKeySpace("/root", testAllocDecoder{})
	var state = NewObservedState(ks, MemberKey(ks, "A", "one"), isConsistent)
	c.Check(ks.Load(ctx, client, 0), gc.IsNil)

	const (
		I1 = pr.SinkID + 1 + iota
		I2
		I1A
		I2A
		MOne
		MTwo
	)
	// Use noopNetwork to inspect arcs prior to overflow being reached.
	var fn = newSparseFlowNetwork(state, state.Items)
	var mf = pr.FindMaxFlow(noopNetwork{fn})

	// Verify expected Source => Item Arcs.
	verifyArcs(c, fn, mf, pr.SourceID, [][]pr.Arc{{
		{To: I1, Capacity: 2},
		{To: I2, Capacity: 2},
	}})
	// Verify Item => ZoneItem Arcs.
	verifyArcs(c, fn, mf, I1, [][]pr.Arc{
		{{To: I1A, Capacity: 2}},
	})
	verifyArcs(c, fn, mf, I2, [][]pr.Arc{
		{{To: I2A, Capacity: 2}},
	})
	// Verify ZoneItem => Member arcs.
	verifyArcs(c, fn, mf, I1A, [][]pr.Arc{
		{}, // No current assignments.
		{
			{To: MOne, Capacity: 1},
			{To: MTwo, Capacity: 1},
		},
	})
	verifyArcs(c, fn, mf, I2A, [][]pr.Arc{
		{},
		{
			{To: MOne, Capacity: 1},
			{To: MTwo, Capacity: 1},
		},
	})
	// Verify initial Member => Sink arc.
	verifyArcs(c, fn, mf, MOne, [][]pr.Arc{
		{{To: pr.SinkID, Capacity: 1}}, // Scaling: 2 * 4/8 => 1.
	})
	verifyArcs(c, fn, mf, MTwo, [][]pr.Arc{
		{{To: pr.SinkID, Capacity: 3}}, // Scaling: 6 * 4/8 => 3.
	})

	mf = pr.FindMaxFlow(fn) // Solve, reaching overflow.

	// Expect that push/relabel raises the height of MOne into "overflow",
	// where instead of pushing flow back to the source it instead
	// allows more flow through to the Sink (aka, assignments to the
	// member), up to the member's capacity.
	verifyArcs(c, fn, mf, MOne, [][]pr.Arc{
		{{To: pr.SinkID, Capacity: 2}}, // Overflow mode.
	})
	// Expect both items are fully assigned, despite surpassing
	// our desired MOne scaling.
	c.Check(fn.extractAssignments(mf, nil), gc.DeepEquals, []Assignment{
		{ItemID: "item-1", MemberZone: "A", MemberSuffix: "one"},
		{ItemID: "item-1", MemberZone: "A", MemberSuffix: "two"},
		{ItemID: "item-2", MemberZone: "A", MemberSuffix: "one"},
		{ItemID: "item-2", MemberZone: "A", MemberSuffix: "two"},
	})
}

func (s *SparseSuite) TestZonePlacementOverflow(c *gc.C) {
	var client, ctx = etcdtest.TestClient(), context.Background()
	defer etcdtest.Cleanup()

	for k, v := range map[string]string{
		"/root/items/item-1": `{"R": 2}`,
		"/root/items/item-2": `{"R": 2}`,
		"/root/items/item-3": `{"R": 2}`,

		"/root/members/A#one":   `{"R": 2}`,
		"/root/members/A#two":   `{"R": 2}`,
		"/root/members/B#three": `{"R": 2}`,
	} {
		var _, err = client.Put(ctx, k, v)
		c.Assert(err, gc.IsNil)
	}
	var ks = NewAllocatorKeySpace("/root", testAllocDecoder{})
	var state = NewObservedState(ks, MemberKey(ks, "A", "one"), isConsistent)
	c.Check(ks.Load(ctx, client, 0), gc.IsNil)

	const (
		I1 = pr.SinkID + 1 + iota
		_  // I2
		_  // I3
		I1A
		I1B
	)
	var fn = newSparseFlowNetwork(state, state.Items)
	var mf = pr.FindMaxFlow(noopNetwork{fn})

	// Expect that initially, we constrain Item => ZoneItem arcs such that
	// balanced assignments are preferred, and we require at least two
	// zones are utilized.
	verifyArcs(c, fn, mf, I1, [][]pr.Arc{
		{}, // No current assignments.
		{
			{To: I1A, Capacity: 1}, // Uniform.
			{To: I1B, Capacity: 1},
		},
		{
			{To: I1A, Capacity: 1}, // R - 1.
			{To: I1B, Capacity: 1},
		},
	})

	mf = pr.FindMaxFlow(fn) // Solve, reaching overflow.

	// Expect that push/relabel raises the height of I3 into "overflow"
	// where instead of pushing flow back to the source, it instead
	// allows all R flow through to a single zone. In other words, we
	// relax the two-zone constraint if it's otherwise not possible to
	// assign all items.
	verifyArcs(c, fn, mf, I1, [][]pr.Arc{
		{
			{To: I1A, Capacity: 2},
			{To: I1B, Capacity: 2},
		},
	})
	// Expect all items are fully assigned, though item-3 is allocated
	// to a single zone.
	c.Check(fn.extractAssignments(mf, nil), gc.DeepEquals, []Assignment{
		{ItemID: "item-1", MemberZone: "A", MemberSuffix: "one"},
		{ItemID: "item-1", MemberZone: "A", MemberSuffix: "two"},
		{ItemID: "item-2", MemberZone: "A", MemberSuffix: "two"},
		{ItemID: "item-2", MemberZone: "B", MemberSuffix: "three"},
		{ItemID: "item-3", MemberZone: "A", MemberSuffix: "one"},
		{ItemID: "item-3", MemberZone: "B", MemberSuffix: "three"},
	})
}

func (s *SparseSuite) TestZoneBalancing(c *gc.C) {
	var client, ctx = etcdtest.TestClient(), context.Background()
	defer etcdtest.Cleanup()

	for k, v := range map[string]string{
		"/root/items/item-1": `{"R": 6}`,

		"/root/members/A#one":   `{"R": 2}`,
		"/root/members/B#two":   `{"R": 2}`,
		"/root/members/C#three": `{"R": 2}`,

		"/root/assign/item-1#A#one#0":   ``,
		"/root/assign/item-1#C#three#1": ``,
	} {
		var _, err = client.Put(ctx, k, v)
		c.Assert(err, gc.IsNil)
	}
	var ks = NewAllocatorKeySpace("/root", testAllocDecoder{})
	var state = NewObservedState(ks, MemberKey(ks, "us-west", "baz"), isConsistent)
	c.Check(ks.Load(ctx, client, 0), gc.IsNil)

	const (
		I1 = pr.SinkID + 1 + iota
		I1A
		I1B
		I1C
	)
	var fn = newSparseFlowNetwork(state, state.Items)
	var mf = pr.FindMaxFlow(noopNetwork{fn})

	verifyArcs(c, fn, mf, I1, [][]pr.Arc{
		{
			{To: I1A, Capacity: 1, PushFront: true},
			{To: I1C, Capacity: 1, PushFront: true},
		},
		{
			{To: I1A, Capacity: 2}, // Uniform.
			{To: I1B, Capacity: 2},
			{To: I1C, Capacity: 2},
		},
		{
			{To: I1A, Capacity: 5}, // R - 1.
			{To: I1B, Capacity: 5},
			{To: I1C, Capacity: 5},
		},
	})
}

func verifyArcs(c *gc.C, fs *sparseFlowNetwork, mf *pr.MaxFlow, from pr.NodeID, expect [][]pr.Arc) {
	var page = pr.PageInitial

	for page != pr.PageEOF {
		var arcs, next = fs.Arcs(mf, from, page)

		c.Assert(len(expect), gc.Not(gc.Equals), 0)
		c.Check(arcs, gc.DeepEquals, expect[0])
		expect = expect[1:]
		page = next
	}
	c.Check(expect, gc.HasLen, 0)
}

// noopNetwork is a pr.Network of size |nodes| with no Arcs.
type noopNetwork struct{ *sparseFlowNetwork }

func (s noopNetwork) Nodes() int                           { return s.sparseFlowNetwork.Nodes() }
func (s noopNetwork) InitialHeight(id pr.NodeID) pr.Height { return 0 }
func (s noopNetwork) Arcs(*pr.MaxFlow, pr.NodeID, pr.PageToken) ([]pr.Arc, pr.PageToken) {
	return nil, pr.PageEOF
}

var _ = gc.Suite(&SparseSuite{})
