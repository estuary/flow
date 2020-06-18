package sparse_push_relabel

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSimpleFixtureOne(t *testing.T) {
	// Build the graph as described by the Push/Relabel Wikipedia page.
	// There is only one valid maximum flow.
	// https://en.wikipedia.org/wiki/Push%E2%80%93relabel_maximum_flow_algorithm#Example
	const (
		A = SinkID + 1
		B = A + 1
		C = B + 1
		D = C + 1
	)
	var arcs = fixedArcs{
		SourceID: {{
			{To: A, Capacity: 15},
			{To: C, Capacity: 4},
		}},
		A: {{{To: B, Capacity: 12}}},
		B: {
			// Arbitrarily split arcs across two pages (doesn't affect results).
			{{To: C, Capacity: 3}},
			{{To: SinkID, Capacity: 7}},
		},
		C: {{{To: D, Capacity: 10}}},
		D: {{
			{To: A, Capacity: 5},
			{To: SinkID, Capacity: 10},
		}},
	}
	var mf = FindMaxFlow(testNetwork{nodes: 6, arcsFn: arcs.fn})

	require.Equal(t, toMap(mf), map[Adjacency]Rate{
		{From: A, To: B}:        10,
		{From: B, To: C}:        3,
		{From: B, To: SinkID}:   7,
		{From: C, To: D}:        7,
		{From: D, To: SinkID}:   7,
		{From: SourceID, To: A}: 10,
		{From: SourceID, To: C}: 4,
	})
}

func TestSimpleFixtureTwo(t *testing.T) {
	// Build the graph as described by the Maximum Flow Wikipedia page.
	// There is only one valid maximum flow.
	// https://en.wikipedia.org/wiki/Maximum_flow_problem
	const (
		O = SinkID + 1
		P = O + 1
		Q = P + 1
		R = Q + 1
	)
	var arcs = fixedArcs{
		SourceID: {{
			{To: O, Capacity: 3},
			{To: P, Capacity: 3},
		}},
		O: {{
			{To: Q, Capacity: 3},
			{To: P, Capacity: 2},
		}},
		P: {{{To: R, Capacity: 2}}},
		Q: {{
			{To: SinkID, Capacity: 2},
			{To: R, Capacity: 4},
		}},
		R: {{{To: SinkID, Capacity: 3}}},
	}
	var mf = FindMaxFlow(testNetwork{nodes: 6, arcsFn: arcs.fn})

	require.Equal(t, toMap(mf), map[Adjacency]Rate{
		{From: O, To: Q}:        3,
		{From: P, To: R}:        2,
		{From: Q, To: R}:        1,
		{From: Q, To: SinkID}:   2,
		{From: R, To: SinkID}:   3,
		{From: SourceID, To: O}: 3,
		{From: SourceID, To: P}: 2,
	})
}

func TestCommonSubstructureFixture(t *testing.T) {
	// Arcs fan out from the source to Nodes [2, 5]. Nodes [2, 5] are then fully
	// connected with Nodes [6, 9], which then fan in to the sink. Arcs have
	// plenty of capacity and multiple max-flows are possible.
	var arcsFn = func(g *MaxFlow, id NodeID, token PageToken) ([]Arc, PageToken) {
		if id == SourceID {
			return []Arc{
				{To: 2, Capacity: 2},
				{To: 3, Capacity: 2},
				{To: 4, Capacity: 2},
				{To: 5, Capacity: 2},
			}, PageEOF
		} else if id >= 2 && id < 6 {
			return []Arc{
				{To: 6, Capacity: 100},
				{To: 7, Capacity: 100},
				{To: 8, Capacity: 100},
				{To: 9, Capacity: 100},
			}, PageEOF
		} else {
			return []Arc{{To: SinkID, Capacity: 100}}, PageEOF
		}
	}
	var mf = FindMaxFlow(testNetwork{nodes: 11, arcsFn: arcsFn})

	// Expect that the NodeID-based shift during arc selection results
	// in flow which is balanced across arcs and Nodes.
	require.Equal(t, toMap(mf), map[Adjacency]Rate{
		{From: SourceID, To: 2}: 2,
		{From: SourceID, To: 3}: 2,
		{From: SourceID, To: 4}: 2,
		{From: SourceID, To: 5}: 2,

		{From: 2, To: 8}: 2,
		{From: 3, To: 9}: 2,
		{From: 4, To: 6}: 2,
		{From: 5, To: 7}: 2,

		{From: 6, To: SinkID}: 2,
		{From: 7, To: SinkID}: 2,
		{From: 8, To: SinkID}: 2,
		{From: 9, To: SinkID}: 2,
	})

	// Again. This time, express a preference for {From: id, To: id+2} adjacency
	// via an initial []Arc page.
	var arcsFn2 = func(g *MaxFlow, id NodeID, token PageToken) ([]Arc, PageToken) {
		if id >= 2 && id < 6 && token == PageInitial {
			return []Arc{{To: id + 4, Capacity: 1, PushFront: true}}, token + 1
		}
		return arcsFn(g, id, token)
	}

	mf = FindMaxFlow(testNetwork{nodes: 11, arcsFn: arcsFn2})

	require.Equal(t, toMap(mf), map[Adjacency]Rate{
		{From: SourceID, To: 2}: 2,
		{From: SourceID, To: 3}: 2,
		{From: SourceID, To: 4}: 2,
		{From: SourceID, To: 5}: 2,

		// Preferred arc usages:
		{From: 2, To: 6}: 1,
		{From: 3, To: 7}: 1,
		{From: 4, To: 8}: 1,
		{From: 5, To: 9}: 1,

		// Remainder usages:
		{From: 2, To: 8}: 1,
		{From: 3, To: 9}: 1,
		{From: 4, To: 6}: 1,
		{From: 5, To: 7}: 1,

		{From: 6, To: SinkID}: 2,
		{From: 7, To: SinkID}: 2,
		{From: 8, To: SinkID}: 2,
		{From: 9, To: SinkID}: 2,
	})
}

func TestOverflowFixture(t *testing.T) {
	// Arcs fan out from the source to Nodes [2, 4], which then fan in to node 5,
	// which connects to the Sink. This 5 => sink arc fundamentally constrains
	// graph throughput and causes residuals to be pushed back to the source.
	var arcsFn = func(g *MaxFlow, id NodeID, token PageToken) ([]Arc, PageToken) {
		if id == SourceID {
			return []Arc{
				{To: 2, Capacity: 1},
				{To: 3, Capacity: 1},
				{To: 4, Capacity: 1},
			}, PageEOF
		} else if id >= 2 && id < 5 {
			return []Arc{
				{To: 5, Capacity: 1},
			}, PageEOF
		}
		return []Arc{{To: SinkID, Capacity: 1}}, PageEOF
	}
	var mf = FindMaxFlow(testNetwork{nodes: 11, arcsFn: arcsFn})

	// Expect we find one of the valid max-flow solutions.
	require.Equal(t, toMap(mf), map[Adjacency]Rate{
		{From: SourceID, To: 4}: 1,
		{From: 4, To: 5}:        1,
		{From: 5, To: SinkID}:   1,
	})

	// Again. This time, upon reaching a threshold height add additional
	// capacity to the sink (though still not enough for all graph excess).
	var arcsFn2 = func(g *MaxFlow, id NodeID, token PageToken) ([]Arc, PageToken) {
		if id == 5 && g.nodes[5].height >= 12 {
			return []Arc{{To: SinkID, Capacity: 2}}, PageEOF
		}
		return arcsFn(g, id, token)
	}

	mf = FindMaxFlow(testNetwork{nodes: 11, arcsFn: arcsFn2})

	// Expect the new capacity is used.
	require.Equal(t, toMap(mf), map[Adjacency]Rate{
		{From: SourceID, To: 2}: 1,
		{From: SourceID, To: 4}: 1,
		{From: 2, To: 5}:        1,
		{From: 4, To: 5}:        1,
		{From: 5, To: SinkID}:   2,
	})
}

func TestNodeDischarge(t *testing.T) {
	var mf = newMaxFlow(testNetwork{nodes: 6})
	mf.nodes[2].height = 2
	mf.nodes[3].height = 3
	mf.nodes[4].height = 4
	mf.nodes[5].height = mf.nodes[SourceID].height

	// Add a residual from 5 => 2.
	var fid = mf.addFlow(Adjacency{From: 5, To: 2}, false)
	mf.flows[fid].Rate = 5
	verifyFlows(t, mf, 5, []expectedFlow{{to: 2, rate: 5}})

	// Add a residual from Source => 2.
	fid = mf.addFlow(Adjacency{From: SourceID, To: 2}, true)
	mf.flows[fid].Rate = 10
	verifyFlows(t, mf, SourceID, []expectedFlow{{to: 2, rate: 10}})

	var arcs = fixedArcs{
		// Arbitrarily split arcs across two pages (doesn't affect results).
		2: {
			{{To: 4, Capacity: 1, PushFront: true}},
			{
				{To: SinkID, Capacity: 2},
				{To: 3, Capacity: 3},
			},
		},
	}
	var s = testNetwork{arcsFn: arcs.fn}

	// Expect our first discharge is to the sink.
	mf.nodes[2].excess = 1
	mf.discharge(2, s)

	verifyFlows(t, mf, 2, []expectedFlow{{to: SinkID, rate: 1}}) // Flow added.
	require.Equal(t, mf.nodes[SinkID].excess, Rate(1))
	require.Equal(t, mf.nodes[2].nextHeight, Height(5)) // nodes[4].height + 1.

	// Expect we can discharge again to the sink (we haven't stepped past the Arc).
	mf.nodes[2].excess = 1
	mf.discharge(2, s)

	verifyFlows(t, mf, 2, []expectedFlow{{to: SinkID, rate: 2}}) // Updated.
	require.Equal(t, mf.nodes[SinkID].excess, Rate(2))
	require.Equal(t, mf.nodes[2].height, Height(2)) // No relabel occurred.

	// Arc to Sink is saturated. We re-label to push to node 3.
	mf.nodes[2].excess = 2
	mf.discharge(2, s)

	verifyFlows(t, mf, 2, []expectedFlow{
		{to: SinkID, rate: 2},
		{to: 3, rate: 2}, // Added.
	})
	require.Equal(t, mf.nodes[3].excess, Rate(2))
	require.Equal(t, mf.nodes[2].height, Height(4))     // A tighter bound was found with node 3.
	require.Equal(t, mf.nodes[2].nextHeight, Height(5)) // nodes[4].height + 1.

	// We're able to push once more to 3, then relabel before pushing to 4.
	mf.nodes[2].excess = 2
	mf.discharge(2, s)

	verifyFlows(t, mf, 2, []expectedFlow{
		{to: 4, rate: 1}, // Added at list front.
		{to: SinkID, rate: 2},
		{to: 3, rate: 3}, // Updated.
	})
	require.Equal(t, mf.nodes[3].excess, Rate(3))
	require.Equal(t, mf.nodes[4].excess, Rate(1))
	require.Equal(t, mf.nodes[2].height, Height(5))
	// Node 3 & the sink are saturated, and we just pushed to 4, so no |nextHeight| yet.
	require.Equal(t, mf.nodes[2].nextHeight, maxHeight)

	// On next discharge we must relabel. We then push along the residual to 5.
	// Though it was added first, the residual from Source used PushFront, and
	// residuals are walked in reverse order.
	mf.nodes[2].excess = 3
	mf.discharge(2, s)

	verifyFlows(t, mf, 5, []expectedFlow{{to: 2, rate: 2}}) // Was 5.
	require.Equal(t, mf.nodes[2].height, Height(7))
	require.Equal(t, mf.nodes[2].nextHeight, maxHeight)
	require.Equal(t, mf.nodes[2].dischargePage, PageEOF)

	// Discharge again. We exhaust the residual to 5, and begin pushing
	// along the residual to Source.
	mf.nodes[2].excess = 3
	mf.discharge(2, s)

	verifyFlows(t, mf, 5, []expectedFlow{})                        // Removed.
	verifyFlows(t, mf, SourceID, []expectedFlow{{to: 2, rate: 9}}) // Was 10.

	mf.nodes[2].excess = 9
	mf.discharge(2, s)

	verifyFlows(t, mf, SourceID, []expectedFlow{}) // Removed.
}

func TestFlowTracking(t *testing.T) {
	var mf = newMaxFlow(testNetwork{nodes: 5})

	// Set initial free Flow fixtures.
	mf.flows = append(mf.flows, Flow{}, Flow{}, Flow{})
	mf.freeFlows = []flowID{2, 3, 1}

	var id = mf.addFlow(Adjacency{SourceID, 2}, false)
	require.Equal(t, id, flowID(1))
	id = mf.addFlow(Adjacency{SourceID, 4}, true)
	require.Equal(t, id, flowID(3))
	id = mf.addFlow(Adjacency{SourceID, 3}, false)
	require.Equal(t, id, flowID(2))
	id = mf.addFlow(Adjacency{2, 3}, true)
	require.Equal(t, id, flowID(4))
	id = mf.addFlow(Adjacency{2, 4}, false)
	require.Equal(t, id, flowID(5))
	id = mf.addFlow(Adjacency{3, 4}, false)
	require.Equal(t, id, flowID(6))

	// Expect each Flow fixture is tracked and properly linked
	// into per-node forward and reverse (residual) lists.
	require.Equal(t, mf.flows, []Flow{
		{}, // Sentinel.
		{
			Adjacency: Adjacency{From: SourceID, To: 2},
			fwdPrev:   3, fwdNext: 2, revPrev: 0, revNext: 0,
		},
		{
			Adjacency: Adjacency{From: SourceID, To: 3},
			fwdPrev:   1, fwdNext: 0, revPrev: 4, revNext: 0,
		},
		{
			Adjacency: Adjacency{From: SourceID, To: 4},
			fwdPrev:   0, fwdNext: 1, revPrev: 0, revNext: 5,
		},
		{
			Adjacency: Adjacency{From: 2, To: 3},
			fwdPrev:   0, fwdNext: 5, revPrev: 0, revNext: 2,
		},
		{
			Adjacency: Adjacency{From: 2, To: 4},
			fwdPrev:   4, fwdNext: 0, revPrev: 3, revNext: 6,
		},
		{
			Adjacency: Adjacency{From: 3, To: 4},
			fwdPrev:   0, fwdNext: 0, revPrev: 5, revNext: 0,
		},
	})

	// Expect Nodes reflect the correct lists heads and tails.
	require.Equal(t, mf.nodes[SourceID].fwdHead, flowID(3))
	require.Equal(t, mf.nodes[SourceID].fwdTail, flowID(2))
	require.Equal(t, mf.nodes[SourceID].revHead, flowID(0))
	require.Equal(t, mf.nodes[SourceID].revTail, flowID(0))

	require.Equal(t, mf.nodes[2].fwdHead, flowID(4))
	require.Equal(t, mf.nodes[2].fwdTail, flowID(5))
	require.Equal(t, mf.nodes[2].revHead, flowID(1))
	require.Equal(t, mf.nodes[2].revTail, flowID(1))

	require.Equal(t, mf.nodes[4].fwdHead, flowID(0))
	require.Equal(t, mf.nodes[4].fwdTail, flowID(0))
	require.Equal(t, mf.nodes[4].revHead, flowID(3))
	require.Equal(t, mf.nodes[4].revTail, flowID(6))

	// Begin incrementally removing flows. Expect links of remaining Flows
	// are updated to reflect removals.
	mf.removeFlow(6)
	mf.removeFlow(1)

	require.Equal(t, mf.flows, []Flow{
		{},
		{},
		{
			Adjacency: Adjacency{From: SourceID, To: 3},
			fwdPrev:   3, fwdNext: 0, revPrev: 4, revNext: 0,
		},
		{
			Adjacency: Adjacency{From: SourceID, To: 4},
			fwdPrev:   0, fwdNext: 2, revPrev: 0, revNext: 5,
		},
		{
			Adjacency: Adjacency{From: 2, To: 3},
			fwdPrev:   0, fwdNext: 5, revPrev: 0, revNext: 2,
		},
		{
			Adjacency: Adjacency{From: 2, To: 4},
			fwdPrev:   4, fwdNext: 0, revPrev: 3, revNext: 0,
		},
		{},
	})

	require.Equal(t, mf.nodes[2].revHead, flowID(0)) // Removed (was 1).
	require.Equal(t, mf.nodes[2].revTail, flowID(0)) // Removed (was 1).

	require.Equal(t, mf.nodes[4].revHead, flowID(3))
	require.Equal(t, mf.nodes[4].revTail, flowID(5)) // Updated (was 6).

	mf.removeFlow(3)
	mf.removeFlow(5)

	require.Equal(t, mf.flows, []Flow{
		{},
		{},
		{
			Adjacency: Adjacency{From: SourceID, To: 3},
			fwdPrev:   0, fwdNext: 0, revPrev: 4, revNext: 0,
		},
		{},
		{
			Adjacency: Adjacency{From: 2, To: 3},
			fwdPrev:   0, fwdNext: 0, revPrev: 0, revNext: 2,
		},
		{},
		{},
	})

	require.Equal(t, mf.nodes[SourceID].fwdHead, flowID(2)) // Updated (was 3).
	require.Equal(t, mf.nodes[SourceID].fwdTail, flowID(2))

	require.Equal(t, mf.nodes[2].fwdHead, flowID(4))
	require.Equal(t, mf.nodes[2].fwdTail, flowID(4)) // Updated (was 5).

	require.Equal(t, mf.nodes[4].revHead, flowID(0)) // Removed (was 3).
	require.Equal(t, mf.nodes[4].revTail, flowID(0)) // Removed (was 5).

	mf.removeFlow(4)
	mf.removeFlow(2)

	require.Equal(t, mf.flows, make([]Flow, 7))

	// Expect after removing all Flows, Nodes are in their initial state.
	for i := range mf.nodes {
		require.Equal(t, mf.nodes[i].fwdHead, flowID(0))
		require.Equal(t, mf.nodes[i].fwdTail, flowID(0))
		require.Equal(t, mf.nodes[i].revHead, flowID(0))
		require.Equal(t, mf.nodes[i].revTail, flowID(0))
	}
	// All prior flowIDs are on the free-list.
	require.Equal(t, mf.freeFlows, []flowID{6, 1, 3, 5, 4, 2})
}

func TestActiveTrackingOnHeight(t *testing.T) {
	var mf = newMaxFlow(testNetwork{nodes: 5})
	mf.nodes[2].height = 2
	mf.nodes[3].height = 3
	mf.nodes[4].height = 4

	require.Equal(t, mf.active, []NodeID{SourceID})
	mf.updateExcess(2, 5)
	require.Equal(t, mf.active, []NodeID{SourceID, 2})

	mf.updateExcess(2, -3)
	mf.updateExcess(4, 2)
	mf.updateExcess(SinkID, 3)
	mf.updateExcess(3, 2)
	mf.updateExcess(4, 1)
	mf.updateExcess(3, -1)

	for _, expect := range []struct {
		id     NodeID
		excess Rate
	}{
		{SourceID, math.MaxInt32},
		{4, 3},
		{3, 1},
		{2, 2},
	} {
		var id, ok = mf.popActiveNode()
		require.True(t, ok)
		require.Equal(t, id, expect.id)
		require.Equal(t, mf.nodes[id].excess, expect.excess)
	}
	var _, ok = mf.popActiveNode()
	require.False(t, ok)
}

func toMap(g *MaxFlow) map[Adjacency]Rate {
	var out = make(map[Adjacency]Rate)

	for _, f := range g.flows {
		if f.Rate != 0 {
			out[f.Adjacency] = f.Rate
		}
	}
	return out
}

type expectedFlow struct {
	to   NodeID
	rate Rate
}

func verifyFlows(t *testing.T, mf *MaxFlow, nid NodeID, expect []expectedFlow) {
	mf.Flows(nid, func(flow Flow) {
		require.Equal(t, flow.To, expect[0].to)
		require.Equal(t, flow.Rate, expect[0].rate)
		expect = expect[1:]
	})
	require.Equal(t, expect, []expectedFlow{})
}

type testNetwork struct {
	nodes  int
	arcsFn func(g *MaxFlow, id NodeID, token PageToken) ([]Arc, PageToken)
}

func (s testNetwork) Nodes() int                     { return s.nodes }
func (s testNetwork) InitialHeight(id NodeID) Height { return 0 }
func (s testNetwork) Arcs(g *MaxFlow, id NodeID, token PageToken) ([]Arc, PageToken) {
	return s.arcsFn(g, id, token)
}

type fixedArcs map[NodeID][][]Arc

func (f fixedArcs) fn(g *MaxFlow, id NodeID, token PageToken) ([]Arc, PageToken) {
	if pages, token := f[id][token], token+1; token == PageToken(len(f[id])) {
		return pages, PageEOF
	} else {
		return pages, token
	}
}
