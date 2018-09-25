package push_relabel

import (
	"testing"

	gc "github.com/go-check/check"
)

type PushRelabelSuite struct{}

func (s *PushRelabelSuite) TestSimpleFixtureOne(c *gc.C) {
	// Build the graph as described by the Push/Relabel Wikipedia page.
	// There is only one valid maximum flow.
	// https://en.wikipedia.org/wiki/Push%E2%80%93relabel_maximum_flow_algorithm#Example
	var S, A, B, C, D, T Node
	S.Height = 6

	var graph = fixture{
		{&S, &A, 15, 0, 10},
		{&S, &C, 4, 0, 4},
		{&A, &B, 12, 0, 10},
		{&C, &D, 10, 0, 7},
		{&D, &A, 5, 0, 0},
		{&B, &C, 3, 0, 3},
		{&B, &T, 7, 0, 7},
		{&D, &T, 10, 0, 7},
	}
	graph.build()
	FindMaxFlow(&S, &T)
	graph.verify(c)
}

func (s *PushRelabelSuite) TestSimpleFixtureTwo(c *gc.C) {
	// Build the graph as described by the Maximum Flow Wikipedia page.
	// There is only one valid maximum flow.
	// https://en.wikipedia.org/wiki/Maximum_flow_problem
	var S, O, P, Q, R, T Node
	S.Height = 6

	var graph = fixture{
		{&S, &O, 3, 0, 3},
		{&S, &P, 3, 0, 2},
		{&O, &P, 2, 0, 0},
		{&O, &Q, 3, 0, 3},
		{&P, &R, 2, 0, 2},
		{&Q, &R, 4, 0, 1},
		{&Q, &T, 2, 0, 2},
		{&R, &T, 3, 0, 3},
	}
	graph.build()
	FindMaxFlow(&S, &T)
	graph.verify(c)
}

func (s *PushRelabelSuite) TestPriorityFixture(c *gc.C) {
	// Build a graph with multiple possible maximum flows. Set Arc priorities
	// to coerce a specific, expected max-flow solution.
	var S, A, B, C, D, E, F, G, H, T Node
	S.Height = 10

	var graph = fixture{
		{&S, &A, 3, 2, 3},
		{&S, &B, 3, 1, 2},
		{&S, &C, 3, 0, 0},

		{&A, &D, 3, 2, 3},
		{&B, &D, 3, 1, 2},
		{&C, &D, 3, 0, 0},

		{&D, &E, 5, 0, 5},

		{&E, &F, 3, 0, 0},
		{&E, &G, 3, 1, 2},
		{&E, &H, 3, 2, 3},

		{&F, &T, 3, 0, 0},
		{&G, &T, 3, 1, 2},
		{&H, &T, 3, 2, 3},
	}
	graph.build()
	SortNodeArcs(S, A, B, C, D, E, F, G, H, T)
	FindMaxFlow(&S, &T)
	graph.verify(c)
}

func (s *PushRelabelSuite) TestScheduleFixture(c *gc.C) {
	// The stability of the priority heuristic begins to break down if not all
	// flow can be pushed from source to sink. Expect that the solution is still
	// mostly stable (but not always), and is stable with this specific fixture.
	var S, I1, I2, I3, I4, I1A, I1B, I2A, I2B, I3A, I3B, I4A, I4B, M1A, M2A, M3B, T Node
	S.Height = 14

	var graph = fixture{
		{&S, &I1, 3, 1, 2},
		{&S, &I2, 3, 1, 2},
		{&S, &I3, 3, 1, 2},

		{&I1, &I1A, 2, 1, 1},
		{&I1, &I1B, 2, 1, 1},
		{&I2, &I2A, 2, 1, 1},
		{&I2, &I2B, 2, 1, 1},
		{&I3, &I3A, 2, 2, 2},
		{&I3, &I3B, 2, 0, 0},
		{&I4, &I4A, 2, 0, 0},
		{&I4, &I4B, 2, 0, 0},

		{&I1A, &M1A, 1, 0, 0},
		{&I1A, &M2A, 1, 2, 1},
		{&I2A, &M1A, 1, 2, 1},
		{&I2A, &M2A, 1, 0, 0},
		{&I3A, &M1A, 1, 2, 1},
		{&I3A, &M2A, 1, 2, 1},
		{&I4A, &M1A, 1, 0, 0},
		{&I4A, &M2A, 1, 0, 0},

		{&I1B, &M3B, 1, 2, 1},
		{&I2B, &M3B, 1, 2, 1},
		{&I3B, &M3B, 1, 0, 0},
		{&I4B, &M3B, 1, 0, 0},

		{&M1A, &T, 2, 2, 2},
		{&M2A, &T, 2, 2, 2},
		{&M3B, &T, 2, 2, 2},
	}
	graph.build()
	FindMaxFlow(&S, &T)
	graph.verify(c)
}

type fixtureArc struct {
	from, to                       *Node
	capacity, priority, expectFlow int
}

type fixture []fixtureArc

func (f fixture) build() {
	for _, a := range f {
		AddArc(a.from, a.to, a.capacity, a.priority)
	}
}

func (f fixture) verify(c *gc.C) {
	for _, a := range f {
		verifyFlow(c, a.from, a.to, a.expectFlow)
	}
}

func verifyFlow(c *gc.C, from, to *Node, flow int) {
	var foundFromTo, foundToFrom bool

	for _, a := range from.Arcs {
		if a.Target == to {
			c.Check(a.Flow, gc.Equals, int32(flow))
			foundFromTo = true
			break
		}
	}
	for _, a := range to.Arcs {
		if a.Target == from {
			c.Check(a.Flow, gc.Equals, int32(-flow))
			foundToFrom = true
			break
		}
	}
	c.Check(foundFromTo, gc.Equals, true)
	c.Check(foundToFrom, gc.Equals, true)
}

var _ = gc.Suite(&PushRelabelSuite{})

func Test(t *testing.T) { gc.TestingT(t) }
