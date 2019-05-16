// Package push_relabel implements a greedy variant of the push/relabel algorithm.
// Specifically, it is a standard variant of the algorithm using node "discharge"
// operations with height-based node prioritization (see [1]), but additionally
// introduces a notion of Arc "Priority" with greedy selection at each step.
// Since push/relabel specifies no order over admissible arcs, this does not
// change the properties of the algorithm. Use of priorities provide no formal
// guarantees (as min-cost/max-flow would, for example). However in practice,
// where priorities encode a previous max-flow solution of a closely related,
// incrementally updated flow network, push/relabel-with-priorities does a good
// job of minimizing departures from the prior solution at low computational
// cost.
//
//  [1] https://en.wikipedia.org/wiki/Push%E2%80%93relabel_maximum_flow_algorithm#%22Current-arc%22_data_structure_and_discharge_operation )
//
package push_relabel

import (
	"container/heap"
	"math"
	"sort"
)

// Arc defines an edge along which flow may occur in a flow network. Arcs are
// created in pairs: every Arc on a Node with positive Capacity, Flow, and
// Priority, has a reciprocal Arc on the target Node with Capacity of zero, and
// negative Flow and Priority of equivalent absolute values (the residual Arc).
type Arc struct {
	// Capacity of the Arc in the flow network. Positive,
	// however residual Arcs may have Capacity of zero.
	Capacity int32
	// Output Flow of the Arc in the network. Zero or positive in Arcs with Capacity > 0,
	// and zero or negative in their Capacity=0 residuals.
	Flow int32
	// Priority is the (descending) order in which Arcs should be selected for.
	Priority int8
	// Index of the reciprocal of this Arc, in |Target.Arcs|.
	reciprocal uint32
	// Target Node of this Arc.
	Target *Node
}

// Node defines a vertex in a flow network, through which flow occurs.
type Node struct {
	// User-defined ID of this Node. Useful for identifying Nodes reached
	// by walking Arcs.
	ID uint32
	// Height label of this Node. Run-time is reduced if this is initialized
	// to the distance of the Node from the flow network sink.
	Height uint32
	// Ordered Arcs of this Node (both primary and residual).
	Arcs []Arc
	// Excess flow of this Node, which must be reduced to zero before push/relabel completes.
	excess uint32
	// next Arc in |Arcs| to be evaluated.
	next uint32
}

// FindMaxFlow determines the maximum flow of the flow network rooted at |source|.
func FindMaxFlow(source, sink *Node) {
	source.excess = math.MaxUint32
	var active = &heightHeap{source}

	for len(*active) != 0 {
		var node = heap.Pop(active).(*Node)

		if node.excess == 0 {
			panic("invalid pre-excess")
		}
		discharge(node, sink, active)

		if node.excess != 0 && node != source {
			panic("invalid post-excess")
		}
	}
}

// discharge pushes all excess flow from a Node, relabeling the Node Height as required.
func discharge(node, sink *Node, active *heightHeap) {
	for node.excess > 0 {

		// "Relabel" case. We examine Arcs having available capacity in the residual graph
		// to identity the neighbor(s) of minimal height to which we could push flow. Then, we
		// increase the |node|'s height to be one higher. This maintains the invariant that
		// we always push flow "downhill".
		if node.next == uint32(len(node.Arcs)) {

			var minHeight uint32 = math.MaxUint32
			for _, adj := range node.Arcs {
				if adj.Capacity-adj.Flow > 0 {
					minHeight = min(minHeight, adj.Target.Height)
				}
			}
			if minHeight == math.MaxUint32 {
				return
			}

			node.Height = minHeight + 1
			node.next = 0
		}

		var adj = node.Arcs[node.next]
		var residual = adj.Capacity - adj.Flow

		if residual > 0 && node.Height > adj.Target.Height {
			var delta = min(node.excess, uint32(residual))

			node.Arcs[node.next].Flow += int32(delta)
			adj.Target.Arcs[adj.reciprocal].Flow -= int32(delta)

			node.excess -= delta
			adj.Target.excess += delta

			if adj.Target.excess == delta && adj.Target != sink {
				// Our push caused |adj.target| to become an active node.
				heap.Push(active, adj.Target)
			}
		}

		node.next++
	}
}

// InitNodes returns a slice of Nodes having size |n|. If |nodes| has
// sufficient capacity, it is re-sliced and returned. Otherwise, a new
// backing slice is allocated. All Nodes are initialized to Height |height|.
func InitNodes(nodes []Node, n int, height int) []Node {
	if cap(nodes) < n {
		var t = make([]Node, n, n*2)
		copy(t, nodes)
		nodes = t
	} else {
		nodes = nodes[:n]
	}

	for i := range nodes {
		nodes[i] = Node{
			ID:     uint32(i),
			Height: uint32(height),
			Arcs:   nodes[i].Arcs[:0],
		}
	}
	return nodes
}

// AddArc adds an Arc from |from| to |to|, having |capacity| and |priority|.
// It also creates a residual Arc from |to| to |from|.
func AddArc(from, to *Node, capacity, priority int) {
	var fromInd, toInd = len(from.Arcs), len(to.Arcs)

	if capacity < 0 || capacity > math.MaxInt32 {
		panic("invalid capacity")
	}
	if priority < 0 || priority > math.MaxInt8 {
		panic("invalid priority")
	}

	from.Arcs = append(from.Arcs, Arc{
		Capacity:   int32(capacity),
		Priority:   int8(priority),
		reciprocal: uint32(toInd),
		Target:     to,
	})
	to.Arcs = append(to.Arcs, Arc{
		Capacity:   0,
		Priority:   int8(-priority),
		reciprocal: uint32(fromInd),
		Target:     from,
	})
}

// SortNodeArcs orders the Arcs of one or more Nodes by their respective priorities.
func SortNodeArcs(nodes ...Node) {
	for n := range nodes {
		var arcs = nodes[n].Arcs

		sort.Slice(arcs, func(i, j int) bool {
			return arcs[i].Priority > arcs[j].Priority
		})
		// Fix-up reciprocal indices.
		for i, a := range arcs {
			a.Target.Arcs[a.reciprocal].reciprocal = uint32(i)
		}
	}
}

func min(a, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}

// heightHeap orders Nodes on descending Node Height.
type heightHeap []*Node

func (h heightHeap) Len() int           { return len(h) }
func (h heightHeap) Less(i, j int) bool { return h[i].Height > h[j].Height }

func (h heightHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *heightHeap) Push(x interface{}) { *h = append(*h, x.(*Node)) }
func (h *heightHeap) Pop() interface{} {
	var old, l = *h, len(*h)
	var x = old[l-1]
	*h = old[0 : l-1]
	return x
}
