// Package sparse_push_relabel implements a greedy variant of the push/relabel algorithm.
// Specifically, it is a standard variant of the algorithm using node "discharge"
// operations with height-based node prioritization (see [1]), but additionally
// introduces a virtual graph model which allows for dynamically controlling the
// possible Arcs and capacities which are exposed to the solver.
//
// As the solver is greedy at each individual step, and push/relabel specifies
// no particular order over admissible arcs, networks can leverage arc
// presentation order to coerce the maximum flow solution towards a previous
// solution without breaking the fundamental properties of the algorithm.
//
// Tuning the presentation of certain arcs over others provide no formal
// guarantees (as min-cost/max-flow would, for example). However in practice,
// where priorities encode a previous max-flow solution of a closely related,
// incrementally updated flow network, push/relabel does a good
// job of minimizing departures from the prior solution at low computational
// cost. The solver will depart from that desired solution only where necessary
// to establish a maximum flow.
//
//	[1] https://en.wikipedia.org/wiki/Push%E2%80%93relabel_maximum_flow_algorithm#%22Current-arc%22_data_structure_and_discharge_operation )
package sparse_push_relabel

import (
	"container/heap"
	"math"
)

type (
	// Rate is the unit of flow velocity.
	Rate int32
	// Height of a node.
	Height int32
	// ID (index) of a node.
	NodeID int32
	// PageToken is used to traverse through a variable number of []Arc "pages"
	// supplied by instances of the Network interface.
	PageToken int32
	// ID (index) of a Flow.
	flowID int32
)

const (
	// PageInitial is the first page of node []Arcs.
	PageInitial PageToken = 0
	// PageEOF indicates that no further pages of node []Arcs remain.
	PageEOF PageToken = math.MaxInt32

	// SourceID is the node from which all flow originates.
	SourceID NodeID = 0
	// SinkID is the node to which all flow is ultimately directed.
	SinkID NodeID = 1

	maxHeight Height = math.MaxInt32
)

// Arc is directed edge between a current node and another.
type Arc struct {
	To       NodeID // Node to which this Arc directs.
	Capacity Rate   // Maximum flow Rate of this Arc.
	// PushFront indicates whether a Flow associated with this Arc should be
	// added at the head or tail of its linked lists. The primary implication
	// is that Flows residuals are examined by discharge() in reverse order
	// (eg, LIFO). By pushing to the front of the list, an Arc can express
	// a preference that its residual should be considered only if no other
	// residual will suffice.
	PushFront bool
}

// Network is a flow network for which a maximum flow is desired. The
// push/relabel solver inspects the Network as needed while executing the
// algorithm. Arcs in particular may be called many times for a given NodeID
// and PageToken.
type Network interface {
	// Nodes returns the number of nodes in the network,
	// including the source & sink.
	Nodes() int
	// InitialHeight returns the initial Height of each node. This may be zero
	// without impacting correctness, but for better performance should be the
	// node's distance from the Sink.
	InitialHeight(NodeID) Height
	// Arcs returns the given page of node []Arcs, along with the next
	// PageToken of Arcs which may be requested. The initial PageToken
	// will be PageInitial, and PageEOF should be returned to indicate
	// that no further pages remain.
	Arcs(*MaxFlow, NodeID, PageToken) ([]Arc, PageToken)
}

// Adjacency represents a directed edge between two nodes.
type Adjacency struct {
	From, To NodeID
}

// Flow is a utilized graph Adjacency having a flow Rate.
type Flow struct {
	Adjacency
	Rate Rate

	fwdPrev, fwdNext flowID
	revPrev, revNext flowID
}

type node struct {
	height, nextHeight Height // Current, and upper-bound next Height of node.
	excess             Rate   // Amount of node's flow excess.

	fwdHead, fwdTail flowID // Head/tail of flows *from* this node.
	revHead, revTail flowID // Head/tail of flows *to* this node.

	dischargePage PageToken // Next PageToken for Arcs() of this node.
	dischargeInd  int       // Next Arc index with that []Arc page.
}

// MaxFlow represents a maximum flow achieved over a Network.
type MaxFlow struct {
	nodes        []node
	active       []NodeID // Nodes having non-zero excess, heaped on node height.
	flows        []Flow   // All network Flows. flowIDs are indexes into this slice.
	freeFlows    []flowID // Free-list of |flows| for re-use.
	heightCounts []int32  // For heights in [0, N), the number of nodes having that height.

	// For the current node being discharge()'d, dischargeIdx is a dense index
	// of a destination NodeID to the flowID which corresponds to the Flow from
	// the current node to the indexed NodeID. Most entries will be zero,
	// assuming a sparse utilization of available arcs. Each call to discharge()
	// sets up and clears this index, which allows O(1) resolution to Flows.
	dischargeIdx []flowID
}

// newMaxFlow returns a *MaxFlow initialized for the Network.
func newMaxFlow(network Network) *MaxFlow {
	var size = network.Nodes()

	var mf = &MaxFlow{
		nodes:        make([]node, size),
		active:       []NodeID{SourceID},
		flows:        []Flow{{}}, // Sentinel.
		freeFlows:    nil,
		heightCounts: make([]int32, size),
		dischargeIdx: make([]flowID, size),
	}

	for i := range mf.nodes {
		mf.nodes[i].nextHeight = maxHeight
	}
	mf.nodes[SourceID].excess = math.MaxInt32
	mf.nodes[SourceID].height = Height(size)

	mf.heightCounts[0] = 1 // Sink. Source height is out of bounds & not tracked.

	for id := SinkID + 1; id != NodeID(size); id++ {
		mf.nodes[id].height = network.InitialHeight(id)
		mf.heightCounts[mf.nodes[id].height] += 1
	}
	return mf
}

// FindMaxFlow solves for the maximum flow of the given Network using a sparse
// variant of the push/relabel algorithm.
func FindMaxFlow(network Network) *MaxFlow {
	var mf = newMaxFlow(network)
	for {
		if id, ok := mf.popActiveNode(); !ok {
			return mf // All done.
		} else {
			mf.discharge(id, network)
		}
	}
}

// RelativeHeight returns the node Height delta, relative to the source node.
// Depending on Network semantics, implementations may wish to use RelativeHeight
// to condition capacities of returned []Arcs, for example by increasing capacity
// if sufficient "pressure" has built up within the network.
func (mf *MaxFlow) RelativeHeight(nid NodeID) Height {
	return mf.nodes[nid].height - mf.nodes[SourceID].height
}

// Flows invokes the callback for each Flow of the given NodeID.
func (mf *MaxFlow) Flows(nodeID NodeID, cb func(Flow)) {
	for id := mf.nodes[nodeID].fwdHead; id != 0; id = mf.flows[id].fwdNext {
		cb(mf.flows[id])
	}
}

// discharge implements the push/relabel "node discharge" operation as it's
// traditionally understood, by seeking to push excess flow of the node
// along its arcs and residuals, relabeling the node as required, until no
// excess node flow remains.
func (mf *MaxFlow) discharge(nid NodeID, structure Network) {
	var node = &mf.nodes[nid]

	// Incrementally update |dischargeIdx| to provide O(1) lookups of NodeID to
	// corresponding flowIDs. We'll undo our modifications upon return,
	// restoring it to an initialized (zero) state. This is fast assuming our
	// linked-list of forward flows is typically small.
	for fid := node.fwdHead; fid != 0; fid = mf.flows[fid].fwdNext {
		mf.dischargeIdx[mf.flows[fid].To] = fid
	}
	defer func() {
		for fid := node.fwdHead; fid != 0; fid = mf.flows[fid].fwdNext {
			mf.dischargeIdx[mf.flows[fid].To] = 0
		}
	}()

	var (
		arcs     []Arc
		arcShift int
		nextPage PageToken
		fid      flowID
	)

	// Recover the page of |arcs| we stopped at on our last discharge. If we're
	// instead walking residuals, restart |fid| from the list tail.
	if node.dischargePage != PageEOF {
		if arcs, nextPage = structure.Arcs(mf, nid, node.dischargePage); len(arcs) != 0 {
			arcShift = int(nid) % len(arcs)
		}
	} else {
		fid = node.revTail
	}

	for {

		if node.dischargePage != PageEOF {
			if node.dischargeInd != len(arcs) {
				goto PushArc
			} else {
				goto NextPage
			}
		} else if fid != 0 {
			goto PushResidual
		} else {
			goto Relabel
		}

	NextPage:

		node.dischargePage, node.dischargeInd = nextPage, 0

		if node.dischargePage != PageEOF {
			if arcs, nextPage = structure.Arcs(mf, nid, node.dischargePage); len(arcs) != 0 {
				arcShift = int(nid) % len(arcs)
			}
		} else {
			fid = node.revTail // Walk backwards from the tail (LIFO order).
		}
		continue

	PushArc:
		{
			// Shift the current arc by |nid| % len(arcs) to deterministically
			// vary the Arc enumeration order of Nodes having common Arc sub-
			// structure. This speeds the algorithm by avoiding having all such
			// Nodes push along a single Arc during initial discharge.
			// We hoist the modulo itself because profiling shows this integer
			// division is otherwise a substantial part of discharge()'s runtime.
			var a = node.dischargeInd + arcShift
			if a >= len(arcs) {
				a -= len(arcs) // Loop back around to walk [0, arcShift).
			}
			// Map Adjacency to a current flowID, or zero if there is no active Flow.
			// Note that flowID 0 is a zero-valued sentinel Flow (ie Rate is 0).
			fid = mf.dischargeIdx[arcs[a].To]

			if mf.flows[fid].Rate >= arcs[a].Capacity || !mf.constrainHeight(node, arcs[a].To) {
				node.dischargeInd++ // Cannot push.
				continue
			}

			if fid == 0 {
				// Adjacency is not yet tracked, and should be.
				fid = mf.addFlow(Adjacency{From: nid, To: arcs[a].To}, arcs[a].PushFront)
				mf.dischargeIdx[arcs[a].To] = fid
			}

			var delta = arcs[a].Capacity - mf.flows[fid].Rate
			if delta > node.excess {
				delta = node.excess
			}
			// Shift |delta| flow from this node to |arc.To|.
			mf.flows[fid].Rate += delta
			mf.updateExcess(nid, -delta)
			mf.updateExcess(arcs[a].To, delta)

			if node.excess == 0 {
				return
			}
			// It's important we don't step |dischargeInd| until after the
			// above excess check. If new excess is later pushed to this node,
			// we want to consider remaining capacity of this Arc *first*.
			node.dischargeInd++
		}
		continue

	PushResidual:
		{
			// Walk residuals in reverse (LIFO) order, from list tail to head.
			var nextFlow = mf.flows[fid].revPrev

			if !mf.constrainHeight(node, mf.flows[fid].From) {
				fid = nextFlow // Cannot push.
				continue
			}

			var delta = mf.flows[fid].Rate
			if delta > node.excess {
				delta = node.excess
			}
			// Shift |delta| flow from this node back along the residual.
			mf.flows[fid].Rate -= delta
			mf.updateExcess(nid, -delta)
			mf.updateExcess(mf.flows[fid].From, delta)

			if mf.flows[fid].Rate == 0 {
				// Flow no longer needs to be tracked. |fid| is invalidated,
				// which is why we first retain |nextFlow|.
				mf.removeFlow(fid)
			}

			if node.excess == 0 {
				return
			}
			fid = nextFlow
		}
		continue

	Relabel:

		if nid == SourceID {
			return // All done (we never relabel the source node).
		}
		var nNodes = Height(len(mf.nodes))

		// We are re-labeling `node`, intending to remove its current labeling
		// of `node.height` and re-labeling it to `node.nextHeight`.
		//
		// Before we do so, check to see if this re-labeling will introduce a "gap"
		// in the set of heights (equivalently: labels) between 0 < G < len(nodes),
		// such that no nodes remain having height G.
		//
		// If there is a gap, then we immediately know that graph nodes having
		// G < node.height < len(nodes) are effectively "cut off" from the sink node.
		// Intuitively, height is a lower-bound on the distance to the sink,
		// and the node's path to the sink has been disconnected by the removal
		// of all nodes having the lower height G.
		//
		// We can skip a bunch of work by immediately re-labeling all such nodes
		// to a height of len(nodes) - 1. This includes the current `node`, as it's
		// in the process of being relabeled to a larger value from `node.height`.
		//
		// Note that conventional push/relabel would re-label to len(nodes) + 1.
		// We use len(nodes) - 1 instead, to explore relative-height heuristics
		// that may open up more capacity and are somewhat specific to the
		// network for solving assignments. If we instead used len(nodes) + 1,
		// then excess can push to the source immediately (terminating the
		// algorithm) before those heuristic capacity changes are fully explored.
		//
		// Additional intuition:
		//  * The sink always has height zero and is never relabeled (it never has excess),
		//    so there's always one node with height zero.
		//  * `node.nextHeight` is larger than `g`, so there's a least one larger height in use.
		//  * Thus, if no nodes remain having height `g` then `g` must be a gap.
		//    (We compare to one and not zero because we're about to decrement but haven't actually done so.)
		if g := node.height; 0 < g && g < nNodes-1 && node.nextHeight < nNodes-1 && mf.heightCounts[g] == 1 {
			// Are there *other* nodes that we can immediately relabel?
			// Test only nodes also having height `nextHeight`, which is not
			// exhaustive but is very fast and works quite well in practice.
			if mf.heightCounts[node.nextHeight] != 0 {
				for n := range mf.nodes {
					if h := mf.nodes[n].height; g < h && h < nNodes { // Does not match `node`.
						mf.heightCounts[h] -= 1
						mf.nodes[n].height = nNodes - 1
						mf.nodes[n].nextHeight = maxHeight
						mf.nodes[n].dischargePage = PageInitial
						mf.nodes[n].dischargeInd = 0
					}
				}
				heap.Init((*heightHeap)(mf)) // Re-heap as we may have changed active node heights.
			}

			node.nextHeight = nNodes - 1
		}

		if node.height < nNodes {
			mf.heightCounts[node.height] -= 1
		}
		if node.nextHeight < nNodes {
			mf.heightCounts[node.nextHeight] += 1
		}
		node.height, node.nextHeight = node.nextHeight, maxHeight

		// Reset to walk Arc pages.
		node.dischargePage, nextPage, arcs = PageInitial, PageInitial, nil
		continue
	}
}

// constrainHeight returns true if |node|'s height is greater than |to|'s, a
// constraint required by push/relabel in order for flow to be pushed from
// |node| to |to|.
//
// If the constraint is not met, constrainHeight returns false and also updates
// |node.nextHeight| to be lower-bound by |to|'s height+1, such that nextHeight
// will express the lowest possible height at which |node| can be relabeled in
// order to have constrainHeight() pass for at least one of the observed |to|'s.
func (mf *MaxFlow) constrainHeight(node *node, to NodeID) bool {
	if node.height <= mf.nodes[to].height {
		if node.nextHeight > mf.nodes[to].height+1 {
			node.nextHeight = mf.nodes[to].height + 1
		}
		return false
	}
	return true
}

// addFlow adds a new tracked zero-Rate Flow of the Adjacency. If |pushFront|,
// then the Flow is added to the head of respective linked-lists, otherwise to
// the tail.
func (mf *MaxFlow) addFlow(adj Adjacency, pushFront bool) flowID {
	var id flowID

	// Grab an available ID from the free-list, or append a new Flow.
	if l := len(mf.freeFlows); l != 0 {
		id = mf.freeFlows[l-1]
		mf.freeFlows = mf.freeFlows[:l-1]
	} else {
		id = flowID(len(mf.flows))
		mf.flows = append(mf.flows, Flow{})
	}

	var flow = Flow{Adjacency: adj}
	var from, to = &mf.nodes[adj.From], &mf.nodes[adj.To]

	if pushFront {
		if from.fwdHead == 0 {
			from.fwdHead, from.fwdTail = id, id
		} else {
			mf.flows[from.fwdHead].fwdPrev = id
			from.fwdHead, flow.fwdNext = id, from.fwdHead
		}

		if to.revHead == 0 {
			to.revHead, to.revTail = id, id
		} else {
			mf.flows[to.revHead].revPrev = id
			to.revHead, flow.revNext = id, to.revHead
		}
	} else {
		if from.fwdTail == 0 {
			from.fwdHead, from.fwdTail = id, id
		} else {
			mf.flows[from.fwdTail].fwdNext = id
			flow.fwdPrev, from.fwdTail = from.fwdTail, id
		}

		if to.revTail == 0 {
			to.revHead, to.revTail = id, id
		} else {
			mf.flows[to.revTail].revNext = id
			flow.revPrev, to.revTail = to.revTail, id
		}
	}

	mf.flows[id] = flow
	return id
}

// removeFlow removes the Flow having flowID.
func (mf *MaxFlow) removeFlow(id flowID) {
	var flow = mf.flows[id]

	if flow.fwdPrev == 0 {
		mf.nodes[flow.From].fwdHead = flow.fwdNext
	} else {
		mf.flows[flow.fwdPrev].fwdNext = flow.fwdNext
	}

	if flow.fwdNext == 0 {
		mf.nodes[flow.From].fwdTail = flow.fwdPrev
	} else {
		mf.flows[flow.fwdNext].fwdPrev = flow.fwdPrev
	}

	if flow.revPrev == 0 {
		mf.nodes[flow.To].revHead = flow.revNext
	} else {
		mf.flows[flow.revPrev].revNext = flow.revNext
	}

	if flow.revNext == 0 {
		mf.nodes[flow.To].revTail = flow.revPrev
	} else {
		mf.flows[flow.revNext].revPrev = flow.revPrev
	}

	mf.freeFlows = append(mf.freeFlows, id)
	mf.flows[id] = Flow{}
}

// updateExcess of Node |id| with a positive or negative |delta|. If the node
// excess was previously zero, it's marked as active for a future discharge.
func (mf *MaxFlow) updateExcess(id NodeID, delta Rate) {
	if mf.nodes[id].excess == 0 && id != SinkID {
		heap.Push((*heightHeap)(mf), id)
	}
	mf.nodes[id].excess += delta
}

// popActiveNode returns the next node having excess flow in need of discharge.
func (mf *MaxFlow) popActiveNode() (NodeID, bool) {
	if len(mf.active) == 0 {
		return 0, false
	} else {
		return heap.Pop((*heightHeap)(mf)).(NodeID), true
	}
}

// heightHeap orders MaxFlow nodes on descending height.
type heightHeap MaxFlow

func (h *heightHeap) Len() int { return len(h.active) }
func (h *heightHeap) Less(i, j int) bool {
	return h.nodes[h.active[i]].height > h.nodes[h.active[j]].height
}
func (h *heightHeap) Swap(i, j int) {
	h.active[i], h.active[j] = h.active[j], h.active[i]
}
func (h *heightHeap) Push(x interface{}) {
	h.active = append(h.active, x.(NodeID))
}
func (h *heightHeap) Pop() interface{} {
	var old, l = h.active, len(h.active)
	var x = old[l-1]
	h.active = old[0 : l-1]
	return x
}
