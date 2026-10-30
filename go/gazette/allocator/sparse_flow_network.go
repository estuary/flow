package allocator

import (
	"sort"
	"strings"

	pr "go.gazette.dev/core/allocator/sparse_push_relabel"
	"go.gazette.dev/core/keyspace"
)

// sparseFlowNetwork models an allocator.State as a flow network, representing
// Items, "Zone Items" (which is an Item within the context of a single zone),
// and Members. Pictorially, the network resembles:
//
//	              Items        Zone-Items         Members
//	              -----        ----------         -------
//
//	                           +-------+
//	                           |       |---\    +---------+
//	                          >|item1/A|    --->|         |
//	                        -/ |       |\       |A/memberX|\
//	                       /   +-------+ -\    ^|         | -\
//	             +-----+ -/    +-------+   -\ / +---------+   \
//	             |     |/      |       |     /\ +---------+    \
//	            >|item1|------>|item1/B|    /  >|         |     -\
//	+------+  -/ |     |       |       |\  /    |A/memberY|--\    \ +--------+
//	|      |-/   +-----+       +-------+ \/    >|         |   ---\ >|        |
//	|source|                             /\  -/ +---------+       ->| target |
//	|      |-\   +-----+       +-------+/  \/                      >|        |
//	+------+  -\ |     |       |       |  -/\                    -/ +--------+
//	            >|item2|------>|item2/A|-/   \                  /
//	             |     |\      |       |      \ +---------+   -/
//	             +-----+ -\    +-------+       v|         | -/
//	                       \   +-------+        |B/memberZ|/
//	                        -\ |       |    --->|         |
//	                          >|item2/B|---/    +---------+
//	                           |       |
//	                           +-------+
//
// The network also represents a number of constraints and goals:
//
//   - Desired Item replication is captured by the capacity from source to Item.
//   - Zone replication constraints (distribution of each Item across 2+ zones) are
//     captured in arcs from Items to Zone Items. Goals for maintaining current
//     assignments and balancing evenly across zones are also expressed.
//   - A preference for current assignments is reflected in arcs from Zone Items
//     to Members.
//   - Desired "fair share" scaled capacity and upper-bound capacity is reflected
//     by arcs from Members to the Sink.
//
// The basic strategy used is to greedily coerce the push/relabel solver towards
// a solution which achieves optimization goals (minimal updates & even balancing)
// while still staying within the algorithmic scaffolding of push/relabel,
// ensuring that a correct maximum-flow solution is ultimately arrived at.
//
// This coercing is expressed through the order in which Arcs are presented to the
// solver, and also by strategically constraining the capacities of those Arcs.
// As the solver finds it is unable to follow the "garden path" (not all network
// excess can be allocated), and as back-tracking builds "pressure" in the network
// (expressed via node heights), the relevant Arc capacities at each step are
// relaxed until a maximal assignment is achieved.
type sparseFlowNetwork struct {
	*State
	myItems     keyspace.KeyValues // Slice of State.Items included in this network.
	myItemSlots int                // Summed of replication slots attributable just to myItems.

	firstItemNodeID     pr.NodeID // First Item NodeID in the graph.
	firstZoneItemNodeID pr.NodeID // First Zone-Item NodeID in the graph.
	firstMemberNodeID   pr.NodeID // First Member NodeID in the graph.

	// For each zone-item, the offset into State.Assignments of its first assignment.
	zoneItemAssignments []keyspace.KeyValues
	// For each zone, an index of member ID suffix to its NodeID.
	memberSuffixIdxByZone []map[string]pr.NodeID
	// For each zone, a slice of Arcs to all members of that zone.
	allZoneItemArcsByZone [][]pr.Arc

	// scratch is a small slice of Arcs for (re)use without allocating. We'll
	// want up-to the number of zones, or the number of Assignments of an Item
	// within a zone -- both should be small, but if we overflow that's fine,
	// as append() will just allocate from heap instead.
	scratch [8]pr.Arc
}

const (
	// By network construction, the push/relabel algorithm will relabel an Item
	// node to a RelativeHeight() of 1 (relative to the source node) just prior
	// to pushing back to the source, which terminates the algorithm.
	//
	// Where we ordinarily require an Item to be replicated across two zones, if
	// we reach this height, we'll instead allow a single zone to hold all Item
	// replicas (as the alternative is not fully replicating the Item at all).
	itemOverflowThreshold = 1
	// Also by network construction, if a Member node is allowed to reach a
	// RelativeHeight() of 1, then we may also cause `itemOverflowThreshold` to
	// be breached. Eg:
	// - Member height S+1 pushes along residual to ZoneItem height S.
	// - ZoneItem height S pushes to Item having height S-1.
	// - Item is relabeled to S+1.
	//
	// Ideally we evenly balance Items across Members, but we'd rather a Member
	// take on more than its fair share of Items than cause an Item to not be
	// replicated across at least two zones. In network terms, we'd rather a
	// Member overflow before we allow an Item to overflow, and we thus never
	// want a Member to reach RelativeHeight of 1 if capacity remains.
	//
	// *However*, note that a relabeling may take us from height S-1 => S+1,
	// skipping S, so we must bound to RelativeHeight() of -1 to ensure this
	// never happens.
	//
	// Note that our push/relabel solver implements the label gap heuristic,
	// which instantly re-sets a subset of nodes to a new height. Usually
	// that height is len(nodes) + 1, but we use len(nodes) - 1 to give the
	// solver time to fully explore these overflow heuristics.
	memberOverflowThreshold = -1

	pageItemArcsUniform    = pr.PageInitial + 1
	pageItemArcsRMinusOne  = pageItemArcsUniform + 1
	pageZoneItemAllMembers = pageItemArcsRMinusOne + 1
)

// newSparseFlowNetwork builds a *sparseFlowNetwork around the given State
// and partitioned sub-slice of State.Items.
func newSparseFlowNetwork(s *State, myItems keyspace.KeyValues) *sparseFlowNetwork {

	// Nodes are ordered as:
	//  - Source node, then
	//  - Sink node, then
	//  - Item Nodes, then
	//  - Zone-Item Nodes, then
	//  - Member Nodes.
	var firstItemNodeID = pr.SinkID + 1 // == 2.
	var firstZoneItemNodeID = firstItemNodeID + pr.NodeID(len(myItems))
	var firstMemberNodeID = firstZoneItemNodeID + pr.NodeID(len(myItems)*len(s.Zones))

	// Left-join |Items| with |Assignments| (which is ordered on Item ID,
	// Member zone, Member suffix) to build an index of zone-item to the
	// offset of its first Assignment (or, the offset to where its Assignment
	// would place if it had one).
	var zoneItemAssignments = make([]keyspace.KeyValues, len(myItems)*len(s.Zones))

	// Accelerate our left-join by skipping to the first assignment of `myItems` via binary search.
	var pivot, _ = s.Assignments.Search(ItemAssignmentsPrefix(s.KS, itemAt(myItems, 0).ID))
	var myAssignments = s.Assignments[pivot:]
	var myItemSlots int

	var it = LeftJoin{
		LenL: len(myItems),
		LenR: len(myAssignments),
		Compare: func(l, r int) int {
			return strings.Compare(itemAt(myItems, l).ID, assignmentAt(myAssignments, r).ItemID)
		},
	}
	for cur, ok := it.Next(); ok; cur, ok = it.Next() {
		var item = cur.Left
		var assignments = myAssignments[cur.RightBegin:cur.RightEnd]
		myItemSlots += myItems[item].Decoded.(Item).DesiredReplication()

		// Left-join zones with |assignments| of this |item|.
		var it2 = LeftJoin{
			LenL: len(s.Zones),
			LenR: len(assignments),
			Compare: func(l, r int) int {
				return strings.Compare(s.Zones[l], assignmentAt(assignments, r).MemberZone)
			},
		}
		for cur2, ok2 := it2.Next(); ok2; cur2, ok2 = it2.Next() {
			var zoneItem = item*len(s.Zones) + cur2.Left
			zoneItemAssignments[zoneItem] = assignments[cur2.RightBegin:cur2.RightEnd]
		}
	}

	var memberSuffixIdxByZone = make([]map[string]pr.NodeID, len(s.Zones))
	var allZoneItemArcsByZone = make([][]pr.Arc, len(s.Zones))

	// Left-join |Zones| with |Members| (which is ordered on Member zone, Member suffix).
	it = LeftJoin{
		LenL: len(s.Zones),
		LenR: len(s.Members),
		Compare: func(l, r int) int {
			return strings.Compare(s.Zones[l], memberAt(s.Members, r).Zone)
		},
	}
	for cur, ok := it.Next(); ok; cur, ok = it.Next() {
		var zone = cur.Left

		memberSuffixIdxByZone[zone] = make(map[string]pr.NodeID)
		allZoneItemArcsByZone[zone] = make([]pr.Arc, 0, cur.RightEnd-cur.RightBegin)

		for m := cur.RightBegin; m != cur.RightEnd; m++ {
			var id = firstMemberNodeID + pr.NodeID(m)
			memberSuffixIdxByZone[zone][memberAt(s.Members, m).Suffix] = id

			allZoneItemArcsByZone[zone] = append(allZoneItemArcsByZone[zone],
				pr.Arc{To: id, Capacity: 1})
		}
	}

	var fs = &sparseFlowNetwork{
		State:                 s,
		myItems:               myItems,
		myItemSlots:           myItemSlots,
		firstItemNodeID:       firstItemNodeID,
		firstZoneItemNodeID:   firstZoneItemNodeID,
		firstMemberNodeID:     firstMemberNodeID,
		zoneItemAssignments:   zoneItemAssignments,
		memberSuffixIdxByZone: memberSuffixIdxByZone,
		allZoneItemArcsByZone: allZoneItemArcsByZone,
	}
	return fs
}

func (fs *sparseFlowNetwork) Nodes() int {
	return int(fs.firstMemberNodeID) + len(fs.Members)
}

func (fs *sparseFlowNetwork) InitialHeight(id pr.NodeID) pr.Height {
	if id < fs.firstZoneItemNodeID {
		return 3 // Item node.
	} else if id < fs.firstMemberNodeID {
		return 2 // Zone-Item node.
	} else {
		return 1 // Member node.
	}
}

func (fs *sparseFlowNetwork) Arcs(mf *pr.MaxFlow, id pr.NodeID, page pr.PageToken) ([]pr.Arc, pr.PageToken) {
	if id == pr.SourceID {
		return fs.buildSourceArcs(), pr.PageEOF // Arcs from the Source to each Item.

	} else if id == pr.SinkID {
		// push/relabel never discharges from the sink, so we never expect to
		// see a corresponding Arcs() call.
		panic("unexpected Arcs call with id == pr.SinkID")

	} else if id < fs.firstZoneItemNodeID {
		var item = int(id - fs.firstItemNodeID)
		var r = itemAt(fs.myItems, item).DesiredReplication()

		// Enumerate Arcs from the Item to each of its Zone-Item Nodes.
		// If there is only one zone, or we will next push back to the Source,
		// then do not constrain the capacity of each Zone-Item Arc
		// (all of the Item's flow may go to any Zone-Item).
		if len(fs.Zones) == 1 || mf.RelativeHeight(id) == itemOverflowThreshold {
			return fs.buildAllItemArcs(item, r), pr.PageEOF
		}

		// Otherwise, enumerate arcs which prefer:
		// - To maintain current Item / zone Assignments, then
		// - Uniformly balancing the Item across zones, then
		// - Any zone, so long as at least two zones are utilized.
		switch page {
		case pr.PageInitial:
			return fs.buildCurrentItemArcs(item, max(r-1, 1)), pageItemArcsUniform
		case pageItemArcsUniform:
			var uniform = scaleAndRound(r, 1, len(fs.Zones))
			return fs.buildAllItemArcs(item, uniform), pageItemArcsRMinusOne
		case pageItemArcsRMinusOne:
			return fs.buildAllItemArcs(item, max(r-1, 1)), pr.PageEOF
		default:
			panic("invalid PageToken")
		}

	} else if id < fs.firstMemberNodeID {
		var zoneItem = int(id - fs.firstZoneItemNodeID)

		// Cycle through two pages of arcs:
		// - Arcs which reflect current Assignments of the Zone-Item to zone Members.
		// - Arcs which represent the total set of zone Members.
		// Intuitively: we prefer to keep current Member Assignments, but will allow
		// a new assignment to any of the zone's Members.
		switch page {
		case pr.PageInitial:
			return fs.buildCurrentZoneItemArcs(zoneItem), pageZoneItemAllMembers
		case pageZoneItemAllMembers:
			return fs.allZoneItemArcsByZone[zoneItem%len(fs.Zones)], pr.PageEOF
		default:
			panic("invalid PageToken")
		}
	} else {
		var member = int(id - fs.firstMemberNodeID)
		return fs.buildMemberArc(mf, id, member), pr.PageEOF
	}
}

// buildSourceArcs enumerates an Arc for each Item node, nominally having capacity
// of the Item's desired replication. If the total number of Item slots greatly
// exceeds Member slots, this degrades the performance and stability of the push/
// relabel solver; we therefore globally bound Item capacities to the number of
// available Member slots.
func (fs *sparseFlowNetwork) buildSourceArcs() []pr.Arc {
	var arcs = make([]pr.Arc, len(fs.myItems))
	var remaining = fs.MemberSlots

	for item := range fs.myItems {
		var c = itemAt(fs.myItems, item).DesiredReplication()

		if c > remaining {
			c = remaining
		}
		remaining -= c

		arcs[item] = pr.Arc{
			To:       fs.firstItemNodeID + pr.NodeID(item),
			Capacity: pr.Rate(c),
		}
	}
	return arcs
}

// buildAllItemArcs enumerates an Arc for each ZoneItem node of the Item,
// each having capacity C.
func (fs *sparseFlowNetwork) buildAllItemArcs(item int, C int) []pr.Arc {
	var (
		arcs = fs.scratch[:0]
		lz   = len(fs.Zones)
	)

	for zone := 0; zone != lz; zone++ {
		arcs = append(arcs, pr.Arc{
			To:       fs.firstZoneItemNodeID + pr.NodeID(item*lz+zone),
			Capacity: pr.Rate(C),
		})
	}
	return arcs
}

// buildCurrentItemArcs enumerates an Arc for each ZoneItem of the Item
// having current Assignments. Arc capacities are the smaller of |bound|
// and the number of current Assignments.
func (fs *sparseFlowNetwork) buildCurrentItemArcs(item int, bound int) []pr.Arc {
	var (
		arcs = fs.scratch[:0]
		lz   = len(fs.Zones)
	)
	for zone := 0; zone != lz; zone++ {
		var n = len(fs.zoneItemAssignments[item*lz+zone])
		if n > bound {
			n = bound
		}
		if n != 0 {
			arcs = append(arcs, pr.Arc{
				To:        fs.firstZoneItemNodeID + pr.NodeID(item*lz+zone),
				Capacity:  pr.Rate(n),
				PushFront: true,
			})
		}
	}
	return arcs
}

// buildMemberArc from member `member` to the sink.
func (fs *sparseFlowNetwork) buildMemberArc(mf *pr.MaxFlow, id pr.NodeID, member int) []pr.Arc {
	var c = fs.memberEffectiveLimit(member)

	// Scale ItemLimit by the relative share of ItemSlots within
	// our subset of the global assignment problem.
	c = scaleAndRound(c, fs.myItemSlots, fs.ItemSlots)

	if mf.RelativeHeight(id) < memberOverflowThreshold {
		// Further scale to our relative "fair share" items.
		// Intuitively, the Member node will resist having more than its fair share of
		// assignments until sufficient pressure builds within the network to indicate
		// that not all assignments can otherwise be made, at which point we'll
		// allow assignments up to our (scaled) full capacity.
		c = scaleAndRound(c, fs.ItemSlots, fs.MemberSlots)
	}
	fs.scratch[0] = pr.Arc{
		To:       pr.SinkID,
		Capacity: pr.Rate(c),
	}
	return fs.scratch[:1]
}

// buildCurrentZoneItemArcs from zone-item |zoneItem| to each Member node of the
// zone having a current assignment.
func (fs *sparseFlowNetwork) buildCurrentZoneItemArcs(zoneItem int) []pr.Arc {
	var (
		arcs = fs.scratch[:0]
		zone = zoneItem % len(fs.Zones)
	)
	for _, a := range fs.zoneItemAssignments[zoneItem] {
		if id, ok := fs.memberSuffixIdxByZone[zone][a.Decoded.(Assignment).MemberSuffix]; ok {
			arcs = append(arcs, pr.Arc{
				To:        id,
				Capacity:  1,
				PushFront: true,
			})
		}
	}
	return arcs
}

// extractAssignments appends and returns the set of ordered []Assignment
// implied by the MaxFlow solution.
func (fs *sparseFlowNetwork) extractAssignments(g *pr.MaxFlow, out []Assignment) []Assignment {
	var lz = len(fs.Zones)

	for item := range fs.myItems {
		var itemID = itemAt(fs.myItems, item).ID
		var sortFrom = len(out)

		for zone := 0; zone != lz; zone++ {
			var nodeID = fs.firstZoneItemNodeID + pr.NodeID(item*lz+zone)

			g.Flows(nodeID, func(flow pr.Flow) {
				var member = memberAt(fs.Members, int(flow.To-fs.firstMemberNodeID))

				out = append(out, Assignment{
					ItemID:       itemID,
					MemberZone:   member.Zone,
					MemberSuffix: member.Suffix,
				})
			})
		}
		// Sort the portion just added to |out| under natural Assignment order.
		sort.Slice(out[sortFrom:], func(i, j int) bool {
			return compareAssignment(out[i+sortFrom], out[j+sortFrom]) < 0
		})
	}
	return out
}

// scaleAndRound returns |c * min(num / denom, 1)|, using integer math and rounding up.
func scaleAndRound(c, num, denom int) int {
	if num > denom {
		return c
	}
	if c *= num; c%denom != 0 {
		c += denom
	}
	return c / denom
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
