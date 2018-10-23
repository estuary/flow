package allocator

import (
	"sort"
	"strings"

	pr "github.com/LiveRamp/gazette/v2/pkg/allocator/push_relabel"
	"github.com/LiveRamp/gazette/v2/pkg/keyspace"
	log "github.com/sirupsen/logrus"
)

// flowNetwork models an allocation state as a flow network, representing Items,
// "Zone Items" (which is an Item within the context of a single zone), and
// Members. Desired replication of Items, zone balancing constraints, Member
// Item limits, and a previous solution are encoded via network Arcs, their
// capacities, and priorities. Eg, zone constraints may be represented
// through intermediate "Zone Items" nodes, and a capacity from each Item which
// is one less than the desired replication (eg, not all Assignments may be in
// a single zone). Current Assignments are captured as priorities on Arcs within
// the constructed network. This modeling allows the incremental allocation
// problem to be reduced to that of obtaining a maximum flow over the resulting
// prioritized network. Pictorially, the network resembles:
//
//               Items        Zone-Items         Members
//               -----        ----------         -------
//
//                            +-------+
//                            |       |---\    +---------+
//                           >|item1/A|    --->|         |
//                         -/ |       |\       |A/memberX|\
//                        /   +-------+ -\    ^|         | -\
//              +-----+ -/    +-------+   -\ / +---------+   \
//              |     |/      |       |     /\ +---------+    \
//             >|item1|------>|item1/B|    /  >|         |     -\
// +------+  -/ |     |       |       |\  /    |A/memberY|--\    \ +--------+
// |      |-/   +-----+       +-------+ \/    >|         |   ---\ >|        |
// |source|                             /\  -/ +---------+       ->| target |
// |      |-\   +-----+       +-------+/  \/                      >|        |
// +------+  -\ |     |       |       |  -/\                    -/ +--------+
//             >|item2|------>|item2/A|-/   \                  /
//              |     |\      |       |      \ +---------+   -/
//              +-----+ -\    +-------+       v|         | -/
//                        \   +-------+        |B/memberZ|/
//                         -\ |       |    --->|         |
//                           >|item2/B|---/    +---------+
//                            |       |
//                            +-------+
//
type flowNetwork struct {
	source    pr.Node
	members   []pr.Node
	items     []pr.Node
	zoneItems []pr.Node
	sink      pr.Node
}

func (fn *flowNetwork) init(s *State) {
	// Size Nodes and set labeled height. Push/Relabel initializes all Node labels
	// to their distance from the Sink node, with the exception of the Source, which
	// is initialized to the total number of vertices.
	fn.items = pr.InitNodes(fn.items, len(s.Items), 3)
	fn.zoneItems = pr.InitNodes(fn.zoneItems, len(s.Items)*len(s.Zones), 2)
	fn.members = pr.InitNodes(fn.members, len(s.Members), 1)
	fn.sink = pr.Node{Arcs: fn.sink.Arcs[:0], Height: 0}
	fn.source = pr.Node{
		Arcs:   fn.source.Arcs[:0],
		Height: uint32(len(fn.items) + len(fn.zoneItems) + len(fn.members) + 2),
	}

	var (
		// We cannot hope to allocate more Item slots than there are Member slots.
		// Also, performance and stability of the prioritized push/relabel solver
		// degrade with the degree of excess flow which cannot be assigned. Make
		// the solution faster and more stable by bounding the effective Item
		// slots to the number of Member slots.
		effectiveSlots int
		// We generally want to place every Item in every available failure Zone.
		// However, we're unable to do this if a given Zone doesn't have capacity
		// to place each Item, in which case we do not require that it do so (and
		// instead just log a warning).
		effectiveZones int
	)

	for zone, zs := range s.ZoneSlots {
		effectiveSlots += zs

		// A zone is "effective" if it can hold all Items, plus one to
		// ensure that Assignments can be rotated without deadlock.
		if zs > len(s.Items) {
			effectiveZones++
		} else {
			log.WithFields(log.Fields{
				"zone":  s.Zones[zone],
				"slots": zs,
				"items": len(s.Items),
			}).Warn("failure zone has insufficient capacity to place every item (add more zone members?)")
		}
	}

	if effectiveSlots <= s.ItemSlots {
		log.WithFields(log.Fields{
			"memberSlots": effectiveSlots,
			"items":       len(s.Items),
			"itemSlots":   s.ItemSlots,
		}).Warn("insufficient total member capacity to reach desired item replication (add more members?)")
	}

	// Perform a Left-join of |Items| with |Assignments| (ordered on item ID, member zone, member suffix).
	// Build arcs from Source to each Item, to ZoneItems, to Members, and finally to the Sink.
	var it = LeftJoin{
		LenL: len(s.Items),
		LenR: len(s.Assignments),
		Compare: func(l, r int) int {
			return strings.Compare(itemAt(s.Items, l).ID, assignmentAt(s.Assignments, r).ItemID)
		},
	}
	for cur, ok := it.Next(); ok; cur, ok = it.Next() {
		var item = cur.Left
		var itemAssignments = s.Assignments[cur.RightBegin:cur.RightEnd]
		var itemSlots = itemAt(s.Items, item).DesiredReplication()

		switch {
		case itemSlots < 0:
			itemSlots = 0
		case itemSlots > effectiveSlots:
			itemSlots = effectiveSlots
		}
		effectiveSlots -= itemSlots

		buildItemArcs(s, fn, item, itemAssignments, itemSlots, effectiveZones)
	}

	// Determine scaling factors for each zone.
	var zsfNum, zsfDenom = zoneScalingFactors(len(s.Items), s.ItemSlots, s.ZoneSlots)

	// Perform a left-join of |Members| with |Zones|. Add Arcs from each Member to sink.
	it = LeftJoin{
		LenL: len(s.Members),
		LenR: len(s.Zones),
		Compare: func(l, r int) int {
			return strings.Compare(memberAt(s.Members, l).Zone, s.Zones[r])
		},
	}
	for cur, ok := it.Next(); ok; cur, ok = it.Next() {
		var member = cur.Left
		var zone = cur.RightBegin

		// Calculate scaled member capacity using integer division, rounded up.
		var limit = memberAt(s.Members, member).ItemLimit() * zsfNum[zone]
		if limit == 0 {
			// Pass.
		} else if limit%zsfDenom[zone] == 0 {
			limit = limit / zsfDenom[zone]
		} else {
			limit = (limit / zsfDenom[zone]) + 1
		}
		// Arc from Member to Sink, with capacity of the adjusted Member ItemLimit.
		// Previous flow is the number of current Assignments.
		addArc(&fn.members[member], &fn.sink, limit, s.MemberTotalCount[member])
	}

	// Sort all Node Arcs by priority.
	pr.SortNodeArcs(fn.source)
	pr.SortNodeArcs(fn.items...)
	pr.SortNodeArcs(fn.zoneItems...)
	pr.SortNodeArcs(fn.members...)
	pr.SortNodeArcs(fn.sink)
}

// zoneScalingFactor computes a scaling factor (0, 1] which is applied to Member
// ItemLimits of a given Zone. Where there are more Member slots than Item slots,
// this balances the smaller set of Items evenly across Zones and their Members,
// rather than having some Members near or fully allocated while others are idle
// (which is an otherwise valid max-flow).
//
// The scaling factor is determined in two parts. First, we allocate a "fixed"
// number of slots for each Zone, which is the smaller of either:
//
//   * The number of Items, or
//   * The total capacity of the Zone.
//
// In other words, we never scale a Zone's capacity any lower than would allow
// us to place every Item in the Zone at least once. We arrive at a fixed
// scaling ratio which is sufficient for the Zone's fixed slots:
//
//                 min(len(Items), zoneSlots)
//   fixedRatio = ----------------------------
//                        zoneSlots
//
// Let |fixedSlots| be the sum total slots which have been allocated in this
// fashion across all Zones. Now define a "dynamic" ratio which is this Zone's
// proportionate share of remaining non-fixed Item slots:
//
//                   max(0, ItemSlots - fixedSlots)
//   dynamicRatio = ---------------------------------
//                   max(1, MemberSlots - fixedSlots)
//
// Intuitively, consider the ratio where fixedSlots = 0: it is the scaling factor
// which, when applied to MemberSlots, would adjust it to exactly match ItemSlots.
// Where fixedSlots > 0, the ratio adjusts the non-fixed remainder of MemberSlots
// to exactly match the non-fixed remainder of ItemSlots. Compose both ratios
// for the final scaling ratio:
//
//   zoneRatio = fixedRatio + (1 - fixedRatio) * dynamicRatio
//
// zoneScalingFactors expands this expression to return the ratio as
// separate integer numerator & denominator components.
func zoneScalingFactors(itemCount, itemSlots int, zoneSlots []int) (num, denom []int) {
	num, denom = make([]int, len(zoneSlots)), make([]int, len(zoneSlots))
	var (
		memberSlots int
		fixedSlots  int
	)
	for _, zs := range zoneSlots {
		memberSlots += zs
		fixedSlots += min(itemCount, zs)
	}
	var (
		itemRemainder   = max(0, min(itemSlots, memberSlots)-fixedSlots)
		memberRemainder = max(1, memberSlots-fixedSlots)
	)
	for zone, zs := range zoneSlots {
		num[zone] = (memberRemainder-itemRemainder)*min(itemCount, zs) + itemRemainder*zs
		denom[zone] = zs * memberRemainder
	}
	return
}

func buildItemArcs(s *State, fn *flowNetwork, item int, itemAssignments keyspace.KeyValues, itemSlots, effectiveZones int) {
	// Item capacity is defined by its replication factor. Within a zone (and
	// assuming there are multiple Zones), capacity is the replication factor
	// minus one (eg, requiring that replicas be split across at least two
	// Zones), lower-bounded to one.
	var zoneSlots = itemSlots
	if zoneSlots > 1 && effectiveZones > 1 {
		zoneSlots--
	}

	// Arc from Source to Item, with capacity of the total desired item replication.
	// Previous flow is the number of current Assignments.
	addArc(&fn.source, &fn.items[item], itemSlots, len(itemAssignments))

	// Perform a Left-join of all Zones with |itemAssignments| (also ordered on zone).
	var zit = LeftJoin{
		LenL: len(s.Zones),
		LenR: len(itemAssignments),
		Compare: func(l, r int) int {
			return strings.Compare(s.Zones[l], assignmentAt(itemAssignments, r).MemberZone)
		},
	}
	// Within the join, we'll be performing nested Left-joins of |Members| with
	// Assignments in the current zone. Take advantage of the fact that |Members|
	// is also ordered on zone and begin each iteration where the last Left off,
	// so that total time is O(len(fn.Zones) + len(Members)).
	var moff int

	for zcur, ok := zit.Next(); ok; zcur, ok = zit.Next() {
		var zone = zcur.Left
		var zoneItem = item*len(s.Zones) + zone
		var zoneAssignments = itemAssignments[zcur.RightBegin:zcur.RightEnd]

		// Arc from Item to ZoneItem, with capacity of |zoneSlots|, and previous flow being
		// the total number of current Assignments to Members in this zone.
		addArc(&fn.items[item], &fn.zoneItems[zoneItem], zoneSlots, len(zoneAssignments))

		// Perform a Left-join of |Members| with |zoneAssignments| (also ordered on member suffix).
		var mit = LeftJoin{
			LenL: len(s.Members) - moff,
			LenR: len(zoneAssignments),
			Compare: func(l, r int) int {
				return strings.Compare(memberAt(s.Members, l+moff).Suffix, assignmentAt(zoneAssignments, r).MemberSuffix)
			},
		}
		for mcur, ok := mit.Next(); ok; mcur, ok = mit.Next() {
			var member = mcur.Left + moff

			switch c := strings.Compare(s.Zones[zone], memberAt(s.Members, member).Zone); c {
			case -1:
				// We've ranged through all Members having this zone. Set |moff|
				// so the next iteration begins with the next zone.
				moff = member
				mit = LeftJoin{} // Zero such that next iteration terminates.
				continue
			case 1:
				// |Zones| must include all Zones appearing in |Members|,
				// and |Members| is ordered on Member (Zone, Suffix).
				panic("invalid member / zone order")
			}

			// Arc from ZoneItem to Member, with capacity of 1 and a previous flow being
			// the number of current Assignments to this member (which can be zero or one).
			addArc(&fn.zoneItems[zoneItem], &fn.members[member], 1, mcur.RightEnd-mcur.RightBegin)
		}
	}
}

func addArc(from, to *pr.Node, capacity, prevFlow int) {
	var priority int
	if prevFlow >= capacity {
		// Arc was previously saturated. Try to saturate it again.
		priority = 2
	} else if prevFlow > 0 {
		// Prefer to send this Arc flow, over other Arcs which had no flow.
		priority = 1
	}

	pr.AddArc(from, to, capacity, priority)
}

func extractItemFlow(s *State, fn *flowNetwork, item int, out []Assignment) []Assignment {
	var start = len(out) // First offset of extracted Assignments.

	// Walk Arcs of each of the Item's ZoneItems, to collect
	// the |desired| Assignment state for this Item.
	for zone := range s.Zones {
		var zoneItem = item*len(s.Zones) + zone

		for _, a := range fn.zoneItems[zoneItem].Arcs {
			if a.Flow <= 0 {
				continue
			}
			var member = memberAt(s.Members, int(a.Target.ID))

			out = append(out, Assignment{
				ItemID:       itemAt(s.Items, item).ID,
				MemberZone:   member.Zone,
				MemberSuffix: member.Suffix,
			})
		}
	}
	// Sort the portion just added to |out| under natural Assignment order.
	sort.Slice(out[start:], func(i, j int) bool {
		return compareAssignment(out[i+start], out[j+start]) < 0
	})
	return out
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
