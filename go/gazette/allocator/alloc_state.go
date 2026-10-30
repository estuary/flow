package allocator

import (
	"cmp"
	"hash/crc64"
	"math"
	"slices"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/keyspace"
)

// IsConsistentFn is a free function which determines whether the Item is to
// be considered "consistent" given its current AssignmentValue and the set of
// all AssignmentValues of the Item.
//
// The meaning of "consistent" is up to the application: generally it means that
// assigned replicas of the Item have synchronized with each other and can
// tolerate the removal of one of their cohort. If an Item is currently
// inconsistent, the allocator will not remove a current Assignment of the Item
// and instead waits for replicas to perform synchronization activities,
// communicated through Etcd, such that IsConsistentFn once again returns true.
type IsConsistentFn func(
	item Item,
	itemAssignment keyspace.KeyValue,
	allAssignmentsOfItem keyspace.KeyValues) bool

// State is an extracted representation of the allocator KeySpace. Clients may
// want to inspect State as part of a KeySpace observer to identify changes to
// local assignments or the overall allocation topology.
type State struct {
	KS           *keyspace.KeySpace
	LocalKey     string         // Unique key of this allocator instance.
	IsConsistent IsConsistentFn // Consistency callback for this allocator.

	// Sub-slices of the KeySpace representing allocator entities.
	Members     keyspace.KeyValues
	Items       keyspace.KeyValues
	Assignments keyspace.KeyValues

	LocalMemberInd int         // Index of |LocalKey| within |Members|, or -1 if not found.
	LocalItems     []LocalItem // Assignments of this instance.

	Zones       []string // Sorted and unique Zones of |Members|.
	ZoneSlots   []int    // Total number of item slots summed across all |Members| of each Zone.
	ItemSlots   int      // Total desired replication slots summed across all |Items|.
	MemberSlots int      // Total available slots for replication summed across all |Members|.
	NetworkHash uint64   // Content-sum which captures Items & Members, and their constraints.

	// Number of total Assignments, and primary Assignments by Member.
	// These share cardinality with |Members|.
	MemberTotalCount   []int
	MemberPrimaryCount []int

	// Number of item slots to shed from each member's ItemLimit, reducing its
	// effective capacity to ItemLimit - ShedCapacity. Exiting members are granted
	// ShedCapacity from available excess cluster capacity, ordered by age
	// (CreateRevision), so that the oldest exiting members drain first.
	// Shares cardinality with |Members|.
	ShedCapacity []int
}

// NewObservedState returns a *State instance which extracts and updates itself
// from the provided KeySpace, pivoted around the Member instance identified by
// |localKey|. Item consistency is determined using the provided IsConsistentFn.
// State should be treated as read-only, and a read lock of the parent KeySpace
// must be obtained before each use.
func NewObservedState(ks *keyspace.KeySpace, localKey string, fn IsConsistentFn) *State {
	var s = &State{
		KS:             ks,
		LocalKey:       localKey,
		IsConsistent:   fn,
		LocalMemberInd: -1,
	}
	ks.Mu.Lock()
	ks.Observers = append(ks.Observers, s.observe)
	ks.Mu.Unlock()
	return s
}

// observe extracts a current State representation from the KeySpace,
// pivoted around the Member instance identified by |LocalKey|.
func (s *State) observe() {

	// Re-init fields of State in preparation for extraction from the KeySpace.
	// KS & LocalKey are not modified, and may be concurrently accessed.
	s.Members = s.KS.Prefixed(s.KS.Root + MembersPrefix)
	s.Items = s.KS.Prefixed(s.KS.Root + ItemsPrefix)
	s.Assignments = s.KS.Prefixed(s.KS.Root + AssignmentsPrefix)
	s.LocalMemberInd = -1
	s.LocalItems = s.LocalItems[:0]
	s.Zones = s.Zones[:0]
	s.ZoneSlots = s.ZoneSlots[:0]
	s.ItemSlots = 0
	s.MemberSlots = 0
	s.NetworkHash = 0
	s.MemberTotalCount = make([]int, len(s.Members))
	s.MemberPrimaryCount = make([]int, len(s.Members))
	s.ShedCapacity = make([]int, len(s.Members))

	// Walk Members to:
	//  * Group the set of ordered |Zones| across all Members.
	//  * Initialize |ZoneSlots|.
	//  * Initialize |MemberSlots|.
	//  * Initialize |NetworkHash|.
	//  * Collect indices of exiting members.
	var exiting []int
	for i := range s.Members {
		var m = memberAt(s.Members, i)
		var slots = m.ItemLimit()
		var zone = len(s.Zones) - 1

		if slots == 0 {
			// Don't collect zones of members having no slots.
		} else if len(s.Zones) == 0 || s.Zones[zone] < m.Zone {
			s.Zones = append(s.Zones, m.Zone)
			s.ZoneSlots = append(s.ZoneSlots, slots)
			zone++
		} else if s.Zones[zone] > m.Zone {
			panic("invalid Member order")
		} else {
			s.ZoneSlots[zone] += slots
		}

		s.MemberSlots += slots
		s.NetworkHash = foldCRC(s.NetworkHash, s.Members[i].Raw.Key, slots)

		if m.IsExiting() {
			exiting = append(exiting, i)
		}
	}

	// Fetch |localMember| identified by |LocalKey|.
	if ind, found := s.Members.Search(s.LocalKey); !found {
		s.LocalMemberInd = -1
	} else {
		s.LocalMemberInd = ind
	}

	// Left-join Items with their Assignments to:
	//   * Initialize |ItemSlots|.
	//   * Initialize |NetworkHash|.
	//   * Collect Items and Assignments which map to the |LocalKey| Member.
	//   * Accumulate per-Member counts of primary and total Assignments.
	var it = LeftJoin{
		LenL: len(s.Items),
		LenR: len(s.Assignments),
		Compare: func(l, r int) int {
			return strings.Compare(itemAt(s.Items, l).ID, assignmentAt(s.Assignments, r).ItemID)
		},
	}

	var maxReplicationFactor int
	for cur, ok := it.Next(); ok; cur, ok = it.Next() {
		var item = itemAt(s.Items, cur.Left)
		var slots = item.DesiredReplication()

		s.ItemSlots += slots
		maxReplicationFactor = max(maxReplicationFactor, slots)
		s.NetworkHash = foldCRC(s.NetworkHash, s.Items[cur.Left].Raw.Key, slots)

		for r := cur.RightBegin; r != cur.RightEnd; r++ {
			var a = assignmentAt(s.Assignments, r)
			var key = MemberKey(s.KS, a.MemberZone, a.MemberSuffix)

			if key == s.LocalKey {
				s.LocalItems = append(s.LocalItems, LocalItem{
					Item:        s.Items[cur.Left],
					Assignments: s.Assignments[cur.RightBegin:cur.RightEnd],
					Index:       r - cur.RightBegin,
				})
			}
			if ind, found := s.Members.Search(key); found {
				if a.Slot == 0 {
					s.MemberPrimaryCount[ind]++
				}
				s.MemberTotalCount[ind]++
			}
		}
	}

	// Compute ShedCapacity for exiting members. ShedCapacity is granted to
	// exiting members, oldest first, up to each member's ItemLimit.
	// Excess slots available to grant as ShedCapacity. When the cluster is
	// overloaded (MemberSlots < ItemSlots), there is no excess to shed.
	var excessSlots = max(0, s.MemberSlots-s.ItemSlots)
	slices.SortFunc(exiting, func(a, b int) int {
		return cmp.Compare(s.Members[a].Raw.CreateRevision, s.Members[b].Raw.CreateRevision)
	})

	// We must retain at least maxReplicationFactor members at full capacity
	// to satisfy replication requirements. This is a numerical bound, not
	// zone-aware: we shed the oldest members first, expecting that replacement
	// members will restore zone diversity as they join.
	var maxShedding = len(s.Members) - maxReplicationFactor
	if maxShedding < 0 {
		maxShedding = 0
	}
	for n, i := range exiting {
		if n == maxShedding || excessSlots == 0 {
			break
		}
		var shed = min(memberAt(s.Members, i).ItemLimit(), excessSlots)
		s.ShedCapacity[i] = shed
		s.MemberSlots -= shed
		excessSlots -= shed
		s.NetworkHash = foldCRC(s.NetworkHash, s.Members[i].Raw.Key, shed)
	}
}

// shouldExit returns true iff the local Member is able to safely exit.
func (s *State) shouldExit() bool {
	return memberAt(s.Members, s.LocalMemberInd).IsExiting() && len(s.LocalItems) == 0
}

// isLeader returns true iff the local Member key is ordered first on
// (CreateRevision, Key) among all Member keys.
func (s *State) isLeader() bool {
	var leader keyspace.KeyValue
	for _, kv := range s.Members {
		if leader.Raw.CreateRevision == 0 || kv.Raw.CreateRevision < leader.Raw.CreateRevision {
			leader = kv
		}
	}
	return string(leader.Raw.Key) == s.LocalKey
}

func (s *State) debugLog() {
	var la []string
	for _, a := range s.LocalItems {
		la = append(la, string(a.Assignments[a.Index].Raw.Key))
	}

	log.WithFields(log.Fields{
		"Assignments":    len(s.Assignments),
		"ItemSlots":      s.ItemSlots,
		"Items":          len(s.Items),
		"LocalItems":     len(la),
		"LocalKey":       s.LocalKey,
		"LocalMemberInd": s.LocalMemberInd,
		"ZoneSlots":      s.ZoneSlots,
		"Members":        len(s.Members),
		"NetworkHash":    s.NetworkHash,
		"Revision":       s.KS.Header.Revision,
		"Zones":          s.Zones,
	}).Info("extracted State")
}

// memberEffectiveLimit returns the effective item limit for the member at
// index |ind|, accounting for any ShedCapacity granted to exiting members.
func (s *State) memberEffectiveLimit(ind int) int {
	return memberAt(s.Members, ind).ItemLimit() - s.ShedCapacity[ind]
}

// memberLoadRatio maps an |assignment| to a Member "load ratio". Given all
// |Members| and their corresponding |counts| (1:1 with |Members|),
// memberLoadRatio maps |assignment| to a Member and, if found, returns the
// ratio of the Member's index in |counts| to the Member's effective item
// limit. If the Member is not found, infinity is returned.
func (s *State) memberLoadRatio(assignment keyspace.KeyValue, counts []int) float32 {
	var a = assignment.Decoded.(Assignment)

	if ind, found := s.Members.Search(MemberKey(s.KS, a.MemberZone, a.MemberSuffix)); found {
		return float32(counts[ind]) / float32(s.memberEffectiveLimit(ind))
	}
	return math.MaxFloat32
}

func foldCRC(crc uint64, key []byte, n int) uint64 {
	var tmp [12]byte
	crc = crc64.Update(crc, crcTable, key)
	crc = crc64.Update(crc, crcTable, strconv.AppendInt(tmp[:0], int64(n), 10))
	return crc
}

var crcTable = crc64.MakeTable(crc64.ECMA)
