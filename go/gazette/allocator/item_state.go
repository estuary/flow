package allocator

import (
	"sort"

	"go.etcd.io/etcd/client/v3"
	"go.gazette.dev/core/keyspace"
)

// itemState is an extracted representation of an Item and a collection of
// desired changes to its Assignments.
type itemState struct {
	global *State

	item    int                // Index of current Item within |global.Items|.
	current keyspace.KeyValues // Sub-slice of Item's current Assignments within |global.Assignments|.

	add     []Assignment       // Assignments we seek to add.
	remove  keyspace.KeyValues // Assignments we seek to remove.
	reorder keyspace.KeyValues // Assignments we seek to keep, and potentially re-order.
}

// init initializes the itemState by deriving the set of added, removed, and reordered Assignments
// given |current| and |desired|.
func (s *itemState) init(item int, current keyspace.KeyValues, desired []Assignment) {
	*s = itemState{
		global: s.global,

		item:    item,
		current: current,

		add:     s.add[:0],
		remove:  s.remove[:0],
		reorder: s.reorder[:0],
	}

	var i, j, nextSlot int

	// Initialize |nextSlot| to be greater than any Slot in use by any |current| Assignment.
	for _, a := range s.current {
		if s := a.Decoded.(Assignment).Slot + 1; s > nextSlot {
			nextSlot = s
		}
	}

	// Jointly walk |current| and |desired| on natural Assignment order
	// to determine desired add/remove/reorder edits.
	for i != len(s.current) && j != len(desired) {
		var a = assignmentAt(s.current, i)

		if c := compareAssignment(a, desired[j]); c > 0 {
			var a = desired[j]
			a.Slot, nextSlot = nextSlot, nextSlot+1
			s.add = append(s.add, a)
			j++
		} else if c < 0 {
			s.remove = append(s.remove, s.current[i])
			i++
		} else {
			s.reorder = append(s.reorder, s.current[i])
			i++
			j++
		}
	}
	for ; i != len(s.current); i++ {
		s.remove = append(s.remove, s.current[i])
	}
	for ; j != len(desired); j++ {
		var a = desired[j]
		a.Slot, nextSlot = nextSlot, nextSlot+1
		s.add = append(s.add, a)
	}
}

// constrainRemovals prunes Assignments from |s.remove| which would otherwise violate
// constraints, moving them to |s.reorder|.
func (s *itemState) constrainRemovals() {
	// Order |s.remove| on decreasing member load ratio
	// (the ratio of the member's total Assignments, vs its item limit).
	sort.Slice(s.remove, func(i, j int) bool {
		var ri = s.global.memberLoadRatio(s.remove[i], s.global.MemberTotalCount)
		var rj = s.global.memberLoadRatio(s.remove[j], s.global.MemberTotalCount)
		return ri > rj
	})
	var item = itemAt(s.global.Items, s.item)

	// Determine the current number of consistent item Assignments.
	var n int
	for _, a := range s.current {
		if s.global.IsConsistent(item, a, s.current) {
			n += 1
		}
	}
	// Release Assignments in decreasing order of member load ratio. Halt if
	// releasing an Assignment would violate the Item replication guarantee.
	var limit int

	for r := item.DesiredReplication(); n >= r && limit != len(s.remove); limit++ {
		if c := s.global.IsConsistent(item, s.remove[limit], s.current); c && n == r {
			break // We cannot remove this assignment without breaking n >= r.
		} else if c {
			n -= 1
		}
	}

	// Truncate removals to |limit|. Append the rest to |reorder|,
	// as we will not be removing these Assignments.
	s.reorder = append(s.reorder, s.remove[limit:]...)
	s.remove = s.remove[:limit]
}

// constrainReorders updates the ordering of |s.reorders| to ensure the best
// Assignment is selected as primary (which is always s.reorders[0]).
func (s *itemState) constrainReorders() {
	// Order |s.reorder| on ascending Assignment Slot.
	sort.Slice(s.reorder, func(i, j int) bool {
		return assignmentAt(s.reorder, i).Slot < assignmentAt(s.reorder, j).Slot
	})
	// The ordering is trivially satisfied iff there are no Assignments,
	// and is otherwise satisfied iff there is a current primary.
	if len(s.reorder) == 0 || assignmentAt(s.reorder, 0).Slot == 0 {
		return
	}
	var item = itemAt(s.global.Items, s.item)

	// There is no current primary. Select an assignment to promote, preferring:
	// a) Assignments which are currently consistent, and then
	// b) Assignments having a lower primary load ratio
	//    (the ratio of the member's primary Assignments, vs its item limit).

	var primary = struct {
		index        int
		isConsistent bool
		loadRatio    float32
		kv           keyspace.KeyValue
	}{index: -1}

	for i := range s.reorder {
		var c = s.global.IsConsistent(item, s.reorder[i], s.current)
		var r = s.global.memberLoadRatio(s.reorder[i], s.global.MemberPrimaryCount)

		if primary.index == -1 ||
			c == true && primary.isConsistent == false ||
			c == primary.isConsistent && r < primary.loadRatio {

			primary.index, primary.isConsistent, primary.loadRatio, primary.kv = i, c, r, s.reorder[i]
		}
	}

	// Shift elements [0, primary.index) to the right, by one.
	copy(s.reorder[1:primary.index+1], s.reorder[:primary.index])
	s.reorder[0] = primary.kv
}

// constrainAdds prunes Assignments from |s.add| which would otherwise violate constraints.
func (s *itemState) constrainAdds() {
	for i := 0; i != len(s.add); {
		var a = s.add[i]

		var ind, found = s.global.Members.Search(MemberKey(s.global.KS, a.MemberZone, a.MemberSuffix))
		if !found {
			panic("member not found")
		}

		if s.global.memberEffectiveLimit(ind) <= s.global.MemberTotalCount[ind] {
			// Addition would violate member's effective limit. Remove this Assignment.
			copy(s.add[i:], s.add[i+1:])
			s.add = s.add[:len(s.add)-1]
		} else {
			i++
		}
	}
}

// buildRemoveOps adds operations to |txn| removing each of the Assignments in |s.remove|.
func (s *itemState) buildRemoveOps(txn checkpointTxn) {
	for i, r := range s.remove {
		// Verify the Item (and Assignment itself) have not changed. Otherwise, the
		// Item Replication may have increased (and this removal could violate it).
		if i == 0 {
			txn.If(modRevisionUnchanged(s.global.Items[s.item]))
		}
		txn.If(modRevisionUnchanged(r))
		// Delete the Assignment to remove.
		txn.Then(clientv3.OpDelete(string(r.Raw.Key)))

		// Update to reflect the member's total count has decreased.
		var a = assignmentAt(s.remove, i)
		if ind, found := s.global.Members.Search(MemberKey(s.global.KS, a.MemberZone, a.MemberSuffix)); found {
			if a.Slot == 0 {
				s.global.MemberPrimaryCount[ind] -= 1
			}
			s.global.MemberTotalCount[ind] -= 1
		}
		// We allow for !found (and do not panic) to gracefully handle assignments
		// which somehow linger after their corresponding member is deleted (note
		// that in practice all keys of an Etcd lease are deleted in a single txn).

		allocatorAssignmentRemovedTotal.Inc()
	}
}

// buildPromoteOps adds operations to |txn| which, if required,
// promote a current Assignment to primary, if required.
func (s *itemState) buildPromoteOps(txn checkpointTxn) {
	if len(s.reorder) == 0 {
		return // No Assignments to promote.
	} else if a := assignmentAt(s.reorder, 0); a.Slot == 0 {
		return // Assignment is already primary.
	} else {
		a.Slot = 0 // Promote to Primary.

		// Update to reflect the member's primary count has increased.
		if ind, found := s.global.Members.Search(MemberKey(s.global.KS, a.MemberZone, a.MemberSuffix)); found {
			s.global.MemberPrimaryCount[ind] += 1
		}
		// Like buildRemoveOps, we allow for the possibility of !found (and do not
		// panic). Note that a member without a member key will have an infinite
		// load ratio, and is therefore promoted only as a last resort.
		s.buildMoveOps(txn, s.reorder[0], a)

		allocatorAssignmentPackedTotal.Inc()
	}
}

// buildAddOps adds operations to |txn| to create new Assignments for each of |s.add|.
func (s *itemState) buildAddOps(txn checkpointTxn) {
	for _, a := range s.add {
		var ind, found = s.global.Members.Search(MemberKey(s.global.KS, a.MemberZone, a.MemberSuffix))
		if !found {
			panic("member not found")
		}

		// Verify the Member has not changed. Otherwise, its ItemLimit may have decreased
		// (and this addition could violate it).
		txn.If(modRevisionUnchanged(s.global.Members[ind]))
		// Put an Assignment with an empty value under the Member's Lease.
		txn.Then(clientv3.OpPut(AssignmentKey(s.global.KS, a), "",
			clientv3.WithLease(clientv3.LeaseID(s.global.Members[ind].Raw.Lease))))

		// Update to reflect the member's total count (and potentially primary count) has increased.
		if a.Slot == 0 {
			s.global.MemberPrimaryCount[ind] += 1
		}
		s.global.MemberTotalCount[ind] += 1

		allocatorAssignmentAddedTotal.Inc()
	}
}

// buildPackOps adds operations to |txn| which shift the Slot of up to one
// current Assignment down to a lower and contiguous Slot index.
func (s *itemState) buildPackOps(txn checkpointTxn) {
	for i := range s.reorder {
		if i == 0 {
			continue // Case handled by buildPromoteOps.
		} else if a := assignmentAt(s.reorder, i); a.Slot != i {
			a.Slot = i
			s.buildMoveOps(txn, s.reorder[i], a)
			allocatorAssignmentPackedTotal.Inc()
			break
		}
	}
}

// buildMoveOps atomically moves the |cur| Assignment to a new key.
func (s *itemState) buildMoveOps(txn checkpointTxn, cur keyspace.KeyValue, a Assignment) {
	// Atomic move of same value from current Assignment key, to a new one under the current Lease.
	txn.If(modRevisionUnchanged(cur)).
		Then(
			clientv3.OpDelete(string(cur.Raw.Key)),
			clientv3.OpPut(AssignmentKey(s.global.KS, a), string(cur.Raw.Value),
				clientv3.WithLease(clientv3.LeaseID(cur.Raw.Lease))))
}

// constrainAndBuildOps applies all constraints, and then applies resulting
// add, remove, promotion, and packing operations to |txn|.
func (s *itemState) constrainAndBuildOps(txn checkpointTxn) error {
	s.constrainRemovals()
	s.constrainReorders()
	s.constrainAdds()

	s.buildAddOps(txn)
	s.buildRemoveOps(txn)
	s.buildPromoteOps(txn)

	if len(s.add) == 0 && len(s.remove) == 0 {
		s.buildPackOps(txn)
	}
	return txn.Checkpoint()
}
