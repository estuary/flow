package v3_allocator

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/LiveRamp/gazette/pkg/keyspace"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

const (
	// Item keys are structured as "prefix/items/id"
	ItemsPrefix = "/items/"
	// Member keys are structured as "prefix/members/zone#suffix"
	MembersPrefix = "/members/"
	// Assignment keys are structured as "prefix/assign/item-id#zone#member-suffix#slot"
	AssignmentsPrefix = "/assign/"
	// '#' is selected as separator, because it's the first visual ASCII character
	// which is not interpreted by shells (preceding visual characters are " and !).
	// The fact that it's lowest-value ensures that the natural ordering of KeySpace
	// entities like Member and Assignment agrees with the lexicographic ordering of
	// their encoded Etcd keys. As fallout, this means ", !, and other non-visual
	// characters below ord('#') = 35 are disallowed (such as ' ', '\t', '\r', '\n'),
	// but everything else is fair game. Note that includes UTF-8, which by design
	// does not collide with the first 128 ASCII code-points.
	Sep, SepByte = "#", '#'
)

// MemberValue is a user-defined Member representation which also supports
// required APIs for use by Allocator.
type MemberValue interface {
	// Maximum number of Items this Member may be assigned.
	ItemLimit() int
}

// ItemValue is a user-defined Item representation which also supports required
// APIs for use by Allocator.
type ItemValue interface {
	// Desired number of assignments for this Item.
	DesiredReplication() int
	// IsConsistent returns true if the |assignment| is consistent. Some
	// implementations may determine consistency within the context of
	// the full set of item assignments, so |allAssignments| is also
	// provided (of which |assignment| is a member).
	IsConsistent(assignment keyspace.KeyValue, allAssignments keyspace.KeyValues) bool
}

// AssignmentValue is a user-defined Assignment representation.
type AssignmentValue interface{}

// AllocatorDecoder decodes "raw" Etcd values of Items, Members, and Assignments
// into their user-defined representations.
type AllocatorDecoder interface {
	DecodeItem(id string, raw *mvccpb.KeyValue) (ItemValue, error)
	DecodeMember(zone, suffix string, raw *mvccpb.KeyValue) (MemberValue, error)
	DecodeAssignment(itemID, memberZone, memberSuffix string, slot int, raw *mvccpb.KeyValue) (AssignmentValue, error)
}

// Item composes an Item ID with its user-defined ItemValue.
type Item struct {
	ID string
	ItemValue
}

// Member composes a Member Zone & Suffix with its user-defined MemberValue.
type Member struct {
	Zone   string
	Suffix string
	MemberValue
}

// Assignment composes an Assignment ItemID, MemberZone, MemberSuffix & Slot
// with its user-defined AssignmentValue.
type Assignment struct {
	ItemID       string
	MemberZone   string
	MemberSuffix string
	Slot         int
	AssignmentValue
}

// LocalItem represents an Item which is assigned to the local Allocator.
type LocalItem struct {
	Item        keyspace.KeyValue  // Item which is locally Assigned.
	Assignments keyspace.KeyValues // All Assignments of the Item.
	Index       int                // The index of the local Assignment within |Assignments|.
}

// NewAllocatorKeyValueDecoder returns a KeyValueDecoder utilizing the supplied
// AllocatorDecoder, and suitable for use with NewKeySpace of the same |prefix|.
// Some implementations may wish to further wrap the returned KeyValueDecoder
// to enable recognition and decoding of additional custom prefixes and entity
// types, beyond the Allocator's Members, Items, & Assignments.
func NewAllocatorKeyValueDecoder(prefix string, decode AllocatorDecoder) keyspace.KeyValueDecoder {
	var membersPrefix = prefix + MembersPrefix
	var itemsPrefix = prefix + ItemsPrefix
	var assignmentsPrefix = prefix + AssignmentsPrefix

	return func(raw *mvccpb.KeyValue) (interface{}, error) {
		switch {
		case bytes.HasPrefix(raw.Key, []byte(membersPrefix)):
			if p := strings.Split(string(raw.Key[len(membersPrefix):]), Sep); len(p) != 2 {
				return nil, fmt.Errorf("expected (zone, suffix) in member key")
			} else if value, err := decode.DecodeMember(p[0], p[1], raw); err != nil {
				return nil, err
			} else {
				return Member{Zone: p[0], Suffix: p[1], MemberValue: value}, nil
			}

		case bytes.HasPrefix(raw.Key, []byte(itemsPrefix)):
			if p := strings.Split(string(raw.Key[len(itemsPrefix):]), Sep); len(p) != 1 {
				return nil, fmt.Errorf("expected (id) in item key")
			} else if value, err := decode.DecodeItem(p[0], raw); err != nil {
				return nil, err
			} else {
				return Item{ID: p[0], ItemValue: value}, nil
			}

		case bytes.HasPrefix(raw.Key, []byte(assignmentsPrefix)):
			if p := strings.Split(string(raw.Key[len(assignmentsPrefix):]), Sep); len(p) != 4 {
				return nil, fmt.Errorf("expected (item-id, member-zone, member-suffix, slot) in assignment key")
			} else if slot, err := strconv.Atoi(p[3]); err != nil {
				return nil, err
			} else if value, err := decode.DecodeAssignment(p[0], p[1], p[2], slot, raw); err != nil {
				return nil, err
			} else {
				return Assignment{ItemID: p[0], MemberZone: p[1], MemberSuffix: p[2], Slot: slot, AssignmentValue: value}, nil
			}

		default:
			return nil, fmt.Errorf("unexpected key prefix")
		}
	}
}

// NewAllocatorKeySpace is a convenience for
// `NewKeySpace(prefix, NewAllocatorKeyValueDecoder(prefix, decode))`.
func NewAllocatorKeySpace(prefix string, decode AllocatorDecoder) *keyspace.KeySpace {
	return keyspace.NewKeySpace(prefix, NewAllocatorKeyValueDecoder(prefix, decode))
}

// MemberKey returns the unique key for a Member with |zone| and |suffix| under the KeySpace.
func MemberKey(ks *keyspace.KeySpace, zone, suffix string) string {
	assertAboveSep(zone)
	assertAboveSep(suffix)
	return ks.Root + MembersPrefix + zone + Sep + suffix
}

// ItemKey returns the unique key for an Item with ID |id| under the KeySpace.
func ItemKey(ks *keyspace.KeySpace, id string) string {
	assertAboveSep(id)
	return ks.Root + ItemsPrefix + id
}

// ItemAssignmentsPrefix returns the unique key prefix for all Assignments of |itemID| under the KeySpace.
func ItemAssignmentsPrefix(ks *keyspace.KeySpace, itemID string) string {
	assertAboveSep(itemID)
	return ks.Root + AssignmentsPrefix + itemID + Sep
}

// AssignmentKey returns the unique key for Assignment |assignment| under the KeySpace.
func AssignmentKey(ks *keyspace.KeySpace, a Assignment) string {
	assertAboveSep(a.MemberZone)
	assertAboveSep(a.MemberSuffix)
	return ItemAssignmentsPrefix(ks, a.ItemID) + a.MemberZone + Sep + a.MemberSuffix + Sep + strconv.Itoa(a.Slot)
}

// LookupMember returns the identified Member, or false if not found.
// The KeySpace must already be locked.
func LookupMember(ks *keyspace.KeySpace, zone, suffix string) (Member, bool) {
	if ind, found := ks.KeyValues.Search(MemberKey(ks, zone, suffix)); found {
		return memberAt(ks.KeyValues, ind), true
	} else {
		return Member{}, false
	}
}

// LookupItem returns the identified Item, or false if not found.
// The KeySpace must already be locked.
func LookupItem(ks *keyspace.KeySpace, id string) (Item, bool) {
	if ind, found := ks.KeyValues.Search(ItemKey(ks, id)); found {
		return itemAt(ks.KeyValues, ind), true
	} else {
		return Item{}, false
	}
}

func memberAt(kv keyspace.KeyValues, i int) Member         { return kv[i].Decoded.(Member) }
func itemAt(kv keyspace.KeyValues, i int) Item             { return kv[i].Decoded.(Item) }
func assignmentAt(kv keyspace.KeyValues, i int) Assignment { return kv[i].Decoded.(Assignment) }

// compareAssignment defines an order of Assignment over ItemID, MemberZone,
// and MemberSuffix. It matches the natural key order, with the exception of
// equating repetitions of (ItemID, MemberZone, MemberSuffix) having differing
// Slots (which is not allowed by the Allocator datamodel).
func compareAssignment(l, r Assignment) int {
	if l.ItemID < r.ItemID {
		return -1
	} else if l.ItemID > r.ItemID {
		return 1
	}
	if l.MemberZone < r.MemberZone {
		return -1
	} else if l.MemberZone > r.MemberZone {
		return 1
	}
	if l.MemberSuffix < r.MemberSuffix {
		return -1
	} else if l.MemberSuffix > r.MemberSuffix {
		return 1
	}
	return 0
}

// leftJoin performs a left join of two comparable, index-able, and ordered collections.
type leftJoin struct {
	// length of the collections.
	lenL, lenR int
	// compare returns -1 if |l| orders before |r|, 0 if they are equal,
	// and 1 if |l| is greater.
	compare func(l, r int) int

	cursor
}

// cursor is a leftJoin result row, relating a |left| index with a
// [rightBegin, rightEnd) range of indices comparing as equal.
type cursor struct {
	left, rightBegin, rightEnd int
}

// next returns the next cursor of the join and true, or if no rows remain in
// the join, a zero-valued cursor and false.
func (j *leftJoin) next() (cursor, bool) {
	for j.left < j.lenL {
		var c int

		if j.rightEnd == j.lenR {
			c = -1
		} else {
			c = j.compare(j.left, j.rightEnd)
		}

		switch c {
		case -1:
			// Left-hand entry is before next right-hand entry. Return left-hand entry with accumulated right-hand entries.
			var cur = j.cursor
			// Step to next left-hand entry. Reset right-hand range to iterate over the accumulated entries again.
			j.left, j.rightEnd = j.left+1, j.rightBegin
			return cur, true
		case 0:
			// Left-hand entry matches right-hand entry. Accumulate and step to next right-hand entry.
			j.rightEnd++
		case 1:
			// Left-hand entry is greater than right-hand entry. Skip right-hand entry.
			if j.rightBegin != j.rightEnd {
				panic("leftJoin inputs are not ordered")
			}
			j.rightEnd, j.rightBegin = j.rightEnd+1, j.rightEnd+1
		}
	}
	return cursor{}, false
}

func assertAboveSep(s string) {
	for i := range s {
		if s[i] <= SepByte {
			panic(fmt.Sprintf("invalid char <= '%c' (ind %d of %+q)", SepByte, i, s))
		}
	}
}
