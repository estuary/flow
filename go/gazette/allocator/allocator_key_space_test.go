package allocator

import (
	"context"
	"encoding/json"
	"fmt"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.gazette.dev/core/etcdtest"
	"go.gazette.dev/core/keyspace"
	gc "gopkg.in/check.v1"
)

type AllocKeySpaceSuite struct{}

// buildAllocKeySpaceFixture constructs a KeySpace fixture used by a number
// of tests across this package.
func buildAllocKeySpaceFixture(c *gc.C, ctx context.Context, client *clientv3.Client) {
	for k, v := range map[string]string{
		"/root/items/item-1":   `{"R": 2}`, // Items index 0
		"/root/items/item-two": `{"R": 1}`, // 1

		// Invalid KeyValues which are omitted.
		"/root/items/invalid#item#key": `{"R": 1}`,
		"/root/items/valid-id":         `invalid value`,

		"/root/members/us-east#bar": `{"R": 1}`, // Members index 0
		"/root/members/us-east#foo": `{"R": 2}`, // 1
		"/root/members/us-west#baz": `{"R": 3}`, // 2.

		// Invalid KeyValues which are omitted.
		"/root/members/invalid-key":       `{"R": 1}`,
		"/root/members/us-west#valid-key": `invalid value`,

		"/root/assign/item-1#us-east#foo#1":       `consistent`, // Assignments index 0
		"/root/assign/item-1#us-west#baz#0":       `consistent`, // 1
		"/root/assign/item-missing#us-west#baz#0": ``,           // 2
		"/root/assign/item-two#missing#member#2":  ``,           // 3
		"/root/assign/item-two#us-east#bar#0":     `consistent`, // 4
		"/root/assign/item-two#us-west#baz#1":     ``,           // 5

		// Invalid KeyValues which are omitted.
		"/root/assign/item-1#us-east#foo#invalid-slot": ``,
		"/root/assign/item-two#valid#key#2":            `invalid value`,

		"/root/aaaaa/unknown#prefix": ``,
		"/root/jjjjj/unknown#prefix": ``,
		"/root/zzzzz/unknown#prefix": ``,
	} {
		var _, err = client.Put(ctx, k, v)
		c.Assert(err, gc.IsNil)
	}
}

func (s *AllocKeySpaceSuite) TestKeyOrderingEdgeCases(c *gc.C) {
	var ks = NewAllocatorKeySpace("/root", testAllocDecoder{})

	// Expect MemberKey and AssignmentKey encodings are able to handle prefix
	// cases appropriately, such that key ordering matches natural ordering.
	var cases = []string{
		MemberKey(ks, "11", "XX"),
		MemberKey(ks, "111", "XX"),
		MemberKey(ks, "1111", "XX"),
		MemberKey(ks, "22", "Y"),
		MemberKey(ks, "22", "YY"),
		MemberKey(ks, "22", "YYY"),
	}
	for i := 1; i != len(cases); i++ {
		c.Check(cases[i-1] < cases[i], gc.Equals, true)
	}

	cases = []string{
		AssignmentKey(ks, Assignment{"11", "xx", "YY", 0, nil}),
		AssignmentKey(ks, Assignment{"111", "xx", "YY", 0, nil}),
		AssignmentKey(ks, Assignment{"22", "xx", "YY", 0, nil}),
		AssignmentKey(ks, Assignment{"22", "xxx", "YY", 0, nil}),
		AssignmentKey(ks, Assignment{"33", "xx", "yy", 0, nil}),
		AssignmentKey(ks, Assignment{"33", "xx", "yyy", 0, nil}),
		AssignmentKey(ks, Assignment{"33", "xx", "yyy", 1, nil}),
	}
	for i := 1; i != len(cases); i++ {
		c.Check(cases[i-1] < cases[i], gc.Equals, true)
	}
}

func (s *AllocKeySpaceSuite) TestKeyConstructorsAssertSepOrdering(c *gc.C) {
	var ks = NewAllocatorKeySpace("/root", testAllocDecoder{})

	var cases = []func(){
		func() { MemberKey(ks, "zone\x00", "suffix") },
		func() { MemberKey(ks, "zone", "suffix\x00") },

		func() { ItemKey(ks, "item\x00") },

		func() { AssignmentKey(ks, Assignment{ItemID: "item\x00", MemberZone: "zone", MemberSuffix: "suffix"}) },
		func() { AssignmentKey(ks, Assignment{ItemID: "item", MemberZone: "zone\x00", MemberSuffix: "suffix"}) },
		func() { AssignmentKey(ks, Assignment{ItemID: "item", MemberZone: "zone", MemberSuffix: "suffix\x00"}) },
	}
	for _, fn := range cases {
		c.Check(fn, gc.PanicMatches, `invalid char <= '#' \(ind \d of ".*"\)`)
	}
}

func (s *AllocKeySpaceSuite) TestAllocKeySpaceDecoding(c *gc.C) {
	var client, ctx = etcdtest.TestClient(), context.Background()
	defer etcdtest.Cleanup()
	buildAllocKeySpaceFixture(c, ctx, client)

	var ks = NewAllocatorKeySpace("/root", testAllocDecoder{})
	c.Check(ks.Load(ctx, client, 0), gc.IsNil)

	// Verify expected contents of the decoded KeySpace.
	var expect = []struct {
		key   string
		value interface{}
	}{
		{"/root/assign/item-1#us-east#foo#1", Assignment{
			ItemID: "item-1", MemberZone: "us-east", MemberSuffix: "foo", Slot: 1, AssignmentValue: testAssignment{true}}},
		{"/root/assign/item-1#us-west#baz#0", Assignment{
			ItemID: "item-1", MemberZone: "us-west", MemberSuffix: "baz", Slot: 0, AssignmentValue: testAssignment{true}}},
		{"/root/assign/item-missing#us-west#baz#0", Assignment{
			ItemID: "item-missing", MemberZone: "us-west", MemberSuffix: "baz", Slot: 0, AssignmentValue: testAssignment{false}}},
		{"/root/assign/item-two#missing#member#2", Assignment{
			ItemID: "item-two", MemberZone: "missing", MemberSuffix: "member", Slot: 2, AssignmentValue: testAssignment{false}}},
		{"/root/assign/item-two#us-east#bar#0", Assignment{
			ItemID: "item-two", MemberZone: "us-east", MemberSuffix: "bar", Slot: 0, AssignmentValue: testAssignment{true}}},
		{"/root/assign/item-two#us-west#baz#1", Assignment{
			ItemID: "item-two", MemberZone: "us-west", MemberSuffix: "baz", Slot: 1, AssignmentValue: testAssignment{false}}},

		{"/root/items/item-1", Item{ID: "item-1", ItemValue: testItem{R: 2}}},
		{"/root/items/item-two", Item{ID: "item-two", ItemValue: testItem{R: 1}}},

		{"/root/members/us-east#bar", Member{Zone: "us-east", Suffix: "bar", MemberValue: testMember{R: 1}}},
		{"/root/members/us-east#foo", Member{Zone: "us-east", Suffix: "foo", MemberValue: testMember{R: 2}}},
		{"/root/members/us-west#baz", Member{Zone: "us-west", Suffix: "baz", MemberValue: testMember{R: 3}}},
	}

	c.Assert(ks.KeyValues, gc.HasLen, len(expect))
	for i, e := range expect {
		c.Check(string(ks.KeyValues[i].Raw.Key), gc.Equals, e.key)
		c.Check(ks.KeyValues[i].Decoded, gc.DeepEquals, e.value)
	}
}

func (s *AllocKeySpaceSuite) TestKeyAndLookupHelpers(c *gc.C) {
	var client, ctx = etcdtest.TestClient(), context.Background()
	defer etcdtest.Cleanup()
	buildAllocKeySpaceFixture(c, ctx, client)

	var ks = NewAllocatorKeySpace("/root", testAllocDecoder{})
	c.Check(ks.Load(ctx, client, 0), gc.IsNil)

	var item, ok = LookupItem(ks, "item-1")
	c.Check(ok, gc.Equals, true)
	c.Check(item, gc.DeepEquals, Item{ID: "item-1", ItemValue: testItem{R: 2}})

	item, ok = LookupItem(ks, "item-does-not-exist")
	c.Check(ok, gc.Equals, false)
	c.Check(item, gc.DeepEquals, Item{})

	member, ok := LookupMember(ks, "us-east", "foo")
	c.Check(ok, gc.Equals, true)
	c.Check(member, gc.DeepEquals, Member{Zone: "us-east", Suffix: "foo", MemberValue: testMember{R: 2}})

	member, ok = LookupMember(ks, "us-west", "not-found")
	c.Check(ok, gc.Equals, false)
	c.Check(member, gc.DeepEquals, Member{})

	c.Check(ks.Prefixed(ItemAssignmentsPrefix(ks, "item-two")),
		gc.DeepEquals, ks.KeyValues[3:6])

	// Expect use of separator ensures the prefix of another item doesn't return its assignments.
	c.Check(ks.Prefixed(ItemAssignmentsPrefix(ks, "item-tw")),
		gc.DeepEquals, ks.KeyValues[3:3])

	ind, ok := ks.Search(AssignmentKey(ks,
		Assignment{ItemID: "item-two", MemberZone: "us-west", MemberSuffix: "baz", Slot: 1}))
	c.Check(ok, gc.Equals, true)
	c.Check(ind, gc.Equals, 5)

	ind, ok = ks.Search(AssignmentKey(ks,
		Assignment{ItemID: "item-two", MemberZone: "us-west", MemberSuffix: "baz", Slot: 10}))
	c.Check(ok, gc.Equals, false)
}

func (s *AllocKeySpaceSuite) TestAssignmentCompare(c *gc.C) {
	var client, ctx = etcdtest.TestClient(), context.Background()
	defer etcdtest.Cleanup()
	buildAllocKeySpaceFixture(c, ctx, client)

	var ks = NewAllocatorKeySpace("/root", testAllocDecoder{})
	c.Check(ks.Load(ctx, client, 0), gc.IsNil)

	// Expect |compareAssignment| matches natural Assignment key order.
	var assignments = ks.Prefixed(ks.Root + AssignmentsPrefix)
	for i := range assignments[:len(assignments)-1] {
		var ai, aii = assignmentAt(assignments, i), assignmentAt(assignments, i+1)
		c.Check(compareAssignment(ai, aii), gc.Equals, -1)
		c.Check(compareAssignment(aii, ai), gc.Equals, 1)
		c.Check(compareAssignment(ai, ai), gc.Equals, 0)
	}

	// Verify ItemID, MemberZone, and MemberSuffix drive the comparision, in that order.
	var a1 = Assignment{ItemID: "aaa", MemberZone: "bbb", MemberSuffix: "ccc"}
	var a2 = a1

	c.Check(compareAssignment(a1, a2), gc.Equals, 0)
	c.Check(compareAssignment(a2, a1), gc.Equals, 0)

	a2.MemberSuffix = "ccd"
	c.Check(compareAssignment(a2, a1), gc.Equals, 1)
	c.Check(compareAssignment(a1, a2), gc.Equals, -1)

	a2.MemberZone = "bba"
	c.Check(compareAssignment(a2, a1), gc.Equals, -1)
	c.Check(compareAssignment(a1, a2), gc.Equals, 1)

	a2.ItemID = "aab"
	c.Check(compareAssignment(a2, a1), gc.Equals, 1)
	c.Check(compareAssignment(a1, a2), gc.Equals, -1)
}

func (s *AllocKeySpaceSuite) TestLeftJoin(c *gc.C) {
	var L1 = []float32{
		1.01, // 0
		1.02, // 1
		3.03, // 2
		4.04, // 3
		4.05, // 4
		5.06, // 5
		7.07, // 6
		9.08, // 7
		9.09, // 8
	}
	var L2 = []float32{
		1.1, // 0
		4.2, // 1
		4.3, // 2
		4.4, // 3
		5.5, // 4
		5.6, // 5
		6.7, // 6
		6.8, // 7
		7.9, // 8
	}
	// Compare the integer portions of L1, L2 items. Decimal portions are not
	// functionally meaningful, and serve only to clarify comments below.
	var compare = func(l, r int) int {
		switch li, ri := int(L1[l]), int(L2[r]); {
		case li < ri:
			return -1
		case li == ri:
			return 0
		default:
			return 1
		}
	}

	var expect = []LeftJoinCursor{
		// 1.01, 1.02 <=> 1.1
		{Left: 0, RightBegin: 0, RightEnd: 1},
		{Left: 1, RightBegin: 0, RightEnd: 1},
		// 3.03 <=> empty
		{Left: 2, RightBegin: 1, RightEnd: 1},
		// 4.04, 4.05 <=> 4.2, 4.3, 4.4
		{Left: 3, RightBegin: 1, RightEnd: 4},
		{Left: 4, RightBegin: 1, RightEnd: 4},
		// 5.06 <=> 5.5, 5.6
		{Left: 5, RightBegin: 4, RightEnd: 6},
		// 7.07 <=> 7.9
		{Left: 6, RightBegin: 8, RightEnd: 9},
		// 9.08, 9.09 <=> empty
		{Left: 7, RightBegin: 9, RightEnd: 9},
		{Left: 8, RightBegin: 9, RightEnd: 9},
	}

	var lj = LeftJoin{
		LenL:    len(L1),
		LenR:    len(L2),
		Compare: compare,
	}
	for _, tc := range expect {
		var cur, ok = lj.Next()
		c.Check(ok, gc.Equals, true)
		c.Check(cur, gc.Equals, tc)
	}
	var _, ok = lj.Next()
	c.Check(ok, gc.Equals, false)

	// Run again, this time reverse L1/L2 Left/right side order.

	expect = []LeftJoinCursor{
		// 1.1 <=> 1.01, 1.05
		{Left: 0, RightBegin: 0, RightEnd: 2},
		// 4.2, 4.3, 4.4 <=> 4.04, 4.05
		{Left: 1, RightBegin: 3, RightEnd: 5},
		{Left: 2, RightBegin: 3, RightEnd: 5},
		{Left: 3, RightBegin: 3, RightEnd: 5},
		// 5.5, 5.6 <=> 5.06
		{Left: 4, RightBegin: 5, RightEnd: 6},
		{Left: 5, RightBegin: 5, RightEnd: 6},
		// 6.7, 6.8 <=> empty
		{Left: 6, RightBegin: 6, RightEnd: 6},
		{Left: 7, RightBegin: 6, RightEnd: 6},
		// 7.9 <=> empty
		{Left: 8, RightBegin: 6, RightEnd: 7},
	}

	lj = LeftJoin{
		LenL:    len(L2),
		LenR:    len(L1),
		Compare: func(l, r int) int { return -1 * compare(r, l) },
	}
	for _, tc := range expect {
		var cur, ok = lj.Next()
		c.Check(ok, gc.Equals, true)
		c.Check(cur, gc.Equals, tc)
	}
	_, ok = lj.Next()
	c.Check(ok, gc.Equals, false)
}

type testAllocDecoder struct{}

func (d testAllocDecoder) DecodeItem(id string, raw *mvccpb.KeyValue) (ItemValue, error) {
	var i testItem
	var err = json.Unmarshal(raw.Value, &i)
	return i, err
}

func (d testAllocDecoder) DecodeMember(zone, suffix string, raw *mvccpb.KeyValue) (MemberValue, error) {
	var m testMember
	var err = json.Unmarshal(raw.Value, &m)
	return m, err
}

func (d testAllocDecoder) DecodeAssignment(itemID, zone, suffix string, slot int, raw *mvccpb.KeyValue) (AssignmentValue, error) {
	switch s := string(raw.Value); s {
	case "":
		return testAssignment{consistent: false}, nil
	case "consistent":
		return testAssignment{consistent: true}, nil
	default:
		return nil, fmt.Errorf("invalid value: %s", s)
	}
}

type testItem struct{ R int }

func (i testItem) DesiredReplication() int { return i.R }

func isConsistent(_ Item, assignment keyspace.KeyValue, allAssignments keyspace.KeyValues) bool {
	return assignment.Decoded.(Assignment).AssignmentValue.(testAssignment).consistent
}

type testMember struct {
	R int  // ItemLimit
	E bool // Exiting
}

func (m testMember) ItemLimit() int   { return m.R }
func (m testMember) Validate() error  { return nil }
// TODO(whb): Zero'ing R is for backward compatibility; remove once deployment is complete.
func (m *testMember) SetExiting()     { m.E = true; m.R = 0 }
func (m testMember) IsExiting() bool  { return m.E }

func (m *testMember) MarshalString() string {
	if b, err := json.Marshal(m); err != nil {
		panic(err)
	} else {
		return string(b)
	}
}

type testAssignment struct{ consistent bool }

var _ = gc.Suite(&AllocKeySpaceSuite{})
