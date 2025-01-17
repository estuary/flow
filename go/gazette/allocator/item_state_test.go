package allocator

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.gazette.dev/core/etcdtest"
	"go.gazette.dev/core/keyspace"
	gc "gopkg.in/check.v1"
)

type ItemStateSuite struct{}

func (s *ItemStateSuite) TestItemConstraintsSomeRemoved(c *gc.C) {
	var ks, is = buildItemStateFixture(c, map[string]string{
		"/root/items/item": `{"R": 1}`,

		// Member item limits imply that load ratio increases with each member index.
		"/root/members/zone#member-1": `{"R": 5}`,
		"/root/members/zone#member-2": `{"R": 4}`,
		"/root/members/zone#member-3": `{"R": 3}`,
		"/root/members/zone#member-4": `{"R": 2}`,
		"/root/members/zone#member-5": `{"R": 1}`,

		// Initial state: Item R = 1, Num Consistent = 2.
		"/root/assign/item#zone#member-1#0": ``,           // 5: Not removed (lower load ratio than member-2).
		"/root/assign/item#zone#member-2#1": `consistent`, // 4: Not removed (would break Item replication).
		"/root/assign/item#zone#member-3#2": ``,           // 3: Removed (not consistent).
		"/root/assign/item#zone#member-4#3": `consistent`, // 2: Removed (consistent, but allowed).
		"/root/assign/item#zone#member-5#4": ``,           // 1: Removed (not consistent).
	})
	var current = ks.Prefixed(ItemAssignmentsPrefix(ks, "item"))

	is.init(0, current, []Assignment{})
	c.Check(is.add, gc.HasLen, 0)
	c.Check(is.remove, gc.HasLen, 5)
	c.Check(is.reorder, gc.HasLen, 0)

	// However, removing all Assignments would break item replication. Expect
	// constrainRemovals walks Assignments to remove by decreasing member load
	// ratio (in this fixture, reverse order), and removes Assignments so long as
	// doing so doesn't break the Item replication constraint.
	is.constrainRemovals()

	c.Check(is.remove, gc.DeepEquals, keyspace.KeyValues{current[4], current[3], current[2]})
	c.Check(is.reorder, gc.DeepEquals, keyspace.KeyValues{current[1], current[0]})
}

func (s *ItemStateSuite) TestItemConstraintsAllRemoved(c *gc.C) {
	var ks, is = buildItemStateFixture(c, map[string]string{
		"/root/items/item": `{"R": 0}`,

		"/root/members/zone#member-1": `{"R": 1}`,
		"/root/members/zone#member-2": `{"R": 2}`,

		"/root/assign/item#zone#member-1#0": ``,
		"/root/assign/item#zone#member-2#1": `consistent`,
	})
	var current = ks.Prefixed(ItemAssignmentsPrefix(ks, "item"))

	is.init(0, current, []Assignment{})
	c.Check(is.add, gc.HasLen, 0)
	c.Check(is.remove, gc.HasLen, 2)
	c.Check(is.reorder, gc.HasLen, 0)

	// Both Assignments are removed (trivially satisfies constraint R=0).
	is.constrainRemovals()
	c.Check(is.remove, gc.DeepEquals, current)
	c.Check(is.reorder, gc.HasLen, 0)
}

func (s *ItemStateSuite) TestItemConstraintsNoneRemovedDueToLoadRatios(c *gc.C) {
	var ks, is = buildItemStateFixture(c, map[string]string{
		"/root/items/item": `{"R": 1}`,

		"/root/members/zone#member-1": `{"R": 2}`,
		"/root/members/zone#member-2": `{"R": 1}`,

		"/root/assign/item#zone#member-1#0": ``,
		"/root/assign/item#zone#member-2#1": `consistent`,
	})
	var current = ks.Prefixed(ItemAssignmentsPrefix(ks, "item"))

	is.init(0, current, []Assignment{})
	c.Check(is.add, gc.HasLen, 0)
	c.Check(is.remove, gc.HasLen, 2)
	c.Check(is.reorder, gc.HasLen, 0)

	// Neither Assignment is removed (member-2 considered first, and would violate R=1).
	is.constrainRemovals()
	c.Check(is.remove, gc.HasLen, 0)
	c.Check(is.reorder, gc.DeepEquals, keyspace.KeyValues{current[1], current[0]})
}

func (s *ItemStateSuite) TestItemConstraintsNoneRemovedWhenConsistencyNotMet(c *gc.C) {
	var ks, is = buildItemStateFixture(c, map[string]string{
		"/root/items/item": `{"R": 2}`,

		"/root/members/zone#member-1": `{"R": 1}`,
		"/root/members/zone#member-2": `{"R": 2}`,
		"/root/members/zone#member-3": `{"R": 3}`,

		"/root/assign/item#zone#member-1#0": ``,
		"/root/assign/item#zone#member-2#1": `consistent`,
		"/root/assign/item#zone#member-3#2": ``,
	})
	var current = ks.Prefixed(ItemAssignmentsPrefix(ks, "item"))

	is.init(0, current, []Assignment{})
	c.Check(is.add, gc.HasLen, 0)
	c.Check(is.remove, gc.HasLen, 3)
	c.Check(is.reorder, gc.HasLen, 0)

	// No Assignments are removed (member-1 would be considered first & allowed if consistency was met).
	is.constrainRemovals()
	c.Check(is.remove, gc.HasLen, 0)
	c.Check(is.reorder, gc.DeepEquals, current)
}

func (s *ItemStateSuite) TestRemoveAndPromotePrimary(c *gc.C) {
	var ks, is = buildItemStateFixture(c, map[string]string{
		"/root/items/item":   `{"R": 1}`,
		"/root/items/zzzz-2": `{"R": 1}`,
		"/root/items/zzzz-3": `{"R": 1}`,

		"/root/members/zone#member-0": `{"R": 1}`,
		"/root/members/zone#member-1": `{"R": 5}`,
		"/root/members/zone#member-2": `{"R": 2}`,
		"/root/members/zone#member-3": `{"R": 3}`,

		"/root/assign/item#zone#member-0#0": ``,           // Removed.
		"/root/assign/item#zone#member-1#1": ``,           // Primary load ratio of zero, but not consistent.
		"/root/assign/item#zone#member-2#2": `consistent`, // Primary load ratio of 1/2.
		"/root/assign/item#zone#member-3#3": `consistent`, // Primary load ratio of 1/3 (selected).

		// Other primary Assignments of fixture Members.
		"/root/assign/zzzz-2#zone#member-2#0": ``,
		"/root/assign/zzzz-3#zone#member-3#0": ``,
	})
	var current = ks.Prefixed(ItemAssignmentsPrefix(ks, "item"))

	is.init(0, current, []Assignment{
		{ItemID: "item", MemberZone: "zone", MemberSuffix: "member-1"},
		{ItemID: "item", MemberZone: "zone", MemberSuffix: "member-2"},
		{ItemID: "item", MemberZone: "zone", MemberSuffix: "member-3"},
	})

	c.Check(is.add, gc.HasLen, 0)
	c.Check(is.remove, gc.HasLen, 1)
	c.Check(is.reorder, gc.HasLen, 3)

	// Expect member-3 was promoted to primary, as it's consistent and has the lowest primary load ratio.
	is.constrainReorders()
	c.Check(is.reorder, gc.DeepEquals, keyspace.KeyValues{current[3], current[1], current[2]})
}

func (s *ItemStateSuite) TestPromoteWithNoCurrentPrimary(c *gc.C) {
	var ks, is = buildItemStateFixture(c, map[string]string{
		"/root/items/item": `{"R": 1}`,

		"/root/members/zone#member-0": `{"R": 3}`,
		"/root/members/zone#member-1": `{"R": 1}`,
		"/root/members/zone#member-2": `{"R": 1}`,

		"/root/assign/item#zone#member-0#1": ``,
		"/root/assign/item#zone#member-1#2": `consistent`,
		"/root/assign/item#zone#member-2#3": ``,
	})
	var current = ks.Prefixed(ItemAssignmentsPrefix(ks, "item"))

	is.init(0, current, []Assignment{
		{ItemID: "item", MemberZone: "zone", MemberSuffix: "member-0"},
		{ItemID: "item", MemberZone: "zone", MemberSuffix: "member-1"},
		{ItemID: "item", MemberZone: "zone", MemberSuffix: "member-2"},
	})

	// Expect member-1 was promoted to primary, as the only consistent replica.
	is.constrainReorders()
	c.Check(is.reorder, gc.DeepEquals, keyspace.KeyValues{current[1], current[0], current[2]})
}

func (s *ItemStateSuite) TestPrimaryAlreadyExists(c *gc.C) {
	var ks, is = buildItemStateFixture(c, map[string]string{
		"/root/items/item": `{"R": 1}`,

		"/root/members/zone#member-0": `{"R": 2}`,
		"/root/members/zone#member-1": `{"R": 1}`,

		"/root/assign/item#zone#member-0#0": ``,
		"/root/assign/item#zone#member-1#1": `consistent`,
	})
	var current = ks.Prefixed(ItemAssignmentsPrefix(ks, "item"))

	is.init(0, current, []Assignment{
		{ItemID: "item", MemberZone: "zone", MemberSuffix: "member-0"},
		{ItemID: "item", MemberZone: "zone", MemberSuffix: "member-1"},
	})

	// Expect order is not changed, even though member-0 is not consistent.
	is.constrainReorders()
	c.Check(is.reorder, gc.DeepEquals, current)
}

func (s *ItemStateSuite) TestAddMemberConstraints(c *gc.C) {
	var ks, is = buildItemStateFixture(c, map[string]string{
		"/root/items/item": `{"R": 1}`,
		"/root/items/zzzz": `{"R": 1}`,

		"/root/members/zone#member-0": `{"R": 1}`,
		"/root/members/zone#member-1": `{"R": 1}`,
		"/root/members/zone#member-2": `{"R": 2}`,
		"/root/members/zone#member-3": `{"R": 2}`,

		"/root/assign/item#zone#member-3#7": ``,

		// member-1 and member-2 have Assignments to another Item.
		"/root/assign/zzzz#zone#member-1#1": ``,
		"/root/assign/zzzz#zone#member-2#2": ``,
	})
	var current = ks.Prefixed(ItemAssignmentsPrefix(ks, "item"))

	is.init(0, current, []Assignment{
		{ItemID: "item", MemberZone: "zone", MemberSuffix: "member-0"},
		{ItemID: "item", MemberZone: "zone", MemberSuffix: "member-1"},
		{ItemID: "item", MemberZone: "zone", MemberSuffix: "member-2"},
	})
	c.Check(is.add, gc.HasLen, 3)

	is.constrainAdds()

	// Expect member-1 is not added, as it would violate the Member ItemLimit.
	// Added slots are initialized at the highest current slot, +1.
	c.Check(is.add, gc.DeepEquals, []Assignment{
		{ItemID: "item", MemberZone: "zone", MemberSuffix: "member-0", Slot: 8},
		{ItemID: "item", MemberZone: "zone", MemberSuffix: "member-2", Slot: 10},
	})
}

func (s *ItemStateSuite) TestBuildRemoveOps(c *gc.C) {
	var ks, is = buildItemStateFixture(c, map[string]string{
		"/root/items/item": `{"R": 0}`,

		"/root/members/zone#member-0": `{"R": 1}`,
		"/root/members/zone#member-1": `{"R": 1}`,

		"/root/assign/item#zone#member-0#0": ``,
		"/root/assign/item#zone#member-1#1": ``,
	})
	var item = ks.Prefixed(ks.Root + ItemsPrefix)[0]
	var current = ks.Prefixed(ItemAssignmentsPrefix(ks, "item"))

	// Precondition: counts reflect |current| Assignments.
	c.Check(current, gc.HasLen, 2)
	c.Check(is.global.MemberTotalCount, gc.DeepEquals, []int{1, 1})
	c.Check(is.global.MemberPrimaryCount, gc.DeepEquals, []int{1, 0})

	is.init(0, current, []Assignment{})
	var txn mockTxnBuilder
	is.buildRemoveOps(&txn)

	// Expect |txn| verifies |item| and |current| Assignments haven't changed.
	c.Check(txn.cmps, gc.DeepEquals, []clientv3.Cmp{
		clientv3.Compare(clientv3.ModRevision("/root/items/item"), "=", item.Raw.ModRevision),
		clientv3.Compare(clientv3.ModRevision("/root/assign/item#zone#member-0#0"), "=", current[0].Raw.ModRevision),
		clientv3.Compare(clientv3.ModRevision("/root/assign/item#zone#member-1#1"), "=", current[1].Raw.ModRevision),
	})
	// And then removes both Assignments.
	c.Check(txn.ops, gc.DeepEquals, []clientv3.Op{
		clientv3.OpDelete("/root/assign/item#zone#member-0#0"),
		clientv3.OpDelete("/root/assign/item#zone#member-1#1"),
	})

	// Expect Member counts were updated to reflect the release of total & primary replicas.
	c.Check(is.global.MemberPrimaryCount, gc.DeepEquals, []int{0, 0})
	c.Check(is.global.MemberTotalCount, gc.DeepEquals, []int{0, 0})
}

func (s *ItemStateSuite) TestBuildPromoteOps(c *gc.C) {
	var ks, is = buildItemStateFixture(c, map[string]string{
		"/root/items/item": `{"R": 2}`,

		"/root/members/zone#member-0": `{"R": 1}`,
		"/root/members/zone#member-1": `{"R": 1}`,

		"/root/assign/item#zone#member-0#8": ``,
		"/root/assign/item#zone#member-1#9": `consistent`,
	})
	var current = ks.Prefixed(ItemAssignmentsPrefix(ks, "item"))
	// Tweak fixture to model Etcd leases on |current| Assignments.
	c.Check(current, gc.HasLen, 2)
	current[0].Raw.Lease = 0xfeed
	current[1].Raw.Lease = 0xbeef

	// Precondition: counts reflect |current| Assignments.
	c.Check(is.global.MemberTotalCount, gc.DeepEquals, []int{1, 1})
	c.Check(is.global.MemberPrimaryCount, gc.DeepEquals, []int{0, 0})

	is.init(0, current, []Assignment{
		{ItemID: "item", MemberZone: "zone", MemberSuffix: "member-0"},
		{ItemID: "item", MemberZone: "zone", MemberSuffix: "member-1"},
	})
	is.constrainReorders() // Selects member-1 for promotion.
	var txn mockTxnBuilder
	is.buildPromoteOps(&txn)

	// Expect |txn| verifies member-1's Assignment hasn't changed.
	c.Check(txn.cmps, gc.DeepEquals, []clientv3.Cmp{
		clientv3.Compare(clientv3.ModRevision("/root/assign/item#zone#member-1#9"), "=", current[1].Raw.ModRevision),
	})
	// And then atomically moves the Assignment to slot 0, preserving its current value & LeaseID.
	c.Check(txn.ops, gc.DeepEquals, []clientv3.Op{
		clientv3.OpDelete("/root/assign/item#zone#member-1#9"),
		clientv3.OpPut("/root/assign/item#zone#member-1#0", "consistent", clientv3.WithLease(0xbeef)),
	})

	// Expect Member counts were updated to reflect the release of total & primary replicas.
	c.Check(is.global.MemberPrimaryCount, gc.DeepEquals, []int{0, 1})
	c.Check(is.global.MemberTotalCount, gc.DeepEquals, []int{1, 1})
}

func (s *ItemStateSuite) TestBuildAddOps(c *gc.C) {
	var ks, is = buildItemStateFixture(c, map[string]string{
		"/root/items/item": `{"R": 2}`,

		"/root/members/zone#member-0": `{"R": 1}`,
		"/root/members/zone#member-1": `{"R": 1}`,
	})
	var members = ks.Prefixed(ks.Root + MembersPrefix)

	// Tweak fixture to model Etcd leases on |Members|.
	c.Check(members, gc.HasLen, 2)
	members[0].Raw.Lease = 0xfeed
	members[1].Raw.Lease = 0xbeef

	// Precondition: counts reflect lack of Assignments.
	c.Check(is.global.MemberTotalCount, gc.DeepEquals, []int{0, 0})
	c.Check(is.global.MemberPrimaryCount, gc.DeepEquals, []int{0, 0})

	is.init(0, nil, []Assignment{
		{ItemID: "item", MemberZone: "zone", MemberSuffix: "member-0"},
		{ItemID: "item", MemberZone: "zone", MemberSuffix: "member-1"},
	})
	var txn mockTxnBuilder
	is.buildAddOps(&txn)

	// Expect |txn| verifies both Members haven't changed.
	c.Check(txn.cmps, gc.DeepEquals, []clientv3.Cmp{
		clientv3.Compare(clientv3.ModRevision("/root/members/zone#member-0"), "=", members[0].Raw.ModRevision),
		clientv3.Compare(clientv3.ModRevision("/root/members/zone#member-1"), "=", members[1].Raw.ModRevision),
	})
	// And then creates empty Assignments for each, using the Member LeaseID.
	c.Check(txn.ops, gc.DeepEquals, []clientv3.Op{
		clientv3.OpPut("/root/assign/item#zone#member-0#0", "", clientv3.WithLease(0xfeed)),
		clientv3.OpPut("/root/assign/item#zone#member-1#1", "", clientv3.WithLease(0xbeef)),
	})

	// Expect Member counts were updated to reflect the addition of total & primary replicas.
	c.Check(is.global.MemberPrimaryCount, gc.DeepEquals, []int{1, 0})
	c.Check(is.global.MemberTotalCount, gc.DeepEquals, []int{1, 1})
}

func (s *ItemStateSuite) TestBuildPackOps(c *gc.C) {
	var ks, is = buildItemStateFixture(c, map[string]string{
		"/root/items/item": `{"R": 4}`,

		"/root/members/zone#member-0": `{"R": 1}`,
		"/root/members/zone#member-1": `{"R": 1}`,
		"/root/members/zone#member-3": `{"R": 1}`,
		"/root/members/zone#member-5": `{"R": 1}`,

		"/root/assign/item#zone#member-0#0": ``,
		"/root/assign/item#zone#member-1#1": ``,
		"/root/assign/item#zone#member-3#3": `consistent`,
		"/root/assign/item#zone#member-5#5": `consistent`,
	})
	var current = ks.Prefixed(ItemAssignmentsPrefix(ks, "item"))

	// Tweak fixture to model an Etcd lease on member-3 Assignments.
	c.Check(current, gc.HasLen, 4)
	current[2].Raw.Lease = 0xfeedbeef

	is.init(0, current, []Assignment{
		{ItemID: "item", MemberZone: "zone", MemberSuffix: "member-0"},
		{ItemID: "item", MemberZone: "zone", MemberSuffix: "member-1"},
		{ItemID: "item", MemberZone: "zone", MemberSuffix: "member-3"},
		{ItemID: "item", MemberZone: "zone", MemberSuffix: "member-5"},
	})
	var txn mockTxnBuilder
	is.buildPackOps(&txn)

	// Expect |txn| verifies member-3 Assignment hasn't changed.
	c.Check(txn.cmps, gc.DeepEquals, []clientv3.Cmp{
		clientv3.Compare(clientv3.ModRevision("/root/assign/item#zone#member-3#3"), "=", current[2].Raw.ModRevision),
	})
	// And then atomically moves the Assignment to slot 2, preserving its current value & LeaseID.
	c.Check(txn.ops, gc.DeepEquals, []clientv3.Op{
		clientv3.OpDelete("/root/assign/item#zone#member-3#3"),
		clientv3.OpPut("/root/assign/item#zone#member-3#2", "consistent", clientv3.WithLease(0xfeedbeef)),
	})

	// Expect counts are unchanged.
	c.Check(is.global.MemberPrimaryCount, gc.DeepEquals, []int{1, 0, 0, 0})
	c.Check(is.global.MemberTotalCount, gc.DeepEquals, []int{1, 1, 1, 1})
}

func buildItemStateFixture(c *gc.C, fixture map[string]string) (*keyspace.KeySpace, *itemState) {
	var client, ctx = etcdtest.TestClient(), context.Background()
	defer etcdtest.Cleanup()

	for k, v := range fixture {
		var _, err = client.Put(ctx, k, v)
		c.Assert(err, gc.IsNil)
	}

	var ks = NewAllocatorKeySpace("/root", testAllocDecoder{})
	var as = NewObservedState(ks, MemberKey(ks, "zone", "member-1"), isConsistent)
	c.Check(ks.Load(ctx, client, 0), gc.IsNil)

	return ks, &itemState{global: as}
}

type mockTxnBuilder struct {
	cmps []clientv3.Cmp
	ops  []clientv3.Op
}

func (b *mockTxnBuilder) If(c ...clientv3.Cmp) checkpointTxn  { b.cmps = append(b.cmps, c...); return b }
func (b *mockTxnBuilder) Then(o ...clientv3.Op) checkpointTxn { b.ops = append(b.ops, o...); return b }
func (b *mockTxnBuilder) Checkpoint() error                   { return nil }
func (b *mockTxnBuilder) Flush() error                        { panic("not supported") }
func (b *mockTxnBuilder) Revision() int64                     { panic("not supported") }

var _ = gc.Suite(&ItemStateSuite{})
