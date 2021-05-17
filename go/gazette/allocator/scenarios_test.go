package allocator

import (
	"context"

	gc "github.com/go-check/check"
	"go.etcd.io/etcd/client/v3"
	"go.gazette.dev/core/etcdtest"
	"go.gazette.dev/core/keyspace"
)

type ScenariosSuite struct {
	client *clientv3.Client
	ctx    context.Context
	ks     *keyspace.KeySpace
}

func (s *ScenariosSuite) SetUpSuite(c *gc.C) {
	s.client = etcdtest.TestClient()
	s.ctx = context.Background()
	s.ks = NewAllocatorKeySpace("/root", testAllocDecoder{})
}

func (s *ScenariosSuite) TearDownSuite(c *gc.C) { etcdtest.Cleanup() }

func (s *ScenariosSuite) SetUpTest(c *gc.C) {
	var _, err = s.client.Delete(s.ctx, "", clientv3.WithPrefix())
	c.Assert(err, gc.IsNil)
}

func (s *ScenariosSuite) TestInitialAllocation(c *gc.C) {
	c.Check(insert(s.ctx, s.client,
		"/root/items/item-1", `{"R": 1}`,
		"/root/items/item-2", `{"R": 2}`,
		"/root/items/item-3", `{"R": 3}`,

		"/root/members/zone-a#member-A1", `{"R": 2}`,
		"/root/members/zone-a#member-A2", `{"R": 2}`,
		"/root/members/zone-b#member-B", `{"R": 4}`,
	), gc.IsNil)
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 1)

	// Expect Items are fully replicated, each Item spans both zones, and no Member ItemLimit is breached.
	c.Check(keys(s.ks.Prefixed(s.ks.Root+AssignmentsPrefix)), gc.DeepEquals, []string{
		"/root/assign/item-1#zone-a#member-A1#0",
		"/root/assign/item-2#zone-a#member-A2#0",
		"/root/assign/item-2#zone-b#member-B#1",
		"/root/assign/item-3#zone-a#member-A1#0",
		"/root/assign/item-3#zone-a#member-A2#1",
		"/root/assign/item-3#zone-b#member-B#2",
	})
}

func (s *ScenariosSuite) TestInitialAllocationRegressionIssue157(c *gc.C) {
	c.Check(insert(s.ctx, s.client, ""+
		"/root/items/item-01", `{"R": 3}`,
		"/root/items/item-02", `{"R": 3}`,
		"/root/items/item-03", `{"R": 3}`,
		"/root/items/item-04", `{"R": 3}`,
		"/root/items/item-05", `{"R": 3}`,
		"/root/items/item-06", `{"R": 3}`,
		"/root/items/item-07", `{"R": 3}`,

		"/root/members/zone-a#member-A1", `{"R": 1024}`,
		"/root/members/zone-a#member-A2", `{"R": 1024}`,
		"/root/members/zone-b#member-B1", `{"R": 1024}`,
	), gc.IsNil)
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 1)

	// Expect Items are fully replicated, each Item spans both zones, and no Member ItemLimit is breached.
	c.Check(keys(s.ks.Prefixed(s.ks.Root+AssignmentsPrefix)), gc.DeepEquals, []string{
		"/root/assign/item-01#zone-a#member-A1#0",
		"/root/assign/item-01#zone-a#member-A2#1",
		"/root/assign/item-01#zone-b#member-B1#2",

		"/root/assign/item-02#zone-a#member-A1#0",
		"/root/assign/item-02#zone-a#member-A2#1",
		"/root/assign/item-02#zone-b#member-B1#2",

		"/root/assign/item-03#zone-a#member-A1#0",
		"/root/assign/item-03#zone-a#member-A2#1",
		"/root/assign/item-03#zone-b#member-B1#2",

		"/root/assign/item-04#zone-a#member-A1#0",
		"/root/assign/item-04#zone-a#member-A2#1",
		"/root/assign/item-04#zone-b#member-B1#2",

		"/root/assign/item-05#zone-a#member-A1#0",
		"/root/assign/item-05#zone-a#member-A2#1",
		"/root/assign/item-05#zone-b#member-B1#2",

		"/root/assign/item-06#zone-a#member-A1#0",
		"/root/assign/item-06#zone-a#member-A2#1",
		"/root/assign/item-06#zone-b#member-B1#2",

		"/root/assign/item-07#zone-a#member-A1#0",
		"/root/assign/item-07#zone-a#member-A2#1",
		"/root/assign/item-07#zone-b#member-B1#2",
	})
}

func (s *ScenariosSuite) TestReplaceWhenNotConsistent(c *gc.C) {
	c.Check(insert(s.ctx, s.client,
		"/root/items/item-1", `{"R": 1}`,
		"/root/items/item-2", `{"R": 2}`,
		"/root/items/item-3", `{"R": 3}`,

		"/root/members/zone-a#member-A", `{"R": 2}`,
		"/root/members/zone-b#member-B", `{"R": 4}`,

		"/root/members/zone-a#member-old", `{"R": 0}`,
		"/root/members/zone-a#member-new", `{"R": 3}`,

		"/root/assign/item-1#zone-a#member-A#0", ``,
		"/root/assign/item-2#zone-a#member-old#0", ``,
		"/root/assign/item-2#zone-b#member-B#1", ``,
		"/root/assign/item-3#zone-a#member-A#0", ``,
		"/root/assign/item-3#zone-a#member-old#1", ``,
		"/root/assign/item-3#zone-b#member-B#2", ``,
	), gc.IsNil)

	// As no Assignments are consistent, the scheduler may over-commit
	// Items but cannot remove any current slots.
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 1)

	// Expect member-new has Assignments which will (eventually) allow Assignments
	// of member-old to be removed.
	c.Check(keys(s.ks.Prefixed(s.ks.Root+AssignmentsPrefix)), gc.DeepEquals, []string{
		"/root/assign/item-1#zone-a#member-A#0",
		"/root/assign/item-2#zone-a#member-new#2",
		"/root/assign/item-2#zone-a#member-old#0",
		"/root/assign/item-2#zone-b#member-B#1",
		"/root/assign/item-3#zone-a#member-A#0",
		"/root/assign/item-3#zone-a#member-new#3",
		"/root/assign/item-3#zone-a#member-old#1",
		"/root/assign/item-3#zone-b#member-B#2",
	})

	c.Check(markAllConsistent(s.ctx, s.client, s.ks), gc.IsNil)
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 3)

	// Expect member-old is removed, and slot indices are compacted.
	c.Check(keys(s.ks.Prefixed(s.ks.Root+AssignmentsPrefix)), gc.DeepEquals, []string{
		"/root/assign/item-1#zone-a#member-A#0",
		"/root/assign/item-2#zone-a#member-new#1",
		"/root/assign/item-2#zone-b#member-B#0",
		"/root/assign/item-3#zone-a#member-A#0",
		"/root/assign/item-3#zone-a#member-new#2",
		"/root/assign/item-3#zone-b#member-B#1",
	})
}

func (s *ScenariosSuite) TestReplaceWhenConsistent(c *gc.C) {
	c.Check(insert(s.ctx, s.client,
		"/root/items/item-1", `{"R": 1}`,
		"/root/items/item-2", `{"R": 2}`,
		"/root/items/item-3", `{"R": 3}`,

		"/root/members/zone-a#member-A", `{"R": 2}`,
		"/root/members/zone-b#member-B", `{"R": 4}`,

		"/root/members/zone-a#member-old", `{"R": 0}`,
		"/root/members/zone-a#member-new", `{"R": 3}`,

		"/root/assign/item-1#zone-a#member-A#0", `consistent`,
		"/root/assign/item-2#zone-a#member-old#0", `consistent`,
		"/root/assign/item-2#zone-b#member-B#1", `consistent`,
		"/root/assign/item-3#zone-a#member-old#0", `consistent`,
		"/root/assign/item-3#zone-a#member-A#1", `consistent`,
		"/root/assign/item-3#zone-b#member-B#2", `consistent`,
	), gc.IsNil)
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 1)

	// Member-old has not been removed. Member-new is preparing to take over.
	c.Check(keys(s.ks.Prefixed(s.ks.Root+AssignmentsPrefix)), gc.DeepEquals, []string{
		"/root/assign/item-1#zone-a#member-A#0",
		"/root/assign/item-2#zone-a#member-new#2", // Added.
		"/root/assign/item-2#zone-a#member-old#0",
		"/root/assign/item-2#zone-b#member-B#1",
		"/root/assign/item-3#zone-a#member-A#1",
		"/root/assign/item-3#zone-a#member-new#3", // Added.
		"/root/assign/item-3#zone-a#member-old#0",
		"/root/assign/item-3#zone-b#member-B#2",
	})

	// Mark member-new's Assignments as consistent.
	c.Check(markAllConsistent(s.ctx, s.client, s.ks), gc.IsNil)
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 2)

	// Expect member-old was removed. Member-new was promoted to item-3 primary
	// (eg, each Member has one primary).
	c.Check(keys(s.ks.Prefixed(s.ks.Root+AssignmentsPrefix)), gc.DeepEquals, []string{
		"/root/assign/item-1#zone-a#member-A#0",
		"/root/assign/item-2#zone-a#member-new#1",
		"/root/assign/item-2#zone-b#member-B#0",
		"/root/assign/item-3#zone-a#member-A#1",
		"/root/assign/item-3#zone-a#member-new#0",
		"/root/assign/item-3#zone-b#member-B#2",
	})
}

func (s *ScenariosSuite) TestUpdateItemLimit(c *gc.C) {
	c.Check(insert(s.ctx, s.client,
		"/root/items/item-1", `{"R": 1}`,
		"/root/items/item-2", `{"R": 2}`,
		"/root/items/item-3", `{"R": 3}`,

		"/root/members/zone-a#member-A", `{"R": 2}`,
		"/root/members/zone-a#member-limit", `{"R": 2}`,
		"/root/members/zone-b#member-B", `{"R": 4}`,

		"/root/assign/item-1#zone-b#member-B#0", `consistent`,
		"/root/assign/item-2#zone-a#member-A#0", `consistent`,
		"/root/assign/item-2#zone-b#member-B#1", `consistent`,
		"/root/assign/item-3#zone-a#member-A#0", `consistent`,
		"/root/assign/item-3#zone-a#member-limit#1", `consistent`,
		"/root/assign/item-3#zone-b#member-B#2", `consistent`,
	), gc.IsNil)
	// Expect current solution is stable.
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 0)

	// Increase member-limit's limit.
	c.Check(update(s.ctx, s.client,
		"/root/members/zone-a#member-limit", `{"R": 10}`), gc.IsNil)
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 1)

	// Expect the scheduler begins to allocate more load to member-limit.
	c.Check(keys(s.ks.Prefixed(s.ks.Root+AssignmentsPrefix)), gc.DeepEquals, []string{
		"/root/assign/item-1#zone-a#member-limit#1", // Acquired.
		"/root/assign/item-1#zone-b#member-B#0",
		"/root/assign/item-2#zone-a#member-A#0",
		"/root/assign/item-2#zone-a#member-limit#2", // Acquired.
		"/root/assign/item-2#zone-b#member-B#1",
		"/root/assign/item-3#zone-a#member-A#0",
		"/root/assign/item-3#zone-a#member-limit#1",
		"/root/assign/item-3#zone-b#member-B#2",
	})

	// Mark the new Assignment as consistent.
	c.Check(markAllConsistent(s.ctx, s.client, s.ks), gc.IsNil)
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 2)

	// Expect member-limit has three Assignments, to the other Member's two/one.
	// Each member is assigned one primary.
	c.Check(keys(s.ks.Prefixed(s.ks.Root+AssignmentsPrefix)), gc.DeepEquals, []string{
		"/root/assign/item-1#zone-a#member-limit#0",
		"/root/assign/item-2#zone-a#member-limit#1",
		"/root/assign/item-2#zone-b#member-B#0",
		"/root/assign/item-3#zone-a#member-A#0",
		"/root/assign/item-3#zone-a#member-limit#1",
		"/root/assign/item-3#zone-b#member-B#2",
	})

	// Restore member-limit's limit.
	c.Check(update(s.ctx, s.client,
		"/root/members/zone-a#member-limit", `{"R": 2}`), gc.IsNil)
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 1)

	// Both member-limit and member-A have scaled R: 2. Where the initial fixture
	// had member-B with three items, now the allocator lazily leaves two items
	// each with member-limit & member-A.
	c.Check(keys(s.ks.Prefixed(s.ks.Root+AssignmentsPrefix)), gc.DeepEquals, []string{
		"/root/assign/item-1#zone-a#member-limit#0",
		"/root/assign/item-2#zone-a#member-A#2", // Acquired.
		"/root/assign/item-2#zone-a#member-limit#1",
		"/root/assign/item-2#zone-b#member-B#0",
		"/root/assign/item-3#zone-a#member-A#0",
		"/root/assign/item-3#zone-a#member-limit#1",
		"/root/assign/item-3#zone-b#member-B#2",
	})

	// Mark member-A's new Assignment as consistent.
	c.Check(markAllConsistent(s.ctx, s.client, s.ks), gc.IsNil)
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 2)

	c.Check(keys(s.ks.Prefixed(s.ks.Root+AssignmentsPrefix)), gc.DeepEquals, []string{
		"/root/assign/item-1#zone-a#member-limit#0",
		"/root/assign/item-2#zone-a#member-A#1",
		"/root/assign/item-2#zone-b#member-B#0",
		"/root/assign/item-3#zone-a#member-A#0",
		"/root/assign/item-3#zone-a#member-limit#1",
		"/root/assign/item-3#zone-b#member-B#2",
	})

	// Lower member-limit's limit to 1.
	c.Check(update(s.ctx, s.client,
		"/root/members/zone-a#member-limit", `{"R": 1}`), gc.IsNil)
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 1)

	c.Check(keys(s.ks.Prefixed(s.ks.Root+AssignmentsPrefix)), gc.DeepEquals, []string{
		"/root/assign/item-1#zone-a#member-limit#0",
		"/root/assign/item-1#zone-b#member-B#1", // Acquired.
		"/root/assign/item-2#zone-a#member-A#1",
		"/root/assign/item-2#zone-b#member-B#0",
		"/root/assign/item-3#zone-a#member-A#0",
		"/root/assign/item-3#zone-a#member-limit#1",
		"/root/assign/item-3#zone-b#member-B#2",
	})

	c.Check(markAllConsistent(s.ctx, s.client, s.ks), gc.IsNil)
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 1)

	c.Check(keys(s.ks.Prefixed(s.ks.Root+AssignmentsPrefix)), gc.DeepEquals, []string{
		"/root/assign/item-1#zone-b#member-B#0",
		"/root/assign/item-2#zone-a#member-A#1",
		"/root/assign/item-2#zone-b#member-B#0",
		"/root/assign/item-3#zone-a#member-A#0",
		"/root/assign/item-3#zone-a#member-limit#1",
		"/root/assign/item-3#zone-b#member-B#2",
	})

	c.Check(markAllConsistent(s.ctx, s.client, s.ks), gc.IsNil)
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 0) // Nothing to do.
}

func (s *ScenariosSuite) TestUpdateDesiredReplication(c *gc.C) {
	c.Check(insert(s.ctx, s.client,
		"/root/items/item-1", `{"R": 1}`,
		"/root/items/item-2", `{"R": 2}`,
		"/root/items/item-3", `{"R": 3}`,

		"/root/members/zone-a#member-A1", `{"R": 2}`,
		"/root/members/zone-a#member-A2", `{"R": 2}`,
		"/root/members/zone-b#member-B", `{"R": 4}`,

		"/root/assign/item-1#zone-b#member-B#0", `consistent`,
		"/root/assign/item-2#zone-a#member-A1#0", `consistent`,
		"/root/assign/item-2#zone-b#member-B#1", `consistent`,
		"/root/assign/item-3#zone-a#member-A1#0", `consistent`,
		"/root/assign/item-3#zone-a#member-A2#1", `consistent`,
		"/root/assign/item-3#zone-b#member-B#2", `consistent`,
	), gc.IsNil)
	// Expect current solution is stable.
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 0)

	// Swap the desired replication of item-1 & item-3.
	c.Check(update(s.ctx, s.client,
		"/root/items/item-1", `{"R": 3}`,
		"/root/items/item-3", `{"R": 1}`), gc.IsNil)
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 4)

	c.Check(keys(s.ks.Prefixed(s.ks.Root+AssignmentsPrefix)), gc.DeepEquals, []string{
		"/root/assign/item-1#zone-a#member-A1#2", // Acquired.
		"/root/assign/item-1#zone-a#member-A2#1", // Acquired.
		"/root/assign/item-1#zone-b#member-B#0",
		"/root/assign/item-2#zone-a#member-A1#0",
		"/root/assign/item-2#zone-b#member-B#1",
		"/root/assign/item-3#zone-a#member-A2#0",
	})

	// Mark Assignments as consistent. No further changes are required.
	c.Check(markAllConsistent(s.ctx, s.client, s.ks), gc.IsNil)
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 0)
}

func (s *ScenariosSuite) TestScaleUpFromInsufficientMemberSlots(c *gc.C) {
	// Create a fixture with more Item slots than Member slots.
	c.Check(insert(s.ctx, s.client,
		"/root/items/item-1", `{"R": 1}`,
		"/root/items/item-2", `{"R": 2}`,
		"/root/items/item-3", `{"R": 3}`,

		"/root/members/zone-a#member-A1", `{"R": 1}`,
	), gc.IsNil)
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 1)

	// Expect only item-1 is allocated.
	c.Check(keys(s.ks.Prefixed(s.ks.Root+AssignmentsPrefix)), gc.DeepEquals, []string{
		"/root/assign/item-1#zone-a#member-A1#0",
	})
	// Add a new member. Still under-provisioned.
	c.Check(insert(s.ctx, s.client, "/root/members/zone-a#member-A2", `{"R": 2}`), gc.IsNil)

	// Expect item-3 is not attempted to be allocated, and item-1 is handed off to member-A2.
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 2)
	c.Check(markAllConsistent(s.ctx, s.client, s.ks), gc.IsNil)
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 1)

	c.Check(keys(s.ks.Prefixed(s.ks.Root+AssignmentsPrefix)), gc.DeepEquals, []string{
		"/root/assign/item-1#zone-a#member-A2#0",
		"/root/assign/item-2#zone-a#member-A1#1",
		"/root/assign/item-2#zone-a#member-A2#0",
	})

	// Add a new member, such that we're fully provisioned.
	c.Check(insert(s.ctx, s.client, "/root/members/zone-a#member-A3", `{"R": 3}`), gc.IsNil)

	// Expect item-3 is allocated, but first item-1 is rotated to A3, item-2 from A1 => A2, and item-3 => A1.
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 2)
	c.Check(markAllConsistent(s.ctx, s.client, s.ks), gc.IsNil)
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 2)

	c.Check(keys(s.ks.Prefixed(s.ks.Root+AssignmentsPrefix)), gc.DeepEquals, []string{
		"/root/assign/item-1#zone-a#member-A3#0",
		"/root/assign/item-2#zone-a#member-A2#0",
		"/root/assign/item-2#zone-a#member-A3#1",
		"/root/assign/item-3#zone-a#member-A1#1",
		"/root/assign/item-3#zone-a#member-A2#2",
		"/root/assign/item-3#zone-a#member-A3#0",
	})
}

func (s *ScenariosSuite) TestScaleDownToInsufficientMemberSlots(c *gc.C) {
	c.Check(insert(s.ctx, s.client,
		"/root/items/item-1", `{"R": 1}`,
		"/root/items/item-2", `{"R": 2}`,
		"/root/items/item-3", `{"R": 3}`,

		"/root/members/zone-a#member-A1", `{"R": 1}`,
		"/root/members/zone-a#member-A2", `{"R": 2}`,
		"/root/members/zone-a#member-A3", `{"R": 3}`,

		"/root/assign/item-1#zone-a#member-A3#0", `consistent`,
		"/root/assign/item-2#zone-a#member-A2#0", `consistent`,
		"/root/assign/item-2#zone-a#member-A3#1", `consistent`,
		"/root/assign/item-3#zone-a#member-A1#1", `consistent`,
		"/root/assign/item-3#zone-a#member-A2#2", `consistent`,
		"/root/assign/item-3#zone-a#member-A3#0", `consistent`,
	), gc.IsNil)
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 0)

	// Reduce A3's ItemLimit to two.
	c.Check(update(s.ctx, s.client, "/root/members/zone-a#member-A3", `{"R": 2}`), gc.IsNil)

	// Expect no changes occur, as it would violate item consistency.
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 0)

	c.Check(keys(s.ks.Prefixed(s.ks.Root+AssignmentsPrefix)), gc.DeepEquals, []string{
		"/root/assign/item-1#zone-a#member-A3#0",
		"/root/assign/item-2#zone-a#member-A2#0",
		"/root/assign/item-2#zone-a#member-A3#1",
		"/root/assign/item-3#zone-a#member-A1#1",
		"/root/assign/item-3#zone-a#member-A2#2",
		"/root/assign/item-3#zone-a#member-A3#0",
	})
}

func (s *ScenariosSuite) TestScaleUpZonesOneToTwo(c *gc.C) {
	// Create a fixture with two zones, one large and one too small.
	c.Check(insert(s.ctx, s.client,
		"/root/items/item-1", `{"R": 3}`,
		"/root/items/item-2", `{"R": 3}`,
		"/root/items/item-3", `{"R": 3}`,

		"/root/members/zone-a#member-A1", `{"R": 100}`,
		"/root/members/zone-a#member-A2", `{"R": 100}`,
		"/root/members/zone-a#member-A3", `{"R": 100}`,
		"/root/members/zone-b#member-B", `{"R": 2}`,
	), gc.IsNil)

	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 1)
	c.Check(markAllConsistent(s.ctx, s.client, s.ks), gc.IsNil)

	// Expect member-B is fully utilized, though its capacity is dwarfed
	// by other members. Also expect one item is replicated only within
	// a single zone.
	c.Check(keys(s.ks.Prefixed(s.ks.Root+AssignmentsPrefix)), gc.DeepEquals, []string{
		"/root/assign/item-1#zone-a#member-A1#0",
		"/root/assign/item-1#zone-a#member-A2#1",
		"/root/assign/item-1#zone-a#member-A3#2",
		"/root/assign/item-2#zone-a#member-A2#0",
		"/root/assign/item-2#zone-a#member-A3#1",
		"/root/assign/item-2#zone-b#member-B#2",
		"/root/assign/item-3#zone-a#member-A1#0",
		"/root/assign/item-3#zone-a#member-A2#1",
		"/root/assign/item-3#zone-b#member-B#2",
	})

	// Add one slot. It's now possible to place all items across two zones.
	c.Check(update(s.ctx, s.client, "/root/members/zone-b#member-B", `{"R": 3}`), gc.IsNil)

	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 1)
	c.Check(markAllConsistent(s.ctx, s.client, s.ks), gc.IsNil)
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 3)
	c.Check(markAllConsistent(s.ctx, s.client, s.ks), gc.IsNil)

	// Expect all items are now replicated across both zones.
	c.Check(keys(s.ks.Prefixed(s.ks.Root+AssignmentsPrefix)), gc.DeepEquals, []string{
		"/root/assign/item-1#zone-a#member-A1#0",
		"/root/assign/item-1#zone-a#member-A3#1",
		"/root/assign/item-1#zone-b#member-B#2",
		"/root/assign/item-2#zone-a#member-A2#0",
		"/root/assign/item-2#zone-a#member-A3#1",
		"/root/assign/item-2#zone-b#member-B#2",
		"/root/assign/item-3#zone-a#member-A1#0",
		"/root/assign/item-3#zone-a#member-A2#1",
		"/root/assign/item-3#zone-b#member-B#2",
	})

	// Add a new R:1 item. Able to be placed without upsetting zone constraints.
	c.Check(insert(s.ctx, s.client, "/root/items/item-4", `{"R": 1}`), gc.IsNil)

	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 1)
	c.Check(markAllConsistent(s.ctx, s.client, s.ks), gc.IsNil)
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 0)

	c.Check(keys(s.ks.Prefixed(s.ks.Root+AssignmentsPrefix)), gc.DeepEquals, []string{
		"/root/assign/item-1#zone-a#member-A1#0",
		"/root/assign/item-1#zone-a#member-A3#1",
		"/root/assign/item-1#zone-b#member-B#2",
		"/root/assign/item-2#zone-a#member-A2#0",
		"/root/assign/item-2#zone-a#member-A3#1",
		"/root/assign/item-2#zone-b#member-B#2",
		"/root/assign/item-3#zone-a#member-A1#0",
		"/root/assign/item-3#zone-a#member-A2#1",
		"/root/assign/item-3#zone-b#member-B#2",
		"/root/assign/item-4#zone-a#member-A1#0", // Added.
	})
}

func (s *ScenariosSuite) TestScaleDownZonesTwoToOne(c *gc.C) {
	// Create a fixture with two zones, with items balanced across them.
	c.Check(insert(s.ctx, s.client,
		"/root/items/item-1", `{"R": 3}`,
		"/root/items/item-2", `{"R": 3}`,
		"/root/items/item-3", `{"R": 2}`,

		"/root/members/zone-a#member-A1", `{"R": 10}`,
		"/root/members/zone-a#member-A2", `{"R": 10}`,
		"/root/members/zone-b#member-B", `{"R": 10}`,

		"/root/assign/item-1#zone-a#member-A1#0", `consistent`,
		"/root/assign/item-1#zone-a#member-A2#1", `consistent`,
		"/root/assign/item-1#zone-b#member-B#2", `consistent`,
		"/root/assign/item-2#zone-a#member-A1#0", `consistent`,
		"/root/assign/item-2#zone-a#member-A2#1", `consistent`,
		"/root/assign/item-2#zone-b#member-B#2", `consistent`,
		"/root/assign/item-3#zone-a#member-A1#0", `consistent`,
		"/root/assign/item-3#zone-b#member-B#1", `consistent`,
	), gc.IsNil)

	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 0) // Fixture is stable.

	// Lower total slots of zone-B.
	c.Check(update(s.ctx, s.client, "/root/members/zone-b#member-B", `{"R": 2}`), gc.IsNil)

	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 1)
	c.Check(markAllConsistent(s.ctx, s.client, s.ks), gc.IsNil)
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 2)

	// Expect items are re-balanced and zone-B is well-utilized,
	// but that multi-zone replication is relaxed.
	c.Check(keys(s.ks.Prefixed(s.ks.Root+AssignmentsPrefix)), gc.DeepEquals, []string{
		"/root/assign/item-1#zone-a#member-A1#0",
		"/root/assign/item-1#zone-a#member-A2#1",
		"/root/assign/item-1#zone-b#member-B#2",
		"/root/assign/item-2#zone-a#member-A1#0",
		"/root/assign/item-2#zone-a#member-A2#1",
		"/root/assign/item-2#zone-b#member-B#2",
		"/root/assign/item-3#zone-a#member-A1#0", // <- Same-zone replication.
		"/root/assign/item-3#zone-a#member-A2#1", // <-
	})

	// Eliminate zone-B, add new zone-A member.
	c.Check(update(s.ctx, s.client, "/root/members/zone-b#member-B", `{"R": 0}`), gc.IsNil)
	c.Check(insert(s.ctx, s.client, "/root/members/zone-a#member-A3", `{"R": 10}`), gc.IsNil)

	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 1)
	c.Check(markAllConsistent(s.ctx, s.client, s.ks), gc.IsNil)
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 2)

	// All items served from zone-A.
	c.Check(keys(s.ks.Prefixed(s.ks.Root+AssignmentsPrefix)), gc.DeepEquals, []string{
		"/root/assign/item-1#zone-a#member-A1#0",
		"/root/assign/item-1#zone-a#member-A2#1",
		"/root/assign/item-1#zone-a#member-A3#2",
		"/root/assign/item-2#zone-a#member-A1#0",
		"/root/assign/item-2#zone-a#member-A2#1",
		"/root/assign/item-2#zone-a#member-A3#2",
		"/root/assign/item-3#zone-a#member-A1#0",
		"/root/assign/item-3#zone-a#member-A2#1",
	})
}

func (s *ScenariosSuite) TestScaleUpZonesTwoToThree(c *gc.C) {
	// Create an initial fixture with two zones.
	c.Check(insert(s.ctx, s.client,
		"/root/items/item-1", `{"R": 2}`,
		"/root/items/item-2", `{"R": 3}`,

		"/root/members/zone-a#member-A1", `{"R": 100}`,
		"/root/members/zone-a#member-A2", `{"R": 100}`,
		"/root/members/zone-b#member-B", `{"R": 100}`,

		"/root/assign/item-1#zone-a#member-A1#0", `consistent`,
		"/root/assign/item-1#zone-b#member-B#1", `consistent`,
		"/root/assign/item-2#zone-a#member-A1#0", `consistent`,
		"/root/assign/item-2#zone-a#member-A2#1", `consistent`,
		"/root/assign/item-2#zone-b#member-B#2", `consistent`,
	), gc.IsNil)

	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 0) // Fixture is stable.

	// Insert member-C with two slots. We expect no changes occur, because the
	// zone is not needed to meet zone constraints, member scaling allows two
	// replicas per member, and we'd rather keep the current solution.
	c.Check(insert(s.ctx, s.client, "/root/members/zone-c#member-C", `{"R": 100}`), gc.IsNil)
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 0)

	// Increase the replication factor of items.
	c.Check(update(s.ctx, s.client, "/root/items/item-1", `{"R": 3}`), gc.IsNil)
	c.Check(update(s.ctx, s.client, "/root/items/item-2", `{"R": 4}`), gc.IsNil)

	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 1)
	c.Check(markAllConsistent(s.ctx, s.client, s.ks), gc.IsNil)
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 0)

	// Expect the new slots were given to member-C, though member-A1 also had
	// scaled capacity, due to the even zone balancing network optimization goal.
	c.Check(keys(s.ks.Prefixed(s.ks.Root+AssignmentsPrefix)), gc.DeepEquals, []string{
		"/root/assign/item-1#zone-a#member-A1#0",
		"/root/assign/item-1#zone-b#member-B#1",
		"/root/assign/item-1#zone-c#member-C#2", // Added.
		"/root/assign/item-2#zone-a#member-A1#0",
		"/root/assign/item-2#zone-a#member-A2#1",
		"/root/assign/item-2#zone-b#member-B#2",
		"/root/assign/item-2#zone-c#member-C#3", // Added.
	})
}

func (s *ScenariosSuite) TestScaleDownZonesThreeToTwo(c *gc.C) {
	c.Check(insert(s.ctx, s.client,
		"/root/items/item-1", `{"R": 3}`,
		"/root/items/item-2", `{"R": 3}`,

		"/root/members/zone-a#member-A1", `{"R": 10}`,
		"/root/members/zone-a#member-A2", `{"R": 10}`,
		"/root/members/zone-a#member-A3", `{"R": 10}`,
		"/root/members/zone-b#member-B", `{"R": 10}`,
		"/root/members/zone-c#member-C", `{"R": 10}`,

		"/root/assign/item-1#zone-a#member-A1#0", `consistent`,
		"/root/assign/item-1#zone-a#member-A2#1", `consistent`,
		"/root/assign/item-1#zone-c#member-C#2", `consistent`,
		"/root/assign/item-2#zone-a#member-A3#0", `consistent`,
		"/root/assign/item-2#zone-b#member-B#1", `consistent`,
		"/root/assign/item-2#zone-c#member-C#2", `consistent`,
	), gc.IsNil)

	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 0) // Fixture is stable.

	// Drop slots of member-C. Expect Items are re-balanced across two remaining zones.
	c.Check(update(s.ctx, s.client, "/root/members/zone-c#member-C", `{"R": 0}`), gc.IsNil)

	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 1)
	c.Check(markAllConsistent(s.ctx, s.client, s.ks), gc.IsNil)
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 2)

	c.Check(keys(s.ks.Prefixed(s.ks.Root+AssignmentsPrefix)), gc.DeepEquals, []string{
		"/root/assign/item-1#zone-a#member-A1#0",
		"/root/assign/item-1#zone-a#member-A2#1",
		"/root/assign/item-1#zone-b#member-B#2",
		"/root/assign/item-2#zone-a#member-A2#2",
		"/root/assign/item-2#zone-a#member-A3#0",
		"/root/assign/item-2#zone-b#member-B#1",
	})
}

func (s *ScenariosSuite) TestUnbalancedZoneAndMemberCapacityRotations(c *gc.C) {
	c.Check(insert(s.ctx, s.client,
		"/root/items/item-1", `{"R": 3}`,
		"/root/items/item-2", `{"R": 3}`,
		"/root/items/item-3", `{"R": 3}`,

		"/root/members/zone-a#member-A1", `{"R": 1}`,
		"/root/members/zone-a#member-A2", `{"R": 3}`,
		"/root/members/zone-b#member-B1", `{"R": 10}`,
		"/root/members/zone-b#member-B2", `{"R": 10}`,

		// Begin with a valid assignment fixture, balanced across zones.
		"/root/assign/item-1#zone-a#member-A1#0", `consistent`,
		"/root/assign/item-1#zone-b#member-B1#1", `consistent`,
		"/root/assign/item-1#zone-b#member-B2#2", `consistent`,
		"/root/assign/item-2#zone-a#member-A2#0", `consistent`,
		"/root/assign/item-2#zone-b#member-B1#1", `consistent`,
		"/root/assign/item-2#zone-b#member-B2#2", `consistent`,
		"/root/assign/item-3#zone-a#member-A2#0", `consistent`,
		"/root/assign/item-3#zone-b#member-B1#1", `consistent`,
		"/root/assign/item-3#zone-b#member-B2#2", `consistent`,
	), gc.IsNil)
	// Expect fixture is stable.
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 0)

	// Swap capacities of zone-A members.
	c.Check(update(s.ctx, s.client,
		"/root/members/zone-a#member-A1", `{"R": 3}`,
		"/root/members/zone-a#member-A2", `{"R": 1}`,
	), gc.IsNil)

	// Expect items are rotated to meet A2's reduced capacity.
	for _, rounds := range []int{1, 3} {
		c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, rounds)
		c.Check(markAllConsistent(s.ctx, s.client, s.ks), gc.IsNil)
	}
	c.Check(keys(s.ks.Prefixed(s.ks.Root+AssignmentsPrefix)), gc.DeepEquals, []string{
		"/root/assign/item-1#zone-a#member-A1#0",
		"/root/assign/item-1#zone-b#member-B1#1",
		"/root/assign/item-1#zone-b#member-B2#2",
		"/root/assign/item-2#zone-a#member-A1#2", // <- Swapped.
		"/root/assign/item-2#zone-b#member-B1#0", // <- Elevated to primary.
		"/root/assign/item-2#zone-b#member-B2#1",
		"/root/assign/item-3#zone-a#member-A2#0",
		"/root/assign/item-3#zone-b#member-B1#1",
		"/root/assign/item-3#zone-b#member-B2#2",
	})

	// Swap capacities of zones A & B.
	c.Check(update(s.ctx, s.client,
		"/root/members/zone-a#member-A1", `{"R": 10}`,
		"/root/members/zone-a#member-A2", `{"R": 10}`,
		"/root/members/zone-b#member-B1", `{"R": 1}`,
		"/root/members/zone-b#member-B2", `{"R": 3}`,
	), gc.IsNil)

	// Expect items are rotated to meet B1's reduced capacity.
	for _, rounds := range []int{1, 3} {
		c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, rounds)
		c.Check(markAllConsistent(s.ctx, s.client, s.ks), gc.IsNil)
	}
	c.Check(keys(s.ks.Prefixed(s.ks.Root+AssignmentsPrefix)), gc.DeepEquals, []string{
		"/root/assign/item-1#zone-a#member-A1#0",
		"/root/assign/item-1#zone-a#member-A2#2", // <- Swapped.
		"/root/assign/item-1#zone-b#member-B2#1",
		"/root/assign/item-2#zone-a#member-A1#1",
		"/root/assign/item-2#zone-a#member-A2#2", // <- Swapped.
		"/root/assign/item-2#zone-b#member-B2#0", // <- Elevated to primary.
		"/root/assign/item-3#zone-a#member-A1#2", // <- Swapped.
		"/root/assign/item-3#zone-a#member-A2#0",
		"/root/assign/item-3#zone-b#member-B1#1",
	})
}

func (s *ScenariosSuite) TestRecoveryOnLeaseAndZoneEviction(c *gc.C) {
	c.Check(insert(s.ctx, s.client,
		"/root/items/item-1", `{"R": 1}`,
		"/root/items/item-2", `{"R": 2}`,
		"/root/items/item-3", `{"R": 3}`,

		"/root/members/zone-a#member-A1", `{"R": 4}`,
		"/root/members/zone-a#member-A2", `{"R": 2}`,
		"/root/members/zone-a#member-A3", `{"R": 1}`,
		"/root/members/zone-b#member-B1", `{"R": 4}`,

		"/root/assign/item-1#zone-a#member-A1#0", `consistent`,
		"/root/assign/item-2#zone-a#member-A2#1", `consistent`,
		"/root/assign/item-2#zone-b#member-B1#0", `consistent`,
		"/root/assign/item-3#zone-a#member-A1#2", `consistent`,
		"/root/assign/item-3#zone-a#member-A3#0", `consistent`,
		"/root/assign/item-3#zone-b#member-B1#1", `consistent`,
	), gc.IsNil)
	// Expect initial two-zone fixture is optimal.
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 0)

	// Member-B1 and its Assignments disappear (eg, simulating lease eviction,
	// and even the loss of an entire zone).
	s.client.Delete(s.ctx, "/root/members/zone-b#member-B1")
	s.client.Delete(s.ctx, "/root/assign/item-2#zone-b#member-B1#0")
	s.client.Delete(s.ctx, "/root/assign/item-3#zone-b#member-B1#1")

	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 3)

	c.Check(keys(s.ks.Prefixed(s.ks.Root+AssignmentsPrefix)), gc.DeepEquals, []string{
		"/root/assign/item-1#zone-a#member-A1#0",
		"/root/assign/item-2#zone-a#member-A1#1",
		"/root/assign/item-2#zone-a#member-A2#0",
		"/root/assign/item-3#zone-a#member-A1#1",
		"/root/assign/item-3#zone-a#member-A2#2",
		"/root/assign/item-3#zone-a#member-A3#0",
	})
}

func (s *ScenariosSuite) TestSingleZoneRebalanceOnMemberScaleUp(c *gc.C) {
	// Initial fixture has one zone, and equal member & item slots.
	c.Check(insert(s.ctx, s.client,
		"/root/items/item-1", `{"R": 1}`,
		"/root/items/item-2", `{"R": 2}`,
		"/root/items/item-3", `{"R": 3}`,

		"/root/members/zone-a#member-A1", `{"R": 3}`,
		"/root/members/zone-a#member-A2", `{"R": 2}`,
		"/root/members/zone-a#member-A3", `{"R": 1}`,
	), gc.IsNil)
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 1)
	c.Check(markAllConsistent(s.ctx, s.client, s.ks), gc.IsNil)

	// Expect Items are fully replicated.
	c.Check(keys(s.ks.Prefixed(s.ks.Root+AssignmentsPrefix)), gc.DeepEquals, []string{
		"/root/assign/item-1#zone-a#member-A1#0",
		"/root/assign/item-2#zone-a#member-A1#0",
		"/root/assign/item-2#zone-a#member-A2#1",
		"/root/assign/item-3#zone-a#member-A1#0",
		"/root/assign/item-3#zone-a#member-A2#1",
		"/root/assign/item-3#zone-a#member-A3#2",
	})

	// Introduce a new member. Expect no changes are made.
	c.Check(insert(s.ctx, s.client,
		"/root/members/zone-a#member-A4", `{"R": 1}`), gc.IsNil)
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 0)

	// Increase its ItemLimit, which causes load to be re-balanced to A4.
	c.Check(update(s.ctx, s.client,
		"/root/members/zone-a#member-A4", `{"R": 3}`), gc.IsNil)

	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 1)
	c.Check(markAllConsistent(s.ctx, s.client, s.ks), gc.IsNil)
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 3)

	c.Check(keys(s.ks.Prefixed(s.ks.Root+AssignmentsPrefix)), gc.DeepEquals, []string{
		"/root/assign/item-1#zone-a#member-A1#0",
		"/root/assign/item-2#zone-a#member-A1#0",
		"/root/assign/item-2#zone-a#member-A2#1",
		"/root/assign/item-3#zone-a#member-A2#0",
		"/root/assign/item-3#zone-a#member-A3#1",
		"/root/assign/item-3#zone-a#member-A4#2",
	})
}

func (s *ScenariosSuite) TestCleanupOfAssignmentsWithoutItems(c *gc.C) {
	c.Check(insert(s.ctx, s.client,
		"/root/items/item-2", `{"R": 1}`,
		"/root/items/item-5", `{"R": 2}`,
		"/root/items/item-6", `{"R": 3}`,

		"/root/members/zone-a#member-A", `{"R": 1}`,
		"/root/members/zone-b#member-B", `{"R": 2}`,
		"/root/members/zone-c#member-C", `{"R": 3}`,

		"/root/assign/item-1#zone-a#member-C#0", `consistent`, // Dead.
		"/root/assign/item-1#zone-b#member-B#1", `consistent`, // Dead.
		"/root/assign/item-2#zone-c#member-C#0", `consistent`, // Live.
		"/root/assign/item-3#zone-a#member-C#0", `consistent`, // Dead.
		"/root/assign/item-4#zone-b#member-B#0", `consistent`, // Dead.
		"/root/assign/item-4#zone-c#member-C#1", `consistent`, // Dead.
		"/root/assign/item-5#zone-b#member-B#1", `consistent`, // Live.
		"/root/assign/item-5#zone-c#member-C#0", `consistent`, // Live.
		"/root/assign/item-6#zone-a#member-A#0", `consistent`, // Live.
		"/root/assign/item-6#zone-b#member-B#1", `consistent`, // Live.
		"/root/assign/item-6#zone-c#member-C#2", `consistent`, // Live.
		"/root/assign/item-7#zone-a#member-A#0", `consistent`, // Dead.
		"/root/assign/item-7#zone-c#member-C#0", `consistent`, // Dead.
	), gc.IsNil)

	// Expect dead Assignments are removed, and remaining live ones are stable.
	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 1)
	c.Check(keys(s.ks.Prefixed(s.ks.Root+AssignmentsPrefix)), gc.DeepEquals, []string{
		"/root/assign/item-2#zone-c#member-C#0",
		"/root/assign/item-5#zone-b#member-B#1",
		"/root/assign/item-5#zone-c#member-C#0",
		"/root/assign/item-6#zone-a#member-A#0",
		"/root/assign/item-6#zone-b#member-B#1",
		"/root/assign/item-6#zone-c#member-C#2",
	})
}

func (s *ScenariosSuite) TestCleanupOfAssignmentsWithoutMember(c *gc.C) {
	c.Check(insert(s.ctx, s.client,
		"/root/items/item-1", `{"R": 1}`,
		"/root/items/item-2", `{"R": 1}`,

		"/root/members/zone-a#member-A", `{"R": 2}`,

		"/root/assign/item-1#zone-f#member-dead#0", `consistent`,
		"/root/assign/item-2#zone-f#member-dead#0", `consistent`,

		"/root/assign/item-1#zone-a#member-A#1", `consistent`,
	), gc.IsNil)

	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 1)
	c.Check(markAllConsistent(s.ctx, s.client, s.ks), gc.IsNil)

	c.Check(keys(s.ks.Prefixed(s.ks.Root+AssignmentsPrefix)), gc.DeepEquals, []string{
		"/root/assign/item-1#zone-a#member-A#0", // Expect member-dead removed, and A promoted.

		"/root/assign/item-2#zone-a#member-A#1",
		"/root/assign/item-2#zone-f#member-dead#0",
	})

	c.Check(serveUntilIdle(c, s.ctx, s.client, s.ks), gc.Equals, 1)
	c.Check(keys(s.ks.Prefixed(s.ks.Root+AssignmentsPrefix)), gc.DeepEquals, []string{
		"/root/assign/item-1#zone-a#member-A#0",
		"/root/assign/item-2#zone-a#member-A#0",
	})
}

// insert creates new keys with values, requiring that the key not already exist.
func insert(ctx context.Context, client *clientv3.Client, keyValues ...string) error {
	var txn = newBatchedTxn(ctx, client)

	for i := 0; i != len(keyValues); i += 2 {
		txn.If(clientv3.Compare(clientv3.Version(keyValues[i]), "=", 0)).
			Then(clientv3.OpPut(keyValues[i], keyValues[i+1]))

		if err := txn.Checkpoint(); err != nil {
			return err
		}
	}
	var _, err = txn.Commit()
	return err
}

// update updates keys with values, requiring that the key already exist.
func update(ctx context.Context, client *clientv3.Client, keyValues ...string) error {
	var txn = newBatchedTxn(ctx, client)

	for i := 0; i != len(keyValues); i += 2 {
		txn.If(clientv3.Compare(clientv3.Version(keyValues[i]), ">", 0)).
			Then(clientv3.OpPut(keyValues[i], keyValues[i+1]))

		if err := txn.Checkpoint(); err != nil {
			return err
		}
	}
	var _, err = txn.Commit()
	return err
}

// markAllConsistent which updates all Assignments to have a value of "consistent".
func markAllConsistent(ctx context.Context, client *clientv3.Client, ks *keyspace.KeySpace) error {
	var txn = newBatchedTxn(ctx, client)

	for _, kv := range ks.Prefixed(ks.Root + AssignmentsPrefix) {
		if string(kv.Raw.Value) == "consistent" {
			continue
		}

		txn.If(modRevisionUnchanged(kv)).
			Then(clientv3.OpPut(string(kv.Raw.Key), "consistent"))

		if err := txn.Checkpoint(); err != nil {
			return err
		}
	}
	var _, err = txn.Commit()
	return err
}

func keys(kv keyspace.KeyValues) []string {
	var r []string
	for i := range kv {
		r = append(r, string(kv[i].Raw.Key))
	}
	return r
}

func serveUntilIdle(c *gc.C, ctx context.Context, client *clientv3.Client, ks *keyspace.KeySpace) int {
	// Pluck out the key of the current Member leader. We'll assume its identity.
	var resp, err = client.Get(ctx, ks.Root+MembersPrefix,
		clientv3.WithPrefix(),
		clientv3.WithLimit(1),
		clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortAscend))
	c.Assert(err, gc.IsNil)

	var state = NewObservedState(ks, string(resp.Kvs[0].Key), isConsistent)

	var result int
	ctx, cancel := context.WithCancel(ctx)

	c.Check(ks.Load(ctx, client, 0), gc.IsNil)
	go ks.Watch(ctx, client)

	// Create and serve an Allocator which will |cancel| when it becomes idle.
	c.Check(Allocate(AllocateArgs{
		Context: ctx,
		Etcd:    client,
		State:   state,
		TestHook: func(round int, idle bool) {
			if idle {
				result = round // Preserve and return the round on which the Allocator became idle.
				cancel()
			}
		},
	}), gc.Equals, context.Canceled)

	return result
}

var _ = gc.Suite(&ScenariosSuite{})
