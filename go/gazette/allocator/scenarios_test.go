package allocator

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.gazette.dev/core/etcdtest"
	"go.gazette.dev/core/keyspace"
)

func TestInitialAllocation(t *testing.T) {
	var ctx, client, ks = testSetup(t)

	require.NoError(t, insert(ctx, client,
		"/root/items/item-1", `{"R": 1}`,
		"/root/items/item-2", `{"R": 2}`,
		"/root/items/item-3", `{"R": 3}`,

		"/root/members/zone-a#member-A1", `{"R": 2}`,
		"/root/members/zone-a#member-A2", `{"R": 2}`,
		"/root/members/zone-b#member-B", `{"R": 4}`,
	))
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 2)

	// Expect Items are fully replicated, each Item spans both zones, and no Member ItemLimit is breached.
	require.Equal(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), []string{
		"/root/assign/item-1#zone-a#member-A1#0",
		"/root/assign/item-2#zone-a#member-A2#0",
		"/root/assign/item-2#zone-b#member-B#1",
		"/root/assign/item-3#zone-a#member-A1#0",
		"/root/assign/item-3#zone-a#member-A2#1",
		"/root/assign/item-3#zone-b#member-B#2",
	})
}

func TestInitialAllocationRegressionIssue157(t *testing.T) {
	var ctx, client, ks = testSetup(t)

	require.NoError(t, insert(ctx, client,
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
	))
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 2)

	// Expect Items are fully replicated, each Item spans both zones, and no Member ItemLimit is breached.
	require.Equal(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), []string{
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

func TestReplaceWhenNotConsistent(t *testing.T) {
	var ctx, client, ks = testSetup(t)

	require.NoError(t, insert(ctx, client,
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
	))

	// As no Assignments become consistent, the scheduler may over-commit
	// Items but cannot remove any current slots.
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, "no match"), 1)

	// Expect member-new has Assignments which will (eventually) allow Assignments
	// of member-old to be removed.
	require.Equal(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), []string{
		"/root/assign/item-1#zone-a#member-A#0",
		"/root/assign/item-2#zone-a#member-new#2",
		"/root/assign/item-2#zone-a#member-old#0",
		"/root/assign/item-2#zone-b#member-B#1",
		"/root/assign/item-3#zone-a#member-A#0",
		"/root/assign/item-3#zone-a#member-new#3",
		"/root/assign/item-3#zone-a#member-old#1",
		"/root/assign/item-3#zone-b#member-B#2",
	})

	// Now assignments can become consistent.
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 4)

	// Expect member-old is removed, and slot indices are compacted.
	require.Equal(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), []string{
		"/root/assign/item-1#zone-a#member-A#0",
		"/root/assign/item-2#zone-a#member-new#1",
		"/root/assign/item-2#zone-b#member-B#0",
		"/root/assign/item-3#zone-a#member-A#0",
		"/root/assign/item-3#zone-a#member-new#2",
		"/root/assign/item-3#zone-b#member-B#1",
	})
}

func TestReplaceWhenConsistent(t *testing.T) {
	var ctx, client, ks = testSetup(t)

	require.NoError(t, insert(ctx, client,
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
	))
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, "no match"), 1)

	// Member-old has not been removed. Member-new is preparing to take over.
	require.Equal(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), []string{
		"/root/assign/item-1#zone-a#member-A#0",
		"/root/assign/item-2#zone-a#member-new#2", // Added.
		"/root/assign/item-2#zone-a#member-old#0",
		"/root/assign/item-2#zone-b#member-B#1",
		"/root/assign/item-3#zone-a#member-A#1",
		"/root/assign/item-3#zone-a#member-new#3", // Added.
		"/root/assign/item-3#zone-a#member-old#0",
		"/root/assign/item-3#zone-b#member-B#2",
	})

	// Now member-new's Assignments become consistent.
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 3)

	// Expect member-old was removed. Member-new was promoted to item-3 primary
	// (eg, each Member has one primary).
	require.Equal(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), []string{
		"/root/assign/item-1#zone-a#member-A#0",
		"/root/assign/item-2#zone-a#member-new#1",
		"/root/assign/item-2#zone-b#member-B#0",
		"/root/assign/item-3#zone-a#member-A#1",
		"/root/assign/item-3#zone-a#member-new#0",
		"/root/assign/item-3#zone-b#member-B#2",
	})
}

func TestUpdateItemLimit(t *testing.T) {
	var ctx, client, ks = testSetup(t)

	require.NoError(t, insert(ctx, client,
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
	))
	// Expect current solution is stable.
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 0)

	// Increase member-limit's limit.
	require.NoError(t, update(ctx, client,
		"/root/members/zone-a#member-limit", `{"R": 10}`))
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, "no match"), 1)

	// Expect the scheduler begins to allocate more load to member-limit.
	require.Equal(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), []string{
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
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 3)

	// Expect member-limit has three Assignments, to the other Member's two/one.
	// Each member is assigned one primary.
	require.Equal(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), []string{
		"/root/assign/item-1#zone-a#member-limit#0",
		"/root/assign/item-2#zone-a#member-limit#1",
		"/root/assign/item-2#zone-b#member-B#0",
		"/root/assign/item-3#zone-a#member-A#0",
		"/root/assign/item-3#zone-a#member-limit#1",
		"/root/assign/item-3#zone-b#member-B#2",
	})

	// Restore member-limit's limit.
	require.NoError(t, update(ctx, client,
		"/root/members/zone-a#member-limit", `{"R": 2}`))
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, "no match"), 1)

	// Both member-limit and member-A have scaled R: 2. Where the initial fixture
	// had member-B with three items, now the allocator lazily leaves two items
	// each with member-limit & member-A.
	require.Equal(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), []string{
		"/root/assign/item-1#zone-a#member-limit#0",
		"/root/assign/item-2#zone-a#member-A#2", // Acquired.
		"/root/assign/item-2#zone-a#member-limit#1",
		"/root/assign/item-2#zone-b#member-B#0",
		"/root/assign/item-3#zone-a#member-A#0",
		"/root/assign/item-3#zone-a#member-limit#1",
		"/root/assign/item-3#zone-b#member-B#2",
	})

	// Mark member-A's new Assignment as consistent.
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 3)

	require.Equal(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), []string{
		"/root/assign/item-1#zone-a#member-limit#0",
		"/root/assign/item-2#zone-a#member-A#1",
		"/root/assign/item-2#zone-b#member-B#0",
		"/root/assign/item-3#zone-a#member-A#0",
		"/root/assign/item-3#zone-a#member-limit#1",
		"/root/assign/item-3#zone-b#member-B#2",
	})

	// Lower member-limit's limit to 1.
	require.NoError(t, update(ctx, client,
		"/root/members/zone-a#member-limit", `{"R": 1}`))
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, "no match"), 1)

	require.Equal(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), []string{
		"/root/assign/item-1#zone-a#member-limit#0",
		"/root/assign/item-1#zone-b#member-B#1", // Acquired.
		"/root/assign/item-2#zone-a#member-A#1",
		"/root/assign/item-2#zone-b#member-B#0",
		"/root/assign/item-3#zone-a#member-A#0",
		"/root/assign/item-3#zone-a#member-limit#1",
		"/root/assign/item-3#zone-b#member-B#2",
	})

	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 2)

	require.Equal(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), []string{
		"/root/assign/item-1#zone-b#member-B#0",
		"/root/assign/item-2#zone-a#member-A#1",
		"/root/assign/item-2#zone-b#member-B#0",
		"/root/assign/item-3#zone-a#member-A#0",
		"/root/assign/item-3#zone-a#member-limit#1",
		"/root/assign/item-3#zone-b#member-B#2",
	})

	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 0) // Nothing to do.
}

func TestUpdateDesiredReplication(t *testing.T) {
	var ctx, client, ks = testSetup(t)

	require.NoError(t, insert(ctx, client,
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
	))
	// Expect current solution is stable.
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 0)

	// Swap the desired replication of item-1 & item-3.
	require.NoError(t, update(ctx, client,
		"/root/items/item-1", `{"R": 3}`,
		"/root/items/item-3", `{"R": 1}`))
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, "no match"), 4)

	require.Equal(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), []string{
		"/root/assign/item-1#zone-a#member-A1#2", // Acquired.
		"/root/assign/item-1#zone-a#member-A2#1", // Acquired.
		"/root/assign/item-1#zone-b#member-B#0",
		"/root/assign/item-2#zone-a#member-A1#0",
		"/root/assign/item-2#zone-b#member-B#1",
		"/root/assign/item-3#zone-a#member-A2#0",
	})

	// Mark Assignments as consistent. No further changes are required.
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 1)
}

func TestScaleUpFromInsufficientMemberSlots(t *testing.T) {
	var ctx, client, ks = testSetup(t)

	// Create a fixture with more Item slots than Member slots.
	require.NoError(t, insert(ctx, client,
		"/root/items/item-1", `{"R": 1}`,
		"/root/items/item-2", `{"R": 2}`,
		"/root/items/item-3", `{"R": 3}`,

		"/root/members/zone-a#member-A1", `{"R": 1}`,
	))
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 2)

	// Expect only item-1 is allocated.
	require.Equal(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), []string{
		"/root/assign/item-1#zone-a#member-A1#0",
	})
	// Add a new member. Still under-provisioned.
	require.NoError(t, insert(ctx, client, "/root/members/zone-a#member-A2", `{"R": 2}`))

	// Expect item-3 is not attempted to be allocated, and item-1 is handed off to member-A2.
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 5)

	require.Equal(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), []string{
		"/root/assign/item-1#zone-a#member-A2#0",
		"/root/assign/item-2#zone-a#member-A1#1",
		"/root/assign/item-2#zone-a#member-A2#0",
	})

	// Add a new member, such that we're fully provisioned.
	require.NoError(t, insert(ctx, client, "/root/members/zone-a#member-A3", `{"R": 3}`))

	// Expect item-3 is allocated, but first item-1 is rotated to A3, item-2 from A1 => A2, and item-3 => A1.
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 6)

	require.Equal(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), []string{
		"/root/assign/item-1#zone-a#member-A3#0",
		"/root/assign/item-2#zone-a#member-A2#0",
		"/root/assign/item-2#zone-a#member-A3#1",
		"/root/assign/item-3#zone-a#member-A1#1",
		"/root/assign/item-3#zone-a#member-A2#2",
		"/root/assign/item-3#zone-a#member-A3#0",
	})
}

func TestScaleDownToInsufficientMemberSlots(t *testing.T) {
	var ctx, client, ks = testSetup(t)

	require.NoError(t, insert(ctx, client,
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
	))
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, "no match"), 0)

	// Reduce A3's ItemLimit to two.
	require.NoError(t, update(ctx, client, "/root/members/zone-a#member-A3", `{"R": 2}`))

	// Expect no changes occur, as it would violate item consistency.
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 0)

	require.Equal(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), []string{
		"/root/assign/item-1#zone-a#member-A3#0",
		"/root/assign/item-2#zone-a#member-A2#0",
		"/root/assign/item-2#zone-a#member-A3#1",
		"/root/assign/item-3#zone-a#member-A1#1",
		"/root/assign/item-3#zone-a#member-A2#2",
		"/root/assign/item-3#zone-a#member-A3#0",
	})
}

func TestScaleUpZonesOneToTwo(t *testing.T) {
	var ctx, client, ks = testSetup(t)

	// Create a fixture with one zone having plenty of capacity.
	require.NoError(t, insert(ctx, client,
		"/root/items/item-1", `{"R": 3}`,
		"/root/items/item-2", `{"R": 3}`,
		"/root/items/item-3", `{"R": 3}`,

		"/root/members/zone-a#member-A1", `{"R": 100}`,
		"/root/members/zone-a#member-A2", `{"R": 100}`,
		"/root/members/zone-a#member-A3", `{"R": 100}`,
	))
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 2)

	// All items are allocated in the single zone.
	require.Equal(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), []string{
		"/root/assign/item-1#zone-a#member-A1#0",
		"/root/assign/item-1#zone-a#member-A2#1",
		"/root/assign/item-1#zone-a#member-A3#2",
		"/root/assign/item-2#zone-a#member-A1#0",
		"/root/assign/item-2#zone-a#member-A2#1",
		"/root/assign/item-2#zone-a#member-A3#2",
		"/root/assign/item-3#zone-a#member-A1#0",
		"/root/assign/item-3#zone-a#member-A2#1",
		"/root/assign/item-3#zone-a#member-A3#2",
	})

	// Add a new broker with a new zone, but insufficient capacity to place all items.
	require.NoError(t, insert(ctx, client, "/root/members/zone-b#member-B", `{"R": 2}`))
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 5)

	// Expect member-B is fully utilized, though its capacity is dwarfed
	// by other members. Also expect one item is replicated only within
	// a single zone.
	require.Equal(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), []string{
		"/root/assign/item-1#zone-a#member-A1#0",
		"/root/assign/item-1#zone-a#member-A3#1",
		"/root/assign/item-1#zone-b#member-B#2",
		"/root/assign/item-2#zone-a#member-A2#0",
		"/root/assign/item-2#zone-a#member-A3#1",
		"/root/assign/item-2#zone-b#member-B#2",
		"/root/assign/item-3#zone-a#member-A1#0",
		"/root/assign/item-3#zone-a#member-A2#1",
		"/root/assign/item-3#zone-a#member-A3#2",
	})

	// Add one slot. It's now possible to place all items across two zones.
	require.NoError(t, update(ctx, client, "/root/members/zone-b#member-B", `{"R": 3}`))
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 4)

	// Expect all items are now replicated across both zones.
	require.Equal(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), []string{
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
	require.NoError(t, insert(ctx, client, "/root/items/item-4", `{"R": 1}`))
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 2)

	require.Equal(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), []string{
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

func TestScaleDownZonesTwoToOne(t *testing.T) {
	var ctx, client, ks = testSetup(t)

	// Create a fixture with two zones, with items balanced across them.
	require.NoError(t, insert(ctx, client,
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
	))
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 0) // Fixture is stable.

	// Lower total slots of zone-B.
	require.NoError(t, update(ctx, client, "/root/members/zone-b#member-B", `{"R": 2}`))
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 4)

	// Expect items are re-balanced and zone-B is well-utilized,
	// but that multi-zone replication is relaxed.
	require.Equal(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), []string{
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
	require.NoError(t, update(ctx, client, "/root/members/zone-b#member-B", `{"R": 0}`))
	require.NoError(t, insert(ctx, client, "/root/members/zone-a#member-A3", `{"R": 10}`))
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 4)

	// All items served from zone-A.
	require.Equal(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), []string{
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

func TestScaleUpZonesTwoToThree(t *testing.T) {
	var ctx, client, ks = testSetup(t)

	// Create an initial fixture with two zones.
	require.NoError(t, insert(ctx, client,
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
	))
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 0) // Fixture is stable.

	// Insert member-C with two slots. We expect no changes occur, because the
	// zone is not needed to meet zone constraints, member scaling allows two
	// replicas per member, and we'd rather keep the current solution.
	require.NoError(t, insert(ctx, client, "/root/members/zone-c#member-C", `{"R": 100}`))
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 0) // No changes.

	// Increase the replication factor of items.
	require.NoError(t, update(ctx, client, "/root/items/item-1", `{"R": 3}`))
	require.NoError(t, update(ctx, client, "/root/items/item-2", `{"R": 4}`))

	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 2)

	// Expect the new slots were given to member-C, though member-A1 also had
	// scaled capacity, due to the even zone balancing network optimization goal.
	require.Equal(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), []string{
		"/root/assign/item-1#zone-a#member-A1#0",
		"/root/assign/item-1#zone-b#member-B#1",
		"/root/assign/item-1#zone-c#member-C#2", // Added.
		"/root/assign/item-2#zone-a#member-A1#0",
		"/root/assign/item-2#zone-a#member-A2#1",
		"/root/assign/item-2#zone-b#member-B#2",
		"/root/assign/item-2#zone-c#member-C#3", // Added.
	})
}

func TestScaleDownZonesThreeToTwo(t *testing.T) {
	var ctx, client, ks = testSetup(t)

	require.NoError(t, insert(ctx, client,
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
	))
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 0) // Fixture is stable.

	// Drop slots of member-C. Expect Items are re-balanced across two remaining zones.
	require.NoError(t, update(ctx, client, "/root/members/zone-c#member-C", `{"R": 0}`))
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 4)

	require.Equal(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), []string{
		"/root/assign/item-1#zone-a#member-A1#0",
		"/root/assign/item-1#zone-a#member-A2#1",
		"/root/assign/item-1#zone-b#member-B#2",
		"/root/assign/item-2#zone-a#member-A1#2",
		"/root/assign/item-2#zone-a#member-A3#0",
		"/root/assign/item-2#zone-b#member-B#1",
	})
}

func TestUnbalancedZoneAndMemberCapacityRotations(t *testing.T) {
	var ctx, client, ks = testSetup(t)

	require.NoError(t, insert(ctx, client,
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
	))
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 0) // Fixture is stable.

	// Swap capacities of zone-A members.
	require.NoError(t, update(ctx, client,
		"/root/members/zone-a#member-A1", `{"R": 3}`,
		"/root/members/zone-a#member-A2", `{"R": 1}`,
	))

	// Expect items are rotated to meet A2's reduced capacity.
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 5)

	require.Equal(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), []string{
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
	require.NoError(t, update(ctx, client,
		"/root/members/zone-a#member-A1", `{"R": 10}`,
		"/root/members/zone-a#member-A2", `{"R": 10}`,
		"/root/members/zone-b#member-B1", `{"R": 1}`,
		"/root/members/zone-b#member-B2", `{"R": 3}`,
	))

	// Expect items are rotated to meet B1's reduced capacity.
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 5)

	require.Equal(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), []string{
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

func TestRecoveryOnLeaseAndZoneEviction(t *testing.T) {
	var ctx, client, ks = testSetup(t)

	require.NoError(t, insert(ctx, client,
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
	))
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 0) // Fixture is stable.

	// Member-B1 and its Assignments disappear (eg, simulating lease eviction,
	// and even the loss of an entire zone).
	client.Delete(ctx, "/root/members/zone-b#member-B1")
	client.Delete(ctx, "/root/assign/item-2#zone-b#member-B1#0")
	client.Delete(ctx, "/root/assign/item-3#zone-b#member-B1#1")

	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 4)

	require.Equal(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), []string{
		"/root/assign/item-1#zone-a#member-A1#0",
		"/root/assign/item-2#zone-a#member-A1#1",
		"/root/assign/item-2#zone-a#member-A2#0",
		"/root/assign/item-3#zone-a#member-A1#1",
		"/root/assign/item-3#zone-a#member-A2#2",
		"/root/assign/item-3#zone-a#member-A3#0",
	})
}

func TestSingleZoneRebalanceOnMemberScaleUp(t *testing.T) {
	var ctx, client, ks = testSetup(t)

	// Initial fixture has one zone, and equal member & item slots.
	require.NoError(t, insert(ctx, client,
		"/root/items/item-1", `{"R": 1}`,
		"/root/items/item-2", `{"R": 2}`,
		"/root/items/item-3", `{"R": 3}`,

		"/root/members/zone-a#member-A1", `{"R": 3}`,
		"/root/members/zone-a#member-A2", `{"R": 2}`,
		"/root/members/zone-a#member-A3", `{"R": 1}`,
	))
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 2)

	// Expect Items are fully replicated.
	require.Equal(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), []string{
		"/root/assign/item-1#zone-a#member-A1#0",
		"/root/assign/item-2#zone-a#member-A1#0",
		"/root/assign/item-2#zone-a#member-A2#1",
		"/root/assign/item-3#zone-a#member-A1#0",
		"/root/assign/item-3#zone-a#member-A2#1",
		"/root/assign/item-3#zone-a#member-A3#2",
	})

	// Introduce a new member. Expect no changes are made.
	require.NoError(t, insert(ctx, client,
		"/root/members/zone-a#member-A4", `{"R": 1}`))
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 0) // No changes.

	// Increase its ItemLimit, which causes load to be re-balanced to A4.
	require.NoError(t, update(ctx, client,
		"/root/members/zone-a#member-A4", `{"R": 3}`))

	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 5)

	require.Equal(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), []string{
		"/root/assign/item-1#zone-a#member-A1#0",
		"/root/assign/item-2#zone-a#member-A1#0",
		"/root/assign/item-2#zone-a#member-A2#1",
		"/root/assign/item-3#zone-a#member-A2#0",
		"/root/assign/item-3#zone-a#member-A3#1",
		"/root/assign/item-3#zone-a#member-A4#2",
	})
}

func TestShedCapacityDrainsExitingMembers(t *testing.T) {
	var ctx, client, ks = testSetup(t)

	// Start with a stable allocation across 4 members.
	// Insert A2 first so it becomes the leader (lowest CreateRevision)
	// and won't be an exiting member.
	require.NoError(t, insert(ctx, client,
		"/root/members/zone-a#member-A2", `{"R": 4}`,
	))
	require.NoError(t, insert(ctx, client,
		"/root/items/item-1", `{"R": 1}`,
		"/root/items/item-2", `{"R": 1}`,
		"/root/items/item-3", `{"R": 1}`,
		"/root/items/item-4", `{"R": 1}`,

		"/root/members/zone-a#member-A1", `{"R": 4}`,
		"/root/members/zone-b#member-B1", `{"R": 4}`,
		"/root/members/zone-b#member-B2", `{"R": 4}`,
	))
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 2)

	// Mark A1 and B1 as exiting. maxRF = 1, maxShedding = 3.
	// Both can shed their full capacity.
	require.NoError(t, update(ctx, client,
		"/root/members/zone-a#member-A1", `{"R": 4, "E": true}`,
		"/root/members/zone-b#member-B1", `{"R": 4, "E": true}`,
	))
	serveUntilIdle(t, ctx, client, ks, "")

	// Expect all assignments moved to non-exiting members.
	var assignmentKeys = keys(ks.Prefixed(ks.Root + AssignmentsPrefix))
	for _, key := range assignmentKeys {
		require.NotContains(t, key, "member-A1")
		require.NotContains(t, key, "member-B1")
	}
	require.Len(t, assignmentKeys, 4) // All 4 items still assigned.
}

func TestShedCapacityLeaderExits(t *testing.T) {
	var ctx, client, ks = testSetup(t)

	// A1 is the oldest member (leader) and will be marked as exiting.
	require.NoError(t, insert(ctx, client,
		"/root/items/item-1", `{"R": 1}`,
		"/root/items/item-2", `{"R": 1}`,
		"/root/items/item-3", `{"R": 1}`,

		"/root/members/zone-a#member-A1", `{"R": 4}`,
		"/root/members/zone-a#member-A2", `{"R": 4}`,
		"/root/members/zone-b#member-B1", `{"R": 4}`,
	))
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 2)

	// Mark A1 (the leader) as exiting.
	require.NoError(t, update(ctx, client,
		"/root/members/zone-a#member-A1", `{"R": 4, "E": true}`,
	))

	// Run the allocator as A1. It drains its own assignments and then
	// Allocate returns nil (shouldExit).
	var state = NewObservedState(ks, "/root/members/zone-a#member-A1", isConsistent)

	ctx2, cancel2 := context.WithCancel(ctx)
	require.NoError(t, ks.Load(ctx2, client, 0))
	go ks.Watch(ctx2, client)

	require.NoError(t, Allocate(AllocateArgs{
		Context: ctx2,
		Etcd:    client,
		State:   state,
		TestHook: func(_ int, idle bool) {
			if idle {
				markAllConsistent(ctx2, client, ks, "")
			}
		},
	}))
	cancel2()

	// A1 drained itself. Verify no assignments remain on A1.
	for _, key := range keys(ks.Prefixed(ks.Root + AssignmentsPrefix)) {
		require.NotContains(t, key, "member-A1")
	}
	require.Len(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), 3)
}

func TestCleanupOfAssignmentsWithoutItems(t *testing.T) {
	var ctx, client, ks = testSetup(t)

	require.NoError(t, insert(ctx, client,
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
	))

	// Expect dead Assignments are removed, and remaining live ones are stable.
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, "no match"), 1)
	require.Equal(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), []string{
		"/root/assign/item-2#zone-c#member-C#0",
		"/root/assign/item-5#zone-b#member-B#1",
		"/root/assign/item-5#zone-c#member-C#0",
		"/root/assign/item-6#zone-a#member-A#0",
		"/root/assign/item-6#zone-b#member-B#1",
		"/root/assign/item-6#zone-c#member-C#2",
	})
}

func TestCleanupOfAssignmentsWithoutMember(t *testing.T) {
	var ctx, client, ks = testSetup(t)

	require.NoError(t, insert(ctx, client,
		"/root/items/item-1", `{"R": 1}`,
		"/root/items/item-2", `{"R": 1}`,

		"/root/members/zone-a#member-A", `{"R": 2}`,

		"/root/assign/item-1#zone-f#member-dead#0", `consistent`,
		"/root/assign/item-2#zone-f#member-dead#0", `consistent`,

		"/root/assign/item-1#zone-a#member-A#1", `consistent`,
	))
	require.Equal(t, serveUntilIdle(t, ctx, client, ks, "no match"), 1)

	require.Equal(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), []string{
		"/root/assign/item-1#zone-a#member-A#0", // Expect member-dead removed, and A promoted.

		"/root/assign/item-2#zone-a#member-A#1",
		"/root/assign/item-2#zone-f#member-dead#0",
	})

	require.Equal(t, serveUntilIdle(t, ctx, client, ks, ""), 2)
	require.Equal(t, keys(ks.Prefixed(ks.Root+AssignmentsPrefix)), []string{
		"/root/assign/item-1#zone-a#member-A#0",
		"/root/assign/item-2#zone-a#member-A#0",
	})
}

func testSetup(t *testing.T) (context.Context, *clientv3.Client, *keyspace.KeySpace) {
	var ctx, client = context.Background(), etcdtest.TestClient()
	t.Cleanup(etcdtest.Cleanup)
	return ctx, client, NewAllocatorKeySpace("/root", testAllocDecoder{})
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
	return txn.Flush()
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
	return txn.Flush()
}

// markAllConsistent which updates all Assignments to have a value of "consistent".
// Or, if all assignments are consistent, it returns ErrNoProgress.
func markAllConsistent(ctx context.Context, client *clientv3.Client, ks *keyspace.KeySpace, when string) error {
	var txn = newBatchedTxn(ctx, client)

	for _, kv := range ks.Prefixed(ks.Root + AssignmentsPrefix) {
		if string(kv.Raw.Value) != when {
			continue
		}

		txn.If(modRevisionUnchanged(kv)).
			Then(clientv3.OpPut(string(kv.Raw.Key), "consistent"))

		if err := txn.Checkpoint(); err != nil {
			return err
		}
	}

	if err := txn.Flush(); err != nil {
		return err
	} else if txn.Revision() == 0 {
		return io.ErrNoProgress
	} else {
		return nil
	}
}

func keys(kv keyspace.KeyValues) []string {
	var r []string
	for i := range kv {
		r = append(r, string(kv[i].Raw.Key))
	}
	return r
}

func serveUntilIdle(t *testing.T, ctx context.Context, client *clientv3.Client, ks *keyspace.KeySpace, when string) int {
	// Pluck out the key of the current Member leader. We'll assume its identity.
	var resp, err = client.Get(ctx, ks.Root+MembersPrefix,
		clientv3.WithPrefix(),
		clientv3.WithLimit(1),
		clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortAscend))
	require.NoError(t, err)

	var state = NewObservedState(ks, string(resp.Kvs[0].Key), isConsistent)

	var result int
	ctx, cancel := context.WithCancel(ctx)

	require.NoError(t, ks.Load(ctx, client, 0))
	go ks.Watch(ctx, client)

	// Create and serve an Allocator which will |cancel| when it becomes idle.
	require.Equal(t, Allocate(AllocateArgs{
		Context: ctx,
		Etcd:    client,
		State:   state,
		TestHook: func(round int, idle bool) {
			if !idle {
				return
			} else if err := markAllConsistent(ctx, client, ks, when); err == nil {
				return // We were able to update some assignments from "" => "consistent".
			} else if err != io.ErrNoProgress {
				panic(err)
			} else {
				// All done. The allocator is idle, and all assignments are already consistent.
				result = round // Preserve and return the round on which the Allocator became idle.
				cancel()
			}
		},
	}), context.Canceled)

	return result
}
