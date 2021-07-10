package allocator

import (
	"context"
	"testing"

	gc "github.com/go-check/check"
	epb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.gazette.dev/core/etcdtest"
)

type AllocatorSuite struct{}

func (s *AllocatorSuite) TestRemoveDeadAssignments(c *gc.C) {
	var client, ctx = etcdtest.TestClient(), context.Background()
	defer etcdtest.Cleanup()
	buildAllocKeySpaceFixture(c, ctx, client)

	var ks = NewAllocatorKeySpace("/root", testAllocDecoder{})
	c.Check(ks.Load(ctx, client, 0), gc.IsNil)

	var assignments = ks.Prefixed(ks.Root + AssignmentsPrefix)
	var txn mockTxnBuilder

	// Only "item-missing" is actually a dead Assignment in the fixture, but we can
	// still pass all Assignments and verify Cmps and Ops of the resulting
	// transaction (which wouldn't actually succeed, since Items exist).
	c.Check(removeDeadAssignments(&txn, ks, assignments), gc.IsNil)

	// Expect Assignments are grouped by Item. The non-existence of the Item is
	// verified, as well as that each Assignment is unchanged.
	c.Check(txn.cmps, gc.DeepEquals, []clientv3.Cmp{
		clientv3.Compare(clientv3.CreateRevision("/root/items/item-1"), "=", 0),
		modRevisionUnchanged(assignments[0]), // assign/item-1#us-east#foo#1
		modRevisionUnchanged(assignments[1]), // assign/item-1#us-west#baz#0
		clientv3.Compare(clientv3.CreateRevision("/root/items/item-missing"), "=", 0),
		modRevisionUnchanged(assignments[2]), // assign/item-missing#us-west#baz#0
		clientv3.Compare(clientv3.CreateRevision("/root/items/item-two"), "=", 0),
		modRevisionUnchanged(assignments[3]), // assign/item-two#missing#member#2
		modRevisionUnchanged(assignments[4]), // assign/item-two#us-east#bar#0
		modRevisionUnchanged(assignments[5]), // assign/item-two#us-west#baz#1
	})

	c.Check(txn.ops, gc.DeepEquals, []clientv3.Op{
		clientv3.OpDelete("/root/assign/item-1#us-east#foo#1"),
		clientv3.OpDelete("/root/assign/item-1#us-west#baz#0"),
		clientv3.OpDelete("/root/assign/item-missing#us-west#baz#0"),
		clientv3.OpDelete("/root/assign/item-two#missing#member#2"),
		clientv3.OpDelete("/root/assign/item-two#us-east#bar#0"),
		clientv3.OpDelete("/root/assign/item-two#us-west#baz#1"),
	})
}

func (s *AllocatorSuite) TestConvergeFixtureCases(c *gc.C) {
	var client, ctx = etcdtest.TestClient(), context.Background()
	defer etcdtest.Cleanup()
	buildAllocKeySpaceFixture(c, ctx, client)

	var ks = NewAllocatorKeySpace("/root", testAllocDecoder{})
	var as = NewObservedState(ks, MemberKey(ks, "us-east", "foo"), isConsistent)
	c.Check(ks.Load(ctx, client, 0), gc.IsNil)

	// Tweak the fixture to associate a lease with Member us-east/foo
	as.Members[1].Raw.Lease = 0xfeedbeef

	// Case 1: desired state matches current state, aside from fix-ups for missing Items / Members.
	var txn mockTxnBuilder
	converge(&txn, as, []Assignment{
		{ItemID: "item-1", MemberZone: "us-east", MemberSuffix: "foo"},
		{ItemID: "item-1", MemberZone: "us-west", MemberSuffix: "baz"},

		{ItemID: "item-two", MemberZone: "us-east", MemberSuffix: "bar"},
		{ItemID: "item-two", MemberZone: "us-west", MemberSuffix: "baz"},
	})

	var expectCmps = []clientv3.Cmp{
		clientv3.Compare(clientv3.CreateRevision("/root/items/item-missing"), "=", 0),
		modRevisionUnchanged(as.Assignments[2]), // assign/item-missing#us-west#baz#0
		modRevisionUnchanged(as.Items[1]),       // items/item-two
		modRevisionUnchanged(as.Assignments[3]), // assign/item-two#missing#member#2
	}
	c.Check(txn.cmps, gc.DeepEquals, expectCmps)
	c.Check(txn.ops, gc.DeepEquals, []clientv3.Op{
		clientv3.OpDelete("/root/assign/item-missing#us-west#baz#0"),
		clientv3.OpDelete("/root/assign/item-two#missing#member#2"),
	})

	// Case 2: desire to flip "foo" and "bar". "bar" is at capacity, "foo" is not:
	// expect an Assignment for "foo" (only) is created.
	txn = mockTxnBuilder{}
	converge(&txn, as, []Assignment{
		{ItemID: "item-1", MemberZone: "us-east", MemberSuffix: "bar"},
		{ItemID: "item-1", MemberZone: "us-west", MemberSuffix: "baz"},

		{ItemID: "item-two", MemberZone: "us-east", MemberSuffix: "foo"},
		{ItemID: "item-two", MemberZone: "us-west", MemberSuffix: "baz"},
	})

	// In addition to the cleanup checks of the previous case,
	// expect Member us-east/foo is also verified as unchanged.
	c.Check(txn.cmps, gc.DeepEquals, append(
		append(expectCmps[:2:2], modRevisionUnchanged(as.Members[1])), expectCmps[2:]...))

	c.Check(txn.ops, gc.DeepEquals, []clientv3.Op{
		clientv3.OpDelete("/root/assign/item-missing#us-west#baz#0"),
		clientv3.OpPut("/root/assign/item-two#us-east#foo#3", "", clientv3.WithLease(0xfeedbeef)),
		clientv3.OpDelete("/root/assign/item-two#missing#member#2"),
	})
}

func (s *AllocatorSuite) TestTxnBatching(c *gc.C) {
	// Cmp and Op fixtures for use in this test.
	var fixedCmp = clientv3.Compare(clientv3.Value("/key-1"), "=", "val-1")
	var testCmp = clientv3.Compare(clientv3.Value("/key-2"), "=", "val-2")
	var testOp = clientv3.OpDelete("/other-key")

	var txnOp clientv3.Op                // Collects last-dispatched OpTxn.
	var txnResp = &clientv3.TxnResponse{ // Returned response fixture.
		Succeeded: true,
		Header:    &epb.ResponseHeader{Revision: 1234},
	}

	var txn = batchedTxn{
		txnDo: func(op clientv3.Op) (*clientv3.TxnResponse, error) {
			var c, o, _ = op.Txn()
			txnOp = clientv3.OpTxn( // Deep-copy |op|.
				append([]clientv3.Cmp(nil), c...),
				append([]clientv3.Op(nil), o...),
				nil)

			return txnResp, nil
		},
		fixedCmps: []clientv3.Cmp{fixedCmp},
	}

	defer func(m int) { maxTxnOps = m }(maxTxnOps) // For this test, fix |maxTxnOps| to 3.
	maxTxnOps = 3

	c.Check(txn.If(testCmp).Then(testOp).Checkpoint(), gc.IsNil) // No flush (2 Cmps, 1 Op).
	c.Check(txn.If(testCmp).Then(testOp).Checkpoint(), gc.IsNil) // No flush (3 Cmps, 2 Ops).

	c.Check(txnOp, gc.DeepEquals, clientv3.Op{})                         // Verify no transaction issued yet.
	c.Check(txn.If(testCmp).Then(testOp, testOp).Checkpoint(), gc.IsNil) // Forces flush (4 Cmps).

	c.Check(txnOp, gc.DeepEquals, clientv3.OpTxn(
		[]clientv3.Cmp{fixedCmp, testCmp, testCmp},
		[]clientv3.Op{testOp, testOp},
		nil,
	))

	c.Check(txn.Then(testOp).Checkpoint(), gc.IsNil)                     // No flush.
	c.Check(txn.If(testCmp).Then(testOp, testOp).Checkpoint(), gc.IsNil) // Flush (4 Ops).

	c.Check(txnOp, gc.DeepEquals, clientv3.OpTxn(
		[]clientv3.Cmp{fixedCmp, testCmp},
		[]clientv3.Op{testOp, testOp, testOp},
		nil,
	))

	// Final commit. Expect it flushes the last checkpoint.
	var r, err = txn.Commit()
	c.Check(r, gc.Equals, txnResp)
	c.Check(err, gc.IsNil)

	c.Check(txnOp, gc.DeepEquals, clientv3.OpTxn(
		[]clientv3.Cmp{fixedCmp, testCmp},
		[]clientv3.Op{testOp, testOp},
		nil,
	))

	// Empty Checkpoint, then Commit. Expect it's treated as a no-op.
	c.Check(txn.Checkpoint(), gc.IsNil)

	r, err = txn.Commit()
	c.Check(r, gc.IsNil)
	c.Check(err, gc.IsNil)

	// Non-empty commit that fails checks. Expect it's mapped to an error.
	c.Check(txn.Then(testOp).Checkpoint(), gc.IsNil)
	txnResp.Succeeded = false

	r, err = txn.Commit()
	c.Check(r, gc.Equals, txnResp)
	c.Check(err, gc.ErrorMatches, "transaction checks did not succeed")
}

var _ = gc.Suite(&AllocatorSuite{})

func Test(t *testing.T) { gc.TestingT(t) }
