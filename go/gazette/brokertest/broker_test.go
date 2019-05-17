package brokertest

import (
	"bufio"
	"context"
	"testing"

	gc "github.com/go-check/check"
	"go.gazette.dev/core/client"
	"go.gazette.dev/core/etcdtest"
	pb "go.gazette.dev/core/protocol"
)

type BrokerSuite struct{}

func (s *BrokerSuite) TestSimpleReadAndWrite(c *gc.C) {
	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var bk = NewBroker(c, etcd, "local", "broker")
	CreateJournals(c, bk, Journal(pb.JournalSpec{Name: "foo/bar"}))

	var ctx = pb.WithDispatchDefault(context.Background())
	var rjc = pb.NewRoutedJournalClient(bk.Client(), pb.NoopDispatchRouter{})

	// Begin a blocking read of the journal.
	var r = client.NewReader(ctx, rjc, pb.ReadRequest{
		Journal: "foo/bar",
		Block:   true,
	})
	var br = bufio.NewReader(r)

	// Asynchronously append some content over two lines.
	var as = client.NewAppendService(context.Background(), rjc)
	var txn = as.StartAppend("foo/bar")
	_, _ = txn.Writer().WriteString("hello, gazette\ngoodbye, gazette")
	c.Check(txn.Release(), gc.IsNil)

	// Expect to read appended content.
	var str, err = br.ReadString('\n')
	c.Check(err, gc.IsNil)
	c.Check(str, gc.Equals, "hello, gazette\n")

	bk.Tasks.Cancel()

	// Next newline is never observed, but we do receive an EOF or
	// gRPC "transport is closing" on server-initiated close.
	str, err = br.ReadString('\n')
	c.Check(err, gc.NotNil)
	c.Check(str, gc.Equals, "goodbye, gazette")

	c.Check(bk.Tasks.Wait(), gc.IsNil)
}

func (s *BrokerSuite) TestReplicatedReadAndWrite(c *gc.C) {
	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var bkA = NewBroker(c, etcd, "A", "broker-one")
	var bkB = NewBroker(c, etcd, "B", "broker-two")
	var rjcA = pb.NewRoutedJournalClient(bkA.srv.MustClient(), pb.NoopDispatchRouter{})
	var rjcB = pb.NewRoutedJournalClient(bkB.srv.MustClient(), pb.NoopDispatchRouter{})

	CreateJournals(c, bkA, Journal(pb.JournalSpec{Name: "foo/bar", Replication: 2}))

	var ctx = pb.WithDispatchDefault(context.Background())

	// Begin a blocking read of the journal from |bkA|.
	var r = client.NewReader(ctx, rjcA,
		pb.ReadRequest{
			Journal: "foo/bar",
			Block:   true,
		})
	var br = bufio.NewReader(r)

	// Asynchronously append some content to |bkB|.
	var as = client.NewAppendService(context.Background(), rjcB)
	var txn = as.StartAppend("foo/bar")
	_, _ = txn.Writer().WriteString("hello, gazette\n")
	c.Check(txn.Release(), gc.IsNil)

	// Expect to read appended content.
	var str, err = br.ReadString('\n')
	c.Check(err, gc.IsNil)
	c.Check(str, gc.Equals, "hello, gazette\n")

	// Update |bkB| spec indicating its desire to exit.
	bkB.Signal()
	updateReplication(c, ctx, rjcA, "foo/bar", 0)
	<-bkA.AllocateIdleCh()              // Assignments are removed.
	c.Check(bkB.Tasks.Wait(), gc.IsNil) // Now |bkB| may exit.

	bkA.Signal()
	c.Check(bkA.Tasks.Wait(), gc.IsNil)
}

func updateReplication(c *gc.C, ctx context.Context, bk pb.RoutedJournalClient, journal pb.Journal, r int32) {
	var lResp, err = bk.List(ctx, &pb.ListRequest{
		Selector: pb.LabelSelector{Include: pb.MustLabelSet("name", journal.String())},
	})
	c.Assert(err, gc.IsNil)
	c.Assert(lResp.Journals, gc.HasLen, 1)

	var req = &pb.ApplyRequest{
		Changes: []pb.ApplyRequest_Change{
			{
				ExpectModRevision: lResp.Journals[0].ModRevision,
			},
		},
	}

	if r == 0 {
		// A JournalSpec may not have replication of zero. Delete it instead.
		req.Changes[0].Delete = lResp.Journals[0].Spec.Name
	} else {
		var spec = lResp.Journals[0].Spec
		spec.Replication = r
		req.Changes[0].Upsert = &spec
	}

	aResp, err := bk.Apply(ctx, req)
	c.Assert(err, gc.IsNil)
	c.Assert(aResp.Status, gc.Equals, pb.Status_OK)
}

var _ = gc.Suite(&BrokerSuite{})

func Test(t *testing.T) { gc.TestingT(t) }
