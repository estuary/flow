package brokertest

import (
	"bufio"
	"context"
	"io"
	"testing"

	"github.com/LiveRamp/gazette/v2/pkg/client"
	"github.com/LiveRamp/gazette/v2/pkg/etcdtest"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	gc "github.com/go-check/check"
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
	txn.Writer().WriteString("hello, gazette\ngoodbye, gazette")
	c.Check(txn.Release(), gc.IsNil)

	// Read the first line.
	str, err := br.ReadString('\n')
	c.Check(err, gc.IsNil)
	c.Check(str, gc.Equals, "hello, gazette\n")

	bk.RevokeLease(c)

	str, err = br.ReadString('\n')
	c.Check(err, gc.Equals, io.EOF)
	c.Check(str, gc.Equals, "goodbye, gazette")

	bk.WaitForExit()
}

var _ = gc.Suite(&BrokerSuite{})

func Test(t *testing.T) { gc.TestingT(t) }
