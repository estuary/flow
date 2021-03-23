package brokertest

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/etcdtest"
	"go.gazette.dev/core/server"
	"google.golang.org/grpc"
)

func TestSimpleReadAndWrite(t *testing.T) {
	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var ctx = pb.WithDispatchDefault(context.Background())
	var bk = NewBroker(t, etcd, "local", "broker")

	CreateJournals(t, bk, Journal(pb.JournalSpec{Name: "foo/bar"}))

	var conn, rjc = newDialedClient(t, bk)
	defer conn.Close()

	// Begin a blocking read of the journal.
	var r = client.NewReader(ctx, rjc, pb.ReadRequest{
		Journal: "foo/bar",
		Block:   true,
	})
	var br = bufio.NewReader(r)

	// Asynchronously append some content over two lines.
	var as = client.NewAppendService(context.Background(), rjc)
	var txn = as.StartAppend(pb.AppendRequest{Journal: "foo/bar"}, nil)
	_, _ = txn.Writer().WriteString("hello, gazette\ngoodbye, gazette")
	require.NoError(t, txn.Release())

	// Expect to read appended content.
	var str, err = br.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, str, "hello, gazette\n")

	bk.Tasks.Cancel() // Non-graceful exit.

	// We receive an EOF on server-initiated close.
	str, err = br.ReadString('\n')
	require.Error(t, io.EOF, err)
	require.Equal(t, str, "goodbye, gazette")

	require.NoError(t, bk.Tasks.Wait())
}

func TestReplicatedReadAndWrite(t *testing.T) {
	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var ctx = pb.WithDispatchDefault(context.Background())
	var bkA = NewBroker(t, etcd, "A", "broker-one")
	var bkB = NewBroker(t, etcd, "B", "broker-two")

	CreateJournals(t, bkA, Journal(pb.JournalSpec{Name: "foo/bar", Replication: 2}))

	var connA, rjcA = newDialedClient(t, bkA)
	defer connA.Close()

	// Begin a blocking read of the journal from |bkA|.
	var r = client.NewReader(ctx, rjcA,
		pb.ReadRequest{
			Journal: "foo/bar",
			Block:   true,
		})
	var br = bufio.NewReader(r)

	// Asynchronously append some content to |bkB|.
	var as = client.NewAppendService(context.Background(),
		pb.NewRoutedJournalClient(bkB.Client(), pb.NoopDispatchRouter{}))
	var txn = as.StartAppend(pb.AppendRequest{Journal: "foo/bar"}, nil)
	_, _ = txn.Writer().WriteString("hello, gazette\n")
	require.NoError(t, txn.Release())

	// Expect to read appended content.
	str, err := br.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, str, "hello, gazette\n")

	// Zero required replicas, allowing brokers to exit.
	updateReplication(t, ctx, rjcA, "foo/bar", 0)
	// Signal desire for both brokers to exit.
	bkA.Signal()
	bkB.Signal()

	// Read stream is closed cleanly.
	str, err = br.ReadString('\n')
	require.Equal(t, io.EOF, err)
	require.Equal(t, str, "")

	require.NoError(t, bkB.Tasks.Wait())
	require.NoError(t, bkA.Tasks.Wait())
}

func TestReassignment(t *testing.T) {
	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var ctx = pb.WithDispatchDefault(context.Background())
	var bkA = NewBroker(t, etcd, "zone", "broker-A")
	var bkB = NewBroker(t, etcd, "zone", "broker-B")

	CreateJournals(t, bkA, Journal(pb.JournalSpec{Name: "foo/bar", Replication: 2}))

	// Precondition: journal is assigned to A & B.
	var rt pb.Route
	require.NoError(t, bkA.WaitForConsistency(ctx, "foo/bar", &rt))
	require.Equal(t, rt, pb.Route{
		Members: []pb.ProcessSpec_ID{
			{"zone", "broker-A"},
			{"zone", "broker-B"},
		},
		Primary: 0,
	})

	// Broker C starts, and A signals for exit.
	var bkC = NewBroker(t, etcd, "zone", "broker-C")

	bkA.Signal()
	require.NoError(t, bkA.Tasks.Wait()) // Exits gracefully.

	// Expect journal was re-assigned to C, with B promoted to primary.
	require.NoError(t, bkB.WaitForConsistency(ctx, "foo/bar", &rt))
	require.Equal(t, rt, pb.Route{
		Members: []pb.ProcessSpec_ID{
			{"zone", "broker-B"},
			{"zone", "broker-C"},
		},
		Primary: 0,
	})

	// Zero required replicas, allowing brokers to exit.
	updateReplication(t, ctx, bkB.Client(), "foo/bar", 0)
	bkB.Signal()
	bkC.Signal()

	require.NoError(t, bkB.Tasks.Wait())
	require.NoError(t, bkC.Tasks.Wait())
}

func TestGracefulStopTimeout(t *testing.T) {
	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()

	// Shorten the graceful stop timeout during this test.
	defer func(d time.Duration) {
		server.GracefulStopTimeout = d
	}(server.GracefulStopTimeout)
	server.GracefulStopTimeout = time.Millisecond * 50

	var ctx = pb.WithDispatchDefault(context.Background())
	var bkA = NewBroker(t, etcd, "zone", "broker-A")
	var bkB = NewBroker(t, etcd, "zone", "broker-B")

	// Journal is exclusively owned by |bkA|.
	CreateJournals(t, bkA, Journal(pb.JournalSpec{Name: "foo/bar", Replication: 1}))

	var connA, rjcA = newDialedClient(t, bkA)
	defer connA.Close()

	// Append a large amount of content, enough to fill a read congestion window.
	var w = client.NewAppender(ctx, rjcA, pb.AppendRequest{Journal: "foo/bar"})
	for i := 0; i != 256; i++ {
		var buf [1 << 15]byte // 32K
		var n, err = w.Write(buf[:])
		require.NoError(t, err)
		require.Equal(t, n, len(buf))
	}
	require.NoError(t, w.Close())

	var connB, rjcB = newDialedClient(t, bkB)
	defer connB.Close()

	// Begin a blocking read over |bkB|'s gRPC service.
	// This request is proxied to |bkA|. Then, don't make progress.
	var r = client.NewReader(ctx, rjcB,
		pb.ReadRequest{Journal: "foo/bar", Block: true})
	var _, err = r.Read(nil)
	require.NoError(t, err)

	// Begin a blocking read over |bkB|'s HTTP gateway.
	// This request is also proxied to |bkA|. Also don't make progress.
	httpResp, err := http.Get("http://" + bkB.Server.Endpoint().URL().Host + "/foo/bar?block=true")
	require.NoError(t, err)

	var oneByte [1]byte
	n, err := httpResp.Body.Read(oneByte[:])
	require.NoError(t, err)
	require.Equal(t, 1, n)

	// Signal the broker to exit. It will, despite the hung RPCs,
	// upon hitting the graceful-stop timeout.
	bkB.Signal()
	require.NoError(t, bkB.Tasks.Wait())

	// Drop replication, allowing broker A to exit gracefully.
	updateReplication(t, ctx, bkA.Client(), "foo/bar", 0)
	bkA.Signal()
	require.NoError(t, bkA.Tasks.Wait())
}

func updateReplication(t require.TestingT, ctx context.Context, bk pb.JournalClient, journal pb.Journal, r int32) {
	var lResp, err = bk.List(ctx, &pb.ListRequest{
		Selector: pb.LabelSelector{Include: pb.MustLabelSet("name", journal.String())},
	})
	require.NoError(t, err)
	require.Len(t, lResp.Journals, 1)

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
	require.NoError(t, err)
	require.Equal(t, aResp.Status, pb.Status_OK)
}

// newDialedClient dials & returns a new ClientConn and wrapping RoutedJournalClient.
// Usually we just use bk.Client(), but tests which race the shutdown of the *Broker
// may see "transport is closing" errors due to the loopback ClientConn being closed
// before the final EOF response is read.
func newDialedClient(t *testing.T, bk *Broker) (*grpc.ClientConn, pb.RoutedJournalClient) {
	var conn, err = grpc.Dial(bk.Endpoint().URL().Host, grpc.WithInsecure())
	require.NoError(t, err)
	return conn, pb.NewRoutedJournalClient(pb.NewJournalClient(conn), pb.NoopDispatchRouter{})
}

func TestMain(m *testing.M) { etcdtest.TestMainWithEtcd(m) }
