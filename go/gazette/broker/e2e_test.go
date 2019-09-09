package broker

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/etcdtest"
	"google.golang.org/grpc"
)

func TestE2EAppendAndReplicatedRead(t *testing.T) {
	var ctx, etcd = pb.WithDispatchDefault(context.Background()), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	var peer = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"})

	setTestJournal(broker, pb.JournalSpec{Name: "journal/one", Replication: 2}, broker.id, peer.id)
	setTestJournal(peer, pb.JournalSpec{Name: "journal/two", Replication: 2}, peer.id, broker.id)

	// Ensure each broker is aware of the other's journal.
	broker.catchUpKeySpace()
	peer.catchUpKeySpace()

	broker.initialFragmentLoad()
	peer.initialFragmentLoad()

	var rOne, _ = peer.client().Read(ctx, &pb.ReadRequest{Journal: "journal/one", Block: true})
	var rTwo, _ = broker.client().Read(ctx, &pb.ReadRequest{Journal: "journal/two", Block: true})

	// First Append is served by |broker|, with its Read served by |peer|.
	var stream, _ = broker.client().Append(ctx)
	assert.NoError(t, stream.Send(&pb.AppendRequest{Journal: "journal/one"}))
	assert.NoError(t, stream.Send(&pb.AppendRequest{Content: []byte("hello")}))
	assert.NoError(t, stream.Send(&pb.AppendRequest{}))
	_, _ = stream.CloseAndRecv()

	// Second Append is served by |peer| (through |broker|), with its Read served by |broker|.
	stream, _ = broker.client().Append(ctx)
	assert.NoError(t, stream.Send(&pb.AppendRequest{Journal: "journal/two"}))
	assert.NoError(t, stream.Send(&pb.AppendRequest{Content: []byte("world!")}))
	assert.NoError(t, stream.Send(&pb.AppendRequest{}))
	_, _ = stream.CloseAndRecv()

	// Read Fragment metadata, then content from each Read stream.
	_, err := rOne.Recv()
	assert.NoError(t, err)
	_, err = rTwo.Recv()
	assert.NoError(t, err)

	expectReadResponse(t, rOne, pb.ReadResponse{Content: []byte("hello")})
	expectReadResponse(t, rTwo, pb.ReadResponse{Content: []byte("world!")})

	// Cancel together as we have replication pipelines in each direction.
	broker.tasks.Cancel()
	peer.tasks.Cancel()

	broker.cleanup()
	peer.cleanup()
}

func TestE2EShutdownWithOngoingAppend(t *testing.T) {
	var ctx, etcd = pb.WithDispatchDefault(context.Background()), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 1}, broker.id)
	broker.initialFragmentLoad()

	// Begin an append which will race broker shutdown. Ideally this would be
	// an Append RPC instead of appendFSM, but the Append RPC doesn't give us
	// the required knobs to ensure the Append wins required races vs replica
	// shutdown (eg, after updating assignments it must complete re-resolution
	// and begin streaming content _before_ this test aborts the journal).
	var fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	assert.True(t, fsm.runTo(stateStreamContent))
	fsm.onStreamContent(&pb.AppendRequest{Content: []byte("woot!")}, nil)
	fsm.onStreamContent(&pb.AppendRequest{}, nil) // Commit intent.

	// Expect the Append resolution is effectively "before" replica cancellation.
	var expectHeader = *broker.header("a/journal")
	broker.tasks.Cancel()                          // Begin shutdown.
	broker.svc.resolver.stopServingLocalReplicas() // For good measure (it's okay to run 2x).

	// Finish Append. Expect it was successful.
	fsm.onStreamContent(nil, io.EOF) // Commit.
	fsm.onReadAcknowledgements()

	assert.Equal(t, stateFinished, fsm.state)
	assert.Equal(t, &pb.Fragment{
		Journal:          "a/journal",
		Begin:            0,
		End:              5,
		Sum:              pb.SHA1SumOf("woot!"),
		CompressionCodec: pb.CompressionCodec_SNAPPY,
	}, fsm.clientFragment)
	assert.Equal(t, expectHeader, fsm.resolved.Header)

	// Expect broker exist gracefully.
	assert.NoError(t, broker.tasks.Wait())
}

func TestE2EShutdownWithOngoingAppendWhichLosesRace(t *testing.T) {
	var ctx, etcd = pb.WithDispatchDefault(context.Background()), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 1}, broker.id)
	broker.initialFragmentLoad()

	var fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	fsm.onResolve()

	assert.Equal(t, pb.Status_OK, fsm.resolved.status)
	assert.NotNil(t, fsm.resolved.replica)

	// |fsm| has resolved to a local replica, but failed to acquire the pipeline in time.
	broker.svc.resolver.stopServingLocalReplicas()

	// It now fails with an error, because we still resolve the journal locally
	// but have stopped serving local replicas.
	assert.False(t, fsm.runTo(stateStreamContent))
	assert.Equal(t, stateError, fsm.state)
	assert.EqualError(t, fsm.err, `resolve: resolver has stopped serving local replicas`)
	fsm.returnPipeline()

	broker.cleanup()
}

func TestE2EReassignmentWithOngoingAppend(t *testing.T) {
	var ctx, etcd = pb.WithDispatchDefault(context.Background()), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 1}, broker.id)
	broker.initialFragmentLoad()

	var fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	assert.True(t, fsm.runTo(stateStreamContent))
	fsm.onStreamContent(&pb.AppendRequest{Content: []byte("woot!")}, nil)
	fsm.onStreamContent(&pb.AppendRequest{}, nil) // Commit intent.

	// Expect the Append is effectively before replica reassignment.
	var expectHeader = *broker.header("a/journal")
	// Re-assign to another arbitrary broker.
	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 1},
		pb.ProcessSpec_ID{Zone: "other", Suffix: "peer"})

	// Finish Append. Expect it was successful.
	fsm.onStreamContent(nil, io.EOF) // Commit.
	fsm.onReadAcknowledgements()

	assert.Equal(t, stateFinished, fsm.state)
	assert.Equal(t, &pb.Fragment{
		Journal:          "a/journal",
		Begin:            0,
		End:              5,
		Sum:              pb.SHA1SumOf("woot!"),
		CompressionCodec: pb.CompressionCodec_SNAPPY,
	}, fsm.clientFragment)
	assert.Equal(t, expectHeader, fsm.resolved.Header)

	broker.cleanup()
}

func TestE2EShutdownWithProxyAppend(t *testing.T) {
	var ctx, etcd = pb.WithDispatchDefault(context.Background()), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	var peer = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"})

	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 2}, broker.id, peer.id)
	broker.initialFragmentLoad()

	// Start a long-lived appendFSM, which builds and holds the pipeline.
	var fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	assert.True(t, fsm.runTo(stateStreamContent))

	var conn, client = newDialedClient(t, broker)
	defer conn.Close()

	// Start an Append RPC which will queue behind appendFSM.
	var app, err = client.Append(ctx)
	assert.NoError(t, err)
	assert.NoError(t, app.Send(&pb.AppendRequest{Journal: "a/journal"}))
	assert.NoError(t, app.Send(&pb.AppendRequest{Content: []byte("world!")}))
	assert.NoError(t, app.Send(&pb.AppendRequest{})) // Intent to commit.
	ensureStreamsAreStarted(t, client)

	// Change the routing for journal/one such that peer is now solely responsible.
	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 1}, peer.id)
	// Notify |broker| to begin shutdown. We still expect all RPCs to complete gracefully.
	broker.tasks.Cancel()
	// Wait for |broker| route application / for replica to start shutdown.
	for {
		var res = broker.resolve("a/journal")
		if res.ProcessId != res.localID {
			break
		}
		<-res.invalidateCh
	}

	// Finally complete |fsm|.
	fsm.onStreamContent(&pb.AppendRequest{Content: []byte("hello ")}, nil)
	fsm.onStreamContent(&pb.AppendRequest{}, nil) // Commit intent.
	fsm.onStreamContent(nil, io.EOF)              // Commit.
	fsm.onReadAcknowledgements()

	assert.Equal(t, stateFinished, fsm.state)
	assert.Equal(t, int64(6), fsm.clientFragment.ContentLength())

	// Expect that |app| can now complete, and that it was served by |peer|.
	peer.initialFragmentLoad()
	resp, err := app.CloseAndRecv()
	assert.NoError(t, err)

	assert.Equal(t, &pb.AppendResponse{
		Status: pb.Status_OK,
		Header: *peer.header("a/journal"),
		Commit: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            6,
			End:              12,
			Sum:              pb.SHA1SumOf("world!"),
			CompressionCodec: pb.CompressionCodec_SNAPPY,
		},
		Registers: boxLabels(),
	}, resp)
	assert.Equal(t, []pb.ProcessSpec_ID{peer.id}, resp.Header.Route.Members)

	assert.NoError(t, broker.tasks.Wait())
	peer.cleanup()
}

func TestE2EReplicaReassignedWithOngoingRead(t *testing.T) {
	var ctx, etcd = pb.WithDispatchDefault(context.Background()), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 1}, broker.id)
	applySpoolContentFixture(broker.replica("a/journal"))

	var stream, err = broker.client().Read(ctx, &pb.ReadRequest{Journal: "a/journal", Block: true})
	assert.NoError(t, err)
	_, _ = stream.Recv() // Read header.
	expectReadResponse(t, stream, pb.ReadResponse{Content: []byte("content!")})

	// Replica is re-assigned.
	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 1},
		pb.ProcessSpec_ID{Zone: "other", Suffix: "broker"})

	// Expect the Read RPC is gracefully closed.
	resp, err := stream.Recv()
	assert.Equal(t, io.EOF, err)
	assert.Nil(t, resp)

	broker.cleanup()
}

func TestE2EShutdownWithOngoingRead(t *testing.T) {
	var ctx, etcd = pb.WithDispatchDefault(context.Background()), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 1}, broker.id)
	applySpoolContentFixture(broker.replica("a/journal"))

	var conn, client = newDialedClient(t, broker)
	defer conn.Close()

	var stream, err = client.Read(ctx, &pb.ReadRequest{Journal: "a/journal", Block: true})
	assert.NoError(t, err)
	_, _ = stream.Recv() // Read header.
	expectReadResponse(t, stream, pb.ReadResponse{Content: []byte("content!")})

	broker.tasks.Cancel() // Begin broker shutdown.

	// Expect the Read RPC is gracefully closed.
	resp, err := stream.Recv()
	assert.Equal(t, io.EOF, err)
	assert.Nil(t, resp)

	// Expect broker exist gracefully.
	assert.NoError(t, broker.tasks.Wait())
}

func TestE2EShutdownWithProxyRead(t *testing.T) {
	var ctx, etcd = pb.WithDispatchDefault(context.Background()), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	var peer = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"})
	setTestJournal(peer, pb.JournalSpec{Name: "a/journal", Replication: 1}, peer.id)
	applySpoolContentFixture(peer.replica("a/journal"))

	// Ensure |broker| is aware of "a/journal" (we only synchronized over the |peer| KeySpace).
	broker.catchUpKeySpace()

	var conn, client = newDialedClient(t, broker)
	defer conn.Close()

	stream, err := client.Read(ctx, &pb.ReadRequest{Journal: "a/journal", Block: true})
	assert.NoError(t, err)
	resp, _ := stream.Recv() // Read header from |peer|.
	assert.Equal(t, resp.Header.ProcessId, peer.id)
	expectReadResponse(t, stream, pb.ReadResponse{Content: []byte("content!")})

	broker.tasks.Cancel() // Begin broker shutdown.

	// Expect the Read RPC is gracefully closed.
	resp, err = stream.Recv()
	assert.Equal(t, io.EOF, err)
	assert.Nil(t, resp)

	// Expect broker exist gracefully.
	assert.NoError(t, broker.tasks.Wait())
	peer.cleanup()
}

func applySpoolContentFixture(r *replica) {
	var spool = <-r.spoolCh
	spool.MustApply(&pb.ReplicateRequest{Content: []byte("content!")})
	spool.MustApply(&pb.ReplicateRequest{Proposal: boxFragment(spool.Next())})
	r.spoolCh <- spool
}

// newDialedClient dials & returns a new ClientConn and wrapping JournalClient.
// Usually we just use bk.client(), but tests which race the shutdown of the broker
// may see "transport is closing" errors due to the loopback ClientConn being closed
// before a final EOF response is read.
func newDialedClient(t *testing.T, bk *testBroker) (*grpc.ClientConn, pb.JournalClient) {
	var conn, err = grpc.Dial(bk.srv.Endpoint().URL().Host, grpc.WithInsecure())
	assert.NoError(t, err)
	return conn, pb.NewJournalClient(conn)
}

// ensureStreamsAreStarted completes a no-op RPC, the purpose of which is to
// ensure that all RPCs streams started before it have been received by the
// server, and will be honored by a server GracefulStop.
func ensureStreamsAreStarted(t *testing.T, jc pb.JournalClient) {
	var s, err = jc.Read(context.Background(), &pb.ReadRequest{Journal: "does/not/exist"})
	assert.NoError(t, err)

	resp, err := s.Recv()
	assert.Equal(t, pb.Status_JOURNAL_NOT_FOUND, resp.Status)
	assert.NoError(t, err)

	_, err = s.Recv()
	assert.Equal(t, io.EOF, err)
}
