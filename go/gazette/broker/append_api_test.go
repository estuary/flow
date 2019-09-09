package broker

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/broker/fragment"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/etcdtest"
)

func TestAppendSingle(t *testing.T) {
	var ctx, etcd = pb.WithDispatchDefault(context.Background()), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 1}, broker.id)
	broker.initialFragmentLoad()

	var stream, _ = broker.client().Append(ctx)
	assert.NoError(t, stream.Send(&pb.AppendRequest{Journal: "a/journal"}))
	assert.NoError(t, stream.Send(&pb.AppendRequest{Content: []byte("foo")}))
	assert.NoError(t, stream.Send(&pb.AppendRequest{Content: []byte("bar")}))
	assert.NoError(t, stream.Send(&pb.AppendRequest{})) // Intend to commit.
	assert.NoError(t, stream.CloseSend())               // Commit.

	// Expect the client stream receives an AppendResponse.
	resp, err := stream.CloseAndRecv()
	assert.NoError(t, err)

	assert.Equal(t, &pb.AppendResponse{
		Status: pb.Status_OK,
		Header: *broker.header("a/journal"),
		Commit: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            0,
			End:              6,
			Sum:              pb.SHA1SumOf("foobar"),
			CompressionCodec: pb.CompressionCodec_SNAPPY,
		},
		Registers: boxLabels(),
	}, resp)

	broker.cleanup()
}

func TestAppendRegisterCheckAndUpdateSequence(t *testing.T) {
	var ctx, etcd = pb.WithDispatchDefault(context.Background()), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 1}, broker.id)
	broker.initialFragmentLoad()

	var selector = func(s string) *pb.LabelSelector {
		var sel, err = pb.ParseLabelSelector(s)
		require.NoError(t, err)
		return &sel
	}
	// Run a sequence of appends, where each confirms and modifies registers.
	var cases = []struct {
		request pb.AppendRequest
		status  pb.Status
		expect  *pb.LabelSet
	}{
		{
			request: pb.AppendRequest{UnionRegisters: boxLabels("AA", "11", "BB", "22")},
			expect:  boxLabels("AA", "11", "BB", "22"),
		},
		{
			request: pb.AppendRequest{
				CheckRegisters:    selector("AA in (11), BB"),
				SubtractRegisters: boxLabels("AA", "11", "BB", "XYZ"),
				UnionRegisters:    boxLabels("CC", "33"),
			},
			expect: boxLabels("BB", "22", "CC", "33"),
		},
		{
			request: pb.AppendRequest{
				CheckRegisters:    selector("!AA, CC"),
				SubtractRegisters: boxLabels("CC", "33"),
				UnionRegisters:    boxLabels("AA", "44"),
			},
			expect: boxLabels("AA", "44", "BB", "22"),
		},
		{
			request: pb.AppendRequest{
				CheckRegisters: selector("AA = 55"),
				UnionRegisters: boxLabels("ZZZ", ""),
			},
			status: pb.Status_REGISTER_MISMATCH,
			expect: boxLabels("AA", "44", "BB", "22"),
		},
	}

	for _, tc := range cases {
		tc.request.Journal = "a/journal"

		var stream, err = broker.client().Append(ctx)
		assert.NoError(t, err)
		assert.NoError(t, stream.Send(&tc.request))
		assert.NoError(t, stream.Send(&pb.AppendRequest{Content: []byte("bb")}))
		assert.NoError(t, stream.Send(&pb.AppendRequest{})) // Intend to commit.
		assert.NoError(t, stream.CloseSend())               // Commit.

		resp, err := stream.CloseAndRecv()
		assert.NoError(t, err)
		assert.Equal(t, tc.status, resp.Status)
		assert.Equal(t, *broker.header("a/journal"), resp.Header)
		assert.Equal(t, tc.expect, resp.Registers)
	}

	broker.cleanup()
}

func TestAppendBadlyBehavedClientCases(t *testing.T) {
	var ctx, etcd = pb.WithDispatchDefault(context.Background()), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 1}, broker.id)
	broker.initialFragmentLoad()

	// Case: client explicitly rolls back by closing the stream without first
	// sending an empty chunk.
	var stream, _ = broker.client().Append(ctx)
	assert.NoError(t, stream.Send(&pb.AppendRequest{Journal: "a/journal"}))
	assert.NoError(t, stream.Send(&pb.AppendRequest{Content: []byte("foo")}))
	assert.NoError(t, stream.CloseSend()) // EOF without commit intent.

	// Expect the client reads an error.
	var _, err = stream.CloseAndRecv()
	assert.EqualError(t, err,
		`rpc error: code = Unknown desc = append stream: unexpected EOF`)

	// Case: client takes too long sending data.
	var restoreTimeout = func(d time.Duration) func() {
		appendChunkTimeout = time.Microsecond
		return func() { appendChunkTimeout = d }
	}(appendChunkTimeout)

	stream, _ = broker.client().Append(ctx)
	assert.NoError(t, stream.Send(&pb.AppendRequest{Journal: "a/journal"}))
	assert.NoError(t, stream.Send(&pb.AppendRequest{Content: []byte("foo")}))

	err = stream.RecvMsg(new(pb.AppendResponse)) // Block until server cancels.
	assert.EqualError(t, err,
		`rpc error: code = Unknown desc = append stream: context deadline exceeded`)
	restoreTimeout()

	// Case: client read error occurs.
	var failCtx, failCtxCancel = context.WithCancel(ctx)

	stream, _ = broker.client().Append(failCtx)
	assert.NoError(t, stream.Send(&pb.AppendRequest{Journal: "a/journal"}))
	assert.NoError(t, stream.Send(&pb.AppendRequest{Content: []byte("foo")}))
	failCtxCancel() // Cancel the stream.

	_, err = stream.CloseAndRecv()
	assert.EqualError(t, err, `rpc error: code = Canceled desc = context canceled`)

	// Case: client attempts register modification but appends no data.
	stream, _ = broker.client().Append(ctx)
	assert.NoError(t, stream.Send(&pb.AppendRequest{
		Journal:        "a/journal",
		UnionRegisters: boxLabels("foo", ""),
	}))
	assert.NoError(t, stream.Send(&pb.AppendRequest{})) // Intend to commit.
	assert.NoError(t, stream.CloseSend())               // Commit.

	_, err = stream.CloseAndRecv()
	assert.EqualError(t, err, `rpc error: code = Unknown desc = append stream: `+
		`register modification requires non-empty Append`)

	broker.cleanup()
}

func TestAppendRequestErrorCases(t *testing.T) {
	var ctx, etcd = pb.WithDispatchDefault(context.Background()), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})

	// Case: AppendRequest which fails to validate.
	var stream, _ = broker.client().Append(ctx)
	assert.NoError(t, stream.Send(&pb.AppendRequest{Journal: "/invalid/name"}))

	var _, err = stream.CloseAndRecv()
	assert.EqualError(t, err, `rpc error: code = Unknown desc = Journal: cannot begin with '/' (/invalid/name)`)

	// Case: Journal doesn't exist.
	stream, _ = broker.client().Append(ctx)
	assert.NoError(t, stream.Send(&pb.AppendRequest{Journal: "does/not/exist"}))

	resp, err := stream.CloseAndRecv()
	assert.NoError(t, err)
	assert.Equal(t, &pb.AppendResponse{
		Status: pb.Status_JOURNAL_NOT_FOUND,
		Header: *broker.header("does/not/exist"),
	}, resp)

	// Case: Journal with no assigned primary.
	// Arrange Journal assignment fixture such that R=1, but the broker has Slot=1 (and is not primary).
	setTestJournal(broker, pb.JournalSpec{Name: "no/primary", Replication: 1}, pb.ProcessSpec_ID{}, broker.id)
	stream, _ = broker.client().Append(ctx)
	assert.NoError(t, stream.Send(&pb.AppendRequest{Journal: "no/primary"}))

	resp, err = stream.CloseAndRecv()
	assert.NoError(t, err)
	assert.Equal(t, &pb.AppendResponse{
		Status: pb.Status_NO_JOURNAL_PRIMARY_BROKER,
		Header: *broker.header("no/primary"),
	}, resp)

	// Case: Journal with a primary but not enough replication peers.
	setTestJournal(broker, pb.JournalSpec{Name: "not/enough/peers", Replication: 2}, broker.id)
	stream, _ = broker.client().Append(ctx)
	assert.NoError(t, stream.Send(&pb.AppendRequest{Journal: "not/enough/peers"}))

	resp, err = stream.CloseAndRecv()
	assert.NoError(t, err)
	assert.Equal(t, &pb.AppendResponse{
		Status: pb.Status_INSUFFICIENT_JOURNAL_BROKERS,
		Header: *broker.header("not/enough/peers"),
	}, resp)

	// Case: Journal which is marked as read-only.
	setTestJournal(broker, pb.JournalSpec{Name: "read/only", Replication: 1, Flags: pb.JournalSpec_O_RDONLY}, broker.id)
	broker.initialFragmentLoad()
	stream, _ = broker.client().Append(ctx)
	assert.NoError(t, stream.Send(&pb.AppendRequest{Journal: "read/only"}))
	assert.NoError(t, stream.Send(&pb.AppendRequest{Content: []byte("foo")}))

	resp, err = stream.CloseAndRecv()
	assert.NoError(t, err)
	assert.Equal(t, &pb.AppendResponse{
		Status: pb.Status_NOT_ALLOWED,
		Header: *broker.header("read/only"),
	}, resp)

	// Case: incorrect request Offset.
	setTestJournal(broker, pb.JournalSpec{Name: "valid/journal", Replication: 1}, broker.id)
	// Initial fragment index load with a non-empty Fragment fixture.
	broker.replica("valid/journal").index.ReplaceRemote(fragment.CoverSet{
		fragment.Fragment{Fragment: pb.Fragment{End: 1024}},
	})
	stream, _ = broker.client().Append(ctx)
	assert.NoError(t, stream.Send(&pb.AppendRequest{Journal: "valid/journal", Offset: 512}))

	resp, err = stream.CloseAndRecv()
	assert.NoError(t, err)
	assert.Equal(t, &pb.AppendResponse{
		Status: pb.Status_WRONG_APPEND_OFFSET,
		Header: *broker.header("read/only"),
	}, resp)

	broker.cleanup()
}

func TestAppendProxyCases(t *testing.T) {
	var ctx, etcd = pb.WithDispatchDefault(context.Background()), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	var peer = newMockBroker(t, etcd, pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"})
	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 1}, peer.id)

	var expectHeader = broker.header("a/journal")
	expectHeader.ProcessId = peer.id

	// Case: initial request is proxied to the peer, with Header attached.
	var stream, _ = broker.client().Append(ctx)
	assert.NoError(t, stream.Send(&pb.AppendRequest{Journal: "a/journal"}))
	assert.Equal(t, pb.AppendRequest{
		Journal: "a/journal",
		Header:  expectHeader,
	}, <-peer.AppendReqCh)

	// Expect client content and EOF are proxied.
	assert.NoError(t, stream.Send(&pb.AppendRequest{Content: []byte("foobar")}))
	assert.Equal(t, pb.AppendRequest{Content: []byte("foobar")}, <-peer.AppendReqCh)
	assert.NoError(t, stream.Send(&pb.AppendRequest{}))
	assert.Equal(t, pb.AppendRequest{}, <-peer.AppendReqCh)
	assert.NoError(t, stream.CloseSend())
	assert.Equal(t, io.EOF, <-peer.ReadLoopErrCh) // Peer reads proxied EOF.

	// Expect peer's response is proxied back to the client.
	peer.AppendRespCh <- pb.AppendResponse{Commit: &pb.Fragment{Begin: 1234, End: 5678}}

	var resp, err = stream.CloseAndRecv()
	assert.NoError(t, err)
	assert.Equal(t, &pb.AppendResponse{Commit: &pb.Fragment{Begin: 1234, End: 5678}}, resp)

	// Case: proxied request fails due to broker failure.
	stream, _ = broker.client().Append(ctx)
	assert.NoError(t, stream.Send(&pb.AppendRequest{Journal: "a/journal"}))

	assert.Equal(t, pb.AppendRequest{
		Journal: "a/journal",
		Header:  expectHeader,
	}, <-peer.AppendReqCh)

	// Expect peer error is proxied back to client.
	go func() {
		assert.Equal(t, io.EOF, <-peer.ReadLoopErrCh) // Peer reads proxy EOF.
		peer.WriteLoopErrCh <- errors.New("some kind of error")
	}()
	_, err = stream.CloseAndRecv()
	assert.EqualError(t, err, `rpc error: code = Unknown desc = some kind of error`)

	// Case: proxy request fails due to client cancellation.
	var failCtx, failCtxCancel = context.WithCancel(ctx)

	stream, _ = broker.client().Append(failCtx)
	assert.NoError(t, stream.Send(&pb.AppendRequest{Journal: "a/journal"}))

	// Peer reads opening request, and then client stream is cancelled.
	assert.Equal(t, pb.AppendRequest{Journal: "a/journal", Header: expectHeader},
		<-peer.AppendReqCh)
	failCtxCancel()

	// Expect cancellation is propagated to peer.
	switch err = <-peer.ReadLoopErrCh; err {
	case context.Canceled, io.EOF:
		// Both errors are valid, and which is read depends on a gRPC race
		// (does cancellation propagate to the stream first, or does the EOF
		// sent by proxyAppend upon its reading an error?).
	default:
		assert.Fail(t, "unexpected ReadLoop err", "err", err)
	}
	peer.WriteLoopErrCh <- nil // Peer RPC completes.

	_, err = stream.CloseAndRecv()
	assert.EqualError(t, err, `rpc error: code = Canceled desc = context canceled`)

	broker.cleanup()
	peer.Cleanup()
}
