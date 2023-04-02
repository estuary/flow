package capture

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/server"
	"go.gazette.dev/core/task"
)

//go:generate flowctl-go api build --build-id test-build --build-db testdata/temp.db --source testdata/flow.yaml
//go:generate sqlite3 file:testdata/temp.db "SELECT WRITEFILE('testdata/capture.proto', spec) FROM built_captures WHERE capture = 'acmeCo/source-test';"

func TestPullClientLifecycle(t *testing.T) {
	var specBytes, err = os.ReadFile("testdata/capture.proto")
	require.NoError(t, err)
	var spec pf.CaptureSpec
	require.NoError(t, spec.Unmarshal(specBytes))

	var server = newTestServer(t)
	server.openedTx = Response_Opened{ExplicitAcknowledgements: true}

	var captured []json.RawMessage
	var reducedCheckpoint pf.ConnectorState
	var startCommitCh = make(chan error)

	rpc, err := Open(
		server.group.Context(),
		server.Client(),
		json.RawMessage(`{"driver":"checkpoint"}`),
		func(*pf.CaptureSpec_Binding) (pf.Combiner, error) {
			return new(pf.MockCombiner), nil
		},
		pf.NewFullRange(),
		&spec,
		"a-version",
		func(err error) { startCommitCh <- err },
	)
	require.NoError(t, err)

	// drain takes Combined documents from the MockCombiner, appending them into
	// |captured|, and reduces the driver checkpoint into |reducedCheckpoint|.
	// It models the caller's expected behavior of producing captured documents
	// into a collection upon notification.
	var drain = func() string {
		var combiners, checkpoint = rpc.PopTransaction()

		var combiner = combiners[0].(*pf.MockCombiner)
		var n = len(combiner.Combined)
		captured = append(captured, combiner.Combined...)
		combiner.Combined = nil

		if checkpoint != nil {
			require.NoError(t, reducedCheckpoint.Reduce(*checkpoint))
		}
		return fmt.Sprintf("%d => %s", n, string(reducedCheckpoint.UpdatedJson))
	}

	server.sendDocs(0, "one", "two")
	server.sendCheckpoint(map[string]int{"a": 1})

	// Expect Read notified our callback.
	require.NoError(t, <-startCommitCh)
	require.Equal(t, `2 => {"a":1}`, drain())

	// Tell Client of a pending log commit.
	var commitOp = client.NewAsyncOperation()
	require.NoError(t, rpc.SetLogCommitOp(commitOp))

	// More docs and a checkpoint, along with a recovery log commit.
	// Note these race within the Read() loop and we can't guarantee a specific
	// ordering between RPC reads and the commit being observed by Read().
	// It doesn't matter, because the client will release documents only after a
	// checkpoint is read, and only after |commitOp| is notified.
	server.sendDocs(0, "three")
	commitOp.Resolve(nil)
	server.sendDocs(0, "four", "five")
	server.sendCheckpoint(map[string]int{"b": 1})

	// Expect Acknowledge was sent to the RPC.
	require.NoError(t, server.recvAck())

	// We were notified that the next commit is ready.
	require.NoError(t, <-startCommitCh)
	require.Equal(t, `3 => {"a":1,"b":1}`, drain())

	commitOp = client.NewAsyncOperation()
	require.NoError(t, rpc.SetLogCommitOp(commitOp))
	commitOp.Resolve(nil)
	require.NoError(t, server.recvAck())

	// A Checkpoint without Documents is also valid.
	server.sendCheckpoint(map[string]int{"a": 2})

	require.NoError(t, <-startCommitCh)
	require.Equal(t, `0 => {"a":2,"b":1}`, drain())

	// Lower the threshold under which we'll combine multiple checkpoints of documents.
	// Of the three following checkpoints, the first and then the second and third will
	// be combined into one commit.
	defer func(i int) { combinerByteThreshold = i }(combinerByteThreshold)
	combinerByteThreshold = 10 // Characters plus enclosing quotes.

	// While this commit runs, the server sends more documents and checkpoints.
	server.sendDocs(0, "six", "seven")
	server.sendDocs(0, "eight")
	server.sendCheckpoint(map[string]int{"c": 1})
	server.sendDocs(0, "nine")
	server.sendCheckpoint(map[string]int{"b": 2})
	server.sendDocs(0, "ten")
	server.sendCheckpoint(map[string]int{"d": 1})
	// Then it fails without waiting for our Acknowledge.
	server.captureDoneOp.Resolve(fmt.Errorf("crash!"))

	// Time for propagation.
	// TODO(johnny): This is a bit racy. We cannot absolutely
	// guarantee that the Client's serve() loop will process
	// the third checkpoint along with the second, prior to the second
	// to-last commit op being resolved. It's likely but not
	// guaranteed.
	time.Sleep(time.Millisecond * 5)

	// Note the server-side of the RPC may now asynchronously exit,
	// which (eventually) invalidates our ability to send Acknowledge
	// messages. Our client will continue to attempt to send them,
	// but swallows EOFs due to server closure, and we can't say how many
	// Acknowledges will get through as server cancellation
	// propagation races the following client-side commits:

	// We finally get around to sending a |commitOp|, and it resolves.
	commitOp = client.NewAsyncOperation()
	require.NoError(t, rpc.SetLogCommitOp(commitOp))
	commitOp.Resolve(nil)

	// Expect we're notified of a 2nd to last commit.
	require.NoError(t, <-startCommitCh)
	require.Equal(t, `3 => {"a":2,"b":1,"c":1}`, drain())

	time.Sleep(time.Millisecond * 5) // More time for propagation.

	commitOp = client.NewAsyncOperation()
	require.NoError(t, rpc.SetLogCommitOp(commitOp))
	commitOp.Resolve(nil)

	// Final commit, which rolls up two checkpoints
	require.NoError(t, <-startCommitCh)
	require.Equal(t, `2 => {"a":2,"b":2,"c":1,"d":1}`, drain())

	commitOp = client.NewAsyncOperation()
	require.NoError(t, rpc.SetLogCommitOp(commitOp))
	commitOp.Resolve(nil)

	// We're notified of the server failure.
	require.EqualError(t, <-startCommitCh, "rpc error: code = Unknown desc = crash!")
	// The client closes gracefully.
	rpc.Close()
	// A further attempt to set a LogCommitOp errors, since serve() is no longer running.
	require.NotNil(t, rpc.SetLogCommitOp(client.NewAsyncOperation()))

	// Consume (raced) Acknowledge messages received by the server
	// from our client, as server cancellation propagated.
	for server.recvAck() == nil {
	}

	// Snapshot the recorded observations of the Open and drains.
	cupaloy.SnapshotT(t,
		"OPEN:", server.openRx,
		"DRIVER CHECKPOINT:", reducedCheckpoint,
		"CAPTURED", captured,
	)
}

func TestPullClientCancel(t *testing.T) {
	var specBytes, err = os.ReadFile("testdata/capture.proto")
	require.NoError(t, err)
	var spec pf.CaptureSpec
	require.NoError(t, spec.Unmarshal(specBytes))

	// Cause Client to consider a transaction "full" after one document.
	defer func(i int) { combinerByteThreshold = i }(combinerByteThreshold)
	combinerByteThreshold = 1

	// Vary the test based on the number of checkpoints pulled before
	// a cancellation is delivered:
	// 0: The PullClient is idle and gracefully EOF's when the RPC channel is closed.
	// 1: The PullClient has a pending transaction and reads RPC channel to closure,
	//    then fails with ErrContextCancelled.
	// 2: The PullClient has a pending transaction, reads the second document,
	//    and stops reading the RPC channel. It fails with ErrContextCancelled.
	for numDocs := 0; numDocs != 3; numDocs++ {
		var server = newTestServer(t)
		var startCommitCh = make(chan error)
		var ctx, cancelFn = context.WithCancel(server.group.Context())

		rpc, err := Open(
			ctx,
			server.Client(),
			json.RawMessage(`{"driver":"checkpoint"}`),
			func(*pf.CaptureSpec_Binding) (pf.Combiner, error) {
				return new(pf.MockCombiner), nil
			},
			pf.NewFullRange(),
			&spec,
			"a-version",
			func(err error) { startCommitCh <- err },
		)
		require.NoError(t, err)

		// Expect the client sends an immediate EOF,
		// because the server didn't elect for acknowledgements.
		require.Equal(t, io.EOF, server.recvAck())

		for i := 0; i != numDocs; i++ {
			server.sendDocs(0, "one", "two")
			server.sendCheckpoint(map[string]int{"a": 1})
		}
		if numDocs != 0 {
			require.NoError(t, <-startCommitCh)
		}

		// If the PullClient is immediately cancelled without being used,
		// it should still tear down correctly.
		cancelFn()

		if numDocs == 0 {
			require.Equal(t, io.EOF, <-startCommitCh)
		} else {
			require.Equal(t, context.Canceled, <-startCommitCh)
		}
		rpc.Close() // Closes gracefully.
	}
}

type testServer struct {
	group  *task.Group
	server *server.Server

	// Resolved to finish (EOF) an ongoing Capture RPC.
	captureDoneOp *client.AsyncOperation
	// Open request read by Capture RPC.
	openRx Request_Open
	// Opened response sent by Capture RPC.
	openedTx Response_Opened
	// Server Capture RPC.
	rpc Connector_CaptureServer
}

func newTestServer(t *testing.T) *testServer {
	pb.RegisterGRPCDispatcher("local")

	var instance = &testServer{
		group:         task.NewGroup(pb.WithDispatchDefault(context.Background())),
		server:        server.MustLoopback(),
		captureDoneOp: client.NewAsyncOperation(),
	}
	RegisterConnectorServer(instance.server.GRPCServer, instance)
	instance.server.QueueTasks(instance.group)

	t.Cleanup(func() {
		instance.group.Cancel()
		instance.server.BoundedGracefulStop()
		require.NoError(t, instance.group.Wait())
	})
	instance.group.GoRun()

	return instance
}

func (t *testServer) Client() ConnectorClient {
	return NewConnectorClient(t.server.GRPCLoopback)
}

var _ ConnectorServer = &testServer{}

func (t *testServer) Capture(rpc Connector_CaptureServer) error {
	t.rpc = rpc

	open, err := t.rpc.Recv()
	if err != nil {
		return err
	} else if open.Open == nil {
		return fmt.Errorf("expected Open got %v", open)
	}

	t.openRx = *open.Open
	if err := t.rpc.Send(&Response{Opened: &t.openedTx}); err != nil {
		return err
	}

	select {
	case <-t.captureDoneOp.Done():
		return t.captureDoneOp.Err()
	case <-t.rpc.Context().Done():
		return nil // Client cancelled.
	}
}

func (t *testServer) sendDocs(binding uint32, docs ...interface{}) {
	for _, doc := range docs {
		var b, err = json.Marshal(doc)
		if err != nil {
			panic(err)
		}

		if err := t.rpc.Send(&Response{
			Captured: &Response_Captured{
				Binding: binding,
				DocJson: b,
			},
		}); err != nil {
			panic(err)
		}
	}
}

func (t *testServer) sendCheckpoint(body interface{}) error {
	var b, err = json.Marshal(body)
	if err != nil {
		panic(err)
	}

	return t.rpc.Send(&Response{Checkpoint: &Response_Checkpoint{
		State: &pf.ConnectorState{
			UpdatedJson: b,
			MergePatch:  true,
		},
	}})
}

func (t *testServer) recvAck() error {
	var m, err = t.rpc.Recv()
	if err != nil {
		return err
	} else if m.Acknowledge == nil {
		return fmt.Errorf("expected Acknowledge")
	}
	return nil
}
