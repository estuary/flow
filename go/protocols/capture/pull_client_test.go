package capture

import (
	"context"
	"encoding/json"
	fmt "fmt"
	"io"
	"io/ioutil"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/broker/client"
)

//go:generate flowctl-go api build --build-id temp.db --directory testdata/ --source testdata/flow.yaml
//go:generate sqlite3 file:testdata/temp.db "SELECT WRITEFILE('testdata/capture.proto', spec) FROM built_captures WHERE capture = 'acmeCo/source-hello-world';"

func TestPullClientLifecycle(t *testing.T) {
	var specBytes, err = ioutil.ReadFile("testdata/capture.proto")
	require.NoError(t, err)
	var spec pf.CaptureSpec
	require.NoError(t, spec.Unmarshal(specBytes))

	var ctx = context.Background()
	var server = &testServer{DoneOp: client.NewAsyncOperation()}
	var conn = AdaptServerToClient(server)
	var captured []json.RawMessage
	var reducedCheckpoint pf.DriverCheckpoint
	var startCommitCh = make(chan error)

	rpc, err := OpenPull(
		ctx,
		conn,
		json.RawMessage(`{"driver":"checkpoint"}`),
		func(*pf.CaptureSpec_Binding) (pf.Combiner, error) {
			return new(pf.MockCombiner), nil
		},
		pf.NewFullRange(),
		&spec,
		"a-version",
		true,
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

		require.NoError(t, reducedCheckpoint.Reduce(checkpoint))
		return fmt.Sprintf("%d => %s", n, string(reducedCheckpoint.DriverCheckpointJson))
	}

	server.sendDocs(0, "one", "two")
	server.sendCheckpoint(map[string]int{"a": 1})

	// Expect Read notified our callback.
	require.NoError(t, <-startCommitCh)
	require.Equal(t, `2 => {"a":1}`, drain())

	// Tell Read of a pending log commit.
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
	// Of the three following checkpoints, the first and second but not third will
	// be combined into one commit.
	defer func(i int) { combinerByteThreshold = i }(combinerByteThreshold)
	combinerByteThreshold = 20 // Characters plus enclosing quotes.

	// While this commit runs, the server sends more documents and checkpoints.
	server.sendDocs(0, "six", "seven")
	server.sendDocs(0, "eight")
	server.sendCheckpoint(map[string]int{"c": 1})
	server.sendDocs(0, "nine")
	server.sendCheckpoint(map[string]int{"b": 2})
	server.sendDocs(0, "ten")
	server.sendCheckpoint(map[string]int{"d": 1})
	// Then it closes without waiting for our Acknowledge.
	server.DoneOp.Resolve(nil)

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
	require.Equal(t, `4 => {"a":2,"b":2,"c":1}`, drain())

	commitOp = client.NewAsyncOperation()
	require.NoError(t, rpc.SetLogCommitOp(commitOp))
	commitOp.Resolve(nil)

	// Final commit, which rolls up two checkpoints
	require.NoError(t, <-startCommitCh)
	require.Equal(t, `1 => {"a":2,"b":2,"c":1,"d":1}`, drain())

	commitOp = client.NewAsyncOperation()
	require.NoError(t, rpc.SetLogCommitOp(commitOp))
	commitOp.Resolve(nil)

	// We're notified of the close.
	require.Equal(t, io.EOF, <-startCommitCh)
	// The client closes gracefully.
	require.NoError(t, rpc.Close())
	// A further attempt to set a LogCommitOp errors, since Read() is no longer listening.
	require.Equal(t, io.EOF, rpc.SetLogCommitOp(client.NewAsyncOperation()))

	// Consume (raced) Acknowledge messages received by the server
	// from our client, as server cancellation propagated.
	for server.recvAck() == nil {
	}
	// Expect the client closed its connection, and the server reads EOF.
	require.Equal(t, io.EOF, server.recvAck())

	// Snapshot the recorded observations of the Open and drains.
	cupaloy.SnapshotT(t,
		"OPEN:", server.OpenRx,
		"DRIVER CHECKPOINT:", reducedCheckpoint,
		"CAPTURED", captured,
	)
}

func TestPullClientCancel(t *testing.T) {
	var specBytes, err = ioutil.ReadFile("testdata/capture.proto")
	require.NoError(t, err)
	var spec pf.CaptureSpec
	require.NoError(t, spec.Unmarshal(specBytes))

	var server = &testServer{DoneOp: client.NewAsyncOperation()}
	var conn = AdaptServerToClient(server)
	var startCommitCh = make(chan error)

	// Cause PullClient to consider a transaction "full" after one document.
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
		var ctx, cancelFn = context.WithCancel(context.Background())

		rpc, err := OpenPull(
			ctx,
			conn,
			json.RawMessage(`{"driver":"checkpoint"}`),
			func(*pf.CaptureSpec_Binding) (pf.Combiner, error) {
				return new(pf.MockCombiner), nil
			},
			pf.NewFullRange(),
			&spec,
			"a-version",
			true,
			func(err error) { startCommitCh <- err },
		)
		require.NoError(t, err)

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
			require.NoError(t, rpc.Close())
			require.Equal(t, io.EOF, <-startCommitCh)
		} else {
			require.Equal(t, context.Canceled, rpc.Close())
			require.Equal(t, context.Canceled, <-startCommitCh)
		}
	}
}

type testServer struct {
	OpenRx   PullRequest_Open
	OpenedTx PullResponse_Opened
	Stream   Driver_PullServer
	DoneOp   *client.AsyncOperation
}

func makeDocs(binding uint32, docs ...interface{}) *Documents {
	var m = &Documents{Binding: binding}

	for _, d := range docs {
		var b, err = json.Marshal(d)
		if err != nil {
			panic(err)
		}
		m.DocsJson = append(m.DocsJson, m.Arena.Add(b))
	}
	return m
}

func (t *testServer) sendDocs(binding uint32, docs ...interface{}) error {
	return t.Stream.Send(&PullResponse{Documents: makeDocs(binding, docs...)})
}

func makeCheckpoint(body interface{}) *pf.DriverCheckpoint {
	var b, err = json.Marshal(body)
	if err != nil {
		panic(err)
	}
	return &pf.DriverCheckpoint{
		DriverCheckpointJson: b,
		Rfc7396MergePatch:    true,
	}
}

func (t *testServer) sendCheckpoint(body interface{}) error {
	return t.Stream.Send(&PullResponse{Checkpoint: makeCheckpoint(body)})
}

func (t *testServer) recvAck() error {
	var m, err = t.Stream.Recv()
	if err != nil {
		return err
	} else if m.Acknowledge == nil {
		return fmt.Errorf("expected Acknowledge")
	}
	return nil
}

var _ DriverServer = &testServer{}

func (t *testServer) Spec(context.Context, *SpecRequest) (*SpecResponse, error) {
	panic("not called")
}
func (t *testServer) Discover(context.Context, *DiscoverRequest) (*DiscoverResponse, error) {
	panic("not called")
}
func (t *testServer) Validate(context.Context, *ValidateRequest) (*ValidateResponse, error) {
	panic("not called")
}
func (t *testServer) ApplyUpsert(context.Context, *ApplyRequest) (*ApplyResponse, error) {
	panic("not called")
}
func (t *testServer) ApplyDelete(context.Context, *ApplyRequest) (*ApplyResponse, error) {
	panic("not called")
}

func (t *testServer) Pull(stream Driver_PullServer) error {
	t.Stream = stream

	open, err := stream.Recv()
	if err != nil {
		return err
	} else if err := open.Validate(); err != nil {
		return err
	} else if open.Open == nil {
		return fmt.Errorf("expected Open got %v", open)
	}

	t.OpenRx = *open.Open
	if err := stream.Send(&PullResponse{Opened: &t.OpenedTx}); err != nil {
		return err
	}

	select {
	case <-t.DoneOp.Done():
		return t.DoneOp.Err()
	case <-stream.Context().Done():
		return nil // Client cancelled.
	}
}
