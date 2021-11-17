package capture

import (
	"context"
	"encoding/json"
	fmt "fmt"
	"io"
	"io/ioutil"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	pf "github.com/estuary/protocols/flow"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/broker/client"
)

//go:generate flowctl api build --build-id temp.db --directory testdata/ --source testdata/flow.yaml
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
	)
	require.NoError(t, err)

	// drain takes Combined documents from the MockCombiner, appending them into
	// |captured|, and reduces the driver checkpoint into |reducedCheckpoint|.
	// It models the caller's expected behavior of producing captured documents
	// into a collection upon notification.
	var drain = func() {
		var combiner = rpc.Combiners()[0].(*pf.MockCombiner)
		captured = append(captured, combiner.Combined...)
		combiner.Combined = nil

		require.NoError(t, reducedCheckpoint.Reduce(rpc.DriverCheckpoint()))
	}

	var startCommitCh = make(chan error)
	go rpc.Read(func(err error) { startCommitCh <- err })

	server.sendDocs("one", "two")
	server.sendCheckpoint(map[string]int{"a": 1})

	// Expect Read notified our callback.
	require.NoError(t, <-startCommitCh)
	drain()

	// Tell Read of a pending log commit.
	var commitOp = client.NewAsyncOperation()
	require.NoError(t, rpc.SetLogCommitOp(commitOp))

	// More docs and a checkpoint, along with a recovery log commit.
	// Note these race within the Read() loop and we can't guarantee a specific
	// ordering between RPC reads and the commit being observed by Read().
	// It doesn't matter, because the client will release documents only after a
	// checkpoint is read, and only after |commitOp| is notified.
	server.sendDocs("three")
	commitOp.Resolve(nil)
	server.sendDocs("four", "five")
	server.sendCheckpoint(map[string]int{"b": 1})

	// Expect Acknowledge was sent to the RPC.
	require.NoError(t, server.recvAck())

	// We were notified that the next commit is ready.
	require.NoError(t, <-startCommitCh)
	drain()

	commitOp = client.NewAsyncOperation()
	require.NoError(t, rpc.SetLogCommitOp(commitOp))
	commitOp.Resolve(nil)
	require.NoError(t, server.recvAck())

	// A Checkpoint without Documents is also valid.
	server.sendCheckpoint(map[string]int{"a": 2})

	require.NoError(t, <-startCommitCh)
	drain()

	// While this commit runs, the server sends more documents and checkpoints.
	server.sendDocs("six", "seven")
	server.sendDocs("eight")
	server.sendCheckpoint(map[string]int{"c": 1})
	server.sendDocs("nine")
	server.sendCheckpoint(map[string]int{"b": 2})
	// Then it closes without waiting for our Acknowledge.
	server.DoneOp.Resolve(nil)

	// We finally get around to sending a |commitOp|, and it resolves.
	commitOp = client.NewAsyncOperation()
	require.NoError(t, rpc.SetLogCommitOp(commitOp))
	commitOp.Resolve(nil)

	// Expect we're notified of a last commit, which rolls up both checkpoints.
	require.NoError(t, <-startCommitCh)
	drain()

	// A further attempt to set a LogCommitOp errors, since Read() is no longer listening.
	require.Equal(t, io.EOF, rpc.SetLogCommitOp(client.NewAsyncOperation()))
	// We're notified of the close.
	require.Equal(t, io.EOF, <-startCommitCh)
	// The client closes gracefully.
	require.NoError(t, rpc.Close())

	// Snapshot the recorded observations of the Open and drains.
	cupaloy.SnapshotT(t,
		"OPEN:", server.OpenRx,
		"DRIVER CHECKPOINT:", reducedCheckpoint,
		"CAPTURED", captured,
	)
}

type testServer struct {
	OpenRx   PullRequest_Open
	OpenedTx PullResponse_Opened
	Stream   Driver_PullServer
	DoneOp   *client.AsyncOperation
}

func (t *testServer) sendDocs(docs ...interface{}) error {
	var m = &Documents{Binding: 0}

	for _, d := range docs {
		var b, err = json.Marshal(d)
		if err != nil {
			return err
		}
		m.DocsJson = append(m.DocsJson, m.Arena.Add(b))
	}
	return t.Stream.Send(&PullResponse{Documents: m})
}

func (t *testServer) sendCheckpoint(body interface{}) error {
	var b, err = json.Marshal(body)
	if err != nil {
		return err
	}
	return t.Stream.Send(&PullResponse{
		Checkpoint: &pf.DriverCheckpoint{
			DriverCheckpointJson: b,
			Rfc7396MergePatch:    true,
		},
	})
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

	return t.DoneOp.Err()
}
