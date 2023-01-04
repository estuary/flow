package materialize

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/server"
	"go.gazette.dev/core/task"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

//go:generate flowctl-go api build --build-id temp.db --directory testdata/ --source testdata/flow.yaml
//go:generate sqlite3 file:testdata/temp.db "SELECT WRITEFILE('testdata/materialization.proto', spec) FROM built_materializations WHERE materialization = 'test/sqlite';"

func TestIntegratedTransactorAndClient(t *testing.T) {
	var specBytes, err = ioutil.ReadFile("testdata/materialization.proto")
	require.NoError(t, err)
	var spec pf.MaterializationSpec
	require.NoError(t, spec.Unmarshal(specBytes))

	pb.RegisterGRPCDispatcher("local")
	var (
		server    = server.MustLoopback()
		taskGroup = task.NewGroup(pb.WithDispatchDefault(context.Background()))
		service   = &testServer{
			Transactor: nil,
			OpenedTx: TransactionResponse_Opened{
				RuntimeCheckpoint: []byte("open-checkpoint"),
			},
		}
	)
	RegisterDriverServer(server.GRPCServer, service)
	server.QueueTasks(taskGroup)

	defer func() {
		taskGroup.Cancel()
		server.BoundedGracefulStop()
		taskGroup.Wait()
	}()
	taskGroup.GoRun()

	var openTransactions = func(combiner pf.Combiner, driverCheckpoint string) (*TxnClient, error) {
		return OpenTransactions(
			taskGroup.Context(),
			NewDriverClient(server.GRPCLoopback),
			json.RawMessage(driverCheckpoint),
			func(*pf.MaterializationSpec_Binding) (pf.Combiner, error) { return combiner, nil },
			pf.NewFullRange(),
			&spec,
			"a-version",
		)
	}

	t.Run("garden-path", func(t *testing.T) {
		var (
			transactor = &testTransactor{}
			combiner   = &pf.MockCombiner{}
		)
		service.Transactor = transactor

		rpc, err := openTransactions(combiner, `{"driver":"checkpoint"}`)
		require.NoError(t, err)
		require.Equal(t, "open-checkpoint", string(rpc.Opened().RuntimeCheckpoint))

		// Set a Loaded fixture to return, and load some documents.
		transactor.Loaded = map[int][]interface{}{
			0: {"found", "also-found"},
		}
		require.NoError(t, rpc.AddDocument(0, tuple.Tuple{1}.Pack(), json.RawMessage(`"one"`)))
		require.NoError(t, rpc.AddDocument(0, tuple.Tuple{2}.Pack(), json.RawMessage(`2`)))
		require.NoError(t, rpc.AddDocument(0, tuple.Tuple{"three"}.Pack(), json.RawMessage(`3`)))
		require.NoError(t, rpc.Flush(pf.Checkpoint{}))

		combiner.AddDrainFixture(false, "one", tuple.Tuple{1}, tuple.Tuple{"val", 1})
		combiner.AddDrainFixture(true, 2, tuple.Tuple{2}, tuple.Tuple{"val", 2})
		combiner.AddDrainFixture(false, 3, tuple.Tuple{"three"}, tuple.Tuple{"val", 3})

		stats, err := rpc.Store()
		require.NoError(t, err)
		require.NotNil(t, stats[0])

		// Set a StartCommit fixture to return, and start to commit.
		transactor.commitOp = client.NewAsyncOperation()
		transactor.StartedCommitTx.DriverCheckpointJson = json.RawMessage(`"driver-checkpoint"`)
		driverCheckpoint, opAcknowledged, err := rpc.StartCommit(pf.Checkpoint{
			Sources: map[pf.Journal]pc.Checkpoint_Source{"1st-flow-fixture": {ReadThrough: 1}}})
		require.NoError(t, err)
		require.Equal(t, `"driver-checkpoint"`, string(driverCheckpoint.DriverCheckpointJson))
		require.NoError(t, rpc.Acknowledge()) // Write Acknowledge.

		// Pipeline the next transaction.
		// Reset Loaded fixture, and load some documents.
		transactor.Loaded = map[int][]interface{}{
			0: {"2nd-round"},
		}
		require.NoError(t, rpc.AddDocument(0, tuple.Tuple{4}.Pack(), json.RawMessage(`"four"`)))

		transactor.commitOp.Resolve(nil)
		require.NoError(t, opAcknowledged.Err())       // Read Acknowledged.
		require.NoError(t, rpc.Flush(pf.Checkpoint{})) // Close Load phase.

		combiner.AddDrainFixture(true, "four", tuple.Tuple{4}, tuple.Tuple{"val", 4})

		stats, err = rpc.Store()
		require.NoError(t, err)
		require.NotNil(t, stats[0])

		// Set a StartCommit fixture to return, and start to commit.
		transactor.StartedCommitTx.DriverCheckpointJson = json.RawMessage(`"2nd-checkpoint"`)
		driverCheckpoint, opAcknowledged, err = rpc.StartCommit(pf.Checkpoint{
			Sources: map[pf.Journal]pc.Checkpoint_Source{"2nd-flow-fixture": {ReadThrough: 1234}}})
		require.NoError(t, err)
		require.Equal(t, `"2nd-checkpoint"`, string(driverCheckpoint.DriverCheckpointJson))
		require.NoError(t, rpc.Acknowledge())    // Write Acknowledge.
		require.NoError(t, opAcknowledged.Err()) // Read Acknowledged.

		// Clear the key cache, and switch to delta-updates mode.
		// Then pipeline the next delta-updates transaction.
		// TODO(johnny): Factor into new sub-test?
		rpc.flighted[0] = make(map[string]json.RawMessage)
		rpc.spec.Bindings[0].DeltaUpdates = true
		defer func() { rpc.spec.Bindings[0].DeltaUpdates = false }()

		transactor.loadNotExpected = true
		transactor.Loaded = nil

		// We do NOT expect to see Load requests for these documents in the snapshot.
		require.NoError(t, rpc.AddDocument(0, tuple.Tuple{"five"}.Pack(), json.RawMessage(`5`)))
		require.NoError(t, rpc.AddDocument(0, tuple.Tuple{"six"}.Pack(), json.RawMessage(`"six"`)))
		require.NoError(t, rpc.Flush(pf.Checkpoint{})) // Close Load phase.

		combiner.AddDrainFixture(true, "five", tuple.Tuple{"five"}, tuple.Tuple{"val", 5})
		combiner.AddDrainFixture(true, "six", tuple.Tuple{"six"}, tuple.Tuple{"val", 6})

		stats, err = rpc.Store()
		require.NoError(t, err)
		require.NotNil(t, stats[0])

		transactor.StartedCommitTx.DriverCheckpointJson = json.RawMessage(`"3rd-checkpoint"`)
		driverCheckpoint, opAcknowledged, err = rpc.StartCommit(pf.Checkpoint{
			Sources: map[pf.Journal]pc.Checkpoint_Source{"3rd-flow-fixture": {ReadThrough: 5678}}})
		require.NoError(t, err)
		require.Equal(t, `"3rd-checkpoint"`, string(driverCheckpoint.DriverCheckpointJson))
		require.NoError(t, rpc.Acknowledge())    // Write Acknowledge.
		require.NoError(t, opAcknowledged.Err()) // Read Acknowledged.

		// We can gracefully close the stream now.
		// This is only a clean close in this post-Acknowledged state.
		require.NoError(t, rpc.Close())

		transactor.commitOp = nil // Nil before snapshot.

		// Snapshot the recorded observations of the MockCombiner and testTransactor.
		cupaloy.SnapshotT(t, "COMBINER:", combiner, "TRANSACTOR:", transactor)

	})

	t.Run("no-op", func(t *testing.T) {
		var (
			transactor = &testTransactor{}
			combiner   = &pf.MockCombiner{}
		)
		service.Transactor = transactor

		var rpc, err = openTransactions(combiner, "{}")
		require.NoError(t, err)

		// Cleanly run through an empty transaction, then gracefully close.
		require.Nil(t, rpc.Flush(pf.Checkpoint{}))
		_, opAcknowledged, err := rpc.StartCommit(pf.Checkpoint{
			Sources: map[pf.Journal]pc.Checkpoint_Source{"foobar": {ReadThrough: 123}}})
		require.NoError(t, err)

		require.NoError(t, rpc.Acknowledge())
		require.NoError(t, opAcknowledged.Err())
		require.NoError(t, rpc.Close())
	})

	t.Run("load-error", func(t *testing.T) {
		var (
			transactor = &testTransactor{}
			combiner   = &pf.MockCombiner{}
		)
		service.Transactor = transactor

		var rpc, err = openTransactions(combiner, "{}")
		require.NoError(t, err)

		// Set a Loaded fixture to return, and load some documents.
		transactor.loadErr = fmt.Errorf("mysterious load failure")
		require.NoError(t, rpc.AddDocument(0, tuple.Tuple{1}.Pack(), json.RawMessage(`"one"`)))
		require.EqualError(t, rpc.Flush(pf.Checkpoint{}),
			"reading Loaded: transactor.Load: mysterious load failure")
	})

	t.Run("store-error", func(t *testing.T) {
		var (
			transactor = &testTransactor{}
			combiner   = &pf.MockCombiner{}
		)
		service.Transactor = transactor

		var rpc, err = openTransactions(combiner, "{}")
		require.NoError(t, err)
		require.Nil(t, rpc.Flush(pf.Checkpoint{}))

		transactor.storeErr = fmt.Errorf("mysterious store failure")
		_, _, err = rpc.StartCommit(pf.Checkpoint{
			Sources: map[pf.Journal]pc.Checkpoint_Source{"foobar": {ReadThrough: 123}}})
		require.EqualError(t, err, "reading StartedCommit: transactor.Store: mysterious store failure")
	})

	t.Run("start-commit-error", func(t *testing.T) {
		var (
			transactor = &testTransactor{}
			combiner   = &pf.MockCombiner{}
		)
		service.Transactor = transactor

		var rpc, err = openTransactions(combiner, "{}")
		require.NoError(t, err)
		require.Nil(t, rpc.Flush(pf.Checkpoint{}))

		transactor.startCommitErr = fmt.Errorf("mysterious start-commit failure")
		_, _, err = rpc.StartCommit(pf.Checkpoint{
			Sources: map[pf.Journal]pc.Checkpoint_Source{"foobar": {ReadThrough: 123}}})
		require.EqualError(t, err, "reading StartedCommit: transactor.StartCommit: mysterious start-commit failure")
	})

	t.Run("async-commit-error", func(t *testing.T) {
		var (
			transactor = &testTransactor{}
			combiner   = &pf.MockCombiner{}
		)
		service.Transactor = transactor

		var rpc, err = openTransactions(combiner, "{}")
		require.NoError(t, err)
		require.Nil(t, rpc.Flush(pf.Checkpoint{}))

		transactor.commitOp = client.NewAsyncOperation()

		_, opAcknowledged, err := rpc.StartCommit(pf.Checkpoint{
			Sources: map[pf.Journal]pc.Checkpoint_Source{"foobar": {ReadThrough: 123}}})
		require.NoError(t, err)
		require.NoError(t, rpc.Acknowledge())

		// Race some document loads.
		transactor.Loaded = map[int][]interface{}{0: {"found", "also-found"}}
		require.NoError(t, rpc.AddDocument(0, tuple.Tuple{1}.Pack(), json.RawMessage(`"one"`)))
		require.NoError(t, rpc.AddDocument(1, tuple.Tuple{2}.Pack(), json.RawMessage(`"one"`)))

		transactor.commitOp.Resolve(fmt.Errorf("mysterious async commit failure"))
		require.EqualError(t, opAcknowledged.Err(),
			"reading Acknowledged: commit failed: mysterious async commit failure")

		// Send a raced Load (after we know that `opAcknowledged` has resolved).
		// While under the hood it sees EOF due to the stream break,
		// expect it's mapped to a causal error for the user.
		require.EqualError(t, rpc.AddDocument(0, tuple.Tuple{3}.Pack(), json.RawMessage(`"one"`)),
			"reading Acknowledged: commit failed: mysterious async commit failure")
	})
}

// testServer implements DriverServer.
type testServer struct {
	OpenRx   TransactionRequest_Open
	OpenedTx TransactionResponse_Opened
	Transactor
}

var _ DriverServer = &testServer{}

func (t *testServer) Spec(context.Context, *SpecRequest) (*SpecResponse, error) {
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

func (t *testServer) Transactions(stream Driver_TransactionsServer) error {
	var err = RunTransactions(stream, func(_ context.Context, open TransactionRequest_Open) (Transactor, *TransactionResponse_Opened, error) {
		t.OpenRx = open
		return t.Transactor, &t.OpenedTx, nil
	})

	if err != nil {
		// Send Internal code, as flow-connector-init does.
		// This is unwrapped by the TxnClient.
		return status.Error(codes.Internal, err.Error())
	}
	return nil
}

// testTransactor implements Transactor.
type testTransactor struct {
	loadNotExpected                   bool
	loadErr, storeErr, startCommitErr error
	commitOp                          *client.AsyncOperation

	LoadBindings []int
	LoadKeys     []tuple.Tuple
	Loaded       map[int][]interface{}

	RuntimeCheckpoint string
	StartedCommitTx   pf.DriverCheckpoint

	StoreBindings []int
	StoreExists   []bool
	StoreKeys     []tuple.Tuple
	StoreValues   []tuple.Tuple
	StoreDocs     []json.RawMessage
}

func (t *testTransactor) Load(it *LoadIterator, loaded func(binding int, doc json.RawMessage) error) error {
	for it.Next() {
		if t.loadNotExpected {
			panic("Load not expected")
		}
		t.LoadBindings = append(t.LoadBindings, it.Binding)
		t.LoadKeys = append(t.LoadKeys, it.Key)
	}

	for binding, docs := range t.Loaded {
		for _, doc := range docs {
			if b, err := json.Marshal(doc); err != nil {
				return fmt.Errorf("json encoding Loaded fixture: %w", err)
			} else if err := loaded(binding, b); err != nil {
				return err
			}
		}
	}

	return t.loadErr
}

func (t *testTransactor) Store(it *StoreIterator) (StartCommitFunc, error) {
	for it.Next() {
		t.StoreBindings = append(t.StoreBindings, it.Binding)
		t.StoreKeys = append(t.StoreKeys, it.Key)
		t.StoreValues = append(t.StoreValues, it.Values)
		t.StoreDocs = append(t.StoreDocs, it.RawJSON)
	}
	return t.startCommit, t.storeErr
}

func (t *testTransactor) startCommit(
	_ context.Context,
	runtimeCheckpoint []byte,
	runtimeAckCh <-chan struct{},
) (*pf.DriverCheckpoint, pf.OpFuture) {
	t.RuntimeCheckpoint = string(runtimeCheckpoint)

	var commitOp pf.OpFuture

	if t.startCommitErr != nil {
		commitOp = pf.FinishedOperation(t.startCommitErr)
	} else if t.commitOp != nil {
		commitOp = t.commitOp // A nil *client.AsyncOperation is not a nil OpFuture.
	}
	return &t.StartedCommitTx, commitOp
}

func (t *testTransactor) Destroy() {}
