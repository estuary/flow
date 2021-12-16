package materialize

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/protocols/fdb/tuple"
	pf "github.com/estuary/protocols/flow"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/broker/client"
	pc "go.gazette.dev/core/consumer/protocol"
)

//go:generate flowctl api build --build-id temp.db --directory testdata/ --source testdata/flow.yaml
//go:generate sqlite3 file:testdata/temp.db "SELECT WRITEFILE('testdata/materialization.proto', spec) FROM built_materializations WHERE materialization = 'test/sqlite';"

func TestIntegratedTransactorAndClient(t *testing.T) {
	var specBytes, err = ioutil.ReadFile("testdata/materialization.proto")
	require.NoError(t, err)
	var spec pf.MaterializationSpec
	require.NoError(t, spec.Unmarshal(specBytes))

	var ctx = context.Background()
	var combiner = &pf.MockCombiner{}
	var transactor = &testTransactor{}
	var server = &testServer{Transactor: transactor}
	var conn = AdaptServerToClient(server)

	// Set an Opened fixture to return, and open the Transactions stream.
	server.OpenedTx.FlowCheckpoint = []byte("open-checkpoint")
	rpc, err := OpenTransactions(
		ctx,
		conn,
		json.RawMessage(`{"driver":"checkpoint"}`),
		func(*pf.MaterializationSpec_Binding) (pf.Combiner, error) { return combiner, nil },
		pf.NewFullRange(),
		&spec,
		"a-version",
	)
	require.NoError(t, err)
	require.Equal(t, "open-checkpoint", string(rpc.Opened().FlowCheckpoint))

	// Set a Loaded fixture to return, and load some documents.
	transactor.Loaded = map[int][]interface{}{
		0: {"found", "also-found"},
	}
	require.NoError(t, rpc.AddDocument(0, tuple.Tuple{1}.Pack(), json.RawMessage(`"one"`)))
	require.NoError(t, rpc.AddDocument(0, tuple.Tuple{2}.Pack(), json.RawMessage(`2`)))
	require.NoError(t, rpc.AddDocument(0, tuple.Tuple{"three"}.Pack(), json.RawMessage(`3`)))

	// Set a Prepared fixture to return, and prepare to commit.
	transactor.PreparedTx.DriverCheckpointJson = json.RawMessage(`"driver-checkpoint"`)
	prepared, err := rpc.Prepare(pf.Checkpoint{})
	require.NoError(t, err)
	require.Equal(t, `"driver-checkpoint"`, string(prepared.DriverCheckpointJson))

	combiner.AddDrainFixture(false, "one", tuple.Tuple{1}, tuple.Tuple{"val", 1})
	combiner.AddDrainFixture(true, 2, tuple.Tuple{2}, tuple.Tuple{"val", 2})
	combiner.AddDrainFixture(false, 3, tuple.Tuple{"three"}, tuple.Tuple{"val", 3})

	// Create some async operation fixtures, and start to commit.
	var logCommittedOp = client.NewAsyncOperation()
	var ops = CommitOps{
		DriverCommitted: client.NewAsyncOperation(),
		LogCommitted:    logCommittedOp,
		Acknowledged:    client.NewAsyncOperation(),
	}
	stats, err := rpc.StartCommit(ops)
	require.NoError(t, err)
	require.NotNil(t, stats[0])

	// Pipeline the next transaction.
	// Reset Loaded fixture, and load some documents.
	transactor.Loaded = map[int][]interface{}{
		0: {"2nd-round"},
	}
	require.NoError(t, rpc.AddDocument(0, tuple.Tuple{4}.Pack(), json.RawMessage(`"four"`)))

	// Resolve the last transaction's commit.
	// Expect Transactor DriverCommitted, which client reads and resolves.
	require.NoError(t, ops.DriverCommitted.Err())
	// Pretend DriverCommitted unblocks the recovery log commit.
	// The client observes and sends Acknowledge.
	logCommittedOp.Resolve(nil)
	// Transactor responds with Acknowledged, which client reads and resolves.
	require.NoError(t, ops.Acknowledged.Err())

	// Set a Prepared fixture to return, and prepare to commit.
	transactor.PreparedTx.DriverCheckpointJson = json.RawMessage(`"2nd-checkpoint"`)
	prepared, err = rpc.Prepare(pf.Checkpoint{
		Sources: map[pf.Journal]pc.Checkpoint_Source{"2nd-flow-fixture": {ReadThrough: 1234}}})
	require.NoError(t, err)
	require.Equal(t, `"2nd-checkpoint"`, string(prepared.DriverCheckpointJson))

	combiner.AddDrainFixture(true, "four", tuple.Tuple{4}, tuple.Tuple{"val", 4})

	// Create new async operation fixtures, and complete a second commit.
	logCommittedOp = client.NewAsyncOperation()
	ops = CommitOps{
		DriverCommitted: client.NewAsyncOperation(),
		LogCommitted:    logCommittedOp,
		Acknowledged:    client.NewAsyncOperation(),
	}
	stats, err = rpc.StartCommit(ops)
	require.NoError(t, err)
	require.NotNil(t, stats[0])

	// Clear the key cache, and switch to delta-updates mode.
	// Then pipeline the next delta-updates transaction.
	rpc.shared.flighted[0] = make(map[string]json.RawMessage)
	rpc.spec.Bindings[0].DeltaUpdates = true
	transactor.loadNotExpected = true

	// We do NOT expect to see Load requests for these documents in the snapshot.
	require.NoError(t, rpc.AddDocument(0, tuple.Tuple{"five"}.Pack(), json.RawMessage(`5`)))
	require.NoError(t, rpc.AddDocument(0, tuple.Tuple{"six"}.Pack(), json.RawMessage(`"six"`)))

	require.NoError(t, ops.DriverCommitted.Err())
	logCommittedOp.Resolve(nil)
	require.NoError(t, ops.Acknowledged.Err())

	transactor.PreparedTx.DriverCheckpointJson = json.RawMessage(`"3rd-checkpoint"`)
	prepared, err = rpc.Prepare(pf.Checkpoint{
		Sources: map[pf.Journal]pc.Checkpoint_Source{"3rd-flow-fixture": {ReadThrough: 5678}}})
	require.NoError(t, err)
	require.Equal(t, `"3rd-checkpoint"`, string(prepared.DriverCheckpointJson))

	combiner.AddDrainFixture(true, "five", tuple.Tuple{"five"}, tuple.Tuple{"val", 5})
	combiner.AddDrainFixture(true, "six", tuple.Tuple{"six"}, tuple.Tuple{"val", 6})

	// Final transaction starts to commit, then commits.
	logCommittedOp = client.NewAsyncOperation()
	ops = CommitOps{
		DriverCommitted: client.NewAsyncOperation(),
		LogCommitted:    logCommittedOp,
		Acknowledged:    client.NewAsyncOperation(),
	}
	stats, err = rpc.StartCommit(ops)
	require.NoError(t, err)
	require.NotNil(t, stats[0])

	require.NoError(t, ops.DriverCommitted.Err())
	logCommittedOp.Resolve(nil)
	require.NoError(t, ops.Acknowledged.Err())

	// We can gracefully close the stream now.
	// This is only a clean close in this post-Acknowledged state.
	require.NoError(t, rpc.Close())

	// Nil fixture before snapshot.
	transactor.Loaded = nil

	// Snapshot the recorded observations of the MockCombiner and testTransactor.
	cupaloy.SnapshotT(t, "COMBINER:", combiner, "TRANSACTOR:", transactor)
}

// testServer implements DriverServer.
type testServer struct {
	OpenRx   TransactionRequest_Open
	OpenedTx TransactionResponse_Opened
	Transactor
}

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
	open, err := stream.Recv()
	if err != nil {
		return err
	} else if err := open.Validate(); err != nil {
		return err
	} else if open.Open == nil {
		return fmt.Errorf("expected Open got %v", open)
	}

	t.OpenRx = *open.Open
	if err := stream.Send(&TransactionResponse{Opened: &t.OpenedTx}); err != nil {
		return err
	}

	return RunTransactions(stream, t.Transactor, logrus.WithFields(nil))
}

// testTransactor implements Transactor.
type testTransactor struct {
	loadNotExpected bool

	LoadBindings []int
	LoadKeys     []tuple.Tuple
	Loaded       map[int][]interface{}

	PrepareRx  TransactionRequest_Prepare
	PreparedTx pf.DriverCheckpoint

	StoreBindings []int
	StoreExists   []bool
	StoreKeys     []tuple.Tuple
	StoreValues   []tuple.Tuple
	StoreDocs     []json.RawMessage
}

func (t *testTransactor) Load(
	it *LoadIterator,
	priorCommittedCh <-chan struct{},
	priorAcknowledgedCh <-chan struct{},
	loaded func(binding int, doc json.RawMessage) error,
) error {
	if t.loadNotExpected {
		panic("Load not expected")
	}
	<-priorCommittedCh

	for it.Next() {
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

	return nil
}

func (t *testTransactor) Prepare(_ context.Context, rx TransactionRequest_Prepare) (pf.DriverCheckpoint, error) {
	t.PrepareRx = rx
	return t.PreparedTx, nil
}

func (t *testTransactor) Store(it *StoreIterator) error {
	for it.Next() {
		t.StoreBindings = append(t.StoreBindings, it.Binding)
		t.StoreKeys = append(t.StoreKeys, it.Key)
		t.StoreValues = append(t.StoreValues, it.Values)
		t.StoreDocs = append(t.StoreDocs, it.RawJSON)
	}
	return nil
}

func (t *testTransactor) Commit(context.Context) error      { return nil }
func (t *testTransactor) Acknowledge(context.Context) error { return nil }
func (t *testTransactor) Destroy()                          {}
