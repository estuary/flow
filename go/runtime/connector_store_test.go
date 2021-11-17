package runtime

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	pf "github.com/estuary/protocols/flow"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/brokertest"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/etcdtest"
)

func TestConnectorInitializationAndStateUpdates(t *testing.T) {
	testWithConnectorStore(t, nil, func(t *testing.T, cs connectorStore) {

		// A new connector store initializes with an empty state.
		require.Equal(t, json.RawMessage(nil), cs.delegate.State.(*storeState).DriverCheckpoint)
		require.Equal(t, json.RawMessage("{}"), cs.driverCheckpoint())

		var cp, err = cs.restoreCheckpoint(nil)
		require.NoError(t, err)
		require.Empty(t, cp.Sources)

		// Patch and commit the state.
		require.NoError(t, cs.startCommit(nil, cp, pf.DriverCheckpoint{
			DriverCheckpointJson: []byte(`{"k1":"v1","n":null}`),
			Rfc7396MergePatch:    true,
		}, nil).Err())
		require.Equal(t, `{"k1":"v1"}`, string(cs.driverCheckpoint()))

		// A non-merged patch replaces the current checkpoint.
		require.NoError(t, cs.startCommit(nil, cp, pf.DriverCheckpoint{
			DriverCheckpointJson: []byte(`{"expect":"k1-is-dropped"}`),
			Rfc7396MergePatch:    false,
		}, nil).Err())
		require.Equal(t, `{"expect":"k1-is-dropped"}`, string(cs.driverCheckpoint()))

		// Empty non-patch update clears a current state.
		require.NoError(t, cs.startCommit(nil, cp, pf.DriverCheckpoint{
			DriverCheckpointJson: nil,
			Rfc7396MergePatch:    false,
		}, nil).Err())
		require.Equal(t, json.RawMessage(`{}`), cs.driverCheckpoint())
	})
}

func TestConnectorWithNilStateFixture(t *testing.T) {
	// Offsets, followed by state, followed by checkpoint. See NewJSONFileStore.
	var fixture = "{}\n{\"driverCheckpoint\":null}\n{}\n"

	testWithConnectorStore(t, []byte(fixture), func(t *testing.T, cs connectorStore) {
		// Expect nil-ness of the driver checkpoint was restored.
		require.Equal(t, json.RawMessage(nil), cs.delegate.State.(*storeState).DriverCheckpoint)
		require.Equal(t, json.RawMessage("{}"), cs.driverCheckpoint())
	})
}

func TestConnectorWithNonNilStateFixture(t *testing.T) {
	// Offsets, followed by state, followed by checkpoint. See NewJSONFileStore.
	var fixture = "{}\n{\"driverCheckpoint\":{\"foo\":42}}\n{}\n"

	testWithConnectorStore(t, []byte(fixture), func(t *testing.T, cs connectorStore) {
		require.Equal(t, json.RawMessage(`{"foo":42}`), cs.driverCheckpoint())
	})
}

func testWithConnectorStore(t *testing.T, fixture []byte, fn func(*testing.T, connectorStore)) {
	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = brokertest.NewBroker(t, etcd, "local", "broker")
	brokertest.CreateJournals(t, broker, brokertest.Journal(
		pb.JournalSpec{Name: "a/log"}))

	// Create state directory and write fixture file.
	var dir, err = ioutil.TempDir("", "connector-store")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	if fixture != nil {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "state.json"), fixture, 0600))
	}

	var ajc = client.NewAppendService(broker.Tasks.Context(), broker.Client())
	var fsm, _ = recoverylog.NewFSM(recoverylog.FSMHints{Log: "a/log"})
	var rec = recoverylog.NewRecorder("a/log", fsm, 1234, dir, ajc)

	store, err := newConnectorStore(rec)
	require.NoError(t, err)

	fn(t, store)

	broker.Tasks.Cancel()
	require.NoError(t, broker.Tasks.Wait())
}

func TestMain(m *testing.M) { etcdtest.TestMainWithEtcd(m) }
