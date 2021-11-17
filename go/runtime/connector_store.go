package runtime

import (
	"bytes"
	"encoding/json"
	"fmt"

	pf "github.com/estuary/protocols/flow"
	"go.gazette.dev/core/broker/client"
	"go.gazette.dev/core/consumer"
	"go.gazette.dev/core/consumer/recoverylog"
)

// connectorStore is used for captures and materializations. It persists a
// storeState containing a DriverCheckpoint updated via RFC7396 Merge Patch,
// along with the usual Gazette checkpoint.
type connectorStore struct {
	delegate *consumer.JSONFileStore
}

type storeState struct {
	DriverCheckpoint json.RawMessage
}

func newConnectorStore(recorder *recoverylog.Recorder) (connectorStore, error) {
	var delegate, err = consumer.NewJSONFileStore(recorder, new(storeState))
	if err != nil {
		return connectorStore{}, fmt.Errorf("consumer.NewJSONFileStore: %w", err)
	}

	// A `nil` driver checkpoint will round-trip through JSON encoding as []byte("null").
	// Restore it's nil-ness after deserialization.
	if bytes.Equal([]byte("null"), delegate.State.(*storeState).DriverCheckpoint) {
		delegate.State.(*storeState).DriverCheckpoint = nil
	}

	return connectorStore{delegate: delegate}, nil
}

func (s *connectorStore) driverCheckpoint() json.RawMessage {
	if cp := s.delegate.State.(*storeState).DriverCheckpoint; len(cp) != 0 {
		return cp
	}
	return []byte("{}")
}

func (s *connectorStore) restoreCheckpoint(shard consumer.Shard) (cp pf.Checkpoint, err error) {
	return s.delegate.RestoreCheckpoint(shard)
}

func (s *connectorStore) startCommit(
	shard consumer.Shard,
	flowCheckpoint pf.Checkpoint,
	driverCheckpoint pf.DriverCheckpoint,
	waitFor consumer.OpFutures,
) consumer.OpFuture {

	var reduced = pf.DriverCheckpoint{
		DriverCheckpointJson: s.delegate.State.(*storeState).DriverCheckpoint,
		Rfc7396MergePatch:    false,
	}
	if err := reduced.Reduce(driverCheckpoint); err != nil {
		return client.FinishedOperation(fmt.Errorf("patching driver checkpoint: %w", err))
	}

	s.delegate.State.(*storeState).DriverCheckpoint = reduced.DriverCheckpointJson
	return s.delegate.StartCommit(shard, flowCheckpoint, waitFor)
}

func (s *connectorStore) destroy() { s.delegate.Destroy() }
