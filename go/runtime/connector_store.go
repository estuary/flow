package runtime

import (
	"bytes"
	"encoding/json"
	"fmt"

	pf "github.com/estuary/flow/go/protocols/flow"
	"go.gazette.dev/core/consumer"
	"go.gazette.dev/core/consumer/recoverylog"
)

// storeState is a JSONFileStore.State used for captures and materializations.
// It persists a DriverCheckpoint updated via RFC7396 Merge Patch.
type storeState struct {
	DriverCheckpoint json.RawMessage
}

func newConnectorStore(recorder *recoverylog.Recorder) (*consumer.JSONFileStore, error) {
	var store, err = consumer.NewJSONFileStore(recorder, new(storeState))
	if err != nil {
		return nil, fmt.Errorf("consumer.NewJSONFileStore: %w", err)
	}

	// A `nil` driver checkpoint will round-trip through JSON encoding as []byte("null").
	// Restore it's nil-ness after deserialization.
	if bytes.Equal([]byte("null"), store.State.(*storeState).DriverCheckpoint) {
		store.State.(*storeState).DriverCheckpoint = nil
	}

	return store, nil
}

func loadDriverCheckpoint(store *consumer.JSONFileStore) json.RawMessage {
	if cp := store.State.(*storeState).DriverCheckpoint; len(cp) != 0 {
		return cp
	}
	return []byte("{}")
}

func updateDriverCheckpoint(
	store *consumer.JSONFileStore,
	driverCheckpoint pf.DriverCheckpoint,
) error {
	var reduced = pf.DriverCheckpoint{
		DriverCheckpointJson: store.State.(*storeState).DriverCheckpoint,
		Rfc7396MergePatch:    false,
	}
	if err := reduced.Reduce(driverCheckpoint); err != nil {
		return fmt.Errorf("patching driver checkpoint: %w", err)
	}
	store.State.(*storeState).DriverCheckpoint = reduced.DriverCheckpointJson

	return nil
}
