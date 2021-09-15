package runtime

import (
	"bytes"
	"encoding/json"
	"fmt"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"go.gazette.dev/core/broker/client"
	"go.gazette.dev/core/consumer"
	pgc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
)

// connectorStore is used for captures and materializations. It persists a
// storeState containing a DriverCheckpoint updated via RFC7396 Merge Patch,
// along with the usual Gazette checkpoint.
type connectorStore struct {
	delegate *consumer.JSONFileStore

	// Pending patch not yet applied to the contained DriverCheckpoint.
	patch json.RawMessage
	// Deferred error encountered while patching a driver checkpoint.
	err error
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

func (s *connectorStore) restoreCheckpoint(shard consumer.Shard) (cp pgc.Checkpoint, err error) {
	// Precondition: connectorStore is zero-valued.
	if s.err != nil {
		panic(s.err)
	} else if len(s.patch) != 0 {
		panic(s.patch)
	}
	return s.delegate.RestoreCheckpoint(shard)
}

func (s *connectorStore) updateDriverCheckpoint(next json.RawMessage, patch bool) {
	if len(next) == 0 {
		// A nil RawMessage is encoded as JSON null, and a non-nil but empty RawMessage
		// is an encoding error. Canonicalize to the former.
		next = nil
	}

	if s.err != nil {
		return
	} else if !patch {
		s.patch = nil
		s.delegate.State.(*storeState).DriverCheckpoint = next
	} else if len(s.patch) == 0 {
		s.patch = next
	} else if s.patch, s.err = jsonpatch.MergeMergePatches(s.patch, next); s.err != nil {
		s.err = fmt.Errorf("merging driver checkpoint patches: %w", s.err)
	}
}

func (s *connectorStore) startCommit(shard consumer.Shard, checkpoint pgc.Checkpoint, waitFor consumer.OpFutures) consumer.OpFuture {
	if s.err != nil {
		// Fallthrough.
	} else if len(s.patch) == 0 {
		// No merge patch to apply.
	} else if next, err := jsonpatch.MergePatch(s.driverCheckpoint(), s.patch); err != nil {
		s.err = fmt.Errorf("patching driver checkpoint: %w", err)
	} else {
		s.delegate.State.(*storeState).DriverCheckpoint = next
		s.patch = nil
	}

	if s.err != nil {
		return client.FinishedOperation(s.err)
	}
	return s.delegate.StartCommit(shard, checkpoint, waitFor)
}

func (s *connectorStore) destroy() { s.delegate.Destroy() }
