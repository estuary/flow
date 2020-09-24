package consumer

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
)

// JSONFileStore is a simple Store implementation which materializes itself as a
// JSON-encoded file, which is re-written at the commit of every consumer
// transaction.
type JSONFileStore struct {
	// State is a user-provided instance which is un/marshal-able to JSON.
	State interface{}

	checkpoint pc.Checkpoint
	fs         afero.Fs
	recorder   *recoverylog.Recorder
}

var _ Store = &JSONFileStore{} // JSONFileStore is-a Store.

// NewJSONFileStore returns a new JSONFileStore. |state| is the runtime instance
// of the Store's state, which is decoded into, encoded from, and retained
// as JSONFileState.State.
func NewJSONFileStore(rec *recoverylog.Recorder, state interface{}) (*JSONFileStore, error) {
	var store = &JSONFileStore{
		State:    state,
		fs:       recoverylog.RecordedAferoFS{Recorder: rec, Fs: afero.NewOsFs()},
		recorder: rec,
	}

	var f, err = store.fs.Open(store.currentPath())

	if os.IsNotExist(err) {
		return store, nil
	} else if err != nil {
		return nil, errors.WithMessage(err, "opening state file")
	}

	var dec = json.NewDecoder(f)
	var offsets pb.Offsets

	if err = dec.Decode(&offsets); err != nil {
		return nil, errors.WithMessage(err, "decode(offsets)")
	} else if err = dec.Decode(store.State); err != nil {
		return nil, errors.WithMessage(err, "decode(state)")
	} else if err = dec.Decode(&store.checkpoint); err != nil && err != io.EOF {
		return nil, errors.WithMessage(err, "decode(checkpoint)")
	} else if err = f.Close(); err != nil {
		return nil, errors.WithMessage(err, "closing state file")
	}

	// Legacy support for offset-only state files.
	// TODO(johnny): Remove with next release.
	for j, o := range offsets {
		store.checkpoint.Sources[j] = &pc.Checkpoint_Source{
			ReadThrough: o,
			Producers:   store.checkpoint.Sources[j].Producers,
		}
	}
	return store, nil
}

// RestoreCheckpoint returns the checkpoint encoded in the recovered JSON state file.
func (s *JSONFileStore) RestoreCheckpoint(Shard) (pc.Checkpoint, error) { return s.checkpoint, nil }

// StartCommit marshals the in-memory state and Checkpoint into a recorded JSON state file.
func (s *JSONFileStore) StartCommit(_ Shard, cp pc.Checkpoint, waitFor OpFutures) OpFuture {
	_ = s.recorder.Barrier(waitFor)
	s.checkpoint = cp

	// Commit by writing the complete state to a temporary file, and then
	// atomically moving it to a well-known location. This ensures that we'll
	// always recover a complete JSON checkpoint, even if a process failure
	// produced a partially written file.

	// We use O_TRUNC and not O_EXCL as it's possible we recovered a DB which
	// had written, but not yet renamed its own "next.json". In this case we
	// over-write the file: it represents a failed consumer transaction
	// which has been rolled back.
	var f, err = s.fs.OpenFile(s.nextPath(), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return client.FinishedOperation(errors.WithMessage(err, "creating state file"))
	}
	var enc = json.NewEncoder(f)

	if err = enc.Encode(pc.FlattenReadThrough(s.checkpoint)); err != nil {
		err = errors.WithMessage(err, "encode(offsets)")
	} else if err = enc.Encode(s.State); err != nil {
		err = errors.WithMessage(err, "encode(state)")
	} else if err = enc.Encode(s.checkpoint); err != nil {
		err = errors.WithMessage(err, "encode(checkpoint)")
	} else if err = f.Close(); err != nil {
		err = errors.WithMessage(err, "closing state file")
	} else if err = s.fs.Rename(s.nextPath(), s.currentPath()); err != nil {
		err = errors.WithMessage(err, "renaming next => current")
	}

	if err != nil {
		return client.FinishedOperation(err)
	}
	return s.recorder.Barrier(nil)
}

// Destroy the JSONFileStore directory and state file.
func (s *JSONFileStore) Destroy() {
	if err := os.RemoveAll(s.recorder.Dir); err != nil {
		log.WithFields(log.Fields{
			"dir": s.recorder.Dir,
			"err": err,
		}).Error("failed to remove JSON store directory")
	}
}

func (s *JSONFileStore) currentPath() string { return filepath.Join(s.recorder.Dir, "state.json") }
func (s *JSONFileStore) nextPath() string    { return filepath.Join(s.recorder.Dir, "next.json") }
