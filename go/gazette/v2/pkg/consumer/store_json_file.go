package consumer

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"

	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/LiveRamp/gazette/v2/pkg/recoverylog"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

// JSONFileStore is a simple Store which materializes itself as a JSON-encoded
// file. The store is careful to flush to a new temporary file which is then
// moved to the well-known location: eg, a process failure cannot result in a
// recovery of a partially written JSON file.
type JSONFileStore struct {
	// State is a user-provided instance which is un/marshal-able to JSON.
	State interface{}

	dir      string
	fs       afero.Fs
	offsets  map[pb.Journal]int64
	recorder *recoverylog.Recorder
}

// NewJSONFileStore returns a new JSONFileStore. |state| is the runtime instance
// of the Store's state, which is decoded into, encoded from, and retained
// as JSONFileState.State.
func NewJSONFileStore(rec *recoverylog.Recorder, dir string, state interface{}) (*JSONFileStore, error) {
	var store = &JSONFileStore{
		State:    reflect.ValueOf(state).Elem().Interface(),
		dir:      dir,
		fs:       recoverylog.RecordedAferoFS{Recorder: rec, Fs: afero.NewOsFs()},
		offsets:  make(map[pb.Journal]int64),
		recorder: rec,
	}

	var f, err = store.fs.Open(store.currentPath())

	if os.IsNotExist(err) {
		return store, nil
	} else if err != nil {
		return nil, extendErr(err, "opening state file")
	}

	var dec = json.NewDecoder(f)

	if err = dec.Decode(&store.offsets); err != nil {
		return nil, extendErr(err, "decoding offsets")
	} else if err = dec.Decode(state); err != nil {
		return nil, extendErr(err, "decoding state")
	} else if err = f.Close(); err != nil {
		return nil, extendErr(err, "closing state file")
	} else if err = store.Flush(nil); err != nil {
		return nil, extendErr(err, "flushing state")
	}
	return store, nil
}

// Recorder of the JSONFileStore.
func (s *JSONFileStore) Recorder() *recoverylog.Recorder { return s.recorder }

// FetchJournalOffsets returns offsets encoded by the JSONFileStore.
func (s *JSONFileStore) FetchJournalOffsets() (map[pb.Journal]int64, error) {
	var offsets = make(map[pb.Journal]int64)
	for k, o := range s.offsets {
		offsets[k] = o
	}
	return offsets, nil
}

// Flush the store's JSONFileState to disk.
func (s *JSONFileStore) Flush(offsets map[pb.Journal]int64) error {
	for k, o := range offsets {
		s.offsets[k] = o
	}

	var f, err = s.fs.OpenFile(s.nextPath(), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return extendErr(err, "creating state file")
	}
	var enc = json.NewEncoder(f)

	if err = enc.Encode(s.offsets); err != nil {
		return extendErr(err, "encoding offsets")
	} else if err = enc.Encode(s.State); err != nil {
		return extendErr(err, "encoding state")
	} else if err = f.Close(); err != nil {
		return extendErr(err, "closing state file")
	} else if err = s.fs.Rename(s.nextPath(), s.currentPath()); err != nil {
		return extendErr(err, "renaming next => current")
	}
	return nil
}

// Destroy the JSONFileStore directory and state file.
func (s *JSONFileStore) Destroy() {
	if err := os.RemoveAll(s.dir); err != nil {
		log.WithFields(log.Fields{
			"dir": s.dir,
			"err": err,
		}).Error("failed to remove JSON store directory")
	}
}

func (s *JSONFileStore) currentPath() string { return filepath.Join(s.dir, "state.json") }
func (s *JSONFileStore) nextPath() string    { return filepath.Join(s.dir, "next.json") }
