package store_rocksdb

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tecbot/gorocksdb"
	"go.gazette.dev/core/protocol"
	"go.gazette.dev/core/recoverylog"
)

func TestStoreWriteAndReadKeysAndOffsets(t *testing.T) {
	var store = newTestStore(t, nil)

	store.WriteBatch.Put([]byte("foo"), []byte("bar"))
	store.WriteBatch.Put([]byte("baz"), []byte("bing"))

	assert.NoError(t, store.Flush(map[protocol.Journal]int64{
		"journal/A": 1234,
	}))

	r, err := store.DB.Get(store.ReadOptions, []byte("foo"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("bar"), r.Data())
	r.Free()

	assert.NoError(t, store.Flush(map[protocol.Journal]int64{
		"journal/B": 5678,
	}))

	offsets, err := store.FetchJournalOffsets()
	assert.NoError(t, err)
	assert.Equal(t, map[protocol.Journal]int64{
		"journal/A": 1234,
		"journal/B": 5678,
	}, offsets)

	store.Destroy()

	// Assert the store directory was removed.
	_, err = os.Stat(store.dir)
	assert.True(t, os.IsNotExist(err))
}

func newTestStore(t assert.TestingT, rec *recoverylog.Recorder) *Store {
	var dir, err = ioutil.TempDir("", "rocksdb")
	assert.NoError(t, err)

	var store = NewStore(rec, dir)
	if rec == nil {
		// Replace observed Env with regular one.
		store.Env = gorocksdb.NewDefaultEnv()
	}
	assert.NoError(t, store.Open())

	return store
}
