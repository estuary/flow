package store_rocksdb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
)

func TestStoreWriteAndReadKeysAndOffsets(t *testing.T) {
	var bk, cleanup = newBrokerAndLog(t)
	defer cleanup()

	var fsm, _ = recoverylog.NewFSM(recoverylog.FSMHints{Log: aRecoveryLog})
	var rep = NewTestReplica(t, bk)
	var recorder = &recoverylog.Recorder{
		FSM:    fsm,
		Author: rep.author,
		Dir:    rep.tmpdir,
		Client: rep.client,
	}
	var store = NewStore(recorder)
	assert.NoError(t, store.Open())

	store.WriteBatch.Put([]byte("foo"), []byte("bar"))
	store.WriteBatch.Put([]byte("baz"), []byte("bing"))

	assert.NoError(t, store.StartCommit(nil, pc.Checkpoint{
		Sources: map[pb.Journal]pc.Checkpoint_Source{
			"journal/A": {ReadThrough: 1234},
		},
	}, nil).Err())

	r, err := store.DB.Get(store.ReadOptions, []byte("foo"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("bar"), r.Data())
	r.Free()

	assert.NoError(t, store.StartCommit(nil, pc.Checkpoint{
		Sources: map[pb.Journal]pc.Checkpoint_Source{
			"journal/B": {ReadThrough: 5678},
		},
	}, nil).Err())

	cp, err := store.RestoreCheckpoint(nil)
	assert.NoError(t, err)
	assert.Equal(t, pc.Checkpoint{
		Sources: map[pb.Journal]pc.Checkpoint_Source{
			"journal/B": {ReadThrough: 5678},
		},
	}, cp)

	store.Destroy()

	// Assert the store directory was removed.
	_, err = os.Stat(recorder.Dir)
	assert.True(t, os.IsNotExist(err))
}
