package consumer

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/labels"
)

func TestStoreAndFetchHints(t *testing.T) {
	var tf, shard, cleanup = newTestFixtureWithIdleShard(t)
	var ctx = context.Background()
	defer cleanup()

	// Note that |shard|'s spec fixture has three hint keys.

	// mkHints builds a valid FSMHints fixture which is unique on |id|.
	var mkHints = func(id int64) recoverylog.FSMHints {
		defer tf.ks.Mu.RUnlock()
		tf.ks.Mu.RLock() // Hold to access |r.spec|.

		return recoverylog.FSMHints{
			Log: shard.Spec().RecoveryLog(),
			LiveNodes: []recoverylog.FnodeSegments{{
				Fnode: recoverylog.Fnode(id),
				Segments: []recoverylog.Segment{
					{Author: 0x1234, FirstSeqNo: id, LastSeqNo: id},
				},
			}},
		}
	}
	// verifyHints confirms that fetchHints returns |id|, and the state of hint
	// keys in etcd matches |idA|, |idB|, |idC|.
	var verifyHints = func(firstID int64, ids ...int64) {
		var hints, err = fetchHints(ctx, shard.Spec(), tf.etcd)
		assert.NoError(t, err)
		assert.Equal(t, shard.Spec().RecoveryLog(), hints.log)
		assert.Len(t, hints.txnResp.Responses, 3)
		assert.Len(t, hints.hints, 3)

		assert.Equal(t, mkHints(firstID), pickFirstHints(hints))
		assert.Len(t, hints.hints, len(ids))

		for i, hint := range hints.hints {
			if ids[i] == 0 {
				assert.Nil(t, hints.hints[i])
			} else {
				assert.Equal(t, ids[i], hint.LiveNodes[0].Segments[0].FirstSeqNo)
			}
		}
	}

	// Touch the shard assignment without updating its value, such that the
	// fixture will on match CreateRevision but not ModRevision. This confirms
	// that store*Hints expects its CreateRevision and not ModRevision.
	var _, err = tf.etcd.Put(ctx, string(shard.resolved.assignment.Raw.Key), "")
	assert.NoError(t, err)

	// Alternate stores of recorded vs recovered hints.
	assert.NoError(t, storeRecoveredHints(shard, mkHints(111)))
	verifyHints(111, 0, 111, 0)
	assert.NoError(t, storeRecordedHints(shard, mkHints(222)))
	verifyHints(222, 222, 111, 0)
	assert.NoError(t, storeRecoveredHints(shard, mkHints(333)))
	verifyHints(222, 222, 333, 111)
	assert.NoError(t, storeRecordedHints(shard, mkHints(444)))
	verifyHints(444, 444, 333, 111)
	assert.NoError(t, storeRecoveredHints(shard, mkHints(555)))
	verifyHints(444, 444, 555, 333)

	// Delete hints in key priority order. Expect older hints are used instead.
	_, _ = tf.etcd.Delete(ctx, shard.Spec().HintPrimaryKey())
	verifyHints(555, 0, 555, 333)
	_, _ = tf.etcd.Delete(ctx, shard.Spec().HintBackupKeys()[0])
	verifyHints(333, 0, 0, 333)
	_, _ = tf.etcd.Delete(ctx, shard.Spec().HintBackupKeys()[1])

	// When no hints exist, default hints are returned.
	h, err := fetchHints(ctx, shard.Spec(), tf.etcd)
	assert.NoError(t, err)
	assert.Equal(t, recoverylog.FSMHints{Log: shard.Spec().RecoveryLog()}, pickFirstHints(h))
}

func TestRecoveryFromEmptyLog(t *testing.T) {
	var tf, shard, cleanup = newTestFixtureWithIdleShard(t)
	defer cleanup()

	go func() { assert.NoError(t, beginRecovery(shard)) }()

	// Precondition: no existing hints in etcd.
	assert.Len(t, etcdGet(t, tf.etcd, shard.Spec().HintPrimaryKey()).Kvs, 0)
	assert.Len(t, etcdGet(t, tf.etcd, shard.Spec().HintBackupKeys()[0]).Kvs, 0)

	var cp, err = completeRecovery(shard)
	require.NoError(t, err)
	assert.Equal(t, pc.Checkpoint{}, cp)
	<-shard.storeReadyCh // Expect it selects.

	// Post-condition: backup (but not primary) hints were updated.
	assert.Len(t, etcdGet(t, tf.etcd, shard.Spec().HintPrimaryKey()).Kvs, 0)
	assert.Len(t, etcdGet(t, tf.etcd, shard.Spec().HintBackupKeys()[0]).Kvs, 1)
}

func TestRecoveryFailsFromInvalidHints(t *testing.T) {
	var tf, shard, cleanup = newTestFixtureWithIdleShard(t)
	defer cleanup()

	_, _ = tf.etcd.Put(context.Background(), shard.Spec().HintPrimaryKey(), "invalid hints")
	assert.EqualError(t, beginRecovery(shard), "fetchHints: hints.Unmarshal: unexpected EOF")
}

func TestRecoveryFailsFromMissingLog(t *testing.T) {
	var _, shard, cleanup = newTestFixtureWithIdleShard(t)
	defer cleanup()

	shard.resolved.spec.RecoveryLogPrefix = "does/not/exist"
	assert.EqualError(t, beginRecovery(shard), "fetching log spec: named journal does"+
		" not exist (does/not/exist/"+shardA+")")
}

func TestRecoveryFailsFromWrongContentType(t *testing.T) {
	var _, shard, cleanup = newTestFixtureWithIdleShard(t)
	defer cleanup()

	var ctx = pb.WithDispatchDefault(context.Background())

	// Fetch current log spec, set an incorrect ContentType, and re-apply.
	var lr, err = shard.ajc.List(ctx, &pb.ListRequest{
		Selector: pb.LabelSelector{Include: pb.MustLabelSet("name", shard.Spec().RecoveryLog().String())},
	})
	assert.NoError(t, err)

	lr.Journals[0].Spec.LabelSet.SetValue(labels.ContentType, "wrong/type")
	_, err = shard.ajc.Apply(ctx, &pb.ApplyRequest{
		Changes: []pb.ApplyRequest_Change{{Upsert: &lr.Journals[0].Spec, ExpectModRevision: lr.Journals[0].ModRevision}},
	})
	assert.NoError(t, err)

	assert.EqualError(t, beginRecovery(shard), "expected label "+labels.ContentType+
		" value "+labels.ContentType_RecoveryLog+" (got wrong/type)")
}

func TestRecoveryFailsFromPlayError(t *testing.T) {
	var tf, shard, cleanup = newTestFixtureWithIdleShard(t)
	defer cleanup()

	// Write a valid FSMHints that references a log offset that doesn't exist.
	var fixture = recoverylog.FSMHints{
		Log: shard.Spec().RecoveryLog(),
		LiveNodes: []recoverylog.FnodeSegments{
			{Fnode: 1, Segments: []recoverylog.Segment{{Author: 123, FirstSeqNo: 1, FirstOffset: 100, LastSeqNo: 1}}},
		},
	}
	var fixtureBytes, _ = json.Marshal(&fixture)
	_, _ = tf.etcd.Put(context.Background(), shard.Spec().HintPrimaryKey(), string(fixtureBytes))

	// Expect playLog returns an immediate error.
	assert.Regexp(t, `playing log .*: max write-head of .* is 0, vs .*`, beginRecovery(shard))

	// Since the error occurred within Player.Play, it also causes completeRecovery to immediately fail.
	var _, err = completeRecovery(shard)
	assert.EqualError(t, err, "completeRecovery aborting due to log playback failure")
}
