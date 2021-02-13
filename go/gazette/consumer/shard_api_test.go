package consumer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/brokertest"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
)

func TestAPIStatCases(t *testing.T) {
	var tf, cleanup = newTestFixture(t)
	defer cleanup()

	tf.allocateShard(makeShard(shardA), localID)
	expectStatusCode(t, tf.state, pc.ReplicaStatus_PRIMARY)

	// Publish to |sourceB|.
	var aa, _ = tf.pub.PublishCommitted(toSourceB, &testMessage{Key: "read", Value: "through"})
	<-aa.Done()

	// Case: Stat local shard. Expect it to have read-through our publish.
	resp, err := tf.service.Stat(context.Background(), &pc.StatRequest{
		Shard:       shardA,
		ReadThrough: pb.Offsets{sourceB.Name: aa.Response().Commit.End},
	})
	require.NoError(t, err)
	require.Equal(t, pc.Status_OK, resp.Status)
	require.Equal(t, map[pb.Journal]int64{
		sourceA.Name: 0,
		sourceB.Name: aa.Response().Commit.End,
	}, resp.ReadThrough)
	require.Equal(t, localID, resp.Header.ProcessId)

	// Case: Stat of non-existent Shard.
	resp, err = tf.service.Stat(context.Background(), &pc.StatRequest{Shard: "missing-shard"})
	require.NoError(t, err)
	require.Equal(t, pc.Status_SHARD_NOT_FOUND, resp.Status)

	// TODO(johnny): Proxy case ought to be unit-tested here.
	// Adding it is currently low-value because it's covered by other E2E tests
	// and newTestFixture isn't set up for verifying proxy interactions.

	tf.allocateShard(makeShard(shardA)) // Cleanup.
}

func TestAPIListCases(t *testing.T) {
	var tf, cleanup = newTestFixture(t)
	defer cleanup()

	var specA = makeShard(shardA)
	specA.Labels = append(specA.Labels, pb.Label{Name: "foo", Value: "bar"})
	var specB = makeShard(shardB)
	specB.Labels = append(specB.Labels, pb.Label{Name: "bar", Value: "baz"})
	var specC = makeShard(shardC)

	tf.allocateShard(specA)
	tf.allocateShard(specB, remoteID)
	tf.allocateShard(specC, localID, remoteID)
	expectStatusCode(t, tf.state, pc.ReplicaStatus_PRIMARY)

	var verify = func(resp *pc.ListResponse, expect ...*pc.ShardSpec) {
		require.Equal(t, pc.Status_OK, resp.Status)
		require.Len(t, resp.Shards, len(expect))

		for i, exp := range expect {
			require.Equal(t, *exp, resp.Shards[i].Spec)

			var numAsn int
			if exp == specB {
				numAsn = 1
			} else if exp == specC {
				numAsn = 2
			}
			require.Len(t, resp.Shards[i].Status, numAsn)
			require.Len(t, resp.Shards[i].Route.Endpoints, numAsn)
		}
	}

	// Case: Empty selector returns all shards.
	var resp, err = tf.service.List(context.Background(), &pc.ListRequest{
		Selector: pb.LabelSelector{},
	})
	require.NoError(t, err)
	verify(resp, specA, specB, specC)

	// Expect current Etcd-backed status is returned with each shard.
	require.Equal(t, pc.ReplicaStatus_PRIMARY, resp.Shards[2].Status[0].Code)

	// Case: Exclude on label.
	resp, err = tf.service.List(context.Background(), &pc.ListRequest{
		Selector: pb.LabelSelector{Exclude: pb.MustLabelSet("foo", "")},
	})
	require.NoError(t, err)
	verify(resp, specB, specC)

	// Case: Meta-label "id" selects specific shards.
	resp, err = tf.service.List(context.Background(), &pc.ListRequest{
		Selector: pb.LabelSelector{Include: pb.MustLabelSet("id", shardC)},
	})
	require.NoError(t, err)
	verify(resp, specC)

	// Case: Errors on request validation error.
	_, err = tf.service.List(context.Background(), &pc.ListRequest{
		Selector: pb.LabelSelector{Include: pb.LabelSet{Labels: []pb.Label{{Name: "invalid label"}}}},
	})
	require.EqualError(t, err, `Selector.Include.Labels[0].Name: not a valid token (invalid label)`)

	tf.allocateShard(specB) // Cleanup.
	tf.allocateShard(specC)
}

func TestAPIApplyCases(t *testing.T) {
	var tf, cleanup = newTestFixture(t)
	defer cleanup()

	var specA = makeShard(shardA)
	var specB = makeShard(shardB)

	var verifyAndFetchRev = func(id pc.ShardID, expect pc.ShardSpec) int64 {
		var resp, err = tf.service.List(context.Background(), &pc.ListRequest{
			Selector: pb.LabelSelector{Include: pb.MustLabelSet("id", id.String())},
		})
		require.NoError(t, err)
		require.Equal(t, pc.Status_OK, resp.Status)
		require.Equal(t, expect, resp.Shards[0].Spec)
		return resp.Shards[0].ModRevision
	}
	var apply = func(req *pc.ApplyRequest) *pc.ApplyResponse {
		var resp, err = tf.service.Apply(context.Background(), req)
		require.NoError(t, err)
		return resp
	}

	// Case: Create new specs A & B.
	require.Equal(t, pc.Status_OK, apply(&pc.ApplyRequest{
		Changes: []pc.ApplyRequest_Change{
			{Upsert: specA},
			{Upsert: specB},
		},
	}).Status)

	// Case: Update existing spec B.
	var origSpecB = *specB
	specB.Labels = append(specB.Labels, pb.Label{Name: "foo", Value: "bar"})

	require.Equal(t, pc.Status_OK, apply(&pc.ApplyRequest{
		Changes: []pc.ApplyRequest_Change{
			{Upsert: specB, ExpectModRevision: verifyAndFetchRev(shardB, origSpecB)},
		},
	}).Status)

	// Case: Delete existing spec A.
	require.Equal(t, pc.Status_OK, apply(&pc.ApplyRequest{
		Changes: []pc.ApplyRequest_Change{
			{Delete: shardA, ExpectModRevision: verifyAndFetchRev(shardA, *specA)},
		},
	}).Status)

	// Case: Deletion at wrong revision fails.
	require.Equal(t, pc.Status_ETCD_TRANSACTION_FAILED, apply(&pc.ApplyRequest{
		Changes: []pc.ApplyRequest_Change{
			{Delete: shardB, ExpectModRevision: verifyAndFetchRev(shardB, *specB) - 1},
		},
	}).Status)

	// Case: Update at wrong revision fails.
	require.Equal(t, pc.Status_ETCD_TRANSACTION_FAILED, apply(&pc.ApplyRequest{
		Changes: []pc.ApplyRequest_Change{
			{Upsert: specB, ExpectModRevision: verifyAndFetchRev(shardB, *specB) - 1},
		},
	}).Status)

	// Case: Update with explicit revision of -1 succeeds.
	require.Equal(t, pc.Status_OK, apply(&pc.ApplyRequest{
		Changes: []pc.ApplyRequest_Change{
			{Upsert: specB, ExpectModRevision: -1},
		},
	}).Status)

	// Case: Deletion with explicit revision of -1 succeeds.
	require.Equal(t, pc.Status_OK, apply(&pc.ApplyRequest{
		Changes: []pc.ApplyRequest_Change{
			{Delete: shardB, ExpectModRevision: -1},
		},
	}).Status)

	// Case: Invalid requests fail with an error.
	var _, err = tf.service.Apply(context.Background(), &pc.ApplyRequest{
		Changes: []pc.ApplyRequest_Change{{Delete: "invalid shard id"}},
	})
	require.EqualError(t, err, `Changes[0].Delete: not a valid token (invalid shard id)`)
}

func TestAPIApplyShardsInBatches(t *testing.T) {
	var ss = newShardServerStub(t)
	defer ss.cleanup()

	var client = pc.NewRoutedShardClient(ss.client(), pb.NoopDispatchRouter{})
	var hdr = &pb.Header{
		ProcessId: pb.ProcessSpec_ID{Zone: "a", Suffix: "broker"},
		Route: pb.Route{
			Members:   []pb.ProcessSpec_ID{{Zone: "a", Suffix: "broker"}},
			Endpoints: []pb.Endpoint{ss.endpoint()},
			Primary:   0,
		},
		Etcd: pb.Header_Etcd{
			ClusterId: 12,
			MemberId:  34,
			Revision:  56,
			RaftTerm:  78,
		},
	}
	var fixture = &pc.ApplyRequest{
		Changes: []pc.ApplyRequest_Change{
			{Upsert: makeShard(shardA), ExpectModRevision: 1},
			{Upsert: makeShard(shardB), ExpectModRevision: 1},
		},
	}

	// Case: all changes are submitted in one batch.
	ss.ApplyFunc = func(ctx context.Context, req *pc.ApplyRequest) (*pc.ApplyResponse, error) {
		require.Equal(t, fixture, req)
		return &pc.ApplyResponse{Status: pc.Status_OK, Header: *hdr}, nil
	}
	var resp, err = ApplyShards(context.Background(), client, fixture)
	require.NoError(t, err)
	require.Equal(t, &pc.ApplyResponse{Status: pc.Status_OK, Header: *hdr}, resp)

	// Case: size == len(req.Changes).
	resp, err = ApplyShardsInBatches(context.Background(), client, fixture, 2)
	require.NoError(t, err)
	require.Equal(t, &pc.ApplyResponse{Status: pc.Status_OK, Header: *hdr}, resp)

	// Case: size < len(req.Changes). Changes are batched.
	var iter = 0
	ss.ApplyFunc = func(ctx context.Context, req *pc.ApplyRequest) (*pc.ApplyResponse, error) {
		require.Equal(t, &pc.ApplyRequest{
			Changes: []pc.ApplyRequest_Change{
				{Upsert: fixture.Changes[iter].Upsert, ExpectModRevision: 1},
			},
		}, req)
		iter++
		return &pc.ApplyResponse{Status: pc.Status_OK, Header: *hdr}, nil
	}
	resp, err = ApplyShardsInBatches(context.Background(), client, fixture, 1)
	require.NoError(t, err)
	require.Equal(t, &pc.ApplyResponse{Status: pc.Status_OK, Header: *hdr}, resp)
}

func TestAPIHintsCases(t *testing.T) {
	var tf, cleanup = newTestFixture(t)
	defer cleanup()

	// Install but don't allocate the shard (as recovery will insert hints,
	// and we don't want any yet).
	var spec = makeShard(shardA)
	tf.allocateShard(spec)

	// Case: No hints exist.
	var resp, err = tf.service.GetHints(context.Background(), &pc.GetHintsRequest{Shard: shardA})
	require.NoError(t, err)
	require.Equal(t, &pc.GetHintsResponse{
		Status:       pc.Status_OK,
		Header:       resp.Header,
		PrimaryHints: pc.GetHintsResponse_ResponseHints{},
		BackupHints:  []pc.GetHintsResponse_ResponseHints{{}, {}},
	}, resp)
	// Picking hints passes through the supplied shard recovery log.
	require.Equal(t, recoverylog.FSMHints{Log: "a/log"}, pickFirstHints(resp, "a/log"))

	// Now allocate the shard, and then install hint fixtures.
	tf.allocateShard(spec, localID)
	expectStatusCode(t, tf.state, pc.ReplicaStatus_PRIMARY)

	var shard = tf.resolver.shards[shardA]
	var mkHints = func(id int64) recoverylog.FSMHints {
		return recoverylog.FSMHints{
			Log: spec.RecoveryLog(),
			LiveNodes: []recoverylog.FnodeSegments{{
				Fnode: recoverylog.Fnode(id),
				Segments: []recoverylog.Segment{
					{Author: 0x1234, FirstSeqNo: id, LastSeqNo: id},
				},
			}},
		}
	}

	var expected []pc.GetHintsResponse_ResponseHints
	for _, i := range []int64{111, 333, 222} {
		var hints = mkHints(i)
		expected = append(expected, pc.GetHintsResponse_ResponseHints{
			Hints: &hints,
		})
	}
	require.NoError(t, storeRecordedHints(shard, mkHints(111)))
	require.NoError(t, storeRecoveredHints(shard, mkHints(222)))
	require.NoError(t, storeRecoveredHints(shard, mkHints(333)))

	// Case: Correctly fetch hints
	resp, err = tf.service.GetHints(shard.ctx, &pc.GetHintsRequest{Shard: shardA})
	require.NoError(t, err)
	require.Equal(t, &pc.GetHintsResponse{
		Status:       pc.Status_OK,
		Header:       resp.Header,
		PrimaryHints: expected[0],
		BackupHints:  expected[1:],
	}, resp)
	require.Equal(t, *expected[0].Hints, pickFirstHints(resp, "a/log"))

	// Case: No primary hints
	_, _ = tf.etcd.Delete(shard.ctx, spec.HintPrimaryKey())
	resp, err = tf.service.GetHints(shard.ctx, &pc.GetHintsRequest{Shard: shardA})
	require.NoError(t, err)
	require.Equal(t, &pc.GetHintsResponse{
		Status:       pc.Status_OK,
		Header:       resp.Header,
		PrimaryHints: pc.GetHintsResponse_ResponseHints{},
		BackupHints:  expected[1:],
	}, resp)
	require.Equal(t, *expected[1].Hints, pickFirstHints(resp, "a/log"))

	// Case: Hint key has not yet been written to
	require.NoError(t, storeRecordedHints(shard, mkHints(111)))
	tf.resolver.shards[shardA].resolved.spec.HintBackups = tf.resolver.shards[shardA].resolved.spec.HintBackups + 1
	resp, err = tf.service.GetHints(shard.ctx, &pc.GetHintsRequest{Shard: shardA})
	require.NoError(t, err)
	require.Equal(t, &pc.GetHintsResponse{
		Status:       pc.Status_OK,
		Header:       resp.Header,
		PrimaryHints: expected[0],
		BackupHints:  append(expected[1:], pc.GetHintsResponse_ResponseHints{}),
	}, resp)

	// Case: No backup hints
	tf.resolver.shards[shardA].resolved.spec.HintBackups = 0
	resp, err = tf.service.GetHints(shard.ctx, &pc.GetHintsRequest{Shard: shardA})
	require.NoError(t, err)
	require.Equal(t, &pc.GetHintsResponse{
		Status:       pc.Status_OK,
		Header:       resp.Header,
		PrimaryHints: expected[0],
	}, resp)

	// Case: Fetch hints for a non-existent shard
	resp, err = tf.service.GetHints(shard.ctx, &pc.GetHintsRequest{Shard: "missing-shard"})
	require.NoError(t, err)
	require.Equal(t, pc.Status_SHARD_NOT_FOUND, resp.Status)

	// Case: Hint does not correspond to correct recovery log
	var hints = mkHints(444)
	hints.Log = "incorrect/log"
	require.NoError(t, storeRecordedHints(shard, hints))
	resp, err = tf.service.GetHints(shard.ctx, &pc.GetHintsRequest{Shard: shardA})
	require.Nil(t, resp)
	require.EqualError(t, err, "hints.Log incorrect/log != ShardSpec.RecoveryLog recovery/logs/shard-A")

	// Case: Invalid hint has been stored
	hints = mkHints(555)
	hints.Log = ""
	require.NoError(t, storeRecordedHints(shard, hints))
	_, err = tf.service.GetHints(shard.ctx, &pc.GetHintsRequest{Shard: shardA})
	require.EqualError(t, err, "validating FSMHints: hinted log not provided")

	tf.allocateShard(spec) // Cleanup.
}

func TestVerifyReferencedJournalsCases(t *testing.T) {
	var tf, cleanup = newTestFixture(t)
	var ctx, jc = context.Background(), tf.broker.Client()
	defer cleanup()

	var req = &pc.ApplyRequest{
		Changes: []pc.ApplyRequest_Change{
			{Upsert: makeShard(shardA)},
			{Delete: "other-shard"},
			{Upsert: makeShard(shardB)},
		},
	}
	// Case: valid request => no error.
	require.NoError(t, VerifyReferencedJournals(ctx, jc, req))

	// Case: shards may omit recovery logs => no error.
	var tmp = req.Changes[0].Upsert.RecoveryLogPrefix
	req.Changes[0].Upsert.RecoveryLogPrefix = ""
	require.NoError(t, VerifyReferencedJournals(ctx, jc, req))
	req.Changes[0].Upsert.RecoveryLogPrefix = tmp

	// Case: recovery log has wrong content type.
	brokertest.CreateJournals(t, tf.broker, brokertest.Journal(
		pb.JournalSpec{Name: pb.Journal(aRecoveryLogPrefix + "/missing-type")}))

	req.Changes[2].Upsert.Id = "missing-type"
	require.EqualError(t, VerifyReferencedJournals(ctx, jc, req),
		"Shard[missing-type]: expected recovery/logs/missing-type to have content-type application/x-gazette-recoverylog but was ''")

	// Case: recovery log doesn't exist.
	req.Changes[2].Upsert.Id = "missing-log"
	require.EqualError(t, VerifyReferencedJournals(ctx, jc, req),
		"Shard[missing-log]: named journal does not exist (recovery/logs/missing-log)")

	req.Changes[2].Upsert.Id = shardB

	// Case: source journal has wrong content type.
	req.Changes[2].Upsert.Sources[1].Journal = pb.Journal(aRecoveryLogPrefix + "/" + shardC)
	require.EqualError(t, VerifyReferencedJournals(ctx, jc, req),
		"Shard[shard-B].Sources[recovery/logs/shard-C] message framing: unrecognized content-type (application/x-gazette-recoverylog)")

	// Case: source journal doesn't exist.
	req.Changes[2].Upsert.Sources[1].Journal = "does/not/exist"
	require.EqualError(t, VerifyReferencedJournals(ctx, jc, req),
		"Shard[shard-B]: named journal does not exist (does/not/exist)")
}
