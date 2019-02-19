package consumer

import (
	"context"
	"errors"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/client"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/LiveRamp/gazette/v2/pkg/recoverylog"
	gc "github.com/go-check/check"
)

type APISuite struct{}

func (s *APISuite) TestStatCases(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	var spec = makeShard(shardA)
	tf.allocateShard(c, spec, localID)
	expectStatusCode(c, tf.state, ReplicaStatus_PRIMARY)

	var res, err = tf.resolver.Resolve(ResolveArgs{Context: tf.ctx, ShardID: shardA})
	c.Assert(err, gc.IsNil)
	defer res.Done()

	runSomeTransactions(c, res.Shard)

	// Determine the write head of |sourceA|. We expect Stat reports we've
	// consumed through this offset.
	var aa = res.Shard.JournalClient().StartAppend(sourceA)
	c.Check(aa.Release(), gc.IsNil)
	<-aa.Done()
	var expectOffset = aa.Response().Commit.End

	// Case: Stat of local shard.
	resp, err := tf.service.Stat(tf.ctx, &StatRequest{Shard: shardA})
	c.Check(err, gc.IsNil)
	c.Check(resp.Status, gc.Equals, Status_OK)
	c.Check(resp.Offsets, gc.DeepEquals, map[pb.Journal]int64{sourceA: expectOffset})
	c.Check(resp.Header.ProcessId, gc.DeepEquals, localID)

	// Case: Stat of non-existent Shard.
	resp, err = tf.service.Stat(tf.ctx, &StatRequest{Shard: "missing-shard"})
	c.Check(err, gc.IsNil)
	c.Check(resp.Status, gc.Equals, Status_SHARD_NOT_FOUND)

	// TODO(johnny): Proxy case ought to be unit-tested here.
	// Adding it is currently low-value because it's covered by other E2E tests
	// and newTestFixture isn't set up for verifying proxy interactions.

	tf.allocateShard(c, spec) // Cleanup.
}

func (s *APISuite) TestListCases(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	var specA = makeShard(shardA)
	specA.Labels = append(specA.Labels, pb.Label{Name: "foo", Value: "bar"})
	var specB = makeShard(shardB)
	specB.Labels = append(specB.Labels, pb.Label{Name: "bar", Value: "baz"})
	var specC = makeShard(shardC)

	tf.allocateShard(c, specA)
	tf.allocateShard(c, specB, remoteID)
	tf.allocateShard(c, specC, localID, remoteID)
	expectStatusCode(c, tf.state, ReplicaStatus_PRIMARY)

	var verify = func(resp *ListResponse, expect ...*ShardSpec) {
		c.Check(resp.Status, gc.Equals, Status_OK)
		c.Assert(resp.Shards, gc.HasLen, len(expect))

		for i, exp := range expect {
			c.Check(resp.Shards[i].Spec, gc.DeepEquals, *exp)

			var numAsn int
			if exp == specB {
				numAsn = 1
			} else if exp == specC {
				numAsn = 2
			}
			c.Check(resp.Shards[i].Status, gc.HasLen, numAsn)
			c.Check(resp.Shards[i].Route.Endpoints, gc.HasLen, numAsn)
		}
	}

	// Case: Empty selector returns all shards.
	var resp, err = tf.service.List(tf.ctx, &ListRequest{
		Selector: pb.LabelSelector{},
	})
	c.Check(err, gc.IsNil)
	verify(resp, specA, specB, specC)

	// Expect current Etcd-backed status is returned with each shard.
	c.Check(resp.Shards[2].Status[0].Code, gc.Equals, ReplicaStatus_PRIMARY)

	// Case: Exclude on label.
	resp, err = tf.service.List(tf.ctx, &ListRequest{
		Selector: pb.LabelSelector{Exclude: pb.MustLabelSet("foo", "")},
	})
	c.Check(err, gc.IsNil)
	verify(resp, specB, specC)

	// Case: Meta-label "id" selects specific shards.
	resp, err = tf.service.List(tf.ctx, &ListRequest{
		Selector: pb.LabelSelector{Include: pb.MustLabelSet("id", shardC)},
	})
	c.Check(err, gc.IsNil)
	verify(resp, specC)

	// Case: Errors on request validation error.
	_, err = tf.service.List(tf.ctx, &ListRequest{
		Selector: pb.LabelSelector{Include: pb.LabelSet{Labels: []pb.Label{{Name: "invalid label"}}}},
	})
	c.Check(err, gc.ErrorMatches, `Selector.Include.Labels\[0\].Name: not a valid token \(invalid label\)`)

	tf.allocateShard(c, specB) // Cleanup.
	tf.allocateShard(c, specC)
}

func (s *APISuite) TestApplyCases(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	var specA = makeShard(shardA)
	var specB = makeShard(shardB)

	var verifyAndFetchRev = func(id ShardID, expect ShardSpec) int64 {
		var resp, err = tf.service.List(tf.ctx, &ListRequest{
			Selector: pb.LabelSelector{Include: pb.MustLabelSet("id", id.String())},
		})
		c.Assert(err, gc.IsNil)
		c.Assert(resp.Status, gc.Equals, Status_OK)
		c.Assert(resp.Shards[0].Spec, gc.DeepEquals, expect)
		return resp.Shards[0].ModRevision
	}

	var must = func(r *ApplyResponse, err error) *ApplyResponse {
		c.Assert(err, gc.IsNil)
		return r
	}

	// Case: Create new specs A & B.
	c.Check(must(tf.service.Apply(tf.ctx, &ApplyRequest{
		Changes: []ApplyRequest_Change{
			{Upsert: specA},
			{Upsert: specB},
		},
	})).Status, gc.Equals, Status_OK)

	// Case: Update existing spec B.
	var origSpecB = *specB
	specB.Labels = append(specB.Labels, pb.Label{Name: "foo", Value: "bar"})

	c.Check(must(tf.service.Apply(tf.ctx, &ApplyRequest{
		Changes: []ApplyRequest_Change{
			{Upsert: specB, ExpectModRevision: verifyAndFetchRev(shardB, origSpecB)},
		},
	})).Status, gc.Equals, Status_OK)

	// Case: Delete existing spec A.
	c.Check(must(tf.service.Apply(tf.ctx, &ApplyRequest{
		Changes: []ApplyRequest_Change{
			{Delete: shardA, ExpectModRevision: verifyAndFetchRev(shardA, *specA)},
		},
	})).Status, gc.Equals, Status_OK)

	// Case: Deletion at wrong revision fails.
	c.Check(must(tf.service.Apply(tf.ctx, &ApplyRequest{
		Changes: []ApplyRequest_Change{
			{Delete: shardB, ExpectModRevision: verifyAndFetchRev(shardB, *specB) - 1},
		},
	})).Status, gc.Equals, Status_ETCD_TRANSACTION_FAILED)

	// Case: Update at wrong revision fails.
	c.Check(must(tf.service.Apply(tf.ctx, &ApplyRequest{
		Changes: []ApplyRequest_Change{
			{Upsert: specB, ExpectModRevision: verifyAndFetchRev(shardB, *specB) - 1},
		},
	})).Status, gc.Equals, Status_ETCD_TRANSACTION_FAILED)

	// Case: Invalid requests fail with an error.
	var _, err = tf.service.Apply(tf.ctx, &ApplyRequest{
		Changes: []ApplyRequest_Change{{Delete: "invalid shard id"}},
	})
	c.Check(err, gc.ErrorMatches, `Changes\[0\].Delete: not a valid token \(invalid shard id\)`)
}

func (s *APISuite) TestApplyShardsInBatches(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var ss = newShardServerStub(ctx, c)
	var client = NewRoutedShardClient(ss.MustClient(), client.NewRouteCache(2, time.Hour))

	var hdr = buildHeaderFixture(ss)
	// Case: size is 0. All changes are submitted.
	var fixture = buildApplyReqFixtue()
	var expected = &ApplyResponse{
		Status: Status_OK,
		Header: *hdr,
	}
	ss.ApplyFunc = func(ctx context.Context, req *ApplyRequest) (*ApplyResponse, error) {
		c.Check(req, gc.DeepEquals, fixture)
		return expected, nil
	}
	resp, err := ApplyShardsInBatches(ctx, client, fixture, 0)
	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, expected)

	// Case: size == len(req.Changes). All changes are submitted.
	resp, err = ApplyShardsInBatches(ctx, client, fixture, 3)
	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, expected)

	// Case: size < len(req.Changes). Changes are batched.
	var iter = 0
	ss.ApplyFunc = func(ctx context.Context, req *ApplyRequest) (*ApplyResponse, error) {
		c.Check(req, gc.DeepEquals, &ApplyRequest{
			Changes: []ApplyRequest_Change{
				{Upsert: fixture.Changes[iter].Upsert, ExpectModRevision: 1},
			},
		})
		iter++
		return expected, nil
	}
	resp, err = ApplyShardsInBatches(ctx, client, fixture, 1)
	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, expected)

	// Case: empty list of changes.
	ss.ApplyFunc = func(ctx context.Context, req *ApplyRequest) (*ApplyResponse, error) {
		c.Error("should not be called")
		return nil, nil
	}
	resp, err = ApplyShardsInBatches(ctx, client, &ApplyRequest{}, 1)
	c.Check(resp, gc.DeepEquals, &ApplyResponse{})
	c.Check(err, gc.IsNil)

	// Case: Return Error from backend.
	ss.ApplyFunc = func(ctx context.Context, req *ApplyRequest) (*ApplyResponse, error) {
		return nil, errors.New("something has gone wrong")
	}
	resp, err = ApplyShardsInBatches(ctx, client, fixture, 1)
	c.Check(err, gc.ErrorMatches, "rpc error: code = Unknown desc = something has gone wrong")

	// Case: Status !OK mapped as an error.
	ss.ApplyFunc = func(ctx context.Context, req *ApplyRequest) (*ApplyResponse, error) {
		return &ApplyResponse{
			Status: Status_ETCD_TRANSACTION_FAILED,
			Header: *hdr,
		}, nil
	}
	resp, err = ApplyShardsInBatches(ctx, client, fixture, 1)
	c.Check(err.Error(), gc.Matches, Status_ETCD_TRANSACTION_FAILED.String())

	// Case: Validation error mapped as error.
	ss.ApplyFunc = func(ctx context.Context, req *ApplyRequest) (*ApplyResponse, error) {
		return &ApplyResponse{
			Status: Status_ETCD_TRANSACTION_FAILED,
		}, nil
	}
	resp, err = ApplyShardsInBatches(ctx, client, fixture, 1)
	c.Check(err, gc.ErrorMatches, `Header.Route: invalid Primary \(0; expected -1 <= Primary < 0\)`)
}

func (s *APISuite) TestHintsCases(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	var spec = makeShard(shardA)
	tf.allocateShard(c, spec, localID)
	expectStatusCode(c, tf.state, ReplicaStatus_PRIMARY)

	var r = tf.resolver.replicas[shardA]
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

	var expected []GetHintsResponse_ResponseHints
	for _, i := range []int64{111, 333, 222} {
		var hints = mkHints(i)
		expected = append(expected, GetHintsResponse_ResponseHints{
			Hints: &hints,
		})
	}
	c.Check(storeRecordedHints(r, mkHints(111), r.etcd), gc.IsNil)
	c.Check(storeRecoveredHints(r, mkHints(222), r.etcd), gc.IsNil)
	c.Check(storeRecoveredHints(r, mkHints(333), r.etcd), gc.IsNil)

	var hdr = pb.NewUnroutedHeader(tf.state)
	// Case: Correctly fetch hints
	var resp, err = tf.service.GetHints(tf.ctx, &GetHintsRequest{Shard: shardA})
	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, &GetHintsResponse{
		Status:       Status_OK,
		Header:       hdr,
		PrimaryHints: expected[0],
		BackupHints:  expected[1:],
	})

	// Case: No primary hints
	_, _ = r.etcd.Delete(r.ctx, r.spec.HintPrimaryKey())
	resp, err = tf.service.GetHints(tf.ctx, &GetHintsRequest{Shard: shardA})
	c.Check(resp, gc.DeepEquals, &GetHintsResponse{
		Status:       Status_OK,
		Header:       hdr,
		PrimaryHints: GetHintsResponse_ResponseHints{},
		BackupHints:  expected[1:],
	})

	// Case: Hint key has not yet been written to
	c.Check(storeRecordedHints(r, mkHints(111), r.etcd), gc.IsNil)
	tf.resolver.replicas[shardA].spec.HintBackups = tf.resolver.replicas[shardA].spec.HintBackups + 1
	resp, err = tf.service.GetHints(tf.ctx, &GetHintsRequest{Shard: shardA})
	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, &GetHintsResponse{
		Status:       Status_OK,
		Header:       hdr,
		PrimaryHints: expected[0],
		BackupHints:  append(expected[1:], GetHintsResponse_ResponseHints{}),
	})

	// Case: No backup hints
	tf.resolver.replicas[shardA].spec.HintBackups = 0
	resp, err = tf.service.GetHints(tf.ctx, &GetHintsRequest{Shard: shardA})
	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, &GetHintsResponse{
		Status:       Status_OK,
		Header:       hdr,
		PrimaryHints: expected[0],
	})

	// Case: Fetch hints for a non-existant shard
	resp, err = tf.service.GetHints(tf.ctx, &GetHintsRequest{Shard: "missing-shard"})
	c.Check(err, gc.IsNil)
	c.Check(resp.Status, gc.Equals, Status_SHARD_NOT_FOUND)
	// Case: Hint does not correspond to correct recovery log
	var hints = mkHints(444)
	hints.Log = "incorrect/log"
	c.Check(storeRecordedHints(r, hints, r.etcd), gc.IsNil)
	resp, err = tf.service.GetHints(tf.ctx, &GetHintsRequest{Shard: shardA})
	c.Check(resp, gc.IsNil)
	c.Check(err.Error(), gc.DeepEquals, "recovered hints recovery log doesn't match ShardSpec.RecoveryLog (incorrect/log vs recovery/logs/shard-A)")

	// Case: Invalid hint has been stored
	hints = mkHints(555)
	hints.Log = ""
	c.Check(storeRecordedHints(r, hints, r.etcd), gc.IsNil)
	resp, err = tf.service.GetHints(tf.ctx, &GetHintsRequest{Shard: shardA})
	c.Check(err.Error(), gc.DeepEquals, "validating FSMHints: hinted log not provided")

	tf.allocateShard(c, spec) // Cleanup.
}

func buildHeaderFixture(ep interface{ Endpoint() pb.Endpoint }) *pb.Header {
	return &pb.Header{
		ProcessId: pb.ProcessSpec_ID{Zone: "a", Suffix: "broker"},
		Route: pb.Route{
			Members:   []pb.ProcessSpec_ID{{Zone: "a", Suffix: "broker"}},
			Endpoints: []pb.Endpoint{ep.Endpoint()},
			Primary:   0,
		},
		Etcd: pb.Header_Etcd{
			ClusterId: 12,
			MemberId:  34,
			Revision:  56,
			RaftTerm:  78,
		},
	}
}

func buildApplyReqFixtue() *ApplyRequest {
	var specA = makeShard(shardA)
	var specB = makeShard(shardB)

	return &ApplyRequest{
		Changes: []ApplyRequest_Change{
			{Upsert: specA, ExpectModRevision: 1},
			{Upsert: specB, ExpectModRevision: 1},
		},
	}
}

var _ = gc.Suite(&APISuite{})
