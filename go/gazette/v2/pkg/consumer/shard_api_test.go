package consumer

import (
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	gc "github.com/go-check/check"
)

type APISuite struct{}

func (s *APISuite) TestStatCases(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	var spec = makeShard(shardA)
	tf.allocateShard(c, spec, localID)
	expectStatusCode(c, tf.state, ReplicaStatus_PRIMARY)

	var r = tf.resolver.replicas[shardA]
	runSomeTransactions(c, r, r.app.(*testApplication), r.store.(*JSONFileStore))

	// Determine the write head of |sourceA|. We expect Stat reports we've
	// consumed through this offset.
	var aa = r.JournalClient().StartAppend(sourceA)
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

var _ = gc.Suite(&APISuite{})
