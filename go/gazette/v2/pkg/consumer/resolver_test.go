package consumer

import (
	"context"
	"errors"
	"time"

	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	gc "github.com/go-check/check"
)

type ResolverSuite struct{}

func (s *ResolverSuite) TestResolutionCases(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	var check = func(r Resolution, err error) Resolution { c.Check(err, gc.IsNil); return r }
	var shardID ShardID = "a-shard"

	// Case: Shard does not exist, nor does our local MemberSpec (yet; it's created by allocateShard below).
	var r = check(tf.resolver.Resolve(ResolveArgs{Context: tf.ctx, ShardID: "other-shard-ID"}))
	c.Check(r, gc.DeepEquals, Resolution{
		Status: Status_SHARD_NOT_FOUND,
		Header: pb.Header{
			ProcessId: pb.ProcessSpec_ID{Zone: "local ConsumerSpec", Suffix: "missing from Etcd"},
			Route:     pb.Route{Primary: -1},
			Etcd:      r.Header.Etcd,
		},
	})

	// Case: Shard is remote, but has no primary.
	tf.allocateShard(c, makeShard(shardID), pb.ProcessSpec_ID{}, remoteID)

	r = check(tf.resolver.Resolve(ResolveArgs{Context: tf.ctx, ShardID: shardID}))
	c.Check(r, gc.DeepEquals, Resolution{
		Status: Status_NO_SHARD_PRIMARY,
		Header: pb.Header{
			ProcessId: localID,
			Route: pb.Route{
				Members:   []pb.ProcessSpec_ID{remoteID},
				Primary:   -1,
				Endpoints: []pb.Endpoint{"http://remote/endpoint"},
			},
			Etcd: r.Header.Etcd,
		},
		Spec: makeShard(shardID),
	})

	// Case: Shard is local, but has a remote primary.
	tf.allocateShard(c, makeShard(shardID), remoteID, localID)

	r = check(tf.resolver.Resolve(ResolveArgs{Context: tf.ctx, ShardID: shardID}))
	c.Check(r, gc.DeepEquals, Resolution{
		Status: Status_NOT_SHARD_PRIMARY,
		Header: pb.Header{
			ProcessId: localID,
			Route: pb.Route{
				Members:   []pb.ProcessSpec_ID{localID, remoteID},
				Primary:   1,
				Endpoints: []pb.Endpoint{"http://local/endpoint", "http://remote/endpoint"},
			},
			Etcd: r.Header.Etcd,
		},
		Spec: makeShard(shardID),
	})

	// Case: Shard is local, has a remote primary, and we may proxy.
	r = check(tf.resolver.Resolve(ResolveArgs{Context: tf.ctx, ShardID: shardID, MayProxy: true}))
	c.Check(r, gc.DeepEquals, Resolution{
		Status: Status_OK,
		Header: pb.Header{
			ProcessId: remoteID,
			Route: pb.Route{
				Members:   []pb.ProcessSpec_ID{localID, remoteID},
				Primary:   1,
				Endpoints: []pb.Endpoint{"http://local/endpoint", "http://remote/endpoint"},
			},
			Etcd: r.Header.Etcd,
		},
		Spec: makeShard(shardID),
	})

	// Interlude: wait for our assignment to reach TAILING. This ensures status
	// update KeySpace changes don't race the next case.
	expectStatusCode(c, tf.state, ReplicaStatus_TAILING)

	// Case: Shard is transitioning to primary. Resolution request includes a
	// ProxyHeader referencing a Revision we don't know about yet, but which will
	// make us primary. Expect Resolve blocks until the Revision is applied, and
	// until recovery completes and the Store is initialized.
	time.AfterFunc(10*time.Millisecond, func() {
		tf.allocateShard(c, makeShard(shardID), localID, remoteID)
	})

	r = check(tf.resolver.Resolve(ResolveArgs{
		Context: tf.ctx,
		ShardID: shardID,
		ProxyHeader: &pb.Header{
			ProcessId: localID,
			Etcd: pb.Header_Etcd{
				ClusterId: tf.ks.Header.ClusterId,
				Revision:  tf.ks.Header.Revision + 1,
			},
		},
	}))

	// These don't play well with gc.DeepEquals.
	var rShard, rStore, rDone = r.Shard, r.Store, r.Done
	r.Shard, r.Store, r.Done = nil, nil, nil

	c.Check(r, gc.DeepEquals, Resolution{
		Status: Status_OK,
		Header: pb.Header{
			ProcessId: localID,
			Route: pb.Route{
				Members:   []pb.ProcessSpec_ID{localID, remoteID},
				Primary:   0,
				Endpoints: []pb.Endpoint{"http://local/endpoint", "http://remote/endpoint"},
			},
			Etcd: r.Header.Etcd,
		},
		Spec: makeShard(shardID),
	})

	c.Check(rShard.Spec(), gc.DeepEquals, makeShard(shardID))
	c.Check(rStore.(*JSONFileStore).State, gc.DeepEquals, map[string]string{})
	rDone()

	expectStatusCode(c, tf.state, ReplicaStatus_PRIMARY)

	tf.allocateShard(c, makeShard(shardID)) // Cleanup.
}

func (s *ResolverSuite) TestErrorCases(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	var shardID ShardID = "a-shard"

	tf.app.newStoreErr = errors.New("NewStore error")
	tf.allocateShard(c, makeShard(shardID), localID)

	tf.ks.Mu.RLock()
	var clusterID, revision = tf.ks.Header.ClusterId, tf.ks.Header.Revision
	tf.ks.Mu.RUnlock()

	// Case: ProxyHeader has wrong ClusterID.
	var _, err = tf.resolver.Resolve(ResolveArgs{
		Context: tf.ctx,
		ShardID: shardID,
		ProxyHeader: &pb.Header{
			ProcessId: localID,
			Etcd: pb.Header_Etcd{
				ClusterId: 0x12345678,
			},
		},
	})
	c.Check(err, gc.ErrorMatches, `proxied request Etcd ClusterId doesn't match our own \(\d+ vs \d+\)`)

	// Case: ProxyHeader has wrong ProcessID.
	_, err = tf.resolver.Resolve(ResolveArgs{
		Context: tf.ctx,
		ShardID: shardID,
		ProxyHeader: &pb.Header{
			ProcessId: pb.ProcessSpec_ID{Zone: "wrong", Suffix: "ID"},
			Etcd: pb.Header_Etcd{
				ClusterId: clusterID,
			},
		},
	})
	c.Check(err, gc.ErrorMatches, `proxied request ProcessId doesn't match our own \(zone.*\)`)

	// Case: Context cancelled while waiting for a future revision.
	var ctx, cancel = context.WithCancel(tf.ctx)
	time.AfterFunc(10*time.Millisecond, cancel)

	_, err = tf.resolver.Resolve(ResolveArgs{
		Context: ctx,
		ShardID: shardID,
		ProxyHeader: &pb.Header{
			ProcessId: localID,
			Etcd: pb.Header_Etcd{
				ClusterId: clusterID,
				Revision:  revision + 100,
			},
		},
	})
	c.Check(err, gc.Equals, context.Canceled)

	// Case: Request context cancelled while waiting for store (which never resolves, because NewStore fails).
	ctx, cancel = context.WithCancel(tf.ctx)
	time.AfterFunc(10*time.Millisecond, cancel)

	_, err = tf.resolver.Resolve(ResolveArgs{Context: ctx, ShardID: shardID})
	c.Check(err, gc.Equals, context.Canceled)

	// Case: Replica context cancelled while waiting for store (which never resolves).
	time.AfterFunc(10*time.Millisecond, func() {
		tf.ks.Mu.RLock()
		tf.resolver.replicas[shardID].cancel()
		tf.ks.Mu.RUnlock()
	})

	_, err = tf.resolver.Resolve(ResolveArgs{Context: context.Background(), ShardID: shardID})
	c.Check(err, gc.Equals, context.Canceled)

	tf.allocateShard(c, makeShard(shardID)) // Cleanup.
}

func (s *ResolverSuite) TestReplicaTransitions(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	tf.allocateShard(c, makeShard("shard-a"), pb.ProcessSpec_ID{}, remoteID)
	tf.allocateShard(c, makeShard("shard-b"), remoteID, localID)
	tf.allocateShard(c, makeShard("shard-c"), remoteID, localID)

	tf.ks.Mu.RLock()
	c.Check(tf.resolver.replicas, gc.HasLen, 2)
	var repB = tf.resolver.replicas["shard-b"]
	var repC = tf.resolver.replicas["shard-c"]
	tf.ks.Mu.RUnlock()

	// Expect |repB| & |repC| begin playback and tail the log.
	<-repB.player.Tailing()
	<-repC.player.Tailing()

	// Promote "shard-b" to primary, and cancel "shard-c".
	tf.allocateShard(c, makeShard("shard-b"), localID, remoteID)
	tf.allocateShard(c, makeShard("shard-c"))

	tf.ks.Mu.RLock()
	c.Check(tf.resolver.replicas, gc.HasLen, 1)
	tf.ks.Mu.RUnlock()

	<-repB.player.Done() // Expect |repB| is transitioned to primary.
	<-repB.storeReadyCh
	<-repC.player.Done() // Expect |repC| is cancelled.
	<-repC.Context().Done()

	// Cancel "shard-b".
	tf.allocateShard(c, makeShard("shard-b"))

	tf.ks.Mu.RLock()
	c.Check(tf.resolver.replicas, gc.HasLen, 0)
	tf.ks.Mu.RUnlock()

	<-repB.Context().Done() // Expect |repB| is cancelled.

	tf.allocateShard(c, makeShard("shard-a")) // Cleanup.
}

var _ = gc.Suite(&ResolverSuite{})
