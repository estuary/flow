package consumer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
)

func TestResolverCases(t *testing.T) {
	var tf, cleanup = newTestFixture(t)

	var resolve = func(args ResolveArgs) Resolution {
		if args.Context == nil {
			args.Context = context.Background()
		}
		var r, err = tf.resolver.Resolve(args)
		assert.NoError(t, err)
		return r
	}

	// Case: Shard does not exist, nor does our local MemberSpec (yet; it's created by allocateShard below).
	assert.Equal(t, Resolution{
		Status: pc.Status_SHARD_NOT_FOUND,
		Header: pb.Header{
			ProcessId: pb.ProcessSpec_ID{Zone: "local ConsumerSpec", Suffix: "missing from Etcd"},
			Route:     pb.Route{Primary: -1},
			Etcd:      pb.FromEtcdResponseHeader(tf.ks.Header),
		},
	}, resolve(ResolveArgs{ShardID: "other-shard-ID"}))

	// Case: Shard is remote, but has no primary.
	tf.allocateShard(makeShard(shardA), pb.ProcessSpec_ID{}, remoteID)

	assert.Equal(t, Resolution{
		Status: pc.Status_NO_SHARD_PRIMARY,
		Header: pb.Header{
			ProcessId: localID,
			Route: pb.Route{
				Members:   []pb.ProcessSpec_ID{remoteID},
				Primary:   -1,
				Endpoints: []pb.Endpoint{"http://remote/endpoint"},
			},
			Etcd: pb.FromEtcdResponseHeader(tf.ks.Header),
		},
		Spec: makeShard(shardA),
	}, resolve(ResolveArgs{ShardID: shardA}))

	// Case: Shard is local, but has a remote primary.
	tf.allocateShard(makeShard(shardA), remoteID, localID)

	assert.Equal(t, Resolution{
		Status: pc.Status_NOT_SHARD_PRIMARY,
		Header: pb.Header{
			ProcessId: localID,
			Route: pb.Route{
				Members:   []pb.ProcessSpec_ID{localID, remoteID},
				Primary:   1,
				Endpoints: []pb.Endpoint{"http://local/endpoint", "http://remote/endpoint"},
			},
			Etcd: pb.FromEtcdResponseHeader(tf.ks.Header),
		},
		Spec: makeShard(shardA),
	}, resolve(ResolveArgs{ShardID: shardA}))

	// Case: Shard is local, has a remote primary, and we may proxy.
	assert.Equal(t, Resolution{
		Status: pc.Status_OK,
		Header: pb.Header{
			ProcessId: remoteID,
			Route: pb.Route{
				Members:   []pb.ProcessSpec_ID{localID, remoteID},
				Primary:   1,
				Endpoints: []pb.Endpoint{"http://local/endpoint", "http://remote/endpoint"},
			},
			Etcd: pb.FromEtcdResponseHeader(tf.ks.Header),
		},
		Spec: makeShard(shardA),
	}, resolve(ResolveArgs{ShardID: shardA, MayProxy: true}))

	// Interlude: wait for our assignment to reach STANDBY, so its status update
	// doesn't race the following allocateShard() etcd transaction.
	expectStatusCode(t, tf.state, pc.ReplicaStatus_STANDBY)

	// Case: Shard is transitioning to primary. Resolution request includes a
	// ProxyHeader referencing a Revision we don't know about yet, but which will
	// make us primary. Expect Resolve blocks until the Revision is applied, and
	// until recovery completes and the Store is initialized.
	time.AfterFunc(time.Millisecond, func() {
		tf.allocateShard(makeShard(shardA), localID, remoteID)
	})
	var r = resolve(ResolveArgs{
		ShardID: shardA,
		ProxyHeader: &pb.Header{
			ProcessId: localID,
			Etcd: pb.Header_Etcd{
				ClusterId: tf.ks.Header.ClusterId,
				Revision:  tf.ks.Header.Revision + 1,
			},
		},
	})
	assert.Equal(t, pc.Status_OK, r.Status)
	assert.Equal(t, pb.Header{
		ProcessId: localID,
		Route: pb.Route{
			Members:   []pb.ProcessSpec_ID{localID, remoteID},
			Primary:   0,
			Endpoints: []pb.Endpoint{"http://local/endpoint", "http://remote/endpoint"},
		},
		Etcd: r.Header.Etcd,
	}, r.Header)
	assert.Equal(t, makeShard(shardA), r.Spec)
	assert.NotNil(t, r.Shard)
	assert.Equal(t, &map[string]string{}, r.Store.(*JSONFileStore).State)
	r.Done()

	expectStatusCode(t, tf.state, pc.ReplicaStatus_PRIMARY)

	// Case: Stat requests ReadThrough offset which is read in the future.
	time.AfterFunc(time.Millisecond, func() {
		_, _ = tf.pub.PublishCommitted(toSourceB, &testMessage{Key: "read", Value: "through"})
	})
	r = resolve(ResolveArgs{
		ShardID:     shardA,
		ReadThrough: pb.Offsets{sourceB.Name: 1},
	})
	assert.Equal(t, pc.Status_OK, r.Status)
	assert.NotNil(t, r.Shard)
	assert.Equal(t, &map[string]string{"read": "through"}, r.Store.(*JSONFileStore).State)
	r.Done()

	// Interlude: Resolver is asked to stop local serving.
	tf.resolver.stopServingLocalShards()

	// Case: resolving to a remote peer still succeeds.
	tf.allocateShard(makeShard(shardA), remoteID, localID)
	r = resolve(ResolveArgs{ShardID: shardA, MayProxy: true})
	assert.Equal(t, pc.Status_OK, r.Status)
	assert.Equal(t, remoteID, r.Header.ProcessId)

	// Case: but an attempt to resolve to a local replica fails.
	tf.allocateShard(makeShard(shardA), localID)
	var _, err = tf.resolver.Resolve(ResolveArgs{Context: context.Background(), ShardID: shardA})
	assert.Equal(t, ErrResolverStopped, err)

	tf.allocateShard(makeShard(shardA)) // Cleanup.
	cleanup()
}

func TestResolverErrorCases(t *testing.T) {
	var tf, cleanup = newTestFixture(t)
	defer cleanup()

	var clusterID, revision = tf.ks.Header.ClusterId, tf.ks.Header.Revision

	tf.app.newStoreErr = errors.New("an error") // app.NewStore fails.
	tf.allocateShard(makeShard(shardA), localID)

	// Case: ProxyHeader has wrong ClusterID.
	var _, err = tf.resolver.Resolve(ResolveArgs{
		Context: context.Background(),
		ShardID: shardA,
		ProxyHeader: &pb.Header{
			ProcessId: localID,
			Etcd: pb.Header_Etcd{
				ClusterId: 0x12345678,
			},
		},
	})
	assert.Regexp(t, `proxied request Etcd ClusterId doesn't match our own \(\d+ vs \d+\)`, err)

	// Case: ProxyHeader has wrong ProcessID.
	_, err = tf.resolver.Resolve(ResolveArgs{
		Context: context.Background(),
		ShardID: shardA,
		ProxyHeader: &pb.Header{
			ProcessId: pb.ProcessSpec_ID{Zone: "wrong", Suffix: "ID"},
			Etcd: pb.Header_Etcd{
				ClusterId: clusterID,
			},
		},
	})
	assert.Regexp(t, `proxied request ProcessId doesn't match our own \(zone.*\)`, err)

	// Case: Context cancelled while waiting for a future revision.
	var ctx, cancel = context.WithCancel(context.Background())
	time.AfterFunc(time.Millisecond, cancel)

	_, err = tf.resolver.Resolve(ResolveArgs{
		Context: ctx,
		ShardID: shardA,
		ProxyHeader: &pb.Header{
			ProcessId: localID,
			Etcd: pb.Header_Etcd{
				ClusterId: clusterID,
				Revision:  revision + 100,
			},
		},
	})
	assert.Equal(t, context.Canceled, err)

	// Case: Request context cancelled while waiting for store (which never resolves, because NewStore fails).
	ctx, cancel = context.WithCancel(context.Background())
	time.AfterFunc(time.Millisecond, cancel)

	_, err = tf.resolver.Resolve(ResolveArgs{Context: ctx, ShardID: shardA})
	assert.Equal(t, context.Canceled, err)

	// Case: Replica context cancelled while waiting for store (which never resolves).
	time.AfterFunc(time.Millisecond, tf.resolver.stopServingLocalShards)

	_, err = tf.resolver.Resolve(ResolveArgs{Context: context.Background(), ShardID: shardA})
	assert.Equal(t, context.Canceled, err)

	tf.allocateShard(makeShard(shardA)) // Cleanup.
}

func TestResolverShardTransitions(t *testing.T) {
	var tf, cleanup = newTestFixture(t)
	defer cleanup()

	tf.allocateShard(makeShard(shardA), pb.ProcessSpec_ID{}, remoteID)
	tf.allocateShard(makeShard(shardB), remoteID, localID)
	tf.allocateShard(makeShard(shardC), remoteID, localID)

	tf.ks.Mu.RLock()
	assert.Len(t, tf.resolver.shards, 2)
	var sB = tf.resolver.shards[shardB]
	var sC = tf.resolver.shards[shardC]
	tf.ks.Mu.RUnlock()

	// Expect |sB| & |sC| begin playback and tail the log.
	<-sB.recovery.player.Tailing()
	<-sC.recovery.player.Tailing()

	// Promote |sB| to primary, and cancel |sC|.
	tf.allocateShard(makeShard(shardB), localID, remoteID)
	tf.allocateShard(makeShard(shardC))

	tf.ks.Mu.RLock()
	assert.Len(t, tf.resolver.shards, 1)
	tf.ks.Mu.RUnlock()

	<-sB.recovery.player.Done() // Expect |sB| is transitioned to primary.
	<-sB.storeReadyCh
	<-sC.recovery.player.Done() // Expect |sC| is cancelled.
	<-sC.Context().Done()

	// Cancel |sdB|.
	tf.allocateShard(makeShard(shardB))

	tf.ks.Mu.RLock()
	assert.Len(t, tf.resolver.shards, 0)
	tf.ks.Mu.RUnlock()

	<-sB.Context().Done() // Expect |sB| is cancelled.

	tf.allocateShard(makeShard(shardA)) // Cleanup.
}
