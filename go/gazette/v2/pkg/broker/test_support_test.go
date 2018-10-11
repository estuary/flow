package broker

import (
	"context"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/allocator"
	"github.com/LiveRamp/gazette/v2/pkg/broker/teststub"
	"github.com/LiveRamp/gazette/v2/pkg/etcdtest"
	"github.com/LiveRamp/gazette/v2/pkg/fragment"
	"github.com/LiveRamp/gazette/v2/pkg/keyspace"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/coreos/etcd/clientv3"
	gc "github.com/go-check/check"
)

// newTestFixture returns a testFixture with a prepared Etcd KeySpace context.
// The returned cleanup function should be deferred.
func newTestFixture(c *gc.C) (testFixture, func()) {
	var etcd = etcdtest.TestClient()
	var ctx, cancel = context.WithCancel(context.Background())

	var ks = NewKeySpace("/broker.test")
	ks.WatchApplyDelay = 0

	var grantResp, err = etcd.Grant(ctx, 60)
	c.Check(err, gc.IsNil)

	c.Assert(ks.Load(ctx, etcd, 0), gc.IsNil)
	go func() { c.Assert(ks.Watch(ctx, etcd), gc.Equals, context.Canceled) }()

	return testFixture{
			ctx:   ctx,
			etcd:  etcd,
			ks:    ks,
			lease: grantResp.ID,
		}, func() {
			etcd.Revoke(ctx, grantResp.ID)
			cancel()
		}
}

type testFixture struct {
	ctx   context.Context
	etcd  *clientv3.Client
	ks    *keyspace.KeySpace
	lease clientv3.LeaseID
}

// newReadyReplica returns a replica which has "performed" an initial remote
// fragment listing, unblocking read and write operations which rely on that
// load first completing. In normal operation the broker.Service would spawn a
// maintenanceLoop() for each replica which drives this action. For tests,
// we fake it.
func newReadyReplica(journal pb.Journal) *replica {
	var r = newReplica(journal)
	r.index.ReplaceRemote(fragment.CoverSet{}) // Initial "load".
	return r
}

type testBroker struct {
	id pb.ProcessSpec_ID
	teststub.Server

	*teststub.Broker // nil if not built with newMockBroker.
	*resolver        // nil if not built with newTestBroker.
}

// newTestBroker returns a local testBroker of |id|. |newReplicaFn| should be
// either |newReadyReplica| or |newReplica|.
func newTestBroker(c *gc.C, tf testFixture, id pb.ProcessSpec_ID,
	newReplicaFn func(journal pb.Journal) *replica) testBroker {

	var state = allocator.NewObservedState(tf.ks, allocator.MemberKey(tf.ks, id.Zone, id.Suffix))
	var res = newResolver(state, newReplicaFn)

	var svc = &Service{resolver: res, etcd: tf.etcd}
	var srv = teststub.NewServer(tf.ctx, svc)
	svc.jc = srv.MustClient()

	mustKeyValues(c, tf, map[string]string{
		state.LocalKey: (&pb.BrokerSpec{
			ProcessSpec: pb.ProcessSpec{
				Id:       id,
				Endpoint: srv.Endpoint(),
			},
		}).MarshalString(),
	})
	return testBroker{
		id:       id,
		Server:   srv,
		resolver: res,
	}
}

// newMockBroker returns a peer testBroker of |id|.
func newMockBroker(c *gc.C, tf testFixture, id pb.ProcessSpec_ID) testBroker {
	var broker = teststub.NewBroker(c, tf.ctx)
	var key = allocator.MemberKey(tf.ks, id.Zone, id.Suffix)

	mustKeyValues(c, tf, map[string]string{
		key: (&pb.BrokerSpec{
			ProcessSpec: pb.ProcessSpec{
				Id:       id,
				Endpoint: broker.Endpoint(),
			},
		}).MarshalString(),
	})
	return testBroker{
		id:     id,
		Server: broker.Server,
		Broker: broker,
	}
}

// newTestJournal creates |journal| with the given |replication| and assigned broker |ids|.
// A zero-valued ProcessSpec_ID means the allocation slot at that index is left empty.
func newTestJournal(c *gc.C, tf testFixture, spec pb.JournalSpec, ids ...pb.ProcessSpec_ID) {
	var kv = make(map[string]string)

	spec = pb.UnionJournalSpecs(spec, pb.JournalSpec{
		Replication: 0, // Must be provided by caller.
		Fragment: pb.JournalSpec_Fragment{
			Length:           1024,
			RefreshInterval:  time.Second,
			CompressionCodec: pb.CompressionCodec_SNAPPY,
		},
	})
	c.Assert(spec.Validate(), gc.IsNil)

	// Create the JournalSpec.
	kv[allocator.ItemKey(tf.ks, spec.Name.String())] = spec.MarshalString()

	// Create broker assignments.
	for slot, id := range ids {
		if id == (pb.ProcessSpec_ID{}) {
			continue
		}
		kv[allocator.AssignmentKey(tf.ks, allocator.Assignment{
			ItemID:       spec.Name.String(),
			MemberZone:   id.Zone,
			MemberSuffix: id.Suffix,
			Slot:         slot,
		})] = ""
	}
	mustKeyValues(c, tf, kv)
}

// mustKeyValues creates keys and values under the testFixture lease.
// The keys must not already exist.
func mustKeyValues(c *gc.C, tf testFixture, kvs map[string]string) {
	var cmps []clientv3.Cmp
	var ops []clientv3.Op

	for k, v := range kvs {
		cmps = append(cmps, clientv3.Compare(clientv3.Version(k), "=", 0))
		ops = append(ops, clientv3.OpPut(k, v, clientv3.WithLease(tf.lease)))
	}
	var resp, err = tf.etcd.Txn(tf.ctx).
		If(cmps...).Then(ops...).Commit()

	c.Assert(err, gc.IsNil)
	c.Assert(resp.Succeeded, gc.Equals, true)

	tf.ks.Mu.RLock()
	c.Assert(tf.ks.WaitForRevision(tf.ctx, resp.Header.Revision), gc.IsNil)
	tf.ks.Mu.RUnlock()
}
