// Package brokertest provides utilities for testing components requiring a live Gazette broker.
package brokertest

import (
	"context"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/allocator"
	"github.com/LiveRamp/gazette/v2/pkg/broker"
	"github.com/LiveRamp/gazette/v2/pkg/broker/teststub"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/coreos/etcd/clientv3"
	gc "github.com/go-check/check"
)

// Broker is a lightweight, embedded Gazette broker suitable for testing client
// functionality which depends on the availability of the Gazette service.
type Broker struct {
	cancel context.CancelFunc
	etcd   *clientv3.Client
	idleCh chan struct{}
	lease  clientv3.LeaseID
	srv    teststub.Server
	state  *allocator.State
	svc    broker.Service
}

// NewBroker returns a ready Broker with the given Context. Note that journals
// must still be created with CreateJournal before use.
func NewBroker(c *gc.C, etcd *clientv3.Client, zone, suffix string) *Broker {
	var ctx, cancel = context.WithCancel(context.Background())

	var grant, err = etcd.Grant(ctx, 0)
	c.Assert(err, gc.IsNil)

	var ks = broker.NewKeySpace("/brokertest")
	ks.WatchApplyDelay = 0 // Speed test execution.
	var key = allocator.MemberKey(ks, zone, suffix)

	var bk = &Broker{
		cancel: cancel,
		etcd:   etcd,
		idleCh: make(chan struct{}, 1),
		lease:  grant.ID,
		state:  allocator.NewObservedState(ks, key),
	}
	bk.srv = teststub.NewServer(ctx, &bk.svc)
	bk.svc = *broker.NewService(bk.state, pb.NewJournalClient(bk.srv.Conn), etcd)

	var spec = pb.BrokerSpec{
		ProcessSpec: pb.ProcessSpec{
			Id:       pb.ProcessSpec_ID{Zone: zone, Suffix: suffix},
			Endpoint: bk.srv.Endpoint(),
		},
		JournalLimit: 100,
	}
	resp, err := etcd.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", 0)).
		Then(clientv3.OpPut(key, spec.MarshalString(), clientv3.WithLease(grant.ID))).
		Commit()

	c.Assert(err, gc.IsNil)
	c.Assert(resp.Succeeded, gc.Equals, true)
	c.Assert(ks.Load(ctx, etcd, resp.Header.Revision), gc.IsNil)

	go func() {
		c.Assert(ks.Watch(ctx, etcd), gc.Equals, context.Canceled)
	}()
	go func() {
		// We signal |idleCh| on the first idle TestHook callback which follows
		// at least one non-idle TestHook. Intuitively, we signal only if Allocate
		// did some work (eg, shuffled assignments), and has since become idle.
		// We do not signal on, eg, an assignment key/value update which does
		// not result in a re-allocation.
		var signalOnIdle bool

		allocator.Allocate(allocator.AllocateArgs{
			Context: ctx,
			Etcd:    etcd,
			State:   bk.state,
			TestHook: func(_ int, isIdle bool) {
				if !isIdle {
					signalOnIdle = true
					return
				} else if !signalOnIdle {
					return
				} else {
					bk.idleCh <- struct{}{}
					signalOnIdle = false
				}
			},
		})
		close(bk.idleCh) // Signal Allocate has exited.
	}()

	return bk
}

// Client of the test Broker.
func (b *Broker) Client() pb.JournalClient { return pb.NewJournalClient(b.srv.Conn) }

// AllocateIdleCh signals when the Broker's Allocate loop took an action, such
// as updating a journal assignment, and has since become idle.
func (b *Broker) AllocateIdleCh() <-chan struct{} { return b.idleCh }

// ZeroJournalLimit of the Broker. The test Broker will eventually exit,
// assuming other Broker(s) are available to take over the assignments.
func (b *Broker) ZeroJournalLimit(c *gc.C) {
	b.state.KS.Mu.RLock()
	var kv = b.state.Members[b.state.LocalMemberInd]
	b.state.KS.Mu.RUnlock()

	var spec = *kv.Decoded.(allocator.Member).MemberValue.(*pb.BrokerSpec)
	spec.ZeroLimit()

	resp, err := b.etcd.Txn(context.Background()).
		If(clientv3.Compare(clientv3.ModRevision(string(kv.Raw.Key)), "=", kv.Raw.ModRevision)).
		Then(clientv3.OpPut(string(kv.Raw.Key), spec.MarshalString(), clientv3.WithIgnoreLease())).
		Commit()

	c.Assert(err, gc.IsNil)
	c.Assert(resp.Succeeded, gc.Equals, true)
}

// RevokeLease of the Broker, allowing its Allocate loop to immediately exit.
func (b *Broker) RevokeLease(c *gc.C) {
	var _, err = b.etcd.Revoke(context.Background(), b.lease)
	c.Assert(err, gc.IsNil)
}

// WaitForExit of the test Broker Allocate loop, and complete its teardown.
// WaitForExit will block indefinitely if the Broker's Journal limit is not
// also zeroed (and another Broker takes over), or its lease revoked.
func (b *Broker) WaitForExit() {
	for range b.idleCh { // |idleCh| is closed when Allocate completes.
	}
	// Cancel our context, which aborts our gRPC server and Etcd Watch,
	// and closes our client connection.
	b.cancel()
}

// Journal returns |spec| after applying reasonable test defaults for fields
// which are not already set.
func Journal(spec pb.JournalSpec) *pb.JournalSpec {
	spec = pb.UnionJournalSpecs(spec, pb.JournalSpec{
		Replication: 1,
		Fragment: pb.JournalSpec_Fragment{
			Length:           1 << 24, // 16MB.
			CompressionCodec: pb.CompressionCodec_SNAPPY,
			RefreshInterval:  time.Minute,
			Retention:        time.Hour,
		},
	})
	return &spec
}

// CreateJournals using the Broker Apply API, and wait for them to be allocated.
func CreateJournals(c *gc.C, bk *Broker, specs ...*pb.JournalSpec) {
	var req = new(pb.ApplyRequest)
	for _, spec := range specs {
		req.Changes = append(req.Changes, pb.ApplyRequest_Change{Upsert: spec})
	}

	var resp, err = bk.Client().Apply(pb.WithDispatchDefault(context.Background()), req)
	c.Assert(err, gc.IsNil)
	c.Assert(resp.Status, gc.Equals, pb.Status_OK)

	// Wait for journal assignments to update.
	<-bk.AllocateIdleCh()
}
