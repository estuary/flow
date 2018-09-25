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
	"github.com/coreos/etcd/clientv3/concurrency"
	gc "github.com/go-check/check"
	"google.golang.org/grpc"
)

// Broker is a lightweight, embedded Gazette broker suitable for testing client
// functionality which depends on the availability of the Gazette service.
type Broker struct {
	cancel  context.CancelFunc
	conn    *grpc.ClientConn
	etcd    *clientv3.Client
	idleCh  chan struct{}
	session *concurrency.Session
	srv     *teststub.Server
	state   *allocator.State
	svc     broker.Service
}

// NewBroker returns a ready Broker with the given Context. Note that journals
// must still be created with CreateJournal before use.
func NewBroker(c *gc.C, etcd *clientv3.Client, zone, suffix string) *Broker {
	var ctx, cancel = context.WithCancel(context.Background())

	var session, err = concurrency.NewSession(etcd)
	c.Assert(err, gc.IsNil)

	var ks = broker.NewKeySpace("/brokertest")
	ks.WatchApplyDelay = 0
	var key = allocator.MemberKey(ks, zone, suffix)

	var bk = &Broker{
		cancel:  cancel,
		etcd:    etcd,
		idleCh:  make(chan struct{}, 1),
		session: session,
		state:   allocator.NewObservedState(ks, key),
	}
	bk.srv = teststub.NewServer(c, ctx, &bk.svc)
	bk.conn = bk.srv.MustConn()
	bk.svc = *broker.NewService(bk.state, pb.NewJournalClient(bk.conn), etcd)

	var spec = pb.BrokerSpec{
		ProcessSpec: pb.ProcessSpec{
			Id:       pb.ProcessSpec_ID{Zone: zone, Suffix: suffix},
			Endpoint: bk.srv.Endpoint(),
		},
		JournalLimit: 100,
	}
	resp, err := etcd.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", 0)).
		Then(clientv3.OpPut(key, spec.MarshalString(), clientv3.WithLease(session.Lease()))).
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
		// did some work (eg, shuffled assignments), and has since become idle. We
		// do not signal on, say, an assignment update which does not cause a
		// re-allocation.
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

func (b *Broker) Client() pb.JournalClient { return pb.NewJournalClient(b.conn) }

func (b *Broker) AllocateIdleCh() <-chan struct{} { return b.idleCh }

func (b *Broker) ZeroJournalLimit(c *gc.C) {
	var kv = b.state.Members[b.state.LocalMemberInd]
	var spec = *kv.Decoded.(allocator.Member).MemberValue.(*pb.BrokerSpec)
	spec.JournalLimit = 0

	resp, err := b.etcd.Txn(context.Background()).
		If(clientv3.Compare(clientv3.ModRevision(string(kv.Raw.Key)), "=", kv.Raw.ModRevision)).
		Then(clientv3.OpPut(string(kv.Raw.Key), spec.MarshalString(), clientv3.WithIgnoreLease())).
		Commit()

	c.Assert(err, gc.IsNil)
	c.Assert(resp.Succeeded, gc.Equals, true)
}

func (b *Broker) RevokeLease(c *gc.C) { c.Assert(b.session.Close(), gc.IsNil) }

func (b *Broker) WaitForExit() {
	for range b.idleCh {
	}
	// Cancel our context, which aborts our gRPC server and Etcd Watch,
	// and close our client connection.
	b.cancel()
	b.conn.Close()
}

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
