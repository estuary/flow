// Package consumertest provides utilities for in-process unit testing of Gazette consumer applications.
package consumertest

import (
	"context"
	"runtime"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/allocator"
	"github.com/LiveRamp/gazette/v2/pkg/client"
	"github.com/LiveRamp/gazette/v2/pkg/consumer"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/LiveRamp/gazette/v2/pkg/server"
	"github.com/coreos/etcd/clientv3"
	gc "github.com/go-check/check"
	"google.golang.org/grpc"
)

// Consumer is a lightweight, embedded Gazette consumer runtime suitable for
// in-process testing of consumer applications.
type Consumer struct {
	// Server is a loopback Server created for this Consumer, which is available
	// for test applications to register APIs against.
	Server *server.Server
	// Service of the Consumer, which is available for test applications.
	Service *consumer.Service

	spec   consumer.ConsumerSpec
	etcd   *clientv3.Client
	idleCh chan struct{}
	lease  clientv3.LeaseID
}

// NewConsumer builds and returns a Consumer.
func NewConsumer(c *gc.C, etcd *clientv3.Client, rjc pb.RoutedJournalClient,
	app consumer.Application, zone, suffix string) *Consumer {

	var srv, err = server.New("127.0.0.1", 0)
	c.Assert(err, gc.IsNil)

	// Grant lease with 1m timeout. Test must complete within this time.
	grant, err := etcd.Grant(context.Background(), 60)
	c.Assert(err, gc.IsNil)

	var ks = consumer.NewKeySpace("/consumertest")
	var key = allocator.MemberKey(ks, zone, suffix)
	var state = allocator.NewObservedState(ks, key)
	var svc = consumer.NewService(app, state, rjc, srv.MustGRPCLoopback(), etcd)

	consumer.RegisterShardServer(srv.GRPCServer, svc)

	return &Consumer{
		spec: consumer.ConsumerSpec{
			ProcessSpec: pb.ProcessSpec{
				Id:       pb.ProcessSpec_ID{Zone: zone, Suffix: suffix},
				Endpoint: srv.Endpoint(),
			},
			ShardLimit: 100,
		},
		Service: svc,
		Server:  srv,

		etcd:   etcd,
		idleCh: make(chan struct{}),
		lease:  grant.ID,
	}
}

// Serve the consumer by serving its Server loopback and invoking Allocate.
// Any APIs attached to the loopback Server should be registered before Serve
// is called. Serve returns once Allocate completes.
func (cmr *Consumer) Serve(c *gc.C, ctx context.Context) {
	var ks = cmr.Service.State.KS
	var key = allocator.MemberKey(ks, cmr.spec.Id.Zone, cmr.spec.Id.Suffix)

	var resp, err = cmr.etcd.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", 0)).
		Then(clientv3.OpPut(key, cmr.spec.MarshalString(), clientv3.WithLease(cmr.lease))).
		Commit()

	c.Assert(err, gc.IsNil)
	c.Assert(resp.Succeeded, gc.Equals, true)

	ks.WatchApplyDelay = 0 // Speedup test execution.
	c.Assert(ks.Load(ctx, cmr.etcd, resp.Header.Revision), gc.IsNil)

	go func() {
		c.Assert(ks.Watch(ctx, cmr.etcd), gc.Equals, context.Canceled)
	}()

	// Arrange to stop the server when |ctx| is cancelled.
	go func() {
		<-ctx.Done()
		cmr.Server.GracefulStop()
	}()
	go cmr.Server.MustServe()

	// We signal |idleCh| on the first idle TestHook callback which follows
	// at least one non-idle TestHook. Intuitively, we signal only if Allocate
	// did some work (eg, shuffled assignments), and has since become idle.
	// We do not signal on, eg, an assignment key/value update which does
	// not result in a re-allocation.
	var signalOnIdle bool
	defer close(cmr.idleCh) // Signal on exit.

	_ = allocator.Allocate(allocator.AllocateArgs{
		Context: ctx,
		Etcd:    cmr.etcd,
		State:   cmr.Service.State,
		TestHook: func(_ int, isIdle bool) {
			if !isIdle {
				signalOnIdle = true
				return
			} else if !signalOnIdle {
				return
			} else {
				select {
				case cmr.idleCh <- struct{}{}:
				case <-time.After(5 * time.Second):
					panic("deadlock in consumer TestHook; is your test ignoring a signal to AllocateIdleCh()?")
				}
				signalOnIdle = false
			}
		},
	})
}

// AllocateIdleCh signals when the Consumer's Allocate loop took an action, such
// as updating a shard assignment, and has since become idle. Tests must
// explicitly receive (and confirm as intended) signals sent on Allocator
// actions, or Consumer will panic.
func (cmr *Consumer) AllocateIdleCh() <-chan struct{} { return cmr.idleCh }

// ZeroShardLimit of the Consumer. The test Consumer will eventually exit,
// assuming other Consumers(s) are available to take over the assignments.
func (cmr *Consumer) ZeroShardLimit(c *gc.C) {
	var state = cmr.Service.State

	state.KS.Mu.RLock()
	var kv = state.Members[state.LocalMemberInd]
	state.KS.Mu.RUnlock()

	var spec = *kv.Decoded.(allocator.Member).MemberValue.(*consumer.ConsumerSpec)
	spec.ZeroLimit()

	resp, err := cmr.etcd.Txn(context.Background()).
		If(clientv3.Compare(clientv3.ModRevision(string(kv.Raw.Key)), "=", kv.Raw.ModRevision)).
		Then(clientv3.OpPut(string(kv.Raw.Key), spec.MarshalString(), clientv3.WithIgnoreLease())).
		Commit()

	c.Assert(err, gc.IsNil)
	c.Assert(resp.Succeeded, gc.Equals, true)
}

// RevokeLease of the Consumer, allowing its Allocate loop to immediately exit.
func (cmr *Consumer) RevokeLease(c *gc.C) {
	var _, err = cmr.etcd.Revoke(context.Background(), cmr.lease)
	c.Assert(err, gc.IsNil)
}

// WaitForExit of the Allocate loop, and complete Consumer teardown.
// WaitForExit will block indefinitely if the Consumer's Shard limit is not
// also zeroed (and another Consumer takes over), or its lease revoked.
func (cmr *Consumer) WaitForExit(c *gc.C) {
	for range cmr.AllocateIdleCh() {
		// |idleCh| is closed when Allocate completes.
	}
	// Wait for all replicas to finish tear-down.
	cmr.Service.Resolver.WaitForLocalReplicas()
	// Close our local grpc.ClientConn.
	c.Assert(cmr.Service.Loopback.Close(), gc.IsNil)
}

// CreateShards using the Consumer Apply API, and wait for them to be allocated.
func CreateShards(c *gc.C, cmr *Consumer, specs ...*consumer.ShardSpec) {
	var req = new(consumer.ApplyRequest)
	for _, spec := range specs {
		req.Changes = append(req.Changes, consumer.ApplyRequest_Change{Upsert: spec})
	}
	// Issue the Apply in a goroutine, so that we may concurrently read from
	// |idleCh|. If Apply were instead called synchronously, there's the
	// possibility of deadlock on TestHook's send to |idleCh|.
	var doneCh = make(chan struct{})
	go func() {
		var resp, err = consumer.NewShardClient(cmr.Service.Loopback).
			Apply(pb.WithDispatchDefault(context.Background()), req)
		c.Assert(err, gc.IsNil)
		c.Assert(resp.Status, gc.Equals, consumer.Status_OK)
		close(doneCh)
	}()

	// Wait for shard assignments to update, and the RPC to complete.
	<-cmr.AllocateIdleCh()
	<-doneCh
}

// WaitForShards queries for shards matching LabelSelector |sel|, determines
// the current write-heads of journals being consumed by matched shards, and
// polls shards until each has caught up to the determined write-heads of its
// consumed journals.
func WaitForShards(ctx context.Context, rjc pb.RoutedJournalClient, conn *grpc.ClientConn, sel pb.LabelSelector) error {
	var sc = consumer.NewShardClient(conn)
	ctx = pb.WithDispatchDefault(ctx)

	var shards, err = consumer.ListShards(ctx, sc, &consumer.ListRequest{Selector: sel})
	if err != nil {
		return err
	}
	// Collect the set of journals being read by shards.
	var expect = make(map[pb.Journal]int64)
	for _, shard := range shards.Shards {
		for _, src := range shard.Spec.Sources {
			expect[src.Journal] = 0
		}
	}
	// Determine the write-head of each journal.
	for journal := range expect {
		var r = client.NewReader(ctx, rjc, pb.ReadRequest{
			Journal: journal,
			Offset:  -1,
			Block:   false,
		})
		if _, err = r.Read(nil); err != client.ErrOffsetNotYetAvailable {
			return err
		}
		expect[journal] = r.Response.WriteHead
	}
	// Poll until each shard has read through its respective journal write-heads.
	for len(shards.Shards) != 0 {
		var shard = shards.Shards[0]

		var resp *consumer.StatResponse
		if resp, err = sc.Stat(ctx, &consumer.StatRequest{Shard: shard.Spec.Id}); err != nil {
			return err
		}

		var done bool
		for _, src := range shard.Spec.Sources {
			if resp.Offsets[src.Journal] >= expect[src.Journal] {
				done = true
			}
		}
		if done {
			shards.Shards = shards.Shards[1:]
		} else {
			runtime.Gosched()
		}
	}
	return nil
}
