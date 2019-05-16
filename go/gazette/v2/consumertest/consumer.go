// Package consumertest provides utilities for in-process unit testing of Gazette consumer applications.
package consumertest

import (
	"context"
	"os"
	"runtime"
	"syscall"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/allocator"
	"github.com/LiveRamp/gazette/v2/pkg/client"
	"github.com/LiveRamp/gazette/v2/pkg/consumer"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/LiveRamp/gazette/v2/pkg/server"
	"github.com/LiveRamp/gazette/v2/pkg/task"
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
	// Tasks of the Consumer.
	Tasks *task.Group

	sigCh  chan<- os.Signal
	idleCh <-chan struct{}
}

// Args of NewConsumer.
type Args struct {
	C        *gc.C
	Etcd     *clientv3.Client       // Etcd client instance.
	Journals pb.RoutedJournalClient // Broker client instance.
	App      consumer.Application   // Application of the consumer.
	Root     string                 // Consumer root in Etcd. Defaults to "/consumertest".
	Zone     string                 // Zone of the consumer. Defaults to "local".
	Suffix   string                 // ID Suffix of the consumer. Defaults to "consumer".
}

// NewConsumer builds and returns a Consumer.
func NewConsumer(args Args) *Consumer {
	if args.Root == "" {
		args.Root = "/consumertest"
	}
	if args.Zone == "" {
		args.Zone = "local"
	}
	if args.Suffix == "" {
		args.Suffix = "consumer"
	}

	var ks = consumer.NewKeySpace(args.Root)
	ks.WatchApplyDelay = 0 // Speedup test execution.
	var state = allocator.NewObservedState(ks, allocator.MemberKey(ks, args.Zone, args.Suffix))

	var srv, err = server.New("127.0.0.1", 0)
	args.C.Assert(err, gc.IsNil)
	var svc = consumer.NewService(args.App, state, args.Journals, srv.MustGRPCLoopback(), args.Etcd)
	consumer.RegisterShardServer(srv.GRPCServer, svc)

	var tasks = task.NewGroup(context.Background())
	var sigCh = make(chan os.Signal, 1)
	var idleCh = make(chan struct{})

	// We signal |idleCh| on the first idle TestHook callback which follows
	// at least one non-idle TestHook. Intuitively, we signal only if Allocate
	// did some work (eg, shuffled assignments), and has since become idle.
	// We do not signal on, eg, an assignment key/value update which does
	// not result in a re-allocation.
	var signalOnIdle bool

	var sessionArgs = allocator.SessionArgs{
		Etcd:     args.Etcd,
		LeaseTTL: time.Second * 60,
		SignalCh: sigCh,
		Spec: &consumer.ConsumerSpec{
			ProcessSpec: pb.ProcessSpec{
				Id:       pb.ProcessSpec_ID{Zone: args.Zone, Suffix: args.Suffix},
				Endpoint: srv.Endpoint(),
			},
			ShardLimit: 100,
		},
		State: state,
		Tasks: tasks,
		TestHook: func(_ int, isIdle bool) {
			if !isIdle {
				signalOnIdle = true
				return
			} else if !signalOnIdle {
				return
			} else {
				select {
				case idleCh <- struct{}{}:
					// Pass.
				case <-tasks.Context().Done():
					return
				case <-time.After(5 * time.Second):
					panic("deadlock in consumer TestHook; is your test ignoring a signal to AllocateIdleCh()?")
				}
				signalOnIdle = false
			}
		},
	}
	args.C.Assert(allocator.StartSession(sessionArgs), gc.IsNil)

	srv.QueueTasks(tasks)
	tasks.Queue("service.Watch", func() error { return svc.Watch(tasks.Context()) })
	tasks.Queue("loopback.Close", func() error {
		<-tasks.Context().Done()
		return svc.Loopback.Close()
	})

	return &Consumer{
		Service: svc,
		Server:  srv,
		Tasks:   tasks,
		sigCh:   sigCh,
		idleCh:  idleCh,
	}
}

// AllocateIdleCh signals when the Consumer's Allocate loop took an action, such
// as updating a shard assignment, and has since become idle. Tests must
// explicitly receive (and confirm as intended) signals sent on Allocator
// actions, or Consumer will panic.
func (cmr *Consumer) AllocateIdleCh() <-chan struct{} { return cmr.idleCh }

// Signal the Consumer. The test Consumer will eventually exit,
// assuming other Consumers(s) are available to take over the assignments.
func (cmr *Consumer) Signal() { cmr.sigCh <- syscall.SIGTERM }

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
