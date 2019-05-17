// Package brokertest provides utilities for testing components requiring a live Gazette broker.
package brokertest

import (
	"context"
	"os"
	"syscall"
	"time"

	gc "github.com/go-check/check"
	"go.etcd.io/etcd/v3/clientv3"
	"go.gazette.dev/core/allocator"
	"go.gazette.dev/core/broker"
	"go.gazette.dev/core/broker/teststub"
	"go.gazette.dev/core/client"
	"go.gazette.dev/core/fragment"
	pb "go.gazette.dev/core/protocol"
	"go.gazette.dev/core/task"
)

// Broker is a lightweight, embedded Gazette broker suitable for testing client
// functionality which depends on the availability of the Gazette service.
type Broker struct {
	Tasks *task.Group

	etcd   *clientv3.Client
	idleCh chan struct{}
	sigCh  chan os.Signal
	srv    teststub.LoopbackServer
	svc    broker.Service
}

// NewBroker builds and returns an in-process Broker identified by |zone| and |suffix|.
func NewBroker(c *gc.C, etcd *clientv3.Client, zone, suffix string) *Broker {
	var bk = &Broker{
		Tasks:  task.NewGroup(context.Background()),
		etcd:   etcd,
		idleCh: make(chan struct{}),
		sigCh:  make(chan os.Signal, 1),
	}

	var ks = broker.NewKeySpace("/brokertest")
	var state = allocator.NewObservedState(ks, allocator.MemberKey(ks, zone, suffix))
	ks.WatchApplyDelay = 0 // Speed test execution.

	bk.srv = teststub.NewLoopbackServer(&bk.svc)
	bk.svc = *broker.NewService(state, pb.NewJournalClient(bk.srv.Conn), etcd)

	// We signal |idleCh| on the first idle TestHook callback which follows
	// at least one non-idle TestHook. Intuitively, we signal only if Allocate
	// did some work (eg, shuffled assignments), and has since become idle.
	// We do not signal on, eg, an assignment key/value update which does
	// not result in a re-allocation.
	var signalOnIdle bool

	var args = allocator.SessionArgs{
		Etcd:     etcd,
		Tasks:    bk.Tasks,
		LeaseTTL: time.Second * 60,
		SignalCh: bk.sigCh,
		Spec: &pb.BrokerSpec{
			ProcessSpec: pb.ProcessSpec{
				Id:       pb.ProcessSpec_ID{Zone: zone, Suffix: suffix},
				Endpoint: bk.srv.Endpoint(),
			},
			JournalLimit: 100,
		},
		State: state,
		TestHook: func(_ int, isIdle bool) {
			if !isIdle {
				signalOnIdle = true
				return
			} else if !signalOnIdle {
				return
			} else {
				select {
				case bk.idleCh <- struct{}{}:
					// Pass.
				case <-bk.Tasks.Context().Done():
					return
				case <-time.After(5 * time.Second):
					panic("deadlock in broker TestHook; is your test ignoring a signal to AllocateIdleCh()?")
				}
				signalOnIdle = false
			}
		},
	}
	c.Assert(allocator.StartSession(args), gc.IsNil)

	bk.srv.QueueTasks(bk.Tasks)
	bk.Tasks.Queue("service.Watch", func() error { return bk.svc.Watch(bk.Tasks.Context()) })

	// TODO(jskelcy): Shared Persister race condition in integration tests (Issue #130)
	broker.SetSharedPersister(fragment.NewPersister(ks))

	bk.Tasks.GoRun()
	return bk
}

// Client of the test Broker.
func (b *Broker) Client() pb.JournalClient { return pb.NewJournalClient(b.srv.Conn) }

// Endpoint of the test Broker.
func (b *Broker) Endpoint() pb.Endpoint { return b.srv.Endpoint() }

// AllocateIdleCh signals when the Broker's Allocate loop took an action, such
// as updating a journal assignment, and has since become idle. Tests must
// explicitly receive (and confirm as intended) signals sent on Allocator
// actions, or Broker will panic.
func (b *Broker) AllocateIdleCh() <-chan struct{} { return b.idleCh }

// Signal the Broker. The test Broker will eventually exit,
// assuming other Broker(s) are available to take over the assignments.
func (b *Broker) Signal() { b.sigCh <- syscall.SIGTERM }

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
	// Issue the Apply in a goroutine, so that we may concurrently read from
	// |idleCh|. If Apply were instead called synchronously, there's the
	// possibility of deadlock on TestHook's send to |idleCh|.
	var doneCh = make(chan struct{})
	go func() {
		var _, err = client.ApplyJournals(pb.WithDispatchDefault(context.Background()), bk.Client(), req)
		c.Assert(err, gc.IsNil)
		close(doneCh)
	}()

	// Wait for journal assignments to update, and the RPC to complete.
	<-bk.AllocateIdleCh()
	<-doneCh
}
