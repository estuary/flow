// Package brokertest provides utilities for testing components requiring a live Gazette broker.
package brokertest

import (
	"context"
	"os"
	"syscall"
	"time"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/clientv3"
	"go.gazette.dev/core/allocator"
	"go.gazette.dev/core/broker"
	"go.gazette.dev/core/broker/client"
	"go.gazette.dev/core/broker/fragment"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/keyspace"
	"go.gazette.dev/core/server"
	"go.gazette.dev/core/task"
)

// Broker is a lightweight, embedded Gazette broker suitable for testing client
// functionality which depends on the availability of the Gazette service.
type Broker struct {
	ID     pb.ProcessSpec_ID
	Tasks  *task.Group
	Server *server.Server

	etcd  *clientv3.Client
	sigCh chan os.Signal
	ks    *keyspace.KeySpace
}

// NewBroker builds and returns an in-process Broker identified by |zone| and |suffix|.
func NewBroker(t assert.TestingT, etcd *clientv3.Client, zone, suffix string) *Broker {
	var (
		id        = pb.ProcessSpec_ID{Zone: zone, Suffix: suffix}
		ks        = broker.NewKeySpace("/broker.test")
		state     = allocator.NewObservedState(ks, allocator.MemberKey(ks, id.Zone, id.Suffix))
		srv       = server.MustLoopback()
		svc       = broker.NewService(state, pb.NewJournalClient(srv.GRPCLoopback), etcd)
		tasks     = task.NewGroup(context.Background())
		sigCh     = make(chan os.Signal, 1)
		allocArgs = allocator.SessionArgs{
			Etcd:     etcd,
			Tasks:    tasks,
			LeaseTTL: time.Second * 60,
			SignalCh: sigCh,
			Spec: &pb.BrokerSpec{
				ProcessSpec:  pb.ProcessSpec{Id: id, Endpoint: srv.Endpoint()},
				JournalLimit: 100,
			},
			State: state,
		}
	)

	assert.NoError(t, allocator.StartSession(allocArgs))
	pb.RegisterJournalServer(srv.GRPCServer, svc)
	// Set, but don't start a Persister for the test.
	broker.SetSharedPersister(fragment.NewPersister(ks))
	ks.WatchApplyDelay = 0 // Speed test execution.

	srv.QueueTasks(tasks)
	svc.QueueTasks(tasks, srv, nil)
	tasks.GoRun()

	return &Broker{
		ID:     id,
		Tasks:  tasks,
		Server: srv,
		etcd:   etcd,
		sigCh:  sigCh,
		ks:     ks,
	}
}

// Client returns a RoutedJournalClient wrapping the GRPCLoopback.
func (b *Broker) Client() pb.RoutedJournalClient {
	return pb.NewRoutedJournalClient(pb.NewJournalClient(b.Server.GRPCLoopback), pb.NoopDispatchRouter{})
}

// Endpoint of the test Broker.
func (b *Broker) Endpoint() pb.Endpoint { return b.Server.Endpoint() }

// Signal the Broker to exit. Wait on its |Tasks| to confirm it exited.
// Note other Broker(s) must be available to take over assignments.
func (b *Broker) Signal() { b.sigCh <- syscall.SIGTERM }

// WaitForConsistency of the named journal until the Context is cancelled.
// If |routeOut| is non-nil, it's populated with the current journal Route.
func (b *Broker) WaitForConsistency(ctx context.Context, journal pb.Journal, routeOut *pb.Route) error {
	var resp, err = b.etcd.Get(ctx, "a-key-that-doesn't-exist")
	if err != nil {
		return err
	}

	var rev = resp.Header.Revision
	var ks = b.ks

	ks.Mu.RLock()
	defer ks.Mu.RUnlock()

	for {
		if err := ks.WaitForRevision(ctx, rev); err != nil {
			return err
		}
		// Determine if the journal is consistent (ie, fully assigned with
		// advertised routes that match current assignments).
		var ind, ok = ks.Search(allocator.ItemKey(ks, journal.String()))
		if ok {
			var item = ks.KeyValues[ind].Decoded.(allocator.ItemValue)
			var asn = ks.KeyValues.Prefixed(allocator.ItemAssignmentsPrefix(ks, journal.String()))

			if len(asn) == item.DesiredReplication() && item.IsConsistent(keyspace.KeyValue{}, asn) {
				if routeOut != nil {
					routeOut.Init(asn)
				}
				return nil // Success.
			}
		}
		// Block for the next KeySpace update.
		rev = ks.Header.Revision + 1
	}
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
func CreateJournals(t assert.TestingT, bk *Broker, specs ...*pb.JournalSpec) {
	var ctx = pb.WithDispatchDefault(context.Background())

	var req = new(pb.ApplyRequest)
	for _, spec := range specs {
		req.Changes = append(req.Changes, pb.ApplyRequest_Change{Upsert: spec})
	}

	var resp, err = client.ApplyJournals(ctx, bk.Client(), req)
	assert.NoError(t, err)
	assert.Equal(t, pb.Status_OK, resp.Status)

	for _, s := range specs {
		assert.NoError(t, bk.WaitForConsistency(ctx, s.Name, nil))
	}
}

func init() { pb.RegisterGRPCDispatcher("local") }
