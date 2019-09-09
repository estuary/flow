package broker

import (
	"context"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/clientv3"
	"go.gazette.dev/core/allocator"
	"go.gazette.dev/core/broker/fragment"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/broker/teststub"
	"go.gazette.dev/core/keyspace"
	"go.gazette.dev/core/server"
	"go.gazette.dev/core/task"
)

// testBroker runs most of a complete broker:
// * Announcing the broker's member Spec to Etcd.
// * Loading and watching a KeySpace wired into the service resolver.
// * Presenting the broker Service over a gRPC loopback server.
//
// A few bits are deliberately left out:
// * It doesn't run an allocator or obtain a lease. It simply reacts to manual
//   KeySpace changes as they are made.
// * pulseDeamon() and fragmentRefreshDaemon() loops are not started with
//   each assigned replica. Unit tests should perform (or test) these functions
//   as needed.
type testBroker struct {
	t     assert.TestingT
	id    pb.ProcessSpec_ID
	tasks *task.Group
	ks    *keyspace.KeySpace
	svc   *Service
	srv   *server.Server
}

// mockBroker pairs a teststub.Broker with that broker's announcement to Etcd.
type mockBroker struct {
	id pb.ProcessSpec_ID
	*teststub.Broker
}

// newTestBroker returns a local testBroker of |id|. |newReplicaFn| should be
// either |newReadyReplica| or |newReplica|.
func newTestBroker(t assert.TestingT, etcd *clientv3.Client, id pb.ProcessSpec_ID) *testBroker {
	var bk = &testBroker{
		t:     t,
		id:    id,
		tasks: task.NewGroup(context.Background()),
		ks:    NewKeySpace("/broker.test"),
	}

	// Initialize server.
	bk.srv = server.MustLoopback()

	var state = allocator.NewObservedState(bk.ks, allocator.MemberKey(bk.ks, bk.id.Zone, bk.id.Suffix))
	bk.svc = &Service{
		jc:               pb.NewJournalClient(bk.srv.GRPCLoopback),
		etcd:             etcd,
		resolver:         newResolver(state, newReplica),
		stopProxyReadsCh: make(chan struct{}),
	}
	bk.ks.WatchApplyDelay = 0 // Speed test execution.

	// Establish broker member key & do initial KeySpace Load.
	_ = mustKeyValues(t, etcd, map[string]string{
		state.LocalKey: (&pb.BrokerSpec{
			ProcessSpec: pb.ProcessSpec{Id: id, Endpoint: bk.srv.Endpoint()},
		}).MarshalString(),
	})
	assert.NoError(t, bk.ks.Load(bk.tasks.Context(), etcd, 0))

	// Set, but don't start a Persister for the test.
	SetSharedPersister(fragment.NewPersister(bk.ks))
	pb.RegisterJournalServer(bk.srv.GRPCServer, bk.svc)

	bk.srv.QueueTasks(bk.tasks)
	bk.svc.QueueTasks(bk.tasks, bk.srv, nil)
	bk.tasks.GoRun()
	return bk
}

// Client returns a JournalClient wrapping the GRPCLoopback.
func (bk *testBroker) client() pb.JournalClient { return bk.svc.jc }

// initialFragmentLoad signals all current replicas that a remote fragment
// refresh has completed, unblocking appends.
func (bk *testBroker) initialFragmentLoad() {
	bk.ks.Mu.RLock()
	for _, r := range bk.svc.resolver.replicas {
		r.index.ReplaceRemote(fragment.CoverSet{})
	}
	bk.ks.Mu.RUnlock()
}

// Cleanup cancels the Broker tasks.Group and asserts that it exits cleanly.
func (bk *testBroker) cleanup() {
	bk.tasks.Cancel()
	assert.NoError(bk.t, bk.tasks.Wait())
}

// resolve returns the resolution of |journal| against the testBroker.
func (bk *testBroker) resolve(journal pb.Journal) *resolution {
	var res, err = bk.svc.resolver.resolve(resolveArgs{
		ctx:      context.Background(),
		journal:  journal,
		mayProxy: true,
	})
	assert.NoError(bk.t, err)
	return res
}

// header returns the broker's current protocol.Header of the journal.
// It's a convenience for populating response expectations.
func (bk *testBroker) header(journal pb.Journal) *pb.Header { return &bk.resolve(journal).Header }

// replica returns the broker's *replica instance for the |journal|.
func (bk *testBroker) replica(journal pb.Journal) *replica { return bk.resolve(journal).replica }

// catchUpKeySpace returns only after testBroker's KeySpace has read through all
// revisions which existed when catchUpKeySpace was called.
func (bk *testBroker) catchUpKeySpace() {
	var ctx = context.Background()
	var resp, err = bk.svc.etcd.Get(ctx, "a-key-we-don't-expect-to-exist")
	assert.NoError(bk.t, err)

	bk.ks.Mu.RLock()
	assert.NoError(bk.t, bk.ks.WaitForRevision(ctx, resp.Header.Revision))
	bk.ks.Mu.RUnlock()
}

// newMockBroker returns a *teststub.Broker with an established member key.
func newMockBroker(t require.TestingT, etcd clientv3.KV, id pb.ProcessSpec_ID) mockBroker {
	var key = allocator.MemberKey(&keyspace.KeySpace{Root: "/broker.test"}, id.Zone, id.Suffix)
	var stub = teststub.NewBroker(t)

	_ = mustKeyValues(t, etcd, map[string]string{
		key: (&pb.BrokerSpec{
			ProcessSpec: pb.ProcessSpec{Id: id, Endpoint: stub.Endpoint()},
		}).MarshalString(),
	})
	return mockBroker{id: id, Broker: stub}
}

// setTestJournal creates or updates |spec| with the given assigned broker |ids|
// via a single Etcd transaction. A zero-valued ProcessSpec_ID means the
// allocation slot at that index is left empty. Any other journal assignments
// are removed.
func setTestJournal(bk *testBroker, spec pb.JournalSpec, ids ...pb.ProcessSpec_ID) {
	spec = pb.UnionJournalSpecs(spec, pb.JournalSpec{
		Replication: 0, // Must be provided by caller.
		Fragment: pb.JournalSpec_Fragment{
			Length:           1024,
			RefreshInterval:  time.Second,
			CompressionCodec: pb.CompressionCodec_SNAPPY,
		},
	})
	assert.NoError(bk.t, spec.Validate())

	// Apply the JournalSpec.
	var op = []clientv3.Op{clientv3.OpPut(
		allocator.ItemKey(bk.ks, spec.Name.String()), spec.MarshalString())}

	bk.ks.Mu.RLock()
	defer bk.ks.Mu.RUnlock()

	// Determine the set of existing journal assignment keys.
	var prev = make(map[string]struct{})
	var prefix = allocator.ItemAssignmentsPrefix(bk.ks, spec.Name.String())
	for _, asn := range bk.ks.Prefixed(prefix) {
		prev[string(asn.Raw.Key)] = struct{}{}
	}

	// Set updated broker assignments.
	for slot, id := range ids {
		if id == (pb.ProcessSpec_ID{}) {
			continue
		}
		var key = allocator.AssignmentKey(bk.ks, allocator.Assignment{
			ItemID:       spec.Name.String(),
			MemberZone:   id.Zone,
			MemberSuffix: id.Suffix,
			Slot:         slot,
		})
		// If |key| exists, leave it alone. Otherwise set it with an empty value.
		if _, ok := prev[key]; ok {
			delete(prev, key)
		} else {
			op = append(op, clientv3.OpPut(key, ""))
		}
	}
	// Remove any other previous assignments.
	for key := range prev {
		op = append(op, clientv3.OpDelete(key))
	}

	var resp, err = bk.svc.etcd.Txn(context.Background()).Then(op...).Commit()
	assert.NoError(bk.t, err)
	assert.True(bk.t, resp.Succeeded)

	assert.NoError(bk.t, bk.ks.WaitForRevision(context.Background(), resp.Header.Revision))
}

// mustKeyValues creates keys and values. The keys must not already exist.
func mustKeyValues(t assert.TestingT, etcd clientv3.KV, kvs map[string]string) int64 {
	var cmps []clientv3.Cmp
	var ops []clientv3.Op

	for k, v := range kvs {
		cmps = append(cmps, clientv3.Compare(clientv3.Version(k), "=", 0))
		ops = append(ops, clientv3.OpPut(k, v))
	}
	var resp, err = etcd.Txn(context.Background()).
		If(cmps...).Then(ops...).Commit()

	assert.NoError(t, err)
	assert.Equal(t, resp.Succeeded, true)

	return resp.Header.Revision
}
