package broker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.gazette.dev/core/allocator"
	"go.gazette.dev/core/broker/fragment"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/etcdtest"
)

func TestReplicaShutdown(t *testing.T) {
	var ctx, etcd = pb.WithDispatchDefault(context.Background()), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	// Construct a replicated journal fixture, and initialize its pipeline.
	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	var peer = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"})

	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 2}, broker.id, peer.id)
	broker.initialFragmentLoad()

	// Start the journal's replication pipeline.
	var fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	require.True(t, fsm.runTo(stateAwaitDesiredReplicas))
	fsm.returnPipeline()

	// Delete the broker's assignment. shutDownReplica() will be started.
	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 2}, pb.ProcessSpec_ID{}, peer.id)

	// Expect replica is canceled as shutDownReplica completes,
	// after which neither the spool nor pipeline are select-able.
	<-fsm.resolved.replica.ctx.Done()
	select {
	case <-fsm.resolved.replica.spoolCh:
		require.FailNow(t, "selected spool")
	case <-fsm.resolved.replica.pipelineCh:
		require.FailNow(t, "selected pipeline")
	default:
	}

	// TODO(johnny): verify the persisted spool as well. Requires hooks into persister.

	broker.cleanup()
	peer.cleanup()
}

func TestReplicaAssignmentUpdateCases(t *testing.T) {
	var ctx, etcd = pb.WithDispatchDefault(context.Background()), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 2},
		broker.id, pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"})

	var res, err = broker.svc.resolver.resolve(resolveArgs{ctx: ctx, journal: "a/journal"})
	require.NoError(t, err)

	// Case: assignments have been modified since resolution.
	_, err = etcd.Put(ctx, string(res.assignments[0].Raw.Key), "", clientv3.WithIgnoreLease())
	require.NoError(t, err)

	rev, err := updateAssignments(ctx, res.assignments, etcd)
	require.NoError(t, err)

	// Expect that updateAssignments left assignments unmodified.
	for _, a := range res.assignments {
		require.Equal(t, &pb.Route{Primary: -1},
			a.Decoded.(allocator.Assignment).AssignmentValue.(*pb.Route))
	}

	// Case: assignments haven't been modified since resolution.
	res, err = broker.svc.resolver.resolve(
		resolveArgs{ctx: ctx, journal: "a/journal", minEtcdRevision: rev})
	require.NoError(t, err)

	rev, err = updateAssignments(ctx, res.assignments, etcd)
	require.NoError(t, err)

	// Expect that, after resolving at the returned Etcd revision,
	// Etcd assignment routes match the expectation.
	res, err = broker.svc.resolver.resolve(
		resolveArgs{ctx: ctx, journal: "a/journal", minEtcdRevision: rev})
	require.NoError(t, err)

	for _, a := range res.assignments {
		require.Equal(t,
			&pb.Route{Members: []pb.ProcessSpec_ID{broker.id, {Zone: "peer", Suffix: "broker"}}},
			a.Decoded.(allocator.Assignment).AssignmentValue.(*pb.Route))
	}

	// Case: Perform a second assignment update. Expect assignments are not modified.
	rev, err = updateAssignments(ctx, res.assignments, etcd)
	require.NoError(t, err)
	res2, err := broker.svc.resolver.resolve(resolveArgs{ctx: ctx, journal: "a/journal"})
	require.NoError(t, err)

	// Assignments were not modified, as they were already equivalent.
	require.Equal(t, res.assignments, res2.assignments)

	broker.cleanup()
}

func TestReplicaNextProposalCases(t *testing.T) {
	defer func(f func() time.Time) { timeNow = f }(timeNow)

	var testData = []struct {
		prepArgs    func(fragment.Spool, pb.JournalSpec_Fragment) (fragment.Spool, int64, pb.JournalSpec_Fragment)
		out         pb.Fragment
		description string
	}{
		{
			prepArgs: func(spool fragment.Spool, spec pb.JournalSpec_Fragment) (fragment.Spool, int64, pb.JournalSpec_Fragment) {
				spool.Begin, spool.End = 0, 100
				spool.FirstAppendTime = time.Time{}.Add(time.Hour * 2)
				spec.Length = 200
				spec.FlushInterval = time.Duration(time.Hour * 6)
				return spool, 0, spec
			},
			out: pb.Fragment{
				Journal:          "a/journal",
				Begin:            0,
				End:              100,
				CompressionCodec: 1,
			},
			description: "Fragment does not need to be flushed",
		},
		{
			prepArgs: func(spool fragment.Spool, spec pb.JournalSpec_Fragment) (fragment.Spool, int64, pb.JournalSpec_Fragment) {
				spool.Begin, spool.End = 1, 200
				spool.FirstAppendTime = time.Time{}.Add(time.Hour * 2)
				spec.Length = 100
				return spool, 0, spec
			},
			out: pb.Fragment{
				Journal:          "a/journal",
				Begin:            200,
				End:              200,
				CompressionCodec: 1,
			},
			description: "Fragment exceeds length, get flush proposal",
		},
		{
			prepArgs: func(spool fragment.Spool, spec pb.JournalSpec_Fragment) (fragment.Spool, int64, pb.JournalSpec_Fragment) {
				spool.Begin, spool.End = 1, 50
				spool.FirstAppendTime = time.Time{}.Add(time.Minute)
				spec.Length = 100
				spec.FlushInterval = time.Duration(time.Minute * 30)
				return spool, 0, spec
			},
			out: pb.Fragment{
				Journal:          "a/journal",
				Begin:            50,
				End:              50,
				CompressionCodec: 1,
			},
			description: "Fragment contains data from previous flush interval",
		},
		{
			prepArgs: func(spool fragment.Spool, spec pb.JournalSpec_Fragment) (fragment.Spool, int64, pb.JournalSpec_Fragment) {
				spool.Begin, spool.End = 10, 20
				spec.Length = 100
				return spool, 20, spec
			},
			out: pb.Fragment{
				Journal:          "a/journal",
				Begin:            20,
				End:              20,
				CompressionCodec: 1,
			},
			description: "Fragment is has roll-to-offset",
		},
		{
			prepArgs: func(spool fragment.Spool, spec pb.JournalSpec_Fragment) (fragment.Spool, int64, pb.JournalSpec_Fragment) {
				spool.Begin, spool.End = 20, 30
				spec.Length = 100
				return spool, 20, spec
			},
			out: pb.Fragment{
				Journal:          "a/journal",
				Begin:            20,
				End:              30,
				CompressionCodec: 1,
			},
			description: "Fragment is has already been rolled-to-offset",
		},
	}

	timeNow = func() time.Time { return time.Time{}.Add(time.Hour) }
	for _, test := range testData {
		var spool, rollToOffset, spec = test.prepArgs(
			fragment.NewSpool("a/journal", &testSpoolObserver{}),
			pb.JournalSpec_Fragment{CompressionCodec: 1},
		)
		var proposal = maybeRollFragment(spool, rollToOffset, spec)
		t.Log(test.description)
		require.Equal(t, proposal, test.out)
	}
}
