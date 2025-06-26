package broker

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/broker/fragment"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/broker/stores"
	"go.gazette.dev/core/etcdtest"
)

func TestFSMResolve(t *testing.T) {
	var ctx, etcd = context.Background(), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	var peer = newMockBroker(t, etcd, pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"})

	// Case: A resolution status error is returned.
	var fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "does/not/exist"})
	fsm.onResolve()
	require.Equal(t, stateError, fsm.state)
	require.Equal(t, pb.Status_JOURNAL_NOT_FOUND, fsm.resolved.status)

	// Case: Context canceled and resolution is aborted.
	fsm = newFSM(broker, newCanceledCtx(), pb.AppendRequest{
		Journal: "a/journal",
		Header:  &pb.Header{Etcd: fsm.resolved.Header.Etcd},
	})
	fsm.req.Header.Etcd.Revision += 1e10 // Future revision blocks indefinitely.
	fsm.onResolve()
	require.Equal(t, stateError, fsm.state)
	require.EqualError(t, fsm.err, "resolve: context canceled")

	// Case: Resolution success, but we don't own the pipeline.
	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 3}, broker.id)
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	fsm.onResolve()
	require.Equal(t, stateAcquirePipeline, fsm.state)

	// Case: Resolution success, and we own the pipeline.
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	fsm.plnReturnCh = make(chan *pipeline)
	fsm.onResolve()
	require.Equal(t, stateStartPipeline, fsm.state)

	// Case: We're not journal primary, but own the pipeline.
	setTestJournal(broker, pb.JournalSpec{Name: "other/journal", Replication: 3}, peer.id)
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "other/journal"})
	fsm.plnReturnCh = make(chan *pipeline, 1)
	fsm.onResolve()
	require.Equal(t, peer.id, fsm.resolved.ProcessId)
	require.Equal(t, stateAwaitDesiredReplicas, fsm.state)
	require.Nil(t, fsm.plnReturnCh)

	broker.cleanup()
	peer.Cleanup()
}

func TestFSMAcquirePipeline(t *testing.T) {
	var ctx, etcd = context.Background(), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 1}, broker.id)

	// Case: success.
	var fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	require.True(t, fsm.runTo(stateStartPipeline))
	require.NotNil(t, fsm.plnReturnCh)
	fsm.returnPipeline()

	// We returned the pipeline, but expect that onAcquirePipeline() confirms
	// invalidation and/or cancellation status even if the pipeline is
	// immediately select-able.

	// Case: replica route is invalidated while we wait.
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	fsm.onResolve()
	fsm.resolved.invalidateCh = newClosedCh()
	fsm.onAcquirePipeline()
	require.Equal(t, stateResolve, fsm.state)
	require.NoError(t, fsm.err)
	fsm.returnPipeline()

	// Case: request is cancelled while we wait.
	fsm = newFSM(broker, newCanceledCtx(), pb.AppendRequest{Journal: "a/journal"})
	fsm.onResolve()
	fsm.onAcquirePipeline()
	require.Equal(t, stateError, fsm.state)
	require.EqualError(t, fsm.err, "waiting for pipeline: context canceled")
	fsm.returnPipeline()

	broker.cleanup()
}

func TestFSMStartAndSync(t *testing.T) {
	var ctx, etcd = context.Background(), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	var peer = newMockBroker(t, etcd, pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"})
	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 2}, broker.id, peer.id)

	// Case: Build a new pipeline from scratch, hitting:
	// - PROPOSAL_MISMATCH error
	// - WRONG_ROUTE error
	// - An unexpected, terminal error.
	var fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	require.True(t, fsm.runTo(stateRecvPipelineSync))

	// Expect peer reads initial fragment proposal for the journal.
	require.Equal(t, pb.ReplicateRequest{
		DeprecatedJournal: "a/journal",
		Header:            boxHeaderProcessID(fsm.resolved.Header, peer.id),
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			CompressionCodec: pb.CompressionCodec_SNAPPY,
		},
		Registers:   boxLabels(),
		Acknowledge: true,
	}, <-peer.ReplReqCh)

	// Peer responds with a fixture that disagrees on Fragment End & registers.
	peer.ReplRespCh <- pb.ReplicateResponse{
		Status:    pb.Status_PROPOSAL_MISMATCH,
		Fragment:  &pb.Fragment{Begin: 567, End: 892},
		Registers: boxLabels("reg", ""),
	}
	fsm.onRecvPipelineSync()

	// Expect we will next roll the current pipeline to the proposed offset & registers.
	require.Equal(t, int64(892), fsm.rollToOffset)
	require.Equal(t, *boxLabels("reg", ""), fsm.registers)
	require.Equal(t, int64(0), fsm.readThroughRev)
	require.Equal(t, stateSendPipelineSync, fsm.state)
	require.NoError(t, fsm.err)
	fsm.onSendPipelineSync()

	require.Equal(t, pb.ReplicateRequest{
		// Header is omitted (not the first message of stream).
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            892,
			End:              892,
			CompressionCodec: pb.CompressionCodec_SNAPPY,
		},
		Registers:   boxLabels("reg", ""),
		Acknowledge: true,
	}, <-peer.ReplReqCh)

	// Respond with a WRONG_ROUTE fixture.
	var wrongRouteHeader = fsm.pln.Header
	wrongRouteHeader.Etcd.Revision++
	wrongRouteHeader.Route = wrongRouteHeader.Route.Copy()
	wrongRouteHeader.Route.Members[1].Suffix = "other"

	peer.ReplRespCh <- pb.ReplicateResponse{
		Status: pb.Status_WRONG_ROUTE,
		Header: &wrongRouteHeader,
	}
	fsm.onRecvPipelineSync()

	// Expect we will next re-resolve at the provided future revision.
	require.Equal(t, int64(0), fsm.rollToOffset)
	require.Equal(t, wrongRouteHeader.Etcd.Revision, fsm.readThroughRev)
	require.Equal(t, stateResolve, fsm.state)
	require.NoError(t, fsm.err)
	require.Nil(t, fsm.pln)                        // Expect pipeline was torn down.
	require.Equal(t, io.EOF, <-peer.ReadLoopErrCh) // Expect EOF was sent to peer on prior pipeline.
	peer.WriteLoopErrCh <- nil                     // Peer closes.

	// Restart. This time, the peer returns an unexpected error.
	fsm.readThroughRev = 0
	require.True(t, fsm.runTo(stateRecvPipelineSync))

	require.NotNil(t, <-peer.ReplReqCh)
	peer.WriteLoopErrCh <- errors.New("foobar")
	require.Equal(t, context.Canceled, <-peer.ReadLoopErrCh) // Peer reads its RPC cancellation.

	fsm.onRecvPipelineSync()
	require.Equal(t, stateError, fsm.state)
	require.EqualError(t, fsm.err, `gatherSync: recv from zone:"peer" suffix:"broker" : `+
		`rpc error: code = Unknown desc = foobar`)
	require.Nil(t, fsm.pln) // Expect pipeline was torn down.
	fsm.returnPipeline()

	// Case: New pipeline from scratch, and sync is successful.
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	require.True(t, fsm.runTo(stateRecvPipelineSync))

	<-peer.ReplReqCh
	peer.ReplRespCh <- pb.ReplicateResponse{Status: pb.Status_OK}

	fsm.onRecvPipelineSync()
	require.Equal(t, stateUpdateAssignments, fsm.state)
	fsm.returnPipeline()

	// Case: As the pipeline is intact and of the correct route,
	// expect it's not synchronized again.
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	require.True(t, fsm.runTo(stateStartPipeline))
	fsm.onStartPipeline()
	require.Equal(t, stateUpdateAssignments, fsm.state)
	fsm.returnPipeline()

	// Case: A synced pipeline of non-equivalent route is torn down and restarted.
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	fsm.onResolve()
	fsm.onAcquirePipeline()
	fsm.pln.Route = wrongRouteHeader.Route
	fsm.onStartPipeline()

	require.Equal(t, stateSendPipelineSync, fsm.state)
	require.Equal(t, io.EOF, <-peer.ReadLoopErrCh) // EOF sent to peer on prior pipeline.
	peer.WriteLoopErrCh <- nil                     // Peer closes.

	// We return a nil pipeline now, and arrange to return the actual one later.
	// This has the effect of making pipeline acquisition succeed, but spool
	// acquisition blocks indefinitely for the test remainder.
	var unlock = func(ch chan *pipeline, pln *pipeline) func() {
		ch <- nil
		return func() {
			require.Nil(t, <-ch) // Pop nil pipeline fixture.
			ch <- pln            // Restore non-nil pipeline, so it may be torn down properly.
		}
	}(fsm.resolved.replica.pipelineCh, fsm.pln)

	// Case: context error while awaiting the spool.
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	require.True(t, fsm.runTo(stateStartPipeline))
	fsm.ctx = newCanceledCtx()
	fsm.onStartPipeline()
	require.Equal(t, stateError, fsm.state)
	require.EqualError(t, fsm.err, "waiting for spool: context canceled")
	fsm.returnPipeline()

	// Case: route invalidation while awaiting the spool.
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	require.True(t, fsm.runTo(stateStartPipeline))
	fsm.resolved.invalidateCh = newClosedCh()
	fsm.onStartPipeline()
	require.Equal(t, stateResolve, fsm.state)
	require.NoError(t, fsm.err)
	fsm.returnPipeline()

	unlock()
	peer.WriteLoopErrCh <- nil                               // Peer closes.
	require.Equal(t, context.Canceled, <-peer.ReadLoopErrCh) // Peer reads its own RPC cancellation.

	broker.cleanup()
	peer.Cleanup()
}

func TestFSMUpdateAssignments(t *testing.T) {
	var ctx, etcd = context.Background(), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	var peer = newMockBroker(t, etcd, pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"})
	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 2}, broker.id, peer.id)

	// Case: error while updating assignments.
	var fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	require.True(t, fsm.runTo(stateRecvPipelineSync))
	<-peer.ReplReqCh
	peer.ReplRespCh <- pb.ReplicateResponse{Status: pb.Status_OK}
	fsm.onRecvPipelineSync()
	fsm.ctx = newCanceledCtx() // Cause updateAssignments to fail.
	fsm.onUpdateAssignments()
	require.Equal(t, stateError, fsm.state)
	require.EqualError(t, fsm.err, `updateAssignments: context canceled`)
	fsm.returnPipeline()

	// Case: assignments must be updated in Etcd.
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	require.True(t, fsm.runTo(stateUpdateAssignments))
	fsm.onUpdateAssignments()
	require.Equal(t, stateResolve, fsm.state)
	require.NotZero(t, fsm.readThroughRev)

	// Case: assignments are already up to date.
	fsm.onResolve()
	fsm.onStartPipeline()
	fsm.onUpdateAssignments()
	require.Equal(t, stateAwaitDesiredReplicas, fsm.state)
	fsm.returnPipeline()

	peer.WriteLoopErrCh <- nil                               // Peer closes.
	require.Equal(t, context.Canceled, <-peer.ReadLoopErrCh) // Peer reads its own RPC cancellation.

	broker.cleanup()
	peer.Cleanup()
}

func TestFSMSuspendAndResume(t *testing.T) {
	var ctx, etcd = context.Background(), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 1}, broker.id)

	// Case: Journal is eligible for partial suspension.
	var fsm = newFSM(broker, ctx, pb.AppendRequest{
		Journal: "a/journal",
		Offset:  1024,
		Suspend: pb.AppendRequest_SUSPEND_NOW,
	})
	fsm.onResolve()
	fsm.resolved.replica.index.ReplaceRemote(fragment.CoverSet{
		{Fragment: pb.Fragment{Journal: "a/journal", Begin: 0, End: 1024}},
	})
	require.True(t, fsm.runTo(stateStreamContent))

	require.Equal(t, pb.Status_SUSPENDED, fsm.resolved.status)
	require.Equal(t, &pb.JournalSpec_Suspend{
		Level:  pb.JournalSpec_Suspend_PARTIAL,
		Offset: 1024,
	}, fsm.resolved.journalSpec.Suspend)

	require.NotNil(t, fsm.plnReturnCh)
	fsm.returnPipeline()

	// Case: Journal is eligible for full suspension.
	broker.initialFragmentLoad() // Clear remote fragments.
	fsm = newFSM(broker, ctx, pb.AppendRequest{
		Journal: "a/journal",
		Suspend: pb.AppendRequest_SUSPEND_IF_FLUSHED,
	})
	require.False(t, fsm.runTo(stateStreamContent))

	require.Equal(t, pb.Status_SUSPENDED, fsm.resolved.status)
	require.Equal(t, &pb.JournalSpec_Suspend{
		Level:  pb.JournalSpec_Suspend_FULL,
		Offset: 1024,
	}, fsm.resolved.journalSpec.Suspend)

	require.NotNil(t, fsm.plnReturnCh)
	fsm.returnPipeline()

	// Case: append doesn't wake the journal.
	fsm = newFSM(broker, ctx, pb.AppendRequest{
		Journal: "a/journal",
		Suspend: pb.AppendRequest_SUSPEND_NO_RESUME,
	})
	require.False(t, fsm.runTo(stateStreamContent))
	require.Equal(t, pb.Status_SUSPENDED, fsm.resolved.status)
	require.Nil(t, fsm.plnReturnCh)

	// Case: append DOES wake the journal.
	fsm = newFSM(broker, ctx, pb.AppendRequest{
		Journal: "a/journal",
		Suspend: pb.AppendRequest_SUSPEND_RESUME,
	})
	require.True(t, fsm.runTo(stateStreamContent))

	require.Equal(t, pb.Status_OK, fsm.resolved.status)
	require.Equal(t, &pb.JournalSpec_Suspend{
		Level:  pb.JournalSpec_Suspend_NONE,
		Offset: 1024,
	}, fsm.resolved.journalSpec.Suspend)
	require.Equal(t, int64(1024), fsm.pln.spool.Begin)

	require.NotNil(t, fsm.plnReturnCh)
	fsm.returnPipeline()

	broker.cleanup()
}

func TestFSMDesiredReplicas(t *testing.T) {
	var ctx, etcd = context.Background(), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	var peer = newMockBroker(t, etcd, pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"})

	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 1}, broker.id)
	setTestJournal(broker, pb.JournalSpec{Name: "remote/journal", Replication: 2}, peer.id, broker.id)
	setTestJournal(broker, pb.JournalSpec{Name: "too/many", Replication: 1}, peer.id, broker.id)
	setTestJournal(broker, pb.JournalSpec{Name: "too/few", Replication: 3}, peer.id, broker.id)

	// Case: local primary with correct number of replicas.
	var fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	require.True(t, fsm.runTo(stateValidatePreconditions))
	fsm.returnPipeline()

	// Case: remote primary with correct number of replicas.
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "remote/journal"})
	fsm.onResolve()
	fsm.onAwaitDesiredReplicas()
	require.Equal(t, stateProxy, fsm.state)

	// Case: journal with too many replicas.
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "too/many"})
	fsm.onResolve()
	fsm.onAwaitDesiredReplicas()
	require.Equal(t, stateResolve, fsm.state)
	require.Equal(t, fsm.resolved.Etcd.Revision+1, fsm.readThroughRev)

	// Case: journal with too few replicas.
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "too/few"})
	fsm.onResolve()
	fsm.onAwaitDesiredReplicas()
	require.Equal(t, stateError, fsm.state)
	require.Equal(t, pb.Status_INSUFFICIENT_JOURNAL_BROKERS, fsm.resolved.status)

	broker.cleanup()
	peer.Cleanup()
}

func TestFSMValidatePreconditions(t *testing.T) {
	var ctx, etcd = context.Background(), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 1}, broker.id)

	// Case: We're canceled while awaiting the first remote fragment refresh.
	var fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	require.True(t, fsm.runTo(stateValidatePreconditions))
	fsm.ctx = newCanceledCtx()
	fsm.onValidatePreconditions()
	require.Equal(t, stateError, fsm.state)
	require.EqualError(t, fsm.err, "waiting for the fragment store: context canceled")
	fsm.returnPipeline()

	// Case: Route is invalidated while awaiting first remote fragment refresh.
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	require.True(t, fsm.runTo(stateValidatePreconditions))
	fsm.resolved.invalidateCh = newClosedCh()
	fsm.onValidatePreconditions()
	require.Equal(t, stateResolve, fsm.state)
	fsm.returnPipeline()

	// Case: Fragment store is healthy (this is the normal path).
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	require.True(t, fsm.runTo(stateValidatePreconditions))

	var as = stores.NewActiveStore(pb.FragmentStore("mock:///test/"), nil, nil)
	as.UpdateHealth(nil) // Mark as healthy
	fsm.resolved.stores = []*stores.ActiveStore{as}
	fsm.resolved.replica.index.ReplaceRemote(fragment.CoverSet{})

	fsm.onValidatePreconditions()
	require.Equal(t, stateStreamContent, fsm.state)
	fsm.returnPipeline()

	// Case: Fragment store is unhealthy but recovers.
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	require.True(t, fsm.runTo(stateValidatePreconditions))

	as = stores.NewActiveStore(pb.FragmentStore("mock:///test/"), nil, nil)
	as.UpdateHealth(errors.New("initial unhealthy state"))
	fsm.resolved.stores = []*stores.ActiveStore{as}

	// Simulate awaited recovery.
	go func() {
		time.Sleep(10 * time.Millisecond)
		as.UpdateHealth(nil) // Recover to healthy
	}()

	fsm.onValidatePreconditions()
	require.Equal(t, stateStreamContent, fsm.state)
	fsm.returnPipeline()

	// Case: Fragment store is unhealthy and exceeds max retries.
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	require.True(t, fsm.runTo(stateValidatePreconditions))

	as = stores.NewActiveStore(pb.FragmentStore("mock:///test/"), nil,
		fmt.Errorf("too bad"))
	fsm.resolved.stores = []*stores.ActiveStore{as}

	fsm.onValidatePreconditions()
	require.Equal(t, stateError, fsm.state)
	require.Equal(t, pb.Status_FRAGMENT_STORE_UNHEALTHY, fsm.resolved.status)
	require.Equal(t, fsm.err.Error(), "fragment store mock:///test/ unhealthy: too bad")
	fsm.returnPipeline()

	// Case: Context canceled while waiting for unhealthy store.
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	require.True(t, fsm.runTo(stateValidatePreconditions))

	as = stores.NewActiveStore(pb.FragmentStore("mock:///test/"), nil, nil)
	as.UpdateHealth(errors.New("initial unhealthy state"))
	fsm.resolved.stores = []*stores.ActiveStore{as}

	fsm.ctx = newCanceledCtx()
	fsm.onValidatePreconditions()
	require.Equal(t, stateError, fsm.state)
	require.EqualError(t, fsm.err, "waiting for the fragment store: context canceled")
	fsm.returnPipeline()

	// Case: Spool & fragment index agree on non-zero end offset. Success.
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	fsm.onResolve()
	fsm.onAcquirePipeline()
	fsm.resolved.replica.index.ReplaceRemote(fragment.CoverSet{
		{Fragment: pb.Fragment{End: 123}},
	})
	fsm.pln.spool.End = 123
	require.True(t, fsm.runTo(stateStreamContent))
	fsm.returnPipeline()

	// Case: Register selector is not matched, but journal has no registers.
	fsm = newFSM(broker, ctx, pb.AppendRequest{
		Journal:        "a/journal",
		CheckRegisters: &pb.LabelSelector{Include: pb.MustLabelSet("not", "matched")},
	})
	require.True(t, fsm.runTo(stateStreamContent))

	// Set fixture for next run.
	fsm.pln.spool.Registers = pb.MustLabelSet("some", "register")
	fsm.returnPipeline()

	// Case: Register selector doesn't match non-empty journal registers.
	fsm = newFSM(broker, ctx, pb.AppendRequest{
		Journal:        "a/journal",
		CheckRegisters: &pb.LabelSelector{Include: pb.MustLabelSet("not", "matched")},
	})
	require.True(t, fsm.runTo(stateValidatePreconditions))
	fsm.onValidatePreconditions()
	require.Equal(t, stateError, fsm.state)
	require.Equal(t, pb.Status_REGISTER_MISMATCH, fsm.resolved.status)
	fsm.returnPipeline()

	// Case: Register selector _is_ matched.
	fsm = newFSM(broker, ctx, pb.AppendRequest{
		Journal:        "a/journal",
		CheckRegisters: &pb.LabelSelector{Exclude: pb.MustLabelSet("is", "matched")},
	})
	require.True(t, fsm.runTo(stateStreamContent))
	fsm.returnPipeline()

	// Case: remote index contains a greater offset than the pipeline, and request omits offset.
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	fsm.onResolve()
	fsm.resolved.replica.index.ReplaceRemote(fragment.CoverSet{
		{Fragment: pb.Fragment{End: 456}},
	})
	require.True(t, fsm.runTo(stateValidatePreconditions))
	fsm.onValidatePreconditions()
	require.Equal(t, stateError, fsm.state)
	require.Equal(t, pb.Status_INDEX_HAS_GREATER_OFFSET, fsm.resolved.status)
	fsm.returnPipeline()

	// Case: remote index contains a greater offset, request omits offset, but journal is not writable.
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	fsm.onResolve()
	fsm.resolved.journalSpec.Flags = pb.JournalSpec_O_RDONLY
	require.True(t, fsm.runTo(stateStreamContent))
	fsm.returnPipeline()

	// Case: request offset doesn't match the max journal offset.
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal", Offset: 455})
	require.True(t, fsm.runTo(stateValidatePreconditions))
	fsm.onValidatePreconditions()
	require.Equal(t, stateError, fsm.state)
	require.Equal(t, pb.Status_WRONG_APPEND_OFFSET, fsm.resolved.status)
	fsm.returnPipeline()

	// Case: request offset does match the max journal offset.
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal", Offset: 456})
	require.True(t, fsm.runTo(stateValidatePreconditions))
	fsm.onValidatePreconditions()
	require.Equal(t, stateSendPipelineSync, fsm.state)
	require.Equal(t, int64(456), fsm.rollToOffset)
	fsm.returnPipeline()

	broker.cleanup()
}

func TestFSMStreamAndReadAcknowledgements(t *testing.T) {
	var ctx, etcd = context.Background(), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	var peerA = newMockBroker(t, etcd, pb.ProcessSpec_ID{Zone: "A", Suffix: "peer"})
	var peerB = newMockBroker(t, etcd, pb.ProcessSpec_ID{Zone: "B", Suffix: "peer"})

	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 3},
		broker.id, peerA.id, peerB.id)
	broker.initialFragmentLoad()

	var peerRecv = func(req pb.ReplicateRequest) {
		for _, p := range []mockBroker{peerA, peerB} {
			require.Equal(t, req, <-p.ReplReqCh)
		}
	}
	var peerSend = func(resp pb.ReplicateResponse) {
		for _, p := range []mockBroker{peerA, peerB} {
			p.ReplRespCh <- resp
		}
	}

	// Case: successful append is streamed from the client, and verifies & updates
	// registers which are initially known only to peers (and not this appendFSM).
	var fsm = newFSM(broker, ctx, pb.AppendRequest{
		Journal:           "a/journal",
		CheckRegisters:    &pb.LabelSelector{Include: pb.MustLabelSet("before", "")},
		UnionRegisters:    boxLabels("after", ""),
		SubtractRegisters: boxLabels("before", ""),
	})
	fsm.onResolve()

	// Asynchronously run the expected peer message flow.
	go func() {
		// Peer reads initial pipeline synchronization.
		for _, p := range []mockBroker{peerA, peerB} {
			require.Equal(t, pb.ReplicateRequest{
				DeprecatedJournal: "a/journal",
				Header:            boxHeaderProcessID(fsm.resolved.Header, p.id),
				Proposal: &pb.Fragment{
					Journal:          "a/journal",
					CompressionCodec: pb.CompressionCodec_SNAPPY,
				},
				Registers:   boxLabels(),
				Acknowledge: true,
			}, <-p.ReplReqCh)
		}
		// Peers respond that they have a larger fragment & populated registers.
		peerSend(pb.ReplicateResponse{
			Status: pb.Status_PROPOSAL_MISMATCH,
			Fragment: &pb.Fragment{
				Journal:          "a/journal",
				End:              2048,
				CompressionCodec: pb.CompressionCodec_SNAPPY,
			},
			Registers: boxLabels("before", ""),
		})
		// Peers read a second sync which rolls the proposal & registers.
		peerRecv(pb.ReplicateRequest{
			Proposal: &pb.Fragment{
				Journal:          "a/journal",
				Begin:            2048,
				End:              2048,
				CompressionCodec: pb.CompressionCodec_SNAPPY,
			},
			Registers:   boxLabels("before", ""),
			Acknowledge: true,
		})
		// Peers acknowledge.
		peerSend(pb.ReplicateResponse{Status: pb.Status_OK})
	}()
	require.True(t, fsm.runTo(stateStreamContent))

	// Reach into our FragmentSpec and change the compression, in order to coerce
	// appendFSM.onStreamContent() to roll the fragment prior to sending the
	// first content chunk.
	fsm.resolved.journalSpec.Fragment.CompressionCodec = pb.CompressionCodec_GZIP
	// Then send the first stream content chunk.
	fsm.onStreamContent(&pb.AppendRequest{Content: []byte("foo")}, nil)

	// Expect an updating proposal is scattered which rolls the fragment prior
	// to the chunk being sent. Then expect the chunk itself.
	peerRecv(pb.ReplicateRequest{
		Proposal: &pb.Fragment{
			Begin:            2048,
			End:              2048,
			Journal:          "a/journal",
			CompressionCodec: pb.CompressionCodec_GZIP,
		},
		Registers:   boxLabels("before", ""),
		Acknowledge: false,
	})
	peerRecv(pb.ReplicateRequest{Content: []byte("foo"), ContentDelta: 0})

	fsm.onStreamContent(&pb.AppendRequest{Content: []byte("bar")}, nil) // Chunk two.
	peerRecv(pb.ReplicateRequest{Content: []byte("bar"), ContentDelta: 3})
	fsm.onStreamContent(&pb.AppendRequest{}, nil) // Intent to commit.
	require.True(t, fsm.clientCommit)

	// Client EOF. Expect a commit proposal is scattered to peers.
	fsm.onStreamContent(nil, io.EOF)

	var expect = pb.ReplicateRequest{
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            2048,
			End:              2054,
			Sum:              pb.SHA1SumOf("foobar"),
			CompressionCodec: pb.CompressionCodec_GZIP,
		},
		Registers:   boxLabels("after", ""), // Union/subtract applied.
		Acknowledge: true,
	}
	peerRecv(expect)

	peerSend(pb.ReplicateResponse{Status: pb.Status_OK})
	fsm.onReadAcknowledgements()

	// Expect the client fragment was calculated (and happens to be the same, since
	// this is the only write of the current spool).
	require.Equal(t, stateFinished, fsm.state)
	require.NoError(t, fsm.err)
	require.Equal(t, expect.Proposal, fsm.clientFragment)
	require.Equal(t, *expect.Proposal, fsm.pln.spool.Fragment.Fragment)
	require.Equal(t, *boxLabels("after", ""), fsm.registers)
	require.Nil(t, fsm.plnReturnCh)

	// Case: A request with register modifications which writes no bytes is an error.
	fsm = newFSM(broker, ctx, pb.AppendRequest{
		Journal:        "a/journal",
		UnionRegisters: boxLabels("foo", ""),
	})
	fsm.runTo(stateStreamContent)

	fsm.onStreamContent(&pb.AppendRequest{}, nil) // Intent to commit.
	fsm.onStreamContent(nil, io.EOF)              // Client EOF.

	// We previously completed a first write of this journal,
	// which causes the _next_ write to roll the spool forward.
	expect = pb.ReplicateRequest{
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            2054,
			End:              2054,
			CompressionCodec: pb.CompressionCodec_GZIP,
		},
		Registers:   boxLabels("after", ""), // Union/subtract applied.
		Acknowledge: false,                  // In-sync pipeline isn't acknowledged again.
	}
	peerRecv(expect)

	// Now the Append validation error causes a rollback.
	expect.Acknowledge = true
	peerRecv(expect)                                     // Rollback.
	peerSend(pb.ReplicateResponse{Status: pb.Status_OK}) // Send & read ACK.
	fsm.onReadAcknowledgements()

	require.Equal(t, stateError, fsm.state)
	require.Equal(t, errors.Cause(fsm.err), errRegisterUpdateWithEmptyAppend)

	// Case: Expect a non-validating AppendRequest is treated as a client error,
	// and triggers rollback. Note that an updating proposal is not required and
	// is not sent this time.
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	fsm.runTo(stateStreamContent)

	fsm.onStreamContent(&pb.AppendRequest{Content: []byte("baz")}, nil) // Valid 1st chunk.
	fsm.onStreamContent(&pb.AppendRequest{Journal: "/invalid"}, nil)    // Invalid.

	peerRecv(pb.ReplicateRequest{Content: []byte("baz")}) // 1st chunk.
	peerRecv(expect)                                      // Rollback.
	peerSend(pb.ReplicateResponse{Status: pb.Status_OK})  // Send & read ACK.
	fsm.onReadAcknowledgements()

	require.Equal(t, stateError, fsm.state)
	require.EqualError(t, fsm.err, `append stream: Journal: cannot begin with '/' (/invalid)`)

	// Case: Valid but unexpected non-content chunk.
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	fsm.runTo(stateStreamContent)

	fsm.onStreamContent(&pb.AppendRequest{Content: []byte("baz")}, nil)   // Valid 1st chunk.
	fsm.onStreamContent(&pb.AppendRequest{Journal: "other/journal"}, nil) // Unexpected.

	peerRecv(pb.ReplicateRequest{Content: []byte("baz")}) // 1st chunk.
	peerRecv(expect)                                      // Rollback.
	peerSend(pb.ReplicateResponse{Status: pb.Status_OK})  // Send & read ACK.
	fsm.onReadAcknowledgements()

	require.Equal(t, stateError, fsm.state)
	require.EqualError(t, fsm.err, `append stream: expected Content chunk`)

	// Case: Expect an EOF without first sending an empty chunk is unexpected,
	// and triggers a rollback. Also change the compression spec but expect an
	// updated proposal is still not sent, as the spool is non-empty and not
	// over the Fragment Length.
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	fsm.runTo(stateStreamContent)

	fsm.resolved.journalSpec.Fragment.CompressionCodec = pb.CompressionCodec_GZIP

	fsm.onStreamContent(&pb.AppendRequest{Content: []byte("baz")}, nil) // Valid 1st chunk.
	require.Equal(t, false, fsm.clientCommit)
	fsm.onStreamContent(nil, io.EOF) // Unexpected EOF.

	peerRecv(pb.ReplicateRequest{Content: []byte("baz")}) // 1st chunk.
	peerRecv(expect)                                      // Rollback.
	peerSend(pb.ReplicateResponse{Status: pb.Status_OK})  // Send & read ACK.
	fsm.onReadAcknowledgements()

	require.Equal(t, stateError, fsm.state)
	require.EqualError(t, fsm.err, `append stream: unexpected EOF`)

	// Case: Expect any other client read error triggers a rollback.
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	fsm.runTo(stateStreamContent)

	fsm.onStreamContent(&pb.AppendRequest{Content: []byte("baz")}, nil) // 1st chunk.
	fsm.onStreamContent(nil, errors.New("some error"))

	peerRecv(pb.ReplicateRequest{Content: []byte("baz")}) // 1st chunk.
	peerRecv(expect)                                      // Rollback.
	peerSend(pb.ReplicateResponse{Status: pb.Status_OK})  // Send & read ACK.
	fsm.onReadAcknowledgements()

	require.Equal(t, stateError, fsm.state)
	require.EqualError(t, fsm.err, `append stream: some error`)

	// Case: Expect *not* reading an EOF after a commit chunk triggers an error and rollback.
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	fsm.runTo(stateStreamContent)

	fsm.onStreamContent(&pb.AppendRequest{Content: []byte("baz")}, nil) // 1st chunk.
	fsm.onStreamContent(&pb.AppendRequest{}, nil)                       // Intent to commit.
	require.Equal(t, true, fsm.clientCommit)
	fsm.onStreamContent(&pb.AppendRequest{Content: []byte("bing")}, nil) // Invalid.

	peerRecv(pb.ReplicateRequest{Content: []byte("baz")}) // 1st chunk.
	peerRecv(expect)                                      // Rollback.
	peerSend(pb.ReplicateResponse{Status: pb.Status_OK})  // Send & read ACK.
	fsm.onReadAcknowledgements()

	require.Equal(t, stateError, fsm.state)
	require.EqualError(t, fsm.err, `append stream: expected EOF after empty Content chunk`)

	// Case: journal writes are disallowed.
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	fsm.runTo(stateStreamContent)

	fsm.resolved.journalSpec.Flags = pb.JournalSpec_O_RDONLY

	fsm.onStreamContent(&pb.AppendRequest{Content: []byte("baz")}, nil) // Disallowed.

	peerRecv(expect)                                     // Rollback.
	peerSend(pb.ReplicateResponse{Status: pb.Status_OK}) // Send & read ACK.
	fsm.onReadAcknowledgements()

	require.Equal(t, stateError, fsm.state)
	require.NoError(t, fsm.err)
	require.Equal(t, pb.Status_NOT_ALLOWED, fsm.resolved.status)

	// Case: journal writes are still disallowed, but append is zero-length.
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	fsm.runTo(stateStreamContent)

	fsm.onStreamContent(&pb.AppendRequest{}, nil) // Intent to commit.
	fsm.onStreamContent(nil, io.EOF)              // Commit.

	peerRecv(expect)                                     // Rollback.
	peerSend(pb.ReplicateResponse{Status: pb.Status_OK}) // Send & read ACK.
	fsm.onReadAcknowledgements()

	require.Equal(t, stateFinished, fsm.state)
	require.NoError(t, fsm.err)
	require.Equal(t, pb.Status_OK, fsm.resolved.status)

	// Case: Writes are allowed again, but pipeline is broken.
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	fsm.runTo(stateStreamContent)

	fsm.resolved.journalSpec.Flags = pb.JournalSpec_O_RDWR // Reset.
	_ = fsm.pln.streams[1].CloseSend()                     // Break |peerB|.
	require.Equal(t, io.EOF, <-peerB.ReadLoopErrCh)        // Peer reads EOF.
	peerB.WriteLoopErrCh <- nil                            // Peer closes RPC.
	_, _ = fsm.pln.streams[1].Recv()                       // We read EOF.

	// Further sends of this stream will err with "SendMsg called after CloseSend"
	// Further recvs will return EOF.

	// This errors on send to |peerB|, but not |peerA|. On the error to |peerB|,
	// expect a rollback is sent to |peerA| and |fsm| transitions to stateReadAcknowledgements.
	fsm.onStreamContent(&pb.AppendRequest{Content: []byte("baz")}, nil)

	require.Equal(t, pb.ReplicateRequest{Content: []byte("baz")}, <-peerA.ReplReqCh)
	require.Equal(t, expect, <-peerA.ReplReqCh)
	peerA.ReplRespCh <- pb.ReplicateResponse{Status: pb.Status_OK}

	// Expect onReadAcknowledgements() tears down to |peerA|.
	go func() {
		require.Equal(t, io.EOF, <-peerA.ReadLoopErrCh)
		peerA.WriteLoopErrCh <- nil
	}()
	fsm.onReadAcknowledgements()

	require.Equal(t, stateError, fsm.state)
	require.EqualError(t, fsm.err, `recv from zone:"B" suffix:"peer" : unexpected EOF`)

	// Expect a nil pipeline was returned (it'll be re-built by the next FSM).
	require.Nil(t, fsm.plnReturnCh)
	require.Nil(t, <-fsm.resolved.replica.pipelineCh)
	fsm.resolved.replica.pipelineCh <- nil

	broker.cleanup()
	peerA.Cleanup()
	peerB.Cleanup()
}

func TestFSMRunBasicCases(t *testing.T) {
	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()

	// appendFSM.run() starts a read-pump goroutine which we'll cause
	// to hang below, and which only then exits on |ctx| cancellation.
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 1}, broker.id)
	broker.initialFragmentLoad()

	var makeRecv = func(chunks []appendChunk) func() (req *pb.AppendRequest, err error) {
		return func() (req *pb.AppendRequest, err error) {
			if len(chunks) == 0 {
				<-broker.tasks.Context().Done() // Block until |broker| shutdown.
			} else {
				req, err = chunks[0].req, chunks[0].err
				chunks = chunks[1:]
			}
			return
		}
	}

	// Case: successful append is streamed from the client.
	var fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	fsm.run(makeRecv([]appendChunk{
		{req: &pb.AppendRequest{Content: []byte("bar")}},
		{req: &pb.AppendRequest{Content: []byte("bing")}},
		{req: &pb.AppendRequest{}},
		{err: io.EOF},
	}))

	require.Equal(t, stateFinished, fsm.state)
	require.Equal(t, &pb.Fragment{
		Journal:          "a/journal",
		Begin:            0,
		End:              7,
		Sum:              pb.SHA1SumOf("barbing"),
		CompressionCodec: pb.CompressionCodec_SNAPPY,
	}, fsm.clientFragment)
	require.Equal(t, int64(3), fsm.clientTotalChunks)
	require.Equal(t, int64(0), fsm.clientDelayedChunks)

	// Case: client timeout triggers a context.DeadlineExceeded.
	var uninstall = installAppendTimeoutFixture()

	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	fsm.run(makeRecv([]appendChunk{
		{req: &pb.AppendRequest{Content: []byte("bar")}},
		// Wait indefinitely for next chunk.
	}))

	require.Equal(t, stateError, fsm.state)
	require.EqualError(t, fsm.err, "append stream: "+ErrFlowControlUnderflow.Error())
	uninstall()

	// Case: client read error.
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	fsm.run(makeRecv([]appendChunk{
		{req: &pb.AppendRequest{Content: []byte("bar")}},
		{err: errors.New("foobar")},
	}))

	require.Equal(t, stateError, fsm.state)
	require.EqualError(t, fsm.err, "append stream: foobar")

	// Case: terminal error prior to content streaming.
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "does/not/exist"})
	fsm.run(nil) // |recv| not called.

	require.Equal(t, stateError, fsm.state)
	require.Equal(t, pb.Status_JOURNAL_NOT_FOUND, fsm.resolved.status)

	broker.cleanup()
}

func TestFSMPipelineRace(t *testing.T) {
	var ctx, etcd = context.Background(), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	var peer = newMockBroker(t, etcd, pb.ProcessSpec_ID{Zone: "A", Suffix: "peer"})
	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 2}, broker.id, peer.id)
	broker.initialFragmentLoad()

	// Start two raced requests.
	var fsm1 = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	var fsm2 = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	fsm1.onResolve()
	fsm2.onResolve()
	fsm1.onAcquirePipeline() // Wins race to acquire the pipeline.

	var wg sync.WaitGroup
	wg.Add(3)

	var doWrite = func(fsm *appendFSM, content string) {
		require.True(t, fsm.runTo(stateStreamContent))
		fsm.onStreamContent(&pb.AppendRequest{Content: []byte(content)}, nil)
		fsm.onStreamContent(&pb.AppendRequest{}, nil)
		fsm.onStreamContent(nil, io.EOF) // Commit.
		fsm.onReadAcknowledgements()
		wg.Done()
	}

	go doWrite(&fsm2, "second")
	go doWrite(&fsm1, "first")

	go func() {
		// Initial pipeline sync.
		require.Equal(t, pb.ReplicateRequest{
			DeprecatedJournal: "a/journal",
			Header:            boxHeaderProcessID(fsm1.resolved.Header, peer.id),
			Proposal: &pb.Fragment{
				Journal:          "a/journal",
				CompressionCodec: pb.CompressionCodec_SNAPPY,
			},
			Registers:   boxLabels(),
			Acknowledge: true,
		}, <-peer.ReplReqCh)
		peer.ReplRespCh <- pb.ReplicateResponse{Status: pb.Status_OK}

		// First append.
		require.Equal(t, pb.ReplicateRequest{Content: []byte("first")}, <-peer.ReplReqCh)
		require.Equal(t, pb.ReplicateRequest{
			Proposal: &pb.Fragment{
				Journal:          "a/journal",
				Begin:            0,
				End:              5,
				Sum:              pb.SHA1SumOf("first"),
				CompressionCodec: pb.CompressionCodec_SNAPPY,
			},
			Registers:   boxLabels(),
			Acknowledge: true,
		}, <-peer.ReplReqCh)

		// Second append. Expect the Spool is rolled due to a first write at
		// Begin: 0, but it is not acknowledged.
		require.Equal(t, pb.ReplicateRequest{
			Proposal: &pb.Fragment{
				Journal:          "a/journal",
				Begin:            5,
				End:              5,
				CompressionCodec: pb.CompressionCodec_SNAPPY,
			},
			Registers:   boxLabels(),
			Acknowledge: false,
		}, <-peer.ReplReqCh)

		require.Equal(t, pb.ReplicateRequest{Content: []byte("second")}, <-peer.ReplReqCh)
		require.Equal(t, pb.ReplicateRequest{
			Proposal: &pb.Fragment{
				Journal:          "a/journal",
				Begin:            5,
				End:              11,
				Sum:              pb.SHA1SumOf("second"),
				CompressionCodec: pb.CompressionCodec_SNAPPY,
			},
			Registers:   boxLabels(),
			Acknowledge: true,
		}, <-peer.ReplReqCh)

		peer.ReplRespCh <- pb.ReplicateResponse{Status: pb.Status_OK} // Ack first append.
		peer.ReplRespCh <- pb.ReplicateResponse{Status: pb.Status_OK} // Ack second.

		wg.Done()
	}()

	wg.Wait()

	// Expect both FSMs completed and |fsm1| ordered before |fsm2|.
	require.Equal(t, stateFinished, fsm1.state)
	require.Equal(t, stateFinished, fsm2.state)
	require.Equal(t, fsm1.clientFragment.End, fsm2.clientFragment.Begin)

	peer.WriteLoopErrCh <- nil                               // Peer closes.
	require.Equal(t, context.Canceled, <-peer.ReadLoopErrCh) // Peer reads its own RPC cancellation.

	broker.cleanup()
	peer.Cleanup()
}

func TestFSMOffsetResetFlow(t *testing.T) {
	var ctx, etcd = context.Background(), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	var peer = newMockBroker(t, etcd, pb.ProcessSpec_ID{Zone: "A", Suffix: "peer"})
	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 2}, broker.id, peer.id)
	broker.initialFragmentLoad()

	// Add a remote Fragment fixture which ends at offset 1024.
	var res, _ = broker.svc.resolver.resolve(ctx, allClaims, "a/journal", resolveOpts{})
	res.replica.index.ReplaceRemote(fragment.CoverSet{fragment.Fragment{
		Fragment: pb.Fragment{Journal: "a/journal", Begin: 0, End: 1024}}})

	// Expect pipeline is started and synchronized exactly once.
	go func() {
		require.Equal(t, pb.ReplicateRequest{
			DeprecatedJournal: "a/journal",
			Header:            boxHeaderProcessID(res.Header, peer.id),
			Proposal: &pb.Fragment{
				Journal:          "a/journal",
				CompressionCodec: pb.CompressionCodec_SNAPPY,
			},
			Registers:   boxLabels(),
			Acknowledge: true,
		}, <-peer.ReplReqCh)
		peer.ReplRespCh <- pb.ReplicateResponse{Status: pb.Status_OK}
	}()

	// Part 1: Offset is not provided, and remote index has a fragment with larger offset.
	var fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal"})
	fsm.run(nil)
	require.Equal(t, stateError, fsm.state)
	require.Equal(t, pb.Status_INDEX_HAS_GREATER_OFFSET, fsm.resolved.status)

	// Part 2: We now submit a request offset which matches the remote fragment offset.
	fsm = newFSM(broker, ctx, pb.AppendRequest{Journal: "a/journal", Offset: 1024})
	require.True(t, fsm.runTo(stateValidatePreconditions))
	fsm.onValidatePreconditions()
	fsm.onSendPipelineSync()

	// Expect a proposal was sent to peer which rolls the pipeline forward,
	require.Equal(t, pb.ReplicateRequest{
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            1024,
			End:              1024,
			CompressionCodec: pb.CompressionCodec_SNAPPY,
		},
		Registers:   boxLabels(),
		Acknowledge: true,
	}, <-peer.ReplReqCh)
	peer.ReplRespCh <- pb.ReplicateResponse{Status: pb.Status_OK}

	// FSM proceeds as per usual from here.
	require.True(t, fsm.runTo(stateStreamContent))
	fsm.onStreamContent(&pb.AppendRequest{Content: []byte("foo")}, nil)
	fsm.onStreamContent(&pb.AppendRequest{}, nil)
	fsm.onStreamContent(nil, io.EOF)

	require.Equal(t, pb.ReplicateRequest{Content: []byte("foo")}, <-peer.ReplReqCh)
	require.Equal(t, pb.ReplicateRequest{
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            1024,
			End:              1027,
			Sum:              pb.SHA1SumOf("foo"),
			CompressionCodec: pb.CompressionCodec_SNAPPY,
		},
		Registers:   boxLabels(),
		Acknowledge: true,
	}, <-peer.ReplReqCh)
	peer.ReplRespCh <- pb.ReplicateResponse{Status: pb.Status_OK}

	fsm.onReadAcknowledgements()
	require.Equal(t, stateFinished, fsm.state)

	peer.WriteLoopErrCh <- nil                               // Peer closes.
	require.Equal(t, context.Canceled, <-peer.ReadLoopErrCh) // Peer reads its own RPC cancellation.

	broker.cleanup()
	peer.Cleanup()
}

func newClosedCh() <-chan struct{} {
	var ch = make(chan struct{})
	close(ch)
	return ch
}

func newCanceledCtx() context.Context {
	var ctx, cancel = context.WithCancel(context.Background())
	cancel()
	return ctx
}

func newFSM(b *testBroker, ctx context.Context, req pb.AppendRequest) appendFSM {
	return appendFSM{svc: b.svc, ctx: ctx, claims: allClaims, req: req}
}
