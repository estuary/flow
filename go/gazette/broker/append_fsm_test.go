package broker

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/broker/fragment"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/etcdtest"
)

func TestFSMResolve(t *testing.T) {
	var ctx, etcd = context.Background(), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	var peer = newMockBroker(t, etcd, pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"})

	// Case: A resolution status error is returned.
	var fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "does/not/exist"}}
	fsm.onResolve()
	assert.Equal(t, stateError, fsm.state)
	assert.Equal(t, pb.Status_JOURNAL_NOT_FOUND, fsm.resolved.status)

	// Case: Context canceled and resolution is aborted.
	fsm = appendFSM{svc: broker.svc, ctx: newCanceledCtx(), req: pb.AppendRequest{
		Journal: "a/journal",
		Header:  &pb.Header{Etcd: fsm.resolved.Header.Etcd},
	}}
	fsm.req.Header.Etcd.Revision += 1e10 // Future revision blocks indefinitely.
	fsm.onResolve()
	assert.Equal(t, stateError, fsm.state)
	assert.EqualError(t, fsm.err, "resolve: context canceled")

	// Case: Resolution success, but we don't own the pipeline.
	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 3}, broker.id)
	fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	fsm.onResolve()
	assert.Equal(t, stateAcquirePipeline, fsm.state)

	// Case: Resolution success, and we own the pipeline.
	fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	fsm.plnReturnCh = make(chan *pipeline)
	fsm.onResolve()
	assert.Equal(t, stateStartPipeline, fsm.state)

	// Case: We're not journal primary, but own the pipeline.
	setTestJournal(broker, pb.JournalSpec{Name: "other/journal", Replication: 3}, peer.id)
	fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "other/journal"}}
	fsm.plnReturnCh = make(chan *pipeline, 1)
	fsm.onResolve()
	assert.Equal(t, peer.id, fsm.resolved.ProcessId)
	assert.Equal(t, stateAwaitDesiredReplicas, fsm.state)
	assert.Nil(t, fsm.plnReturnCh)

	broker.cleanup()
	peer.Cleanup()
}

func TestFSMAcquirePipeline(t *testing.T) {
	var ctx, etcd = context.Background(), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 1}, broker.id)

	// Case: success.
	var fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	assert.True(t, fsm.runTo(stateStartPipeline))
	assert.NotNil(t, fsm.plnReturnCh)
	fsm.returnPipeline()

	// We returned the pipeline, but expect that onAcquirePipeline() confirms
	// invalidation and/or cancellation status even if the pipeline is
	// immediately select-able.

	// Case: replica route is invalidated while we wait.
	fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	fsm.onResolve()
	fsm.resolved.invalidateCh = newClosedCh()
	fsm.onAcquirePipeline()
	assert.Equal(t, stateResolve, fsm.state)
	assert.NoError(t, fsm.err)
	fsm.returnPipeline()

	// Case: request is cancelled while we wait.
	fsm = appendFSM{svc: broker.svc, ctx: newCanceledCtx(), req: pb.AppendRequest{Journal: "a/journal"}}
	fsm.onResolve()
	fsm.onAcquirePipeline()
	assert.Equal(t, stateError, fsm.state)
	assert.EqualError(t, fsm.err, "waiting for pipeline: context canceled")
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
	var fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	assert.True(t, fsm.runTo(stateRecvPipelineSync))

	// Expect peer reads initial fragment proposal for the journal.
	assert.Equal(t, pb.ReplicateRequest{
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
	assert.Equal(t, int64(892), fsm.rollToOffset)
	assert.Equal(t, *boxLabels("reg", ""), fsm.registers)
	assert.Equal(t, int64(0), fsm.readThroughRev)
	assert.Equal(t, stateSendPipelineSync, fsm.state)
	assert.NoError(t, fsm.err)
	fsm.onSendPipelineSync()

	assert.Equal(t, pb.ReplicateRequest{
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
	assert.Equal(t, int64(0), fsm.rollToOffset)
	assert.Equal(t, wrongRouteHeader.Etcd.Revision, fsm.readThroughRev)
	assert.Equal(t, stateResolve, fsm.state)
	assert.NoError(t, fsm.err)
	assert.Nil(t, fsm.pln)                        // Expect pipeline was torn down.
	assert.Equal(t, io.EOF, <-peer.ReadLoopErrCh) // Expect EOF was sent to peer on prior pipeline.
	peer.WriteLoopErrCh <- nil                    // Peer closes.

	// Restart. This time, the peer returns an unexpected error.
	fsm.readThroughRev = 0
	assert.True(t, fsm.runTo(stateRecvPipelineSync))

	assert.NotNil(t, <-peer.ReplReqCh)
	peer.WriteLoopErrCh <- errors.New("foobar")
	assert.Equal(t, context.Canceled, <-peer.ReadLoopErrCh) // Peer reads its RPC cancellation.

	fsm.onRecvPipelineSync()
	assert.Equal(t, stateError, fsm.state)
	assert.EqualError(t, fsm.err, `gatherSync: recv from zone:"peer" suffix:"broker" : `+
		`rpc error: code = Unknown desc = foobar`)
	assert.Nil(t, fsm.pln) // Expect pipeline was torn down.
	fsm.returnPipeline()

	// Case: New pipeline from scratch, and sync is successful.
	fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	assert.True(t, fsm.runTo(stateRecvPipelineSync))

	_ = <-peer.ReplReqCh
	peer.ReplRespCh <- pb.ReplicateResponse{Status: pb.Status_OK}

	fsm.onRecvPipelineSync()
	assert.Equal(t, stateUpdateAssignments, fsm.state)
	fsm.returnPipeline()

	// Case: As the pipeline is intact and of the correct route,
	// expect it's not synchronized again.
	fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	assert.True(t, fsm.runTo(stateStartPipeline))
	fsm.onStartPipeline()
	assert.Equal(t, stateUpdateAssignments, fsm.state)
	fsm.returnPipeline()

	// Case: A synced pipeline of non-equivalent route is torn down and restarted.
	fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	fsm.onResolve()
	fsm.onAcquirePipeline()
	fsm.pln.Route = wrongRouteHeader.Route
	fsm.onStartPipeline()

	assert.Equal(t, stateSendPipelineSync, fsm.state)
	assert.Equal(t, io.EOF, <-peer.ReadLoopErrCh) // EOF sent to peer on prior pipeline.
	peer.WriteLoopErrCh <- nil                    // Peer closes.

	// We return a nil pipeline now, and arrange to return the actual one later.
	// This has the effect of making pipeline acquisition succeed, but spool
	// acquisition blocks indefinitely for the test remainder.
	var unlock = func(ch chan *pipeline, pln *pipeline) func() {
		ch <- nil
		return func() {
			assert.Nil(t, <-ch) // Pop nil pipeline fixture.
			ch <- pln           // Restore non-nil pipeline, so it may be torn down properly.
		}
	}(fsm.resolved.replica.pipelineCh, fsm.pln)

	// Case: context error while awaiting the spool.
	fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	assert.True(t, fsm.runTo(stateStartPipeline))
	fsm.ctx = newCanceledCtx()
	fsm.onStartPipeline()
	assert.Equal(t, stateError, fsm.state)
	assert.EqualError(t, fsm.err, "waiting for spool: context canceled")
	fsm.returnPipeline()

	// Case: route invalidation while awaiting the spool.
	fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	assert.True(t, fsm.runTo(stateStartPipeline))
	fsm.resolved.invalidateCh = newClosedCh()
	fsm.onStartPipeline()
	assert.Equal(t, stateResolve, fsm.state)
	assert.NoError(t, fsm.err)
	fsm.returnPipeline()

	unlock()
	peer.WriteLoopErrCh <- nil                              // Peer closes.
	assert.Equal(t, context.Canceled, <-peer.ReadLoopErrCh) // Peer reads its own RPC cancellation.

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
	var fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	assert.True(t, fsm.runTo(stateRecvPipelineSync))
	_ = <-peer.ReplReqCh
	peer.ReplRespCh <- pb.ReplicateResponse{Status: pb.Status_OK}
	fsm.onRecvPipelineSync()
	fsm.ctx = newCanceledCtx() // Cause updateAssignments to fail.
	fsm.onUpdateAssignments()
	assert.Equal(t, stateError, fsm.state)
	assert.EqualError(t, fsm.err, `updateAssignments: context canceled`)
	fsm.returnPipeline()

	// Case: assignments must be updated in Etcd.
	fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	assert.True(t, fsm.runTo(stateUpdateAssignments))
	fsm.onUpdateAssignments()
	assert.Equal(t, stateResolve, fsm.state)
	assert.NotZero(t, fsm.readThroughRev)

	// Case: assignments are already up to date.
	fsm.onResolve()
	fsm.onStartPipeline()
	fsm.onUpdateAssignments()
	assert.Equal(t, stateAwaitDesiredReplicas, fsm.state)
	fsm.returnPipeline()

	peer.WriteLoopErrCh <- nil                              // Peer closes.
	assert.Equal(t, context.Canceled, <-peer.ReadLoopErrCh) // Peer reads its own RPC cancellation.

	broker.cleanup()
	peer.Cleanup()
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
	var fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	assert.True(t, fsm.runTo(stateValidatePreconditions))
	fsm.returnPipeline()

	// Case: remote primary with correct number of replicas.
	fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "remote/journal"}}
	fsm.onResolve()
	fsm.onAwaitDesiredReplicas()
	assert.Equal(t, stateProxy, fsm.state)

	// Case: journal with too many replicas.
	fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "too/many"}}
	fsm.onResolve()
	fsm.onAwaitDesiredReplicas()
	assert.Equal(t, stateResolve, fsm.state)
	assert.Equal(t, fsm.resolved.Etcd.Revision+1, fsm.readThroughRev)

	// Case: journal with too few replicas.
	fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "too/few"}}
	fsm.onResolve()
	fsm.onAwaitDesiredReplicas()
	assert.Equal(t, stateError, fsm.state)
	assert.Equal(t, pb.Status_INSUFFICIENT_JOURNAL_BROKERS, fsm.resolved.status)

	broker.cleanup()
	peer.Cleanup()
}

func TestFSMValidatePreconditions(t *testing.T) {
	var ctx, etcd = context.Background(), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 1}, broker.id)

	// Case: We're canceled while awaiting the first remote fragment refresh.
	var fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	assert.True(t, fsm.runTo(stateValidatePreconditions))
	fsm.ctx = newCanceledCtx()
	fsm.onValidatePreconditions()
	assert.Equal(t, stateError, fsm.state)
	assert.EqualError(t, fsm.err, "WaitForFirstRemoteRefresh: context canceled")
	fsm.returnPipeline()

	// Case: Spool & fragment index agree on non-zero end offset. Success.
	fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	fsm.onResolve()
	fsm.onAcquirePipeline()
	fsm.resolved.replica.index.ReplaceRemote(fragment.CoverSet{
		{Fragment: pb.Fragment{End: 123}},
	})
	fsm.pln.spool.End = 123
	assert.True(t, fsm.runTo(stateStreamContent))
	fsm.returnPipeline()

	// Case: Register selector is not matched.
	fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{
		Journal:        "a/journal",
		CheckRegisters: &pb.LabelSelector{Include: pb.MustLabelSet("not", "matched")},
	}}
	assert.True(t, fsm.runTo(stateValidatePreconditions))
	fsm.onValidatePreconditions()
	assert.Equal(t, stateError, fsm.state)
	assert.Equal(t, pb.Status_REGISTER_MISMATCH, fsm.resolved.status)
	fsm.returnPipeline()

	// Case: Register selector _is_ matched.
	fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{
		Journal:        "a/journal",
		CheckRegisters: &pb.LabelSelector{Exclude: pb.MustLabelSet("is", "matched")},
	}}
	assert.True(t, fsm.runTo(stateStreamContent))
	fsm.returnPipeline()

	// Case: remote index contains a greater offset than the pipeline, and request omits offset.
	fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	fsm.onResolve()
	fsm.resolved.replica.index.ReplaceRemote(fragment.CoverSet{
		{Fragment: pb.Fragment{End: 456}},
	})
	assert.True(t, fsm.runTo(stateValidatePreconditions))
	fsm.onValidatePreconditions()
	assert.Equal(t, stateError, fsm.state)
	assert.Equal(t, pb.Status_INDEX_HAS_GREATER_OFFSET, fsm.resolved.status)
	fsm.returnPipeline()

	// Case: remote index contains a greater offset, request omits offset, but journal is not writable.
	fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	fsm.onResolve()
	fsm.resolved.journalSpec.Flags = pb.JournalSpec_O_RDONLY
	assert.True(t, fsm.runTo(stateStreamContent))
	fsm.returnPipeline()

	// Case: request offset doesn't match the max journal offset.
	fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal", Offset: 455}}
	assert.True(t, fsm.runTo(stateValidatePreconditions))
	fsm.onValidatePreconditions()
	assert.Equal(t, stateError, fsm.state)
	assert.Equal(t, pb.Status_WRONG_APPEND_OFFSET, fsm.resolved.status)
	fsm.returnPipeline()

	// Case: request offset does match the max journal offset.
	fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal", Offset: 456}}
	assert.True(t, fsm.runTo(stateValidatePreconditions))
	fsm.onValidatePreconditions()
	assert.Equal(t, stateSendPipelineSync, fsm.state)
	assert.Equal(t, int64(456), fsm.rollToOffset)
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
	var fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{
		Journal:           "a/journal",
		CheckRegisters:    &pb.LabelSelector{Include: pb.MustLabelSet("before", "")},
		UnionRegisters:    boxLabels("after", ""),
		SubtractRegisters: boxLabels("before", ""),
	}}
	fsm.onResolve()

	// Asynchronously run the expected peer message flow.
	go func() {
		// Peer reads initial pipeline synchronization.
		for _, p := range []mockBroker{peerA, peerB} {
			assert.Equal(t, pb.ReplicateRequest{
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
	assert.True(t, fsm.runTo(stateStreamContent))

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
	assert.True(t, fsm.clientCommit)

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
	assert.Equal(t, stateFinished, fsm.state)
	assert.NoError(t, fsm.err)
	assert.Equal(t, expect.Proposal, fsm.clientFragment)
	assert.Equal(t, *expect.Proposal, fsm.pln.spool.Fragment.Fragment)
	assert.Equal(t, *boxLabels("after", ""), fsm.registers)
	assert.Nil(t, fsm.plnReturnCh)

	// Case: A request with register modifications which writes no bytes is an error.
	fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{
		Journal:        "a/journal",
		UnionRegisters: boxLabels("foo", ""),
	}}
	fsm.runTo(stateStreamContent)

	fsm.onStreamContent(&pb.AppendRequest{}, nil) // Intent to commit.
	fsm.onStreamContent(nil, io.EOF)              // Client EOF.

	peerRecv(expect)                                     // Rollback.
	peerSend(pb.ReplicateResponse{Status: pb.Status_OK}) // Send & read ACK.
	fsm.onReadAcknowledgements()

	assert.Equal(t, stateError, fsm.state)
	assert.Equal(t, errors.Cause(fsm.err), errRegisterUpdateWithEmptyAppend)

	// Case: Expect a non-validating AppendRequest is treated as a client error,
	// and triggers rollback. Note that an updating proposal is not required and
	// is not sent this time.
	fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	fsm.runTo(stateStreamContent)

	fsm.onStreamContent(&pb.AppendRequest{Content: []byte("baz")}, nil) // Valid 1st chunk.
	fsm.onStreamContent(&pb.AppendRequest{Journal: "/invalid"}, nil)    // Invalid.

	peerRecv(pb.ReplicateRequest{Content: []byte("baz")}) // 1st chunk.
	peerRecv(expect)                                      // Rollback.
	peerSend(pb.ReplicateResponse{Status: pb.Status_OK})  // Send & read ACK.
	fsm.onReadAcknowledgements()

	assert.Equal(t, stateError, fsm.state)
	assert.EqualError(t, fsm.err, `append stream: Journal: cannot begin with '/' (/invalid)`)

	// Case: Valid but unexpected non-content chunk.
	fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	fsm.runTo(stateStreamContent)

	fsm.onStreamContent(&pb.AppendRequest{Content: []byte("baz")}, nil)   // Valid 1st chunk.
	fsm.onStreamContent(&pb.AppendRequest{Journal: "other/journal"}, nil) // Unexpected.

	peerRecv(pb.ReplicateRequest{Content: []byte("baz")}) // 1st chunk.
	peerRecv(expect)                                      // Rollback.
	peerSend(pb.ReplicateResponse{Status: pb.Status_OK})  // Send & read ACK.
	fsm.onReadAcknowledgements()

	assert.Equal(t, stateError, fsm.state)
	assert.EqualError(t, fsm.err, `append stream: expected Content chunk`)

	// Case: Expect an EOF without first sending an empty chunk is unexpected,
	// and triggers a rollback. Also change the compression spec but expect an
	// updated proposal is still not sent, as the spool is non-empty and not
	// over the Fragment Length.
	fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	fsm.runTo(stateStreamContent)

	fsm.resolved.journalSpec.Fragment.CompressionCodec = pb.CompressionCodec_GZIP

	fsm.onStreamContent(&pb.AppendRequest{Content: []byte("baz")}, nil) // Valid 1st chunk.
	assert.Equal(t, false, fsm.clientCommit)
	fsm.onStreamContent(nil, io.EOF) // Unexpected EOF.

	peerRecv(pb.ReplicateRequest{Content: []byte("baz")}) // 1st chunk.
	peerRecv(expect)                                      // Rollback.
	peerSend(pb.ReplicateResponse{Status: pb.Status_OK})  // Send & read ACK.
	fsm.onReadAcknowledgements()

	assert.Equal(t, stateError, fsm.state)
	assert.EqualError(t, fsm.err, `append stream: unexpected EOF`)

	// Case: Expect any other client read error triggers a rollback.
	fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	fsm.runTo(stateStreamContent)

	fsm.onStreamContent(&pb.AppendRequest{Content: []byte("baz")}, nil) // 1st chunk.
	fsm.onStreamContent(nil, errors.New("some error"))

	peerRecv(pb.ReplicateRequest{Content: []byte("baz")}) // 1st chunk.
	peerRecv(expect)                                      // Rollback.
	peerSend(pb.ReplicateResponse{Status: pb.Status_OK})  // Send & read ACK.
	fsm.onReadAcknowledgements()

	assert.Equal(t, stateError, fsm.state)
	assert.EqualError(t, fsm.err, `append stream: some error`)

	// Case: Expect *not* reading an EOF after a commit chunk triggers an error and rollback.
	fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	fsm.runTo(stateStreamContent)

	fsm.onStreamContent(&pb.AppendRequest{Content: []byte("baz")}, nil) // 1st chunk.
	fsm.onStreamContent(&pb.AppendRequest{}, nil)                       // Intent to commit.
	assert.Equal(t, true, fsm.clientCommit)
	fsm.onStreamContent(&pb.AppendRequest{Content: []byte("bing")}, nil) // Invalid.

	peerRecv(pb.ReplicateRequest{Content: []byte("baz")}) // 1st chunk.
	peerRecv(expect)                                      // Rollback.
	peerSend(pb.ReplicateResponse{Status: pb.Status_OK})  // Send & read ACK.
	fsm.onReadAcknowledgements()

	assert.Equal(t, stateError, fsm.state)
	assert.EqualError(t, fsm.err, `append stream: expected EOF after empty Content chunk`)

	// Case: journal writes are disallowed.
	fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	fsm.runTo(stateStreamContent)

	fsm.resolved.journalSpec.Flags = pb.JournalSpec_O_RDONLY

	fsm.onStreamContent(&pb.AppendRequest{Content: []byte("baz")}, nil) // Disallowed.

	peerRecv(expect)                                     // Rollback.
	peerSend(pb.ReplicateResponse{Status: pb.Status_OK}) // Send & read ACK.
	fsm.onReadAcknowledgements()

	assert.Equal(t, stateError, fsm.state)
	assert.NoError(t, fsm.err)
	assert.Equal(t, pb.Status_NOT_ALLOWED, fsm.resolved.status)

	// Case: journal writes are still disallowed, but append is zero-length.
	fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	fsm.runTo(stateStreamContent)

	fsm.onStreamContent(&pb.AppendRequest{}, nil) // Intent to commit.
	fsm.onStreamContent(nil, io.EOF)              // Commit.

	peerRecv(expect)                                     // Rollback.
	peerSend(pb.ReplicateResponse{Status: pb.Status_OK}) // Send & read ACK.
	fsm.onReadAcknowledgements()

	assert.Equal(t, stateFinished, fsm.state)
	assert.NoError(t, fsm.err)
	assert.Equal(t, pb.Status_OK, fsm.resolved.status)

	// Case: Writes are allowed again, but pipeline is broken.
	fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	fsm.runTo(stateStreamContent)

	fsm.resolved.journalSpec.Flags = pb.JournalSpec_O_RDWR // Reset.
	_ = fsm.pln.streams[1].CloseSend()                     // Break |peerB|.
	assert.Equal(t, io.EOF, <-peerB.ReadLoopErrCh)         // Peer reads EOF.
	peerB.WriteLoopErrCh <- nil                            // Peer closes RPC.
	_, _ = fsm.pln.streams[1].Recv()                       // We read EOF.

	// Further sends of this stream will err with "SendMsg called after CloseSend"
	// Further recvs will return EOF.

	// This errors on send to |peerB|, but not |peerA|. On the error to |peerB|,
	// expect a rollback is sent to |peerA| and |fsm| transitions to stateReadAcknowledgements.
	fsm.onStreamContent(&pb.AppendRequest{Content: []byte("baz")}, nil)

	assert.Equal(t, pb.ReplicateRequest{Content: []byte("baz")}, <-peerA.ReplReqCh)
	assert.Equal(t, expect, <-peerA.ReplReqCh)
	peerA.ReplRespCh <- pb.ReplicateResponse{Status: pb.Status_OK}

	// Expect onReadAcknowledgements() tears down to |peerA|.
	go func() {
		assert.Equal(t, io.EOF, <-peerA.ReadLoopErrCh)
		peerA.WriteLoopErrCh <- nil
	}()
	fsm.onReadAcknowledgements()

	assert.Equal(t, stateError, fsm.state)
	assert.EqualError(t, fsm.err, `recv from zone:"B" suffix:"peer" : unexpected EOF`)

	// Expect a nil pipeline was returned (it'll be re-built by the next FSM).
	assert.Nil(t, fsm.plnReturnCh)
	assert.Nil(t, <-fsm.resolved.replica.pipelineCh)
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
	var fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	fsm.run(makeRecv([]appendChunk{
		{req: &pb.AppendRequest{Content: []byte("bar")}},
		{req: &pb.AppendRequest{Content: []byte("bing")}},
		{req: &pb.AppendRequest{}},
		{err: io.EOF},
	}))

	assert.Equal(t, stateFinished, fsm.state)
	assert.Equal(t, &pb.Fragment{
		Journal:          "a/journal",
		Begin:            0,
		End:              7,
		Sum:              pb.SHA1SumOf("barbing"),
		CompressionCodec: pb.CompressionCodec_SNAPPY,
	}, fsm.clientFragment)

	// Case: client timeout triggers a context.DeadlineExceeded.
	var restoreTimeout = func(d time.Duration) func() {
		appendChunkTimeout = time.Microsecond
		return func() { appendChunkTimeout = d }
	}(appendChunkTimeout)

	fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	fsm.run(makeRecv([]appendChunk{
		{req: &pb.AppendRequest{Content: []byte("bar")}},
		// Wait indefinitely for next chunk.
	}))

	assert.Equal(t, stateError, fsm.state)
	assert.EqualError(t, fsm.err, `append stream: context deadline exceeded`)
	restoreTimeout()

	// Case: client read error.
	fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	fsm.run(makeRecv([]appendChunk{
		{req: &pb.AppendRequest{Content: []byte("bar")}},
		{err: errors.New("foobar")},
	}))

	assert.Equal(t, stateError, fsm.state)
	assert.EqualError(t, fsm.err, "append stream: foobar")

	// Case: terminal error prior to content streaming.
	fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "does/not/exist"}}
	fsm.run(nil) // |recv| not called.

	assert.Equal(t, stateError, fsm.state)
	assert.Equal(t, pb.Status_JOURNAL_NOT_FOUND, fsm.resolved.status)

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
	var fsm1 = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	var fsm2 = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	fsm1.onResolve()
	fsm2.onResolve()
	fsm1.onAcquirePipeline() // Wins race to acquire the pipeline.

	var wg sync.WaitGroup
	wg.Add(3)

	var doWrite = func(fsm *appendFSM, content string) {
		assert.True(t, fsm.runTo(stateStreamContent))
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
		assert.Equal(t, pb.ReplicateRequest{
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
		assert.Equal(t, pb.ReplicateRequest{Content: []byte("first")}, <-peer.ReplReqCh)
		assert.Equal(t, pb.ReplicateRequest{
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
		assert.Equal(t, pb.ReplicateRequest{
			Proposal: &pb.Fragment{
				Journal:          "a/journal",
				Begin:            5,
				End:              5,
				CompressionCodec: pb.CompressionCodec_SNAPPY,
			},
			Registers:   boxLabels(),
			Acknowledge: false,
		}, <-peer.ReplReqCh)

		assert.Equal(t, pb.ReplicateRequest{Content: []byte("second")}, <-peer.ReplReqCh)
		assert.Equal(t, pb.ReplicateRequest{
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
	assert.Equal(t, stateFinished, fsm1.state)
	assert.Equal(t, stateFinished, fsm2.state)
	assert.Equal(t, fsm1.clientFragment.End, fsm2.clientFragment.Begin)

	peer.WriteLoopErrCh <- nil                              // Peer closes.
	assert.Equal(t, context.Canceled, <-peer.ReadLoopErrCh) // Peer reads its own RPC cancellation.

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
	var res, _ = broker.svc.resolver.resolve(resolveArgs{ctx: ctx, journal: "a/journal"})
	res.replica.index.ReplaceRemote(fragment.CoverSet{fragment.Fragment{
		Fragment: pb.Fragment{Journal: "a/journal", Begin: 0, End: 1024}}})

	// Expect pipeline is started and synchronized exactly once.
	go func() {
		assert.Equal(t, pb.ReplicateRequest{
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
	var fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal"}}
	fsm.run(nil)
	assert.Equal(t, stateError, fsm.state)
	assert.Equal(t, pb.Status_INDEX_HAS_GREATER_OFFSET, fsm.resolved.status)

	// Part 2: We now submit a request offset which matches the remote fragment offset.
	fsm = appendFSM{svc: broker.svc, ctx: ctx, req: pb.AppendRequest{Journal: "a/journal", Offset: 1024}}
	assert.True(t, fsm.runTo(stateValidatePreconditions))
	fsm.onValidatePreconditions()
	fsm.onSendPipelineSync()

	// Expect a proposal was sent to peer which rolls the pipeline forward,
	assert.Equal(t, pb.ReplicateRequest{
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
	assert.True(t, fsm.runTo(stateStreamContent))
	fsm.onStreamContent(&pb.AppendRequest{Content: []byte("foo")}, nil)
	fsm.onStreamContent(&pb.AppendRequest{}, nil)
	fsm.onStreamContent(nil, io.EOF)

	assert.Equal(t, pb.ReplicateRequest{Content: []byte("foo")}, <-peer.ReplReqCh)
	assert.Equal(t, pb.ReplicateRequest{
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
	assert.Equal(t, stateFinished, fsm.state)

	peer.WriteLoopErrCh <- nil                              // Peer closes.
	assert.Equal(t, context.Canceled, <-peer.ReadLoopErrCh) // Peer reads its own RPC cancellation.

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
