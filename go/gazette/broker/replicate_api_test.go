package broker

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/etcdtest"
)

func TestReplicateStreamAndCommit(t *testing.T) {
	var ctx, etcd = pb.WithDispatchDefault(context.Background()), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 2},
		pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"}, broker.id)
	var expectHeader = broker.header("a/journal")

	// Start stream & initial sync.
	var stream, _ = broker.client().Replicate(ctx)
	assert.NoError(t, stream.Send(&pb.ReplicateRequest{
		DeprecatedJournal: "a/journal",
		Header:            expectHeader,
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			CompressionCodec: pb.CompressionCodec_NONE,
		},
		Registers:   boxLabels(),
		Acknowledge: true,
	}))
	expectReplResponse(t, stream, &pb.ReplicateResponse{Status: pb.Status_OK, Header: expectHeader})

	// Replicate content.
	assert.NoError(t, stream.Send(&pb.ReplicateRequest{Content: []byte("foobar"), ContentDelta: 0}))
	assert.NoError(t, stream.Send(&pb.ReplicateRequest{Content: []byte("bazbing"), ContentDelta: 6}))

	// Precondition: content not observable in the Fragment index.
	assert.Equal(t, int64(0), broker.replica("a/journal").index.EndOffset())

	// Commit.
	assert.NoError(t, stream.Send(&pb.ReplicateRequest{
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            0,
			End:              13,
			Sum:              pb.SHA1SumOf("foobarbazbing"),
			CompressionCodec: pb.CompressionCodec_NONE,
		},
		Registers:   boxLabels("reg", "value"),
		Acknowledge: true,
	}))
	expectReplResponse(t, stream, &pb.ReplicateResponse{Status: pb.Status_OK})

	// Post-condition: content is now observable.
	assert.Equal(t, int64(13), broker.replica("a/journal").index.EndOffset())

	// Send EOF and expect its returned.
	assert.NoError(t, stream.CloseSend())
	var _, err = stream.Recv()
	assert.Equal(t, io.EOF, err)

	broker.cleanup()
}

func TestReplicateRequestErrorCases(t *testing.T) {
	var ctx, etcd = pb.WithDispatchDefault(context.Background()), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})

	// Case: Resolution error (Journal not found).
	var stream, _ = broker.client().Replicate(ctx)
	assert.NoError(t, stream.Send(&pb.ReplicateRequest{
		DeprecatedJournal: "does/not/exist",
		Header:            broker.header("does/not/exist"),
		Proposal: &pb.Fragment{
			Journal:          "does/not/exist",
			CompressionCodec: pb.CompressionCodec_NONE,
		},
		Registers:   boxLabels(),
		Acknowledge: true,
	}))
	expectReplResponse(t, stream, &pb.ReplicateResponse{
		Status: pb.Status_JOURNAL_NOT_FOUND,
		Header: broker.header("does/not/exist"),
	})
	// Expect broker closes.
	var _, err = stream.Recv()
	assert.Equal(t, io.EOF, err)

	// Case: request Route doesn't match the broker's own resolution.
	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 2},
		pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"}, broker.id)
	stream, _ = broker.client().Replicate(ctx)

	var wrongHeader = broker.header("a/journal")
	wrongHeader.Route = pb.Route{Primary: -1}

	assert.NoError(t, stream.Send(&pb.ReplicateRequest{
		DeprecatedJournal: "a/journal",
		Header:            wrongHeader,
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			CompressionCodec: pb.CompressionCodec_NONE,
		},
		Registers:   boxLabels(),
		Acknowledge: true,
	}))
	expectReplResponse(t, stream, &pb.ReplicateResponse{
		Status: pb.Status_WRONG_ROUTE,
		Header: broker.header("a/journal"),
	})
	// Expect broker closes.
	_, err = stream.Recv()
	assert.Equal(t, io.EOF, err)

	// Case: acknowledged proposal doesn't match.
	stream, _ = broker.client().Replicate(ctx)

	assert.NoError(t, stream.Send(&pb.ReplicateRequest{
		DeprecatedJournal: "a/journal",
		Header:            broker.header("a/journal"),
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            1234,
			End:              5678,
			CompressionCodec: pb.CompressionCodec_NONE,
		},
		Registers:   boxLabels(),
		Acknowledge: true,
	}))
	expectReplResponse(t, stream, &pb.ReplicateResponse{
		Status: pb.Status_PROPOSAL_MISMATCH,
		Header: broker.header("a/journal"),
		Fragment: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            5678, // Spool is rolled forward.
			End:              5678,
			CompressionCodec: pb.CompressionCodec_NONE,
		},
		Registers: boxLabels(),
	})
	// |stream| remains open.

	// Case: acknowledged registers don't match.
	var proposal = pb.Fragment{
		Journal:          "a/journal",
		Begin:            5678,
		End:              5678,
		CompressionCodec: pb.CompressionCodec_NONE,
	}
	assert.NoError(t, stream.Send(&pb.ReplicateRequest{
		Proposal:    &proposal,
		Registers:   boxLabels("wrong", "register"),
		Acknowledge: true,
	}))
	expectReplResponse(t, stream, &pb.ReplicateResponse{
		Status:    pb.Status_PROPOSAL_MISMATCH,
		Fragment:  &proposal,
		Registers: boxLabels(),
	})

	// Case: proposal is made without Acknowledge set, and fails to apply.
	assert.NoError(t, stream.Send(&pb.ReplicateRequest{
		Proposal:    &proposal,
		Registers:   boxLabels("still", "wrong"),
		Acknowledge: false,
	}))

	// Expect broker closes.
	_, err = stream.Recv()
	assert.Regexp(t, `.* no ack requested but status != OK: status:PROPOSAL_MISMATCH .*`, err)

	broker.cleanup()
}

func TestReplicateBlockingRestart(t *testing.T) {
	var ctx, etcd = pb.WithDispatchDefault(context.Background()), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 3},
		pb.ProcessSpec_ID{Zone: "A", Suffix: "peer"},
		pb.ProcessSpec_ID{Zone: "B", Suffix: "peer"},
		broker.id)

	// Lock the spool, such that API attempts will block indefinitely.
	var unlock = func(r *replica) func() {
		var spool = <-r.spoolCh
		return func() { r.spoolCh <- spool }
	}(broker.replica("a/journal"))

	// Case: The replica route is invalidated while the broker blocks.
	var stream, _ = broker.client().Replicate(ctx)
	assert.NoError(t, stream.Send(&pb.ReplicateRequest{
		DeprecatedJournal: "a/journal",
		Header:            broker.header("a/journal"),
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			CompressionCodec: pb.CompressionCodec_NONE,
		},
		Registers:   boxLabels(),
		Acknowledge: true,
	}))
	// Delete one of the peer assignments, invalidating the prior route.
	_, _ = etcd.Delete(ctx, allocator.AssignmentKey(broker.ks, allocator.Assignment{
		ItemID:       "a/journal",
		MemberZone:   "B",
		MemberSuffix: "peer",
		Slot:         1,
	}))
	// Replicate RPC restarts, re-resolves, and now responds with an error.
	var resp, _ = stream.Recv()
	assert.Equal(t, &pb.ReplicateResponse{
		Status: pb.Status_WRONG_ROUTE,
		Header: broker.header("a/journal"),
	}, resp)

	unlock()
	broker.cleanup()
}

func expectReplResponse(t assert.TestingT, stream pb.Journal_ReplicateClient, expect *pb.ReplicateResponse) {
	var resp, err = stream.Recv()
	assert.NoError(t, err)
	assert.Equal(t, expect, resp)
}
