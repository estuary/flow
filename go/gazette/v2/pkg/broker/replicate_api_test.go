package broker

import (
	"io"

	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	gc "github.com/go-check/check"
)

type ReplicateSuite struct{}

func (s *ReplicateSuite) TestStreamAndCommit(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	var broker = newTestBroker(c, tf, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"}, newReplica)
	var peer = newMockBroker(c, tf, pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"})

	newTestJournal(c, tf, pb.JournalSpec{Name: "a/journal", Replication: 2}, peer.id, broker.id)
	var res, _ = broker.resolve(resolveArgs{ctx: tf.ctx, journal: "a/journal"})
	var stream, _ = broker.MustClient().Replicate(pb.WithDispatchDefault(tf.ctx))

	// Initial sync.
	c.Check(stream.Send(&pb.ReplicateRequest{
		Journal: "a/journal",
		Header:  &res.Header,
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			CompressionCodec: pb.CompressionCodec_NONE,
		},
		Acknowledge: true,
	}), gc.IsNil)
	expectReplResponse(c, stream, &pb.ReplicateResponse{Status: pb.Status_OK})

	// Replicate content.
	c.Check(stream.Send(&pb.ReplicateRequest{Content: []byte("foobar"), ContentDelta: 0}), gc.IsNil)
	c.Check(stream.Send(&pb.ReplicateRequest{Content: []byte("bazbing"), ContentDelta: 6}), gc.IsNil)

	// Precondition: content not observable in the Fragment index.
	c.Check(res.replica.index.EndOffset(), gc.Equals, int64(0))

	// Commit.
	c.Check(stream.Send(&pb.ReplicateRequest{
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            0,
			End:              13,
			Sum:              pb.SHA1SumOf("foobarbazbing"),
			CompressionCodec: pb.CompressionCodec_NONE,
		},
		Acknowledge: true,
	}), gc.IsNil)
	expectReplResponse(c, stream, &pb.ReplicateResponse{Status: pb.Status_OK})

	// Post-condition: content is now observable.
	c.Check(res.replica.index.EndOffset(), gc.Equals, int64(13))

	// Send EOF and expect one.
	c.Check(stream.CloseSend(), gc.IsNil)
	var _, err = stream.Recv()
	c.Check(err, gc.Equals, io.EOF)
}

func (s *ReplicateSuite) TestErrorCases(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	var broker = newTestBroker(c, tf, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"}, newReplica)
	var peer = newMockBroker(c, tf, pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"})

	var ctx = pb.WithDispatchDefault(tf.ctx)

	// Case: Resolution error (Journal not found).
	var stream, _ = broker.MustClient().Replicate(ctx)
	var res, _ = broker.resolve(resolveArgs{ctx: ctx, journal: "does/not/exist"})

	c.Check(stream.Send(&pb.ReplicateRequest{
		Journal: "does/not/exist",
		Header:  &res.Header,
		Proposal: &pb.Fragment{
			Journal:          "does/not/exist",
			CompressionCodec: pb.CompressionCodec_NONE,
		},
		Acknowledge: true,
	}), gc.IsNil)

	expectReplResponse(c, stream, &pb.ReplicateResponse{
		Status: pb.Status_JOURNAL_NOT_FOUND,
		Header: &res.Header,
	})

	// Expect broker closes.
	var _, err = stream.Recv()
	c.Check(err, gc.Equals, io.EOF)

	// Case: request Route doesn't match the broker's own resolution.
	newTestJournal(c, tf, pb.JournalSpec{Name: "a/journal", Replication: 2}, peer.id, broker.id)
	stream, _ = broker.MustClient().Replicate(ctx)
	res, _ = broker.resolve(resolveArgs{ctx: ctx, journal: "a/journal"})

	var hdr = res.Header
	hdr.Route = pb.Route{Primary: -1}
	hdr.Etcd.Revision -= 1

	c.Check(stream.Send(&pb.ReplicateRequest{
		Journal: "a/journal",
		Header:  &hdr,
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			CompressionCodec: pb.CompressionCodec_NONE,
		},
		Acknowledge: true,
	}), gc.IsNil)

	expectReplResponse(c, stream, &pb.ReplicateResponse{
		Status: pb.Status_WRONG_ROUTE,
		Header: &res.Header,
	})

	// Expect broker closes.
	_, err = stream.Recv()
	c.Check(err, gc.Equals, io.EOF)

	// Case: acknowledged proposal doesn't match.
	stream, _ = broker.MustClient().Replicate(ctx)

	c.Check(stream.Send(&pb.ReplicateRequest{
		Journal: "a/journal",
		Header:  &res.Header,
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            1234,
			End:              5678,
			CompressionCodec: pb.CompressionCodec_NONE,
		},
		Acknowledge: true,
	}), gc.IsNil)

	expectReplResponse(c, stream, &pb.ReplicateResponse{
		Status: pb.Status_FRAGMENT_MISMATCH,
		Fragment: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            5678, // Spool is rolled forward.
			End:              5678,
			CompressionCodec: pb.CompressionCodec_NONE,
		},
	})

	// |stream| remains open.

	// Case: proposal is made without Acknowledge set, and fails to apply.
	c.Check(stream.Send(&pb.ReplicateRequest{
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            1234,
			End:              5678,
			CompressionCodec: pb.CompressionCodec_NONE,
		},
		Acknowledge: false,
	}), gc.IsNil)

	// Expect broker closes.
	_, err = stream.Recv()
	c.Check(err, gc.ErrorMatches, `.* no ack requested but status != OK: status:FRAGMENT_MISMATCH .*`)
}

func expectReplResponse(c *gc.C, stream pb.Journal_ReplicateClient, expect *pb.ReplicateResponse) {
	var resp, err = stream.Recv()
	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, expect)
}

var _ = gc.Suite(&ReplicateSuite{})
