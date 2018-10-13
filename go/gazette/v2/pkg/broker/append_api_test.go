package broker

import (
	"context"
	"errors"
	"io"

	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	gc "github.com/go-check/check"
)

type AppendSuite struct{}

func (s *AppendSuite) TestSingleAppend(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	var broker = newTestBroker(c, tf, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"}, newReadyReplica)
	var peer = newMockBroker(c, tf, pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"})

	newTestJournal(c, tf, pb.JournalSpec{Name: "a/journal", Replication: 2}, broker.id, peer.id)
	var res, _ = broker.resolve(resolveArgs{ctx: tf.ctx, journal: "a/journal"})

	var stream, _ = broker.MustClient().Append(pb.WithDispatchDefault(tf.ctx))
	c.Check(stream.Send(&pb.AppendRequest{Journal: "a/journal"}), gc.IsNil)
	expectPipelineSync(c, peer, res.Header)

	c.Check(stream.Send(&pb.AppendRequest{Content: []byte("foo")}), gc.IsNil)
	c.Check(<-peer.ReplReqCh, gc.DeepEquals, &pb.ReplicateRequest{Content: []byte("foo"), ContentDelta: 0})
	c.Check(stream.Send(&pb.AppendRequest{Content: []byte("bar")}), gc.IsNil)
	c.Check(<-peer.ReplReqCh, gc.DeepEquals, &pb.ReplicateRequest{Content: []byte("bar"), ContentDelta: 3})

	// Send empty chunk and close the Append RPC. Expect replication peer receives a commit request.
	c.Check(stream.Send(&pb.AppendRequest{}), gc.IsNil)
	c.Check(stream.CloseSend(), gc.IsNil)

	c.Check(<-peer.ReplReqCh, gc.DeepEquals, &pb.ReplicateRequest{
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            0,
			End:              6,
			Sum:              pb.SHA1SumOf("foobar"),
			CompressionCodec: pb.CompressionCodec_SNAPPY,
		},
		Acknowledge: true,
	})
	peer.ReplRespCh <- &pb.ReplicateResponse{Status: pb.Status_OK} // Acknowledge.

	// Expect the client stream receives an AppendResponse.
	resp, err := stream.CloseAndRecv()
	c.Check(err, gc.IsNil)

	c.Check(resp, gc.DeepEquals, &pb.AppendResponse{
		Status: pb.Status_OK,
		Header: &res.Header,
		Commit: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            0,
			End:              6,
			Sum:              pb.SHA1SumOf("foobar"),
			CompressionCodec: pb.CompressionCodec_SNAPPY,
		},
	})
}

func (s *AppendSuite) TestPipelinedAppends(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	var broker = newTestBroker(c, tf, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"}, newReadyReplica)
	var peer = newMockBroker(c, tf, pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"})

	newTestJournal(c, tf, pb.JournalSpec{Name: "a/journal", Replication: 2}, broker.id, peer.id)
	var res, _ = broker.resolve(resolveArgs{ctx: tf.ctx, journal: "a/journal"})

	// Build two raced Append requests.
	var ctx = pb.WithDispatchDefault(tf.ctx)
	var stream1, _ = broker.MustClient().Append(ctx)
	var stream2, _ = broker.MustClient().Append(ctx)

	// |stream1| is sequenced first; expect it's replicated to the peer.
	c.Check(stream1.Send(&pb.AppendRequest{Journal: "a/journal"}), gc.IsNil)
	expectPipelineSync(c, peer, res.Header)
	c.Check(stream1.Send(&pb.AppendRequest{Content: []byte("foo")}), gc.IsNil)
	c.Check(<-peer.ReplReqCh, gc.DeepEquals, &pb.ReplicateRequest{Content: []byte("foo"), ContentDelta: 0})
	c.Check(stream1.Send(&pb.AppendRequest{}), gc.IsNil) // Signal commit.
	c.Check(stream1.CloseSend(), gc.IsNil)

	c.Check(<-peer.ReplReqCh, gc.DeepEquals, &pb.ReplicateRequest{
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            0,
			End:              3,
			Sum:              pb.SHA1SumOf("foo"),
			CompressionCodec: pb.CompressionCodec_SNAPPY,
		},
		Acknowledge: true,
	})

	// |stream2| follows. Expect it's also replicated, without first performing a pipeline sync.
	c.Check(stream2.Send(&pb.AppendRequest{Journal: "a/journal"}), gc.IsNil)
	c.Check(stream2.Send(&pb.AppendRequest{Content: []byte("bar")}), gc.IsNil)
	c.Check(<-peer.ReplReqCh, gc.DeepEquals, &pb.ReplicateRequest{Content: []byte("bar"), ContentDelta: 0})
	c.Check(stream2.Send(&pb.AppendRequest{Content: []byte("baz")}), gc.IsNil)
	c.Check(<-peer.ReplReqCh, gc.DeepEquals, &pb.ReplicateRequest{Content: []byte("baz"), ContentDelta: 3})
	c.Check(stream2.Send(&pb.AppendRequest{}), gc.IsNil) // Signal commit.
	c.Check(stream2.CloseSend(), gc.IsNil)

	c.Check(<-peer.ReplReqCh, gc.DeepEquals, &pb.ReplicateRequest{
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            0,
			End:              9,
			Sum:              pb.SHA1SumOf("foobarbaz"),
			CompressionCodec: pb.CompressionCodec_SNAPPY,
		},
		Acknowledge: true,
	})

	// Peer finally acknowledges first commit. This unblocks |stream1|'s response.
	peer.ReplRespCh <- &pb.ReplicateResponse{Status: pb.Status_OK}
	resp, err := stream1.CloseAndRecv()
	c.Check(err, gc.IsNil)

	c.Check(resp, gc.DeepEquals, &pb.AppendResponse{
		Status: pb.Status_OK,
		Header: &res.Header,
		Commit: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            0,
			End:              3,
			Sum:              pb.SHA1SumOf("foo"),
			CompressionCodec: pb.CompressionCodec_SNAPPY,
		},
	})

	// Peer acknowledges second commit. |stream2|'s response is unblocked.
	peer.ReplRespCh <- &pb.ReplicateResponse{Status: pb.Status_OK}
	resp, err = stream2.CloseAndRecv()
	c.Check(err, gc.IsNil)

	c.Check(resp, gc.DeepEquals, &pb.AppendResponse{
		Status: pb.Status_OK,
		Header: &res.Header,
		Commit: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            3,
			End:              9,
			Sum:              pb.SHA1SumOf("barbaz"),
			CompressionCodec: pb.CompressionCodec_SNAPPY,
		},
	})
}

func (s *AppendSuite) TestRollbackCases(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	var broker = newTestBroker(c, tf, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"}, newReadyReplica)
	var peer = newMockBroker(c, tf, pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"})

	newTestJournal(c, tf, pb.JournalSpec{Name: "a/journal", Replication: 2}, broker.id, peer.id)
	var res, _ = broker.resolve(resolveArgs{ctx: tf.ctx, journal: "a/journal"})

	var ctx = pb.WithDispatchDefault(tf.ctx)

	// Case: client explicitly rolls back by closing the stream without first sending an empty chunk.
	var stream, _ = broker.MustClient().Append(ctx)
	c.Check(stream.Send(&pb.AppendRequest{Journal: "a/journal"}), gc.IsNil)
	expectPipelineSync(c, peer, res.Header)

	c.Check(stream.Send(&pb.AppendRequest{Content: []byte("foo")}), gc.IsNil)
	c.Check(<-peer.ReplReqCh, gc.DeepEquals, &pb.ReplicateRequest{Content: []byte("foo"), ContentDelta: 0})
	c.Check(stream.CloseSend(), gc.IsNil)

	// Expect replication peer receives a rollback.
	c.Check(<-peer.ReplReqCh, gc.DeepEquals, &pb.ReplicateRequest{
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			CompressionCodec: pb.CompressionCodec_SNAPPY,
		},
		Acknowledge: true,
	})
	peer.ReplRespCh <- &pb.ReplicateResponse{Status: pb.Status_OK} // Acknowledge.

	// Expect the client reads an error.
	var _, err = stream.CloseAndRecv()
	c.Check(err, gc.ErrorMatches, `rpc error: code = Unknown desc = unexpected EOF`)

	// Case: client read error occurs.
	var failCtx, failCtxCancel = context.WithCancel(ctx)

	stream, _ = broker.MustClient().Append(failCtx)
	c.Check(stream.Send(&pb.AppendRequest{Journal: "a/journal"}), gc.IsNil)

	c.Check(stream.Send(&pb.AppendRequest{Content: []byte("foo")}), gc.IsNil)
	c.Check(<-peer.ReplReqCh, gc.DeepEquals, &pb.ReplicateRequest{Content: []byte("foo"), ContentDelta: 0})
	failCtxCancel() // Cancel the stream.

	// Expect replication peer receives a rollback.
	c.Check(<-peer.ReplReqCh, gc.DeepEquals, &pb.ReplicateRequest{
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			CompressionCodec: pb.CompressionCodec_SNAPPY,
		},
		Acknowledge: true,
	})
	peer.ReplRespCh <- &pb.ReplicateResponse{Status: pb.Status_OK} // Acknowledge.

	_, err = stream.CloseAndRecv()
	c.Check(err, gc.ErrorMatches, `rpc error: code = Canceled desc = context canceled`)
}

func (s *AppendSuite) TestRequestErrorCases(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	var broker = newTestBroker(c, tf, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"}, newReplica)
	var peer = newMockBroker(c, tf, pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"})

	var ctx = pb.WithDispatchDefault(tf.ctx)

	// Case: AppendRequest which fails to validate.
	var stream, _ = broker.MustClient().Append(ctx)
	c.Check(stream.Send(&pb.AppendRequest{Journal: "/invalid/name"}), gc.IsNil)

	var _, err = stream.CloseAndRecv()
	c.Check(err, gc.ErrorMatches, `rpc error: code = Unknown desc = Journal: cannot begin with '/' .*`)

	// Case: Journal doesn't exist.
	stream, _ = broker.MustClient().Append(ctx)
	c.Check(stream.Send(&pb.AppendRequest{Journal: "does/not/exist"}), gc.IsNil)
	var res, _ = broker.resolve(resolveArgs{ctx: ctx, journal: "does/not/exist"})

	resp, err := stream.CloseAndRecv()
	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, &pb.AppendResponse{Status: pb.Status_JOURNAL_NOT_FOUND, Header: &res.Header})
	c.Check(resp.Header.Route, gc.DeepEquals, res.Header.Route)

	// Case: Journal with no assigned primary.
	// Arrange Journal assignment fixture such that R=1, but the broker has Slot=1 (and is not primary).
	newTestJournal(c, tf, pb.JournalSpec{Name: "no/primary", Replication: 1}, pb.ProcessSpec_ID{}, broker.id)
	res, _ = broker.resolve(resolveArgs{ctx: ctx, journal: "no/primary"})

	stream, _ = broker.MustClient().Append(ctx)
	c.Check(stream.Send(&pb.AppendRequest{Journal: "no/primary"}), gc.IsNil)

	resp, err = stream.CloseAndRecv()
	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, &pb.AppendResponse{Status: pb.Status_NO_JOURNAL_PRIMARY_BROKER, Header: &res.Header})

	// Case: Journal with a primary but not enough replication peers.
	newTestJournal(c, tf, pb.JournalSpec{Name: "not/enough/peers", Replication: 3}, broker.id, peer.id)
	res, _ = broker.resolve(resolveArgs{ctx: ctx, journal: "not/enough/peers"})

	stream, _ = broker.MustClient().Append(ctx)
	c.Check(stream.Send(&pb.AppendRequest{Journal: "not/enough/peers"}), gc.IsNil)

	resp, err = stream.CloseAndRecv()
	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, &pb.AppendResponse{Status: pb.Status_INSUFFICIENT_JOURNAL_BROKERS, Header: &res.Header})

	// Case: Journal which is marked as read-only.
	newTestJournal(c, tf, pb.JournalSpec{Name: "read/only", Replication: 1, Flags: pb.JournalSpec_O_RDONLY}, broker.id)
	res, _ = broker.resolve(resolveArgs{ctx: ctx, journal: "read/only"})

	stream, _ = broker.MustClient().Append(ctx)
	c.Check(stream.Send(&pb.AppendRequest{Journal: "read/only"}), gc.IsNil)

	resp, err = stream.CloseAndRecv()
	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, &pb.AppendResponse{Status: pb.Status_NOT_ALLOWED, Header: &res.Header})
}

func (s *AppendSuite) TestProxyCases(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	var broker = newTestBroker(c, tf, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"}, newReplica)
	var peer = newMockBroker(c, tf, pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"})

	newTestJournal(c, tf, pb.JournalSpec{Name: "a/journal", Replication: 1}, peer.id)
	var res, _ = broker.resolve(resolveArgs{
		ctx:            tf.ctx,
		journal:        "a/journal",
		mayProxy:       true,
		requirePrimary: true,
	})
	var ctx = pb.WithDispatchDefault(tf.ctx)

	// Case: initial request is proxied to the peer, with Header attached.
	var stream, _ = broker.MustClient().Append(ctx)
	c.Check(stream.Send(&pb.AppendRequest{Journal: "a/journal"}), gc.IsNil)

	c.Check(<-peer.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{
		Journal: "a/journal",
		Header:  &res.Header,
	})

	// Expect client content and EOF are proxied.
	c.Check(stream.Send(&pb.AppendRequest{Content: []byte("foobar")}), gc.IsNil)
	c.Check(<-peer.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{Content: []byte("foobar")})
	c.Check(stream.Send(&pb.AppendRequest{}), gc.IsNil)
	c.Check(<-peer.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{})
	c.Check(stream.CloseSend(), gc.IsNil)
	c.Check(<-peer.AppendReqCh, gc.IsNil)

	// Expect peer's response is proxied back to the client.
	peer.AppendRespCh <- &pb.AppendResponse{Commit: &pb.Fragment{Begin: 1234, End: 5678}}

	var resp, err = stream.CloseAndRecv()
	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, &pb.AppendResponse{Commit: &pb.Fragment{Begin: 1234, End: 5678}})

	// Case: proxied request fails due to broker failure.
	stream, _ = broker.MustClient().Append(ctx)
	c.Check(stream.Send(&pb.AppendRequest{Journal: "a/journal"}), gc.IsNil)

	c.Check(<-peer.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{
		Journal: "a/journal",
		Header:  &res.Header,
	})

	// Expect peer error is proxied back to client.
	peer.ErrCh <- errors.New("some kind of error")
	_, err = stream.CloseAndRecv()
	c.Check(<-peer.AppendReqCh, gc.IsNil) // Read client EOF.
	c.Check(err, gc.ErrorMatches, `rpc error: code = Unknown desc = some kind of error`)

	// Case: proxied request fails due to client read error.
	var failCtx, failCtxCancel = context.WithCancel(ctx)

	stream, _ = broker.MustClient().Append(failCtx)
	c.Check(stream.Send(&pb.AppendRequest{Journal: "a/journal"}), gc.IsNil)

	c.Check(<-peer.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{
		Journal: "a/journal",
		Header:  &res.Header,
	})

	failCtxCancel()
	c.Check(<-peer.AppendReqCh, gc.IsNil) // Expect EOF, which is treated as rollback by appender.

	_, err = stream.CloseAndRecv()
	c.Check(err, gc.ErrorMatches, `rpc error: code = Canceled desc = context canceled`)
}

func (s *AppendSuite) TestAppenderCases(c *gc.C) {
	var rm = newReplicationMock(c)
	defer rm.cancel()

	// Tweak Spool fixture to have a non-zero size & sum.
	var spool = <-rm.spoolCh
	spool.Fragment.End = 24
	spool.Fragment.Sum = pb.SHA1Sum{Part1: 1234}
	rm.spoolCh <- spool
	var pln = rm.newPipeline(rm.header(0, 100))

	var spec = pb.JournalSpec_Fragment{
		Length:           16, // Fix such that current Spool is over target length.
		CompressionCodec: pb.CompressionCodec_SNAPPY,
		Stores:           []pb.FragmentStore{"s3://a-bucket/path"},
	}
	var appender = beginAppending(pln, spec)

	// Expect an updating proposal is scattered.
	c.Check(<-rm.brokerA.ReplReqCh, gc.DeepEquals, &pb.ReplicateRequest{
		Proposal: &pb.Fragment{
			Begin:            24,
			End:              24,
			Journal:          "a/journal",
			CompressionCodec: pb.CompressionCodec_SNAPPY,
			BackingStore:     "s3://a-bucket/path",
		},
		Acknowledge: false,
	})
	<-rm.brokerC.ReplReqCh

	// Chunk one. Expect it's forwarded to peers.
	c.Check(appender.onRecv(&pb.AppendRequest{Content: []byte("foo")}, nil), gc.Equals, true)
	var req, _ = <-rm.brokerA.ReplReqCh, <-rm.brokerC.ReplReqCh
	c.Check(req, gc.DeepEquals, &pb.ReplicateRequest{
		Content:      []byte("foo"),
		ContentDelta: 0,
	})
	// Chunk two.
	c.Check(appender.onRecv(&pb.AppendRequest{Content: []byte("bar")}, nil), gc.Equals, true)
	req, _ = <-rm.brokerA.ReplReqCh, <-rm.brokerC.ReplReqCh
	c.Check(req, gc.DeepEquals, &pb.ReplicateRequest{
		Content:      []byte("bar"),
		ContentDelta: 3,
	})
	// Empty chunk signals we wish to commit.
	c.Check(appender.onRecv(&pb.AppendRequest{}, nil), gc.Equals, true)
	c.Check(appender.reqCommit, gc.Equals, true)

	// Client EOF. Expect a commit proposal is scattered to peers.
	c.Check(appender.onRecv(nil, io.EOF), gc.Equals, false)

	var expect = &pb.Fragment{
		Journal:          "a/journal",
		Begin:            24,
		End:              30,
		Sum:              pb.SHA1Sum{Part1: 0x8843d7f92416211d, Part2: 0xe9ebb963ff4ce281, Part3: 0x25932878},
		CompressionCodec: pb.CompressionCodec_SNAPPY,
		BackingStore:     "s3://a-bucket/path",
	}
	req, _ = <-rm.brokerA.ReplReqCh, <-rm.brokerC.ReplReqCh
	c.Check(req, gc.DeepEquals, &pb.ReplicateRequest{Proposal: expect, Acknowledge: true})

	c.Check(appender.reqFragment, gc.DeepEquals, &pb.Fragment{
		Journal:          "a/journal",
		Begin:            24,
		End:              30,
		Sum:              pb.SHA1Sum{Part1: 0x8843d7f92416211d, Part2: 0xe9ebb963ff4ce281, Part3: 0x25932878},
		CompressionCodec: pb.CompressionCodec_SNAPPY,
	})
	c.Check(appender.reqErr, gc.IsNil)

	// Case: Expect a non-validating AppendRequest is treated as a client error, and triggers rollback.
	// Also, first expect an updating proposal is not required and is not sent this time.
	appender = beginAppending(pln, spec)

	// Valid first chunk.
	c.Check(appender.onRecv(&pb.AppendRequest{Content: []byte("baz")}, nil), gc.Equals, true)
	req, _ = <-rm.brokerA.ReplReqCh, <-rm.brokerC.ReplReqCh
	c.Check(req, gc.DeepEquals, &pb.ReplicateRequest{Content: []byte("baz")})

	// Send invalid AppendRequest.
	c.Check(appender.onRecv(&pb.AppendRequest{Journal: "/invalid"}, nil), gc.Equals, false)
	c.Check(appender.reqFragment, gc.IsNil)
	c.Check(appender.reqErr, gc.ErrorMatches, `Journal: cannot begin with '/' \(/invalid\)`)

	// Expect a rollback is scattered to peers.
	req, _ = <-rm.brokerA.ReplReqCh, <-rm.brokerC.ReplReqCh
	c.Check(req, gc.DeepEquals, &pb.ReplicateRequest{Proposal: expect, Acknowledge: true})

	// Case: Expect an EOF without first sending an empty chunk is unexpected, and triggers a rollback.
	// Also check an updated proposal is still not sent, despite the spec codec
	// differing, because the spool is non-empty and not over the Fragment Length.
	spec.CompressionCodec = pb.CompressionCodec_GZIP
	appender = beginAppending(pln, spec)

	// Send unexpected EOF.
	c.Check(appender.onRecv(nil, io.EOF), gc.Equals, false)
	c.Check(appender.reqErr, gc.Equals, io.ErrUnexpectedEOF)

	req, _ = <-rm.brokerA.ReplReqCh, <-rm.brokerC.ReplReqCh
	c.Check(req, gc.DeepEquals, &pb.ReplicateRequest{Proposal: expect, Acknowledge: true})

	// Case: Expect another read error triggers a rollback.
	appender = beginAppending(pln, spec)
	c.Check(appender.onRecv(nil, errors.New("some error")), gc.Equals, false)
	c.Check(appender.reqErr, gc.DeepEquals, errors.New("some error"))

	req, _ = <-rm.brokerA.ReplReqCh, <-rm.brokerC.ReplReqCh
	c.Check(req, gc.DeepEquals, &pb.ReplicateRequest{Proposal: expect, Acknowledge: true})

	// Case: Expect *not* reading an EOF after a commit chunk triggers an error and rollback.
	appender = beginAppending(pln, spec)

	c.Check(appender.onRecv(&pb.AppendRequest{}, nil), gc.Equals, true)
	c.Check(appender.onRecv(&pb.AppendRequest{Content: []byte("foo")}, nil), gc.Equals, false)
	c.Check(appender.reqErr, gc.DeepEquals, errExpectedEOF)

	req, _ = <-rm.brokerA.ReplReqCh, <-rm.brokerC.ReplReqCh
	c.Check(req, gc.DeepEquals, &pb.ReplicateRequest{Proposal: expect, Acknowledge: true})
}

func expectPipelineSync(c *gc.C, peer testBroker, hdr pb.Header) {
	// Expect an initial request, with header, synchronizing the replication pipeline.
	c.Check(<-peer.ReplReqCh, gc.DeepEquals, &pb.ReplicateRequest{
		Journal: "a/journal",
		Header:  boxHeaderProcessID(hdr, peer.id),
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			CompressionCodec: pb.CompressionCodec_NONE,
		},
		Acknowledge: true,
	})
	peer.ReplRespCh <- &pb.ReplicateResponse{Status: pb.Status_OK} // Acknowledge.

	// Expect a non-ack'd command to roll the Spool to the SNAPPY codec (configured in the fixture).
	c.Check(<-peer.ReplReqCh, gc.DeepEquals, &pb.ReplicateRequest{
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			CompressionCodec: pb.CompressionCodec_SNAPPY,
		},
	})
}

var _ = gc.Suite(&AppendSuite{})
