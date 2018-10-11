package broker

import (
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	gc "github.com/go-check/check"
)

// TODO(johnny): Add additional E2E testing scenarios (Issue #66).

type E2ESuite struct{}

func (s *E2ESuite) TestReplicatedAppendAndRead(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	var broker = newTestBroker(c, tf, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"}, newReadyReplica)
	var peer = newTestBroker(c, tf, pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"}, newReadyReplica)

	newTestJournal(c, tf, pb.JournalSpec{Name: "journal/one", Replication: 2}, broker.id, peer.id)
	newTestJournal(c, tf, pb.JournalSpec{Name: "journal/two", Replication: 2}, peer.id, broker.id)

	var ctx = pb.WithDispatchDefault(tf.ctx)
	var rOne, _ = peer.MustClient().Read(ctx, &pb.ReadRequest{Journal: "journal/one", Block: true})
	var rTwo, _ = broker.MustClient().Read(ctx, &pb.ReadRequest{Journal: "journal/two", Block: true})

	// First Append is served by |broker|, with its Read served by |peer|.
	var stream, _ = broker.MustClient().Append(ctx)
	c.Check(stream.Send(&pb.AppendRequest{Journal: "journal/one"}), gc.IsNil)
	c.Check(stream.Send(&pb.AppendRequest{Content: []byte("hello")}), gc.IsNil)
	c.Check(stream.Send(&pb.AppendRequest{}), gc.IsNil)
	_, _ = stream.CloseAndRecv()

	// Second Append is served by |peer| (through |broker|), with its Read served by |broker|.
	stream, _ = broker.MustClient().Append(ctx)
	c.Check(stream.Send(&pb.AppendRequest{Journal: "journal/two"}), gc.IsNil)
	c.Check(stream.Send(&pb.AppendRequest{Content: []byte("world!")}), gc.IsNil)
	c.Check(stream.Send(&pb.AppendRequest{}), gc.IsNil)
	_, _ = stream.CloseAndRecv()

	// Read Fragment metadata, then content from each Read stream.
	_, err := rOne.Recv()
	c.Check(err, gc.IsNil)
	_, err = rTwo.Recv()
	c.Check(err, gc.IsNil)

	expectReadResponse(c, rOne, pb.ReadResponse{Content: []byte("hello")})
	expectReadResponse(c, rTwo, pb.ReadResponse{Content: []byte("world!")})
}

var _ = gc.Suite(&E2ESuite{})
