package broker

import (
	"context"

	"github.com/LiveRamp/gazette/v2/pkg/fragment"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	gc "github.com/go-check/check"
)

// TODO(johnny): Add additional E2E testing scenarios (Issue #66).

type E2ESuite struct{}

func (s *E2ESuite) TestReplicatedAppendAndRead(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var ks = NewKeySpace("/root")
	var broker = newTestBroker(c, ctx, ks, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	var peer = newTestBroker(c, ctx, ks, pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"})

	newTestJournal(c, ks, "journal/one", 2, broker.id, peer.id)
	newTestJournal(c, ks, "journal/two", 2, peer.id, broker.id)

	// "Complete" initial remote load.
	broker.replicas["journal/one"].index.ReplaceRemote(fragment.CoverSet{})
	peer.replicas["journal/two"].index.ReplaceRemote(fragment.CoverSet{})

	ctx = pb.WithDispatchDefault(ctx)
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

	expectReadResponse(c, rOne, &pb.ReadResponse{Content: []byte("hello")})
	expectReadResponse(c, rTwo, &pb.ReadResponse{Content: []byte("world!")})
}

var _ = gc.Suite(&E2ESuite{})
