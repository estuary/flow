package client

import (
	"context"
	"io"

	gc "github.com/go-check/check"

	"github.com/LiveRamp/gazette/pkg/broker/teststub"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

type RoutingSuite struct{}

func (s *RoutingSuite) TestRoutingCases(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var def = teststub.NewBroker(c, ctx)
	var peerA = teststub.NewBroker(c, ctx)
	var peerB = teststub.NewBroker(c, ctx)

	var dialer, _ = NewDialer(1)

	var rc, err = NewRoutingClient(def.MustClient(), "A", dialer, 8)
	c.Assert(err, gc.IsNil)

	var verifyAppend = func(client pb.Broker_AppendClient, err error) func(*teststub.Broker) {
		return func(broker *teststub.Broker) {
			c.Check(err, gc.IsNil)

			c.Check(client.Send(&pb.AppendRequest{Journal: "a/journal"}), gc.IsNil)
			c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{Journal: "a/journal"})
			broker.AppendRespCh <- &pb.AppendResponse{}

			client.CloseAndRecv()
			c.Check(<-broker.AppendReqCh, gc.IsNil)
		}
	}
	var verifyRead = func(client pb.Broker_ReadClient, err error) func(*teststub.Broker) {
		return func(broker *teststub.Broker) {
			c.Check(err, gc.IsNil)

			c.Check(<-broker.ReadReqCh, gc.DeepEquals, &pb.ReadRequest{Journal: "a/journal"})
			broker.ErrCh <- nil

			_, err = client.Recv()
			c.Check(err, gc.Equals, io.EOF)
		}
	}

	// No Route is set. Expect Read and Append go to the default client.
	verifyRead(rc.Read(ctx, &pb.ReadRequest{Journal: "a/journal"}))(def)
	verifyAppend(rc.Append(WithJournalHint(ctx, "a/journal")))(def)

	var route = pb.Route{
		Members:   []pb.ProcessSpec_ID{{Zone: "A", Suffix: "1"}, {Zone: "B", Suffix: "2"}},
		Primary:   1,
		Endpoints: []pb.Endpoint{peerA.Endpoint(), peerB.Endpoint()},
	}

	// Set a Route for the journal.
	rc.(RouteUpdater).UpdateRoute("a/journal", &route)

	// Expect Read requests go to |peerA|, as it's the same zone as the client.
	verifyRead(rc.Read(ctx, &pb.ReadRequest{Journal: "a/journal"}))(peerA)
	// Expect Append requests go to |peerB|, as the primary.
	verifyAppend(rc.Append(WithJournalHint(ctx, "a/journal")))(peerB)

	// Route is purged. Expect Read & Append go to the default client again.
	rc.(RouteUpdater).UpdateRoute("a/journal", nil)
	verifyRead(rc.Read(ctx, &pb.ReadRequest{Journal: "a/journal"}))(def)
	verifyAppend(rc.Append(WithJournalHint(ctx, "a/journal")))(def)

	// Set a ListFunc fixture which returns |route|, and execute it.
	def.ListFunc = func(context.Context, *pb.ListRequest) (*pb.ListResponse, error) {
		return &pb.ListResponse{
			Journals: []pb.ListResponse_Journal{
				{
					Spec:  pb.JournalSpec{Name: "a/journal"},
					Route: route,
				},
			},
		}, nil
	}
	_, err = rc.List(ctx, &pb.ListRequest{})
	c.Check(err, gc.IsNil)

	// Expect requests once again reflect |route|.
	verifyRead(rc.Read(ctx, &pb.ReadRequest{Journal: "a/journal"}))(peerA)
	verifyAppend(rc.Append(WithJournalHint(ctx, "a/journal")))(peerB)
}

var _ = gc.Suite(&RoutingSuite{})
