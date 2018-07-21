package client

import (
	"context"

	gc "github.com/go-check/check"
	"google.golang.org/grpc"

	"github.com/LiveRamp/gazette/pkg/broker/teststub"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

type DialerSuite struct{}

func (s *DialerSuite) TestDialer(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var peer1, peer2 = teststub.NewBroker(c, ctx), teststub.NewBroker(c, ctx)

	var route = pb.Route{
		Members:   []pb.ProcessSpec_ID{{"zone-a", "member-1"}, {"zone-b", "member-2"}},
		Endpoints: []pb.Endpoint{peer1.Endpoint(), peer2.Endpoint()},
	}

	var d, err = NewDialer(8)
	c.Check(err, gc.IsNil)

	var conn1a, conn1b, conn2 *grpc.ClientConn

	// Dial first peer.
	conn1a, err = d.Dial(ctx, pb.ProcessSpec_ID{Zone: "zone-a", Suffix: "member-1"}, route)
	c.Check(conn1a, gc.NotNil)
	c.Check(err, gc.IsNil)

	// Dial second peer. Connection instances differ.
	conn2, err = d.Dial(ctx, pb.ProcessSpec_ID{Zone: "zone-b", Suffix: "member-2"}, route)
	c.Check(conn2, gc.NotNil)
	c.Check(err, gc.IsNil)
	c.Check(conn1a, gc.Not(gc.Equals), conn2)

	// Dial first peer again. Expect the connection is cached and returned again.
	conn1b, err = d.Dial(ctx, pb.ProcessSpec_ID{Zone: "zone-a", Suffix: "member-1"}, route)
	c.Check(err, gc.IsNil)
	c.Check(conn1a, gc.Equals, conn1b)

	// Expect an error for an ID not in the Route (even if it's cached).
	route = pb.Route{
		Members:   []pb.ProcessSpec_ID{{"zone-a", "member-1"}},
		Endpoints: []pb.Endpoint{peer1.Endpoint()},
	}
	_, err = d.Dial(ctx, pb.ProcessSpec_ID{Zone: "zone-b", Suffix: "member-2"}, route)
	c.Check(err, gc.ErrorMatches, `no such Member in Route \(id: zone:"zone-b" suffix:"member-2" .*\)`)

	// Also expect an error if Endpoints are not attached.
	route.Endpoints = nil
	_, err = d.Dial(ctx, pb.ProcessSpec_ID{Zone: "zone-a", Suffix: "member-1"}, route)
	c.Check(err, gc.ErrorMatches, `missing Route Endpoints \(id: zone:"zone-a" suffix:"member-1" .*\)`)
}

var _ = gc.Suite(&DialerSuite{})
