package grpctest

import (
	"context"
	"net"

	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"google.golang.org/grpc"
)

// Server is a local gRPC server for use within tests.
type Server struct {
	*grpc.Server
	Ctx  context.Context
	Conn *grpc.ClientConn

	listener net.Listener
}

// NewServer returns a gRPC Server of the provided JournalServer.
func NewServer(ctx context.Context) Server {
	var l, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}

	conn, err := grpc.DialContext(ctx, l.Addr().String(),
		grpc.WithInsecure(),
		grpc.WithBalancerName(pb.DispatcherGRPCBalancerName))

	if err != nil {
		panic(err)
	}

	var p = Server{
		Server:   grpc.NewServer(),
		Ctx:      ctx,
		Conn:     conn,
		listener: l,
	}
	// Arrange to stop the Server when |ctx| is cancelled.
	go func() {
		<-ctx.Done()
		p.Conn.Close()
		p.Server.GracefulStop()
	}()

	return p
}

// Endpoint of the Server.
func (s Server) Endpoint() pb.Endpoint {
	return pb.Endpoint("http://" + s.listener.Addr().String() + "/path")
}

// Serve the test gRPC Server, blocking until its Context is cancelled.
func (s Server) Serve() { s.Server.Serve(s.listener) }
