package consumer

import (
	"context"

	"github.com/LiveRamp/gazette/v2/pkg/server"
	gc "github.com/go-check/check"
	"google.golang.org/grpc"
)

// loopbackServer serves a ShardServer over a loopback, for use within tests.
type loopbackServer struct {
	*server.Server
	Conn *grpc.ClientConn
}

// newLoopbackServer returns a loopbackServer of the provided ShardServer.
func newLoopbackServer(ctx context.Context, ss ShardServer) loopbackServer {
	var srv, err = server.New("127.0.0.1", 0)
	if err != nil {
		panic(err)
	}
	RegisterShardServer(srv.GRPCServer, ss)
	var conn = srv.MustGRPCLoopback()

	// Arrange to stop the server when |ctx| is cancelled.
	go func() {
		<-ctx.Done()
		_ = conn.Close()
		srv.GracefulStop()
	}()
	go srv.MustServe()

	return loopbackServer{Server: srv, Conn: conn}
}

// MustClient returns a ShardClient of the test loopbackServer.
func (s loopbackServer) MustClient() ShardClient { return NewShardClient(s.Conn) }

// shardServerStub stubs the read and write loops of ShardServer RPCs.
type shardServerStub struct {
	loopbackServer
	c *gc.C

	StatFunc     func(context.Context, *StatRequest) (*StatResponse, error)
	ListFunc     func(context.Context, *ListRequest) (*ListResponse, error)
	ApplyFunc    func(context.Context, *ApplyRequest) (*ApplyResponse, error)
	GetHintsFunc func(context.Context, *GetHintsRequest) (*GetHintsResponse, error)
}

// newShardServerStub returns a shardServerStub instance served by a local GRPC server.
func newShardServerStub(ctx context.Context, c *gc.C) *shardServerStub {
	var s = &shardServerStub{
		c: c,
	}
	s.loopbackServer = newLoopbackServer(ctx, s)
	return s
}

// Stat implements the shardServerStub interface by proxying through StatFunc.
func (s *shardServerStub) Stat(ctx context.Context, req *StatRequest) (*StatResponse, error) {
	return s.StatFunc(ctx, req)
}

// List implements the shardServerStub interface by proxying through ListFunc.
func (s *shardServerStub) List(ctx context.Context, req *ListRequest) (*ListResponse, error) {
	return s.ListFunc(ctx, req)
}

// Apply implements the shardServerStub interface by proxying through ApplyFunc.
func (s *shardServerStub) Apply(ctx context.Context, req *ApplyRequest) (*ApplyResponse, error) {
	return s.ApplyFunc(ctx, req)
}

// GetHints implements the shardServerStub interface by proxying through GetHintsFunc.
func (s *shardServerStub) GetHints(ctx context.Context, req *GetHintsRequest) (*GetHintsResponse, error) {
	return s.GetHintsFunc(ctx, req)
}
