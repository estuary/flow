package consumer

import (
	"context"

	"github.com/LiveRamp/gazette/v2/pkg/server"
	"github.com/LiveRamp/gazette/v2/pkg/task"
	gc "github.com/go-check/check"
	"google.golang.org/grpc"
)

// loopbackServer serves a ShardServer over a loopback, for use within tests.
type loopbackServer struct {
	*server.Server
	Conn *grpc.ClientConn
}

// newLoopbackServer returns a loopbackServer of the provided ShardServer.
func newLoopbackServer(ss ShardServer) loopbackServer {
	var srv, err = server.New("127.0.0.1", 0)
	if err != nil {
		panic(err)
	}
	RegisterShardServer(srv.GRPCServer, ss)

	return loopbackServer{Server: srv, Conn: srv.MustGRPCLoopback()}
}

// MustClient returns a ShardClient of the test loopbackServer.
func (s loopbackServer) MustClient() ShardClient { return NewShardClient(s.Conn) }

// shardServerStub stubs the read and write loops of ShardServer RPCs.
type shardServerStub struct {
	c     *gc.C
	tasks *task.Group
	loopbackServer

	StatFunc     func(context.Context, *StatRequest) (*StatResponse, error)
	ListFunc     func(context.Context, *ListRequest) (*ListResponse, error)
	ApplyFunc    func(context.Context, *ApplyRequest) (*ApplyResponse, error)
	GetHintsFunc func(context.Context, *GetHintsRequest) (*GetHintsResponse, error)
}

// newShardServerStub returns a shardServerStub instance served by a local GRPC server.
func newShardServerStub(c *gc.C, ctx context.Context) *shardServerStub {
	var s = &shardServerStub{
		c:     c,
		tasks: task.NewGroup(ctx),
	}
	s.loopbackServer = newLoopbackServer(s)
	s.loopbackServer.QueueTasks(s.tasks)

	s.tasks.Queue("Conn.Close", func() error {
		<-s.tasks.Context().Done()
		return s.loopbackServer.Conn.Close()
	})
	s.tasks.GoRun()

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
