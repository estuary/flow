package consumer

import (
	"context"

	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/server"
	"go.gazette.dev/core/task"
)

// shardServerStub stubs the read and write loops of ShardServer RPCs.
// C.f. teststub.Broker
type shardServerStub struct {
	t     require.TestingT
	tasks *task.Group
	srv   *server.Server

	StatFunc     func(context.Context, *pc.StatRequest) (*pc.StatResponse, error)
	ListFunc     func(context.Context, *pc.ListRequest) (*pc.ListResponse, error)
	ApplyFunc    func(context.Context, *pc.ApplyRequest) (*pc.ApplyResponse, error)
	GetHintsFunc func(context.Context, *pc.GetHintsRequest) (*pc.GetHintsResponse, error)
}

// newShardServerStub returns a shardServerStub instance served by a local GRPC server.
func newShardServerStub(t require.TestingT) *shardServerStub {
	var s = &shardServerStub{
		t:     t,
		tasks: task.NewGroup(context.Background()),
		srv:   server.MustLoopback(),
	}
	pc.RegisterShardServer(s.srv.GRPCServer, s)
	s.srv.QueueTasks(s.tasks)
	s.tasks.GoRun()
	return s
}

// Client returns a JournalClient wrapping the GRPCLoopback.
func (s *shardServerStub) client() pc.ShardClient { return pc.NewShardClient(s.srv.GRPCLoopback) }

// Endpoint returns the server Endpoint.
func (s *shardServerStub) endpoint() pb.Endpoint { return s.srv.Endpoint() }

// cleanup cancels the shardServerStub task.Group and asserts that it exits cleanly.
func (s *shardServerStub) cleanup() {
	s.tasks.Cancel()
	s.srv.BoundedGracefulStop()
	require.NoError(s.t, s.srv.GRPCLoopback.Close())
	require.NoError(s.t, s.tasks.Wait())
}

// Stat implements the shardServerStub interface by proxying through StatFunc.
func (s *shardServerStub) Stat(ctx context.Context, req *pc.StatRequest) (*pc.StatResponse, error) {
	return s.StatFunc(ctx, req)
}

// List implements the shardServerStub interface by proxying through ListFunc.
func (s *shardServerStub) List(ctx context.Context, req *pc.ListRequest) (*pc.ListResponse, error) {
	return s.ListFunc(ctx, req)
}

// Apply implements the shardServerStub interface by proxying through ApplyFunc.
func (s *shardServerStub) Apply(ctx context.Context, req *pc.ApplyRequest) (*pc.ApplyResponse, error) {
	return s.ApplyFunc(ctx, req)
}

// GetHints implements the shardServerStub interface by proxying through GetHintsFunc.
func (s *shardServerStub) GetHints(ctx context.Context, req *pc.GetHintsRequest) (*pc.GetHintsResponse, error) {
	return s.GetHintsFunc(ctx, req)
}
