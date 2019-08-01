package consumer

import (
	"context"

	"github.com/stretchr/testify/assert"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/server"
	"go.gazette.dev/core/task"
)

// shardServerStub stubs the read and write loops of ShardServer RPCs.
// C.f. teststub.Broker
type shardServerStub struct {
	t     assert.TestingT
	tasks *task.Group
	srv   *server.Server

	StatFunc     func(context.Context, *StatRequest) (*StatResponse, error)
	ListFunc     func(context.Context, *ListRequest) (*ListResponse, error)
	ApplyFunc    func(context.Context, *ApplyRequest) (*ApplyResponse, error)
	GetHintsFunc func(context.Context, *GetHintsRequest) (*GetHintsResponse, error)
}

// newShardServerStub returns a shardServerStub instance served by a local GRPC server.
func newShardServerStub(t assert.TestingT) *shardServerStub {
	var s = &shardServerStub{
		t:     t,
		tasks: task.NewGroup(context.Background()),
		srv:   server.MustLoopback(),
	}
	RegisterShardServer(s.srv.GRPCServer, s)
	s.srv.QueueTasks(s.tasks)
	s.tasks.GoRun()
	return s
}

// Client returns a JournalClient wrapping the GRPCLoopback.
func (s *shardServerStub) client() ShardClient { return NewShardClient(s.srv.GRPCLoopback) }

// Endpoint returns the server Endpoint.
func (s *shardServerStub) endpoint() pb.Endpoint { return s.srv.Endpoint() }

// cleanup cancels the shardServerStub task.Group and asserts that it exits cleanly.
func (s *shardServerStub) cleanup() {
	s.tasks.Cancel()
	s.srv.GRPCServer.GracefulStop()
	assert.NoError(s.t, s.srv.GRPCLoopback.Close())
	assert.NoError(s.t, s.tasks.Wait())
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
