package teststub

import (
	"context"
	"sync"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/server"
	"go.gazette.dev/core/task"
)

// Broker stubs the read and write loops of broker RPCs, routing them onto
// channels which can be synchronously read and written within test bodies.
type Broker struct {
	t     require.TestingT
	tasks *task.Group
	srv   *server.Server
	wg    sync.WaitGroup

	ReadLoopErrCh  chan error                // Chan from which tests read final read-loop errors.
	WriteLoopErrCh chan error                // Chan to which tests write final write-loop error (or nil).
	ReplReqCh      chan pb.ReplicateRequest  // Chan from which tests read ReplicateRequest.
	ReplRespCh     chan pb.ReplicateResponse // Chan to which tests write ReplicateResponse.
	ReadReqCh      chan pb.ReadRequest       // Chan from which tests read ReadRequest.
	ReadRespCh     chan pb.ReadResponse      // Chan to which tests write ReadResponse.
	AppendReqCh    chan pb.AppendRequest     // Chan from which tests read AppendRequest.
	AppendRespCh   chan pb.AppendResponse    // Chan to which tests write AppendResponse.

	ListFunc          func(context.Context, *pb.ListRequest) (*pb.ListResponse, error)           // List implementation.
	ApplyFunc         func(context.Context, *pb.ApplyRequest) (*pb.ApplyResponse, error)         // Apply implementation.
	ListFragmentsFunc func(context.Context, *pb.FragmentsRequest) (*pb.FragmentsResponse, error) // ListFragments implementation.
}

// NewBroker returns a Broker instance served by a local gRPC server.
func NewBroker(t require.TestingT) *Broker {
	var p = &Broker{
		t:              t,
		srv:            server.MustLoopback(),
		tasks:          task.NewGroup(context.Background()),
		ReplReqCh:      make(chan pb.ReplicateRequest),
		ReplRespCh:     make(chan pb.ReplicateResponse),
		ReadReqCh:      make(chan pb.ReadRequest),
		ReadRespCh:     make(chan pb.ReadResponse),
		AppendReqCh:    make(chan pb.AppendRequest),
		AppendRespCh:   make(chan pb.AppendResponse),
		ReadLoopErrCh:  make(chan error),
		WriteLoopErrCh: make(chan error),
	}
	pb.RegisterJournalServer(p.srv.GRPCServer, p)
	p.srv.QueueTasks(p.tasks)
	p.tasks.GoRun()
	return p
}

// Client returns a RoutedJournalClient wrapping the GRPCLoopback.
func (b *Broker) Client() pb.RoutedJournalClient {
	return pb.NewRoutedJournalClient(pb.NewJournalClient(b.srv.GRPCLoopback), pb.NoopDispatchRouter{})
}

// Endpoint returns the server Endpoint.
func (b *Broker) Endpoint() pb.Endpoint { return b.srv.Endpoint() }

// Cleanup cancels the Broker tasks.Group and asserts that it exits cleanly.
func (b *Broker) Cleanup() {
	b.tasks.Cancel()
	b.srv.GRPCServer.GracefulStop()
	b.wg.Wait() // Ensure all read loops have exited.
	assert.NoError(b.t, b.srv.GRPCLoopback.Close())
	assert.NoError(b.t, b.tasks.Wait())
}

// Replicate implements the JournalServer interface by proxying requests &
// responses through channels ReplReqCh & ReplRespCh.
func (b *Broker) Replicate(srv pb.Journal_ReplicateServer) error {
	// Start a read loop of requests from |srv|.
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		for {
			if msg, err := srv.Recv(); err == nil {
				select {
				case b.ReplReqCh <- *msg:
				case <-time.After(timeout):
					require.FailNow(b.t, "test didn't recv from ReplReqCh", "msg", msg)
				}
			} else {
				if srv.Context().Err() != nil {
					err = srv.Context().Err()
				}
				select {
				case b.ReadLoopErrCh <- err:
				case <-time.After(timeout):
					require.FailNow(b.t, "test didn't recv from ReadLoopErrCh", "err", err)
				}
				return
			}
		}
	}()

	for {
		select {
		case resp := <-b.ReplRespCh:
			assert.NoError(b.t, srv.Send(&resp))
		case err := <-b.WriteLoopErrCh:
			return err
		case <-time.After(timeout):
			require.FailNow(b.t, "test didn't send to ReplRespCh or WriteLoopErrCh")
		}
	}
}

// Read implements the JournalServer interface by proxying requests & responses
// through channels ReadReqCh & ReadResponseCh.
func (b *Broker) Read(req *pb.ReadRequest, srv pb.Journal_ReadServer) error {
	select {
	case b.ReadReqCh <- *req:
		// |req| passed to test.
	case <-time.After(timeout):
		require.FailNow(b.t, "test didn't read from ReadReqCh")
	}

	for {
		select {
		case resp := <-b.ReadRespCh:
			// We may be sending into a cancelled context (c.f. TestReaderRetries).
			// Otherwise, we expect to be able to send without error.
			if err := srv.Send(&resp); srv.Context().Err() == nil {
				assert.NoError(b.t, err)
			}
		case err := <-b.WriteLoopErrCh:
			return err
		case <-time.After(timeout):
			require.FailNow(b.t, "test didn't send to ReadRespCh or WriteLoopErrCh")
		}
	}
}

// Append implements the JournalServer interface by proxying requests &
// responses through channels AppendReqCh & AppendRespCh.
func (b *Broker) Append(srv pb.Journal_AppendServer) error {
	// Start a read loop of requests from |srv|.
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		for {
			if msg, err := srv.Recv(); err == nil {
				select {
				case b.AppendReqCh <- *msg:
				case <-time.After(timeout):
					require.FailNow(b.t, "test didn't recv from AppendReqCh", "msg", msg)
				}
			} else {
				if srv.Context().Err() != nil {
					err = srv.Context().Err()
				}
				select {
				case b.ReadLoopErrCh <- err:
				case <-time.After(timeout):
					require.FailNow(b.t, "test didn't recv from ReadLoopErrCh", "err", err)
				}
				return
			}
		}
	}()

	for {
		select {
		case resp := <-b.AppendRespCh:
			assert.NoError(b.t, srv.SendAndClose(&resp))
			return nil
		case err := <-b.WriteLoopErrCh:
			return err
		case <-time.After(timeout):
			require.FailNow(b.t, "test didn't send to AppendRespCh or WriteLoopErrCh")
		}
	}
}

// List implements the JournalServer interface by proxying through ListFunc.
func (b *Broker) List(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	return b.ListFunc(ctx, req)
}

// Apply implements the JournalServer interface by proxying through ApplyFunc.
func (b *Broker) Apply(ctx context.Context, req *pb.ApplyRequest) (*pb.ApplyResponse, error) {
	return b.ApplyFunc(ctx, req)
}

// ListFragments implements the JournalServer interface by proxying through FragmentsFunc.
func (b *Broker) ListFragments(ctx context.Context, req *pb.FragmentsRequest) (*pb.FragmentsResponse, error) {
	return b.ListFragmentsFunc(ctx, req)
}

func init() { pb.RegisterGRPCDispatcher("local") }

const timeout = time.Second
