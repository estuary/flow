package teststub

import (
	"context"
	"io"
	"time"

	"github.com/stretchr/testify/assert"
	pb "go.gazette.dev/core/protocol"
	"go.gazette.dev/core/server"
	"go.gazette.dev/core/task"
)

// Broker stubs the read and write loops of broker RPCs, routing them onto
// channels which can be synchronously read and written within test bodies.
type Broker struct {
	t     assert.TestingT
	tasks *task.Group
	srv   *server.Server

	ReplReqCh  chan *pb.ReplicateRequest
	ReplRespCh chan *pb.ReplicateResponse

	ReadReqCh  chan *pb.ReadRequest
	ReadRespCh chan *pb.ReadResponse

	AppendReqCh  chan *pb.AppendRequest
	AppendRespCh chan *pb.AppendResponse

	ListFunc          func(context.Context, *pb.ListRequest) (*pb.ListResponse, error)
	ApplyFunc         func(context.Context, *pb.ApplyRequest) (*pb.ApplyResponse, error)
	ListFragmentsFunc func(context.Context, *pb.FragmentsRequest) (*pb.FragmentsResponse, error)

	ErrCh chan error
}

// NewBroker returns a Broker instance served by a local GRPC server.
func NewBroker(t assert.TestingT) *Broker {
	var p = &Broker{
		t:            t,
		srv:          server.MustLoopback(),
		tasks:        task.NewGroup(context.Background()),
		ReplReqCh:    make(chan *pb.ReplicateRequest),
		ReplRespCh:   make(chan *pb.ReplicateResponse),
		ReadReqCh:    make(chan *pb.ReadRequest),
		ReadRespCh:   make(chan *pb.ReadResponse),
		AppendReqCh:  make(chan *pb.AppendRequest),
		AppendRespCh: make(chan *pb.AppendResponse),
		ErrCh:        make(chan error),
	}
	pb.RegisterJournalServer(p.srv.GRPCServer, p)
	p.srv.QueueTasks(p.tasks)
	p.tasks.GoRun()
	return p
}

// Client returns a JournalClient wrapping the GRPCLoopback.
func (b *Broker) Client() pb.JournalClient { return pb.NewJournalClient(b.srv.GRPCLoopback) }

// Endpoint returns the server Endpoint.
func (b *Broker) Endpoint() pb.Endpoint { return b.srv.Endpoint() }

// Cleanup cancels the Broker tasks.Group and asserts that it exits cleanly.
func (b *Broker) Cleanup() {
	b.tasks.Cancel()
	b.srv.GRPCServer.GracefulStop()
	assert.NoError(b.t, b.srv.GRPCLoopback.Close())
	assert.NoError(b.t, b.tasks.Wait())
}

// Replicate implements the JournalServer interface by proxying requests &
// responses through channels ReplReqCh & ReplRespCh.
func (b *Broker) Replicate(srv pb.Journal_ReplicateServer) error {
	// Start a read loop of requests from |srv|.
	go func() {
		for {
			var msg, err = srv.Recv()
			if err == io.EOF {
				// Pass.
			} else if srv.Context().Err() == nil {
				// Allow a cancellation due to Replicate returning before reading
				// EOF, but no other errors are expected.
				assert.NoError(b.t, err)
			}

			select {
			case b.ReplReqCh <- msg:
			case <-srv.Context().Done(): // RPC is complete.
			case <-time.After(5 * time.Second):
				panic("test didn't recv from ReplReqCh")
			}

			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case resp := <-b.ReplRespCh:
			assert.NoError(b.t, srv.Send(resp))
		case err := <-b.ErrCh:
			return err
		case <-srv.Context().Done():
			assert.FailNow(b.t, "unexpected client RPC cancellation")
		case <-time.After(5 * time.Second):
			panic("test didn't send to ReplRespCh or ErrCh")
		}
	}
}

// Read implements the JournalServer interface by proxying requests & responses
// through channels ReadReqCh & ReadResponseCh.
func (b *Broker) Read(req *pb.ReadRequest, srv pb.Journal_ReadServer) error {
	select {
	case b.ReadReqCh <- req:
		// |req| passed to test.
	case _ = <-srv.Context().Done():
		return nil // Cancellation is the standard means to terminate a Read RPC.
	case <-time.After(5 * time.Second):
		panic("test didn't read from ReadReqCh")
	}

	for {
		select {
		case resp := <-b.ReadRespCh:
			// We may be sending into a cancelled context (c.f. TestReaderRetries).
			if err := srv.Send(resp); err != nil {
				return err
			}
		case err := <-b.ErrCh:
			return err
		case _ = <-srv.Context().Done():
			return nil
		case <-time.After(5 * time.Second):
			panic("test didn't send to ReadRespCh or ErrCh")
		}
	}
}

// Append implements the JournalServer interface by proxying requests &
// responses through channels AppendReqCh & AppendRespCh.
func (b *Broker) Append(srv pb.Journal_AppendServer) error {
	// Start a read loop of requests from |srv|.
	go func() {
		for {
			var msg, err = srv.Recv()
			if err == io.EOF {
				// Pass.
			} else if srv.Context().Err() == nil {
				// Like Replicate, allow a cancellation due to Append returning
				// before reading EOF, but no other errors are expected.
				assert.NoError(b.t, err)
			}

			select {
			case b.AppendReqCh <- msg:
			case <-srv.Context().Done(): // RPC is complete.
			case <-time.After(5 * time.Second):
				panic("test didn't recv from AppendReqCh")
			}

			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case resp := <-b.AppendRespCh:
			assert.NoError(b.t, srv.SendAndClose(resp))
			return nil
		case err := <-b.ErrCh:
			return err
		case <-srv.Context().Done():
			// Cancellation is atypical but not uncommon.
			return srv.Context().Err()
		case <-time.After(5 * time.Second):
			panic("test didn't send to AppendRespCh or ErrCh")
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
