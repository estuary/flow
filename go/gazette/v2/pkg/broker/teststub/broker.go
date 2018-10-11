package teststub

import (
	"context"
	"io"

	"github.com/LiveRamp/gazette/v2/pkg/grpctest"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	gc "github.com/go-check/check"
	log "github.com/sirupsen/logrus"
)

// Server is a local gRPC server for use within tests.
type Server struct {
	grpctest.Server
}

// NewServer returns a grpctest.Server of the provided JournalServer.
func NewServer(ctx context.Context, journalServer pb.JournalServer) Server {
	var s = grpctest.NewServer(ctx)

	pb.RegisterJournalServer(s.Server, journalServer)
	go s.Serve()

	return Server{Server: s}
}

// MustClient returns a JournalClient of the test Server.
func (s Server) MustClient() pb.JournalClient {
	return pb.NewJournalClient(s.Conn)
}

// Broker stubs the read and write loops of broker RPCs, routing them onto
// channels which can be synchronously read and written within test bodies.
type Broker struct {
	Server
	c *gc.C

	ReplReqCh  chan *pb.ReplicateRequest
	ReplRespCh chan *pb.ReplicateResponse

	ReadReqCh  chan *pb.ReadRequest
	ReadRespCh chan *pb.ReadResponse

	AppendReqCh  chan *pb.AppendRequest
	AppendRespCh chan *pb.AppendResponse

	ListFunc  func(context.Context, *pb.ListRequest) (*pb.ListResponse, error)
	ApplyFunc func(context.Context, *pb.ApplyRequest) (*pb.ApplyResponse, error)

	ErrCh chan error
}

// NewBroker returns a Broker instance served by a local GRPC server.
func NewBroker(c *gc.C, ctx context.Context) *Broker {
	var p = &Broker{
		c:            c,
		ReplReqCh:    make(chan *pb.ReplicateRequest),
		ReplRespCh:   make(chan *pb.ReplicateResponse),
		ReadReqCh:    make(chan *pb.ReadRequest),
		ReadRespCh:   make(chan *pb.ReadResponse),
		AppendReqCh:  make(chan *pb.AppendRequest),
		AppendRespCh: make(chan *pb.AppendResponse),
		ErrCh:        make(chan error),
	}
	p.Server = NewServer(ctx, p)
	return p
}

// Replicate implements the JournalServer interface by proxying requests &
// responses through channels ReplReqCh & ReplRespCh.
func (p *Broker) Replicate(srv pb.Journal_ReplicateServer) error {
	// Start a read loop of requests from |srv|.
	go func() {
		log.WithField("id", p.Endpoint()).Info("replicate read loop started")
		for done := false; !done; {
			var msg, err = srv.Recv()

			if err == io.EOF {
				msg, err, done = nil, nil, true
			} else if err != nil {
				done = true

				p.c.Check(err, gc.ErrorMatches, `rpc error: code = Canceled desc = context canceled`)
			}

			log.WithFields(log.Fields{"ep": p.Endpoint(), "msg": msg, "err": err, "done": done}).Info("read")

			select {
			case p.ReplReqCh <- msg:
				// Pass.
			case <-p.Ctx.Done():
				done = true
			}
		}
	}()

	for {
		select {
		case resp := <-p.ReplRespCh:
			p.c.Check(srv.Send(resp), gc.IsNil)
			log.WithFields(log.Fields{"ep": p.Endpoint(), "resp": resp}).Info("sent")
		case err := <-p.ErrCh:
			log.WithFields(log.Fields{"ep": p.Endpoint(), "err": err}).Info("closing")
			return err
		case <-p.Ctx.Done():
			log.WithFields(log.Fields{"ep": p.Endpoint()}).Info("cancelled")
			return p.Ctx.Err()
		}
	}
}

// Read implements the JournalServer interface by proxying requests & responses
// through channels ReadReqCh & ReadResponseCh.
func (p *Broker) Read(req *pb.ReadRequest, srv pb.Journal_ReadServer) error {
	select {
	case p.ReadReqCh <- req:
		// Pass.
	case <-p.Ctx.Done():
		return p.Ctx.Err()
	}

	for {
		select {
		case resp := <-p.ReadRespCh:
			srv.Send(resp) // This may return cancelled context error.
			log.WithFields(log.Fields{"ep": p.Endpoint(), "resp": resp}).Info("sent")
		case err := <-p.ErrCh:
			log.WithFields(log.Fields{"ep": p.Endpoint(), "err": err}).Info("closing")
			return err
		case <-p.Ctx.Done():
			log.WithFields(log.Fields{"ep": p.Endpoint()}).Info("cancelled")
			return p.Ctx.Err()
		}
	}
}

// Append implements the JournalServer interface by proxying requests &
// responses through channels AppendReqCh & AppendRespCh.
func (p *Broker) Append(srv pb.Journal_AppendServer) error {
	// Start a read loop of requests from |srv|.
	go func() {
		log.WithField("ep", p.Endpoint()).Info("append read loop started")
		for done := false; !done; {
			var msg, err = srv.Recv()

			if err == io.EOF {
				msg, err, done = nil, nil, true
			} else if err != nil {
				done = true

				p.c.Check(err, gc.ErrorMatches, `rpc error: code = Canceled desc = context canceled`)
			}

			log.WithFields(log.Fields{"ep": p.Endpoint(), "msg": msg, "err": err, "done": done}).Info("read")

			select {
			case p.AppendReqCh <- msg:
				// Pass.
			case <-p.Ctx.Done():
				done = true
			}
		}
	}()

	for {
		select {
		case resp := <-p.AppendRespCh:
			log.WithFields(log.Fields{"ep": p.Endpoint(), "resp": resp}).Info("sending")
			return srv.SendAndClose(resp)
		case err := <-p.ErrCh:
			log.WithFields(log.Fields{"ep": p.Endpoint(), "err": err}).Info("closing")
			return err
		case <-p.Ctx.Done():
			log.WithFields(log.Fields{"ep": p.Endpoint()}).Info("cancelled")
			return p.Ctx.Err()
		}
	}
}

// List implements the JournalServer interface by proxying through ListFunc.
func (p *Broker) List(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	return p.ListFunc(ctx, req)
}

// Apply implements the JournalServer interface by proxying through ApplyFunc.
func (p *Broker) Apply(ctx context.Context, req *pb.ApplyRequest) (*pb.ApplyResponse, error) {
	return p.ApplyFunc(ctx, req)
}

func init() { pb.RegisterGRPCDispatcher("local") }
