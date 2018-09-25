package teststub

import (
	"context"
	"io"
	"net"

	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	gc "github.com/go-check/check"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// Server wraps a protocol.JournalServer with a gRPC server for use within tests.
type Server struct {
	c        *gc.C
	ctx      context.Context
	listener net.Listener
	srv      *grpc.Server
}

// NewServer returns a local GRPC server wrapping the provided BrokerServer.
func NewServer(c *gc.C, ctx context.Context, srv pb.JournalServer) *Server {
	var l, err = net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, gc.IsNil)

	var p = &Server{
		c:        c,
		ctx:      ctx,
		listener: l,
		srv:      grpc.NewServer(),
	}

	pb.RegisterJournalServer(p.srv, srv)
	go p.srv.Serve(p.listener)

	go func() {
		<-ctx.Done()
		p.srv.GracefulStop()
	}()

	return p
}

func (s *Server) Endpoint() pb.Endpoint {
	return pb.Endpoint("http://" + s.listener.Addr().String() + "/path")
}

func (s *Server) Dial(ctx context.Context) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, s.listener.Addr().String(),
		grpc.WithInsecure(),
		grpc.WithBalancerName(pb.DispatcherGRPCBalancerName))
}

func (s *Server) MustConn() *grpc.ClientConn {
	var conn, err = s.Dial(s.ctx)
	s.c.Assert(err, gc.IsNil)
	return conn
}

func (s *Server) MustClient() pb.JournalClient {
	return pb.NewJournalClient(s.MustConn())
}

// Broker stubs the read and write loops of broker RPCs, routing them onto
// channels which can be synchronously read and written within test bodies.
type Broker struct {
	*Server

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
		ReplReqCh:    make(chan *pb.ReplicateRequest),
		ReplRespCh:   make(chan *pb.ReplicateResponse),
		ReadReqCh:    make(chan *pb.ReadRequest),
		ReadRespCh:   make(chan *pb.ReadResponse),
		AppendReqCh:  make(chan *pb.AppendRequest),
		AppendRespCh: make(chan *pb.AppendResponse),
		ErrCh:        make(chan error),
	}
	p.Server = NewServer(c, ctx, p)
	return p
}

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
			case <-p.ctx.Done():
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
		case <-p.ctx.Done():
			log.WithFields(log.Fields{"ep": p.Endpoint()}).Info("cancelled")
			return p.ctx.Err()
		}
	}
}

func (p *Broker) Read(req *pb.ReadRequest, srv pb.Journal_ReadServer) error {
	select {
	case p.ReadReqCh <- req:
		// Pass.
	case <-p.ctx.Done():
		return p.ctx.Err()
	}

	for {
		select {
		case resp := <-p.ReadRespCh:
			srv.Send(resp) // This may return cancelled context error.
			log.WithFields(log.Fields{"ep": p.Endpoint(), "resp": resp}).Info("sent")
		case err := <-p.ErrCh:
			log.WithFields(log.Fields{"ep": p.Endpoint(), "err": err}).Info("closing")
			return err
		case <-p.ctx.Done():
			log.WithFields(log.Fields{"ep": p.Endpoint()}).Info("cancelled")
			return p.ctx.Err()
		}
	}
}

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
			case <-p.ctx.Done():
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
		case <-p.ctx.Done():
			log.WithFields(log.Fields{"ep": p.Endpoint()}).Info("cancelled")
			return p.ctx.Err()
		}
	}
}

func (p *Broker) List(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	return p.ListFunc(ctx, req)
}

func (p *Broker) Apply(ctx context.Context, req *pb.ApplyRequest) (*pb.ApplyResponse, error) {
	return p.ApplyFunc(ctx, req)
}

func init() { pb.RegisterGRPCDispatcher("local") }
