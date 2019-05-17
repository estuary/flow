package server

import (
	"context"
	"fmt"
	"net"
	"net/http"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/soheilhy/cmux"
	"go.gazette.dev/core/keepalive"
	"go.gazette.dev/core/protocol"
	"go.gazette.dev/core/task"
	"google.golang.org/grpc"
)

// Server bundles gRPC & HTTP servers, multiplexed over a single bound TCP
// socket (using CMux). Additional protocols may be added to the Server by
// interacting directly with its provided CMux.
type Server struct {
	// RawListener is the bound TCP listener of the Server.
	RawListener *net.TCPListener
	// CMux wraps RawListener to provide connection protocol multiplexing over
	// a single bound socket. gRPC and HTTP Listeners are provided by default.
	// Additional Listeners may be added directly via CMux.Match() -- though
	// it is then the user's responsibility to Serve the resulting Listeners.
	CMux cmux.CMux
	// GRPCListener is a CMux Listener for gRPC connections.
	GRPCListener net.Listener
	// HTTPListener is a CMux Listener for HTTP connections.
	HTTPListener net.Listener
	// HTTPMux is the http.ServeMux which is served by Serve().
	HTTPMux *http.ServeMux
	// GRPCServer is the gRPC server mux which is served by Serve().
	GRPCServer *grpc.Server
	// Ctx is cancelled when Server.GracefulStop is called.
	Ctx context.Context

	cancel context.CancelFunc
}

// New builds and returns a Server of the given TCP network interface |iface|
// and |port|. |port| may be zero, in which case a random free port is assigned.
func New(iface string, port uint16) (*Server, error) {
	var addr = fmt.Sprintf("%s:%d", iface, port)

	var raw, err = net.Listen("tcp", addr)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to bind service address (%s)", addr)
	}

	var ctx, cancel = context.WithCancel(context.Background())

	var srv = &Server{
		HTTPMux:     http.DefaultServeMux,
		GRPCServer:  grpc.NewServer(),
		RawListener: raw.(*net.TCPListener),
		Ctx:         ctx,
		cancel:      cancel,
	}

	srv.CMux = cmux.New(keepalive.TCPListener{TCPListener: srv.RawListener})

	srv.CMux.HandleError(func(err error) bool {
		if _, ok := err.(net.Error); !ok {
			log.WithField("err", err).Warn("failed to CMux client connection to a listener")
		}
		return true // Continue serving RawListener.
	})

	// GRPCListener sniffs for HTTP/2 in-the-clear connections which have
	// "Content-Type: application/grpc". Note this matcher will send an initial
	// empty SETTINGS frame to the client, as gRPC clients delay the first
	// request until the HTTP/2 handshake has completed.
	srv.GRPCListener = srv.CMux.MatchWithWriters(
		cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))

	// Connections sending HTTP/1 verbs (GET, PUT, POST etc) are assumed to be HTTP.
	srv.HTTPListener = srv.CMux.Match(cmux.HTTP1Fast())

	return srv, nil
}

// Endpoint of the Server.
func (s *Server) Endpoint() protocol.Endpoint {
	return protocol.Endpoint("http://" + s.RawListener.Addr().String())
}

// QueueTasks serving the CMux, HTTP, and gRPC component servers onto the task.Group.
// If additional Listeners are derived from the Server.CMux, attempts to Accept
// will block until the CMux itself begins serving.
func (s *Server) QueueTasks(tg *task.Group) {
	tg.Queue("CMux.Serve", func() error {
		if err := s.CMux.Serve(); err != nil && s.Ctx.Err() == nil {
			return err
		}
		return nil // Swallow error after GracefulStop.
	})
	tg.Queue("http.Serve", func() error {
		if err := http.Serve(s.HTTPListener, s.HTTPMux); err != nil && s.Ctx.Err() == nil {
			return err
		}
		return nil // Swallow error after GracefulStop.
	})
	tg.Queue("GRPCServer.Serve", func() error {
		if err := s.GRPCServer.Serve(s.GRPCListener); err != grpc.ErrServerStopped {
			return err
		}
		return nil // GracefulStop was called before GoServe.
	})
	tg.Queue("GRPCServer.GracefulStop", func() error {
		<-tg.Context().Done() // Block until task.Group is cancelled.

		// GRPCServer.GracefulStop will close GRPCListener, which closes RawListener.
		// Cancel |s.Ctx| so Serve loops and dialed loopback clients recognize this
		// as a graceful closure.
		s.cancel()

		s.GRPCServer.GracefulStop()
		return nil
	})
}

// GRPCLoopback dials and returns a connection to the local gRPC server.
func (s *Server) GRPCLoopback() (*grpc.ClientConn, error) {
	var addr = s.RawListener.Addr().String()

	var cc, err = grpc.DialContext(s.Ctx, addr,
		grpc.WithInsecure(),
		grpc.WithContextDialer(keepalive.DialerFunc),
		grpc.WithBalancerName(protocol.DispatcherGRPCBalancerName))

	if err != nil {
		return nil, err
	}
	return cc, nil
}

// MustGRPCLoopback dials and returns a connection to the local gRPC server,
// and panics on error.
func (s *Server) MustGRPCLoopback() *grpc.ClientConn {
	if cc, err := s.GRPCLoopback(); err != nil {
		panic(err)
	} else {
		return cc
	}
}
