package server

import (
	"context"
	"fmt"
	"net"
	"net/http"

	"github.com/LiveRamp/gazette/v2/pkg/keepalive"
	"github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/pkg/errors"
	"github.com/soheilhy/cmux"
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
	srv.GRPCListener = srv.CMux.Match(cmux.HTTP2HeaderField("content-type", "application/grpc"))
	srv.HTTPListener = srv.CMux.Match(cmux.HTTP1Fast())

	return srv, nil
}

// Endpoint of the Server.
func (s *Server) Endpoint() protocol.Endpoint {
	return protocol.Endpoint("http://" + s.RawListener.Addr().String())
}

// Serve the Server, including the CMux, HTTP Server, and gRPC server. The first
// encountered error is returned. If additional Listeners have been derived from
// the Server.CMux, Serve must first be called to begin serving the CMux itself,
// and then connections may be Accept()'d from the derived Listeners.
func (s *Server) Serve() error {
	var errs = make(chan error, 3)

	go func() {
		if err := s.CMux.Serve(); err != nil && s.Ctx.Err() == nil {
			errs <- err
		} else {
			errs <- nil
		}
	}()
	go func() {
		if err := http.Serve(s.HTTPListener, s.HTTPMux); err != nil && s.Ctx.Err() == nil {
			errs <- err
		} else {
			errs <- nil
		}
	}()
	go func() {
		errs <- s.GRPCServer.Serve(s.GRPCListener)
	}()

	// Block until all goroutines finish, or an error is encountered.
	for i := 0; i != 3; i++ {
		if err := <-errs; err != nil {
			return err
		}
	}
	return nil
}

// MustServe calls Serve, and panics on an encountered error.
func (s *Server) MustServe() {
	switch err := s.Serve(); err {
	case nil:
		// Normal exit after GracefulStop.
	case grpc.ErrServerStopped:
		// GracefulStop called before Serve.
	default:
		panic(err)
	}
}

// GracefulStop the Server. Attempts to Accept() connections from CMux Listeners
// may begin failing after a GracefulStop call is started. The Server.Ctx may be
// inspected to determine if the Server is stopping.
// Returns when server stop is complete.
func (s *Server) GracefulStop() {
	// GRPCServer.GracefulStop will close GRPCListener, which closes RawListener.
	// Cancel our context so Serve loops recognize this is a graceful closure.
	s.cancel()

	s.GRPCServer.GracefulStop()
}

// GRPCLoopback dials and returns a connection to the local gRPC server.
func (s *Server) GRPCLoopback() (*grpc.ClientConn, error) {
	var addr = s.RawListener.Addr().String()

	var cc, err = grpc.DialContext(s.Ctx, addr,
		grpc.WithInsecure(),
		grpc.WithDialer(keepalive.DialerFunc),
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
