package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/soheilhy/cmux"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/task"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Server bundles gRPC & HTTP servers, multiplexed over a single bound TCP
// socket (using CMux). Additional protocols may be added to the Server by
// interacting directly with its provided CMux.
type Server struct {
	// RawListener is the bound listener of the Server.
	RawListener net.Listener
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
	// GRPCLoopback is a dialed connection to this GRPCServer.
	GRPCLoopback *grpc.ClientConn

	httpServer http.Server
}

// New builds and returns a Server of the given TCP network interface |iface|
// and |port|. |port| may be empty, in which case a random free port is assigned.
func New(iface string, port string) (*Server, error) {
	var network, addr string
	if port == "" {
		network, addr = "tcp", fmt.Sprintf("%s:0", iface) // Assign a random free port.
	} else if u, err := url.Parse(port); err == nil && u.Scheme == "unix" {
		network, addr = "unix", u.Path
	} else {
		network, addr = "tcp", fmt.Sprintf("%s:%s", iface, port)
	}

	var raw, err = net.Listen(network, addr)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to bind service address (%s)", addr)
	}

	var srv = &Server{
		HTTPMux: http.DefaultServeMux,
		GRPCServer: grpc.NewServer(
			grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
			grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
		),
		RawListener: raw,
	}
	srv.CMux = cmux.New(srv.RawListener)

	srv.CMux.HandleError(func(err error) bool {
		if _, ok := err.(net.Error); !ok {
			log.WithField("err", err).Warn("failed to CMux client connection to a listener")
		}
		return true // Continue serving RawListener.
	})
	// CMux ReadTimeout controls how long we'll wait for an opening send from
	// the client which allows CMux to sniff a matching listening mux. It has
	// no effect once the connection has been matched to a mux.
	// See: https://github.com/soheilhy/cmux/issues/76
	srv.CMux.SetReadTimeout(GracefulStopTimeout / 2)

	// GRPCListener sniffs for HTTP/2 in-the-clear connections which have
	// "Content-Type: application/grpc". Note this matcher will send an initial
	// empty SETTINGS frame to the client, as gRPC clients delay the first
	// request until the HTTP/2 handshake has completed.
	srv.GRPCListener = srv.CMux.MatchWithWriters(
		cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))

	srv.GRPCLoopback, err = grpc.DialContext(
		context.Background(),
		srv.RawListener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingConfig": [{"%s":{}}]}`, pb.DispatcherGRPCBalancerName)),
		// This grpc.ClientConn connects to this server's loopback, and also
		// to peer server addresses via the dispatch balancer. It has particular
		// knowledge of what addresses *should* be reach-able (from Etcd
		// advertisements). Use an aggressive back-off for server-to-server
		// connections, as it's crucial for quick cluster recovery from
		// partitions, etc.
		grpc.WithBackoffMaxDelay(time.Millisecond*500),
		// Instrument client for gRPC metric collection.
		grpc.WithUnaryInterceptor(grpc_prometheus.UnaryClientInterceptor),
		grpc.WithStreamInterceptor(grpc_prometheus.StreamClientInterceptor),
	)

	if err != nil {
		return nil, errors.Wrapf(err, "failed to dial gRPC loopback")
	}

	// Connections sending HTTP/1 verbs (GET, PUT, POST etc) are assumed to be HTTP.
	srv.HTTPListener = srv.CMux.Match(cmux.HTTP1Fast())

	return srv, nil
}

// MustLoopback builds and returns a new Server instance bound to a random
// port on the loopback interface. It panics on error.
func MustLoopback() *Server {
	if srv, err := New("127.0.0.1", ""); err != nil {
		log.WithField("err", err).Panic("failed to build Server")
		panic("not reached")
	} else {
		return srv
	}
}

// Endpoint of the Server.
func (s *Server) Endpoint() pb.Endpoint {
	return pb.Endpoint("http://" + s.RawListener.Addr().String())
}

// QueueTasks serving the CMux, HTTP, and gRPC component servers onto the task.Group.
// If additional Listeners are derived from the Server.CMux, attempts to Accept
// will block until the CMux itself begins serving.
func (s *Server) QueueTasks(tg *task.Group) {
	tg.Queue("server.ServeCMux", func() error {
		if err := s.CMux.Serve(); err != nil && tg.Context().Err() == nil {
			return err
		}
		return nil // Swallow error on cancellation.
	})
	tg.Queue("server.ServeHTTP", func() error {
		// Disable Close() of the HTTPListener, because http.Server Shutdown()
		// is invoked after grpc.Server GracefulStop(), which has already closed
		// the underlying listener.
		var ln = noopCloser{s.HTTPListener}

		s.httpServer.Handler = s.HTTPMux
		if err := s.httpServer.Serve(ln); err != nil && tg.Context().Err() == nil {
			return err
		}
		return nil // Swallow error on cancellation.
	})
	tg.Queue("server.ServeGRPC", func() error {
		if err := s.GRPCServer.Serve(s.GRPCListener); err != grpc.ErrServerStopped {
			return err
		}
		return nil
	})
}

// BoundedGracefulStop attempts to perform a graceful stop of the server,
// but falls back to a hard stop if the graceful stop doesn't complete
// reasonably quickly.
func (s *Server) BoundedGracefulStop() {
	var ctx, cancel = context.WithCancel(context.Background())
	var timer = time.AfterFunc(GracefulStopTimeout, func() {
		log.Error("grpc.GracefulStop took too long, issuing a hard Stop")

		// Close loopback even though the server isn't stopped, to unblock any
		// requests which may be wedged sending to an unresponsive peer.
		_ = s.GRPCLoopback.Close()

		s.GRPCServer.Stop()
		cancel()
	})
	// GracefulStop immediately closes the underlying RawListener.
	s.GRPCServer.GracefulStop()

	// Shutdown causes httpServer.Serve to return immediately.
	if err := s.httpServer.Shutdown(ctx); err != nil {
		log.WithField("err", err).Error("http.Server Shutdown finished with error")
	}
	timer.Stop()
}

type noopCloser struct {
	net.Listener
}

func (noopCloser) Close() error { return nil }

// GracefulStopTimeout is the amount of time BoundedGracefulStop will wait
// before performing a hard server Stop.
var GracefulStopTimeout = 15 * time.Second
