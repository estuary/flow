package mainboilerplate

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/LiveRamp/gazette/v2/pkg/allocator"
	"github.com/LiveRamp/gazette/v2/pkg/keepalive"
	"github.com/LiveRamp/gazette/v2/pkg/keyspace"
	"github.com/LiveRamp/gazette/v2/pkg/protocol"
	log "github.com/sirupsen/logrus"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
)

// ZoneConfig configures the zone of the application.
type ZoneConfig struct {
	Zone string `long:"zone" env:"ZONE" default:"local" description:"Availability zone within which this process is running"`
}

// ServiceConfig represents identification and addressing configuration of the process.
type ServiceConfig struct {
	ZoneConfig
	ID   string `long:"id" env:"ID" default:"localhost" description:"Unique ID of the process"`
	Host string `long:"host" env:"HOST" default:"localhost" description:"Addressable, advertised hostname of this process"`
	Port uint16 `long:"port" env:"PORT" default:"8080" description:"Service port for HTTP and gRPC requests"`
}

// ProcessSpec of the ServiceConfig.
func (cfg ServiceConfig) ProcessSpec() protocol.ProcessSpec {
	return protocol.ProcessSpec{
		Id:       protocol.ProcessSpec_ID{Zone: cfg.Zone, Suffix: cfg.ID},
		Endpoint: protocol.Endpoint(fmt.Sprintf("http://%s:%d", cfg.Host, cfg.Port)),
	}
}

// MemberKey of an allocator implied by the ServiceConfig.
func (cfg ServiceConfig) MemberKey(ks *keyspace.KeySpace) string {
	return allocator.MemberKey(ks, cfg.Zone, cfg.ID)
}

// ServerContext collects an http.ServeMux & grpc.Server, as well as the
// bound net.Listeners which they serve.
type ServerContext struct {
	HTTPMux    *http.ServeMux
	GRPCServer *grpc.Server

	RawListener  *net.TCPListener
	CMux         cmux.CMux
	GRPCListener net.Listener
	HTTPListener net.Listener

	ctx    context.Context
	cancel context.CancelFunc
}

// MustBuildServer builds and returns a ServerContext from the ServiceConfig.
func MustBuildServer(cfg ServiceConfig) ServerContext {
	var raw, err = net.Listen("tcp", fmt.Sprintf(":%d", cfg.Port))
	Must(err, "failed to bind local service address", "port", cfg.Port)

	var ctx, cancel = context.WithCancel(context.Background())

	var sl = ServerContext{
		HTTPMux:     http.DefaultServeMux,
		GRPCServer:  grpc.NewServer(),
		RawListener: raw.(*net.TCPListener),
		ctx:         ctx,
		cancel:      cancel,
	}

	sl.CMux = cmux.New(keepalive.TCPListener{TCPListener: sl.RawListener})
	sl.GRPCListener = sl.CMux.Match(cmux.HTTP2HeaderField("content-type", "application/grpc"))
	sl.HTTPListener = sl.CMux.Match(cmux.HTTP1Fast())
	return sl
}

// Serve the ServerContext. Fatals if the cmux.CMux, grpc.Server,
// or http.ServeMux return an error without the Context also having
// been cancelled.
func (c *ServerContext) Serve() {
	go func() {
		if err := c.CMux.Serve(); err != nil && c.ctx.Err() == nil {
			Must(err, "cmux.Serve failed")
		}
	}()
	go func() {
		if err := http.Serve(c.HTTPListener, c.HTTPMux); err != nil && c.ctx.Err() == nil {
			Must(err, "http.Serve failed")
		}
	}()
	Must(c.GRPCServer.Serve(c.GRPCListener), "grpc.Serve failed")
}

func (c *ServerContext) GracefulStop() {
	// GRPCServer.GracefulStop will close GRPCListener, which closes RawListener.
	// Cancel our context so Serve loops recognize this is a graceful closure.
	c.cancel()

	c.GRPCServer.GracefulStop()
}

// Loopback dials and returns a connection to the local gRPC server.
func (c *ServerContext) Loopback() *grpc.ClientConn {
	var addr = c.RawListener.Addr().String()

	var cc, err = grpc.DialContext(c.ctx, addr,
		grpc.WithInsecure(),
		grpc.WithDialer(keepalive.DialerFunc),
		grpc.WithBalancerName(protocol.DispatcherGRPCBalancerName))

	if err != nil {
		log.WithFields(log.Fields{"addr": addr, "err": err}).Fatal("failed to dial service loopback")
	}
	return cc
}

type memberSpec interface {
	MarshalString() string
	ZeroLimit()
	Validate() error
}

// AnnounceServeAndAllocate will announce the |spec| to |etcd|, begin
// asynchronously serving the ServerContext, and synchronously run
// allocator.Allocate. It installs a signal handler which zeros the |spec| item
// limit and updates the announcement, causing the Allocate to gracefully exit.
// This is the principal service loop of gazette brokers and consumers.
func AnnounceServeAndAllocate(etcd EtcdContext, srv ServerContext, state *allocator.State, spec memberSpec) {
	Must(spec.Validate(), "member specification validation error")

	var ann = allocator.Announce(etcd.Etcd, state.LocalKey, spec.MarshalString(), etcd.Session.Lease())
	Must(state.KS.Load(context.Background(), etcd.Etcd, ann.Revision), "failed to load KeySpace")

	// Register a signal handler which zeros our advertised limit in Etcd.
	// Upon seeing this, Allocator will work to discharge all of our assigned
	// items, and Allocate will exit gracefully when none remain.
	var signalCh = make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		var sig = <-signalCh
		log.WithField("signal", sig).Info("caught signal")

		spec.ZeroLimit()
		Must(ann.Update(spec.MarshalString()), "failed to update member announcement", "key", state.LocalKey)
	}()

	// Now that the KeySpace has been loaded, we can begin serving requests.
	go srv.Serve()
	go func() { Must(state.KS.Watch(context.Background(), etcd.Etcd), "keyspace Watch failed") }()

	Must(allocator.Allocate(allocator.AllocateArgs{
		Context: context.Background(),
		Etcd:    etcd.Etcd,
		State:   state,
	}), "Allocate failed")

	// Close our session to remove our member key. If we were leader,
	// this notifies peers that we are no longer allocating.
	etcd.Session.Close()
	srv.GracefulStop()
}
