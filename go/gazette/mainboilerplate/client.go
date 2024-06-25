package mainboilerplate

import (
	"context"
	"fmt"
	"math"
	"time"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"go.gazette.dev/core/auth"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

// AddressConfig of a remote service.
type AddressConfig struct {
	Address       pb.Endpoint `long:"address" env:"ADDRESS" default:"http://localhost:8080" description:"Service address endpoint"`
	CertFile      string      `long:"cert-file" env:"CERT_FILE" default:"" description:"Path to the client TLS certificate"`
	CertKeyFile   string      `long:"cert-key-file" env:"CERT_KEY_FILE" default:"" description:"Path to the client TLS private key"`
	TrustedCAFile string      `long:"trusted-ca-file" env:"TRUSTED_CA_FILE" default:"" description:"Path to the trusted CA for client verification of server certificates"`
	AuthKeys      string      `long:"auth-keys" env:"AUTH_KEYS" description:"Whitespace or comma separated, base64-encoded keys. The first key is used to sign Authorization tokens." json:"-"`
}

// MustDial dials the server address using a protocol.Dispatcher balancer, and panics on error.
func (c *AddressConfig) MustDial(ctx context.Context) *grpc.ClientConn {
	var tlsConfig, err = server.BuildTLSConfig(c.CertFile, c.CertKeyFile, c.TrustedCAFile)
	Must(err, "failed to build TLS config")

	// Use a tighter bound for the maximum back-off delay (default is 120s).
	var backoffConfig = backoff.DefaultConfig
	backoffConfig.MaxDelay = 5 * time.Second

	cc, err := grpc.DialContext(ctx,
		c.Address.GRPCAddr(),
		grpc.WithTransportCredentials(pb.NewDispatchedCredentials(tlsConfig, c.Address)),
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoffConfig}),
		// A single Gazette broker frequently serves LOTS of Journals.
		// Readers will start many concurrent reads of various journals,
		// but may process them in arbitrary orders, which means a journal
		// stream could be "readable" and have available stream-level flow control,
		// but still not send data because the connection-level flow control window
		// is filled. So, effectively disable connection-level flow control and use
		// only stream-level flow control.
		grpc.WithInitialConnWindowSize(math.MaxInt32),
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingConfig": [{"%s":{}}]}`, pb.DispatcherGRPCBalancerName)),
		// Instrument client for gRPC metric collection.
		grpc.WithUnaryInterceptor(grpc_prometheus.UnaryClientInterceptor),
		grpc.WithStreamInterceptor(grpc_prometheus.StreamClientInterceptor),
	)
	Must(err, "failed to dial remote service", "endpoint", c.Address)

	return cc
}

// MustJournalClient dials and returns a new JournalClient.
func (c *AddressConfig) MustJournalClient(ctx context.Context) pb.JournalClient {
	var authorizer pb.Authorizer
	var err error

	if c.AuthKeys != "" {
		authorizer, err = auth.NewKeyedAuth(c.AuthKeys)
		Must(err, "parsing authorization keys")
	} else {
		authorizer = auth.NewNoopAuth()
	}

	var conn = c.MustDial(ctx)
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	var jc = pb.NewJournalClient(conn)
	return pb.NewAuthJournalClient(jc, authorizer)
}

// MustShardClient dials and returns a new ShardClient.
func (c *AddressConfig) MustShardClient(ctx context.Context) pc.ShardClient {
	var authorizer pb.Authorizer
	var err error

	if c.AuthKeys != "" {
		authorizer, err = auth.NewKeyedAuth(c.AuthKeys)
		Must(err, "parsing authorization keys")
	} else {
		authorizer = auth.NewNoopAuth()
	}

	var conn = c.MustDial(ctx)
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	var sc = pc.NewShardClient(conn)
	return pc.NewAuthShardClient(sc, authorizer)
}

// ClientConfig configures the client of a remote Gazette service.
type ClientConfig struct {
	AddressConfig

	Cache struct {
		Size int           `long:"cache.size" env:"CACHE_SIZE" default:"0" description:"Size of client route cache. If <= zero, no cache is used (server always proxies)"`
		TTL  time.Duration `long:"cache.ttl" env:"CACHE_TTL" default:"1m" description:"Time-to-live of route cache entries."`
	}
}

// BuildRouter returns a configured DispatchRouter.
func (c *ClientConfig) BuildRouter() pb.DispatchRouter {
	if c.Cache.Size <= 0 {
		return pb.NoopDispatchRouter{}
	}
	return client.NewRouteCache(c.Cache.Size, c.Cache.TTL)
}

// MustRoutedJournalClient composes MustDial and BuildRouter to return a RoutedJournalClient.
func (c *ClientConfig) MustRoutedJournalClient(ctx context.Context) pb.RoutedJournalClient {
	return pb.NewRoutedJournalClient(c.MustJournalClient(ctx), c.BuildRouter())
}

// MustRoutedShardClient composes MustDial and BuildRouter to return a RoutedShardClient.
func (c *ClientConfig) MustRoutedShardClient(ctx context.Context) pc.RoutedShardClient {
	return pc.NewRoutedShardClient(c.MustShardClient(ctx), c.BuildRouter())
}
