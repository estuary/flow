package mainboilerplate

import (
	"context"
	"time"

	"go.gazette.dev/core/client"
	"go.gazette.dev/core/consumer"
	"go.gazette.dev/core/keepalive"
	pb "go.gazette.dev/core/protocol"
	"google.golang.org/grpc"
)

// AddressConfig of a remote service.
type AddressConfig struct {
	Address pb.Endpoint `long:"address" env:"ADDRESS" default:"http://localhost:8080" description:"Service address endpoint"`
}

// Dial the server address using a protocol.Dispatcher balancer.
func (c *AddressConfig) MustDial(ctx context.Context) *grpc.ClientConn {
	var cc, err = grpc.DialContext(ctx, c.Address.URL().Host,
		grpc.WithInsecure(),
		grpc.WithContextDialer(keepalive.DialerFunc),
		grpc.WithBalancerName(pb.DispatcherGRPCBalancerName))
	Must(err, "failed to dial remote service", "endpoint", c.Address)

	return cc
}

// MustJournalClient dials and returns a new JournalClient.
func (c *AddressConfig) MustJournalClient(ctx context.Context) pb.JournalClient {
	return pb.NewJournalClient(c.MustDial(ctx))
}

// MustShardClient dials and returns a new ShardClient.
func (c *AddressConfig) MustShardClient(ctx context.Context) consumer.ShardClient {
	return consumer.NewShardClient(c.MustDial(ctx))
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
func (c *ClientConfig) MustRoutedShardClient(ctx context.Context) consumer.RoutedShardClient {
	return consumer.NewRoutedShardClient(c.MustShardClient(ctx), c.BuildRouter())
}
