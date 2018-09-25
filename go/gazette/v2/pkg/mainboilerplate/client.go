package mainboilerplate

import (
	"context"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/client"
	"github.com/LiveRamp/gazette/v2/pkg/consumer"
	"github.com/LiveRamp/gazette/v2/pkg/keepalive"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"google.golang.org/grpc"
)

// AddressConfig of a remote service.
type AddressConfig struct {
	Address pb.Endpoint `long:"address" env:"ADDRESS" default:"http://localhost:8080" description:"Service address endpoint"`
}

// Dial the server address using a protocol.Dispatcher balancer.
func (c *AddressConfig) Dial(ctx context.Context) *grpc.ClientConn {
	var cc, err = grpc.DialContext(ctx, c.Address.URL().Host,
		grpc.WithInsecure(),
		grpc.WithDialer(keepalive.DialerFunc),
		grpc.WithBalancerName(pb.DispatcherGRPCBalancerName))
	Must(err, "failed to dial remote service", "endpoint", c.Address)

	return cc
}

// JournalClient dials and returns a new JournalClient.
func (c *AddressConfig) JournalClient(ctx context.Context) pb.JournalClient {
	return pb.NewJournalClient(c.Dial(ctx))
}

// ShardClient dials and returns a new ShardClient.
func (c *AddressConfig) ShardClient(ctx context.Context) consumer.ShardClient {
	return consumer.NewShardClient(c.Dial(ctx))
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

// RoutedJournalClient composes Dial and BuildRouter to return a RoutedJournalClient.
func (c *ClientConfig) RoutedJournalClient(ctx context.Context) pb.RoutedJournalClient {
	return pb.NewRoutedJournalClient(c.JournalClient(ctx), c.BuildRouter())
}

// RoutedShardClient composes Dial and BuildRouter to return a RoutedShardClient.
func (c *ClientConfig) RoutedShardClient(ctx context.Context) consumer.RoutedShardClient {
	return consumer.NewRoutedShardClient(c.ShardClient(ctx), c.BuildRouter())
}
