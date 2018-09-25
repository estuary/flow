package client

import (
	"context"
	"fmt"
	"time"

	"github.com/hashicorp/golang-lru"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

// Dialer dials processes identified by Routes, caching ready connections of those previously dialed.
type Dialer struct {
	cache *lru.Cache
}

// NewDialer builds and returns a dialer.
func NewDialer(size int) (Dialer, error) {
	var cache, err = lru.NewWithEvict(size, func(key, value interface{}) {
		if err := value.(*grpc.ClientConn).Close(); err != nil {
			log.WithFields(log.Fields{"broker": key, "err": err}).
				Warn("failed to Close evicted grpc.ClientConn")
		}
	})
	return Dialer{cache: cache}, err
}

func (d Dialer) Dial(ctx context.Context, id pb.ProcessSpec_ID, route pb.Route) (*grpc.ClientConn, error) {
	var ind int
	for ind = 0; ind != len(route.Members) && route.Members[ind] != id; ind++ {
	}

	if ind == len(route.Members) {
		return nil, fmt.Errorf("no such Member in Route (id: %s, route: %s)", id.String(), route.String())
	} else if len(route.Endpoints) != len(route.Members) || route.Endpoints[ind] == "" {
		return nil, fmt.Errorf("missing Route Endpoints (id: %s, route: %s)", id.String(), route.String())
	}

	// We perform the cache check explicitly _after_ examining Route, to prevent
	// development errors which appear as transient bugs due to caching effects.
	if v, ok := d.cache.Get(id); ok {
		return v.(*grpc.ClientConn), nil
	}
	var conn, err = d.DialEndpoint(ctx, route.Endpoints[ind])

	if err == nil {
		d.cache.Add(id, conn)
	}
	return conn, err
}

func (d Dialer) DialEndpoint(ctx context.Context, ep pb.Endpoint) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, ep.URL().Host,
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: time.Second * 30}),
		grpc.WithInsecure(),
	)
}
