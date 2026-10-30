package client

import (
	"context"
	"time"

	"github.com/hashicorp/golang-lru"
	pb "go.gazette.dev/core/broker/protocol"
)

// RouteCache caches observed Routes for items (eg, Journals, or Shards).
// It implements the protocol.DispatchRouter interface, and where a cached
// Route of an item is available, it enables applications to dispatch RPCs
// directly to the most appropriate broker or consumer process. This reduces
// the overall number of network hops, and especially the number of hops
// crossing availability zones (which often cost more).
//
// For example, RouteCache can direct an application to a broker in its same
// availability zone which is replicating a desired journal, and to which a
// long-lived Read RPC can be dispatched.
//
//	// Adapt a JournalClient to a RoutedJournalClient by using a RouteCache.
//	var jc protocol.JournalClient
//	var rjc = protocol.NewRoutedJournalClient(jc, NewRouteCache(256, time.Hour))
type RouteCache struct {
	cache *lru.Cache
	ttl   time.Duration
}

// NewRouteCache returns a RouteCache of the given size (which must be > 0)
// and caching Duration.
func NewRouteCache(size int, ttl time.Duration) *RouteCache {
	var cache, err = lru.New(size)
	if err != nil {
		panic(err.Error()) // Only errors on size <= 0.
	}
	return &RouteCache{
		cache: cache,
		ttl:   ttl,
	}
}

// UpdateRoute caches the provided Route for the item, or invalidates it if
// the route is nil or empty.
func (rc *RouteCache) UpdateRoute(item string, route *pb.Route) {
	if route == nil || len(route.Members) == 0 {
		rc.cache.Remove(item)
	} else {
		var cr = cachedRoute{
			route: route.Copy(),
			at:    timeNow(),
		}
		rc.cache.Add(item, cr)
	}
}

// Route queries for a cached Route of the item.
func (rc *RouteCache) Route(_ context.Context, item string) pb.Route {
	if v, ok := rc.cache.Get(item); ok {
		// If the TTL has elapsed, treat as a cache miss and remove.
		if cr := v.(cachedRoute); cr.at.Add(rc.ttl).Before(timeNow()) {
			rc.cache.Remove(item)
		} else {
			return cr.route
		}
	}
	return pb.Route{Primary: -1}
}

// IsNoopRouter returns false.
func (rc *RouteCache) IsNoopRouter() bool { return false }

type cachedRoute struct {
	route pb.Route
	at    time.Time
}

var timeNow = time.Now
