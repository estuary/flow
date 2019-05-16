package client

import (
	"context"
	"time"

	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/hashicorp/golang-lru"
)

// RouteCache caches observed Routes for JournalSpecs (and consumer
// ShardSpecs, or any other allocator.Item)
type RouteCache struct {
	cache lru.Cache
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
		cache: *cache,
		ttl:   ttl,
	}
}

// UpdateRoute caches the provided Route for |item|, or invalidates it if |route| is nil.
func (rc *RouteCache) UpdateRoute(item string, route *pb.Route) {
	if route == nil {
		rc.cache.Remove(item)
	} else {
		var cr = cachedRoute{
			route: *route,
			at:    timeNow(),
		}
		rc.cache.Add(item, cr)
	}
}

// Route queries for a cached Route of |item|.
func (rc *RouteCache) Route(ctx context.Context, item string) pb.Route {
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
