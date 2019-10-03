package client

import (
	"context"
	"time"

	gc "github.com/go-check/check"
	pb "go.gazette.dev/core/broker/protocol"
)

type RouteCacheSuite struct{}

func (s *RouteCacheSuite) TestCachingCases(c *gc.C) {
	defer func(f func() time.Time) { timeNow = f }(timeNow)

	var fixedtime int64 = 1000
	timeNow = func() time.Time { return time.Unix(fixedtime, 0) }

	var ctx = context.Background()
	var rc = NewRouteCache(3, time.Minute)

	for _, s := range []string{"A", "B", "C", "D"} {
		rc.UpdateRoute(s, buildRouteFixture(s))
	}
	c.Check(rc.cache.Len(), gc.Equals, 3)

	// Case: Cached routes are returned.
	c.Check(rc.Route(ctx, "D"), gc.DeepEquals, *buildRouteFixture("D")) // Hit.

	// Case: Routes which have fallen out of cache are not.
	c.Check(rc.Route(ctx, "A"), gc.DeepEquals, pb.Route{Primary: -1}) // Miss.

	// Case: Nil or empty routes invalidate the cache.
	rc.UpdateRoute("C", nil)
	rc.UpdateRoute("C", new(pb.Route))
	c.Check(rc.Route(ctx, "C"), gc.DeepEquals, pb.Route{Primary: -1}) // Miss.

	// Case: TTLs are enforced.
	fixedtime += 31
	rc.UpdateRoute("B", buildRouteFixture("B"))

	// Precondition: both B and D are cached.
	c.Check(rc.Route(ctx, "B"), gc.DeepEquals, *buildRouteFixture("B"))
	c.Check(rc.Route(ctx, "D"), gc.DeepEquals, *buildRouteFixture("D"))

	fixedtime += 30

	// TTL for D has elapsed, but not for B.
	c.Check(rc.Route(ctx, "B"), gc.DeepEquals, *buildRouteFixture("B"))
	c.Check(rc.Route(ctx, "D"), gc.DeepEquals, pb.Route{Primary: -1})
}

func buildRouteFixture(id string) *pb.Route {
	return &pb.Route{
		Primary: 0,
		Members: []pb.ProcessSpec_ID{{Zone: id, Suffix: id}},
	}
}

var _ = gc.Suite(&RouteCacheSuite{})
