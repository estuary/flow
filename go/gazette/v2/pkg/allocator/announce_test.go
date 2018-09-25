package v3_allocator

import (
	"context"
	"time"

	"github.com/coreos/etcd/clientv3/concurrency"
	gc "github.com/go-check/check"
)

type AnnounceSuite struct{}

func (s *AnnounceSuite) TestAnnounceUpdateAndExpire(c *gc.C) {
	var client = etcdCluster.RandClient()
	var ctx = context.Background()
	const key = "/announce/key"

	var session, err = concurrency.NewSession(client, concurrency.WithTTL(5))
	c.Check(err, gc.IsNil)

	a, err := Announce(ctx, client, key, "val-1", session.Lease())
	c.Check(err, gc.IsNil)

	c.Check(a.Update(ctx, "val-2"), gc.IsNil)
	c.Check(a.Update(ctx, "val-3"), gc.IsNil)

	c.Check(session.Close(), gc.IsNil)

	resp, err := client.Get(context.Background(), key)
	c.Check(err, gc.IsNil)
	c.Check(resp.Count, gc.Equals, int64(0))

	c.Check(a.Update(ctx, "val-4"), gc.ErrorMatches,
		`key modified or deleted externally \(expected revision \d+\)`)
}

func (s *AnnounceSuite) TestAnnounceConflict(c *gc.C) {
	var client = etcdCluster.RandClient()
	var ctx, cancel = context.WithCancel(context.Background())
	const key = "/announce/key"

	var session, err = concurrency.NewSession(client, concurrency.WithTTL(5))
	c.Check(err, gc.IsNil)
	defer session.Close()

	_, err = Announce(ctx, client, key, "val-other", session.Lease())
	c.Check(err, gc.IsNil)

	// Temporarily set the retry interval to a very short duration.
	defer func(d time.Duration) {
		announceConflictRetryInterval = d
	}(announceConflictRetryInterval)

	// Expect that Announce retries until the context is cancelled.
	announceConflictRetryInterval = time.Millisecond
	time.AfterFunc(10*announceConflictRetryInterval, cancel)

	_, err = Announce(ctx, client, key, "val-other", session.Lease())
	c.Check(err, gc.Equals, context.Canceled)
}

var _ = gc.Suite(&AnnounceSuite{})
