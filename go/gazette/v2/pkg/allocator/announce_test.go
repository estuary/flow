package allocator

import (
	"context"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/etcdtest"
	"github.com/coreos/etcd/clientv3/concurrency"
	gc "github.com/go-check/check"
)

type AnnounceSuite struct{}

func (s *AnnounceSuite) TestAnnounceUpdateAndExpire(c *gc.C) {
	var client = etcdtest.TestClient()
	defer etcdtest.Cleanup()
	const key = "/announce/key"

	var session, err = concurrency.NewSession(client, concurrency.WithTTL(5))
	c.Check(err, gc.IsNil)

	var a = Announce(client, key, "val-1", session.Lease())
	c.Check(a.Update("val-2"), gc.IsNil)
	c.Check(a.Update("val-3"), gc.IsNil)

	c.Check(session.Close(), gc.IsNil)

	resp, err := client.Get(context.Background(), key)
	c.Check(err, gc.IsNil)
	c.Check(resp.Count, gc.Equals, int64(0))

	c.Check(a.Update("val-4"), gc.ErrorMatches,
		`key modified or deleted externally \(expected revision \d+\)`)
}

func (s *AnnounceSuite) TestAnnounceConflict(c *gc.C) {
	var client = etcdtest.TestClient()
	defer etcdtest.Cleanup()
	const key = "/announce/key"

	var session1, err = concurrency.NewSession(client, concurrency.WithTTL(5))
	c.Check(err, gc.IsNil)

	Announce(client, key, "value1", session1.Lease())

	// Temporarily set the retry interval to a very short duration.
	defer func(d time.Duration) {
		announceConflictRetryInterval = d
	}(announceConflictRetryInterval)

	announceConflictRetryInterval = time.Millisecond

	session2, err := concurrency.NewSession(client, concurrency.WithTTL(5))
	c.Check(err, gc.IsNil)
	defer session2.Close()

	// Expect that Announce retries until the prior lease is revoked,
	// and it can announce its value.
	time.AfterFunc(10*announceConflictRetryInterval, func() {
		c.Check(session1.Close(), gc.IsNil) // Prior key is removed with lease.
	})
	Announce(client, key, "value2", session2.Lease())

	resp, err := client.Get(context.Background(), key)
	c.Check(err, gc.IsNil)
	c.Check(string(resp.Kvs[0].Value), gc.Equals, "value2")
}

var _ = gc.Suite(&AnnounceSuite{})
