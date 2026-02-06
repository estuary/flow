package allocator

import (
	"context"
	"os"
	"time"

	"go.etcd.io/etcd/client/v3/concurrency"
	"go.gazette.dev/core/etcdtest"
	"go.gazette.dev/core/task"
	gc "gopkg.in/check.v1"
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

func (s *AnnounceSuite) TestAnnounceIdempotency(c *gc.C) {
	var client = etcdtest.TestClient()
	defer etcdtest.Cleanup()
	const key = "/announce/key"

	var session, err = concurrency.NewSession(client, concurrency.WithTTL(5))
	c.Check(err, gc.IsNil)
	defer session.Close()

	// First announce should succeed
	var a1 = Announce(client, key, "value", session.Lease())
	c.Check(a1, gc.NotNil)

	// Second announce with same lease should also succeed (idempotent)
	var a2 = Announce(client, key, "value", session.Lease())
	c.Check(a2, gc.NotNil)

	// Both announcements should have the same revision since they're the same key
	c.Check(a1.Revision, gc.Equals, a2.Revision)
}

func (s *AnnounceSuite) TestBasicSessionStart(c *gc.C) {
	var (
		etcd  = etcdtest.TestClient()
		ks    = NewAllocatorKeySpace("/root", testAllocDecoder{})
		sigCh = make(chan os.Signal)
		spec  = &testMember{R: 10}
		state = NewObservedState(ks, MemberKey(ks, "a", "member"), isConsistent)

		args = SessionArgs{
			Etcd:     etcd,
			Tasks:    task.NewGroup(context.Background()),
			Spec:     spec,
			State:    state,
			LeaseTTL: time.Second * 60,
			SignalCh: sigCh,
		}
	)
	c.Check(StartSession(args), gc.IsNil)

	// Expect our MemberSpec was announced and loaded by the KeySpace.
	c.Assert(state.Members, gc.HasLen, 1)
	c.Check(string(state.Members[0].Raw.Key), gc.Equals, "/root/members/a#member")
	c.Check(string(state.Members[0].Raw.Value), gc.Equals, `{"R":10,"E":false}`)
	c.Check(state.Members[0].Raw.Lease, gc.Not(gc.Equals), 0)

	args.Tasks.Queue("Watch", func() error {
		if err := ks.Watch(args.Tasks.Context(), etcd); err != context.Canceled {
			return err
		}
		return nil
	})
	args.Tasks.GoRun()

	// By signaling, expect that the member is marked as exiting, Allocate exits and
	// cancels the task.Group, and the lease is cancelled.
	close(sigCh)

	c.Check(args.Tasks.Wait(), gc.IsNil) // All tasks have exited.
	c.Check(spec.E, gc.Equals, true) // Member was marked as exiting.
	c.Check(spec.R, gc.Equals, 0)   // TODO(whb): Remove once backward compat is removed.

	leasesResp, err := etcd.Leases(context.Background())
	c.Check(err, gc.IsNil)
	c.Check(leasesResp.Leases, gc.HasLen, 0)
}

var _ = gc.Suite(&AnnounceSuite{})
