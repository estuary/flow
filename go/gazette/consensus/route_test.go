package consensus

import (
	"time"

	etcd "github.com/coreos/etcd/client"
	gc "github.com/go-check/check"
)

type RouteSuite struct{}

func (s *RouteSuite) TestOrdering(c *gc.C) {
	var rt = s.fixture(time.Time{}, nil)

	c.Check(rt.Entries[0].Key, gc.Equals, "/foo/bar/ccc")
	c.Check(rt.Entries[1].Key, gc.Equals, "/foo/bar/aaa")
	c.Check(rt.Entries[2].Key, gc.Equals, "/foo/bar/bbb")
}

func (s *RouteSuite) TestIndex(c *gc.C) {
	var rt = s.fixture(time.Time{}, nil)

	c.Check(rt.Index("aaa"), gc.Equals, 1)
	c.Check(rt.Index("bbb"), gc.Equals, 2)
	c.Check(rt.Index("ccc"), gc.Equals, 0)
	c.Check(rt.Index("ddd"), gc.Equals, -1)
}

func (s *RouteSuite) TestReadyForHandoff(c *gc.C) {
	var rt = s.fixture(time.Time{}, nil)
	var alloc = new(MockAllocator)

	// No replicas required: always ready for handoff.
	alloc.On("Replicas").Return(0).Once()
	c.Check(rt.IsReadyForHandoff(alloc), gc.Equals, true)

	// More replicas than entries: not ready.
	alloc.On("Replicas").Return(100).Once()
	c.Check(rt.IsReadyForHandoff(alloc), gc.Equals, false)

	alloc.On("ItemIsReadyForPromotion", "bar", "ready").Return(true)
	alloc.On("ItemIsReadyForPromotion", "bar", "not-ready").Return(false)

	// Sufficient entries, but one is not ready.
	alloc.On("Replicas").Return(2).Once()
	c.Check(rt.IsReadyForHandoff(alloc), gc.Equals, false)

	// Required number of replicas are ready.
	alloc.On("Replicas").Return(1).Once()
	c.Check(rt.IsReadyForHandoff(alloc), gc.Equals, true)
}

func (s *RouteSuite) TestCopy(c *gc.C) {
	var rt1 = s.fixture(time.Time{}, nil)
	var rt2 = rt1.Copy()

	// Expect |rt2| has distinct Entries storage from |rt1|.
	c.Check(rt1, gc.DeepEquals, rt2)
	rt1.Entries[0], rt1.Entries[1] = rt1.Entries[1], rt1.Entries[0]
	c.Check(rt1, gc.Not(gc.DeepEquals), rt2)
}

func (s *RouteSuite) TestSoonToExpireNodesAreStripped(c *gc.C) {
	var now = time.Unix(0, lockDuration.Nanoseconds())
	var nearFuture = now.Add(lockInvalidHorizon)
	var rt = s.fixture(now, &nearFuture)

	c.Check(rt.Entries, gc.HasLen, 2)
	c.Check(rt.Index("aaa"), gc.Equals, -1)
	c.Check(rt.Index("bbb"), gc.Equals, 1)
	c.Check(rt.Index("ccc"), gc.Equals, 0)
}

func (s *RouteSuite) fixture(now time.Time, expireAAA *time.Time) Route {
	var expireBBB = now.Add(lockInvalidHorizon + 1) // A value beyond the lock invalidation horizon.

	var item = &etcd.Node{
		Key: "/foo/bar",
		Nodes: []*etcd.Node{
			{Key: "/foo/bar/aaa", Value: "ready", CreatedIndex: 2, Expiration: expireAAA},
			{Key: "/foo/bar/bbb", Value: "not-ready", CreatedIndex: 3, Expiration: &expireBBB},
			{Key: "/foo/bar/ccc", CreatedIndex: 1},
		},
	}

	var rt = Route{
		Item:    item,
		Entries: append([]*etcd.Node{}, item.Nodes...),
	}
	rt.init(now)

	return rt
}

var _ = gc.Suite(&RouteSuite{})
