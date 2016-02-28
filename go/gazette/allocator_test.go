package consensus

import (
	"errors"
	"time"

	etcd "github.com/coreos/etcd/client"
	gc "github.com/go-check/check"
	"github.com/stretchr/testify/mock"
)

type AllocSuite struct{}

func (s *AllocSuite) TestAllocParamExtraction(c *gc.C) {
	alloc := &MockAllocator{}
	alloc.On("InstanceKey").Return("my-key")
	alloc.On("Replicas").Return(1)
	alloc.On("FixedItems").Return([]string{"a-master", "b-created"})
	alloc.On("ItemIsReadyForPromotion", "ready").Return(true)
	alloc.On("ItemIsReadyForPromotion", "not-ready").Return(false)

	params := allocParams{Allocator: alloc}
	params.Input.Time = time.Unix(1234, 0)
	params.Input.Index = 45678
	params.Input.Tree = buildTree(c, []etcd.Node{
		// Item we master, but cannot release.
		{Key: "/foo/items/a-master/my-key", CreatedIndex: 333},
		{Key: "/foo/items/a-master/other-key", Value: "not-ready", CreatedIndex: 444},
		// Item with no directory (b-created). Has open master slot.
		// Item we replicate.
		{Key: "/foo/items/c-replica/other-key", CreatedIndex: 111},
		{Key: "/foo/items/c-replica/my-key", CreatedIndex: 222},
		// Item we master, and can release.
		{Key: "/foo/items/d-releaseable/my-key", CreatedIndex: 555},
		{Key: "/foo/items/d-releaseable/other-key", Value: "ready", CreatedIndex: 666},
		// Item we hold a spurious lock to.
		{Key: "/foo/items/e-extra/other-key", CreatedIndex: 777},
		{Key: "/foo/items/e-extra/another-key", CreatedIndex: 888},
		{Key: "/foo/items/e-extra/my-key", CreatedIndex: 999},
		// Item we could allocate (has open replica slot).
		{Key: "/foo/items/f-open/other-key", CreatedIndex: 1010},
		// Item which is full (no replica slots).
		{Key: "/foo/items/g-full/other-key", CreatedIndex: 1111},
		{Key: "/foo/items/g-full/another-key", CreatedIndex: 1212},
		// Member annoucements.
		{Key: "/foo/members/my-key"},
		{Key: "/foo/members/other-key"},
	}).Nodes[0]

	// Verify that expected ItemRoute() calls occur.
	routeExpectations := map[string]struct {
		ind  int
		keys []string
	}{
		"a-master":      {0, []string{"my-key", "other-key"}},
		"b-created":     {-1, []string{}},
		"c-replica":     {1, []string{"other-key", "my-key"}},
		"d-releaseable": {0, []string{"my-key", "other-key"}},
		"e-extra":       {2, []string{"other-key", "another-key", "my-key"}},
		"f-open":        {-1, []string{"other-key"}},
		"g-full":        {-1, []string{"other-key", "another-key"}},
	}
	for k, v := range routeExpectations {
		name, exp := k, v // Copy and retain for closure.
		alloc.On("ItemRoute", name, mock.AnythingOfType("Route"), exp.ind, params.Input.Tree).
			Run(func(args mock.Arguments) {
				rt := args.Get(1).(Route)

				c.Check(rt.EtcdIndex, gc.Equals, params.Input.Index)
				c.Check(rt.Item.Key, gc.Equals, "/foo/items/"+name)

				c.Check(len(rt.Entries), gc.Equals, len(exp.keys))
				for j := range exp.keys {
					c.Check(rt.Entries[j].Key, gc.Equals, rt.Item.Key+"/"+exp.keys[j])
				}
			}).Once()
	}
	allocExtract(&params)

	keysOf := func(l []*etcd.Node) (r []string) {
		for i := range l {
			r = append(r, l[i].Key[len("/foo/items/"):])
		}
		return
	}
	// Verify extracted Item parameters.
	c.Check(keysOf(params.Item.Master), gc.DeepEquals,
		[]string{"a-master/my-key", "d-releaseable/my-key"})
	c.Check(keysOf(params.Item.Replica), gc.DeepEquals, []string{"c-replica/my-key"})
	c.Check(keysOf(params.Item.Extra), gc.DeepEquals, []string{"e-extra/my-key"})
	c.Check(params.Item.OpenMasters, gc.DeepEquals, []string{"b-created"})
	c.Check(params.Item.OpenReplicas, gc.DeepEquals, []string{"f-open"})
	c.Check(keysOf(params.Item.Releaseable), gc.DeepEquals, []string{"d-releaseable/my-key"})
	c.Check(params.Item.Count, gc.Equals, 7)

	// Verify extracted Member parameters.
	c.Check(params.Member.Entry.Key, gc.Equals, "/foo/members/my-key")
	c.Check(params.Member.Count, gc.Equals, 2)

	alloc.AssertExpectations(c)
}

func (s *AllocSuite) TestAllocParamExtractionEmptyTree(c *gc.C) {
	alloc := &MockAllocator{}

	alloc.On("InstanceKey").Return("my-key")
	alloc.On("FixedItems").Return([]string{"a-item"})

	params := allocParams{Allocator: alloc}
	params.Input.Time = time.Unix(1234, 0)
	params.Input.Index = 4567
	params.Input.Tree = &etcd.Node{Dir: true, Key: "/foo"}

	alloc.On("ItemRoute", "a-item", mock.MatchedBy(func(rt Route) bool {
		c.Check(rt.EtcdIndex, gc.Equals, params.Input.Index)
		c.Check(rt.Item.Key, gc.Equals, "/foo/items/a-item")
		c.Check(rt.Entries, gc.HasLen, 0)
		return true
	}), -1, params.Input.Tree)

	allocExtract(&params)

	c.Check(params.Item.Master, gc.IsNil)
	c.Check(params.Item.Replica, gc.IsNil)
	c.Check(params.Item.Extra, gc.IsNil)
	c.Check(params.Item.OpenMasters, gc.DeepEquals, []string{"a-item"})
	c.Check(params.Item.OpenReplicas, gc.IsNil)
	c.Check(params.Item.Releaseable, gc.IsNil)
	c.Check(params.Item.Count, gc.Equals, 1)
	c.Check(params.Member.Entry, gc.IsNil)
	c.Check(params.Member.Count, gc.Equals, 0)

	alloc.AssertExpectations(c)
}

func (s *AllocSuite) TestDesiredCounts(c *gc.C) {
	var mockAlloc MockAllocator
	var p = allocParams{Allocator: &mockAlloc}

	mockAlloc.On("Replicas").Return(2)
	p.Item.Count = 6
	p.Member.Count = 2

	// No member entry: desired counts are zero.
	dm, dt := targetCounts(&p)
	c.Check(dm, gc.Equals, 0)
	c.Check(dt, gc.Equals, 0)
	p.Member.Entry = &etcd.Node{}

	// Desires an even share of mastered items, for a total count of the
	// replication factor plus one.
	dm, dt = targetCounts(&p)
	c.Check(dm, gc.Equals, 3)
	c.Check(dt, gc.Equals, 9)

	// Fractional counts are rounded up.
	p.Item.Count = 5
	dm, dt = targetCounts(&p)
	c.Check(dm, gc.Equals, 3)
	c.Check(dt, gc.Equals, 9)
}

func (s *AllocSuite) TestAllocationActions(c *gc.C) {
	var mockKV MockKeysAPI
	var mockAlloc MockAllocator

	var model allocParams
	model.Allocator = &mockAlloc
	model.Input.Time = time.Unix(1234, 0)
	model.Item.Count = 5

	beforeHorizon := model.Input.Time.Add(lockDuration / 2).Add(-time.Microsecond)
	afterHorizon := model.Input.Time.Add(lockDuration / 2).Add(time.Microsecond)

	model.Member.Entry = &etcd.Node{Key: "/foo/members/my-key", Expiration: &afterHorizon}
	model.Member.Count = 2

	mockAlloc.On("KeysAPI").Return(&mockKV)
	mockAlloc.On("InstanceKey").Return("my-key")
	mockAlloc.On("ItemState", "an-item").Return("a-value")
	mockAlloc.On("PathRoot").Return("/foo")
	mockAlloc.On("Replicas").Return(2)

	var respFixture = &etcd.Response{Action: "verifies response pass-through"}
	var errFixture = errors.New("verifies error pass-through")

	var underTest = model

	verify := func(desiredMaster, desiredTotal int, expectAct bool) {
		if response, err := allocAction(&underTest, desiredMaster, desiredTotal); expectAct {
			c.Check(response, gc.Equals, respFixture)
			c.Check(err, gc.Equals, errFixture)
		} else {
			c.Check(response, gc.IsNil)
			c.Check(err, gc.IsNil)
		}
		underTest = model // Reset.
	}

	// Default member entry is up to date. No refresh.
	verify(0, 0, false)

	// Entry is refreshed if horizon has elapsed.
	underTest.Member.Entry = &etcd.Node{
		Key: "/foo/members/my-key", Expiration: &beforeHorizon, ModifiedIndex: 123}

	mockKV.On("Set", mock.Anything, "/foo/members/my-key", "",
		&etcd.SetOptions{PrevIndex: 123, TTL: lockDuration}).
		Return(respFixture, errFixture).Once()
	verify(0, 0, true)

	// Expect |upToDate| isn't refreshed as a Master or Replica.
	upToDate := []*etcd.Node{{
		Key:        "/foo/items/an-item/my-key",
		Expiration: &afterHorizon,
		Value:      "a-value",
	}}

	underTest.Item.Master = upToDate
	verify(0, 0, false)
	underTest.Item.Replica = upToDate
	verify(0, 0, false)

	// Refresh is required because updated value needs to be persisted.
	valueChanged := []*etcd.Node{{
		Key:           "/foo/items/an-item/my-key",
		Expiration:    &afterHorizon,
		Value:         "different-value",
		ModifiedIndex: 234,
	}}

	mockKV.On("Set", mock.Anything, "/foo/items/an-item/my-key", "a-value",
		&etcd.SetOptions{PrevIndex: 234, TTL: lockDuration}).
		Return(respFixture, errFixture).Times(4)

	underTest.Item.Master = valueChanged
	verify(0, 0, true)
	underTest.Item.Replica = valueChanged
	verify(0, 0, true)

	// Refresh is required because TTL refresh horizon has elapsed.
	outOfDate := []*etcd.Node{{
		Key:           "/foo/items/an-item/my-key",
		Expiration:    &beforeHorizon,
		Value:         "a-value",
		ModifiedIndex: 234,
	}}

	underTest.Item.Master = outOfDate
	verify(0, 0, true)
	underTest.Item.Replica = outOfDate
	verify(0, 0, true)

	// Release of extra item lock.
	underTest.Item.Extra = []*etcd.Node{{
		Key:           "/foo/items/extra-item/my-key",
		ModifiedIndex: 345,
	}}

	mockKV.On("Delete", mock.Anything, "/foo/items/extra-item/my-key",
		&etcd.DeleteOptions{PrevIndex: 345}).Return(respFixture, errFixture).Once()
	verify(0, 0, true)

	// Release of mastered item entry.
	underTest.Item.Releaseable = []*etcd.Node{{
		Key:           "/foo/items/an-item/my-key",
		Expiration:    &afterHorizon,
		Value:         "a-value",
		ModifiedIndex: 456,
	}}
	mockKV.On("Delete", mock.Anything, "/foo/items/an-item/my-key",
		&etcd.DeleteOptions{PrevIndex: 456}).Return(respFixture, errFixture).Twice()

	underTest.Item.Master = underTest.Item.Releaseable
	verify(0, 0, true)

	// If member lock is present, replicas are never released.
	underTest.Item.Replica = []*etcd.Node{{
		Key:           "/foo/items/an-item/my-key",
		Expiration:    &afterHorizon,
		Value:         "a-value",
		ModifiedIndex: 567,
	}}
	verify(0, 0, false)

	// However, they will be if the lock is missing.
	underTest.Member.Entry = nil
	underTest.Item.Replica = []*etcd.Node{{
		Key:           "/foo/items/an-item/my-key",
		Expiration:    &afterHorizon,
		Value:         "a-value",
		ModifiedIndex: 456,
	}}
	verify(0, 0, true)

	// Acquire of master entry.
	mockKV.On("Set", mock.Anything, "/foo/items/new-item/my-key", "",
		&etcd.SetOptions{PrevExist: "false", TTL: lockDuration}).
		Return(respFixture, errFixture).Twice()

	underTest.Item.OpenMasters = []string{"new-item"}
	verify(1, 0, true)

	// Aquire of replica entry.
	underTest.Item.OpenReplicas = []string{"new-item"}
	verify(0, 1, true)
}

func (s *AllocSuite) TestNextDeadline(c *gc.C) {
	var p allocParams

	box := func(t time.Time) *time.Time { return &t }
	now := time.Unix(12345, 0)

	// Expect the earliest TTL horizon of the member entry & all items is returned.
	c.Check(nextDeadline(&p), gc.Equals, time.Time{})
	p.Member.Entry = &etcd.Node{Expiration: box(now.Add(lockDuration))}
	c.Check(nextDeadline(&p), gc.Equals, now.Add(lockDuration/2))
	p.Item.Master = []*etcd.Node{{Expiration: box(now.Add(lockDuration - 1))}}
	c.Check(nextDeadline(&p), gc.Equals, now.Add(lockDuration/2-1))
	p.Item.Replica = []*etcd.Node{{Expiration: box(now.Add(lockDuration - 2))}}
	c.Check(nextDeadline(&p), gc.Equals, now.Add(lockDuration/2-2))
}

func buildTree(c *gc.C, nodes []etcd.Node) *etcd.Node {
	tree := &etcd.Node{Dir: true}

	for i := range nodes {
		_, err := PatchTree(tree, &etcd.Response{Node: &nodes[i], Action: "create"})
		c.Assert(err, gc.IsNil)
	}
	return tree
}

var _ = gc.Suite(&AllocSuite{})
