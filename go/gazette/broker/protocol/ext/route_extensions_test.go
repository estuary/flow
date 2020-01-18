package ext

import (
	gc "github.com/go-check/check"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/keyspace"
)

type RouteSuite struct{}

func (s *RouteSuite) TestInitialization(c *gc.C) {
	var rt BrokerRoute

	rt.Init(nil)
	c.Check(rt, gc.DeepEquals, BrokerRoute{Route: pb.Route{Primary: -1, Members: nil}})

	var kv = keyspace.KeyValues{
		{Decoded: allocator.Assignment{MemberZone: "zone-a", MemberSuffix: "member-1", Slot: 1}},
		{Decoded: allocator.Assignment{MemberZone: "zone-a", MemberSuffix: "member-3", Slot: 3}},
		{Decoded: allocator.Assignment{MemberZone: "zone-b", MemberSuffix: "member-2", Slot: 2}},
		{Decoded: allocator.Assignment{MemberZone: "zone-c", MemberSuffix: "member-4", Slot: 0}},
	}
	rt.Init(kv)

	c.Check(rt, gc.DeepEquals, BrokerRoute{Route: pb.Route{
		Primary: 3,
		Members: []pb.ProcessSpec_ID{
			{Zone: "zone-a", Suffix: "member-1"},
			{Zone: "zone-a", Suffix: "member-3"},
			{Zone: "zone-b", Suffix: "member-2"},
			{Zone: "zone-c", Suffix: "member-4"},
		},
	}})

	kv = kv[:3] // This time, remove the primary Assignment.
	rt.Init(kv)

	c.Check(rt, gc.DeepEquals, BrokerRoute{Route: pb.Route{
		Primary: -1,
		Members: []pb.ProcessSpec_ID{
			{Zone: "zone-a", Suffix: "member-1"},
			{Zone: "zone-a", Suffix: "member-3"},
			{Zone: "zone-b", Suffix: "member-2"},
		},
	}})
}

func (s *RouteSuite) TestEndpointAttachmentAndCopy(c *gc.C) {
	var ks = keyspace.KeySpace{
		Root: "/root",
		KeyValues: keyspace.KeyValues{
			{Raw: mvccpb.KeyValue{Key: []byte("/root/members/zone-a#member-1")},
				Decoded: allocator.Member{Zone: "zone-a", Suffix: "member-1",
					MemberValue: &pb.BrokerSpec{ProcessSpec: pb.ProcessSpec{Endpoint: "http://host-a:port/path"}}}},
			{Raw: mvccpb.KeyValue{Key: []byte("/root/members/zone-b#member-2")},
				Decoded: allocator.Member{Zone: "zone-b", Suffix: "member-2",
					MemberValue: &pb.BrokerSpec{ProcessSpec: pb.ProcessSpec{Endpoint: "http://host-b:port/path"}}}},
		},
	}
	var rt = BrokerRoute{Route: pb.Route{
		Members: []pb.ProcessSpec_ID{
			{Zone: "zone-a", Suffix: "member-1"},
			{Zone: "zone-a", Suffix: "member-3"},
			{Zone: "zone-b", Suffix: "member-2"},
		},
	}}

	rt.AttachEndpoints(&ks)
	c.Check(rt.Route.Endpoints, gc.DeepEquals, []pb.Endpoint{"http://host-a:port/path", "", "http://host-b:port/path"})

	var other = rt.Route.Copy()

	// Expect |other| is deeply equal while referencing different slices.
	c.Check(rt.Route.Members, gc.DeepEquals, other.Members)
	c.Check(rt.Route.Endpoints, gc.DeepEquals, other.Endpoints)
	c.Check(&rt.Route.Members[0], gc.Not(gc.Equals), &other.Members[0])
	c.Check(&rt.Route.Endpoints[0], gc.Not(gc.Equals), &other.Endpoints[0])
}
