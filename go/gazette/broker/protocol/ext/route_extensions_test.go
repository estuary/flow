package ext

import (
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/keyspace"
	gc "gopkg.in/check.v1"
)

type RouteSuite struct{}

func (s *RouteSuite) TestInitialization(c *gc.C) {

	var rt = pb.Route{}
	Init(&rt, nil)
	c.Check(rt, gc.DeepEquals, pb.Route{Primary: -1, Members: nil})

	var kv = keyspace.KeyValues{
		{Decoded: allocator.Assignment{MemberZone: "zone-a", MemberSuffix: "member-1", Slot: 1}},
		{Decoded: allocator.Assignment{MemberZone: "zone-a", MemberSuffix: "member-3", Slot: 3}},
		{Decoded: allocator.Assignment{MemberZone: "zone-b", MemberSuffix: "member-2", Slot: 2}},
		{Decoded: allocator.Assignment{MemberZone: "zone-c", MemberSuffix: "member-4", Slot: 0}},
	}
	Init(&rt, kv)

	c.Check(rt, gc.DeepEquals, pb.Route{
		Primary: 3,
		Members: []pb.ProcessSpec_ID{
			{Zone: "zone-a", Suffix: "member-1"},
			{Zone: "zone-a", Suffix: "member-3"},
			{Zone: "zone-b", Suffix: "member-2"},
			{Zone: "zone-c", Suffix: "member-4"},
		},
	})

	kv = kv[:3] // This time, remove the primary Assignment.
	Init(&rt, kv)

	c.Check(rt, gc.DeepEquals, pb.Route{
		Primary: -1,
		Members: []pb.ProcessSpec_ID{
			{Zone: "zone-a", Suffix: "member-1"},
			{Zone: "zone-a", Suffix: "member-3"},
			{Zone: "zone-b", Suffix: "member-2"},
		},
	})
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
	var rt = pb.Route{
		Members: []pb.ProcessSpec_ID{
			{Zone: "zone-a", Suffix: "member-1"},
			{Zone: "zone-a", Suffix: "member-3"},
			{Zone: "zone-b", Suffix: "member-2"},
		},
	}

	AttachEndpoints(&rt, &ks)
	c.Check(rt.Endpoints, gc.DeepEquals, []pb.Endpoint{"http://host-a:port/path", "", "http://host-b:port/path"})

	var other = rt.Copy()

	// Expect |other| is deeply equal while referencing different slices.
	c.Check(rt.Members, gc.DeepEquals, other.Members)
	c.Check(rt.Endpoints, gc.DeepEquals, other.Endpoints)
	c.Check(&rt.Members[0], gc.Not(gc.Equals), &other.Members[0])
	c.Check(&rt.Endpoints[0], gc.Not(gc.Equals), &other.Endpoints[0])
}
