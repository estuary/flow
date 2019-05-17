package protocol

import (
	gc "github.com/go-check/check"
	"go.etcd.io/etcd/v3/mvcc/mvccpb"
	"go.gazette.dev/core/allocator"
	"go.gazette.dev/core/keyspace"
)

type RouteSuite struct{}

func (s *RouteSuite) TestInitialization(c *gc.C) {
	var rt Route

	rt.Init(nil)
	c.Check(rt, gc.DeepEquals, Route{Primary: -1, Members: nil})

	var kv = keyspace.KeyValues{
		{Decoded: allocator.Assignment{MemberZone: "zone-a", MemberSuffix: "member-1", Slot: 1}},
		{Decoded: allocator.Assignment{MemberZone: "zone-a", MemberSuffix: "member-3", Slot: 3}},
		{Decoded: allocator.Assignment{MemberZone: "zone-b", MemberSuffix: "member-2", Slot: 2}},
		{Decoded: allocator.Assignment{MemberZone: "zone-c", MemberSuffix: "member-4", Slot: 0}},
	}
	rt.Init(kv)

	c.Check(rt, gc.DeepEquals, Route{
		Primary: 3,
		Members: []ProcessSpec_ID{
			{Zone: "zone-a", Suffix: "member-1"},
			{Zone: "zone-a", Suffix: "member-3"},
			{Zone: "zone-b", Suffix: "member-2"},
			{Zone: "zone-c", Suffix: "member-4"},
		},
	})

	kv = kv[:3] // This time, remove the primary Assignment.
	rt.Init(kv)

	c.Check(rt, gc.DeepEquals, Route{
		Primary: -1,
		Members: []ProcessSpec_ID{
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
					MemberValue: &BrokerSpec{ProcessSpec: ProcessSpec{Endpoint: "http://host-a:port/path"}}}},
			{Raw: mvccpb.KeyValue{Key: []byte("/root/members/zone-b#member-2")},
				Decoded: allocator.Member{Zone: "zone-b", Suffix: "member-2",
					MemberValue: &BrokerSpec{ProcessSpec: ProcessSpec{Endpoint: "http://host-b:port/path"}}}},
		},
	}
	var rt = Route{
		Members: []ProcessSpec_ID{
			{Zone: "zone-a", Suffix: "member-1"},
			{Zone: "zone-a", Suffix: "member-3"},
			{Zone: "zone-b", Suffix: "member-2"},
		},
	}

	rt.AttachEndpoints(&ks)
	c.Check(rt.Endpoints, gc.DeepEquals, []Endpoint{"http://host-a:port/path", "", "http://host-b:port/path"})

	var other = rt.Copy()

	// Expect |other| is deeply equal while referencing different slices.
	c.Check(rt.Members, gc.DeepEquals, other.Members)
	c.Check(rt.Endpoints, gc.DeepEquals, other.Endpoints)
	c.Check(&rt.Members[0], gc.Not(gc.Equals), &other.Members[0])
	c.Check(&rt.Endpoints[0], gc.Not(gc.Equals), &other.Endpoints[0])
}

func (s *RouteSuite) TestValidationCases(c *gc.C) {
	var rt = Route{
		Primary: -1,
		Members: []ProcessSpec_ID{
			{Zone: "zone-a", Suffix: "member-1"},
			{Zone: "zone-a", Suffix: "member-3"},
			{Zone: "zone-b", Suffix: "member-2"},
		},
		Endpoints: []Endpoint{"http://foo", "http://bar", "http://baz"},
	}
	c.Check(rt.Validate(), gc.IsNil)

	rt.Members[0].Zone = "invalid zone"
	c.Check(rt.Validate(), gc.ErrorMatches, `Members\[0\].Zone: not a valid token \(invalid zone\)`)
	rt.Members[0].Zone = "zone-a"

	rt.Members[1], rt.Members[2] = rt.Members[2], rt.Members[1]
	c.Check(rt.Validate(), gc.ErrorMatches, `Members not in unique, sorted order \(index 2; {Zone.*?} <= {Zone.*?}\)`)
	rt.Members[1], rt.Members[2] = rt.Members[2], rt.Members[1]

	rt.Primary = -2
	c.Check(rt.Validate(), gc.ErrorMatches, `invalid Primary \(-2; expected -1 <= Primary < 3\)`)
	rt.Primary = 3
	c.Check(rt.Validate(), gc.ErrorMatches, `invalid Primary \(3; expected -1 <= Primary < 3\)`)
	rt.Primary = 2

	rt.Endpoints = append(rt.Endpoints, "http://bing")
	c.Check(rt.Validate(), gc.ErrorMatches, `len\(Endpoints\) != 0, and != len\(Members\) \(4 vs 3\)`)

	rt.Endpoints = rt.Endpoints[:3]
	rt.Endpoints[2] = "invalid"
	c.Check(rt.Validate(), gc.ErrorMatches, `Endpoints\[2\]: not absolute: invalid`)
	rt.Endpoints[2] = "http://baz"
}

func (s *RouteSuite) TestEquivalencyCases(c *gc.C) {
	var rt = Route{
		Primary: -1,
		Members: []ProcessSpec_ID{
			{Zone: "zone-a", Suffix: "member-1"},
			{Zone: "zone-a", Suffix: "member-3"},
			{Zone: "zone-b", Suffix: "member-2"},
		},
		Endpoints: []Endpoint{"http://foo", "http://bar", "http://baz"},
	}
	var other = rt.Copy()
	other.Endpoints = nil // Endpoints are optional for equivalency.

	c.Check(rt.Equivalent(&other), gc.Equals, true)
	c.Check(other.Equivalent(&rt), gc.Equals, true)

	rt.Primary = 1
	c.Check(rt.Equivalent(&other), gc.Equals, false)
	c.Check(other.Equivalent(&rt), gc.Equals, false)

	other.Primary = 1
	other.Members[1].Zone = "zone-aa"
	c.Check(rt.Equivalent(&other), gc.Equals, false)
	c.Check(other.Equivalent(&rt), gc.Equals, false)

	rt.Members[1].Zone = "zone-aa"
	c.Check(rt.Equivalent(&other), gc.Equals, true)
	c.Check(other.Equivalent(&rt), gc.Equals, true)
}

var _ = gc.Suite(&RouteSuite{})
