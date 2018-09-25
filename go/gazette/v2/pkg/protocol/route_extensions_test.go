package protocol

import (
	"math/rand"
	"time"

	"github.com/LiveRamp/gazette/pkg/keyspace"
	"github.com/LiveRamp/gazette/pkg/v3.allocator"
	"github.com/coreos/etcd/mvcc/mvccpb"
	gc "github.com/go-check/check"
)

type RouteSuite struct{}

func (s *RouteSuite) TestInitialization(c *gc.C) {
	var rt Route

	rt.Init(nil)
	c.Check(rt, gc.DeepEquals, Route{Primary: -1, Members: nil})

	var kv = keyspace.KeyValues{
		{Decoded: v3_allocator.Assignment{MemberZone: "zone-a", MemberSuffix: "member-1", Slot: 1}},
		{Decoded: v3_allocator.Assignment{MemberZone: "zone-a", MemberSuffix: "member-3", Slot: 3}},
		{Decoded: v3_allocator.Assignment{MemberZone: "zone-b", MemberSuffix: "member-2", Slot: 2}},
		{Decoded: v3_allocator.Assignment{MemberZone: "zone-c", MemberSuffix: "member-4", Slot: 0}},
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
				Decoded: v3_allocator.Member{Zone: "zone-a", Suffix: "member-1",
					MemberValue: &BrokerSpec{ProcessSpec: ProcessSpec{Endpoint: "http://host-a:port/path"}}}},
			{Raw: mvccpb.KeyValue{Key: []byte("/root/members/zone-b#member-2")},
				Decoded: v3_allocator.Member{Zone: "zone-b", Suffix: "member-2",
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
	c.Check(rt.Validate(), gc.ErrorMatches, `Members\[0\].Zone: not base64 alphabet \(invalid zone\)`)
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

func (s *RouteSuite) TestReplicaSelection(c *gc.C) {
	var rt = Route{
		Primary: -1,
		Members: []ProcessSpec_ID{
			{Zone: "zone-a", Suffix: "member-1"},
			{Zone: "zone-a", Suffix: "member-3"},
			{Zone: "zone-b", Suffix: "member-2"},
		},
		Endpoints: []Endpoint{"http://foo", "http://bar", "http://baz"},
	}

	rand.Seed(time.Now().UnixNano())

	// If the exact preferred ID is available, expect it's returned.
	c.Check(rt.SelectReplica(ProcessSpec_ID{"zone-a", "member-3"}), gc.Equals, 1)
	// Otherwise, expect an ID from its Zone is returned.
	c.Check(rt.SelectReplica(ProcessSpec_ID{"zone-b", "other-peer"}), gc.Equals, 2)
	c.Check(rt.SelectReplica(ProcessSpec_ID{"zone-a", "other-peer"}) < 2, gc.Equals, true)
	// Otherwise, selection is at random.
	c.Check(rt.SelectReplica(ProcessSpec_ID{"other-zone", "peer"}) < 3, gc.Equals, true)
}

var _ = gc.Suite(&RouteSuite{})
