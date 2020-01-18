package protocol

import (
	gc "github.com/go-check/check"
)

type RouteSuite struct{}

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
