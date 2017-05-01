package cloudstore

import (
	gc "github.com/go-check/check"
)

type PropertiesSuite struct{}

func (s *PropertiesSuite) TestMergeProperties(c *gc.C) {
	var a, b = MapProperties{"one": "a-one", "two": "a-two"},
		MapProperties{"two": "b-two", "three": "b-three"}

	var merged, err = mergeProperties(a, b)
	c.Check(err, gc.IsNil)
	c.Check(merged.Get("one"), gc.Equals, "a-one")
	c.Check(merged.Get("two"), gc.Equals, "a-two") // a wins out on key conflict
	c.Check(merged.Get("three"), gc.Equals, "b-three")
}

func (s *PropertiesSuite) TestMergeEmptyProperties(c *gc.C) {
	var a = MapProperties{"one": "a-one", "two": "a-two"}

	var merged, err = mergeProperties(a, EmptyProperties())
	c.Check(err, gc.IsNil)
	c.Check(merged, gc.DeepEquals, a)
}

func (s *PropertiesSuite) TestMergeNotMaps(c *gc.C) {
	var fake fakeProperties
	var real = EmptyProperties()

	var merged, err = mergeProperties(fake, real)
	c.Check(merged, gc.IsNil)
	c.Check(err, gc.ErrorMatches, ".*is not a MapProperties.*")

	merged, err = mergeProperties(real, fake)
	c.Check(merged, gc.IsNil)
	c.Check(err, gc.ErrorMatches, ".*is not a MapProperties.*")
}

type fakeProperties struct{}

func (fp fakeProperties) Get(key string) string { return "foo" }

var _ = gc.Suite(new(PropertiesSuite))
