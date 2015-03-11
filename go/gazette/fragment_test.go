package gazette

import (
	gc "github.com/go-check/check"
)

type FragmentSuite struct {
}

func (s *FragmentSuite) TestSetAddInsertAtEnd(c *gc.C) {
	var set FragmentSet

	c.Check(set.Add(Fragment{Begin: 100, End: 200}), gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 200, End: 300}), gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 300, End: 400}), gc.Equals, true)

	c.Check(set, gc.DeepEquals, FragmentSet{
		{Begin: 100, End: 200},
		{Begin: 200, End: 300},
		{Begin: 300, End: 400},
	})
}

func (s *FragmentSuite) TestSetAddReplaceRangeAtEnd(c *gc.C) {
	var set FragmentSet

	c.Check(set.Add(Fragment{Begin: 100, End: 200}), gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 200, End: 300}), gc.Equals, true) // Replace.
	c.Check(set.Add(Fragment{Begin: 300, End: 400}), gc.Equals, true) // Replace.
	c.Check(set.Add(Fragment{Begin: 400, End: 500}), gc.Equals, true) // Replace.
	c.Check(set.Add(Fragment{Begin: 150, End: 500}), gc.Equals, true)

	c.Check(set, gc.DeepEquals, FragmentSet{
		{Begin: 100, End: 200},
		{Begin: 150, End: 500},
	})
}

func (s *FragmentSuite) TestSetAddReplaceOneAtEnd(c *gc.C) {
	var set FragmentSet

	c.Check(set.Add(Fragment{Begin: 100, End: 200}), gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 200, End: 300}), gc.Equals, true) // Replace.
	c.Check(set.Add(Fragment{Begin: 150, End: 300}), gc.Equals, true)

	c.Check(set, gc.DeepEquals, FragmentSet{
		{Begin: 100, End: 200},
		{Begin: 150, End: 300},
	})
}

func (s *FragmentSuite) TestSetAddReplaceRangeInMiddle(c *gc.C) {
	var set FragmentSet

	c.Check(set.Add(Fragment{Begin: 100, End: 200}), gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 200, End: 300}), gc.Equals, true) // Replace.
	c.Check(set.Add(Fragment{Begin: 300, End: 400}), gc.Equals, true) // Replace.
	c.Check(set.Add(Fragment{Begin: 400, End: 500}), gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 150, End: 450}), gc.Equals, true)

	c.Check(set, gc.DeepEquals, FragmentSet{
		{Begin: 100, End: 200},
		{Begin: 150, End: 450},
		{Begin: 400, End: 500},
	})
}

func (s *FragmentSuite) TestSetAddReplaceOneInMiddle(c *gc.C) {
	var set FragmentSet

	c.Check(set.Add(Fragment{Begin: 100, End: 200}), gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 200, End: 300}), gc.Equals, true) // Replace.
	c.Check(set.Add(Fragment{Begin: 300, End: 400}), gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 150, End: 350}), gc.Equals, true)

	c.Check(set, gc.DeepEquals, FragmentSet{
		{Begin: 100, End: 200},
		{Begin: 150, End: 350},
		{Begin: 300, End: 400},
	})
}

func (s *FragmentSuite) TestSetAddInsertInMiddleExactBoundaries(c *gc.C) {
	var set FragmentSet

	c.Check(set.Add(Fragment{Begin: 100, End: 200}), gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 300, End: 400}), gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 200, End: 300}), gc.Equals, true)

	c.Check(set, gc.DeepEquals, FragmentSet{
		{Begin: 100, End: 200},
		{Begin: 200, End: 300},
		{Begin: 300, End: 400},
	})
}

func (s *FragmentSuite) TestSetAddInsertInMiddleCloseBoundaries(c *gc.C) {
	var set FragmentSet

	c.Check(set.Add(Fragment{Begin: 100, End: 200}), gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 300, End: 400}), gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 201, End: 299}), gc.Equals, true)

	c.Check(set, gc.DeepEquals, FragmentSet{
		{Begin: 100, End: 200},
		{Begin: 201, End: 299},
		{Begin: 300, End: 400},
	})
}

func (s *FragmentSuite) TestSetAddReplaceRangeAtBeginning(c *gc.C) {
	var set FragmentSet

	c.Check(set.Add(Fragment{Begin: 100, End: 200}), gc.Equals, true) // Replace.
	c.Check(set.Add(Fragment{Begin: 200, End: 300}), gc.Equals, true) // Replace.
	c.Check(set.Add(Fragment{Begin: 300, End: 400}), gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 100, End: 300}), gc.Equals, true)

	c.Check(set, gc.DeepEquals, FragmentSet{
		{Begin: 100, End: 300},
		{Begin: 300, End: 400},
	})
}

func (s *FragmentSuite) TestSetAddReplaceOneAtBeginning(c *gc.C) {
	var set FragmentSet

	c.Check(set.Add(Fragment{Begin: 100, End: 200}), gc.Equals, true) // Replace.
	c.Check(set.Add(Fragment{Begin: 200, End: 300}), gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 300, End: 400}), gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 99, End: 200}), gc.Equals, true)

	c.Check(set, gc.DeepEquals, FragmentSet{
		{Begin: 99, End: 200},
		{Begin: 200, End: 300},
		{Begin: 300, End: 400},
	})
}

func (s *FragmentSuite) TestSetAddInsertAtBeginning(c *gc.C) {
	var set FragmentSet

	c.Check(set.Add(Fragment{Begin: 200, End: 300}), gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 300, End: 400}), gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 199, End: 200}), gc.Equals, true)

	c.Check(set, gc.DeepEquals, FragmentSet{
		{Begin: 199, End: 200},
		{Begin: 200, End: 300},
		{Begin: 300, End: 400},
	})
}

func (s *FragmentSuite) TestSetAddOverlappingRanges(c *gc.C) {
	var set FragmentSet

	c.Check(set.Add(Fragment{Begin: 100, End: 150}), gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 149, End: 201}), gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 200, End: 250}), gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 250, End: 300}), gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 299, End: 351}), gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 350, End: 400}), gc.Equals, true)

	c.Check(set.Add(Fragment{Begin: 200, End: 300}), gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 300, End: 400}), gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 100, End: 200}), gc.Equals, true)

	c.Check(set, gc.DeepEquals, FragmentSet{
		{Begin: 100, End: 200},
		{Begin: 149, End: 201},
		{Begin: 200, End: 300},
		{Begin: 299, End: 351},
		{Begin: 300, End: 400},
	})
}

func (s *FragmentSuite) TestSetAddNoAction(c *gc.C) {
	var set FragmentSet

	c.Check(set.Add(Fragment{Begin: 100, End: 200, SHA1Sum: []byte{1}}),
		gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 200, End: 300, SHA1Sum: []byte{2}}),
		gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 300, End: 400, SHA1Sum: []byte{3}}),
		gc.Equals, true)

	// Precondition.
	c.Check(set, gc.DeepEquals, FragmentSet{
		{Begin: 100, End: 200, SHA1Sum: []byte{1}},
		{Begin: 200, End: 300, SHA1Sum: []byte{2}},
		{Begin: 300, End: 400, SHA1Sum: []byte{3}},
	})

	c.Check(set.Add(Fragment{Begin: 100, End: 200}), gc.Equals, false)
	c.Check(set.Add(Fragment{Begin: 101, End: 200}), gc.Equals, false)
	c.Check(set.Add(Fragment{Begin: 100, End: 199}), gc.Equals, false)
	c.Check(set.Add(Fragment{Begin: 200, End: 300}), gc.Equals, false)
	c.Check(set.Add(Fragment{Begin: 201, End: 300}), gc.Equals, false)
	c.Check(set.Add(Fragment{Begin: 200, End: 299}), gc.Equals, false)
	c.Check(set.Add(Fragment{Begin: 300, End: 400}), gc.Equals, false)
	c.Check(set.Add(Fragment{Begin: 301, End: 400}), gc.Equals, false)

	// Postcondition. No change.
	c.Check(set, gc.DeepEquals, FragmentSet{
		{Begin: 100, End: 200, SHA1Sum: []byte{1}},
		{Begin: 200, End: 300, SHA1Sum: []byte{2}},
		{Begin: 300, End: 400, SHA1Sum: []byte{3}},
	})
}

var _ = gc.Suite(&FragmentSuite{})
