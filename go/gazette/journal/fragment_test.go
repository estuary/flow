package journal

import (
	"crypto/sha1"
	"math"

	gc "github.com/go-check/check"
)

type FragmentSuite struct {
}

func (s *FragmentSuite) TestContentName(c *gc.C) {
	fragment := Fragment{
		Journal: "a/journal",
		Begin:   1234567890,
		End:     math.MaxInt64,
		Sum: [...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
			11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
	}
	c.Assert(fragment.ContentName(), gc.Equals,
		"00000000499602d2-7fffffffffffffff-0102030405060708090a0b0c0d0e0f1011121314")
}

func (s *SpoolSuite) TestContentPath(c *gc.C) {
	fragment := Fragment{
		Journal: "a/journal/name",
		Begin:   1234567890,
		End:     math.MaxInt64,
		Sum: [...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
			11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
	}
	c.Assert(fragment.ContentPath(), gc.Equals, "a/journal/name/"+
		"00000000499602d2-7fffffffffffffff-0102030405060708090a0b0c0d0e0f1011121314")
}

func (s *FragmentSuite) TestParsing(c *gc.C) {
	fragment, err := ParseFragment("a/journal",
		"00000000499602d2-7fffffffffffffff-0102030405060708090a0b0c0d0e0f1011121314")
	c.Assert(err, gc.IsNil)
	c.Assert(fragment, gc.DeepEquals, Fragment{
		Journal: "a/journal",
		Begin:   1234567890,
		End:     math.MaxInt64,
		Sum: [...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
			11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
	})

	// Empty spool (begin == end, and zero checksum).
	fragment, err = ParseFragment("a/journal",
		"00000000499602d2-00000000499602d2-0000000000000000000000000000000000000000")
	c.Assert(err, gc.IsNil)
	c.Assert(fragment, gc.DeepEquals, Fragment{
		Journal: "a/journal",
		Begin:   1234567890,
		End:     1234567890,
		Sum:     [sha1.Size]byte{},
	})

	_, err = ParseFragment("a/journal",
		"00000000499602d2-7fffffffffffffff-010203040506")
	c.Assert(err, gc.ErrorMatches, "invalid checksum")

	_, err = ParseFragment("a/journal",
		"2-1-0102030405060708090a0b0c0d0e0f1011121314")
	c.Assert(err, gc.ErrorMatches, "invalid content range")
	_, err = ParseFragment("a/journal",
		"1-0102030405060708090a0b0c0d0e0f1011121314")
	c.Assert(err, gc.ErrorMatches, "wrong format")
}

func (s *FragmentSuite) TestSetAddInsertAtEnd(c *gc.C) {
	var set FragmentSet

	c.Check(set.Add(Fragment{Begin: 100, End: 200}), gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 200, End: 300}), gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 201, End: 301}), gc.Equals, true)

	c.Check(set, gc.DeepEquals, FragmentSet{
		{Begin: 100, End: 200},
		{Begin: 200, End: 300},
		{Begin: 201, End: 301},
	})
}

func (s *FragmentSuite) TestSetAddReplaceRangeAtEnd(c *gc.C) {
	var set FragmentSet

	c.Check(set.Add(Fragment{Begin: 100, End: 200}), gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 200, End: 300}), gc.Equals, true) // Replaced.
	c.Check(set.Add(Fragment{Begin: 300, End: 400}), gc.Equals, true) // Replaced.
	c.Check(set.Add(Fragment{Begin: 400, End: 500}), gc.Equals, true) // Replaced.
	c.Check(set.Add(Fragment{Begin: 150, End: 500}), gc.Equals, true)

	c.Check(set, gc.DeepEquals, FragmentSet{
		{Begin: 100, End: 200},
		{Begin: 150, End: 500},
	})
}

func (s *FragmentSuite) TestSetAddReplaceOneAtEnd(c *gc.C) {
	var set FragmentSet

	c.Check(set.Add(Fragment{Begin: 100, End: 200}), gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 200, End: 300}), gc.Equals, true) // Replaced.
	c.Check(set.Add(Fragment{Begin: 199, End: 300}), gc.Equals, true)

	c.Check(set, gc.DeepEquals, FragmentSet{
		{Begin: 100, End: 200},
		{Begin: 199, End: 300},
	})

	set = FragmentSet{}
	c.Check(set.Add(Fragment{Begin: 100, End: 200}), gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 200, End: 300}), gc.Equals, true) // Replaced.
	c.Check(set.Add(Fragment{Begin: 200, End: 301}), gc.Equals, true)

	c.Check(set, gc.DeepEquals, FragmentSet{
		{Begin: 100, End: 200},
		{Begin: 200, End: 301},
	})
}

func (s *FragmentSuite) TestSetAddReplaceRangeInMiddle(c *gc.C) {
	var set FragmentSet

	c.Check(set.Add(Fragment{Begin: 100, End: 200}), gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 200, End: 300}), gc.Equals, true) // Replaced.
	c.Check(set.Add(Fragment{Begin: 300, End: 400}), gc.Equals, true) // Replaced.
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
	c.Check(set.Add(Fragment{Begin: 200, End: 300}), gc.Equals, true) // Replaced.
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

	c.Check(set.Add(Fragment{Begin: 100, End: 200}), gc.Equals, true) // Replaced.
	c.Check(set.Add(Fragment{Begin: 200, End: 300}), gc.Equals, true) // Replaced.
	c.Check(set.Add(Fragment{Begin: 300, End: 400}), gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 100, End: 300}), gc.Equals, true)

	c.Check(set, gc.DeepEquals, FragmentSet{
		{Begin: 100, End: 300},
		{Begin: 300, End: 400},
	})
}

func (s *FragmentSuite) TestSetAddReplaceOneAtBeginning(c *gc.C) {
	var set FragmentSet

	c.Check(set.Add(Fragment{Begin: 100, End: 200}), gc.Equals, true) // Replaced.
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

	c.Check(set.Add(Fragment{Begin: 100, End: 200}),
		gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 200, End: 300}),
		gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 300, End: 400}),
		gc.Equals, true)

	// Precondition.
	c.Check(set, gc.DeepEquals, FragmentSet{
		{Begin: 100, End: 200},
		{Begin: 200, End: 300},
		{Begin: 300, End: 400},
	})

	// Expect that only Add()s which exactly replace an entry are accepted.
	c.Check(set.Add(Fragment{Begin: 101, End: 200}), gc.Equals, false)
	c.Check(set.Add(Fragment{Begin: 100, End: 199}), gc.Equals, false)
	c.Check(set.Add(Fragment{Begin: 100, End: 200}), gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 201, End: 300}), gc.Equals, false)
	c.Check(set.Add(Fragment{Begin: 200, End: 299}), gc.Equals, false)
	c.Check(set.Add(Fragment{Begin: 200, End: 300}), gc.Equals, true)
	c.Check(set.Add(Fragment{Begin: 301, End: 400}), gc.Equals, false)
	c.Check(set.Add(Fragment{Begin: 300, End: 400}), gc.Equals, true)

	// Postcondition. No change.
	c.Check(set, gc.DeepEquals, FragmentSet{
		{Begin: 100, End: 200},
		{Begin: 200, End: 300},
		{Begin: 300, End: 400},
	})
}

func (s *FragmentSuite) TestOffset(c *gc.C) {
	var set FragmentSet
	c.Check(set.BeginOffset(), gc.Equals, int64(0))
	c.Check(set.EndOffset(), gc.Equals, int64(0))

	set.Add(Fragment{Begin: 100, End: 150})
	c.Check(set.BeginOffset(), gc.Equals, int64(100))
	c.Check(set.EndOffset(), gc.Equals, int64(150))

	set.Add(Fragment{Begin: 140, End: 250})
	c.Check(set.BeginOffset(), gc.Equals, int64(100))
	c.Check(set.EndOffset(), gc.Equals, int64(250))

	set.Add(Fragment{Begin: 50, End: 100})
	c.Check(set.BeginOffset(), gc.Equals, int64(50))
	c.Check(set.EndOffset(), gc.Equals, int64(250))
}

func (s *FragmentSuite) TestLongestOverlappingFragment(c *gc.C) {
	var set FragmentSet

	set.Add(Fragment{Begin: 100, End: 200})
	set.Add(Fragment{Begin: 149, End: 201})
	set.Add(Fragment{Begin: 200, End: 300})
	set.Add(Fragment{Begin: 299, End: 351})
	set.Add(Fragment{Begin: 300, End: 400})
	set.Add(Fragment{Begin: 500, End: 600})

	c.Check(set.LongestOverlappingFragment(0), gc.Equals, 0)
	c.Check(set.LongestOverlappingFragment(100), gc.Equals, 0)
	c.Check(set.LongestOverlappingFragment(148), gc.Equals, 0)
	c.Check(set.LongestOverlappingFragment(149), gc.Equals, 1)
	c.Check(set.LongestOverlappingFragment(199), gc.Equals, 1)
	c.Check(set.LongestOverlappingFragment(200), gc.Equals, 2)
	c.Check(set.LongestOverlappingFragment(298), gc.Equals, 2)
	c.Check(set.LongestOverlappingFragment(299), gc.Equals, 3)
	c.Check(set.LongestOverlappingFragment(300), gc.Equals, 4)
	c.Check(set.LongestOverlappingFragment(300), gc.Equals, 4)
	c.Check(set.LongestOverlappingFragment(400), gc.Equals, 5) // Not covered.
	c.Check(set.LongestOverlappingFragment(401), gc.Equals, 5) // Not covered.
	c.Check(set.LongestOverlappingFragment(599), gc.Equals, 5)
	c.Check(set.LongestOverlappingFragment(600), gc.Equals, 6)
}

func (s *FragmentSuite) TestIssue97Regression(c *gc.C) {
	var set FragmentSet

	f, _ := ParseFragment("a/journal",
		"0000000000000000-00000000400a8d41-c06dffc4f1ebfa897c93ee09ea209aa913cd2e7d")
	set.Add(f)
	f, _ = ParseFragment("a/journal",
		"00000000400a8d41-00000000595722d7-604d0222b7c79ccf16dd679bffdf3d35bb90322c")
	set.Add(f)
	f, _ = ParseFragment("a/journal",
		"00000000400a8d41-000000007a3fb5ee-a6796f9d09b9404880aa724390a7fa78db340f50")
	set.Add(f)
	f, _ = ParseFragment("a/journal",
		"00000000400a8d41-000000007af92e59-ecb336b0362997cff582d8e78496137a61928b4d")
	set.Add(f)
	f, _ = ParseFragment("a/journal",
		"00000000595722d7-000000007af92fb4-d17340442525025e062dfc50355b90508ddb402f")
	set.Add(f)
	f, _ = ParseFragment("a/journal",
		"000000007a3fb5ee-000000007af92fb4-fe7b4bbbeded5b66cb21ccad1c9d1ce294d48989")
	set.Add(f)
	f, _ = ParseFragment("a/journal",
		"000000007af92e59-000000007af92e59-0000000000000000000000000000000000000000")
	set.Add(f)

	c.Check(set, gc.DeepEquals, FragmentSet{
		Fragment{Journal: "a/journal", Begin: 0, End: 0x400a8d41,
			Sum: [20]uint8{0xc0, 0x6d, 0xff, 0xc4, 0xf1, 0xeb, 0xfa, 0x89, 0x7c, 0x93,
				0xee, 0x9, 0xea, 0x20, 0x9a, 0xa9, 0x13, 0xcd, 0x2e, 0x7d}},
		Fragment{Journal: "a/journal", Begin: 0x400a8d41, End: 0x7af92e59,
			Sum: [20]uint8{0xec, 0xb3, 0x36, 0xb0, 0x36, 0x29, 0x97, 0xcf, 0xf5, 0x82,
				0xd8, 0xe7, 0x84, 0x96, 0x13, 0x7a, 0x61, 0x92, 0x8b, 0x4d}},
		Fragment{Journal: "a/journal", Begin: 0x595722d7, End: 0x7af92fb4,
			Sum: [20]uint8{0xd1, 0x73, 0x40, 0x44, 0x25, 0x25, 0x2, 0x5e, 0x6, 0x2d,
				0xfc, 0x50, 0x35, 0x5b, 0x90, 0x50, 0x8d, 0xdb, 0x40, 0x2f}},
	})
}

var _ = gc.Suite(&FragmentSuite{})
