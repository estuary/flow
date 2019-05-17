package fragment

import (
	"testing"

	"github.com/gazette/gazette/v2/protocol"
	gc "github.com/go-check/check"
)

type CoverSetSuite struct{}

func (s *CoverSetSuite) TestSetAddInsertAtEnd(c *gc.C) {
	var set CoverSet

	c.Check(setAdd(&set, 100, 200), gc.Equals, true)
	c.Check(setAdd(&set, 200, 300), gc.Equals, true)
	c.Check(setAdd(&set, 201, 301), gc.Equals, true)

	c.Check(set, gc.DeepEquals, CoverSet{
		{Fragment: protocol.Fragment{Begin: 100, End: 200}},
		{Fragment: protocol.Fragment{Begin: 200, End: 300}},
		{Fragment: protocol.Fragment{Begin: 201, End: 301}},
	})
}

func (s *CoverSetSuite) TestSetAddReplaceRangeAtEnd(c *gc.C) {
	var set CoverSet

	c.Check(setAdd(&set, 100, 200), gc.Equals, true)
	c.Check(setAdd(&set, 200, 300), gc.Equals, true) // Replaced.
	c.Check(setAdd(&set, 300, 400), gc.Equals, true) // Replaced.
	c.Check(setAdd(&set, 400, 500), gc.Equals, true) // Replaced.
	c.Check(setAdd(&set, 150, 500), gc.Equals, true)

	c.Check(set, gc.DeepEquals, CoverSet{
		{Fragment: protocol.Fragment{Begin: 100, End: 200}},
		{Fragment: protocol.Fragment{Begin: 150, End: 500}},
	})
}

func (s *CoverSetSuite) TestSetAddReplaceOneAtEnd(c *gc.C) {
	var set CoverSet

	c.Check(setAdd(&set, 100, 200), gc.Equals, true)
	c.Check(setAdd(&set, 200, 300), gc.Equals, true) // Replaced.
	c.Check(setAdd(&set, 199, 300), gc.Equals, true)

	c.Check(set, gc.DeepEquals, CoverSet{
		{Fragment: protocol.Fragment{Begin: 100, End: 200}},
		{Fragment: protocol.Fragment{Begin: 199, End: 300}},
	})

	set = CoverSet{}
	c.Check(setAdd(&set, 100, 200), gc.Equals, true)
	c.Check(setAdd(&set, 200, 300), gc.Equals, true) // Replaced.
	c.Check(setAdd(&set, 200, 301), gc.Equals, true)

	c.Check(set, gc.DeepEquals, CoverSet{
		{Fragment: protocol.Fragment{Begin: 100, End: 200}},
		{Fragment: protocol.Fragment{Begin: 200, End: 301}},
	})
}

func (s *CoverSetSuite) TestSetAddReplaceRangeInMiddle(c *gc.C) {
	var set CoverSet

	c.Check(setAdd(&set, 100, 200), gc.Equals, true)
	c.Check(setAdd(&set, 200, 300), gc.Equals, true) // Replaced.
	c.Check(setAdd(&set, 300, 400), gc.Equals, true) // Replaced.
	c.Check(setAdd(&set, 400, 500), gc.Equals, true)
	c.Check(setAdd(&set, 150, 450), gc.Equals, true)

	c.Check(set, gc.DeepEquals, CoverSet{
		{Fragment: protocol.Fragment{Begin: 100, End: 200}},
		{Fragment: protocol.Fragment{Begin: 150, End: 450}},
		{Fragment: protocol.Fragment{Begin: 400, End: 500}},
	})
}

func (s *CoverSetSuite) TestSetAddReplaceOneInMiddle(c *gc.C) {
	var set CoverSet

	c.Check(setAdd(&set, 100, 200), gc.Equals, true)
	c.Check(setAdd(&set, 200, 300), gc.Equals, true) // Replaced.
	c.Check(setAdd(&set, 300, 400), gc.Equals, true)
	c.Check(setAdd(&set, 150, 350), gc.Equals, true)

	c.Check(set, gc.DeepEquals, CoverSet{
		{Fragment: protocol.Fragment{Begin: 100, End: 200}},
		{Fragment: protocol.Fragment{Begin: 150, End: 350}},
		{Fragment: protocol.Fragment{Begin: 300, End: 400}},
	})
}

func (s *CoverSetSuite) TestSetAddInsertInMiddleExactBoundaries(c *gc.C) {
	var set CoverSet

	c.Check(setAdd(&set, 100, 200), gc.Equals, true)
	c.Check(setAdd(&set, 300, 400), gc.Equals, true)
	c.Check(setAdd(&set, 200, 300), gc.Equals, true)

	c.Check(set, gc.DeepEquals, CoverSet{
		{Fragment: protocol.Fragment{Begin: 100, End: 200}},
		{Fragment: protocol.Fragment{Begin: 200, End: 300}},
		{Fragment: protocol.Fragment{Begin: 300, End: 400}},
	})
}

func (s *CoverSetSuite) TestSetAddInsertInMiddleCloseBoundaries(c *gc.C) {
	var set CoverSet

	c.Check(setAdd(&set, 100, 200), gc.Equals, true)
	c.Check(setAdd(&set, 300, 400), gc.Equals, true)
	c.Check(setAdd(&set, 201, 299), gc.Equals, true)

	c.Check(set, gc.DeepEquals, CoverSet{
		{Fragment: protocol.Fragment{Begin: 100, End: 200}},
		{Fragment: protocol.Fragment{Begin: 201, End: 299}},
		{Fragment: protocol.Fragment{Begin: 300, End: 400}},
	})
}

func (s *CoverSetSuite) TestSetAddReplaceRangeAtBeginning(c *gc.C) {
	var set CoverSet

	c.Check(setAdd(&set, 100, 200), gc.Equals, true) // Replaced.
	c.Check(setAdd(&set, 200, 300), gc.Equals, true) // Replaced.
	c.Check(setAdd(&set, 300, 400), gc.Equals, true)
	c.Check(setAdd(&set, 100, 300), gc.Equals, true)

	c.Check(set, gc.DeepEquals, CoverSet{
		{Fragment: protocol.Fragment{Begin: 100, End: 300}},
		{Fragment: protocol.Fragment{Begin: 300, End: 400}},
	})
}

func (s *CoverSetSuite) TestSetAddReplaceOneAtBeginning(c *gc.C) {
	var set CoverSet

	c.Check(setAdd(&set, 100, 200), gc.Equals, true) // Replaced.
	c.Check(setAdd(&set, 200, 300), gc.Equals, true)
	c.Check(setAdd(&set, 300, 400), gc.Equals, true)
	c.Check(setAdd(&set, 99, 200), gc.Equals, true)

	c.Check(set, gc.DeepEquals, CoverSet{
		{Fragment: protocol.Fragment{Begin: 99, End: 200}},
		{Fragment: protocol.Fragment{Begin: 200, End: 300}},
		{Fragment: protocol.Fragment{Begin: 300, End: 400}},
	})
}

func (s *CoverSetSuite) TestSetAddInsertAtBeginning(c *gc.C) {
	var set CoverSet

	c.Check(setAdd(&set, 200, 300), gc.Equals, true)
	c.Check(setAdd(&set, 300, 400), gc.Equals, true)
	c.Check(setAdd(&set, 199, 200), gc.Equals, true)

	c.Check(set, gc.DeepEquals, CoverSet{
		{Fragment: protocol.Fragment{Begin: 199, End: 200}},
		{Fragment: protocol.Fragment{Begin: 200, End: 300}},
		{Fragment: protocol.Fragment{Begin: 300, End: 400}},
	})
}

func (s *CoverSetSuite) TestSetAddOverlappingRanges(c *gc.C) {
	var set CoverSet

	c.Check(setAdd(&set, 100, 150), gc.Equals, true)
	c.Check(setAdd(&set, 149, 201), gc.Equals, true)
	c.Check(setAdd(&set, 200, 250), gc.Equals, true)
	c.Check(setAdd(&set, 250, 300), gc.Equals, true)
	c.Check(setAdd(&set, 299, 351), gc.Equals, true)
	c.Check(setAdd(&set, 350, 400), gc.Equals, true)

	c.Check(setAdd(&set, 200, 300), gc.Equals, true)
	c.Check(setAdd(&set, 300, 400), gc.Equals, true)
	c.Check(setAdd(&set, 100, 200), gc.Equals, true)

	c.Check(set, gc.DeepEquals, CoverSet{
		{Fragment: protocol.Fragment{Begin: 100, End: 200}},
		{Fragment: protocol.Fragment{Begin: 149, End: 201}},
		{Fragment: protocol.Fragment{Begin: 200, End: 300}},
		{Fragment: protocol.Fragment{Begin: 299, End: 351}},
		{Fragment: protocol.Fragment{Begin: 300, End: 400}},
	})
}

func (s *CoverSetSuite) TestSetAddNoAction(c *gc.C) {
	var set CoverSet

	c.Check(setAdd(&set, 100, 200), gc.Equals, true)
	c.Check(setAdd(&set, 200, 300), gc.Equals, true)
	c.Check(setAdd(&set, 300, 400), gc.Equals, true)

	// Precondition.
	c.Check(set, gc.DeepEquals, CoverSet{
		{Fragment: protocol.Fragment{Begin: 100, End: 200}},
		{Fragment: protocol.Fragment{Begin: 200, End: 300}},
		{Fragment: protocol.Fragment{Begin: 300, End: 400}},
	})

	// Expect that only Add()s which exactly replace an entry are accepted.
	c.Check(setAdd(&set, 101, 200), gc.Equals, false)
	c.Check(setAdd(&set, 100, 199), gc.Equals, false)
	c.Check(setAdd(&set, 100, 200), gc.Equals, true)
	c.Check(setAdd(&set, 201, 300), gc.Equals, false)
	c.Check(setAdd(&set, 200, 299), gc.Equals, false)
	c.Check(setAdd(&set, 200, 300), gc.Equals, true)
	c.Check(setAdd(&set, 301, 400), gc.Equals, false)
	c.Check(setAdd(&set, 300, 400), gc.Equals, true)

	// Postcondition. No change.
	c.Check(set, gc.DeepEquals, CoverSet{
		{Fragment: protocol.Fragment{Begin: 100, End: 200}},
		{Fragment: protocol.Fragment{Begin: 200, End: 300}},
		{Fragment: protocol.Fragment{Begin: 300, End: 400}},
	})
}

func (s *CoverSetSuite) TestOffset(c *gc.C) {
	var set CoverSet
	c.Check(set.BeginOffset(), gc.Equals, int64(0))
	c.Check(set.EndOffset(), gc.Equals, int64(0))

	setAdd(&set, 100, 150)
	c.Check(set.BeginOffset(), gc.Equals, int64(100))
	c.Check(set.EndOffset(), gc.Equals, int64(150))

	setAdd(&set, 140, 250)
	c.Check(set.BeginOffset(), gc.Equals, int64(100))
	c.Check(set.EndOffset(), gc.Equals, int64(250))

	setAdd(&set, 50, 100)
	c.Check(set.BeginOffset(), gc.Equals, int64(50))
	c.Check(set.EndOffset(), gc.Equals, int64(250))
}

func (s *CoverSetSuite) TestLongestOverlappingFragment(c *gc.C) {
	var set CoverSet

	setAdd(&set, 100, 200)
	setAdd(&set, 149, 201)
	setAdd(&set, 200, 300)
	setAdd(&set, 299, 351)
	setAdd(&set, 300, 400)
	setAdd(&set, 500, 600)

	var cases = []struct {
		offset int64
		found  bool
		ind    int
	}{
		{0, false, 0},
		{100, true, 0},
		{148, true, 0},
		{149, true, 1},
		{199, true, 1},
		{200, true, 2},
		{298, true, 2},
		{299, true, 3},
		{300, true, 4},
		{350, true, 4},
		{399, true, 4},
		{400, false, 5},
		{401, false, 5},
		{499, false, 5},
		{500, true, 5},
		{599, true, 5},
		{600, false, 6},
	}

	for _, tc := range cases {
		var ind, found = set.LongestOverlappingFragment(tc.offset)
		c.Check(ind, gc.Equals, tc.ind)
		c.Check(found, gc.Equals, tc.found)
	}
}

func (s *CoverSetSuite) TestSetDifference(c *gc.C) {
	var a, b CoverSet

	c.Check(setAdd(&b, 100, 200), gc.Equals, true)
	c.Check(setAdd(&b, 150, 225), gc.Equals, true)
	c.Check(setAdd(&b, 225, 250), gc.Equals, true)
	c.Check(setAdd(&b, 250, 300), gc.Equals, true)
	c.Check(setAdd(&b, 301, 400), gc.Equals, true)

	c.Check(setAdd(&a, 99, 105), gc.Equals, true)  // Begins before first Fragment of |b|.
	c.Check(setAdd(&a, 100, 110), gc.Equals, true) // Small Fragment overlapped by one Fragment of |b|.
	c.Check(setAdd(&a, 110, 120), gc.Equals, true) // Another, overlapped by |b|.
	c.Check(setAdd(&a, 130, 210), gc.Equals, true) // Larger Fragment covered by two Fragments in |b|.
	c.Check(setAdd(&a, 150, 225), gc.Equals, true) // Exactly matched by Fragment in |b|.
	c.Check(setAdd(&a, 151, 300), gc.Equals, true) // Larger Fragment covered by three Fragments in |b|.
	c.Check(setAdd(&a, 152, 301), gc.Equals, true) // *Not* covered by |b|, due to 1-byte gap in coverage.
	c.Check(setAdd(&a, 300, 350), gc.Equals, true) // Another, not covered due to gap in coverage.
	c.Check(setAdd(&a, 301, 360), gc.Equals, true) // Fully covered Fragment.
	c.Check(setAdd(&a, 360, 401), gc.Equals, true) // Fragment which spans past |b|'s coverage.
	c.Check(setAdd(&a, 400, 450), gc.Equals, true) // Fragment fully outside |b|'s coverage.

	var out = CoverSetDifference(a, b)

	c.Check(out, gc.DeepEquals, CoverSet{
		{Fragment: protocol.Fragment{Begin: 99, End: 105}},
		{Fragment: protocol.Fragment{Begin: 152, End: 301}},
		{Fragment: protocol.Fragment{Begin: 300, End: 350}},
		{Fragment: protocol.Fragment{Begin: 360, End: 401}},
		{Fragment: protocol.Fragment{Begin: 400, End: 450}},
	})
}

func (s *CoverSetSuite) TestParseWithMultipleOverlaps(c *gc.C) {
	var set CoverSet

	f, _ := protocol.ParseContentName("a/journal",
		"0000000000000000-00000000400a8d41-0000000000000000000000000000000000000000.raw")
	set, _ = set.Add(Fragment{Fragment: f})
	f, _ = protocol.ParseContentName("a/journal",
		"00000000400a8d41-00000000595722d7-0000000000000000000000000000000000000000.raw")
	set, _ = set.Add(Fragment{Fragment: f})
	f, _ = protocol.ParseContentName("a/journal",
		"00000000400a8d41-000000007a3fb5ee-0000000000000000000000000000000000000000.raw")
	set, _ = set.Add(Fragment{Fragment: f})
	f, _ = protocol.ParseContentName("a/journal",
		"00000000400a8d41-000000007af92e59-0000000000000000000000000000000000000000.raw")
	set, _ = set.Add(Fragment{Fragment: f})
	f, _ = protocol.ParseContentName("a/journal",
		"00000000595722d7-000000007af92fb4-0000000000000000000000000000000000000000.raw")
	set, _ = set.Add(Fragment{Fragment: f})
	f, _ = protocol.ParseContentName("a/journal",
		"000000007a3fb5ee-000000007af92fb4-0000000000000000000000000000000000000000.raw")
	set, _ = set.Add(Fragment{Fragment: f})
	f, _ = protocol.ParseContentName("a/journal",
		"000000007af92e59-000000007af92e59-0000000000000000000000000000000000000000.raw")
	set, _ = set.Add(Fragment{Fragment: f})

	const none = protocol.CompressionCodec_NONE

	c.Check(set, gc.DeepEquals, CoverSet{
		{Fragment: protocol.Fragment{Journal: "a/journal", Begin: 0, End: 0x400a8d41, CompressionCodec: none}},
		{Fragment: protocol.Fragment{Journal: "a/journal", Begin: 0x400a8d41, End: 0x7af92e59, CompressionCodec: none}},
		{Fragment: protocol.Fragment{Journal: "a/journal", Begin: 0x595722d7, End: 0x7af92fb4, CompressionCodec: none}},
	})
}

func setAdd(s *CoverSet, begin, end int64) bool {
	var updated bool
	*s, updated = s.Add(Fragment{
		Fragment: protocol.Fragment{Begin: begin, End: end},
	})
	return updated
}

var _ = gc.Suite(&CoverSetSuite{})

func Test(t *testing.T) { gc.TestingT(t) }
