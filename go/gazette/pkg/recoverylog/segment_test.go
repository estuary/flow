package recoverylog

import (
	"math/rand"
	"time"

	gc "github.com/go-check/check"
)

type SegmentSuite struct{}

func (s *SegmentSuite) SetUpTest(c *gc.C) {
	var seed = time.Now().UnixNano()
	c.Log("using seed: ", seed)
	rand.Seed(seed)
}

func (s *SegmentSuite) TestModelIdentity(c *gc.C) {
	// Add items of model to an empty SegmentSet. Expect to get back the model.
	c.Check(permuteAndAdd(c, SegmentSet{}, []Segment(modelSegmentSet())),
		gc.DeepEquals, modelSegmentSet())
}

func (s *SegmentSuite) TestInsertionCases(c *gc.C) {
	c.Check(permuteAndAdd(c, modelSegmentSet(), []Segment{
		{FirstSeqNo: 1, LastSeqNo: 4, Author: 2},
		{FirstSeqNo: 12, LastSeqNo: 12, Author: 2},
		{FirstSeqNo: 16, LastSeqNo: 18, Author: 2},
	}), gc.DeepEquals, SegmentSet{
		{FirstSeqNo: 1, LastSeqNo: 4, Author: 2},
		{FirstSeqNo: 5, LastSeqNo: 10},
		{FirstSeqNo: 11, LastSeqNo: 11, Author: 1},
		{FirstSeqNo: 12, LastSeqNo: 12, Author: 2},
		{FirstSeqNo: 13, LastSeqNo: 15, Author: 1},
		{FirstSeqNo: 16, LastSeqNo: 18, Author: 2},
	})
}

func (s *SegmentSuite) TestMergeCases(c *gc.C) {
	c.Check(permuteAndAdd(c, modelSegmentSet(), []Segment{
		{FirstSeqNo: 1, LastSeqNo: 4},
		{FirstSeqNo: 12, LastSeqNo: 12, Author: 1},
		{FirstSeqNo: 16, LastSeqNo: 18, Author: 1},
	}), gc.DeepEquals, SegmentSet{
		{FirstSeqNo: 1, LastSeqNo: 10},
		{FirstSeqNo: 11, LastSeqNo: 18, Author: 1},
	})
}

func (s *SegmentSuite) TestOverlapCases(c *gc.C) {
	c.Check(permuteAndAdd(c, modelSegmentSet(), []Segment{
		{FirstSeqNo: 5, LastSeqNo: 9},              // No-op.
		{FirstSeqNo: 6, LastSeqNo: 10},             // No-op.
		{FirstSeqNo: 11, LastSeqNo: 11, Author: 1}, // No-op.
		{FirstSeqNo: 13, LastSeqNo: 14, Author: 1}, // No-op.
		{FirstSeqNo: 14, LastSeqNo: 15, Author: 1}, // No-op.
		{FirstSeqNo: 11, LastSeqNo: 15, Author: 1}, // Merges.
	}), gc.DeepEquals, SegmentSet{
		{FirstSeqNo: 5, LastSeqNo: 10},
		{FirstSeqNo: 11, LastSeqNo: 15, Author: 1},
	})
}

func (s *SegmentSuite) TestPointExtensions(c *gc.C) {
	c.Check(permuteAndAdd(c, modelSegmentSet(), []Segment{
		{FirstSeqNo: 16, LastSeqNo: 16, Author: 1},
		{FirstSeqNo: 17, LastSeqNo: 17, Author: 1},
		{FirstSeqNo: 18, LastSeqNo: 18, Author: 1},
		{FirstSeqNo: 19, LastSeqNo: 19, Author: 2},
		{FirstSeqNo: 20, LastSeqNo: 20, Author: 2},
	}), gc.DeepEquals, SegmentSet{
		{FirstSeqNo: 5, LastSeqNo: 10},
		{FirstSeqNo: 11, LastSeqNo: 11, Author: 1},
		{FirstSeqNo: 13, LastSeqNo: 18, Author: 1},
		{FirstSeqNo: 19, LastSeqNo: 20, Author: 2},
	})
}

func (s *SegmentSuite) TestInconsistencyChecks(c *gc.C) {
	var model = modelSegmentSet()
	for i := range model {
		model[i].FirstOffset = int64(i) // Apply increasing offsets to |model|.
	}

	var cases = []Segment{
		// Internal inconsistency.
		{LastSeqNo: 5, FirstSeqNo: 6},
		// Overlapping, incorrect author.
		{FirstSeqNo: 4, LastSeqNo: 5, Author: 2},
		{FirstSeqNo: 10, LastSeqNo: 11, Author: 0},
		{FirstSeqNo: 10, LastSeqNo: 11, Author: 1},
		{FirstSeqNo: 15, LastSeqNo: 15, Author: 2},
		// Non-monotonic offset.
		{FirstSeqNo: 12, LastSeqNo: 12, Author: 1, FirstOffset: 0},
		{FirstSeqNo: 11, LastSeqNo: 12, Author: 1, FirstOffset: 2},
	}

	for _, tc := range cases {
		c.Check(model.Add(tc), gc.NotNil)
	}
}

func permuteAndAdd(c *gc.C, to SegmentSet, from []Segment) SegmentSet {
	for _, i := range rand.Perm(len(from)) {
		c.Check(to.Add(from[i]), gc.IsNil)
	}
	return to
}

func modelSegmentSet() SegmentSet {
	return SegmentSet{
		{FirstSeqNo: 5, LastSeqNo: 10},
		{FirstSeqNo: 11, LastSeqNo: 11, Author: 1},
		{FirstSeqNo: 13, LastSeqNo: 15, Author: 1},
	}
}

var _ = gc.Suite(&SegmentSuite{})
