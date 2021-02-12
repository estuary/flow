package recoverylog

import (
	"math/rand"
	"time"

	gc "github.com/go-check/check"
	pb "go.gazette.dev/core/broker/protocol"
)

type SegmentSuite struct{}

func (s *SegmentSuite) SetUpTest(c *gc.C) {
	var seed = time.Now().UnixNano()
	c.Log("using seed: ", seed)
	rand.Seed(seed)
}

func (s *SegmentSuite) TestSegmentValidationCases(c *gc.C) {
	var seg, model = Segment{}, Segment{
		Author:        0xfefe,
		FirstSeqNo:    10,
		FirstChecksum: 0xabab,
		FirstOffset:   0,
		LastSeqNo:     10,
		LastOffset:    1,
		Log:           "a/log",
	}

	for _, tc := range []struct {
		fn  func()
		err string
	}{
		{func() { seg.Author = 0 }, "Author is zero"},
		{func() { seg.FirstSeqNo = 0 }, "FirstSeqNo <= 0"},
		{func() { seg.FirstSeqNo = -1 }, "FirstSeqNo <= 0"},
		{func() { seg.FirstOffset = -1 }, "FirstOffset < 0"},
		{func() { seg.LastSeqNo = 9 }, "LastSeqNo < segment.FirstSeqNo"},
		{func() { seg.FirstOffset, seg.LastOffset = 100, 99 }, "LastOffset <= segment.FirstOffset"},
		{func() { seg.Log = "" }, "Log: invalid length .*"},
	} {
		c.Check(model.Validate(), gc.IsNil)

		seg = model
		tc.fn()
		c.Check(seg.Validate(), gc.ErrorMatches, tc.err)
	}

	// LastOffset may also be zero.
	model.LastOffset = 0
	c.Check(model.Validate(), gc.IsNil)
}

func (s *SegmentSuite) TestSegmentReductionCases(c *gc.C) {
	for _, tc := range []struct {
		a, b, e Segment
		err     string
	}{
		// Overlap.
		{
			a: Segment{Author: 0x1, FirstSeqNo: 2, LastSeqNo: 7, FirstOffset: 200, LastOffset: 700, FirstChecksum: 0x22, Log: A},
			b: Segment{Author: 0x1, FirstSeqNo: 4, LastSeqNo: 9, FirstOffset: 400, LastOffset: 901, FirstChecksum: 0x44, Log: A},
			e: Segment{Author: 0x1, FirstSeqNo: 2, LastSeqNo: 9, FirstOffset: 200, LastOffset: 901, FirstChecksum: 0x22, Log: A},
		},
		// Overlap, non-monotonic FirstOffset.
		{
			a:   Segment{Author: 0x1, FirstSeqNo: 2, LastSeqNo: 7, FirstOffset: 200, LastOffset: 701, FirstChecksum: 0x22, Log: A},
			b:   Segment{Author: 0x1, FirstSeqNo: 4, LastSeqNo: 9, FirstOffset: 100, LastOffset: 901, FirstChecksum: 0x44, Log: A},
			err: "expected monotonic FirstOffset: .*",
		},
		// Overlap, non-monotonic LastOffset.
		{
			a:   Segment{Author: 0x1, FirstSeqNo: 2, LastSeqNo: 7, FirstOffset: 200, LastOffset: 701, FirstChecksum: 0x22, Log: A},
			b:   Segment{Author: 0x1, FirstSeqNo: 4, LastSeqNo: 9, FirstOffset: 400, LastOffset: 700, FirstChecksum: 0x44, Log: A},
			err: "expected monotonic LastOffset: .*",
		},
		// Overlap, and preceding Segment is missing LastOffset.
		{
			a:   Segment{Author: 0x1, FirstSeqNo: 2, LastSeqNo: 7, FirstOffset: 200, LastOffset: 000, FirstChecksum: 0x22, Log: A},
			b:   Segment{Author: 0x1, FirstSeqNo: 4, LastSeqNo: 9, FirstOffset: 400, LastOffset: 901, FirstChecksum: 0x44, Log: A},
			err: "expected preceding Segment to also include LastOffset: .*",
		},
		// Overlap, mismatched authors.
		{
			a:   Segment{Author: 0x1, FirstSeqNo: 2, LastSeqNo: 7, FirstOffset: 200, LastOffset: 701, FirstChecksum: 0x22, Log: A},
			b:   Segment{Author: 0x2, FirstSeqNo: 4, LastSeqNo: 9, FirstOffset: 400, LastOffset: 901, FirstChecksum: 0x44, Log: A},
			err: "expected Segment Author equality: .*",
		},
		// Overlap, mismatched logs.
		{
			a:   Segment{Author: 0x1, FirstSeqNo: 2, LastSeqNo: 7, FirstOffset: 200, LastOffset: 701, FirstChecksum: 0x22, Log: A},
			b:   Segment{Author: 0x1, FirstSeqNo: 4, LastSeqNo: 9, FirstOffset: 400, LastOffset: 901, FirstChecksum: 0x44, Log: B},
			err: "expected Segment Log equality: .*",
		},
		// Covered.
		{
			a: Segment{Author: 0x1, FirstSeqNo: 2, LastSeqNo: 7, FirstOffset: 200, LastOffset: 701, FirstChecksum: 0x22, Log: A},
			b: Segment{Author: 0x1, FirstSeqNo: 3, LastSeqNo: 6, FirstOffset: 300, LastOffset: 000, FirstChecksum: 0x33, Log: A},
			e: Segment{Author: 0x1, FirstSeqNo: 2, LastSeqNo: 7, FirstOffset: 200, LastOffset: 701, FirstChecksum: 0x22, Log: A},
		},
		// Disjoint.
		{
			a:   Segment{Author: 0x1, FirstSeqNo: 2, LastSeqNo: 4, FirstOffset: 200, LastOffset: 401, FirstChecksum: 0x22, Log: A},
			b:   Segment{Author: 0x1, FirstSeqNo: 6, LastSeqNo: 7, FirstOffset: 600, FirstChecksum: 0x66, Log: A},
			err: errNotReducible.Error(),
		},
		// Equal-right (note non-zero LastOffset is preferred).
		{
			a: Segment{Author: 0x1, FirstSeqNo: 2, LastSeqNo: 7, FirstOffset: 200, LastOffset: 701, FirstChecksum: 0x22, Log: A},
			b: Segment{Author: 0x1, FirstSeqNo: 4, LastSeqNo: 7, FirstOffset: 400, LastOffset: 000, FirstChecksum: 0x44, Log: A},
			e: Segment{Author: 0x1, FirstSeqNo: 2, LastSeqNo: 7, FirstOffset: 200, LastOffset: 701, FirstChecksum: 0x22, Log: A},
		},
		// Equal-right, but non-equal LastOffset.
		{
			a:   Segment{Author: 0x1, FirstSeqNo: 2, LastSeqNo: 7, FirstOffset: 200, LastOffset: 701, FirstChecksum: 0x22, Log: A},
			b:   Segment{Author: 0x1, FirstSeqNo: 4, LastSeqNo: 7, FirstOffset: 400, LastOffset: 702, FirstChecksum: 0x44, Log: A},
			err: "expected monotonic LastOffset: .*",
		},
		// Equal-left (note FirstOffset need not be equal, and largest is preferred).
		{
			a: Segment{Author: 0x1, FirstSeqNo: 2, LastSeqNo: 7, FirstOffset: 200, LastOffset: 701, FirstChecksum: 0x22, Log: A},
			b: Segment{Author: 0x1, FirstSeqNo: 2, LastSeqNo: 9, FirstOffset: 210, LastOffset: 901, FirstChecksum: 0x22, Log: A},
			e: Segment{Author: 0x1, FirstSeqNo: 2, LastSeqNo: 9, FirstOffset: 210, LastOffset: 901, FirstChecksum: 0x22, Log: A},
		},
		// Equal-left, but checksum mismatch.
		{
			a:   Segment{Author: 0x1, FirstSeqNo: 2, LastSeqNo: 7, FirstOffset: 200, LastOffset: 701, FirstChecksum: 0x22, Log: A},
			b:   Segment{Author: 0x1, FirstSeqNo: 2, LastSeqNo: 9, FirstOffset: 210, LastOffset: 901, FirstChecksum: 0xbad, Log: A},
			err: "expected FirstChecksum equality: .*",
		},
		// Identity.
		{
			a: Segment{Author: 0x1, FirstSeqNo: 2, LastSeqNo: 7, FirstOffset: 190, LastOffset: 701, FirstChecksum: 0x22, Log: A},
			b: Segment{Author: 0x1, FirstSeqNo: 2, LastSeqNo: 7, FirstOffset: 200, LastOffset: 000, FirstChecksum: 0x22, Log: A},
			e: Segment{Author: 0x1, FirstSeqNo: 2, LastSeqNo: 7, FirstOffset: 200, LastOffset: 701, FirstChecksum: 0x22, Log: A},
		},
		// Adjacent.
		{
			a: Segment{Author: 0x1, FirstSeqNo: 2, LastSeqNo: 4, FirstOffset: 200, LastOffset: 401, FirstChecksum: 0x22, Log: A},
			b: Segment{Author: 0x1, FirstSeqNo: 5, LastSeqNo: 7, FirstOffset: 500, LastOffset: 701, FirstChecksum: 0x55, Log: A},
			e: Segment{Author: 0x1, FirstSeqNo: 2, LastSeqNo: 7, FirstOffset: 200, LastOffset: 701, FirstChecksum: 0x22, Log: A},
		},
		// Adjacent, but different authors.
		{
			a:   Segment{Author: 0x1, FirstSeqNo: 2, LastSeqNo: 4, FirstOffset: 200, LastOffset: 401, FirstChecksum: 0x22, Log: A},
			b:   Segment{Author: 0x2, FirstSeqNo: 5, LastSeqNo: 7, FirstOffset: 500, LastOffset: 701, FirstChecksum: 0x55, Log: A},
			err: errNotReducible.Error(),
		},
		// Adjacent, but different logs.
		{
			a:   Segment{Author: 0x1, FirstSeqNo: 2, LastSeqNo: 4, FirstOffset: 200, LastOffset: 401, FirstChecksum: 0x22, Log: A},
			b:   Segment{Author: 0x1, FirstSeqNo: 5, LastSeqNo: 7, FirstOffset: 500, LastOffset: 701, FirstChecksum: 0x55, Log: B},
			err: errNotReducible.Error(),
		},
		// Adjacent, different logs, but LastOffset is zero.
		{
			a:   Segment{Author: 0x1, FirstSeqNo: 2, LastSeqNo: 4, FirstOffset: 200, LastOffset: 000, FirstChecksum: 0x22, Log: A},
			b:   Segment{Author: 0x1, FirstSeqNo: 5, LastSeqNo: 7, FirstOffset: 500, LastOffset: 000, FirstChecksum: 0x55, Log: B},
			err: "Segment cannot use a different log where preceding Segment.LastOffset is zero: .*",
		},
	} {
		// Verify the expected symmetric reduction.
		var o1, err1 = reduceSegment(tc.a, tc.b)
		var o2, err2 = reduceSegment(tc.b, tc.a)

		if tc.err != "" {
			c.Check(err1, gc.ErrorMatches, tc.err)
			c.Check(err2, gc.ErrorMatches, tc.err)
		} else {
			c.Check(o1, gc.Equals, tc.e)
			c.Check(o2, gc.Equals, tc.e)
			c.Check(err1, gc.IsNil)
			c.Check(err2, gc.IsNil)
		}
	}
}

func (s *SegmentSuite) TestSetIdentityCases(c *gc.C) {
	// Add items of model to an empty SegmentSet. Expect to get back the model.
	c.Check(permuteAndAdd(c, SegmentSet{}, []Segment(modelSegmentSet())),
		gc.DeepEquals, modelSegmentSet())
}

func (s *SegmentSuite) TestSetNonOverlappingInsertionCases(c *gc.C) {
	c.Check(permuteAndAdd(c, modelSegmentSet(), []Segment{
		{Author: 0xf, FirstSeqNo: 1, LastSeqNo: 4, FirstOffset: 100, LastOffset: 401, Log: A},
		{Author: 0xf, FirstSeqNo: 12, LastSeqNo: 12, FirstOffset: 1200, LastOffset: 1201, Log: A},
		{Author: 0xf, FirstSeqNo: 16, LastSeqNo: 18, FirstOffset: 1600, LastOffset: 1801, Log: A},
		{Author: 0xf, FirstSeqNo: 19, LastSeqNo: 20, FirstOffset: 20, LastOffset: 21, Log: B},
	}), gc.DeepEquals, SegmentSet{
		// Insertions are trivially interleaved with existing Segments.
		{Author: 0xf, FirstSeqNo: 1, LastSeqNo: 4, FirstOffset: 100, LastOffset: 401, Log: A},
		{Author: 0xa, FirstSeqNo: 5, LastSeqNo: 10, FirstOffset: 500, LastOffset: 1001, Log: A},
		{Author: 0xb, FirstSeqNo: 11, LastSeqNo: 11, FirstOffset: 1100, LastOffset: 1101, Log: A},
		{Author: 0xf, FirstSeqNo: 12, LastSeqNo: 12, FirstOffset: 1200, LastOffset: 1201, Log: A},
		{Author: 0xb, FirstSeqNo: 13, LastSeqNo: 15, FirstOffset: 1300, LastOffset: 1501, Log: A},
		{Author: 0xf, FirstSeqNo: 16, LastSeqNo: 18, FirstOffset: 1600, LastOffset: 1801, Log: A},
		{Author: 0xf, FirstSeqNo: 19, LastSeqNo: 20, FirstOffset: 20, LastOffset: 21, Log: B},
	})
}

func (s *SegmentSuite) TestSetOverlapCases(c *gc.C) {
	c.Check(permuteAndAdd(c, modelSegmentSet(), []Segment{
		{Author: 0xa, FirstSeqNo: 1, LastSeqNo: 4, FirstOffset: 100, LastOffset: 401, Log: A},
		{Author: 0xb, FirstSeqNo: 12, LastSeqNo: 12, FirstOffset: 1200, LastOffset: 1201, Log: A},
		{Author: 0xb, FirstSeqNo: 16, LastSeqNo: 18, FirstOffset: 1600, Log: A},
	}), gc.DeepEquals, SegmentSet{
		{Author: 0xa, FirstSeqNo: 1, LastSeqNo: 10, FirstOffset: 100, LastOffset: 1001, Log: A}, // FirstOffset updated.
		{Author: 0xb, FirstSeqNo: 11, LastSeqNo: 18, FirstOffset: 1100, Log: A},                 // LastOffset not known.
	})
}

func (s *SegmentSuite) TestSetCoveredCases(c *gc.C) {
	c.Check(permuteAndAdd(c, modelSegmentSet(), []Segment{
		{Author: 0xa, FirstSeqNo: 5, LastSeqNo: 9, FirstOffset: 500, Log: A},    // No-op.
		{Author: 0xa, FirstSeqNo: 6, LastSeqNo: 10, FirstOffset: 600, Log: A},   // No-op.
		{Author: 0xb, FirstSeqNo: 11, LastSeqNo: 11, FirstOffset: 1100, Log: A}, // No-op.
		{Author: 0xb, FirstSeqNo: 13, LastSeqNo: 14, FirstOffset: 1300, Log: A}, // No-op.
		{Author: 0xb, FirstSeqNo: 14, LastSeqNo: 15, FirstOffset: 1400, Log: A}, // No-op.
	}), gc.DeepEquals, modelSegmentSet())
}

func (s *SegmentSuite) TestSetPointExtensions(c *gc.C) {
	c.Check(permuteAndAdd(c, modelSegmentSet(), []Segment{
		{Author: 0xb, FirstSeqNo: 16, FirstOffset: 1600, LastSeqNo: 16, LastOffset: 1601, Log: A},
		{Author: 0xb, FirstSeqNo: 17, FirstOffset: 1700, LastSeqNo: 17, LastOffset: 1701, Log: A},
		{Author: 0xb, FirstSeqNo: 18, FirstOffset: 1800, LastSeqNo: 18, LastOffset: 1801, Log: A},
		{Author: 0xc, FirstSeqNo: 19, FirstOffset: 1900, LastSeqNo: 19, LastOffset: 0000, Log: A},
		{Author: 0xc, FirstSeqNo: 20, FirstOffset: 2000, LastSeqNo: 20, LastOffset: 0000, Log: A},
	}), gc.DeepEquals, SegmentSet{
		{Author: 0xa, FirstSeqNo: 5, LastSeqNo: 10, FirstOffset: 500, LastOffset: 1001, Log: A},
		{Author: 0xb, FirstSeqNo: 11, LastSeqNo: 11, FirstOffset: 1100, LastOffset: 1101, Log: A},
		{Author: 0xb, FirstSeqNo: 13, LastSeqNo: 18, FirstOffset: 1300, LastOffset: 1801, Log: A},
		{Author: 0xc, FirstSeqNo: 19, LastSeqNo: 20, FirstOffset: 1900, LastOffset: 0000, Log: A},
	})
}

func (s *SegmentSuite) TestSetConsistencyChecks(c *gc.C) {
	var model = modelSegmentSet()

	var cases = []struct {
		Segment
		err string
	}{
		// Overlapping, incorrect author.
		{Segment{Author: 0xc, FirstSeqNo: 4, LastSeqNo: 5, FirstOffset: 400, LastOffset: 501, Log: A},
			"expected Segment Author equality: .*"},
		{Segment{Author: 0xa, FirstSeqNo: 10, LastSeqNo: 11, FirstOffset: 1000, LastOffset: 1101, Log: A},
			"expected Segment Author equality: .*"},
		{Segment{Author: 0xb, FirstSeqNo: 10, LastSeqNo: 11, FirstOffset: 1000, LastOffset: 1101, Log: A},
			"expected Segment Author equality: .*"},
		{Segment{Author: 0xc, FirstSeqNo: 15, LastSeqNo: 15, FirstOffset: 1500, LastOffset: 1501, Log: A},
			"expected Segment Author equality: .*"},
		// Overlapping, incorrect log.
		{Segment{Author: 0xa, FirstSeqNo: 4, LastSeqNo: 5, FirstOffset: 400, LastOffset: 501, Log: "wrong"},
			"expected Segment Log equality: .*"},
		// Missing LastOffset.
		{Segment{Author: 0xb, FirstSeqNo: 12, LastSeqNo: 12, FirstOffset: 1200, LastOffset: 000, Log: A},
			"expected preceding Segment to also include LastOffset: .*"},
		// Non-monotonic offsets.
		{Segment{Author: 0xb, FirstSeqNo: 12, LastSeqNo: 12, FirstOffset: 1050, LastOffset: 1200, Log: A},
			"expected monotonic FirstOffset: .*"},
		{Segment{Author: 0xa, FirstSeqNo: 3, LastSeqNo: 3, FirstOffset: 300, LastOffset: 1050, Log: A},
			"expected monotonic LastOffset: .*"},
	}

	for _, tc := range cases {
		c.Check(model.Add(tc.Segment), gc.ErrorMatches, tc.err)
	}
}

func (s *SegmentSuite) TestIntersectionCases(c *gc.C) {
	var model = modelSegmentSet()

	var cases = []struct {
		first, last int64
		expect      SegmentSet
	}{
		{0, 100000, model},
		{0, 500, model[0:0]},
		{0, 501, model[0:1]},
		{1000, 1001, model[0:1]},
		{1000, 1100, model[0:1]},
		{1000, 1101, model[0:2]},
		{1000, 1300, model[0:2]},
		{1000, 1301, model},
		{1000, 100000, model},
		{1001, 1001, model[1:1]},
		{1001, 1100, model[1:1]},
		{1001, 1101, model[1:2]},
		{1100, 1101, model[1:2]},
		{1101, 1101, model[2:2]},
		{1101, 1300, model[2:2]},
		{1101, 1301, model[2:3]},
		{1300, 1301, model[2:3]},
		{1300, 100000, model[2:3]},
		{1500, 100000, model[2:3]},
		{1501, 100000, model[3:3]},
		{10000, 100000, model[3:3]},
	}

	for _, tc := range cases {
		c.Check(model.Intersect(A, tc.first, tc.last), gc.DeepEquals, tc.expect)
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
		{Author: 0xa, FirstSeqNo: 5, LastSeqNo: 10, FirstOffset: 500, LastOffset: 1001, Log: A},
		{Author: 0xb, FirstSeqNo: 11, LastSeqNo: 11, FirstOffset: 1100, LastOffset: 1101, Log: A},
		{Author: 0xb, FirstSeqNo: 13, LastSeqNo: 15, FirstOffset: 1300, LastOffset: 1501, Log: A},
	}
}

var (
	_            = gc.Suite(&SegmentSuite{})
	A pb.Journal = "log/A"
	B pb.Journal = "log/B"
)
