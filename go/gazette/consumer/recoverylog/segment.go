package recoverylog

import (
	"errors"
	"fmt"
	"sort"

	pb "go.gazette.dev/core/broker/protocol"
)

// Validate returns an error if the Segment is inconsistent.
func (s Segment) Validate() error {
	if s.Author == 0 {
		return pb.NewValidationError("Author is zero")
	} else if s.FirstSeqNo <= 0 {
		return pb.NewValidationError("FirstSeqNo <= 0")
	} else if s.FirstOffset < 0 {
		return pb.NewValidationError("FirstOffset < 0")
	} else if s.LastSeqNo < s.FirstSeqNo {
		return pb.NewValidationError("LastSeqNo < segment.FirstSeqNo")
	} else if s.LastOffset != 0 && s.LastOffset <= s.FirstOffset {
		return pb.NewValidationError("LastOffset <= segment.FirstOffset")
	} else if err := s.Log.Validate(); err != nil {
		return pb.ExtendContext(err, "Log")
	}
	return nil
}

// SegmentSet is a collection of Segment with the following invariants:
//  * Entries have strictly increasing SeqNo, and are non-overlapping
//    (s[i].LastSeqNo < s[i+1].SeqNo; note this implies a single author
//     for any covered SeqNo).
//  * Entries of the same Log have monotonically increasing FirstOffset.
//  * Entries of the same Log have monotonically increasing LastOffset, however a strict
//    suffix of entries are permitted to have a LastOffset of zero (implied
//    infinite LastOffset).
type SegmentSet []Segment

// Add a Segment to this SegmentSet. An error is returned
// if |segment| would result in an inconsistent SegmentSet.
func (s *SegmentSet) Add(segment Segment) error {
	if err := segment.Validate(); err != nil {
		return err
	}

	// Find the insertion range [begin, end) of |segment|.
	var begin = sort.Search(len(*s), func(i int) bool {
		return (*s)[i].FirstSeqNo >= segment.FirstSeqNo
	})
	// Attempt to reduce |segment| with the previous Segment. The SegmentSet
	// invariant ensures there can be at most one such reduction (FirstSeqNo is
	// strictly increasing).
	if begin > 0 {
		if s, err := reduceSegment((*s)[begin-1], segment); err == nil {
			segment, begin = s, begin-1
		} else if err == errNotReducible {
			// Pass.
		} else {
			return err
		}
	}
	// Perform iterative reductions of |segment| with following Segments of the set.
	var end = begin
	for ; end != len(*s); end++ {
		if s, err := reduceSegment((*s)[end], segment); err == nil {
			segment = s
			continue
		} else if err == errNotReducible {
			break
		} else {
			return err
		}
	}

	// Splice |segment| into the SegmentSet, replacing |begin| to |end| (exclusive).
	if begin == len(*s) {
		*s = append(*s, segment)
	} else {
		*s = append((*s)[:begin+1], (*s)[end:]...)
		(*s)[begin] = segment
	}
	return nil
}

// Intersect returns a slice of this SegmentSet which overlaps with
// the provided [firstOffset, lastOffset) range.
// All Segments of this set MUST have the provided |log|, or Intersect panics.
// Intersect also panics if the first/lastOffset range is mis-ordered.
func (s SegmentSet) Intersect(log pb.Journal, firstOffset, lastOffset int64) SegmentSet {
	if lastOffset < firstOffset {
		panic("invalid range")
	}

	// Find the first Segment which has a FirstOffset >= |lastOffset|, if one exists.
	// This is the (exclusive) end of our range.
	var j = sort.Search(len(s), func(i int) bool {
		if s[i].Log != log {
			panic("segment.Log != log")
		}
		return s[i].FirstOffset >= lastOffset
	})

	// Walk backwards until we find a Segment having LastOffset != 0 and
	// <= |firstOffset|, which is the (exclusive) beginning of our range. Since
	// we enforce LastOffset monotonicity, and also that only a strict suffix
	// may have LastOffset == 0, we're guaranteed no preceding Segment may overlap.
	var i = j
	for ; i != 0 && (s[i-1].LastOffset == 0 || s[i-1].LastOffset > firstOffset); i-- {
	}
	return s[i:j:j]
}

// reduceSegment returns a single Segment which is the reduction of |a| and |b|,
// an error indicating a data-model inconsistency, or errNotReducible if a
// reduction cannot be performed.
func reduceSegment(a, b Segment) (Segment, error) {
	// Establish invariants:
	//  * a.FirstSeqNo <= b.FirstSeqNo.
	//  * If FirstSeqNo's are equal, then a.LastSeqNo >= b.LastSeqNo.
	//
	// This leaves the remaining possible cases:
	//
	//  Overlap:            Covered:         Disjoint:
	//   |---- a ----|       |---- a ----|    |-- a --|
	//      |---- b ----|      |-- b --|                |-- b --|
	//
	//  Equal-right:        Equal-left:
	//   |---- a ----|       |---- a ----|
	//      |-- b ---|       |-- b ---|
	//
	//  Identity:           Adjacent:
	//   |---- a ----|       |-- a --|
	//   |---- b ----|                |-- b --|
	if a.FirstSeqNo > b.FirstSeqNo ||
		a.FirstSeqNo == b.FirstSeqNo && a.LastSeqNo < b.LastSeqNo {
		a, b = b, a
	}

	switch {

	// Offset ordering constraint checks.
	case a.FirstSeqNo < b.FirstSeqNo && a.Log == b.Log && a.FirstOffset > b.FirstOffset:
		return a, fmt.Errorf("expected monotonic FirstOffset: %#v vs %#v", a, b)
	case a.FirstSeqNo < b.FirstSeqNo && a.LastOffset == 0 && b.LastOffset != 0:
		return a, fmt.Errorf("expected preceding Segment to also include LastOffset: %#v vs %#v", a, b)

	case a.LastOffset == 0 && a.Log != b.Log:
		return a, fmt.Errorf("Segment cannot use a different log where preceding Segment.LastOffset is zero: %#v vs %#v", a, b)

	case a.LastSeqNo <= b.LastSeqNo && a.Log == b.Log && nonZeroAndLess(b.LastOffset, a.LastOffset):
		fallthrough
	case a.LastSeqNo >= b.LastSeqNo && a.Log == b.Log && nonZeroAndLess(a.LastOffset, b.LastOffset):
		return a, fmt.Errorf("expected monotonic LastOffset: %#v vs %#v", a, b)

	// Checksum constraint check.
	case a.FirstSeqNo == b.FirstSeqNo && a.FirstChecksum != b.FirstChecksum:
		return a, fmt.Errorf("expected FirstChecksum equality: %#v vs %#v", a, b)

	// Cases where we cannot reduce further:
	case a.LastSeqNo+1 < b.FirstSeqNo: // Disjoint.
		return a, errNotReducible
	case a.LastSeqNo+1 == b.FirstSeqNo && a.Author != b.Author: // Adjacent, but of different authors.
		return a, errNotReducible
	case a.LastSeqNo+1 == b.FirstSeqNo && a.Log != b.Log: // Adjacent, but of different logs.
		return a, errNotReducible

	// Remaining cases have overlap, and therefor must have the same Author & Log.
	case a.Author != b.Author:
		return a, fmt.Errorf("expected Segment Author equality: %#v vs %#v", a, b)
	case a.Log != b.Log:
		return a, fmt.Errorf("expected Segment Log equality: %#v vs %#v", a, b)

	default:
		// |a| & |b| overlap and we're reducing into a single Segment.
		// Invariant: a.FirstSeqNo <= b.FirstSeqNo

		// FirstOffset is a lower-bound. Prefer the tighter (larger) bound.
		if a.FirstSeqNo == b.FirstSeqNo && a.FirstOffset < b.FirstOffset {
			a.FirstOffset = b.FirstOffset
		}
		// LastOffset is optional. Prefer that it be set. Note we've verified monotonicity already.
		if a.LastSeqNo == b.LastSeqNo && a.LastOffset == 0 {
			a.LastOffset = b.LastOffset
		}
		// Extend |a| in the Overlap & Adjacent cases.
		if a.LastSeqNo < b.LastSeqNo {
			a.LastSeqNo, a.LastOffset = b.LastSeqNo, b.LastOffset
		}
		return a, nil
	}
}

// nonZeroAndLess returns whether |a| is non-zero and less-than |b|.
func nonZeroAndLess(a, b int64) bool {
	return a != 0 && a < b
}

var errNotReducible = errors.New("not reducible")
