package recoverylog

import (
	"errors"
	"sort"
)

// SegmentSet is a collection of Segment with the following invariants:
//  * Entries have strictly increasing SeqNo, and are non-overlapping
//    (s[i].LastSeqNo < s[i+1].SeqNo; note this implies a single author
//     for any covered SeqNo).
//  * Entries have monotonicly increasing Offset.
type SegmentSet []Segment

// Adds a Segment to this SegmentSet. An error is returned
// if |segment| would result in an inconsistent SegmentSet.
func (s *SegmentSet) Add(segment Segment) error {
	if segment.LastSeqNo < segment.FirstSeqNo {
		return errors.New("segment.LastSeqNo < segment.FirstSeqNo")
	}

	// Find the insertion range [begin, end) of |segment|.
	begin := sort.Search(len(*s), func(i int) bool {
		return (*s)[i].FirstSeqNo >= segment.FirstSeqNo
	})
	// If |segment| overlaps with the previous Segment, merge left. The SegmentSet
	// invariant ensures there can be at most one such merge (FirstSeqNo is
	// strictly increasing).
	if begin > 0 {
		prev := (*s)[begin-1]

		if prev.FirstOffset > segment.FirstOffset {
			// FirstOffset should be monotonically increasing.
			return errors.New("FirstOffset is not monotonically increasing")
		}

		if prev.Author != segment.Author {
			if prev.LastSeqNo >= segment.FirstSeqNo {
				// Overlapping Segments should have the same Author.
				return errors.New("overlapping Segment Authors differ")
			}
		} else if prev.LastSeqNo+1 >= segment.FirstSeqNo {
			// Adjacent or overlapping Segments with same Author.
			// Left-merge |segment| to cover |prev|.
			segment.FirstSeqNo = prev.FirstSeqNo
			segment.FirstOffset = prev.FirstOffset
			segment.FirstChecksum = prev.FirstChecksum
			begin--
		}
	}

	// While |segment| overlaps with next Segment(s), merge-right.
	end := begin
	for ; end != len(*s); end++ {
		next := (*s)[end]

		if next.FirstOffset < segment.FirstOffset {
			// FirstOffset should be monotonically increasing.
			return errors.New("FirstOffset is not monotonically increasing")
		}

		if next.Author != segment.Author {
			if next.FirstSeqNo <= segment.LastSeqNo {
				// Overlapping Segments should have the same Author.
				return errors.New("overlapping Segment Authors differ")
			}
		} else if next.FirstSeqNo <= segment.LastSeqNo+1 {
			// Adjacent or overlapping Segments with same Author.
			// Right-merge |segment| to cover |next|.
			if next.LastSeqNo > segment.LastSeqNo {
				segment.LastSeqNo = next.LastSeqNo
			}
			continue
		}
		break
	}

	if begin == len(*s) {
		*s = append(*s, segment)
	} else {
		*s = append((*s)[:begin+1], (*s)[end:]...)
		(*s)[begin] = segment
	}
	return nil
}
