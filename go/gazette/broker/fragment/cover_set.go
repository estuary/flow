package fragment

import "sort"

// CoverSet maintains Fragments ordered on |Begin| and |End|, with the invariant
// that no Fragment is fully overlapped by another Fragment in the set (though it
// may be overlapped by a combination of other Fragments). Intuitively, CoverSet
// represents the set of offsets which are "covered" by a collection of Fragments,
// and is able to map, for each byte offset, a "best" covering Fragment. It
// employs a heuristic of preferring larger fragments (and will replace spans of
// overlapped smaller fragments). An implication of its invariant is that no two
// Fragments have the same |Begin| or |End| (as that would imply an overlap).
// Both are monotonically increasing in the set: set[0].Begin represents the
// minimum offset, and set[len(set)-1].End represents the maximum offset.
type CoverSet []Fragment

// BeginOffset returns the first (lowest) Begin offset of any Fragment in the CoverSet.
func (s CoverSet) BeginOffset() int64 {
	if len(s) == 0 {
		return 0
	}
	return s[0].Begin
}

// EndOffset returns the last (largest) End offset of any Fragment in the set.
func (s CoverSet) EndOffset() int64 {
	if len(s) == 0 {
		return 0
	}
	return s[len(s)-1].End
}

// Add the Fragment to the CoverSet. The CoverSet is returned, along with an
// indication of whether an offset span was updated to reflect Fragment. All
// updates occur in-place.
func (s CoverSet) Add(fragment Fragment) (CoverSet, bool) {
	var lens = len(s)

	if lens != 0 && fragment.Journal != s[0].Journal {
		panic("journal mismatch")
	}

	// O(1) fast paths:
	// 1) Ignore empty Fragments.
	if fragment.ContentLength() == 0 {
		return s, false
	}
	// 2) CoverSet is empty.
	if lens == 0 {
		s = append(s, fragment)
		return s, true
	}
	// 3) Fragment replaces or is sequenced after the current last Fragment.
	if i := lens - 1; s[i].End < fragment.End {
		if s[i].Begin >= fragment.Begin {
			// Replace.
			s[i] = fragment
		} else {
			s = append(s, fragment)
		}
		return s, true
	}

	// Slow path: Identify the range of set entries to be replaced by this fragment.
	var rBegin, rEnd int

	// Find the first index which covers fragment.Begin.
	rBegin = sort.Search(lens, func(i int) bool {
		return s[i].End > fragment.Begin
	})
	for rBegin != lens {
		// Does this index already fully cover fragment, without being equal to it?
		if (s[rBegin].Begin <= fragment.Begin && s[rBegin].End > fragment.End) ||
			(s[rBegin].Begin < fragment.Begin && s[rBegin].End >= fragment.End) {
			return s, false
		}
		// Skip to the first index not less than fragment.Begin.
		if s[rBegin].Begin >= fragment.Begin {
			break
		}
		rBegin++
	}
	// Skip to one past the last index <= fragment.End.
	for rEnd = rBegin; rEnd != lens && s[rEnd].End <= fragment.End; rEnd++ {
	}

	var newSize = lens + 1 - (rEnd - rBegin)
	if newSize > lens {
		s = append(s, Fragment{})
	}
	// Splice |fragment| into the range [rBegin, rEnd).
	copy(s[rBegin+1:], s[rEnd:])
	s[rBegin] = fragment
	return s[:newSize], true
}

// LongestOverlappingFragment finds and returns the index |ind| of the Fragment
// covering |offset| which also has the most content following |offset|. If no
// fragment covers |offset|, the index of the next Fragment beginning after
// |offset| is returned (which may be beyond the current CoverSet range). |found|
// indicates whether an overlapping Fragment was found.
func (s CoverSet) LongestOverlappingFragment(offset int64) (ind int, found bool) {
	// Find the first index having End > |offset|.
	ind = sort.Search(len(s), func(i int) bool {
		return s[i].End > offset
	})
	found = ind < len(s) && s[ind].Begin <= offset

	// Skip forward so long as the fragment covers |offset|.
	for ; ind+1 < len(s) && s[ind+1].Begin <= offset; ind++ {
	}
	return
}

// CoverSetDifference returns the subset of Fragments in |a| which cover
// byte offsets not also covered by Fragments in |b|.
func CoverSetDifference(a, b CoverSet) CoverSet {
	var out CoverSet

	for len(a) != 0 {
		var ind, _ = b.LongestOverlappingFragment(a[0].Begin)

		// Skip |b| forward to |ind|, as (by construction) no Fragment prior to
		// |ind| can overlap an offset >= a[0].Begin.
		b, ind = b[ind:], 0

		// While the |ind| Fragment in |b| doesn't cover all the way to a[0].End,
		// and the |ind+1| Fragment extends its contiguous span, increment |ind|.
		// In other words, allow a sequence of smaller contiguous or overlapping
		// Fragments in |b| to collectively cover a[0].
		for ; ind+1 < len(b) &&
			b[ind].End < a[0].End &&
			b[ind+1].Begin <= b[ind].End; ind++ {
		}

		if len(b) == ind || a[0].Begin < b[0].Begin || a[0].End > b[ind].End {
			out = append(out, a[0]) // a[0] is not covered.
		}
		a = a[1:]
	}
	return out
}
