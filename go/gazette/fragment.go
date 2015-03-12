package gazette

import (
	"sort"
)

type Fragment struct {
	Begin, End int64
	SHA1Sum    []byte
}

func (f Fragment) Size() int64 {
	return f.End - f.Begin
}

// Maintains fragments ordered on |Begin| and |End|, with the invariant that
// no fragment is fully overlapped by another fragment in the set (though it
// may be overlapped by a combination of other fragments). Larger fragments
// are preferred (and will replace spans of overlapped smaller fragments).
// An implication of this invariant is that no two fragments have the same
// |Begin| or |End| (as that would imply an overlap). Both are monotonically
// increasing in the set: set[0].Begin represents the minimum offset, and
// set[len(set)-1].End represents the maximum offset.
type FragmentSet []Fragment

func (s *FragmentSet) BeginOffset() int64 {
	if len(*s) == 0 {
		return 0
	}
	return (*s)[0].Begin
}
func (s *FragmentSet) EndOffset() int64 {
	if len(*s) == 0 {
		return 0
	}
	return (*s)[len(*s)-1].End
}

func (s *FragmentSet) Add(fragment Fragment) bool {
	set := *s

	// O(1) fast path which appends to end.
	if len(set) == 0 || set[len(set)-1].End < fragment.End {
		*s = append(*s, fragment)
		return true
	}
	// Identify the range of set entries to be replaced by this fragment.
	var rBegin, rEnd int

	// Find the first index which covers fragment.Begin.
	rBegin = sort.Search(len(set), func(i int) bool {
		return set[i].End > fragment.Begin
	})
	// Does this index already fully cover fragment?
	if set[rBegin].Begin <= fragment.Begin && set[rBegin].End >= fragment.End {
		return false
	}
	// Skip to the first index not less than fragment.Begin.
	for ; set[rBegin].Begin < fragment.Begin; rBegin++ {
	}
	// Skip to one past the last index <= fragment.End.
	for rEnd = rBegin; rEnd != len(set) && set[rEnd].End <= fragment.End; rEnd++ {
	}

	newSize := len(set) + 1 - (rEnd - rBegin)
	if newSize > len(set) {
		set = append(set, Fragment{})
	}
	// Splice |fragment| into the range [rBegin, rEnd).
	copy(set[rBegin+1:], set[rEnd:])
	set[rBegin] = fragment
	set = set[:newSize]

	*s = set
	return true
}
