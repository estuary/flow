package gazette

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type Fragment struct {
	Begin, End int64
	Sum        []byte
}

func (f Fragment) Size() int64 {
	return f.End - f.Begin
}

func ParseFragment(fragment string) (Fragment, error) {
	var r Fragment
	var err error

	fields := strings.Split(fragment, "-")

	if len(fields) != 3 {
		err = errors.New("wrong format")
	} else if r.Begin, err = strconv.ParseInt(fields[0], 16, 64); err != nil {
	} else if r.End, err = strconv.ParseInt(fields[1], 16, 64); err != nil {
	} else if r.Sum, err = hex.DecodeString(fields[2]); err != nil {
	} else if r.Begin != r.End && len(r.Sum) != sha1.Size {
		err = errors.New("invalid checksum")
	} else if r.Begin == r.End && len(r.Sum) != 0 {
		err = errors.New("invalid checksum")
	} else if r.End < r.Begin {
		err = errors.New("invalid content range")
	}
	return r, err
}

func (f Fragment) ContentName() string {
	return fmt.Sprintf("%016x-%016x-%x", f.Begin, f.End, f.Sum)
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

	// O(1) fast paths.
	if fragment.Size() == 0 {
		return false
	}
	if len(set) == 0 {
		*s = append(*s, fragment)
		return true
	}
	if i := len(set) - 1; set[i].End < fragment.End {
		if set[i].Begin >= fragment.Begin {
			// Replace.
			set[i] = fragment
		} else {
			*s = append(*s, fragment)
		}
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

// Finds and returns the fragment covering |offset|, which has the most
// content after |offset|. If no fragment covers |offset|, the first
// fragment beginning after |offset| is returned.
func (s *FragmentSet) LongestOverlappingFragment(offset int64) int {
	set := *s

	// Find the first index having End > |offset|.
	ind := sort.Search(len(set), func(i int) bool {
		return set[i].End > offset
	})
	// Skip forward so long as the fragment covers |offset|.
	for ; ind+1 < len(set) && set[ind+1].Begin <= offset; ind++ {
	}
	return ind
}
