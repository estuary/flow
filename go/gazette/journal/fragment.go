package journal

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/pippio/api-server/cloudstore"
)

type Fragment struct {
	Journal    Name
	Begin, End int64
	Sum        [sha1.Size]byte

	// Backing file of the fragment, if present locally.
	File FragmentFile
	// If fragment is remote, the time of last modification.
	// TODO(johnny): Is this the appropriate factoring?
	RemoteModTime time.Time
}

func (f Fragment) ContentName() string {
	return fmt.Sprintf("%016x-%016x-%x", f.Begin, f.End, f.Sum)
}
func (f *Fragment) ContentPath() string {
	return f.Journal.String() + "/" + f.ContentName()
}

func (f Fragment) Size() int64 {
	return f.End - f.Begin
}

func (f Fragment) ReaderFromOffset(offset int64,
	cfs cloudstore.FileSystem) (io.ReadCloser, error) {

	if f.IsLocal() {
		return ioutil.NopCloser(&BoundedReaderAt{
			f.File, f.End - offset, offset - f.Begin}), nil
	}
	file, err := cfs.Open(f.ContentPath())
	if err != nil {
		return nil, err
	}
	_, err = file.Seek(offset-f.Begin, 0)
	return file, err
}

func (f Fragment) IsLocal() bool {
	return f.File != nil
}

func (f Fragment) AsDirectURL(cfs cloudstore.FileSystem) (*url.URL, error) {
	if f.IsLocal() {
		return nil, errors.New("not a remote fragment")
	}
	return cfs.ToURL(f.ContentPath(), "GET", time.Minute)
}

func ParseFragment(journal Name, contentName string) (Fragment, error) {
	var r Fragment
	var sum []byte
	var err error

	r.Journal = journal
	fields := strings.Split(contentName, "-")

	if len(fields) != 3 {
		err = errors.New("wrong format")
	} else if r.Begin, err = strconv.ParseInt(fields[0], 16, 64); err != nil {
	} else if r.End, err = strconv.ParseInt(fields[1], 16, 64); err != nil {
	} else if sum, err = hex.DecodeString(fields[2]); err != nil {
	} else if len(sum) != sha1.Size {
		err = errors.New("invalid checksum")
	} else if r.End < r.Begin {
		err = errors.New("invalid content range")
	}
	copy(r.Sum[:], sum)
	return r, err
}

func LocalFragments(directory string, journal Name) []Fragment {
	var out []Fragment

	walk := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		relative, _ := filepath.Rel(directory, path)
		journal := Name(filepath.Dir(relative))
		contentName := filepath.Base(path)

		fragment, err := ParseFragment(journal, contentName)
		if err != nil {
			log.WithFields(log.Fields{"err": err, "path": path}).
				Error("failed to parse fragment")
			return nil
		}
		if info.Size() < fragment.Size() {
			log.WithFields(log.Fields{"err": err, "path": path}).
				Error("fragment is truncated")
			return nil
		}
		fragment.File, err = os.Open(path)
		if err != nil {
			log.WithFields(log.Fields{"err": err, "path": path}).
				Error("failed to open local fragment file")
			return nil
		}
		out = append(out, fragment)
		return nil
	}
	filepath.Walk(filepath.Join(directory, journal.String()), walk)
	return out
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
	for rBegin != len(set) {
		// Does this index already fully cover fragment, without being equal to it?
		if (set[rBegin].Begin <= fragment.Begin && set[rBegin].End > fragment.End) ||
			(set[rBegin].Begin < fragment.Begin && set[rBegin].End >= fragment.End) {
			return false
		}
		// Skip to the first index not less than fragment.Begin.
		if set[rBegin].Begin >= fragment.Begin {
			break
		}
		rBegin++
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
