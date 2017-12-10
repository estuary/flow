package gazette

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"sort"
	"time"

	etcd "github.com/coreos/etcd/client"
	gc "github.com/go-check/check"
	"github.com/stretchr/testify/mock"

	"github.com/LiveRamp/gazette/pkg/cloudstore"
	"github.com/LiveRamp/gazette/pkg/consensus"
	"github.com/LiveRamp/gazette/pkg/journal"
)

type PersisterSuite struct {
	keysAPI   *consensus.MockKeysAPI
	cfs       cloudstore.FileSystem
	file      *journal.MockFragmentFile
	fragment  journal.Fragment
	persister *Persister
}

func (s *PersisterSuite) SetUpTest(c *gc.C) {
	s.keysAPI = new(consensus.MockKeysAPI)
	s.cfs = cloudstore.NewTmpFileSystem()
	s.file = &journal.MockFragmentFile{}
	s.fragment = journal.Fragment{
		Journal: "a/journal",
		Begin:   1000,
		End:     1010,
		Sum: [...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
			11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
		File: s.file,
	}
	s.persister = NewPersister("base/directory", s.cfs, s.keysAPI, "route-key")
}

func (s *PersisterSuite) TearDownTest(c *gc.C) {
	c.Check(s.cfs.Close(), gc.IsNil)
}

func (s *PersisterSuite) TestPersistence(c *gc.C) {
	c.Skip("PUB-1980 flake")

	var contentFixture = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	var lockPath = PersisterLocksRoot + s.fragment.ContentName()

	// Expect lock is created, refreshed, and deleted.
	s.keysAPI.On("Set", mock.Anything, lockPath, "route-key",
		&etcd.SetOptions{
			PrevExist: etcd.PrevNoExist,
			TTL:       time.Millisecond,
		}).Return(&etcd.Response{Index: 1234}, nil).Once()

	s.keysAPI.On("Set", mock.Anything, lockPath, "route-key",
		&etcd.SetOptions{
			PrevExist: etcd.PrevExist,
			PrevIndex: 1234,
			TTL:       time.Millisecond,
		}).Return(&etcd.Response{Index: 2345}, nil).Once()

	// We *may* see additional Sets, if the lock refresh Goroutine races and
	// wins against the blocking ReadAt fixture.
	s.keysAPI.On("Set", mock.Anything, lockPath, "route-key",
		&etcd.SetOptions{
			PrevExist: etcd.PrevExist,
			PrevIndex: 2345,
			TTL:       time.Millisecond,
		}).Return(&etcd.Response{Index: 2345}, nil)

	s.keysAPI.On("Delete", mock.Anything, lockPath,
		&etcd.DeleteOptions{PrevIndex: 2345}).Return(&etcd.Response{}, nil)

	// Expect fragment.File to be read. Return a value fixture we'll verify later.
	s.file.On("ReadAt", mock.AnythingOfType("[]uint8"), int64(0)).
		Return(10, nil).
		After(2 * time.Millisecond). // Block long enough to trigger lock refresh.
		Run(func(args mock.Arguments) {
			copy(args.Get(0).([]byte), contentFixture)
		}).Once()

	// Intercept and validate the call to os.Remove.
	s.persister.osRemove = func(path string) error {
		c.Check(path, gc.Equals, "base/directory/a/journal/"+s.fragment.ContentName())
		s.persister.osRemove = nil // Mark we were called.
		return nil
	}

	s.persister.persisterLockTTL = time.Millisecond
	s.persister.convergeOne(s.fragment)

	s.file.AssertExpectations(c)
	c.Check(s.persister.osRemove, gc.IsNil)

	// Verify written content.
	r, err := s.cfs.Open(s.fragment.ContentPath())
	c.Check(err, gc.IsNil)
	content, _ := ioutil.ReadAll(r)
	c.Check(content, gc.DeepEquals, contentFixture)
}

func (s *PersisterSuite) TestLockIsAlreadyHeld(c *gc.C) {
	var lockPath = PersisterLocksRoot + s.fragment.ContentName()

	// Expect lock creation is attempted, but return an error that it exists.
	s.keysAPI.On("Set", mock.Anything, lockPath, "route-key",
		&etcd.SetOptions{
			PrevExist: etcd.PrevNoExist,
			TTL:       kPersisterLockTTL,
		}).Return(nil, etcd.Error{Code: etcd.ErrorCodeNodeExist})

	// Note we're implicitly verifying that the local file is not read,
	// by not setting up expectations.

	// Also expect the local file is left alone.
	s.persister.osRemove = func(string) error {
		c.Log("os.Remove() called")
		c.Fail()
		return nil
	}
	s.persister.convergeOne(s.fragment)

	s.keysAPI.AssertExpectations(c)
	s.file.AssertExpectations(c)

	// Expect it's not present on target filesystem.
	_, err := s.cfs.Open(s.fragment.ContentPath())
	c.Check(os.IsNotExist(err), gc.Equals, true)
}

func (s *PersisterSuite) TestTargetFileAlreadyExists(c *gc.C) {
	var lockPath = PersisterLocksRoot + s.fragment.ContentName()

	{
		c.Assert(s.cfs.MkdirAll(s.fragment.Journal.String(), 0740), gc.IsNil)
		w, err := s.cfs.OpenFile(s.fragment.ContentPath(),
			os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0640)
		c.Check(err, gc.IsNil)
		w.Write([]byte("previous-content"))
		c.Assert(w.Close(), gc.IsNil)
	}

	// Expect a lock to be obtained, and then released.
	s.keysAPI.On("Set", mock.Anything, lockPath, "route-key",
		&etcd.SetOptions{
			PrevExist: etcd.PrevNoExist,
			TTL:       kPersisterLockTTL,
		}).Return(&etcd.Response{Index: 1234}, nil)

	s.keysAPI.On("Delete", mock.Anything, lockPath,
		&etcd.DeleteOptions{PrevIndex: 1234}).Return(&etcd.Response{}, nil)

	// Expect fragment.File is *not* read, but that the file *is* removed.
	s.persister.osRemove = func(path string) error {
		c.Check(path, gc.Equals, "base/directory/a/journal/"+s.fragment.ContentName())
		s.persister.osRemove = nil // Mark we were called.
		return nil
	}
	s.persister.convergeOne(s.fragment)

	s.keysAPI.AssertExpectations(c)
	s.file.AssertExpectations(c)
	c.Check(s.persister.osRemove, gc.IsNil)
}

func (s *PersisterSuite) TestEmptyFragment(c *gc.C) {
	emptyFragment := journal.Fragment{
		Journal: "a/journal",
		Begin:   1000,
		End:     1000,
		Sum:     [20]byte{},
		File:    &journal.MockFragmentFile{},
	}

	// Intercept and validate the call to os.Remove.
	s.persister.osRemove = func(path string) error {
		c.Check(path, gc.Equals, "base/directory/a/journal/"+emptyFragment.ContentName())
		s.persister.osRemove = nil // Mark we were called.
		return nil
	}

	s.persister.Persist(emptyFragment)

	c.Check(s.persister.queue, gc.HasLen, 0)
	c.Check(s.persister.osRemove, gc.IsNil) // Verify osRemove() was called.
}

func (s *PersisterSuite) TestStringFunction(c *gc.C) {
	// Make sure that JSON marshaler doesn't choke on the |File| field.
	fp, err := os.Open("/dev/urandom")
	c.Assert(err, gc.IsNil)
	defer fp.Close()

	s.fragment.File = fp

	// Make sure the code handles multiple offsets of the same journal.
	frag2 := s.fragment
	frag2.Begin = 2000
	frag2.End = 3000

	s.persister.Persist(s.fragment)
	s.persister.Persist(frag2)

	var parsed map[string][]string
	err = json.Unmarshal([]byte(s.persister.String()), &parsed)
	c.Assert(err, gc.IsNil)
	sort.Strings(parsed["a/journal"])

	c.Check(parsed["a/journal"], gc.DeepEquals, []string{
		"00000000000003e8-00000000000003f2-0102030405060708090a0b0c0d0e0f1011121314",
		"00000000000007d0-0000000000000bb8-0102030405060708090a0b0c0d0e0f1011121314",
	})
}

var _ = gc.Suite(&PersisterSuite{})
