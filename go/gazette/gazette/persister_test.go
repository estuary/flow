package gazette

import (
	"io/ioutil"
	"os"
	"time"

	"github.com/coreos/go-etcd/etcd"
	gc "github.com/go-check/check"
	"github.com/stretchr/testify/mock"

	"github.com/pippio/api-server/cloudstore"
	"github.com/pippio/api-server/discovery"
)

type PersisterSuite struct {
	etcd      *discovery.EtcdMemoryService
	cfs       cloudstore.FileSystem
	file      *MockFragmentFile
	fragment  Fragment
	persister *Persister
}

func (s *PersisterSuite) SetUpTest(c *gc.C) {
	s.etcd = discovery.NewEtcdMemoryService()
	s.etcd.MakeDirectory(PersisterLocksRoot)

	s.cfs = cloudstore.NewTmpFileSystem()
	s.file = &MockFragmentFile{}
	s.fragment = Fragment{
		Journal: "a/journal",
		Begin:   1000,
		End:     1010,
		Sum: [...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
			11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
		File: s.file,
	}
	s.persister = NewPersister("base/directory", s.cfs, s.etcd, "route-key")
}

func (s *PersisterSuite) TearDownTest(c *gc.C) {
	c.Check(s.cfs.Close(), gc.IsNil)
}

func (s *PersisterSuite) TestPersistence(c *gc.C) {
	kContentFixture := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	// Monitor persister locks. Expect a lock to be obtained, refreshed twice,
	// and then released.
	subscriber := &discovery.MockEtcdSubscriber{}
	s.expectLockUnlock(subscriber, 2, c)
	c.Check(s.etcd.Subscribe(PersisterLocksRoot, subscriber), gc.IsNil)

	s.persister.persisterLockTTL = 10 * time.Millisecond

	// Expect fragment.File to be read. Return a value fixture we'll verify later.
	s.file.On("ReadAt", mock.AnythingOfType("[]uint8"), int64(0)).
		Return(10, nil).
		After(13 * time.Millisecond). // Delay, triggering 2 lock refreshes.
		Run(func(args mock.Arguments) {
		copy(args.Get(0).([]byte), kContentFixture)
	}).Once()

	// Intercept and validate the call to os.Remove.
	s.persister.osRemove = func(path string) error {
		c.Check(path, gc.Equals, "base/directory/a/journal/"+s.fragment.ContentName())
		s.persister.osRemove = nil // Mark we were called.
		return nil
	}
	s.persister.convergeOne(s.fragment)

	subscriber.AssertExpectations(c)
	s.file.AssertExpectations(c)
	c.Check(s.persister.osRemove, gc.IsNil)

	// Verify written content.
	r, err := s.cfs.Open(s.fragment.ContentPath())
	c.Check(err, gc.IsNil)
	content, _ := ioutil.ReadAll(r)
	c.Check(content, gc.DeepEquals, kContentFixture)
}

func (s *PersisterSuite) TestLockIsAlreadyHeld(c *gc.C) {
	s.etcd.Create(PersisterLocksRoot+s.fragment.ContentName(), "another-broker", 0)

	// Expect that no persister lock changes are made.
	subscriber := &discovery.MockEtcdSubscriber{}
	subscriber.On("OnEtcdUpdate", mock.Anything, mock.Anything).Return().Once()
	c.Check(s.etcd.Subscribe(PersisterLocksRoot, subscriber), gc.IsNil)

	// Note we're implicitly verifying that the local file is not read,
	// by not setting up expectations.

	// Also expect the local file is left alone.
	s.persister.osRemove = func(string) error {
		c.Log("os.Remove() called")
		c.Fail()
		return nil
	}
	s.persister.convergeOne(s.fragment)

	subscriber.AssertExpectations(c)
	s.file.AssertExpectations(c)

	// Expect it's not present on target filesystem.
	_, err := s.cfs.Open(s.fragment.ContentPath())
	c.Check(os.IsNotExist(err), gc.Equals, true)
}

func (s *PersisterSuite) TestTargetFileAlreadyExists(c *gc.C) {
	{
		c.Assert(s.cfs.MkdirAll(s.fragment.Journal, 0740), gc.IsNil)
		w, err := s.cfs.OpenFile(s.fragment.ContentPath(),
			os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0640)
		c.Check(err, gc.IsNil)
		w.Write([]byte("previous-content"))
		c.Assert(w.Close(), gc.IsNil)
	}

	// Expect a lock to be obtained, and then released.
	subscriber := &discovery.MockEtcdSubscriber{}
	s.expectLockUnlock(subscriber, 0, c)
	c.Check(s.etcd.Subscribe(PersisterLocksRoot, subscriber), gc.IsNil)

	// Expect fragment.File is *not* read, but that the file *is* removed.
	s.persister.osRemove = func(path string) error {
		c.Check(path, gc.Equals, "base/directory/a/journal/"+s.fragment.ContentName())
		s.persister.osRemove = nil // Mark we were called.
		return nil
	}
	s.persister.convergeOne(s.fragment)

	subscriber.AssertExpectations(c)
	s.file.AssertExpectations(c)
	c.Check(s.persister.osRemove, gc.IsNil)
}

func (s *PersisterSuite) expectLockUnlock(sub *discovery.MockEtcdSubscriber,
	refreshCount int, c *gc.C) {

	responseArg := mock.AnythingOfType("*etcd.Response")
	treeArg := mock.AnythingOfType("*etcd.Node")
	lockKey := PersisterLocksRoot + s.fragment.ContentName()

	// Expect callback on initial subscription.
	sub.On("OnEtcdUpdate", responseArg, treeArg).Return().Once()

	// Expect a persister lock to be created.
	sub.On("OnEtcdUpdate", responseArg, treeArg).Return().Run(
		func(args mock.Arguments) {
			response := args.Get(0).(*etcd.Response)
			c.Check(response.Action, gc.Equals, discovery.EtcdCreateOp)
			c.Check(response.Node.Key, gc.Equals, lockKey)
			c.Check(response.Node.Value, gc.Equals, "route-key")
		}).Once()

	if refreshCount != 0 {
		// Expect |refreshCount| refreshes of the lock.
		sub.On("OnEtcdUpdate", responseArg, treeArg).Return().Run(
			func(args mock.Arguments) {
				response := args.Get(0).(*etcd.Response)
				c.Check(response.Action, gc.Equals, discovery.EtcdUpdateOp)
				c.Check(response.Node.Key, gc.Equals, lockKey)
			}).Times(refreshCount)
	}

	// Expect a persister lock to be released.
	sub.On("OnEtcdUpdate", mock.Anything, mock.Anything).Return().Run(
		func(args mock.Arguments) {
			response := args.Get(0).(*etcd.Response)
			c.Check(response.Action, gc.Equals, discovery.EtcdDeleteOp)
			c.Check(response.Node.Key, gc.Equals, lockKey)
		}).Once()
}

var _ = gc.Suite(&PersisterSuite{})
