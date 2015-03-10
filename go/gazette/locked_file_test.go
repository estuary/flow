package gazette

import (
	gc "github.com/go-check/check"
	"io/ioutil"
	"os"
	"path/filepath"
	"syscall"
)

type LockedFileSuite struct{}

func (s *LockedFileSuite) TestAttemptToOpenTwice(c *gc.C) {
	dir, _ := ioutil.TempDir("", "locked-file-suite")
	path := filepath.Join(dir, "file")
	defer os.RemoveAll(dir)

	f1, err := openLockedFile(path, os.O_RDWR|os.O_CREATE, 0644)
	c.Assert(f1, gc.NotNil)
	c.Assert(err, gc.IsNil)

	f2, err := openLockedFile(path, os.O_RDONLY, 0644)
	c.Assert(f2, gc.IsNil)
	c.Assert(err, gc.Equals, syscall.Errno(syscall.EAGAIN))

	c.Assert(f1.Close(), gc.IsNil)

	f2, err = openLockedFile(path, os.O_RDONLY, 0644)
	c.Assert(f2, gc.NotNil)
	c.Assert(err, gc.IsNil)
}

var _ = gc.Suite(&LockedFileSuite{})
