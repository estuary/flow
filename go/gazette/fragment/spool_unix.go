// +build !windows

package fragment

import (
	"io/ioutil"
	"os"
)

// newSpoolFile creates and returns a temporary file which has already had its
// one-and-only hard link removed from the file system. So long as the *os.File
// remains open, the OS will defer collecting the allocated inode and reclaiming
// disk resources, but the file becomes unaddressable and its resources released
// to the OS after an explicit call to Close, or if the os.File is garbage-
// collected (such that the runtime finalizer calls Close on our behalf).
var newSpoolFile = func() (File, error) {
	if f, err := ioutil.TempFile("", "spool"); err != nil {
		return f, err
	} else {
		return f, os.Remove(f.Name())
	}
}
