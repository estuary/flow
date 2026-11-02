//go:build !windows
// +build !windows

package client

import (
	"bufio"
	"io/ioutil"
	"os"
)

// newAppendBuffer creates and returns a temporary file which has already had its
// one-and-only hard link removed from the file system. So long as the *os.File
// remains open, the OS will defer collecting the allocated inode and reclaiming
// disk resources, but the file becomes unaddressable and its resources released
// to the OS after the os.File is garbage-collected (such that the runtime
// finalizer calls Close on our behalf).
var newAppendBuffer = func() (*appendBuffer, error) {
	if f, err := ioutil.TempFile("", "gazette-append"); err != nil {
		return nil, err
	} else if err = os.Remove(f.Name()); err != nil {
		return nil, err
	} else {
		var fb = &appendBuffer{file: f}
		fb.buf = bufio.NewWriterSize(fb, appendBufferSize)
		return fb, nil
	}
}
