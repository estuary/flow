//go:build windows
// +build windows

package client

import (
	"bufio"
	"io/ioutil"
	"os"
	"runtime"

	log "github.com/sirupsen/logrus"
)

// newAppendBuffer creates and returns a temporary file which also has a configured
// runtime finalizer that will remove the file from the file system. This
// behavior is used because Windows does not support removing the one-and-only
// hard link of a currently-open file.
var newAppendBuffer = func() (*appendBuffer, error) {
	if f, err := ioutil.TempFile("", "gazette-append"); err != nil {
		return nil, err
	} else {
		runtime.SetFinalizer(f, removeFileFinalizer)
		var fb = &appendBuffer{file: f}
		fb.buf = bufio.NewWriterSize(fb, appendBufferSize)
		return fb, nil
	}
}

func removeFileFinalizer(f *os.File) {
	if err := f.Close(); err != nil {
		log.WithFields(log.Fields{"name": f.Name(), "err": err}).Error("failed to Close file in finalizer")
	}
	if err := os.Remove(f.Name()); err != nil {
		log.WithFields(log.Fields{"name": f.Name(), "err": err}).Error("failed to Remove file in finalizer")
	}
}
