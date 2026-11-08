//go:build windows

package fragment

import (
	"os"
	"runtime"

	log "github.com/sirupsen/logrus"
)

// newSpoolFile creates and returns a temporary file which also has a configured
// runtime finalizer which will remove the file from the file system. This
// behavior is used because Windows does not support removing the one-and-only
// hard link of a currently-open file.
var newSpoolFile = func() (File, error) {
	if f, err := os.CreateTemp("", "spool"); err != nil {
		return nil, err
	} else {
		runtime.SetFinalizer(f, removeFileFinalizer)
		return f, nil
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
