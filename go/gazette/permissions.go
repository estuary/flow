package cloudstore

import (
	"fmt"
	"io"
	"os"
)

func checkPermissions(ep Endpoint, fname string) error {
	var fs FileSystem
	var err error
	if err = ep.Validate(); err != nil {
		return fmt.Errorf("could not validate endpoint: %s", err)
	}

	if fs, err = ep.Connect(EmptyProperties()); err != nil {
		return fmt.Errorf("could not connect: %s", err)
	}

	defer fs.Close()

	if fname != "" {
		if file, err := fs.OpenFile(fname, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0640); err != nil {
			return fmt.Errorf("could not open file: %s", err)
		} else if _, err := file.Write([]byte("")); err != nil {
			return fmt.Errorf("could not write to file: %s", err)
		} else if err := file.Close(); err != nil {
			return fmt.Errorf("could not flush file: %s", err)
		} else if err := fs.Remove(fname); err != nil {
			return fmt.Errorf("could not remove file: %s", err)
		}
	} else {
		// Try at least to read a file list.
		if dir, err := fs.OpenFile("", os.O_RDONLY, 0); err != nil {
			return fmt.Errorf("could not open directory: %s", err)
		} else if _, err := dir.Readdir(0); err != nil && err != io.EOF {
			return fmt.Errorf("could not read from directory: %s", err)
		}
	}

	return nil
}
