package gazette

import (
	"compress/gzip"
	"crypto/sha1"
	"errors"
	"fmt"
	"github.com/pippio/api-server/logging"
	"hash"
	"io"
	"os"
	"path/filepath"
)

type Spool struct {
	BaseLocalDirectory string

	Journal                      string
	Begin, LastCommit, LastWrite int64

	Error       error
	backingFile lockedFile
	SHA1Summer  hash.Hash

	LastCommitSum []byte
}

func NewSpool(localDirectory, journal string, offset int64) *Spool {
	return &Spool{
		BaseLocalDirectory: localDirectory,

		Journal:    journal,
		Begin:      offset,
		LastCommit: offset,
		LastWrite:  offset,
	}
}

func (s *Spool) ContentAddress() string {
	return filepath.Join(s.Journal,
		fmt.Sprintf("%16x-%16x-%x", s.Begin, s.LastCommit, s.LastCommitSum))
}

func (s *Spool) LocalPath() string {
	return filepath.Join(s.BaseLocalDirectory, s.ContentAddress())
}

func (s *Spool) CommittedSize() int64 {
	return s.LastCommit - s.Begin
}

func (s *Spool) Create() error {
	var err error
	path := s.LocalPath()

	// Create a new backing file. This will fail if the named file exists.
	if err = os.MkdirAll(filepath.Dir(path), 0644); err != nil {
		return err
	}
	if s.backingFile, err = openLockedFile(path,
		os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600); err != nil {
		return err
	}
	s.SHA1Summer = sha1.New()
	return nil
}

func (s *Spool) Recover() error {
	var err error

	if s.backingFile, err = openLockedFile(s.LocalPath(),
		os.O_RDONLY, 0600); err != nil {
		return err
	} else if info, err := s.backingFile.File().Stat(); err != nil {
		return err
	} else if info.Size() < s.CommittedSize() {
		return errors.New("spool is truncated")
	}
	return nil
}

func (s *Spool) Write(buf []byte) error {
	// First write?
	if s.CommittedSize() == 0 && s.backingFile == nil {
		if err := s.Create(); err != nil {
			return err // Don't assert(), as this is recoverable.
		}
	}
	if _, err := s.backingFile.File().Write(buf); err != nil {
		return s.assert(err)
	}
	// Interface returns |error|, but can never actually fail.
	_, _ = s.SHA1Summer.Write(buf)
	s.LastWrite += int64(len(buf))
	return nil
}

func (s *Spool) Commit() error {
	if s.Error != nil {
		return s.Error
	} else if err := s.backingFile.File().Sync(); err != nil {
		return s.assert(err)
	}

	previousPath := s.LocalPath()
	s.LastCommit = s.LastWrite
	s.LastCommitSum = s.SHA1Summer.Sum(nil)
	newPath := s.LocalPath()

	return s.assert(os.Rename(previousPath, newPath))
}

func (s *Spool) Persist(context logging.StorageContext) error {
	if s.CommittedSize() == 0 {
		return nil
	}
	if s.backingFile == nil {
		if err := s.Recover(); err != nil {
			return err
		}
	}
	if _, err := s.backingFile.File().Seek(0, os.SEEK_SET); err != nil {
		return err
	}

	w, err := context.Create(s.ContentAddress(), "application/octet-stream",
		"gzip", nil)
	if err != nil {
		return context.ErrorOccurred(err)
	}
	gzipW := gzip.NewWriter(w)

	_, err = io.CopyN(gzipW, s.backingFile.File(), s.CommittedSize())
	if err == nil {
		err = gzipW.Close()
	}
	if err == nil {
		err = w.Close()
	}
	if err != nil {
		return context.ErrorOccurred(err)
	} else {
		os.Remove(s.LocalPath())
		return nil
	}
}

func (s *Spool) assert(err error) error {
	if s.Error == nil && err != nil {
		// Retain first error seen.
		s.Error = err
	}
	return err
}

func (s *Spool) Fragment() Fragment {
	return Fragment{
		Begin:   s.Begin,
		End:     s.LastCommit,
		SHA1Sum: s.LastCommitSum,
	}
}
