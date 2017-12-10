package journal

import (
	"crypto/sha1"
	"errors"
	"hash"
	"io"
	"os"
	"path/filepath"
)

var ErrInvalidDelta = errors.New("invalid delta")

type Spool struct {
	Fragment
	// Local directory of |Fragment.File|.
	directory string
	// Number of uncommitted bytes written to |Fragment.File| after
	// the committed portion (ending at |Fragment.End|).
	delta int64
	// Incrementally builds |Fragment.Sum| as commits occur.
	sha1Summer hash.Hash
	// Retained IO error.
	err error
}

func NewSpool(directory string, at Mark) (*Spool, error) {
	spool := &Spool{
		Fragment: Fragment{
			Journal: at.Journal,
			Begin:   at.Offset,
			End:     at.Offset,
		},
		directory: directory,
	}
	path := spool.LocalPath()

	err := os.MkdirAll(filepath.Dir(path), 0700)
	if err != nil {
		return nil, err
	}
	// Create a new backing file. This will fail if the named file exists.
	spool.File, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return spool, err
	}
	spool.sha1Summer = sha1.New()

	return spool, err
}

func (s *Spool) Write(buf []byte) (n int, err error) {
	if s.err != nil {
		return 0, s.err
	}
	if n, err := s.File.Write(buf); err != nil {
		return n, s.setErr(err)
	}
	s.delta += int64(len(buf))
	return len(buf), nil
}

func (s *Spool) Commit(delta int64) error {
	if s.err != nil {
		return s.err
	} else if delta > s.delta {
		return ErrInvalidDelta
	} else if s.delta == 0 {
		return nil // Trivial commit.
	}

	// Feed the new portion into sha1Summer, and flush the file.
	if _, err := s.File.Seek(s.End-s.Begin, 0); err != nil {
		return s.setErr(err)
	} else if _, err = io.CopyN(s.sha1Summer, s.File, delta); err != nil {
		return s.setErr(err)
	} else if err := fdatasync(int(s.File.Fd())); err != nil {
		return s.setErr(err)
	}
	s.delta = 0

	// Perform an (atomic) file rename to record the commit.
	previousPath := s.LocalPath()
	s.End += delta
	copy(s.Sum[:], s.sha1Summer.Sum(nil))
	newPath := s.LocalPath()

	return s.setErr(os.Rename(previousPath, newPath))
}

func (s *Spool) LocalPath() string {
	return filepath.Join(s.directory, s.Fragment.ContentPath())
}

func (s *Spool) setErr(err error) error {
	if s.err == nil && err != nil {
		// Retain first error seen.
		s.err = err
	}
	return err
}
