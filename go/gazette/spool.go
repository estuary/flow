package gazette

import (
	"compress/gzip"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/pippio/api-server/logging"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

func (s *Spool) ContentName() string {
	return filepath.Join(s.Journal,
		fmt.Sprintf("%016x-%016x-%x", s.Begin, s.LastCommit, s.LastCommitSum))
}

func ParseContentName(name string) (begin, end int64, sum []byte, err error) {
	fields := strings.Split(name, "-")

	if len(fields) != 3 {
		err = errors.New("wrong format")
	} else if begin, err = strconv.ParseInt(fields[0], 16, 64); err != nil {
	} else if end, err = strconv.ParseInt(fields[1], 16, 64); err != nil {
	} else if sum, err = hex.DecodeString(fields[2]); err != nil {
	} else if begin != end && len(sum) != sha1.Size {
		err = errors.New("invalid checksum")
	} else if begin == end && len(sum) != 0 {
		err = errors.New("invalid checksum")
	} else if end < begin {
		err = errors.New("invalid content range")
	}
	return
}

func (s *Spool) LocalPath() string {
	return filepath.Join(s.BaseLocalDirectory, s.ContentName())
}

func (s *Spool) CommittedSize() int64 {
	return s.LastCommit - s.Begin
}

func (s *Spool) Create() error {
	var err error
	path := s.LocalPath()

	// Create a new backing file. This will fail if the named file exists.
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
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
	if s.Error != nil {
		return s.Error
	}
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
		os.Remove(s.LocalPath()) // Implicit success. Delete local file.
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
	log.WithField("name", s.ContentName()).Info("persisting")

	w, err := context.Create(s.ContentName(), "application/octet-stream",
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
		s.backingFile.Close()
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

func RecoverSpools(localDirectory string) []*Spool {
	var recovered []*Spool

	walk := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		relative, _ := filepath.Rel(localDirectory, path)
		contentName := filepath.Base(path)
		journalName := filepath.Dir(relative)

		begin, end, sum, err := ParseContentName(contentName)
		if err != nil {
			log.WithFields(log.Fields{"err": err, "path": path}).
				Error("failed to recover spool")
			return nil
		}

		spool := &Spool{
			BaseLocalDirectory: localDirectory,
			Journal:            journalName,
			Begin:              begin,
			LastCommit:         end,
			LastCommitSum:      sum,
		}
		if err := spool.Recover(); err != nil {
			log.WithFields(log.Fields{"err": err, "path": path}).
				Error("failed to recover spool")
		} else {
			recovered = append(recovered, spool)
		}
		return nil
	}
	filepath.Walk(localDirectory, walk)
	return recovered
}
