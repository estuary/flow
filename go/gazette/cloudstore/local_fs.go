package cloudstore

import (
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
)

type localFs struct {
	http.Dir
	temp bool
}

type localFile struct {
	os.File
}

// For local filesystems, ContentSignature is the filesize
//TODO(Azim): Re-open the file and hash the first ~1MiB or so
func (f localFile) ContentSignature() (string, error) {
	var stat, err = f.Stat()
	if err != nil {
		return "", err
	} else {
		return strconv.FormatInt(stat.Size(), 10), nil
	}
}

func (fs localFs) toPath(name string) string {
	return filepath.Join(filepath.FromSlash(string(fs.Dir)), filepath.FromSlash(name))
}

func (fs localFs) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	if f, err := os.OpenFile(fs.toPath(name), flag, perm); err != nil {
		return nil, err
	} else {
		return &localFile{*f}, err
	}
}

func (fs localFs) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(fs.toPath(path), perm)
}

func (fs localFs) Remove(name string) error {
	return os.Remove(fs.toPath(name))
}

func (fs localFs) ToURL(name, method string, validFor time.Duration) (
	*url.URL, error) {
	return &url.URL{Scheme: "file", Path: fs.toPath(name)}, nil
}

func (fs localFs) ProducesAuthorizedURL() bool {
	return true
}

func (fs localFs) CopyAtomic(to File, from io.Reader) (int64, error) {
	defer to.Close()

	n, err := io.Copy(to, from)
	if err != nil {
		// Approximate cloud semantics by removing the partial file.
		if rmErr := os.Remove(to.(*localFile).Name()); rmErr != nil {
			log.WithFields(log.Fields{"err": rmErr, "path": to.(*localFile).Name()}).
				Warn("failed to remove incompleted localFs transfer")
		}
	}
	return n, err
}

func (fs localFs) Close() error {
	if fs.temp {
		return os.RemoveAll(filepath.FromSlash(string(fs.Dir)))
	}
	return nil
}

func (fs localFs) Walk(root string, walkFn filepath.WalkFunc) error {
	var joined = filepath.Join(string(fs.Dir), root)
	return filepath.Walk(joined, func(path string, info os.FileInfo, err error) error {
		// Per cloudstore convention, do not return directories, only files. However,
		// do not return SkipDir; recurse into the directory.
		// If a file, recast path relative to |fs| root.
		if err != nil {
			return err
		} else if info.IsDir() {
			return nil
		} else if rel, err := filepath.Rel(string(fs.Dir), path); err != nil {
			return err
		} else if rel == "." || rel == ".." {
			// Never return "." or ".." as they are not real directories.
			return nil
		} else {
			return walkFn(rel, info, err)
		}
	})
}
