package fs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gorilla/schema"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/stores"
	"go.gazette.dev/core/broker/stores/common"
)

// FileSystemStoreRoot is the filesystem path which roots fragment ContentPaths
// of a file:// fragment store. It must be set at program startup prior to use.
var FileSystemStoreRoot = "/dev/null/must/configure/file/store/root"

// StoreQueryArgs contains fields that are parsed from the query arguments
// of a file:// fragment store URL.
type StoreQueryArgs struct {
	common.RewriterConfig
}

type store struct {
	args   StoreQueryArgs
	prefix string
}

// New creates a new filesystem Store from the provided URL.
func New(ep *url.URL) (stores.Store, error) {
	var s = &store{}
	s.prefix = ep.Path
	return s, parseStoreArgs(ep, &s.args)
}

func (s store) SignGet(path string, _ time.Duration) (string, error) {
	return "file://" + s.args.RewritePath(s.prefix, path), nil
}

func (s store) Exists(_ context.Context, path string) (bool, error) {
	var fsPath = filepath.Join(FileSystemStoreRoot, filepath.FromSlash(s.args.RewritePath(s.prefix, path)))

	if _, err := os.Stat(fsPath); os.IsNotExist(err) {
		return false, nil
	} else if err == nil {
		return true, nil
	} else {
		return false, err
	}
}

func (s store) Get(_ context.Context, path string) (io.ReadCloser, error) {
	var fsPath = filepath.Join(FileSystemStoreRoot, filepath.FromSlash(s.args.RewritePath(s.prefix, path)))
	return os.Open(fsPath)
}

func (s store) Put(_ context.Context, path string, content io.ReaderAt, contentLength int64, contentEncoding string) error {
	// Verify that the base directory exists (FileSystemStoreRoot + prefix)
	var baseDir = filepath.Join(FileSystemStoreRoot, filepath.FromSlash(s.prefix))
	if _, err := os.Stat(baseDir); err != nil {
		return fmt.Errorf("%s %s: %w", invalidFileStoreDirectory, baseDir, err)
	}
	var fsPath = filepath.Join(FileSystemStoreRoot, filepath.FromSlash(s.args.RewritePath(s.prefix, path)))

	if err := os.MkdirAll(filepath.Dir(fsPath), 0750); err != nil {
		return err
	}

	var f *os.File
	var err error
	f, err = os.CreateTemp(filepath.Dir(fsPath), ".partial-"+filepath.Base(fsPath))
	if err != nil {
		return err
	}

	defer func(name string) {
		if rmErr := os.Remove(f.Name()); rmErr != nil && !os.IsNotExist(rmErr) {
			log.WithFields(log.Fields{"err": rmErr, "path": fsPath}).
				Warn("failed to cleanup temp file")
		}
	}(f.Name())

	// io.Copy only needs io.Reader, so we use io.NewSectionReader to adapt io.ReaderAt
	_, err = io.Copy(f, io.NewSectionReader(content, 0, contentLength))

	if err == nil {
		err = f.Close()
	}
	if err == nil {
		err = os.Rename(f.Name(), fsPath)
	}
	return err
}

func (s store) List(_ context.Context, prefix string, callback func(path string, modTime time.Time) error) error {
	var dir = filepath.Join(FileSystemStoreRoot,
		filepath.FromSlash(s.args.RewritePath(s.prefix, prefix)))

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return nil
	}
	return filepath.Walk(dir,
		func(name string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			} else if info.IsDir() {
				return nil // Descend into directory.
			}

			// Convert absolute path to relative path from the listing directory
			relPath, err := filepath.Rel(dir, name)
			if err != nil {
				return err
			}

			// Convert to forward slashes for consistency
			return callback(filepath.ToSlash(relPath), info.ModTime())
		})
}

func (s store) Remove(_ context.Context, path string) error {
	var fsPath = filepath.Join(FileSystemStoreRoot, filepath.FromSlash(s.args.RewritePath(s.prefix, path)))
	return os.Remove(fsPath)
}

func (s store) IsAuthError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, os.ErrPermission) || os.IsPermission(err) || strings.Contains(err.Error(), invalidFileStoreDirectory)
}

func parseStoreArgs(ep *url.URL, args interface{}) error {
	var decoder = schema.NewDecoder()
	decoder.IgnoreUnknownKeys(false)

	if q, err := url.ParseQuery(ep.RawQuery); err != nil {
		return err
	} else if err = decoder.Decode(args, q); err != nil {
		return fmt.Errorf("parsing store URL arguments: %s", err)
	}
	return nil
}

const invalidFileStoreDirectory = "invalid file store directory"
