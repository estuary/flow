package fragment

import (
	"context"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"time"

	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	log "github.com/sirupsen/logrus"
)

// FileSystemStoreRoot is the filesystem path which roots fragment ContentPaths
// of a file:// fragment store. It must be set at program startup prior to use.
var FileSystemStoreRoot = "/dev/null/invalid/example/path/to/store"

type fsCfg struct {
	rewriterCfg
}

type fsBackend struct{}

func (s fsBackend) Provider() string {
	return "fs"
}

func (s fsBackend) SignGet(ep *url.URL, fragment pb.Fragment, _ time.Duration) (string, error) {
	var cfg, err = s.fsCfg(ep)
	if err != nil {
		return "", err
	}

	return "file://" + cfg.rewritePath(ep.Path, fragment.ContentPath()), nil
}

func (s fsBackend) Exists(_ context.Context, ep *url.URL, fragment pb.Fragment) (bool, error) {
	var cfg, err = s.fsCfg(ep)
	if err != nil {
		return false, err
	}

	var path = filepath.Join(FileSystemStoreRoot, filepath.FromSlash(cfg.rewritePath(ep.Path, fragment.ContentPath())))

	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false, nil
	} else if err == nil {
		return true, nil
	} else {
		return false, err
	}
}

func (s fsBackend) Open(_ context.Context, ep *url.URL, fragment pb.Fragment) (io.ReadCloser, error) {
	var cfg, err = s.fsCfg(ep)
	if err != nil {
		return nil, err
	}

	var path = filepath.Join(FileSystemStoreRoot, filepath.FromSlash(cfg.rewritePath(ep.Path, fragment.ContentPath())))
	return os.Open(path)
}

func (s fsBackend) Persist(_ context.Context, ep *url.URL, spool Spool) error {
	var cfg, err = s.fsCfg(ep)
	if err != nil {
		return err
	}

	var path = filepath.Join(FileSystemStoreRoot, filepath.FromSlash(cfg.rewritePath(ep.Path, spool.ContentPath())))

	// Create the journal's fragment directory, if not already present.
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return err
	}

	// Open a temp file under the target path directory.
	var f *os.File
	f, err = ioutil.TempFile(filepath.Dir(path), ".partial-"+filepath.Base(path))
	if err != nil {
		return err
	}

	defer func(name string) {
		if rmErr := os.Remove(f.Name()); rmErr != nil {
			log.WithFields(log.Fields{"err": rmErr, "path": path}).
				Warn("failed to cleanup temp file")
		}
	}(f.Name())

	switch spool.CompressionCodec {
	case pb.CompressionCodec_NONE:
		_, err = io.Copy(f, io.NewSectionReader(spool.File, 0, spool.ContentLength()))
	default:
		_, err = io.Copy(f, io.NewSectionReader(spool.compressedFile, 0, spool.compressedLength))
	}

	if err == nil {
		err = f.Close()
	}
	if err == nil {
		err = os.Link(f.Name(), path)
	}
	return err
}

func (s fsBackend) List(_ context.Context, store pb.FragmentStore, ep *url.URL, name pb.Journal, callback func(pb.Fragment)) error {
	var cfg, err = s.fsCfg(ep)
	if err != nil {
		return err
	}

	var root = filepath.Join(FileSystemStoreRoot, filepath.FromSlash(ep.Path))

	return filepath.Walk(filepath.Join(FileSystemStoreRoot, filepath.FromSlash(cfg.rewritePath(ep.Path, name.String()+"/"))),
		func(path string, info os.FileInfo, err error) error {

			var name string

			if err != nil {
				return err
			} else if info.IsDir() {
				return nil // Descend into directory.
			} else if name, err = filepath.Rel(root, path); err != nil {
				return err
			} else if name == "." || name == ".." {
				// Never return "." or ".." as they are not real directories.
				return nil
			}

			if frag, err := pb.ParseContentPath(filepath.ToSlash(name)); err != nil {
				log.WithFields(log.Fields{"path": path, "err": err}).Warning("parsing fragment")
			} else if info.Size() == 0 && frag.ContentLength() > 0 {
				log.WithFields(log.Fields{"path": path}).Warning("zero-length fragment")
			} else {
				frag.ModTime = info.ModTime().Unix()
				frag.BackingStore = store
				callback(frag)
			}
			return nil
		})
}

func (s fsBackend) Remove(_ context.Context, fragment pb.Fragment) error {
	var ep = fragment.BackingStore.URL()
	var cfg, err = s.fsCfg(ep)
	if err != nil {
		return err
	}

	var path = filepath.Join(FileSystemStoreRoot, filepath.FromSlash(cfg.rewritePath(ep.Path, fragment.ContentPath())))
	return os.Remove(path)
}

func (s fsBackend) fsCfg(ep *url.URL) (cfg fsCfg, err error) {
	err = parseStoreArgs(ep, &cfg)
	return cfg, err
}
