package journal

import (
	"io"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/pippio/api-server/cloudstore"
)

const (
	indexWatcherPeriod = 5 * time.Minute

	// The value of 1,000 was chosen as it's the default "maxResults" value in
	// Google Cloud Storage's objects list API:
	//   https://cloud.google.com/storage/docs/json_api/v1/objects/list
	indexWatcherIncrementalLoadSize = 1000
)

// IndexWatcher monitors a journal's storage location in the cloud filesystem
// for new fragments, by performing periodic directory listings. When new
// fragment metadata arrives, it's published to the journal Tail via a shared
// channel, which indexes the fragment and makes it available for read requests.
type IndexWatcher struct {
	journal Name

	cfs    cloudstore.FileSystem
	cursor interface{}

	// Channel into which discovered fragments are produced.
	updates chan<- Fragment

	stop        chan struct{}
	initialLoad chan struct{}
}

func NewIndexWatcher(journal Name, cfs cloudstore.FileSystem,
	updates chan<- Fragment) *IndexWatcher {
	w := &IndexWatcher{
		journal:     journal,
		cfs:         cfs,
		updates:     updates,
		stop:        make(chan struct{}),
		initialLoad: make(chan struct{}),
	}
	return w
}

func (w *IndexWatcher) StartWatchingIndex() *IndexWatcher {
	go w.loop()
	return w
}

func (w *IndexWatcher) WaitForInitialLoad() {
	<-w.initialLoad
}

func (w *IndexWatcher) Stop() {
	w.stop <- struct{}{}
	<-w.stop // Blocks until loop() exits.
}

func (w *IndexWatcher) loop() {
	// Copy so we can locally nil it after closing.
	initialLoad := w.initialLoad

	ticker := time.NewTicker(indexWatcherPeriod)
loop:
	for {
		if err := w.onRefresh(); err != nil {
			log.WithFields(log.Fields{"journal": w.journal, "err": err}).
				Warn("failed to refresh index")
		} else if initialLoad != nil {
			close(initialLoad)
			initialLoad = nil
		}

		select {
		case <-ticker.C:
		case <-w.stop:
			break loop
		}
	}
	if initialLoad != nil {
		// Attempts to wait for initial load will no longer block.
		close(initialLoad)
	}
	ticker.Stop()
	log.WithField("journal", w.journal).Info("index watch loop exiting")
	close(w.stop)
}

func (w *IndexWatcher) onRefresh() error {
	// Add a trailing slash to unambiguously represent a directory. Some cloud
	// FileSystems (eg, GCS) require this if no subordinate files are present.
	dirPath := w.journal.String() + "/"

	// Open the fragment directory, making it first if necessary.
	if err := w.cfs.MkdirAll(dirPath, 0750); err != nil {
		return err
	}
	dir, err := w.cfs.Open(dirPath)
	if err != nil {
		return err
	}
	// Perform iterative incremental loads until no new fragments are available.
	for {
		files, err := dir.Readdir(indexWatcherIncrementalLoadSize)

		for _, file := range files {
			if file.IsDir() {
				log.WithField("path", file.Name()).
					Warning("unexpected directory in fragment index")
				continue
			}
			fragment, err := ParseFragment(w.journal, file.Name())
			if err != nil {
				log.WithFields(log.Fields{"path": file.Name(), "err": err}).
					Warning("failed to parse content-name")
			} else {
				fragment.RemoteModTime = file.ModTime()
				w.updates <- fragment
			}
		}

		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
	}
	return nil
}
