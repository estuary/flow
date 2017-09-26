package journal

import (
	"io"
	"os"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/pippio/gazette/cloudstore"
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

	return &IndexWatcher{
		journal:     journal,
		cfs:         cfs,
		updates:     updates,
		stop:        make(chan struct{}),
		initialLoad: make(chan struct{}),
	}
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
	var ticker = time.NewTicker(indexWatcherPeriod)
	var signal = w.initialLoad

	for done := false; !done; {
		if err := w.onRefresh(); err != nil {
			log.WithFields(log.Fields{"journal": w.journal, "err": err}).
				Warn("failed to refresh index")
		} else if signal != nil {
			close(signal) // Unblocks reads of |w.initialLoad|.
			signal = nil
		}

		select {
		case <-ticker.C:
		case <-w.stop:
			done = true
		}
	}
	if signal != nil {
		close(signal)
	}
	ticker.Stop()
	close(w.stop)
}

func (w *IndexWatcher) onRefresh() error {
	// Open the fragment directory.
	var dir, err = w.cfs.Open(w.journal.String())
	if os.IsNotExist(err) {
		// Non-existent directories are permitted. In theory, we should be stricter
		// here because the CreateAPI first makes the journal fragment directory.
		// In practice, cloud filesystems don't universally support POSIX directory
		// semantics, so tolerate implementations which return "does not exist".
		return nil
	} else if err != nil {
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
				continue
			}

			if file.Size() == 0 && fragment.Size() > 0 {
				log.WithField("path", file.Name()).Error("zero-length fragment")
				continue
			}

			fragment.RemoteModTime = file.ModTime()
			w.updates <- fragment
		}

		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
	}
	return nil
}
