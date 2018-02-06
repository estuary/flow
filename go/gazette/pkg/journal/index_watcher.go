package journal

import (
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/LiveRamp/gazette/pkg/cloudstore"
)

const indexWatcherPeriod = 5 * time.Minute

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
	return w.cfs.Walk(w.journal.String()+"/", NewWalkFuncAdapter(func(fragment Fragment) error {
		w.updates <- fragment
		return nil
	}))
}
