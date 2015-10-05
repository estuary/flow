package gazette

import (
	"io"
	"time"
)

// Client interface for writing a []byte payload to a journal. Returns a
// Promise which is resolved when the write has been fully committed.
type JournalWriter interface {
	Write(journal string, buffer []byte) (Promise, error)
}

// Client interface for opening a journal at an offset.
type JournalOpener interface {
	OpenJournalAt(journal string, offset int64) (io.ReadCloser, error)
}

// Promise is a simple notification primitive for asynchronous events.
type Promise chan struct{}

func (s Promise) Resolve() {
	close(s)
}

func (s Promise) Wait() {
	<-s
}

// Repeatedly invokes |task| with period |period| until the Promise resolves.
func (s Promise) WaitWithPeriodicTask(period time.Duration, task func()) {
	ticker := time.NewTicker(period)

	for {
		select {
		case <-s:
			ticker.Stop()
			return
		case <-ticker.C:
			task()
		}
	}
}
