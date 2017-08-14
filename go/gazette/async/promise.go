// Package async implements a simple Promise API.
package async

import (
	"time"
)

// Promise is a simple notification primitive for asynchronous events.
type Promise chan struct{}

// Resolve wakes any clients currently waiting on the Promise
func (s Promise) Resolve() {
	close(s)
}

// Wait synchronously blocks until the Promise is resolved.
func (s Promise) Wait() {
	<-s
}

// WaitWithPeriodicTask repeatedly invokes |task| with period |period| until
// the Promise is resolved.
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
