package async

import (
	"time"
)

// Promise is a simple notification primitive for asynchronous events.
type Promise chan struct{}

// Resolves a Promise to wake any clients currently waiting on it.
func (s Promise) Resolve() {
	close(s)
}

// Synchronously wait for this Promise to be resolved.
func (s Promise) Wait() {
	<-s
}

// Repeatedly invokes |task| with period |period| until the Promise is resolved.
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
