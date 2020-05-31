// Package recoverylog specifies a finite state machine for recording
// and replaying observed filesystem operations into a Gazette journal.
package recoverylog

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	recoveredBytesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gazette_recoverylog_recovered_bytes_total",
		Help: "Cumulative number of bytes recovered to local disk from recovery logs.",
	})
)
