// Package broker implements the broker runtime and protocol.JournalServer APIs
// (Read, Append, Replicate, List, Apply). Its `pipeline` type manages the
// coordination of write transactions, and `resolver` the mapping of journal
// names to Routes of responsible brokers. `replica` is a top-level collection
// of runtime state and maintenance tasks associated with the processing of a
// journal. gRPC proxy support is also implemented by this package.
package broker

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	journalServerResponseTimeSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "gazette_journal_server_response_time_seconds",
		Help: "DEPRECATED Response time of JournalServer operations (use gRPC metrics instead of this)",
	}, []string{"operation", "status"})
	writeHeadGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gazette_write_head",
		Help: "Current write head of the journal (i.e., next byte offset to be written).",
	}, []string{"journal"})
)
