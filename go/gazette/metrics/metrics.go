package metrics

import "github.com/prometheus/client_golang/prometheus"

// Keys for gazette metrics.
const (
	Fail = "fail"
	Ok   = "ok"
)

// Collectors for gazette.Client and gazette.WriteService metrics.
var (
	GazetteDiscardBytesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gazette_discard_bytes_total",
		Help: "Cumulative number of bytes read and discarded during a fragment seek.",
	})
	GazetteReadBytesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gazette_read_bytes_total",
		Help: "Cumulative number of bytes read.",
	})
	GazetteWriteBytesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gazette_write_bytes_total",
		Help: "Cumulative number of bytes written.",
	})
	GazetteWriteCountTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gazette_write_count_total",
		Help: "Cumulative number of writes.",
	})
	GazetteWriteDurationTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gazette_write_duration_seconds_total",
		Help: "Cumulative number of seconds spent writing.",
	})
	GazetteWriteFailureTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gazette_write_failure_total",
		Help: "Cumulative number of write errors returned to clients.",
	})
)
