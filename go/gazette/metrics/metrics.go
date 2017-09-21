package metrics

import "github.com/prometheus/client_golang/prometheus"

// Key constants are exported primarily for documentation reasons. Typically,
// they will not be used programmatically outside of defining the collectors.

// Keys for gazette metrics.
const (
	CoalescedAppendsTotalKey          = "x_gazette_coalesced_appends_total"
	CommittedBytesTotalKey            = "x_gazette_committed_bytes_total"
	FailedCommitsTotalKey             = "x_gazette_failed_commits_total"
	RecoveryLogRecoveredBytesTotalKey = "x_gazette_recoverylog_recovered_bytes_total"
)

// Collectors for gazette metrics.
var (
	CoalescedAppendsTotal = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: CoalescedAppendsTotalKey,
		Help: "Number of journal append requests bundled into a single write transaction.",
	})
	CommittedBytesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: CommittedBytesTotalKey,
		Help: "Cumulative number of bytes committed.",
	})
	FailedCommitsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: FailedCommitsTotalKey,
		Help: "Cumulative number of failed commits.",
	})
	RecoveryLogRecoveredBytesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: RecoveryLogRecoveredBytesTotalKey,
		Help: "Cumulative number of bytes recovered.",
	})
)

func GazetteCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		CoalescedAppendsTotal,
		CommittedBytesTotal,
		FailedCommitsTotal,
		RecoveryLogRecoveredBytesTotal,
	}
}

// Keys for gazconsumer metrics.
const (
	GazconsumerLagBytesKey = "x_gazconsumer_lag_bytes"
)

// Collectors for gazconsumer metrics.
var (
	GazconsumerLagBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: GazconsumerLagBytesKey,
		Help: "Lag of the consumer on a per shard basis.",
	}, []string{"consumer"})
)

func GazconsumerCollectors() []prometheus.Collector {
	return []prometheus.Collector{GazconsumerLagBytes}
}
