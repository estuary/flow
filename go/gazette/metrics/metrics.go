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

// Keys for gazretention metrics.
const (
	GazretentionParsedFragmentsTotalKey  = "x_gazretention_parsed_fragments_total"
	GazretentionParsedBytesTotalKey      = "x_gazretention_parsed_bytes_total"
	GazretentionDeletedFragmentsTotalKey = "x_gazretention_deleted_fragments_total"
	GazretentionDeletedBytesTotalKey     = "x_gazretention_deleted_bytes_total"
)

// Collectors for gazretention metrics.
var (
	GazretentionDeletedBytesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: GazretentionDeletedBytesTotalKey,
		Help: "Cumulative number of bytes deleted.",
	}, []string{"prefix"})
	GazretentionDeletedFragmentsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: GazretentionDeletedFragmentsTotalKey,
		Help: "Cumulative number of fragments deleted.",
	}, []string{"prefix"})
	GazretentionParsedBytesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: GazretentionParsedBytesTotalKey,
		Help: "Cumulative number of bytes parsed.",
	}, []string{"prefix"})
	GazretentionParsedFragmentsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: GazretentionParsedFragmentsTotalKey,
		Help: "Cumulative number of fragments parsed.",
	}, []string{"prefix"})
)

func GazretentionCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		GazretentionDeletedBytesTotal,
		GazretentionDeletedFragmentsTotal,
		GazretentionParsedBytesTotal,
		GazretentionParsedFragmentsTotal,
	}
}

// Keys for gazette.Client and gazette.WriteService metrics.
const (
	GazetteDiscardBytesTotalKey         = "gazette_discard_bytes_total"
	GazetteReadBytesTotalKey            = "gazette_read_bytes_total"
	GazetteWriteBytesTotalKey           = "gazette_write_bytes_total"
	GazetteWriteCountTotalKey           = "gazette_write_count_total"
	GazetteWriteDurationSecondsTotalKey = "gazette_write_duration_seconds_total"
)

// Collectors for gazette.Client and gazette.WriteService metrics.
// TODO(rupert): Should prefix be GazetteClient-, "gazette_client_-"?
var (
	GazetteDiscardBytesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: GazetteDiscardBytesTotalKey,
		Help: "Cumulative number of bytes read but discarded.",
	})
	GazetteReadBytesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: GazetteReadBytesTotalKey,
		Help: "Cumulative number of bytes read.",
	})
	GazetteWriteBytesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: GazetteWriteBytesTotalKey,
		Help: "Cumulative number of bytes written.",
	})
	GazetteWriteCountTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: GazetteWriteCountTotalKey,
		Help: "Cumulative number of writes.",
	})
	GazetteWriteDurationTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: GazetteWriteDurationSecondsTotalKey,
		Help: "Cumulative number of seconds spent writing.",
	})
)

// GazetteClientCollectors returns the metrics used by gazette.Client and
// gazette.WriteService.
func GazetteClientCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		GazetteDiscardBytesTotal,
		GazetteReadBytesTotal,
		GazetteWriteBytesTotal,
		GazetteWriteCountTotal,
		GazetteWriteDurationTotal,
	}
}

// Keys for consumer.Runner metrics.
const (
	GazetteConsumerTxCountTotalKey          = "gazette_consumer_tx_count_total"
	GazetteConsumerTxMessagesTotalKey       = "gazette_consumer_tx_messages_total"
	GazetteConsumerTxSecondsTotalKey        = "gazette_consumer_tx_seconds_total"
	GazetteConsumerTxStalledSecondsTotalKey = "gazette_consumer_tx_stalled_seconds_total"
)

// Collectors for consumer.Runner metrics.
var (
	GazetteConsumerTxCountTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: GazetteConsumerTxCountTotalKey,
		Help: "Cumulative number of transactions",
	})
	GazetteConsumerTxMessagesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: GazetteConsumerTxMessagesTotalKey,
		Help: "Cumulative number of messages.",
	})
	GazetteConsumerTxSecondsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: GazetteConsumerTxSecondsTotalKey,
		Help: "Cumulative number of seconds processing transactions.",
	})
	GazetteConsumerTxStalledSecondsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: GazetteConsumerTxStalledSecondsTotalKey,
		Help: "Cumulative number of seconds transactions have stalled.",
	})
)

// GazetteConsumerCollectors returns the metrics used by the consumer package.
func GazetteConsumerCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		GazetteConsumerTxCountTotal,
		GazetteConsumerTxMessagesTotal,
		GazetteConsumerTxSecondsTotal,
		GazetteConsumerTxStalledSecondsTotal,
	}
}
