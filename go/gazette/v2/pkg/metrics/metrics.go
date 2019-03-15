package metrics

import "github.com/prometheus/client_golang/prometheus"

// Key constants are exported primarily for documentation reasons. Typically,
// they will not be used programmatically outside of defining the collectors.

// Keys for gazette metrics.
const (
	CommittedBytesTotalKey            = "gazette_committed_bytes_total"
	CommitsTotalKey                   = "gazette_commits_total"
	RecoveryLogRecoveredBytesTotalKey = "gazette_recoverylog_recovered_bytes_total"
	StoreRequestKey                   = "gazette_store_requests_total"

	Fail = "fail"
	Ok   = "ok"
)

// Collectors for gazette metrics.
var (
	CommittedBytesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: CommittedBytesTotalKey,
		Help: "Cumulative number of bytes committed.",
	})
	CommitsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: CommitsTotalKey,
		Help: "Cumulative number of commits.",
	}, []string{"status"})
	RecoveryLogRecoveredBytesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: RecoveryLogRecoveredBytesTotalKey,
		Help: "Cumulative number of bytes recovered.",
	})
	StoreRequestTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: StoreRequestKey,
		Help: "Cumulative number of fragment store operations.",
	}, []string{"provider", "operation", "status"})
)

// GazetteBrokerCollectors lists collectors used by the gazette broker.
func GazetteBrokerCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		CommittedBytesTotal,
		CommitsTotal,
		RecoveryLogRecoveredBytesTotal,
		StoreRequestTotal,
	}
}

// Keys for gazconsumer metrics.
const (
	GazconsumerLagBytesKey = "gazconsumer_lag_bytes"
)

// Collectors for gazconsumer metrics.
var (
	GazconsumerLagBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: GazconsumerLagBytesKey,
		Help: "Lag of the consumer on a per shard basis.",
	}, []string{"consumer"})
)

// GazetteconsumerCollectors lists collectors used by the gazette broker.
func GazconsumerCollectors() []prometheus.Collector {
	return []prometheus.Collector{GazconsumerLagBytes}
}

// Keys for gazretention metrics.
const (
	GazretentionDeletedBytesTotalKey      = "gazretention_deleted_bytes_total"
	GazretentionDeletedFragmentsTotalKey  = "gazretention_deleted_fragments_total"
	GazretentionRetainedBytesTotalKey     = "gazretention_retained_bytes_total"
	GazretentionRetainedFragmentsTotalKey = "gazretention_retained_fragments_total"
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
	GazretentionRetainedBytesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: GazretentionRetainedBytesTotalKey,
		Help: "Cumulative number of bytes retained.",
	}, []string{"prefix"})
	GazretentionRetainedFragmentsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: GazretentionRetainedFragmentsTotalKey,
		Help: "Cumulative number of fragments retained.",
	}, []string{"prefix"})
)

func GazretentionCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		GazretentionDeletedBytesTotal,
		GazretentionDeletedFragmentsTotal,
		GazretentionRetainedBytesTotal,
		GazretentionRetainedFragmentsTotal,
	}
}

// Keys for gazette.Client and gazette.WriteService metrics.
const (
	GazetteDiscardBytesTotalKey         = "gazette_discard_bytes_total"
	GazetteReadBytesTotalKey            = "gazette_read_bytes_total"
	GazetteWriteBytesTotalKey           = "gazette_write_bytes_total"
	GazetteWriteCountTotalKey           = "gazette_write_count_total"
	GazetteWriteDurationSecondsTotalKey = "gazette_write_duration_seconds_total"
	GazetteWriteFailureTotalKey         = "gazette_write_failure_total"
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
	GazetteWriteFailureTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: GazetteWriteFailureTotalKey,
		Help: "Cumulative number of write errors returned to clients.",
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
	GazetteConsumerTxConsumeSecondsTotalKey = "gazette_consumer_tx_consume_seconds_total"
	GazetteConsumerTxStalledSecondsTotalKey = "gazette_consumer_tx_stalled_seconds_total"
	GazetteConsumerTxFlushSecondsTotalKey   = "gazette_consumer_tx_flush_seconds_total"
	GazetteConsumerTxSyncSecondsTotalKey    = "gazette_consumer_tx_sync_seconds_total"
	GazetteConsumerConsumedBytesTotalKey    = "gazette_consumer_consumed_bytes_total"
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
	GazetteConsumerTxConsumeSecondsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: GazetteConsumerTxConsumeSecondsTotalKey,
		Help: "Cumulative number of seconds transactions were processing messages.",
	})
	GazetteConsumerTxStalledSecondsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: GazetteConsumerTxStalledSecondsTotalKey,
		Help: "Cumulative number of seconds transactions were stalled waiting for Gazette IO.",
	})
	GazetteConsumerTxFlushSecondsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: GazetteConsumerTxFlushSecondsTotalKey,
		Help: "Cumulative number of seconds transactions were flushing their commit.",
	})
	GazetteConsumerTxSyncSecondsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: GazetteConsumerTxSyncSecondsTotalKey,
		Help: "Cumulative number of seconds transactions were waiting for their commit to sync.",
	})
	GazetteConsumerBytesConsumedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: GazetteConsumerConsumedBytesTotalKey,
		Help: "Cumulative number of bytes consumed.",
	})
)

// GazetteConsumerCollectors returns the metrics used by the consumer package.
func GazetteConsumerCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		GazetteConsumerTxCountTotal,
		GazetteConsumerTxMessagesTotal,
		GazetteConsumerTxSecondsTotal,
		GazetteConsumerTxConsumeSecondsTotal,
		GazetteConsumerTxStalledSecondsTotal,
		GazetteConsumerTxFlushSecondsTotal,
		GazetteConsumerBytesConsumedTotal,
	}
}
