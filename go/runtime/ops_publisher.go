package runtime

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/ops"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"go.gazette.dev/core/broker/client"
	"go.gazette.dev/core/message"
)

type OpsPublisher struct {
	labels       labels.ShardLabeling
	mapper       flow.Mapper
	opsLogsSpec  *pf.CollectionSpec
	opsStatsSpec *pf.CollectionSpec
	publisher    *message.Publisher
}

var _ ops.Publisher = &OpsPublisher{}

func NewOpsPublisher(
	ajc client.AsyncJournalClient,
	labels labels.ShardLabeling,
	mapper flow.Mapper,
	opsLogsSpec *pf.CollectionSpec,
	opsStatsSpec *pf.CollectionSpec,
) (*OpsPublisher, error) {
	// Sanity-check the shape of logs and stats collections.
	if err := ops.ValidateLogsCollection(opsLogsSpec); err != nil {
		return nil, err
	} else if err := ops.ValidateStatsCollection(opsStatsSpec); err != nil {
		return nil, err
	}

	// Passing a nil timepoint to NewPublisher means that the timepoint that's encoded in the
	// UUID of log documents will always reflect the current wall-clock time, even when those
	// log documents were produced during test runs, where `readDelay`s might normally cause
	// time to skip forward. This probably only matters in extremely outlandish test scenarios,
	// and so it doesn't seem worth the complexity to modify this timepoint during tests.
	var publisher = message.NewPublisher(ajc, nil)

	return &OpsPublisher{
		labels:       labels,
		mapper:       mapper,
		opsLogsSpec:  opsLogsSpec,
		opsStatsSpec: opsStatsSpec,
		publisher:    publisher,
	}, nil
}

func (p *OpsPublisher) Labels() labels.ShardLabeling { return p.labels }

// PublishStats implements `ops.Publisher`. The stats event is written using
// `PublishCommitted`, and is committed outside of any consumer transaction.
// This means that this function is not appropriate for use in recording the
// transaction processing stats, and is intended only for recording things that happen
// outside of the normal transaction processing loop, such as network traffic.
func (p *OpsPublisher) PublishStats(stats ops.StatsEvent) {
	// TODO: verify that the key/partitions are correct
	var key = tuple.Tuple{
		stats.Shard.Name,
		stats.Shard.KeyBegin,
		stats.Shard.RClockBegin,
		stats.Timestamp.Format(time.RFC3339),
	}
	var partitions = tuple.Tuple{
		stats.Shard.Kind,
		stats.Shard.Name,
	}
	// flow.Mappable replaces this sentinel in the marshalled JSON bytes.
	stats.Meta.UUID = string(pf.DocumentUUIDPlaceholder)

	var buf, err = json.Marshal(stats)
	if err != nil {
		panic(fmt.Errorf("marshal of ops.StatsEvent should always succeed but: %w", err))
	}

	var mappable = flow.Mappable{
		Spec:       p.opsStatsSpec,
		Doc:        json.RawMessage(buf),
		Partitions: partitions,
		PackedKey:  key.Pack(),
	}
	// Best effort. PublishCommitted only fails if the publisher itself is cancelled.
	_, _ = p.publisher.PublishCommitted(p.mapper.Map, mappable)
}

func (p *OpsPublisher) PublishLog(log ops.Log) {
	var key = tuple.Tuple{
		log.Shard.Name,
		log.Shard.KeyBegin,
		log.Shard.RClockBegin,
		log.Timestamp.Format(time.RFC3339),
	}
	var partitions = tuple.Tuple{
		log.Shard.Kind,
		log.Shard.Name,
	}
	// flow.Mappable replaces this sentinel in the marshalled JSON bytes.
	log.Meta.UUID = string(pf.DocumentUUIDPlaceholder)

	var buf, err = json.Marshal(log)
	if err != nil {
		panic(fmt.Errorf("marshal of ops.Log should always succeed but: %w", err))
	}

	var mappable = flow.Mappable{
		Spec:       p.opsLogsSpec,
		Doc:        json.RawMessage(buf),
		Partitions: partitions,
		PackedKey:  key.Pack(),
	}
	// Best effort. PublishCommitted only fails if the publisher itself is cancelled.
	_, _ = p.publisher.PublishCommitted(p.mapper.Map, mappable)
}

// StatsFormatter creates stats documents for publishing into ops/<tenant>/stats collections.
// This does not actually do the publishing, since that's better handled by the runtime
// applications, which can do so transactionally. This factoring is influenced by the constraints
// imposed by materializations, which can't produce stats until after StartCommit is called.
type StatsFormatter struct {
	txnOpened       time.Time
	partitions      tuple.Tuple
	statsCollection *pf.CollectionSpec
	shard           ops.ShardRef
}

// NewStatsFormatter returns a new StatsFormatter, which will create stats documents for the given
// statsCollection. An error is returned if the statsCollection
// doesn't match the expected partitioning, since extraction of partition fields is done manually
// for ops collections.
func NewStatsFormatter(
	labeling labels.ShardLabeling,
	statsCollection *pf.CollectionSpec,
) (*StatsFormatter, error) {
	if err := ops.ValidateStatsCollection(statsCollection); err != nil {
		return nil, err
	}
	return &StatsFormatter{
		partitions:      tuple.Tuple{labeling.TaskType, labeling.TaskName},
		statsCollection: statsCollection,
		shard:           ops.NewShardRef(labeling),
	}, nil
}

// TxnOpened marks the start of a new transaction, setting the timestamp for the
// next StatsEvent.
func (s *StatsFormatter) TxnOpened() {
	s.txnOpened = time.Now().UTC()
}

// NewStatsEvent returns a new StatsEvent that's initialized with information
// about the shard and transaction timing. The transaction duration will be
// computed by subtracting the time set by `TxnOpened` from the current time.
func (s *StatsFormatter) NewStatsEvent() ops.StatsEvent {
	return ops.StatsEvent{
		Meta:  ops.Meta{UUID: string(pf.DocumentUUIDPlaceholder)},
		Shard: s.shard,
		// Truncate the timestamp for stats events in order to give users a reasonable roll-up of
		// stats by default.
		Timestamp:        s.txnOpened.Truncate(time.Minute),
		TxnCount:         1,
		OpenSecondsTotal: time.Since(s.txnOpened).Seconds(),
	}
}

func (s *StatsFormatter) FormatEvent(event ops.StatsEvent) flow.Mappable {
	var doc, err = json.Marshal(event)
	if err != nil {
		panic(fmt.Sprintf("marshaling stats json cannot fail: %v", err))
	}
	// We currently omit the key from this Mappable, which is fine because we don't actually use it
	// for publishing stats.
	return flow.Mappable{
		Spec:       s.statsCollection,
		Doc:        doc,
		Partitions: s.partitions,
	}
}
