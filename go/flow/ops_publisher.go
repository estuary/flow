package flow

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/ops"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"go.gazette.dev/core/broker/client"
	"go.gazette.dev/core/message"
)

type OpsPublisher struct {
	labels       labels.ShardLabeling
	mapper       Mapper
	opsLogsSpec  *pf.CollectionSpec
	opsStatsSpec *pf.CollectionSpec
	publisher    *message.Publisher
}

var _ ops.Publisher = &OpsPublisher{}

func NewOpsPublisher(
	ajc client.AsyncJournalClient,
	labels labels.ShardLabeling,
	mapper Mapper,
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

	var mappable = Mappable{
		Spec:       p.opsLogsSpec,
		Doc:        json.RawMessage(buf),
		Partitions: partitions,
		PackedKey:  key.Pack(),
	}
	// Best effort. PublishCommitted only fails if the publisher itself is cancelled.
	_, _ = p.publisher.PublishCommitted(p.mapper.Map, mappable)
}
