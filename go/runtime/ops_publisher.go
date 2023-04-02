package runtime

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/estuary/flow/go/protocols/ops"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/types"
	"go.gazette.dev/core/broker/client"
	"go.gazette.dev/core/message"
)

type OpsPublisher struct {
	labels       labels.ShardLabeling
	shard        *ops.ShardRef
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
		shard:        ops.NewShardRef(labels),
		mapper:       mapper,
		opsLogsSpec:  opsLogsSpec,
		opsStatsSpec: opsStatsSpec,
		publisher:    publisher,
	}, nil
}

func (p *OpsPublisher) Labels() labels.ShardLabeling { return p.labels }

func (p *OpsPublisher) PublishStats(out ops.Stats, immediate bool) error {
	var key, partitions = shardKeyAndPartitions(out.Shard, out.Timestamp)
	out.Meta = &ops.Meta{Uuid: string(pf.DocumentUUIDPlaceholder)}

	var buf bytes.Buffer
	if err := (&jsonpb.Marshaler{}).Marshal(&buf, &out); err != nil {
		panic(fmt.Errorf("marshal of *ops.Stats should always succeed but: %w", err))
	}

	var msg = flow.Mappable{
		Spec:       p.opsStatsSpec,
		Doc:        buf.Bytes(),
		Partitions: partitions,
		PackedKey:  key.Pack(),
	}

	if immediate {
		var _, err = p.publisher.PublishCommitted(p.mapper.Map, msg)
		return err
	} else {
		var _, err = p.publisher.PublishUncommitted(p.mapper.Map, msg)
		return err
	}
}

func (p *OpsPublisher) PublishLog(out ops.Log) {
	var key, partitions = shardKeyAndPartitions(out.Shard, out.Timestamp)
	out.Meta = &ops.Meta{Uuid: string(pf.DocumentUUIDPlaceholder)}

	var buf bytes.Buffer
	if err := (&jsonpb.Marshaler{}).Marshal(&buf, &out); err != nil {
		panic(fmt.Errorf("marshal of *ops.Log should always succeed but: %w", err))
	}

	var msg = flow.Mappable{
		Spec:       p.opsLogsSpec,
		Doc:        json.RawMessage(buf.Bytes()),
		Partitions: partitions,
		PackedKey:  key.Pack(),
	}
	// Best effort. PublishCommitted only fails if the publisher itself is cancelled.
	_, _ = p.publisher.PublishCommitted(p.mapper.Map, msg)
}

func shardKeyAndPartitions(shard *ops.ShardRef, ts *types.Timestamp) (tuple.Tuple, tuple.Tuple) {
	var key = tuple.Tuple{
		shard.Name,
		shard.KeyBegin,
		shard.RClockBegin,
		time.Unix(ts.Seconds, int64(ts.Nanos)).Format(time.RFC3339),
	}
	var partitions = tuple.Tuple{
		shard.Kind.String(),
		shard.Name,
	}
	return key, partitions
}
