package runtime

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/estuary/flow/go/protocols/ops"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/types"
	"go.gazette.dev/core/broker/client"
	"go.gazette.dev/core/message"
)

type OpsPublisher struct {
	logsPublisher *message.Publisher
	mapper        flow.Mapper
	mu            sync.Mutex

	// Fields that update with each task term:
	labels       ops.ShardLabeling
	opsLogsSpec  *pf.CollectionSpec
	opsStatsSpec *pf.CollectionSpec
	shard        *ops.ShardRef
}

var _ ops.Publisher = &OpsPublisher{}

func NewOpsPublisher(
	logsPublisher *message.Publisher,
	mapper flow.Mapper,
) *OpsPublisher {
	return &OpsPublisher{
		logsPublisher: logsPublisher,
		mapper:        mapper,
		mu:            sync.Mutex{},
	}
}

func (p *OpsPublisher) UpdateLabels(
	labels ops.ShardLabeling,
	opsLogsSpec *pf.CollectionSpec,
	opsStatsSpec *pf.CollectionSpec,
) error {
	// Sanity-check the shape of logs and stats collections.
	if err := ops.ValidateLogsCollection(opsLogsSpec); err != nil {
		return err
	} else if err := ops.ValidateStatsCollection(opsStatsSpec); err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.labels = labels
	p.opsLogsSpec = opsLogsSpec
	p.opsStatsSpec = opsStatsSpec
	p.shard = ops.NewShardRef(labels)

	return nil
}

func (p *OpsPublisher) Labels() ops.ShardLabeling {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.labels
}

func (p *OpsPublisher) PublishStats(
	out ops.Stats,
	pub func(mapping message.MappingFunc, msg message.Message) (*client.AsyncAppend, error),
) error {

	var key, partitions = shardKeyAndPartitions(out.Shard, out.Timestamp)
	out.Meta = &ops.Meta{Uuid: string(pf.DocumentUUIDPlaceholder)}

	var buf bytes.Buffer
	if err := (&jsonpb.Marshaler{}).Marshal(&buf, &out); err != nil {
		panic(fmt.Errorf("marshal of *ops.Stats should always succeed but: %w", err))
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	var msg = flow.Mappable{
		Spec:       p.opsStatsSpec,
		Doc:        buf.Bytes(),
		Partitions: partitions,
		PackedKey:  key.Pack(),
	}

	var _, err = pub(p.mapper.Map, msg)
	return err
}

func (p *OpsPublisher) PublishLog(out ops.Log) {
	var key, partitions = shardKeyAndPartitions(out.Shard, out.Timestamp)
	out.Meta = &ops.Meta{Uuid: string(pf.DocumentUUIDPlaceholder)}

	var buf bytes.Buffer
	if err := (&jsonpb.Marshaler{}).Marshal(&buf, &out); err != nil {
		panic(fmt.Errorf("marshal of *ops.Log should always succeed but: %w", err))
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	var msg = flow.Mappable{
		Spec:       p.opsLogsSpec,
		Doc:        json.RawMessage(buf.Bytes()),
		Partitions: partitions,
		PackedKey:  key.Pack(),
	}
	// Best effort. PublishCommitted only fails if the publisher itself is cancelled.
	_, _ = p.logsPublisher.PublishCommitted(p.mapper.Map, msg)
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
