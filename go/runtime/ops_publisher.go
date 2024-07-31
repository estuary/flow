package runtime

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/estuary/flow/go/protocols/ops"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/types"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
)

type OpsPublisher struct {
	logsPublisher *message.Publisher
	mu            sync.Mutex

	// Fields that update with each task term:
	labels ops.ShardLabeling
	shard  *ops.ShardRef
}

var _ ops.Publisher = &OpsPublisher{}

func NewOpsPublisher(logsPublisher *message.Publisher) *OpsPublisher {
	return &OpsPublisher{logsPublisher: logsPublisher, mu: sync.Mutex{}}
}

func (p *OpsPublisher) UpdateLabels(labels ops.ShardLabeling) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.labels = labels
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
	out.Meta = &ops.Meta{Uuid: string(pf.DocumentUUIDPlaceholder)}

	var buf bytes.Buffer
	if err := (&jsonpb.Marshaler{}).Marshal(&buf, &out); err != nil {
		panic(fmt.Errorf("marshal of *ops.Stats should always succeed but: %w", err))
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.labels.StatsJournal == "local" {
		ops.NewLocalPublisher(p.labels).PublishLog(ops.Log{
			Level:     ops.Log_debug,
			Shard:     out.Shard,
			Timestamp: out.Timestamp,
			Meta:      out.Meta,
			Message:   "transaction stats",
			FieldsJsonMap: map[string]json.RawMessage{
				"stats": json.RawMessage(buf.Bytes()),
			},
		})
		return nil
	}

	var _, err = pub(
		func(message.Mappable) (pb.Journal, string, error) {
			return p.labels.StatsJournal, labels.ContentType_JSONLines, nil
		}, flow.Mappable{
			Spec: &opsPlaceholderSpec,
			Doc:  buf.Bytes(),
		},
	)
	return err
}

func (p *OpsPublisher) PublishLog(out ops.Log) {
	out.Meta = &ops.Meta{Uuid: string(pf.DocumentUUIDPlaceholder)}

	var buf bytes.Buffer
	if err := (&jsonpb.Marshaler{}).Marshal(&buf, &out); err != nil {
		panic(fmt.Errorf("marshal of *ops.Log should always succeed but: %w", err))
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.labels.LogsJournal == "local" {
		ops.NewLocalPublisher(p.labels).PublishLog(out)
		return
	}

	// Best effort. PublishCommitted only fails if the publisher itself is cancelled.
	_, _ = p.logsPublisher.PublishCommitted(
		func(message.Mappable) (pb.Journal, string, error) {
			return p.labels.LogsJournal, labels.ContentType_JSONLines, nil
		},
		flow.Mappable{
			Spec: &opsPlaceholderSpec,
			Doc:  json.RawMessage(buf.Bytes()),
		},
	)
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

var opsPlaceholderSpec = pf.CollectionSpec{
	AckTemplateJson: json.RawMessage(`{"_meta":{"uuid":"` + string(pf.DocumentUUIDPlaceholder) + `"},"ack":true}`),
}
