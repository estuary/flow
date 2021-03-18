package flow

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/estuary/flow/go/fdb/tuple"
	flowLabels "github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocols/flow"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/keyspace"
	"go.gazette.dev/core/labels"
)

// Journals is a type wrapper of a KeySpace that's a local mirror of Gazette
// journals accross the cluster.
type Journals struct {
	*keyspace.KeySpace
}

// NewJournalsKeySpace builds a KeySpace over all JournalSpecs managed by the
// broker cluster utilizing the |brokerRoot| Etcd prefix.
func NewJournalsKeySpace(ctx context.Context, etcd *clientv3.Client, root string) (Journals, error) {
	if root != path.Clean(root) {
		return Journals{}, fmt.Errorf("%q is not a clean path", root)
	}

	var journals = Journals{
		KeySpace: keyspace.NewKeySpace(
			path.Clean(root+allocator.ItemsPrefix),
			func(raw *mvccpb.KeyValue) (interface{}, error) {
				var s = new(pb.JournalSpec)

				if err := s.Unmarshal(raw.Value); err != nil {
					return nil, err
				} else if err = s.Validate(); err != nil {
					return nil, err
				}
				return s, nil
			},
		),
	}

	if err := journals.KeySpace.Load(ctx, etcd, 0); err != nil {
		return Journals{}, fmt.Errorf("initial load of %q: %w", root, err)
	}
	return journals, nil
}

// BuildPartitionSpec returns a JournalSpec for the given collection, partition fields, and journal rules.
func BuildPartitionSpec(collection *pf.CollectionSpec, partitions tuple.Tuple, rules []pf.JournalRules_Rule) pb.JournalSpec {
	// Baseline specification common to all partitions.
	var journal = pb.JournalSpec{
		Replication: 3,
		Fragment: pb.JournalSpec_Fragment{
			Length:              1 << 29, // 512MB.
			CompressionCodec:    pb.CompressionCodec_GZIP_OFFLOAD_DECOMPRESSION,
			RefreshInterval:     5 * time.Minute,
			PathPostfixTemplate: `utc_date={{.Spool.FirstAppendTime.Format "2006-01-02"}}/utc_hour={{.Spool.FirstAppendTime.Format "15"}}`,
			FlushInterval:       6 * time.Hour,
		},
	}

	journal.LabelSet.SetValue(flowLabels.Collection, collection.Collection.String())
	journal.LabelSet.SetValue(flowLabels.KeyBegin, "00")
	journal.LabelSet.SetValue(flowLabels.KeyEnd, "ffffffff")
	journal.LabelSet.SetValue(labels.ContentType, labels.ContentType_JSONLines)
	journal.LabelSet.SetValue(labels.ManagedBy, flowLabels.ManagedByFlow)

	var name strings.Builder
	name.WriteString(collection.Collection.String())

	if len(collection.PartitionFields) != len(partitions) {
		panic("spec partition fields and partitions have mis-matched lengths")
	}

	for i, field := range collection.PartitionFields {
		var v = encodePartitionElement(nil, partitions[i])
		journal.LabelSet.AddValue(flowLabels.FieldPrefix+field, string(v))

		name.WriteByte('/')
		name.WriteString(field)
		name.WriteByte('=')
		name.Write(v)
	}
	name.WriteString("/pivot=00")

	for _, rule := range rules {
		if rule.Selector.Matches(journal.LabelSet) {
			journal = pb.UnionJournalSpecs(rule.Template, journal)
		}
	}

	journal.Name = pb.Journal(name.String())
	return journal
}

// BuildRecoveryLogSpec returns a JournalSpec for the given collection, partition fields, and journal rules.
func BuildRecoveryLogSpec(shard *pc.ShardSpec, rules []pf.JournalRules_Rule) pb.JournalSpec {
	var journal = pb.JournalSpec{
		Replication: 3,
		Fragment: pb.JournalSpec_Fragment{
			Length:           1 << 28, // 256.
			CompressionCodec: pb.CompressionCodec_SNAPPY,
			RefreshInterval:  5 * time.Minute,
		},
	}
	journal.LabelSet.SetValue(labels.ContentType, labels.ContentType_RecoveryLog)
	journal.LabelSet.SetValue(labels.ManagedBy, flowLabels.ManagedByFlow)

	for _, rule := range rules {
		if rule.Selector.Matches(journal.LabelSet) {
			journal = pb.UnionJournalSpecs(rule.Template, journal)
		}
	}

	journal.Name = shard.RecoveryLog()
	return journal
}

func encodePartitionElement(b []byte, elem tuple.TupleElement) []byte {
	switch v := elem.(type) {
	case nil:
		return append(b, "null"...)
	case bool:
		if v {
			return append(b, "true"...)
		}
		return append(b, "false"...)
	case uint64:
		return strconv.AppendUint(b, v, 10)
	case int64:
		return strconv.AppendInt(b, v, 10)
	case int:
		return strconv.AppendInt(b, int64(v), 10)
	case string:
		return append(b, url.PathEscape(v)...)
	default:
		panic(fmt.Sprintf("invalid element type: %#v", elem))
	}
}
