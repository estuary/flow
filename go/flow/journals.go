package flow

import (
	"context"
	"fmt"
	"path"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/keyspace"
)

// NewJournalsKeySpace builds a KeySpace over all JournalSpecs managed by the
// broker cluster utilizing the |brokerRoot| Etcd prefix.
func NewJournalsKeySpace(ctx context.Context, etcd *clientv3.Client, brokerRoot string) (*keyspace.KeySpace, error) {
	var ks = keyspace.NewKeySpace(
		path.Clean(brokerRoot+allocator.ItemsPrefix),
		func(raw *mvccpb.KeyValue) (interface{}, error) {
			var s = new(pb.JournalSpec)

			if err := s.Unmarshal(raw.Value); err != nil {
				return nil, err
			} else if err = s.Validate(); err != nil {
				return nil, err
			}
			return s, nil
		},
	)
	if err := ks.Load(ctx, etcd, 0); err != nil {
		return nil, fmt.Errorf("initial load of journals: %w", err)
	}
	return ks, nil
}
