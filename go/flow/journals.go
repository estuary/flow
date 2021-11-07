package flow

import (
	"context"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.gazette.dev/core/broker"
	"go.gazette.dev/core/keyspace"
)

// Journals is a type wrapper of a KeySpace that's a local mirror of Gazette
// journals accross the cluster.
type Journals struct {
	*keyspace.KeySpace
}

// NewJournalsKeySpace builds a KeySpace over all JournalSpecs managed by the
// broker cluster utilizing the |brokerRoot| Etcd prefix.
func NewJournalsKeySpace(ctx context.Context, etcd *clientv3.Client, root string) (Journals, error) {
	var journals = Journals{KeySpace: broker.NewKeySpace(root)}

	if err := journals.KeySpace.Load(ctx, etcd, 0); err != nil {
		return Journals{}, fmt.Errorf("initial load of %q: %w", root, err)
	}
	return journals, nil
}
