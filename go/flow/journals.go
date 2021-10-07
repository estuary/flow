package flow

import (
	"context"
	"fmt"
	"path"

	pf "github.com/estuary/protocols/flow"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.gazette.dev/core/allocator"
	"go.gazette.dev/core/keyspace"
)

// Journals is a type wrapper of a KeySpace that's a local mirror of Gazette
// journals accross the cluster.
type Journals struct {
	*keyspace.KeySpace
}

// GetJournal returns the named JournalSpec and its current Etcd ModRevision.
// If |name| is not a journal, it returns nil and revision zero.
func (j Journals) GetJournal(name pf.Journal) (_ *pf.JournalSpec, revision int64) {
	j.Mu.RLock()
	defer j.Mu.RUnlock()

	var ind, found = j.Search(path.Join(j.Root + "/" + name.String()))
	if found {
		return j.KeyValues[ind].Decoded.(*pf.JournalSpec), j.KeyValues[ind].Raw.ModRevision
	}
	return nil, 0
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
				var s = new(pf.JournalSpec)

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
