package runtime

import (
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/estuary/flow/go/protocols/ops"
	"github.com/stretchr/testify/require"
)

func TestRemoveOldOpsJournalAckIntents(t *testing.T) {
	cp := pf.Checkpoint{
		AckIntents: map[pf.Journal][]byte{
			pf.Journal("a/good/journal"): []byte("keep1"),
			pf.Journal("ops/estuary/logs/kind=materialization/name=materialization"):       []byte("drop1"),
			pf.Journal("ops/estuary/stats/kind=capture/name=capture"):                      []byte("drop2"),
			pf.Journal("ops/some.tenant/logs/kind=derivation/name=something"):              []byte("drop3"),
			pf.Journal("ops/other-tenant/stats/kind=materialization/name=someting/else"):   []byte("drop4"),
			pf.Journal("ops.us-central1.v1/stats/kind=materialization/name=something"):     []byte("keep2"),
			pf.Journal("ops.us-central1.v1/logs/kind=capture/name=another"):                []byte("keep3"),
			pf.Journal("hello/ops/estuary/logs/kind=materialization/name=materialization"): []byte("keep4"),
		},
	}

	removeOldOpsJournalAckIntents(cp.AckIntents)

	want := pf.Checkpoint{
		AckIntents: map[pf.Journal][]byte{
			pf.Journal("a/good/journal"): []byte("keep1"),
			pf.Journal("ops.us-central1.v1/stats/kind=materialization/name=something"):     []byte("keep2"),
			pf.Journal("ops.us-central1.v1/logs/kind=capture/name=another"):                []byte("keep3"),
			pf.Journal("hello/ops/estuary/logs/kind=materialization/name=materialization"): []byte("keep4"),
		},
	}

	require.Equal(t, want, cp)
}

func TestStatsAccumulation(t *testing.T) {
	var actual = make(map[string]*ops.Stats_Binding)

	mergeBinding(actual, "test/collectionA", &pf.CombineAPI_Stats{
		Right: &pf.DocsAndBytes{
			Docs:  0,
			Bytes: 0,
		},
		Out: &pf.DocsAndBytes{
			Docs:  5,
			Bytes: 5555,
		},
	})
	mergeBinding(actual, "test/collectionB", &pf.CombineAPI_Stats{
		Right: &pf.DocsAndBytes{
			Docs:  1,
			Bytes: 23,
		},
		Out: &pf.DocsAndBytes{
			Docs:  4,
			Bytes: 56,
		},
	})
	mergeBinding(actual, "test/collectionA", &pf.CombineAPI_Stats{
		Left: &pf.DocsAndBytes{
			Docs:  1,
			Bytes: 1111,
		},
		Out: &pf.DocsAndBytes{
			Docs:  2,
			Bytes: 2222,
		},
	})
	mergeBinding(actual, "test/collectionC", &pf.CombineAPI_Stats{
		Left: &pf.DocsAndBytes{
			Docs:  0,
			Bytes: 0,
		},
	})
	mergeBinding(actual, "test/collectionD", nil)

	require.Equal(t, map[string]*ops.Stats_Binding{
		"test/collectionA": {
			Left: &ops.Stats_DocsAndBytes{
				DocsTotal:  1,
				BytesTotal: 1111,
			},
			Out: &ops.Stats_DocsAndBytes{
				DocsTotal:  7,
				BytesTotal: 7777,
			},
		},
		"test/collectionB": {
			Right: &ops.Stats_DocsAndBytes{
				DocsTotal:  1,
				BytesTotal: 23,
			},
			Out: &ops.Stats_DocsAndBytes{
				DocsTotal:  4,
				BytesTotal: 56,
			},
		},
	}, actual)
}
