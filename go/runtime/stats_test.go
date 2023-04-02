package runtime

import (
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/estuary/flow/go/protocols/ops"
	"github.com/stretchr/testify/require"
)

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
