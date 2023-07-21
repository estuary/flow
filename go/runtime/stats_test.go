package runtime

import (
	"testing"
	"time"

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

func TestIntervalJitterAndDurations(t *testing.T) {
	const period = time.Minute

	for _, tc := range []struct {
		n string
		i time.Duration
	}{{"foo", 35}, {"bar", 52}, {"baz", 0}, {"bing", 39}, {"quip", 56}} {
		require.Equal(t, time.Second*tc.i, intervalJitter(period, tc.n), tc.n)

	}

	require.Equal(t, 20*time.Second, durationToNextInterval(time.Unix(1000, 0), period))
	require.Equal(t, 60*time.Second-100*time.Nanosecond, durationToNextInterval(time.Unix(1020, 100), period))
	require.Equal(t, 1*time.Second, durationToNextInterval(time.Unix(1079, 0), period))
	require.Equal(t, 59*time.Second, durationToNextInterval(time.Unix(1081, 0), period))
}

func TestIntervalStatsShape(t *testing.T) {
	var labels = ops.ShardLabeling{
		TaskName: "some/task",
		Range:    pf.NewFullRange(),
		TaskType: ops.TaskType_capture,
	}

	require.Equal(t,
		`shard:<kind:capture name:"some/task" key_begin:"00000000" r_clock_begin:"00000000" > timestamp:<seconds:1600000000 > interval:<uptime_seconds:300 usage_rate:1 > `,
		intervalStats(time.Unix(1600000000, 0), 5*time.Minute, labels).String())

	labels.TaskType = ops.TaskType_derivation

	require.Equal(t,
		`shard:<kind:derivation name:"some/task" key_begin:"00000000" r_clock_begin:"00000000" > timestamp:<seconds:1500000000 > interval:<uptime_seconds:600 > `,
		intervalStats(time.Unix(1500000000, 0), 10*time.Minute, labels).String())
}
