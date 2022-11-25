package runtime

import (
	"testing"
	"time"

	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestMaterializationStats(t *testing.T) {
	// Simulate a materialization with two bindings, where only one of them participated in the
	// transaction, to exercise the conditional inclusion of binding stats.
	var materializationSpec = pf.MaterializationSpec{
		Materialization: pf.Materialization("test/materialization"),
		Bindings: []*pf.MaterializationSpec_Binding{
			{
				Collection: pf.CollectionSpec{
					Collection: pf.Collection("test/collectionA"),
				},
			},
			{
				Collection: pf.CollectionSpec{
					Collection: pf.Collection("test/collectionA"),
				},
			},
		},
	}
	var subject = Materialize{
		taskTerm: taskTerm{
			StatsFormatter: newTestFormatter("test/materialization", "materialization"),
		},
		spec: materializationSpec,
	}

	var input = []*pf.CombineAPI_Stats{
		nil,
		{
			Left: &pf.DocsAndBytes{
				Docs:  3,
				Bytes: 3333,
			},
			Right: &pf.DocsAndBytes{
				Docs:  2,
				Bytes: 2222,
			},
			Out: &pf.DocsAndBytes{
				Docs:  4,
				Bytes: 4444,
			},
		},
	}
	var actual = subject.materializationStats(input)
	assertEventMeta(t, actual, &materializationSpec, "materialization")
	var expected = map[string]MaterializeBindingStats{
		"test/collectionA": {
			Left: DocsAndBytes{
				Docs:  3,
				Bytes: 3333,
			},
			Right: DocsAndBytes{
				Docs:  2,
				Bytes: 2222,
			},
			Out: DocsAndBytes{
				Docs:  4,
				Bytes: 4444,
			},
		},
	}
	require.Equal(t, expected, actual.Materialize)

	// Test where both bindings have stats, and both bindings reference the same collection.
	// The stats for both bindings should be summed under the same collection name.
	input = []*pf.CombineAPI_Stats{
		{
			Left: &pf.DocsAndBytes{
				Docs:  1,
				Bytes: 1111,
			},
			Right: &pf.DocsAndBytes{
				Docs:  2,
				Bytes: 2222,
			},
			Out: &pf.DocsAndBytes{
				Docs:  3,
				Bytes: 3333,
			},
		},
		{
			Left: &pf.DocsAndBytes{
				Docs:  4,
				Bytes: 4444,
			},
			Right: &pf.DocsAndBytes{
				Docs:  5,
				Bytes: 5555,
			},
			Out: &pf.DocsAndBytes{
				Docs:  6,
				Bytes: 6666,
			},
		},
	}
	actual = subject.materializationStats(input)
	assertEventMeta(t, actual, &materializationSpec, "materialization")
	expected = map[string]MaterializeBindingStats{
		"test/collectionA": {
			Left: DocsAndBytes{
				Docs:  5,
				Bytes: 5555,
			},
			Right: DocsAndBytes{
				Docs:  7,
				Bytes: 7777,
			},
			Out: DocsAndBytes{
				Docs:  9,
				Bytes: 9999,
			},
		},
	}
	require.Equal(t, expected, actual.Materialize)
}

/*
The helper functions below are also used in capture_test.go and derive_test.go.
*/

func testTxnStartTime() time.Time {
	var ts, err = time.Parse(time.RFC3339, "2021-09-10T08:09:10.1234Z")
	if err != nil {
		panic(err)
	}
	return ts
}

func assertEventMeta(t *testing.T, event StatsEvent, task pf.Task, expectKind string) {
	require.Equal(t, task.TaskName(), event.Shard.Name)
	require.Equal(t, expectKind, event.Shard.Kind)
	require.Equal(t, "00000000", event.Shard.KeyBegin)
	require.Equal(t, "00000000", event.Shard.RClockBegin)
	// Assert that the timestamp is a truncated version of the full timestamp above
	require.Equal(t, "2021-09-10T08:09:00Z", event.Timestamp.Format(time.RFC3339))
}

func newTestFormatter(name, kind string) *StatsFormatter {
	var labeling = labels.ShardLabeling{
		Build:    "tha build",
		Range:    pf.NewFullRange(),
		TaskName: name,
		TaskType: kind,
	}
	var testStatsCollectionSpec = &pf.CollectionSpec{
		Collection:      pf.Collection("ops/test/stats"),
		PartitionFields: []string{"kind", "name"},
		KeyPtrs:         []string{"/shard/name", "/shard/keyBegin", "/shard/rClockBegin", "/ts"},
	}
	var f, err = NewStatsFormatter(labeling, testStatsCollectionSpec)
	if err != nil {
		panic(err)
	}
	// Set the txnOpened time to a known value, so tests can assert that the timestamp
	// on the event is correctly truncated.
	f.txnOpened = testTxnStartTime()
	return f
}
