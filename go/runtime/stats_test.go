package runtime

import (
	"testing"
	"time"

	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestCaptureStats(t *testing.T) {
	// Simulate a capture with two bindings, where only one of them participated in the transaction.
	var captureSpec = &pf.CaptureSpec{
		Capture: pf.Capture("test/capture"),
		Bindings: []*pf.CaptureSpec_Binding{
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
	var subject = newTestFormatter(captureSpec, "capture")
	var inputs = []*pf.CombineAPI_Stats{
		nil,
		{
			Right: &pf.DocsAndBytes{
				Docs:  2,
				Bytes: 2222,
			},
			Out: &pf.DocsAndBytes{
				Docs:  5,
				Bytes: 5555,
			},
		},
	}
	var actual = subject.captureStats(testTxnStartTime(), inputs)

	var expect = map[string]CaptureBindingStats{
		"test/collectionA": {
			Right: DocsAndBytes{
				Docs:  2,
				Bytes: 2222,
			},
			Out: DocsAndBytes{
				Docs:  5,
				Bytes: 5555,
			},
		},
	}
	assertEventMeta(t, actual, captureSpec, "capture")
	require.Equal(t, expect, actual.Capture)

	// Test where stats exist for multiple bindings that each reference the same collection
	// and assert that the result is the sum.
	inputs = []*pf.CombineAPI_Stats{
		{
			Right: &pf.DocsAndBytes{
				Docs:  1,
				Bytes: 1111,
			},
			Out: &pf.DocsAndBytes{
				Docs:  2,
				Bytes: 2222,
			},
		},
		{
			Right: &pf.DocsAndBytes{
				Docs:  3,
				Bytes: 3333,
			},
			Out: &pf.DocsAndBytes{
				Docs:  4,
				Bytes: 4444,
			},
		},
	}
	actual = subject.captureStats(testTxnStartTime(), inputs)

	expect = map[string]CaptureBindingStats{
		"test/collectionA": {
			Right: DocsAndBytes{
				Docs:  4,
				Bytes: 4444,
			},
			Out: DocsAndBytes{
				Docs:  6,
				Bytes: 6666,
			},
		},
	}
	assertEventMeta(t, actual, captureSpec, "capture")
	require.Equal(t, expect, actual.Capture)
}

func TestDeriveStats(t *testing.T) {
	// Simulate a derivation with 4 transforms, and then publish stats for 3 of them, each with a
	// different combination of update and publish lambdas. This tests all of the conditional
	// inclusions of transforms and invocation stats.
	var derivationSpec = &pf.DerivationSpec{
		Transforms: []pf.TransformSpec{
			{
				Transform:     pf.Transform("transformA"),
				UpdateLambda:  &pf.LambdaSpec{},
				PublishLambda: &pf.LambdaSpec{},
			},
			{
				Transform:    pf.Transform("transformB"),
				UpdateLambda: &pf.LambdaSpec{},
			},
			{
				Transform:     pf.Transform("transformC"),
				PublishLambda: &pf.LambdaSpec{},
			},
			{
				Transform:     pf.Transform("transformD"),
				UpdateLambda:  &pf.LambdaSpec{},
				PublishLambda: &pf.LambdaSpec{},
			},
		},
	}
	var subject = newTestFormatter(derivationSpec, "derivation")
	var input = pf.DeriveAPI_Stats{
		Transforms: []*pf.DeriveAPI_Stats_TransformStats{
			{
				Input: &pf.DocsAndBytes{
					Docs:  6,
					Bytes: 6666,
				},
				Publish: &pf.DeriveAPI_Stats_InvokeStats{
					Output: &pf.DocsAndBytes{
						Docs:  2,
						Bytes: 2222,
					},
					TotalSeconds: 2.0,
				},
				Update: &pf.DeriveAPI_Stats_InvokeStats{
					Output: &pf.DocsAndBytes{
						Docs:  3,
						Bytes: 3333,
					},
					TotalSeconds: 3.0,
				},
			},
			{
				Input: &pf.DocsAndBytes{
					Docs:  1,
					Bytes: 1111,
				},
				Update: &pf.DeriveAPI_Stats_InvokeStats{
					Output: &pf.DocsAndBytes{
						Docs:  7,
						Bytes: 7777,
					},
					TotalSeconds: 7.0,
				},
			},
			{
				Input: &pf.DocsAndBytes{
					Docs:  8,
					Bytes: 8888,
				},
				Publish: &pf.DeriveAPI_Stats_InvokeStats{
					Output: &pf.DocsAndBytes{
						Docs:  9,
						Bytes: 9999,
					},
					TotalSeconds: 9.0,
				},
			},
			// Last entry is nil, which would be the case if no documents were processed for the
			// transform in a given transaction.
			nil,
		},
		Registers: &pf.DeriveAPI_Stats_RegisterStats{
			Created: 4,
		},
		Output: &pf.DocsAndBytes{
			Docs:  5,
			Bytes: 5555,
		},
	}

	var actual = subject.deriveStats(testTxnStartTime(), &input)
	assertEventMeta(t, actual, derivationSpec, "derivation")

	var expected = DeriveStats{
		Transforms: map[string]DeriveTransformStats{
			"transformA": {
				Input: DocsAndBytes{
					Docs:  6,
					Bytes: 6666,
				},
				Publish: &InvokeStats{
					Out: DocsAndBytes{
						Docs:  2,
						Bytes: 2222,
					},
					SecondsTotal: 2.0,
				},
				Update: &InvokeStats{
					Out: DocsAndBytes{
						Docs:  3,
						Bytes: 3333,
					},
					SecondsTotal: 3.0,
				},
			},
			"transformB": {
				Input: DocsAndBytes{
					Docs:  1,
					Bytes: 1111,
				},
				Update: &InvokeStats{
					Out: DocsAndBytes{
						Docs:  7,
						Bytes: 7777,
					},
					SecondsTotal: 7.0,
				},
			},
			"transformC": {
				Input: DocsAndBytes{
					Docs:  8,
					Bytes: 8888,
				},
				Publish: &InvokeStats{
					Out: DocsAndBytes{
						Docs:  9,
						Bytes: 9999,
					},
					SecondsTotal: 9.0,
				},
			},
		},
		Out: DocsAndBytes{
			Docs:  5,
			Bytes: 5555,
		},
		Registers: &DeriveRegisterStats{
			CreatedTotal: 4,
		},
	}
	require.Equal(t, &expected, actual.Derive)
}

func TestMaterializationStats(t *testing.T) {
	// Simulate a materialization with two bindings, where only one of them participated in the
	// transaction, to exercise the conditional inclusion of binding stats.
	var materializationSpec = &pf.MaterializationSpec{
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
	var subject = newTestFormatter(materializationSpec, "materialization")

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
	var actual = subject.materializationStats(testTxnStartTime(), input)
	assertEventMeta(t, actual, materializationSpec, "materialization")
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
	actual = subject.materializationStats(testTxnStartTime(), input)
	assertEventMeta(t, actual, materializationSpec, "materialization")
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

func newTestFormatter(task pf.Task, kind string) *StatsFormatter {
	var labeling = labels.ShardLabeling{
		Build:    "tha build",
		Range:    pf.NewFullRange(),
		TaskName: task.TaskName(),
		TaskType: kind,
	}
	var testStatsCollectionSpec = &pf.CollectionSpec{
		Collection:      pf.Collection("ops/test/stats"),
		PartitionFields: []string{"kind", "name"},
	}
	var f, err = NewStatsFormatter(labeling, testStatsCollectionSpec, task)
	if err != nil {
		panic(err)
	}
	return f
}
