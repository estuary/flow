package testing

import (
	"fmt"
	"sort"
	"testing"
	"time"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
)

func transformFixture(source pf.Collection, transform pf.Transform,
	derivation pf.Collection, readDelay uint32) pf.CollectionSpec_Derivation_Transform {

	return pf.CollectionSpec_Derivation_Transform{
		Name:              transform,
		Collection:        pf.CollectionSpec{Name: source},
		ReadDelaySeconds:  readDelay,
		JournalReadSuffix: fmt.Sprintf("derive/%s/%s", derivation, transform),

		// This is merely a place to retain `derivation` so we can group these
		// later, and has no semantic association with an actual shuffle key.
		ShuffleKey: []string{derivation.String()},
	}
}

func derivationsFixture(transforms ...pf.CollectionSpec_Derivation_Transform) []*pf.CollectionSpec {
	var grouped = make(map[string][]pf.CollectionSpec_Derivation_Transform)
	for _, t := range transforms {
		grouped[t.ShuffleKey[0]] = append(grouped[t.ShuffleKey[0]], t)
	}

	var out []*pf.CollectionSpec
	for name, group := range grouped {
		out = append(out, &pf.CollectionSpec{
			Name: pf.Collection(name),
			Derivation: &pf.CollectionSpec_Derivation{
				Transforms:    group,
				ShardTemplate: &pc.ShardSpec{Disable: false},
			},
		})
	}
	return out
}

func TestGraphAntecedents(t *testing.T) {
	var derivations = derivationsFixture(
		transformFixture("A", "A to B", "B", 0),
		transformFixture("B", "B to C", "C", 0),
		transformFixture("B", "B to A", "A", 0),
		transformFixture("X", "X to Y", "Y", 0),
	)
	var graph = NewGraph(nil, derivations, nil)

	require.False(t, graph.HasPendingWrite("A"))
	require.False(t, graph.HasPendingWrite("B"))
	require.False(t, graph.HasPendingWrite("C"))
	require.False(t, graph.HasPendingWrite("X"))
	require.False(t, graph.HasPendingWrite("Y"))

	graph.pending = append(graph.pending, PendingStat{
		ReadyAt:  1,
		TaskName: "B",
	})

	require.True(t, graph.HasPendingWrite("A"))
	require.True(t, graph.HasPendingWrite("B"))
	require.True(t, graph.HasPendingWrite("C"))
	require.False(t, graph.HasPendingWrite("X"))
	require.False(t, graph.HasPendingWrite("Y"))

	graph.pending = append(graph.pending, PendingStat{
		ReadyAt:  1,
		TaskName: "Y",
	})

	require.False(t, graph.HasPendingWrite("X"))
	require.True(t, graph.HasPendingWrite("Y"))
}

func TestGraphIngestProjection(t *testing.T) {
	var derivations = derivationsFixture(
		transformFixture("A", "A-to-B", "B", 10),
		transformFixture("A", "A-to-C", "C", 5),
	)
	var graph = NewGraph(nil, derivations, nil)

	// Two ingests into "A" complete, with raced Clocks.
	graph.CompletedIngest("A", pb.Offsets{"A/foo": 2})
	graph.CompletedIngest("A", pb.Offsets{"A/foo": 1, "A/bar": 1})

	// Impose an ordering on (unordered) graph.pending.
	sort.Slice(graph.pending, func(i, j int) bool {
		return graph.pending[i].TaskName < graph.pending[j].TaskName
	})

	// Expect PendingStats were created with reduced clocks.
	require.Equal(t, []PendingStat{
		{
			ReadyAt:  TestTime(10 * time.Second),
			TaskName: "B",
			ReadThrough: pb.Offsets{
				"A/foo;derive/B/A-to-B": 2,
				"A/bar;derive/B/A-to-B": 1,
			},
		},
		{
			ReadyAt:  TestTime(5 * time.Second),
			TaskName: "C",
			ReadThrough: pb.Offsets{
				"A/foo;derive/C/A-to-C": 2,
				"A/bar;derive/C/A-to-C": 1,
			},
		},
	}, graph.pending)

	require.Equal(t, pb.Offsets{"A/foo": 2, "A/bar": 1}, graph.writeClock)
}

func TestStatProjection(t *testing.T) {
	var derivations = derivationsFixture(
		transformFixture("A", "A-to-B", "B", 0),
		transformFixture("B", "B-to-C", "C", 0),
	)
	var graph = NewGraph(nil, derivations, nil)

	// Two stats of "B" transformation complete.
	graph.CompletedStat(
		"B",
		pb.Offsets{"A/data;derive/B/A-to-B": 1},
		pb.Offsets{"B/data": 2},
	)
	graph.CompletedStat(
		"B",
		pb.Offsets{"A/data;derive/B/A-to-B": 2},
		pb.Offsets{"B/data": 1},
	)

	// Expect last read clock was tracked.
	require.Equal(t, pb.Offsets{"A/data;derive/B/A-to-B": 2}, graph.readThrough["B"])

	// Expect write clocks were merged into a new pending stat of C.
	require.Equal(t, []PendingStat{
		{
			ReadyAt:     0,
			TaskName:    "C",
			ReadThrough: pb.Offsets{"B/data;derive/C/B-to-C": 2},
		},
	}, graph.pending)

	require.Equal(t, pb.Offsets{"B/data": 2}, graph.writeClock)
}

func TestProjectionAlreadyRead(t *testing.T) {
	var derivations = derivationsFixture(
		transformFixture("A", "A-to-B", "B", 0),
		transformFixture("B", "B-to-B", "B", 0), // Self-cycle.
	)
	var graph = NewGraph(nil, derivations, nil)

	var progressFixture = pb.Offsets{
		"A/data;derive/B/A-to-B": 5,
		"B/data;derive/B/B-to-B": 6,
	}

	// Stat of "B" completes, updating progress on reading "A" & "B" data.
	graph.CompletedStat(
		"B",
		progressFixture.Copy(),
		pb.Offsets{"B/data": 6}, // Contained by |progressFixture|.
	)

	// Ingest of "A" completes (also contained by |progressFixture|).
	graph.CompletedIngest("A", pb.Offsets{"A/data": 5})

	// Expect no pending stat of B was created (though it cycles, it's already read it's own write).
	require.Empty(t, graph.pending)

	require.Equal(t, pb.Offsets{"A/data": 5, "B/data": 6}, graph.writeClock)

	// Completed ingest & stat which *do* require a new stat.
	graph.CompletedIngest("A", pb.Offsets{"A/data": 50})
	graph.CompletedStat(
		"B",
		progressFixture.Copy(),
		pb.Offsets{"B/data": 60},
	)

	require.Equal(t, []PendingStat{
		{
			ReadyAt:  0,
			TaskName: "B",
			ReadThrough: pb.Offsets{
				"A/data;derive/B/A-to-B": 50,
				"B/data;derive/B/B-to-B": 60,
			},
		},
	}, graph.pending)

	require.Equal(t, pb.Offsets{"A/data": 50, "B/data": 60}, graph.writeClock)
}

func TestReadyStats(t *testing.T) {
	var derivations = derivationsFixture(
		transformFixture("A", "A-to-A", "A", 0),
		transformFixture("A", "A-to-B", "B", 0),
		transformFixture("A", "A-to-C", "C", 0),
	)
	var graph = NewGraph(nil, derivations, nil)

	// Install pending fixtures.
	graph.pending = []PendingStat{
		{ReadyAt: 10, TaskName: "A", ReadThrough: pb.Offsets{"a": 1}},
		{ReadyAt: 10, TaskName: "B", ReadThrough: pb.Offsets{"a": 2}},
		{ReadyAt: 5, TaskName: "C", ReadThrough: pb.Offsets{"a": 3}},
	}

	var ready, nextTime, nextName = graph.PopReadyStats()
	require.Empty(t, ready)
	require.Equal(t, TestTime(5), nextTime)
	require.Equal(t, TaskName("C"), nextName)
	graph.CompletedAdvance(4)

	ready, nextTime, nextName = graph.PopReadyStats()
	require.Empty(t, ready)
	require.Equal(t, TestTime(1), nextTime)
	require.Equal(t, TaskName("C"), nextName)
	graph.CompletedAdvance(1)

	ready, nextTime, nextName = graph.PopReadyStats()
	require.Equal(t, []PendingStat{
		{ReadyAt: 5, TaskName: "C", ReadThrough: pb.Offsets{"a": 3}},
	}, ready)
	require.Equal(t, TestTime(0), nextTime)
	require.Equal(t, TaskName("C"), nextName)

	ready, nextTime, nextName = graph.PopReadyStats()
	require.Empty(t, ready)
	require.Equal(t, TestTime(5), nextTime)
	require.Equal(t, TaskName("A"), nextName)
	graph.CompletedAdvance(5)

	ready, nextTime, nextName = graph.PopReadyStats()
	require.Equal(t, []PendingStat{
		{ReadyAt: 10, TaskName: "A", ReadThrough: pb.Offsets{"a": 1}},
		{ReadyAt: 10, TaskName: "B", ReadThrough: pb.Offsets{"a": 2}},
	}, ready)
	require.Equal(t, TestTime(0), nextTime)
	require.Equal(t, TaskName("A"), nextName)

	ready, nextTime, nextName = graph.PopReadyStats()
	require.Empty(t, ready)
	require.Equal(t, TestTime(-1), nextTime)
	require.Equal(t, TaskName(""), nextName)
}

func TestTaskIndexing(t *testing.T) {
	var captures = []*pf.CaptureSpec{
		{
			Name: "a/capture/task",
			Bindings: []*pf.CaptureSpec_Binding{
				{Collection: pf.CollectionSpec{Name: "a/capture/one"}},
				{Collection: pf.CollectionSpec{Name: "a/capture/two"}},
			},
			ShardTemplate: &pc.ShardSpec{Disable: false},
		},
	}
	var derivations = []*pf.CollectionSpec{
		{
			Name: "a/derivation",
			Derivation: &pf.CollectionSpec_Derivation{
				Transforms: []pf.CollectionSpec_Derivation_Transform{
					{
						Collection:        pf.CollectionSpec{Name: "a/capture/one"},
						JournalReadSuffix: "derive/A",
					},
					{
						Collection:        pf.CollectionSpec{Name: "a/capture/one"},
						JournalReadSuffix: "derive/AA",
						ReadDelaySeconds:  5,
					},
					{
						Collection:        pf.CollectionSpec{Name: "a/capture/two"},
						JournalReadSuffix: "derive/B",
					},
				},
				ShardTemplate: &pc.ShardSpec{Disable: false},
			},
		},
	}
	var materializations = []*pf.MaterializationSpec{
		{
			Name: "a/materialization",
			Bindings: []*pf.MaterializationSpec_Binding{
				{
					Collection:        pf.CollectionSpec{Name: "a/derivation"},
					JournalReadSuffix: "mat/1",
				},
				{
					Collection:        pf.CollectionSpec{Name: "a/capture/two"},
					JournalReadSuffix: "mat/2",
				},
			},
			ShardTemplate: &pc.ShardSpec{Disable: false},
		},
	}
	// Build a Graph from the task fixtures, and verify the expected indices.
	var graph = NewGraph(captures, derivations, materializations)

	require.Equal(t, map[TaskName][]pf.Collection{
		"a/capture/task": {"a/capture/one", "a/capture/two"},
		"a/derivation":   {"a/derivation"},
	}, graph.outputs)

	require.Equal(t, map[pf.Collection][]taskRead{
		"a/capture/one": {
			{task: "a/derivation", suffix: ";derive/A", delay: 0},
			{task: "a/derivation", suffix: ";derive/AA", delay: TestTime(5 * time.Second)},
		},
		"a/capture/task": {
			{task: "a/capture/task", suffix: "", delay: 0},
		},
		"a/capture/two": {
			{task: "a/derivation", suffix: ";derive/B", delay: 0},
			{task: "a/materialization", suffix: ";mat/2", delay: 0},
		},
		"a/derivation": {
			{task: "a/materialization", suffix: ";mat/1", delay: 0},
		}}, graph.readers)
}
