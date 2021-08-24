package testing

import (
	"fmt"
	"sort"
	"testing"
	"time"

	pf "github.com/estuary/protocols/flow"
	"github.com/stretchr/testify/require"
)

func transformFixture(source pf.Collection, transform pf.Transform,
	derivation pf.Collection, readDelay uint32) pf.TransformSpec {

	return pf.TransformSpec{
		Derivation: derivation,
		Transform:  transform,
		Shuffle: pf.Shuffle{
			SourceCollection: source,
			GroupName:        fmt.Sprintf("derive/%s/%s", derivation, transform),
			ReadDelaySeconds: readDelay,
		},
	}
}

func derivationsFixture(transforms ...pf.TransformSpec) []*pf.CatalogTask {
	var grouped = make(map[pf.Collection][]pf.TransformSpec)
	for _, t := range transforms {
		grouped[t.Derivation] = append(grouped[t.Derivation], t)
	}

	var out []*pf.CatalogTask
	for _, group := range grouped {
		out = append(out, &pf.CatalogTask{
			Derivation: &pf.DerivationSpec{
				Collection: pf.CollectionSpec{Collection: group[0].Derivation},
				Transforms: group,
			}})
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
	var graph = NewGraph(derivations)

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
	var graph = NewGraph(derivations)

	// Two ingests into "A" complete, with raced Clocks.
	graph.CompletedIngest("A", clockFixtureOne(10, "A/foo", 2))
	graph.CompletedIngest("A", clockFixture(11, []string{"A/foo", "A/bar"}, []int{1, 1}))

	// Impose an ordering on (unordered) graph.pending.
	sort.Slice(graph.pending, func(i, j int) bool {
		return graph.pending[i].TaskName < graph.pending[j].TaskName
	})

	// Expect PendingStats were created with reduced clocks.
	require.Equal(t, []PendingStat{
		{
			ReadyAt:  TestTime(10 * time.Second),
			TaskName: "B",
			ReadThrough: clockFixture(11,
				[]string{"A/foo;derive/B/A-to-B", "A/bar;derive/B/A-to-B"},
				[]int{2, 1}),
		},
		{
			ReadyAt:  TestTime(5 * time.Second),
			TaskName: "C",
			ReadThrough: clockFixture(11,
				[]string{"A/foo;derive/C/A-to-C", "A/bar;derive/C/A-to-C"},
				[]int{2, 1}),
		},
	}, graph.pending)

	require.Equal(t, clockFixture(11, []string{"A/foo", "A/bar"}, []int{2, 1}),
		graph.writeClock)
}

func TestStatProjection(t *testing.T) {
	var derivations = derivationsFixture(
		transformFixture("A", "A-to-B", "B", 0),
		transformFixture("B", "B-to-C", "C", 0),
	)
	var graph = NewGraph(derivations)

	// Two stats of "B" transformation complete.
	graph.CompletedStat(
		"B",
		clockFixtureOne(10, "A/data;derive/B/A-to-B", 1),
		clockFixtureOne(10, "B/data", 2),
	)
	graph.CompletedStat(
		"B",
		clockFixtureOne(15, "A/data;derive/B/A-to-B", 2),
		clockFixtureOne(20, "B/data", 1),
	)

	// Expect last read clock was tracked.
	require.Equal(t, clockFixtureOne(15, "A/data;derive/B/A-to-B", 2), graph.readThrough["B"])

	// Expect write clocks were merged into a new pending stat of C.
	require.Equal(t, []PendingStat{
		{
			ReadyAt:     0,
			TaskName:    "C",
			ReadThrough: clockFixtureOne(20, "B/data;derive/C/B-to-C", 2),
		},
	}, graph.pending)

	require.Equal(t, clockFixtureOne(20, "B/data", 2), graph.writeClock)
}

func TestProjectionAlreadyRead(t *testing.T) {
	var derivations = derivationsFixture(
		transformFixture("A", "A-to-B", "B", 0),
		transformFixture("B", "B-to-B", "B", 0), // Self-cycle.
	)
	var graph = NewGraph(derivations)

	var progressFixture = clockFixture(4,
		[]string{"A/data;derive/B/A-to-B", "B/data;derive/B/B-to-B"}, []int{5, 6})

	// Stat of "B" completes, updating progress on reading "A" & "B" data.
	graph.CompletedStat(
		"B",
		progressFixture.Copy(),
		clockFixtureOne(4, "B/data", 6), // Contained by |progressFixture|.
	)

	// Ingest of "A" completes (also contained by |progressFixture|).
	graph.CompletedIngest("A", clockFixtureOne(4, "A/data", 5))

	// Expect no pending stat of B was created (though it cycles, it's already read it's own write).
	require.Empty(t, graph.pending)

	require.Equal(t, clockFixture(4, []string{"A/data", "B/data"}, []int{5, 6}),
		graph.writeClock)

	// Completed ingest & stat which *do* require a new stat.
	graph.CompletedIngest("A", clockFixtureOne(4, "A/data", 50))
	graph.CompletedStat(
		"B",
		progressFixture.Copy(),
		clockFixtureOne(4, "B/data", 60),
	)

	require.Equal(t, []PendingStat{
		{
			ReadyAt:  0,
			TaskName: "B",
			ReadThrough: clockFixture(4,
				[]string{"A/data;derive/B/A-to-B", "B/data;derive/B/B-to-B"}, []int{50, 60}),
		},
	}, graph.pending)

	require.Equal(t, clockFixture(4, []string{"A/data", "B/data"}, []int{50, 60}),
		graph.writeClock)
}

func TestReadyStats(t *testing.T) {
	var derivations = derivationsFixture(
		transformFixture("A", "A-to-A", "A", 0),
		transformFixture("A", "A-to-B", "B", 0),
		transformFixture("A", "A-to-C", "C", 0),
	)
	var graph = NewGraph(derivations)

	// Install pending fixtures.
	graph.pending = []PendingStat{
		{ReadyAt: 10, TaskName: "A", ReadThrough: clockFixture(1, nil, nil)},
		{ReadyAt: 10, TaskName: "B", ReadThrough: clockFixture(2, nil, nil)},
		{ReadyAt: 5, TaskName: "C", ReadThrough: clockFixture(3, nil, nil)},
	}

	var ready, next = graph.PopReadyStats()
	require.Empty(t, ready)
	require.Equal(t, TestTime(5), next)
	graph.CompletedAdvance(4)

	ready, next = graph.PopReadyStats()
	require.Empty(t, ready)
	require.Equal(t, TestTime(1), next)
	graph.CompletedAdvance(1)

	ready, next = graph.PopReadyStats()
	require.Equal(t, []PendingStat{
		{ReadyAt: 5, TaskName: "C", ReadThrough: clockFixture(3, nil, nil)},
	}, ready)
	require.Equal(t, TestTime(0), next)

	ready, next = graph.PopReadyStats()
	require.Empty(t, ready)
	require.Equal(t, TestTime(5), next)
	graph.CompletedAdvance(5)

	ready, next = graph.PopReadyStats()
	require.Equal(t, []PendingStat{
		{ReadyAt: 10, TaskName: "A", ReadThrough: clockFixture(1, nil, nil)},
		{ReadyAt: 10, TaskName: "B", ReadThrough: clockFixture(2, nil, nil)},
	}, ready)
	require.Equal(t, TestTime(0), next)

	ready, next = graph.PopReadyStats()
	require.Empty(t, ready)
	require.Equal(t, TestTime(-1), next)
}

func TestTaskIndexing(t *testing.T) {
	var tasks = []*pf.CatalogTask{
		{
			Capture: &pf.CaptureSpec{
				Capture: "a/capture/task",
				Bindings: []*pf.CaptureSpec_Binding{
					{Collection: pf.CollectionSpec{Collection: "a/capture/one"}},
					{Collection: pf.CollectionSpec{Collection: "a/capture/two"}},
				},
			},
		},
		{
			Derivation: &pf.DerivationSpec{
				Collection: pf.CollectionSpec{Collection: "a/derivation"},
				Transforms: []pf.TransformSpec{
					{
						Shuffle: pf.Shuffle{
							SourceCollection: "a/capture/one",
							GroupName:        "derive/A",
						},
					},
					{
						Shuffle: pf.Shuffle{
							SourceCollection: "a/capture/one",
							GroupName:        "derive/AA",
							ReadDelaySeconds: 5,
						},
					},
					{
						Shuffle: pf.Shuffle{
							SourceCollection: "a/capture/two",
							GroupName:        "derive/B",
						},
					},
				},
			},
		},
		{
			Materialization: &pf.MaterializationSpec{
				Materialization: "a/materialization",
				Bindings: []*pf.MaterializationSpec_Binding{
					{
						Shuffle: pf.Shuffle{
							SourceCollection: "a/derivation",
							GroupName:        "mat/1",
						},
					},
					{
						Shuffle: pf.Shuffle{
							SourceCollection: "a/capture/two",
							GroupName:        "mat/2",
						},
					},
				},
			},
		},
	}

	// Build a Graph from the task fixtures, and verify the expected indices.
	var graph = NewGraph(tasks)

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
