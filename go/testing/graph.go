package testing

import (
	"strings"
	"time"

	pf "github.com/estuary/flow/go/protocols/flow"
	pb "go.gazette.dev/core/broker/protocol"
)

// TestTime is an effective test time-point, in seconds. It has no relation
// to wall-clock time; test time is synthetically advanced as a test progresses.
type TestTime time.Duration

func (tt TestTime) String() string {
	return time.Duration(tt).String()
}

// TaskName is a type wrapper of a CatalogTask.Name()
// (which is itself a pf.Capture, pf.Collection (derivation), or pf.Materialization).
type TaskName string

// PendingStat is a CatalogTask's read of its source which may not have
// happened yet.
type PendingStat struct {
	// Time at which the task's read is unblocked.
	ReadyAt TestTime
	// Name of the CatalogTask.
	TaskName TaskName
	// Clock which this stat must read through.
	ReadThrough pb.Offsets
}

// Graph maintains the data-flow status of a running catalog.
type Graph struct {
	// Current test time.
	atTime TestTime
	// Index of CatalogTasks to their output Collections.
	outputs map[TaskName][]pf.Collection
	// Index of read Collections, and the CatalogTasks that read them.
	// Recall that a CatalogTask can have more than one read of a collection.
	readers map[pf.Collection][]taskRead
	// Index of each task to its readThrough Clock.
	readThrough map[TaskName]pb.Offsets
	// Pending reads which remain to be stat-ed.
	pending []PendingStat
	// Overall progress of the cluster.
	writeClock pb.Offsets
}

type taskRead struct {
	task TaskName
	// Expected suffix which the taskRead appends to read journal names.
	suffix string
	// Read delay applied by the taskRead.
	delay TestTime
}

// NewGraph constructs a new *Graph.
func NewGraph(
	captures []*pf.CaptureSpec,
	collections []*pf.CollectionSpec,
	materializations []*pf.MaterializationSpec,
) *Graph {
	var g = &Graph{
		atTime:      0,
		outputs:     make(map[TaskName][]pf.Collection),
		readers:     make(map[pf.Collection][]taskRead),
		readThrough: make(map[TaskName]pb.Offsets),
		pending:     nil,
		writeClock:  nil,
	}

	for _, t := range captures {
		g.addTask(t)
	}
	for _, t := range collections {
		if t.Derivation != nil {
			g.addTask(t)
		}
	}
	for _, t := range materializations {
		g.addTask(t)
	}

	return g
}

// addTask to the Graph, tracking dataflow through the task.
func (g *Graph) addTask(t pf.Task) {
	if t.TaskShardTemplate().Disable {
		// Ignore dataflows of disabled tasks.
		return
	}
	var name = TaskName(t.TaskName())

	// Index into |outputs|.
	if capture, ok := t.(*pf.CaptureSpec); ok {
		for _, b := range capture.Bindings {
			g.outputs[name] = append(g.outputs[name], b.Collection.Name)
		}
	} else if derivation, ok := t.(*pf.CollectionSpec); ok {
		g.outputs[name] = append(g.outputs[name], derivation.Name)
	}

	// Index into |readers|.
	if derivation, ok := t.(*pf.CollectionSpec); ok && derivation.Derivation != nil {
		for _, t := range derivation.Derivation.Transforms {
			g.readers[t.Collection.Name] = append(
				g.readers[t.Collection.Name],
				taskRead{
					task:   name,
					suffix: ";" + t.JournalReadSuffix,
					delay:  TestTime(time.Second * time.Duration(t.ReadDelaySeconds)),
				})
		}
	} else if materialization, ok := t.(*pf.MaterializationSpec); ok {
		for _, b := range materialization.Bindings {
			g.readers[b.Collection.Name] = append(
				g.readers[b.Collection.Name],
				taskRead{
					task:   name,
					suffix: ";" + b.JournalReadSuffix,
					delay:  0,
				})
		}
	} else if _, ok := t.(*pf.CaptureSpec); ok {
		// Synthesize a taskRead for pseudo-journals of a capture task.
		g.readers[pf.Collection(name)] = []taskRead{{
			task:   name,
			suffix: "",
			delay:  0,
		}}
	}

	g.readThrough[name] = nil
}

// HasPendingWrite is true if there is at least one pending task which may
// directly or recursively write into the named |collection|.
func (g *Graph) HasPendingWrite(collection pf.Collection) bool {
	// See: https://cybernetist.com/2019/03/09/breadth-first-search-using-go-standard-library/
	var fifo []TaskName
	var visited = make(map[TaskName]struct{})

	for _, pending := range g.pending {
		fifo = append(fifo, pending.TaskName)
		visited[pending.TaskName] = struct{}{}
	}

	for len(fifo) != 0 {
		var task TaskName
		task, fifo = fifo[0], fifo[1:] // Pop next node to visit.

		// For each collection produced into by |task|, and for each
		// |child| task which in turn reads that collection, enqueue the child.
		for _, output := range g.outputs[task] {

			// If the present |task| directly outputs into |collection|,
			// then |collection| has a pending write (from |task| itself,
			// or a parent task which could cause |task| to be pending).
			if output == collection {
				return true // Search target found.
			}

			for _, r := range g.readers[output] {
				if _, ok := visited[r.task]; !ok {
					// Queue for exploration.
					visited[r.task] = struct{}{}
					fifo = append(fifo, r.task)
				}
			}
		}
	}
	return false
}

// PopReadyStats removes and returns tracked PendingStats having ready-at
// times equal to the current test time. It also returns the delta between
// the current TestTime, and the next ready PendingStat (which is always
// zero if PendingStats are returned) as well as the TaskName associated.
func (g *Graph) PopReadyStats() ([]PendingStat, TestTime, TaskName) {
	var ready []PendingStat
	var nextReadyTime TestTime = -1
	var nextReadyName TaskName
	var r, w int // Read & write index.

	// Process |pending| by copying out matched elements and
	// shifting remaining elements down.
	for ; r != len(g.pending); r++ {
		var delta = g.pending[r].ReadyAt - g.atTime

		if nextReadyTime == -1 || delta < nextReadyTime {
			nextReadyTime = delta
			nextReadyName = g.pending[r].TaskName
		}

		if delta == 0 {
			ready = append(ready, g.pending[r])
		} else {
			g.pending[w] = g.pending[r]
			w++
		}
	}
	g.pending = g.pending[:w]

	return ready, nextReadyTime, nextReadyName
}

// CompletedIngest tells the Graph of a completed ingestion step.
func (g *Graph) CompletedIngest(collection pf.Collection, writeAt pb.Offsets) {
	g.writeClock = MaxClock(g.writeClock, writeAt)
	g.projectWrite(collection, writeAt)
}

// CompletedStat tells the Graph of a completed task stat.
//   - |readClock| is a min-reduced Clock over read progress across derivation shards.
//     It's journals include group-name suffixes (as returned from Gazette's Stat).
//   - |writeClock| is a max-reduced Clock over write progress across derivation shards.
//     It's journals *don't* include group names (again, as returned from Gazette's Stat).
func (g *Graph) CompletedStat(task TaskName, readThrough pb.Offsets, writeAt pb.Offsets) {
	g.writeClock = MaxClock(g.writeClock, writeAt)
	g.readThrough[task] = readThrough // Track progress of this task.

	for _, output := range g.outputs[task] {
		g.projectWrite(output, writeAt)
	}
}

func (g *Graph) projectWrite(collection pf.Collection, writeAt pb.Offsets) {
	for _, r := range g.readers[collection] {
		// Map `writeAt` to its corresponding `readThrough`,
		// having filtered journals and suffixes scoped to this transform.
		var readThrough = make(pb.Offsets)
		for journal, offset := range writeAt {
			if strings.HasPrefix(journal.String(), collection.String()+"/") {
				readThrough[journal+pb.Journal(r.suffix)] = offset
			}
		}

		if ContainsClock(g.readThrough[r.task], readThrough) {
			continue // Transform stat not required.
		}

		// Stat implied by this projected write.
		var add = PendingStat{
			ReadyAt:     g.atTime + r.delay,
			TaskName:    r.task,
			ReadThrough: readThrough,
		}

		// Fold |stat| into a matched PendingStat, if one exists. Otherwise add it.
		var found bool
		for i := range g.pending {
			var pending = &g.pending[i] // Mutate in place.

			if pending.TaskName == add.TaskName && pending.ReadyAt == add.ReadyAt {
				pending.ReadThrough = MaxClock(pending.ReadThrough, add.ReadThrough)
				found = true
			}
		}
		if !found {
			g.pending = append(g.pending, add)
		}
	}
}

// CompletedAdvance tells the Graph of an increment in the current TestTime.
func (g *Graph) CompletedAdvance(delta TestTime) {
	g.atTime += delta

	for _, pending := range g.pending {
		if pending.ReadyAt < g.atTime {
			panic("time advanced beyond pending stat")
		}
	}
}
