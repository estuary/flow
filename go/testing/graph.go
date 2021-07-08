package testing

import (
	"time"

	pf "github.com/estuary/flow/go/protocols/flow"
	pb "go.gazette.dev/core/broker/protocol"
)

// TestTime is an effective test time-point, in seconds. It has no relation
// to wall-clock time; test time is synthetically advanced as a test progresses.
type TestTime time.Duration

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
	ReadThrough *Clock
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
	readThrough map[TaskName]*Clock
	// Pending reads which remain to be stat-ed.
	pending []PendingStat
	// Overall progress of the cluster.
	writeClock *Clock
}

type taskRead struct {
	task TaskName
	// Expected suffix which the taskRead appends to read journal names.
	suffix string
	// Read delay applied by the taskRead.
	delay TestTime
}

// NewGraph constructs a new *Graph.
func NewGraph(tasks []*pf.CatalogTask) *Graph {
	var outputs = make(map[TaskName][]pf.Collection)
	var readers = make(map[pf.Collection][]taskRead)
	var readThrough = make(map[TaskName]*Clock)

	for _, t := range tasks {
		var name = TaskName(t.Name())

		// Index into |outputs|.
		if t.Capture != nil {
			for _, b := range t.Capture.Bindings {
				outputs[name] = append(outputs[name], b.Collection.Collection)
			}
		} else if t.Derivation != nil {
			outputs[name] = append(outputs[name], t.Derivation.Collection.Collection)
		}

		// Index into |readers|.
		for _, shuffle := range t.Shuffles() {
			readers[shuffle.SourceCollection] = append(
				readers[shuffle.SourceCollection],
				taskRead{
					task:   name,
					suffix: ";" + shuffle.GroupName,
					delay:  TestTime(time.Second * time.Duration(shuffle.ReadDelaySeconds)),
				})
		}
		// Synthesize a taskRead for pseudo-journals of a capture task.
		if t.Capture != nil {
			readers[pf.Collection(name)] = []taskRead{{
				task:   name,
				suffix: "",
				delay:  0,
			}}
		}

		readThrough[name] = new(Clock)
	}

	return &Graph{
		atTime:      0,
		outputs:     outputs,
		readers:     readers,
		readThrough: readThrough,
		pending:     nil,
		writeClock:  new(Clock),
	}
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
// zero if PendingStats are returned).
func (g *Graph) PopReadyStats() ([]PendingStat, TestTime) {
	var ready []PendingStat
	var nextReady TestTime = -1
	var r, w int // Read & write index.

	// Process |pending| by copying out matched elements and
	// shifting remaining elements down.
	for ; r != len(g.pending); r++ {
		var delta = g.pending[r].ReadyAt - g.atTime

		if nextReady == -1 || delta < nextReady {
			nextReady = delta
		}

		if delta == 0 {
			ready = append(ready, g.pending[r])
		} else {
			g.pending[w] = g.pending[r]
			w++
		}
	}
	g.pending = g.pending[:w]

	return ready, nextReady
}

// CompletedIngest tells the Graph of a completed ingestion step.
func (g *Graph) CompletedIngest(collection pf.Collection, writeClock *Clock) {
	g.writeClock.ReduceMax(writeClock.Etcd, writeClock.Offsets)
	g.projectWrite(collection, writeClock)
}

// CompletedStat tells the Graph of a completed task stat.
// * |readClock| is a min-reduced Clock over read progress across derivation shards.
//   It's journals include group-name suffixes (as returned from Gazette's Stat).
// * |writeClock| is a max-reduced Clock over write progress across derivation shards.
//   It's journals *don't* include group names (again, as returned from Gazette's Stat).
func (g *Graph) CompletedStat(task TaskName, readClock *Clock, writeClock *Clock) {
	g.writeClock.ReduceMax(writeClock.Etcd, writeClock.Offsets)
	g.readThrough[task] = readClock // Track progress of this task.

	for _, output := range g.outputs[task] {
		g.projectWrite(output, writeClock)
	}
}

func (g *Graph) projectWrite(collection pf.Collection, writeClock *Clock) {
	for _, r := range g.readers[collection] {
		// Map |writeClock| to its equivalent read |clock|, having journal suffixes
		// specific to this transform's GroupName.
		var clock = &Clock{
			Etcd:    writeClock.Etcd,
			Offsets: make(pb.Offsets, len(writeClock.Offsets)),
		}
		for journal, offset := range writeClock.Offsets {
			journal = pb.Journal(journal + pb.Journal(r.suffix))
			clock.Offsets[journal] = offset
		}

		// Has |task| already read through the mapped read |clock| ?
		if g.readThrough[r.task].Contains(clock) {
			continue // Transform stat not required.
		}

		// Stat implied by this projected write.
		var add = PendingStat{
			ReadyAt:     g.atTime + r.delay,
			TaskName:    r.task,
			ReadThrough: clock,
		}

		// Fold |stat| into a matched PendingStat, if one exists. Otherwise add it.
		var found bool
		for _, pending := range g.pending {
			if pending.TaskName == add.TaskName && pending.ReadyAt == add.ReadyAt {
				pending.ReadThrough.ReduceMax(add.ReadThrough.Etcd, add.ReadThrough.Offsets)
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
