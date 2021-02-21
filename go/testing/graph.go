package testing

import (
	"fmt"
	"time"

	pf "github.com/estuary/flow/go/protocols/flow"
	pb "go.gazette.dev/core/broker/protocol"
)

// TestTime is an effective test time-point, in seconds. It has no relation
// to wall-clock time; test time is synthetically advanced as a test progresses.
type TestTime time.Duration

// PendingStat is a derivation's read of its source which
// may not have happened yet. Field ordering is important:
// we dequeue PendingStats in the order which they're ready.
type PendingStat struct {
	// Time at which the transformation's read is unblocked.
	ReadyAt TestTime
	// Derivation of the transformation.
	Derivation pf.Collection
	// Clock which this stat must read through.
	ReadThrough *Clock
}

// Graph maintains the data-flow status of a running catalog.
type Graph struct {
	// Current test time.
	atTime TestTime
	// Index of transforms for each derivation.
	transforms map[pf.Collection][]pf.TransformSpec
	// Index of read readThrough for each derivation.
	readThrough map[pf.Collection]*Clock
	// Pending reads which remain to be stat-ed.
	pending []PendingStat
	// Overall progress of the cluster.
	writeClock *Clock
}

// NewGraph constructs a new *Graph.
func NewGraph(transforms []pf.TransformSpec) *Graph {
	var grouped = make(map[pf.Collection][]pf.TransformSpec)
	var readThrough = make(map[pf.Collection]*Clock)

	for _, transform := range transforms {
		grouped[transform.Derivation] = append(grouped[transform.Derivation], transform)
	}
	for derivation := range grouped {
		readThrough[derivation] = new(Clock)
	}

	return &Graph{
		atTime:      0,
		transforms:  grouped,
		readThrough: readThrough,
		pending:     nil,
		writeClock:  new(Clock),
	}
}

// HasPendingParent is true if there is at least one PendingStat which will derive into
// |collection|, or one of it's antecedents.
func (g *Graph) HasPendingParent(collection pf.Collection) bool {
	// See: https://cybernetist.com/2019/03/09/breadth-first-search-using-go-standard-library/
	var visited = map[pf.Collection]struct{}{
		collection: {},
	}
	var fifo = []pf.Collection{collection}

	for len(fifo) != 0 {
		collection, fifo = fifo[0], fifo[1:] // Pop next node to visit.

		// Does |collection| have a pending stat (our search goal) ?
		for _, pending := range g.pending {
			if pending.Derivation == collection {
				return true
			}
		}

		for _, tf := range g.transforms[collection] {
			var next = tf.Shuffle.SourceCollection
			if _, ok := visited[next]; !ok {
				// Queue for exploration.
				visited[next] = struct{}{}
				fifo = append(fifo, next)
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

// CompletedStat tells the Graph of a completed derivation stat.
// * |readClock| is a min-reduced Clock over read progress across derivation shards.
//   It's journals include group-name suffixes (as returned from Gazette's Stat).
// * |writeClock| is a max-reduced Clock over write progress across derivation shards.
//   It's journals *don't* include group names (again, as returned from Gazette's Stat).
func (g *Graph) CompletedStat(derivation pf.Collection, readClock *Clock, writeClock *Clock) {
	g.writeClock.ReduceMax(writeClock.Etcd, writeClock.Offsets)
	g.readThrough[derivation] = readClock // Track progress of this derivation.
	g.projectWrite(derivation, writeClock)
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

func (g *Graph) projectWrite(collection pf.Collection, writeClock *Clock) {

	for derivation, transforms := range g.transforms {
		for _, transform := range transforms {
			// Skip transforms that don't read from |collection|.
			if transform.Shuffle.SourceCollection != collection {
				continue
			}
			// Map |writeClock| to its equivalent read Clock, having journal suffixes
			// specific to this transform's GroupName.
			var clock = &Clock{
				Etcd:    writeClock.Etcd,
				Offsets: make(pb.Offsets, len(writeClock.Offsets)),
			}
			for journal, offset := range writeClock.Offsets {
				journal = pb.Journal(fmt.Sprintf("%s;%s", journal, transform.Shuffle.GroupName))
				clock.Offsets[journal] = offset
			}

			// Has the derivation already read through |writeClock| ?
			if g.readThrough[derivation].Contains(clock) {
				continue // Transform stat not required.
			}

			// Stat implied by this projected write.
			var add = PendingStat{
				ReadyAt: g.atTime + TestTime(
					time.Second*time.Duration(transform.Shuffle.ReadDelaySeconds)),
				Derivation:  derivation,
				ReadThrough: clock,
			}

			// Fold |stat| into a matched PendingStat, if one exists. Otherwise add it.
			var found bool
			for _, pending := range g.pending {
				if pending.Derivation == add.Derivation && pending.ReadyAt == add.ReadyAt {
					pending.ReadThrough.ReduceMax(add.ReadThrough.Etcd, add.ReadThrough.Offsets)
					found = true
				}
			}
			if !found {
				g.pending = append(g.pending, add)
			}
		}
	}
}
