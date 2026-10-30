package allocator

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/etcdtest"
	"go.gazette.dev/core/keyspace"
	gc "gopkg.in/check.v1"
)

func BenchmarkAll(b *testing.B) {
	b.Run("simulated-deploy", func(b *testing.B) {
		benchmarkSimulatedDeploy(b, false)
	})
	b.Run("simulated-deploy-down-first", func(b *testing.B) {
		benchmarkSimulatedDeploy(b, true)
	})
}

type BenchmarkHealthSuite struct{}

// TestBenchmarkHealth runs benchmarks with a small N to ensure they don't bit rot.
func (s *BenchmarkHealthSuite) TestBenchmarkHealth(c *gc.C) {
	var fakeB = testing.B{N: 1}

	benchmarkSimulatedDeploy(&fakeB, false)
	BenchmarkChangingReplication(&fakeB)
}

var _ = gc.Suite(&BenchmarkHealthSuite{})

// benchmarkSimulatedDeploy simulates a rolling deployment where half of the
// cluster's members are cycled out and replaced. If |downFirst|, members are
// marked as exiting before their replacements join, stressing the ShedCapacity
// mechanism. Otherwise, replacements join first (the typical rolling deploy).
func benchmarkSimulatedDeploy(b *testing.B, downFirst bool) {
	var client = etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var ctx = context.Background()
	var ks = NewAllocatorKeySpace("/root", testAllocDecoder{})
	var state = NewObservedState(ks, MemberKey(ks, "zone-a", "leader"), isConsistent)

	// Each stage of the deployment will cycle |NMembers10| Members.
	var NMembers10 = b.N
	var NMembersHalf = b.N * 5
	var NMembers = b.N * 10
	var NItems = NMembers * 200

	var mode = "up-first"
	if downFirst {
		mode = "down-first"
	}
	b.Logf("Benchmarking %s with %d items, %d members (%d /2, %d /10)", mode, NItems, NMembers, NMembersHalf, NMembers10)

	// Snapshot counters before this run.
	var addsBefore = counterVal(allocatorAssignmentAddedTotal)
	var removesBefore = counterVal(allocatorAssignmentRemovedTotal)
	var packsBefore = counterVal(allocatorAssignmentPackedTotal)

	// fill inserts (if |asInsert|) or modifies keys/values defined by |kvcb| and the range [begin, end).
	var fill = func(begin, end int, asInsert bool, kvcb func(i int) (string, string)) {
		var kv = make([]string, 0, 2*(end-begin))

		for i := begin; i != end; i++ {
			var k, v = kvcb(i)
			kv = append(kv, k)
			kv = append(kv, v)
		}
		if asInsert {
			require.NoError(b, insert(ctx, client, kv...))
		} else {
			require.NoError(b, update(ctx, client, kv...))
		}
	}

	// Insert a Member key which will act as the leader, and will not be rolled.
	require.NoError(b, insert(ctx, client, state.LocalKey, `{"R": 1}`))

	// Announce half of Members...
	fill(0, NMembersHalf, true, func(i int) (string, string) {
		return benchMemberKey(ks, i), `{"R": 1500}`
	})
	// And all Items.
	fill(0, NItems, true, func(i int) (string, string) {
		return ItemKey(ks, fmt.Sprintf("i%05d", i)), `{"R": 3}`
	})

	var testState = struct {
		nextBlock int  // Next block of Members to cycle down & up.
		nextDown  bool // Are we next scaling down, or up?
	}{nextDown: downFirst}

	var scaleUp = func(begin, end int) {
		fill(NMembersHalf+begin, NMembersHalf+end, true, func(i int) (string, string) {
			return benchMemberKey(ks, i), `{"R": 1205}` // Less than before, but _just_ enough.
		})
	}
	var scaleDown = func(begin, end int) {
		fill(begin, end, false, func(i int) (string, string) {
			return benchMemberKey(ks, i), `{"R": 1500, "E": true}`
		})
	}

	var testHook = func(round int, idle bool) {
		var begin = NMembers10 * testState.nextBlock
		var end = NMembers10 * (testState.nextBlock + 1)

		log.WithFields(log.Fields{
			"round": round,
			"idle":  idle,
			"begin": begin,
			"end":   end,
		}).Info("ScheduleCallback")

		if !idle {
			return
		} else if err := markAllConsistent(ctx, client, ks, ""); err == nil {
			log.Info("marked some items as consistent")
			return // We marked some items as consistent. Keep going.
		} else if err == io.ErrNoProgress {
			// Continue the next test step below.
		} else {
			log.WithField("err", err).Warn("failed to mark all consistent (will retry)")
			return
		}

		log.WithFields(log.Fields{
			"state.nextBlock": testState.nextBlock,
			"state.nextDown":  testState.nextDown,
		}).Info("next test step")

		if begin == NMembersHalf {
			// We've cycled all Members. Gracefully exit by marking as exiting,
			// and waiting for Serve to complete.
			update(ctx, client, state.LocalKey, `{"R": 1, "E": true}`)
			return
		}

		if testState.nextDown {
			scaleDown(begin, end)
		} else {
			scaleUp(begin, end)
		}
		testState.nextDown = !testState.nextDown

		// Advance to next block after completing a full cycle (both up and down).
		if testState.nextDown == downFirst {
			testState.nextBlock += 1
		}
	}

	require.NoError(b, ks.Load(ctx, client, 0))
	go ks.Watch(ctx, client)

	require.NoError(b, Allocate(AllocateArgs{
		Context:  ctx,
		Etcd:     client,
		State:    state,
		TestHook: testHook,
	}))

	log.WithFields(log.Fields{
		"mode":    mode,
		"adds":    counterVal(allocatorAssignmentAddedTotal) - addsBefore,
		"removes": counterVal(allocatorAssignmentRemovedTotal) - removesBefore,
		"packs":   counterVal(allocatorAssignmentPackedTotal) - packsBefore,
	}).Info("final metrics")
}

func BenchmarkChangingReplication(b *testing.B) {
	var client = etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var ctx = context.Background()
	var ks = NewAllocatorKeySpace("/root", testAllocDecoder{})
	var state = NewObservedState(ks, MemberKey(ks, "zone", "leader"), isConsistent)

	var NMembers = 10
	var NItems = 323 // Or: 32303
	var NMaxRun = 200
	var rng = rand.NewPCG(8675, 309)

	b.Logf("Benchmarking with %d items, %d members", NItems, NMembers)

	// fill inserts (if `asInsert`) or modifies keys/values defined by `kvcb` and the range [begin, end).
	var fill = func(begin, end int, asInsert bool, kvcb func(i int) (string, string)) {
		var kv = make([]string, 0, 2*(end-begin))

		for i := begin; i != end; i++ {
			var k, v = kvcb(i)
			kv = append(kv, k)
			kv = append(kv, v)
		}
		if asInsert {
			require.NoError(b, insert(ctx, client, kv...))
		} else {
			require.NoError(b, update(ctx, client, kv...))
		}
	}

	// Insert a Member key which will act as the leader.
	require.NoError(b, insert(ctx, client, state.LocalKey, `{"R": 1}`))

	// Announce all Members.
	fill(0, NMembers, true, func(i int) (string, string) {
		return MemberKey(ks, "zone-a", fmt.Sprintf("m%05d", i)), `{"R": 10000}`
	})
	// Announce all Items with full replication.
	fill(0, NItems, true, func(i int) (string, string) {
		return ItemKey(ks, fmt.Sprintf("i%05d", i)), `{"R":2}`
	})

	var testState = struct {
		step  int
		total int
	}{step: 0, total: 0}

	var testHook = func(round int, idle bool) {
		if !idle {
			return
		} else if err := markAllConsistent(ctx, client, ks, ""); err == nil {
			return
		} else if err == io.ErrNoProgress {
			// Continue the next test step below.
		} else {
			log.WithField("err", err).Warn("failed to mark all consistent (will retry)")
			return
		}

		// Pick a run of contiguous items, and update each to a random replication.
		// This strategy is designed to excercise imbalances of the number of
		// replication slots across allocation sub-problems.
		var r = rng.Uint64() % 3
		var run = 1 + int(rng.Uint64()%(uint64(NMaxRun)-1))
		var start = int(rng.Uint64() % uint64((NItems - run)))

		if r == 2 {
			r = 3 // All items begin with R=2.
		}

		log.WithFields(log.Fields{
			"r":     r,
			"run":   run,
			"start": start,
			"step":  testState.step,
		}).Info("next test step")

		if testState.step == b.N {
			// Begin a graceful exit.
			update(ctx, client, state.LocalKey, `{"R": 0, "E": true}`)
			return
		}
		testState.step += 1
		testState.total += run

		var value = fmt.Sprintf(`{"R":%d}`, r)
		fill(start, start+run, false, func(i int) (string, string) {
			return ItemKey(ks, fmt.Sprintf("i%05d", i)), value
		})
	}

	require.NoError(b, ks.Load(ctx, client, 0))
	go ks.Watch(ctx, client)

	require.NoError(b, Allocate(AllocateArgs{
		Context:  ctx,
		Etcd:     client,
		State:    state,
		TestHook: testHook,
	}))

	var adds = counterVal(allocatorAssignmentAddedTotal)
	var packs = counterVal(allocatorAssignmentPackedTotal)
	var removes = counterVal(allocatorAssignmentRemovedTotal)
	var ratio = (float64(adds-2*float64(NItems)) + float64(removes)) / float64(testState.total)

	log.WithFields(log.Fields{
		"adds":      adds,
		"packs":     packs,
		"removes":   removes,
		"run.ratio": ratio,
		"run.total": testState.total,
	}).Info("final metrics")
}

func benchMemberKey(ks *keyspace.KeySpace, i int) string {
	var zone string

	switch i % 5 {
	case 0, 2, 4:
		zone = "zone-a" // Larger than zone-b.
	case 1, 3:
		zone = "zone-b"
	}
	return MemberKey(ks, zone, fmt.Sprintf("m%05d", i))
}

func counterVal(c prometheus.Counter) float64 {
	var out dto.Metric
	if err := c.Write(&out); err != nil {
		panic(err)
	}
	return *out.Counter.Value
}
