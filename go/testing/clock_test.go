package testing

import (
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
)

func clockFixture(journals []string, offsets []int) pb.Offsets {
	var m = make(pb.Offsets)
	for i := range journals {
		m[pb.Journal(journals[i])] = int64(offsets[i])
	}
	return m
}

func clockFixtureOne(journal string, offset int) pb.Offsets {
	return clockFixture([]string{journal}, []int{offset})
}

func TestClockReductionAndOrdering(t *testing.T) {
	var c1 = pb.Offsets{"one": 1, "two": 2, "three": 3}
	var c2 = pb.Offsets{"one": 2, "two": 1, "four": 4}

	var rMin = MinClock(c1, c2)
	var rMax = MaxClock(c1, c2)

	require.Equal(t, rMin, pb.Offsets{"one": 1, "two": 1, "three": 3, "four": 4})
	require.Equal(t, rMax, pb.Offsets{"one": 2, "two": 2, "three": 3, "four": 4})

	// Verify ordering expectations.
	require.False(t, ContainsClock(c1, rMin))
	require.False(t, ContainsClock(c2, rMin))

	require.False(t, ContainsClock(c1, c2))
	require.False(t, ContainsClock(c2, c1))

	require.True(t, ContainsClock(rMax, c1))
	require.True(t, ContainsClock(rMax, c2))

	require.True(t, ContainsClock(rMax, rMin))
	require.False(t, ContainsClock(rMin, rMax))
}
