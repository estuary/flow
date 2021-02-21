package testing

import (
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
)

func clockFixture(rev int64, journals []string, offsets []int) *Clock {
	var m = make(pb.Offsets)
	for i := range journals {
		m[pb.Journal(journals[i])] = int64(offsets[i])
	}
	return &Clock{Etcd: pb.Header_Etcd{Revision: rev}, Offsets: m}
}

func clockFixtureOne(rev int64, journal string, offset int) *Clock {
	return clockFixture(rev, []string{journal}, []int{offset})
}

func TestClockReductionAndOrdering(t *testing.T) {
	var c1 = clockFixture(10, []string{"one", "two", "three"}, []int{1, 2, 3})
	var c2 = clockFixture(20, []string{"one", "two", "four"}, []int{2, 1, 4})

	var rMin = c1.Copy()
	var rMax = c1.Copy()

	rMin.ReduceMin(c2.Etcd, c2.Offsets)
	rMax.ReduceMax(c2.Etcd, c2.Offsets)

	require.Equal(t, rMin, clockFixture(10, []string{"one", "two", "three", "four"}, []int{1, 1, 3, 4}))
	require.Equal(t, rMax, clockFixture(20, []string{"one", "two", "three", "four"}, []int{2, 2, 3, 4}))

	// Verify ordering expectations.
	require.True(t, c1.Contains(rMin))
	require.True(t, c2.Contains(rMin))

	require.True(t, rMax.Contains(c1))
	require.True(t, rMax.Contains(c2))

	require.False(t, c1.Contains(c2))
	require.False(t, c2.Contains(c1))

	require.True(t, rMax.Contains(rMin))
	require.False(t, rMin.Contains(rMax))
}
