package flow

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRangeSpecOrdering(t *testing.T) {
	var model = RangeSpec{
		KeyBegin:    0x00,
		KeyEnd:      0xb0,
		RClockBegin: 0xc0,
		RClockEnd:   0xd0,
	}
	// RangeSpec is not less than itself.
	require.False(t, model.Less(&model))
	require.True(t, model.Equal(&model))

	// |model| and |other| cover discontinuous chunks of r-clock range.
	var other = RangeSpec{
		KeyBegin:    0x00,
		KeyEnd:      0xb0,
		RClockBegin: 0xe0,
		RClockEnd:   0xf0,
	}
	require.True(t, model.Less(&other))
	require.False(t, other.Less(&model))
	require.False(t, other.Equal(&model))

	// |model| and |other| are continuous r-clock ranges, but non-overlapping.
	other.RClockBegin = 0xd1
	require.True(t, model.Less(&other))
	require.False(t, other.Less(&model))
	require.False(t, other.Equal(&model))

	// |other| r-clock range now overlaps with |model|.
	other.RClockBegin = 0xc9
	require.False(t, model.Less(&other))
	require.False(t, other.Less(&model))

	// |model| and |other| cover discontinuous chunks of key range.
	// They continue to cover overlapping r-clock range.
	model.KeyEnd = 0x9f
	other.KeyBegin = 0xaa
	require.True(t, model.Less(&other))
	require.False(t, other.Less(&model))

	// They cover continuous but non-overlapping key ranges.
	other.KeyBegin = 0xa0
	require.True(t, model.Less(&other))
	require.False(t, other.Less(&model))
}
