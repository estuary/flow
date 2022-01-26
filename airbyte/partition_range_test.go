package airbyte

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPartitionRangeOverlap(t *testing.T) {
	for _, testCase := range []struct {
		expected                 RangeOverlap
		flowStart, flowEnd       uint32
		kinesisStart, kinesisEnd uint32
	}{
		{FullRangeOverlap, 0, math.MaxUint32, 0, math.MaxUint32},
		{FullRangeOverlap, 0, math.MaxUint32, 5, 5},
		{PartialRangeOverlap, 5, 6, 4, 5},
		{FullRangeOverlap, 4, 6, 6, 6},
		{NoRangeOverlap, 0, 5, 9, 10},
		{NoRangeOverlap, 6, 8, 0, 0},
	} {
		var flowRange = Range{
			Begin: testCase.flowStart,
			End:   testCase.flowEnd,
		}
		var kinesisRange = Range{
			Begin: testCase.kinesisStart,
			End:   testCase.kinesisEnd,
		}

		if o := flowRange.Overlaps(kinesisRange); o != testCase.expected {
			t.Logf("expected %#v, but got %#v", testCase.expected, o)
			t.Fail()
		}
	}
}

func TestRangeRoundTrip(t *testing.T) {
	var rng = Range{
		Begin: 12345,
		End:   678910,
	}
	require.NoError(t, rng.Validate())

	var b, err = json.Marshal(rng)
	require.NoError(t, err)
	require.Equal(t, `{"begin":"00003039","end":"000a5bfe"}`, string(b))

	var rng2 Range
	require.NoError(t, json.Unmarshal(b, &rng2))
	require.NoError(t, rng2.Validate())
	require.Equal(t, rng, rng2)
}
