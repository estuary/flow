package fragment

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/broker/protocol"
)

func TestSetAddInsertAtEnd(t *testing.T) {
	var set CoverSet

	require.True(t, setAdd(&set, 100, 200))
	require.True(t, setAdd(&set, 200, 300))
	require.True(t, setAdd(&set, 201, 301))

	require.Equal(t, CoverSet{
		{Fragment: protocol.Fragment{Begin: 100, End: 200}},
		{Fragment: protocol.Fragment{Begin: 200, End: 300}},
		{Fragment: protocol.Fragment{Begin: 201, End: 301}},
	}, set)
}

func TestSetAddReplaceRangeAtEnd(t *testing.T) {
	var set CoverSet

	require.True(t, setAdd(&set, 100, 200))
	require.True(t, setAdd(&set, 200, 300)) // Replaced.
	require.True(t, setAdd(&set, 300, 400)) // Replaced.
	require.True(t, setAdd(&set, 400, 500)) // Replaced.
	require.True(t, setAdd(&set, 150, 500))

	require.Equal(t, CoverSet{
		{Fragment: protocol.Fragment{Begin: 100, End: 200}},
		{Fragment: protocol.Fragment{Begin: 150, End: 500}},
	}, set)
}

func TestSetAddReplaceOneAtEnd(t *testing.T) {
	var set CoverSet

	require.True(t, setAdd(&set, 100, 200))
	require.True(t, setAdd(&set, 200, 300)) // Replaced.
	require.True(t, setAdd(&set, 199, 300))

	require.Equal(t, CoverSet{
		{Fragment: protocol.Fragment{Begin: 100, End: 200}},
		{Fragment: protocol.Fragment{Begin: 199, End: 300}},
	}, set)

	set = CoverSet{}
	require.True(t, setAdd(&set, 100, 200))
	require.True(t, setAdd(&set, 200, 300)) // Replaced.
	require.True(t, setAdd(&set, 200, 301))

	require.Equal(t, CoverSet{
		{Fragment: protocol.Fragment{Begin: 100, End: 200}},
		{Fragment: protocol.Fragment{Begin: 200, End: 301}},
	}, set)
}

func TestSetAddReplaceRangeInMiddle(t *testing.T) {
	var set CoverSet

	require.True(t, setAdd(&set, 100, 200))
	require.True(t, setAdd(&set, 200, 300)) // Replaced.
	require.True(t, setAdd(&set, 300, 400)) // Replaced.
	require.True(t, setAdd(&set, 400, 500))
	require.True(t, setAdd(&set, 150, 450))

	require.Equal(t, CoverSet{
		{Fragment: protocol.Fragment{Begin: 100, End: 200}},
		{Fragment: protocol.Fragment{Begin: 150, End: 450}},
		{Fragment: protocol.Fragment{Begin: 400, End: 500}},
	}, set)
}

func TestSetAddReplaceOneInMiddle(t *testing.T) {
	var set CoverSet

	require.True(t, setAdd(&set, 100, 200))
	require.True(t, setAdd(&set, 200, 300)) // Replaced.
	require.True(t, setAdd(&set, 300, 400))
	require.True(t, setAdd(&set, 150, 350))

	require.Equal(t, CoverSet{
		{Fragment: protocol.Fragment{Begin: 100, End: 200}},
		{Fragment: protocol.Fragment{Begin: 150, End: 350}},
		{Fragment: protocol.Fragment{Begin: 300, End: 400}},
	}, set)
}

func TestSetAddInsertInMiddleExactBoundaries(t *testing.T) {
	var set CoverSet

	require.True(t, setAdd(&set, 100, 200))
	require.True(t, setAdd(&set, 300, 400))
	require.True(t, setAdd(&set, 200, 300))

	require.Equal(t, CoverSet{
		{Fragment: protocol.Fragment{Begin: 100, End: 200}},
		{Fragment: protocol.Fragment{Begin: 200, End: 300}},
		{Fragment: protocol.Fragment{Begin: 300, End: 400}},
	}, set)
}

func TestSetAddInsertInMiddleCloseBoundaries(t *testing.T) {
	var set CoverSet

	require.True(t, setAdd(&set, 100, 200))
	require.True(t, setAdd(&set, 300, 400))
	require.True(t, setAdd(&set, 201, 299))

	require.Equal(t, CoverSet{
		{Fragment: protocol.Fragment{Begin: 100, End: 200}},
		{Fragment: protocol.Fragment{Begin: 201, End: 299}},
		{Fragment: protocol.Fragment{Begin: 300, End: 400}},
	}, set)
}

func TestSetAddReplaceRangeAtBeginning(t *testing.T) {
	var set CoverSet

	require.True(t, setAdd(&set, 100, 200)) // Replaced.
	require.True(t, setAdd(&set, 200, 300)) // Replaced.
	require.True(t, setAdd(&set, 300, 400))
	require.True(t, setAdd(&set, 100, 300))

	require.Equal(t, CoverSet{
		{Fragment: protocol.Fragment{Begin: 100, End: 300}},
		{Fragment: protocol.Fragment{Begin: 300, End: 400}},
	}, set)
}

func TestSetAddReplaceOneAtBeginning(t *testing.T) {
	var set CoverSet

	require.True(t, setAdd(&set, 100, 200)) // Replaced.
	require.True(t, setAdd(&set, 200, 300))
	require.True(t, setAdd(&set, 300, 400))
	require.True(t, setAdd(&set, 99, 200))

	require.Equal(t, CoverSet{
		{Fragment: protocol.Fragment{Begin: 99, End: 200}},
		{Fragment: protocol.Fragment{Begin: 200, End: 300}},
		{Fragment: protocol.Fragment{Begin: 300, End: 400}},
	}, set)
}

func TestSetAddInsertAtBeginning(t *testing.T) {
	var set CoverSet

	require.True(t, setAdd(&set, 200, 300))
	require.True(t, setAdd(&set, 300, 400))
	require.True(t, setAdd(&set, 199, 200))

	require.Equal(t, CoverSet{
		{Fragment: protocol.Fragment{Begin: 199, End: 200}},
		{Fragment: protocol.Fragment{Begin: 200, End: 300}},
		{Fragment: protocol.Fragment{Begin: 300, End: 400}},
	}, set)
}

func TestSetAddOverlappingRanges(t *testing.T) {
	var set CoverSet

	require.True(t, setAdd(&set, 100, 150))
	require.True(t, setAdd(&set, 149, 201))
	require.True(t, setAdd(&set, 200, 250))
	require.True(t, setAdd(&set, 250, 300))
	require.True(t, setAdd(&set, 299, 351))
	require.True(t, setAdd(&set, 350, 400))

	require.True(t, setAdd(&set, 200, 300))
	require.True(t, setAdd(&set, 300, 400))
	require.True(t, setAdd(&set, 100, 200))

	require.Equal(t, CoverSet{
		{Fragment: protocol.Fragment{Begin: 100, End: 200}},
		{Fragment: protocol.Fragment{Begin: 149, End: 201}},
		{Fragment: protocol.Fragment{Begin: 200, End: 300}},
		{Fragment: protocol.Fragment{Begin: 299, End: 351}},
		{Fragment: protocol.Fragment{Begin: 300, End: 400}},
	}, set)
}

func TestSetAddNoAction(t *testing.T) {
	var set CoverSet

	require.True(t, setAdd(&set, 100, 200))
	require.True(t, setAdd(&set, 200, 300))
	require.True(t, setAdd(&set, 300, 400))

	// Precondition.
	require.Equal(t, CoverSet{
		{Fragment: protocol.Fragment{Begin: 100, End: 200}},
		{Fragment: protocol.Fragment{Begin: 200, End: 300}},
		{Fragment: protocol.Fragment{Begin: 300, End: 400}},
	}, set)

	// Expect that only Add()s which exactly replace an entry are accepted.
	require.False(t, setAdd(&set, 101, 200))
	require.False(t, setAdd(&set, 100, 199))
	require.True(t, setAdd(&set, 100, 200))
	require.False(t, setAdd(&set, 201, 300))
	require.False(t, setAdd(&set, 200, 299))
	require.True(t, setAdd(&set, 200, 300))
	require.False(t, setAdd(&set, 301, 400))
	require.True(t, setAdd(&set, 300, 400))

	// Postcondition. No change.
	require.Equal(t, CoverSet{
		{Fragment: protocol.Fragment{Begin: 100, End: 200}},
		{Fragment: protocol.Fragment{Begin: 200, End: 300}},
		{Fragment: protocol.Fragment{Begin: 300, End: 400}},
	}, set)
}

func TestOffset(t *testing.T) {
	var set CoverSet
	require.Equal(t, int64(0), set.BeginOffset())
	require.Equal(t, int64(0), set.EndOffset())

	setAdd(&set, 100, 150)
	require.Equal(t, int64(100), set.BeginOffset())
	require.Equal(t, int64(150), set.EndOffset())

	setAdd(&set, 140, 250)
	require.Equal(t, int64(100), set.BeginOffset())
	require.Equal(t, int64(250), set.EndOffset())

	setAdd(&set, 50, 100)
	require.Equal(t, int64(50), set.BeginOffset())
	require.Equal(t, int64(250), set.EndOffset())
}

func TestLongestOverlappingFragment(t *testing.T) {
	var set CoverSet

	setAdd(&set, 100, 200)
	setAdd(&set, 149, 201)
	setAdd(&set, 200, 300)
	setAdd(&set, 299, 351)
	setAdd(&set, 300, 400)
	setAdd(&set, 500, 600)

	var cases = []struct {
		offset int64
		found  bool
		ind    int
	}{
		{0, false, 0},
		{100, true, 0},
		{148, true, 0},
		{149, true, 1},
		{199, true, 1},
		{200, true, 2},
		{298, true, 2},
		{299, true, 3},
		{300, true, 4},
		{350, true, 4},
		{399, true, 4},
		{400, false, 5},
		{401, false, 5},
		{499, false, 5},
		{500, true, 5},
		{599, true, 5},
		{600, false, 6},
	}

	for _, tc := range cases {
		var ind, found = set.LongestOverlappingFragment(tc.offset)
		require.Equal(t, tc.ind, ind)
		require.Equal(t, tc.found, found)
	}
}

func TestSetDifference(t *testing.T) {
	var a, b CoverSet

	require.True(t, setAdd(&b, 100, 200))
	require.True(t, setAdd(&b, 150, 225))
	require.True(t, setAdd(&b, 225, 250))
	require.True(t, setAdd(&b, 250, 300))
	require.True(t, setAdd(&b, 301, 400))

	require.True(t, setAdd(&a, 99, 105))  // Begins before first Fragment of |b|.
	require.True(t, setAdd(&a, 100, 110)) // Small Fragment overlapped by one Fragment of |b|.
	require.True(t, setAdd(&a, 110, 120)) // Another, overlapped by |b|.
	require.True(t, setAdd(&a, 130, 210)) // Larger Fragment covered by two Fragments in |b|.
	require.True(t, setAdd(&a, 150, 225)) // Exactly matched by Fragment in |b|.
	require.True(t, setAdd(&a, 151, 300)) // Larger Fragment covered by three Fragments in |b|.
	require.True(t, setAdd(&a, 152, 301)) // *Not* covered by |b|, due to 1-byte gap in coverage.
	require.True(t, setAdd(&a, 300, 350)) // Another, not covered due to gap in coverage.
	require.True(t, setAdd(&a, 301, 360)) // Fully covered Fragment.
	require.True(t, setAdd(&a, 360, 401)) // Fragment which spans past |b|'s coverage.
	require.True(t, setAdd(&a, 400, 450)) // Fragment fully outside |b|'s coverage.

	var out = CoverSetDifference(a, b)

	require.Equal(t, CoverSet{
		{Fragment: protocol.Fragment{Begin: 99, End: 105}},
		{Fragment: protocol.Fragment{Begin: 152, End: 301}},
		{Fragment: protocol.Fragment{Begin: 300, End: 350}},
		{Fragment: protocol.Fragment{Begin: 360, End: 401}},
		{Fragment: protocol.Fragment{Begin: 400, End: 450}},
	}, out)
}

func TestParseWithMultipleOverlaps(t *testing.T) {
	var set CoverSet

	f, _ := protocol.ParseFragmentFromRelativePath("a/journal",
		"0000000000000000-00000000400a8d41-0000000000000000000000000000000000000000.raw")
	set, _ = set.Add(Fragment{Fragment: f})
	f, _ = protocol.ParseFragmentFromRelativePath("a/journal",
		"00000000400a8d41-00000000595722d7-0000000000000000000000000000000000000000.raw")
	set, _ = set.Add(Fragment{Fragment: f})
	f, _ = protocol.ParseFragmentFromRelativePath("a/journal",
		"00000000400a8d41-000000007a3fb5ee-0000000000000000000000000000000000000000.raw")
	set, _ = set.Add(Fragment{Fragment: f})
	f, _ = protocol.ParseFragmentFromRelativePath("a/journal",
		"00000000400a8d41-000000007af92e59-0000000000000000000000000000000000000000.raw")
	set, _ = set.Add(Fragment{Fragment: f})
	f, _ = protocol.ParseFragmentFromRelativePath("a/journal",
		"00000000595722d7-000000007af92fb4-0000000000000000000000000000000000000000.raw")
	set, _ = set.Add(Fragment{Fragment: f})
	f, _ = protocol.ParseFragmentFromRelativePath("a/journal",
		"000000007a3fb5ee-000000007af92fb4-0000000000000000000000000000000000000000.raw")
	set, _ = set.Add(Fragment{Fragment: f})
	f, _ = protocol.ParseFragmentFromRelativePath("a/journal",
		"000000007af92e59-000000007af92e59-0000000000000000000000000000000000000000.raw")
	set, _ = set.Add(Fragment{Fragment: f})

	const none = protocol.CompressionCodec_NONE

	require.Equal(t, CoverSet{
		{Fragment: protocol.Fragment{Journal: "a/journal", Begin: 0, End: 0x400a8d41, CompressionCodec: none}},
		{Fragment: protocol.Fragment{Journal: "a/journal", Begin: 0x400a8d41, End: 0x7af92e59, CompressionCodec: none}},
		{Fragment: protocol.Fragment{Journal: "a/journal", Begin: 0x595722d7, End: 0x7af92fb4, CompressionCodec: none}},
	}, set)
}

func setAdd(s *CoverSet, begin, end int64) bool {
	var updated bool
	*s, updated = s.Add(Fragment{
		Fragment: protocol.Fragment{Begin: begin, End: end},
	})
	return updated
}
