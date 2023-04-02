package flow

import (
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
)

func TestJournalShuffle(t *testing.T) {
	var m = &JournalShuffle{
		Journal:     "bad journal",
		Coordinator: "bad shard",
		Shuffle: &Shuffle{
			ShuffleKeyPtrs: nil,
		},
	}

	require.EqualError(t, m.Validate(), "Journal: not a valid token (bad journal)")
	m.Journal = "a/journal"
	require.EqualError(t, m.Validate(), "Coordinator: not a valid token (bad shard)")
	m.Coordinator = "some-shard"

	require.EqualError(t, m.Validate(), "Shuffle: missing GroupName")
	m.GroupName = "group/name"
	require.EqualError(t, m.Validate(), "Shuffle.SourceCollection: invalid length (0; expected 1 <= length <= 512)")
	m.SourceCollection = "source/collection"
	require.EqualError(t, m.Validate(), "Shuffle: missing SourceUuidPtr")
	m.SourceUuidPtr = "/uuid"

	require.EqualError(t, m.Validate(), "Shuffle: missing ShuffleKeyPtr")
	m.Shuffle.ShuffleKeyPtrs = []string{"/foo"}

	require.EqualError(t, m.Validate(), "missing BuildId")
	m.BuildId = "an-id"

	require.Nil(t, m.Validate())
}

func TestShuffleRequest(t *testing.T) {
	var m = &ShuffleRequest{
		Resolution: badHeaderFixture(),
		Shuffle: JournalShuffle{
			Journal:     "a/journal",
			Coordinator: "bad coordinator",
			Shuffle: &Shuffle{
				GroupName:        "group/name",
				ShuffleKeyPtrs:   []string{"/foo"},
				SourceCollection: "source",
				SourceUuidPtr:    "/uuid",
			},
			BuildId: "an-id",
		},
		Range: RangeSpec{
			KeyBegin:    42,
			KeyEnd:      32,
			RClockBegin: 1,
			RClockEnd:   0,
		},
		Offset:    -1,
		EndOffset: 100,
	}

	require.EqualError(t, m.Validate(), "Resolution.Etcd: invalid ClusterId (expected != 0)")
	m.Resolution.Etcd.ClusterId = 1234
	require.EqualError(t, m.Validate(), "Shuffle.Coordinator: not a valid token (bad coordinator)")
	m.Shuffle.Coordinator = "a-coordinator"
	require.EqualError(t, m.Validate(), "Range: expected KeyBegin <= KeyEnd (0000002a vs 00000020)")
	m.Range.KeyEnd = 52
	require.EqualError(t, m.Validate(), "Range: expected RClockBegin <= RClockEnd (00000001 vs 00000000)")
	m.Range.RClockEnd = 12345
	require.EqualError(t, m.Validate(), "invalid Offset (-1; expected 0 <= Offset <= MaxInt64)")
	m.Offset = 200
	require.EqualError(t, m.Validate(), "invalid EndOffset (100; expected 0 or Offset <= EndOffset)")
	m.EndOffset = 300

	require.Nil(t, m.Validate())
}

func badHeaderFixture() *pb.Header {
	return &pb.Header{
		ProcessId: pb.ProcessSpec_ID{Zone: "zone", Suffix: "name"},
		Route:     pb.Route{Primary: 0, Members: []pb.ProcessSpec_ID{{Zone: "zone", Suffix: "name"}}},
		Etcd: pb.Header_Etcd{
			ClusterId: 0, // ClusterId is invalid, but easily fixed up.
			MemberId:  34,
			Revision:  56,
			RaftTerm:  78,
		},
	}
}

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
