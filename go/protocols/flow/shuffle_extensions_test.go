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
		Shuffle: Shuffle{
			ShuffleKeyPtr: nil,
			Hash:          99999,
		},
	}

	require.EqualError(t, m.Validate(), "Journal: not a valid token (bad journal)")
	m.Journal = "a/journal"
	require.EqualError(t, m.Validate(), "Coordinator: not a valid token (bad shard)")
	m.Coordinator = "some-shard"

	require.EqualError(t, m.Validate(), "Shuffle: expected at least one ShuffleKeyPtr")
	m.Shuffle.ShuffleKeyPtr = []string{"/foo"}
	require.EqualError(t, m.Validate(), "Shuffle: unknown Hash (99999)")
	m.Shuffle.Hash = Shuffle_MD5

	require.Nil(t, m.Validate())
}

func TestShuffleRequest(t *testing.T) {
	var m = &ShuffleRequest{
		Resolution: badHeaderFixture(),
		Shuffle: JournalShuffle{
			Journal:     "a/journal",
			Coordinator: "bad coordinator",
			Shuffle: Shuffle{
				ShuffleKeyPtr: []string{"/foo"},
			},
		},
		Range: RangeSpec{
			KeyBegin:    nil,
			KeyEnd:      nil,
			RClockBegin: 0,
			RClockEnd:   0,
		},
		Offset:    -1,
		EndOffset: 100,
	}

	require.EqualError(t, m.Validate(), "Resolution.Etcd: invalid ClusterId (expected != 0)")
	m.Resolution.Etcd.ClusterId = 1234
	require.EqualError(t, m.Validate(), "Shuffle.Coordinator: not a valid token (bad coordinator)")
	m.Shuffle.Coordinator = "a-coordinator"
	require.EqualError(t, m.Validate(), "Range: expected KeyBegin < KeyEnd ([] vs [])")
	m.Range.KeyEnd = []byte("end")
	require.EqualError(t, m.Validate(), "Range: expected RClockBegin < RClockEnd (0 vs 0)")
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
