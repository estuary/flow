package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
)

func TestRing(t *testing.T) {
	var m = &Ring{
		Name:    "bad name",
		Members: []Ring_Member{},
	}

	require.EqualError(t, m.Validate(), "Name: not a valid token (bad name)")
	m.Name = "a-ring"

	require.EqualError(t, m.Validate(), "expected at least one Member")
	m.Members = []Ring_Member{
		{},
		{MinMsgClock: 789},
		{MinMsgClock: 456, MaxMsgClock: 123}, // Out of order.
		{MaxMsgClock: 1011},
	}

	require.EqualError(t, m.Validate(), "Members[2]: invalid min/max clocks (min clock 456 > max 123)")
	m.Members[2].MinMsgClock, m.Members[2].MaxMsgClock = m.Members[2].MaxMsgClock, m.Members[2].MinMsgClock

	require.Nil(t, m.Validate())

	m.Members = append(m.Members, make([]Ring_Member, 1024)...)
	require.Equal(t, pc.ShardID("a-ring-008"), m.ShardID(8))
	require.Equal(t, pc.ShardID("a-ring-00c"), m.ShardID(12))
	require.Equal(t, pc.ShardID("a-ring-3fa"), m.ShardID(1018))
}

func TestShuffleConfig(t *testing.T) {
	var m = &ShuffleConfig{
		Journal: "bad journal",
		Ring:    Ring{},
	}

	require.EqualError(t, m.Validate(), "Journal: not a valid token (bad journal)")
	m.Journal = "a/journal"

	require.EqualError(t, m.Validate(), "Ring.Name: invalid length (0; expected 4 <= length <= 508)")
	m.Ring = Ring{
		Name:    "a-ring",
		Members: []Ring_Member{{}, {}},
	}
	m.Coordinator = 3

	require.EqualError(t, m.Validate(), "invalid Coordinator (expected < len(Members); got 3 vs 2)")
	m.Coordinator = 1

	require.EqualError(t, m.Validate(), "Shuffle: expected at least one ShuffleKeyPtr")
	m.Shuffle.ShuffleKeyPtr = []string{"/foo"}

	require.EqualError(t, m.Validate(), "Shuffle: expected one of ChooseFrom or BroadcastTo to be non-zero")
	m.Shuffle.ChooseFrom = 2

	require.Nil(t, m.Validate())

	require.Equal(t, pc.ShardID("a-ring-001"), m.CoordinatorShard())
}

func TestShuffleRequest(t *testing.T) {
	var m = &ShuffleRequest{
		Resolution: badHeaderFixture(),
		Config: ShuffleConfig{
			Journal: "a/journal",
			Ring: Ring{
				Name:    "a-ring",
				Members: nil, // Missing.
			},
			Shuffle: Shuffle{
				ShuffleKeyPtr: []string{"/foo"},
				BroadcastTo:   1,
			},
		},
		Offset:    -1,
		EndOffset: 100,
	}

	require.EqualError(t, m.Validate(), "Resolution.Etcd: invalid ClusterId (expected != 0)")
	m.Resolution.Etcd.ClusterId = 1234

	require.EqualError(t, m.Validate(), "Config.Ring: expected at least one Member")
	m.Config.Ring.Members = []Ring_Member{{}, {}}

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
