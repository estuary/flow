package runtime

import (
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
)

func TestShuffleRequest(t *testing.T) {
	var m = &ShuffleRequest{
		Journal:   "bad journal",
		Replay:    true,
		BuildId:   "",
		Offset:    -1,
		EndOffset: 100,
		Range: pf.RangeSpec{
			KeyBegin:    42,
			KeyEnd:      32,
			RClockBegin: 1,
			RClockEnd:   0,
		},
		Coordinator:     "bad coordinator",
		Resolution:      badHeaderFixture(),
		ShuffleIndex:    1234,
		Derivation:      nil,
		Materialization: nil,
	}

	require.EqualError(t, m.Validate(), "Journal: not a valid token (bad journal)")
	m.Journal = "a/journal"
	require.EqualError(t, m.Validate(), "invalid Offset (-1; expected 0 <= Offset <= MaxInt64)")
	m.Offset = 200
	require.EqualError(t, m.Validate(), "invalid EndOffset (100; expected 0 or Offset <= EndOffset)")
	m.EndOffset = 300
	require.EqualError(t, m.Validate(), "Range: expected KeyBegin <= KeyEnd (0000002a vs 00000020)")
	m.Range.KeyEnd = 52
	require.EqualError(t, m.Validate(), "Range: expected RClockBegin <= RClockEnd (00000001 vs 00000000)")
	m.Range.RClockEnd = 12345
	require.EqualError(t, m.Validate(), "Coordinator: not a valid token (bad coordinator)")
	m.Coordinator = "a-coordinator"
	require.EqualError(t, m.Validate(), "Resolution.Etcd: invalid ClusterId (expected != 0)")
	m.Resolution.Etcd.ClusterId = 1234
	require.EqualError(t, m.Validate(), "missing BuildId")
	m.BuildId = "an-id"

	// Not covered here.
	require.EqualError(t, m.Validate(), "missing Derivation or Materialization")
}

func TestShuffleResponse(t *testing.T) {
	var m = &ShuffleResponse{
		Status:      1234,
		Header:      nil,
		ReadThrough: -100,
		WriteHead:   50,
		Docs: []pf.Slice{
			{
				Begin: 0,
				End:   10,
			},
			{
				Begin: 10,
				End:   20,
			},
		},
		Offsets:   []pb.Offset{1000},
		UuidParts: []pf.UUIDParts{},
		PackedKey: []pf.Slice{},
	}

	require.EqualError(t, m.Validate(), "Status: invalid status (1234)")
	m.Status = pc.Status_OK
	require.EqualError(t, m.Validate(), "missing Header")
	m.Header = badHeaderFixture()
	require.EqualError(t, m.Validate(), "Header.Etcd: invalid ClusterId (expected != 0)")
	m.Header.Etcd.ClusterId = 1234
	require.EqualError(t, m.Validate(), "invalid ReadThrough (-100; expected 0 <= ReadThrough <= MaxInt64)")
	m.ReadThrough = 100
	require.EqualError(t, m.Validate(), "invalid WriteHead (50; expected WriteHead >= ReadThrough)")
	m.WriteHead = 100

	require.EqualError(t, m.Validate(), "wrong number of Offsets (1; expected 4)")
	m.Offsets = []pb.Offset{1000, 2000, 3000, 4000}
	require.EqualError(t, m.Validate(), "wrong number of UuidParts (0; expected 2)")
	m.UuidParts = []pf.UUIDParts{{}, {}}
	require.EqualError(t, m.Validate(), "wrong number of PackedKey (0; expected 2)")
	m.PackedKey = []pf.Slice{{}, {}}

	require.NoError(t, m.Validate())
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
