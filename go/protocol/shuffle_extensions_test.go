package protocol

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

func TestValueConversions(t *testing.T) {
	var arena Arena

	var cases = []struct {
		value            Field_Value
		asInterface      interface{}
		asPartition      string
		fromInterfaceErr string
	}{
		{Field_Value{Kind: Field_Value_NULL}, nil, "null", ""},
		{Field_Value{Kind: Field_Value_TRUE}, true, "true", ""},
		{Field_Value{Kind: Field_Value_FALSE}, false, "false", ""},
		{Field_Value{Kind: Field_Value_UNSIGNED, Unsigned: 32}, uint64(32), "32", ""},
		{Field_Value{Kind: Field_Value_SIGNED, Signed: -42}, int64(-42), "-42", ""},
		{Field_Value{Kind: Field_Value_DOUBLE, Double: -4.2}, float64(-4.2), "-4.2", ""},
		{Field_Value{Kind: Field_Value_STRING, Bytes: arena.Add([]byte("hel lo"))}, "hel lo", "hel%20lo", ""},
		{Field_Value{Kind: Field_Value_ARRAY, Bytes: arena.Add([]byte("[true]"))}, []byte("[true]"), "%5Btrue%5D",
			"couldn't convert from interface []byte{0x5b, 0x74, 0x72, 0x75, 0x65, 0x5d}"},
		{Field_Value{Kind: Field_Value_OBJECT, Bytes: arena.Add([]byte("{\"t\":1}"))}, []byte("{\"t\":1}"), "%7B%22t%22:1%7D",
			"couldn't convert from interface []byte{0x7b, 0x22, 0x74, 0x22, 0x3a, 0x31, 0x7d}"},
	}
	for _, tc := range cases {
		require.Equal(t, tc.asInterface, tc.value.ToInterface(arena))
		require.Equal(t, tc.asPartition, string(tc.value.EncodePartition(nil, arena)))

		// Round-trip from interface to value and back. Expect that works,
		// unless the test notes that conversions will error.
		var vv, err = ValueFromInterface(&arena, tc.asInterface)
		if tc.fromInterfaceErr != "" {
			require.EqualError(t, err, tc.fromInterfaceErr)
		} else {
			require.NoError(t, err)
			require.Equal(t, tc.asInterface, vv.ToInterface(arena))
		}
	}
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
