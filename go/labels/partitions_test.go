package labels

import (
	"math"
	"testing"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
)

func TestPartitionEncodeDecode(t *testing.T) {
	var cases = []struct {
		value  any
		expect string
	}{
		{nil, "%_null"},
		{true, "%_true"},
		{false, "%_false"},
		{uint64(123), "%_123"},
		{int64(-123), "%_-123"},
		{uint64(math.MaxUint64), "%_18446744073709551615"},
		{int64(-math.MaxInt64), "%_-9223372036854775807"},
		// Strings that *look* like other scalar types.
		{"null", "null"},
		{"%_null", "%25_null"},
		{"true", "true"},
		{"false", "false"},
		{"123", "123"},
		{"-123", "-123"},
		{"hello, world!", "hello%2C%20world%21"},
		{"Baz!@\"Bing\"", "Baz%21%40%22Bing%22"},
		{"no.no&no-no@no$yes_yes();", "no.no%26no-no%40no%24yes_yes%28%29%3B"},
		{"http://example/path?q1=v1&q2=v2;ex%20tra", "http%3A%2F%2Fexample%2Fpath%3Fq1%3Dv1%26q2%3Dv2%3Bex%2520tra"},
	}

	for _, tc := range cases {
		var b = EncodePartitionValue([]byte("xyz"), tc.value)
		require.Equal(t, tc.expect, string(b[3:]))

		var out, err = DecodePartitionValue(string(b[3:]))
		require.NoError(t, err)

		require.Equal(t, tc.value, out)
	}
}

func TestPartitionLabelGeneration(t *testing.T) {
	var tuple = tuple.Tuple{"Ba+z!@_\"Bi.n/g\" http://example-host/path?q1=v1&q2=v+2;ex%%tra", int64(-123), true}
	var fields = []string{"Loo", "bar", "foo"}

	var encoding = EncodePartitionLabels(
		fields,
		tuple,
		pb.MustLabelSet("pass", "through"),
	)

	require.Equal(t,
		pb.MustLabelSet(
			"pass", "through",
			FieldPrefix+"Loo", "Ba%2Bz%21%40_%22Bi.n%2Fg%22%20http%3A%2F%2Fexample-host%2Fpath%3Fq1%3Dv1%26q2%3Dv%2B2%3Bex%25%25tra",
			FieldPrefix+"bar", "%_-123",
			FieldPrefix+"foo", "%_true",
		),
		encoding,
	)

	var decoding, err = DecodePartitionLabels(fields, encoding)
	require.NoError(t, err)

	require.Equal(t, tuple, decoding)
}

func TestJournalSuffixGeneration(t *testing.T) {
	var set = EncodePartitionLabels(
		[]string{"bar", "foo"},
		tuple.Tuple{"hi there", 32},
		pb.MustLabelSet(
			KeyBegin, KeyBeginMin,
			KeyEnd, KeyEndMax,
		),
	)

	// Case: KeyBegin is zero.
	var suffix, err = PartitionSuffix(set)
	require.NoError(t, err)
	require.Equal(t, "bar=hi%20there/foo=%_32/pivot=00", suffix)

	// Case: KeyBegin is non-zero
	set = EncodeHexU32Label(KeyBegin, 6152432, set)
	set = EncodeHexU32Label(KeyEnd, 7891011, set)

	suffix, err = PartitionSuffix(set)
	require.NoError(t, err)
	require.Equal(t, "bar=hi%20there/foo=%_32/pivot=005de0f0", suffix)

	// Case: No partitions.
	set = EncodeHexU32Label(KeyBegin, 6152432, pb.LabelSet{})
	set = EncodeHexU32Label(KeyEnd, 7891011, set)

	suffix, err = PartitionSuffix(set)
	require.NoError(t, err)
	require.Equal(t, "pivot=005de0f0", suffix)

	// Case: missing required label.
	set.Remove(KeyBegin)
	_, err = PartitionSuffix(set)
	require.EqualError(t, err, "expected one estuary.dev/key-begin: []")
}

func TestShardSuffixGeneration(t *testing.T) {
	var labels = EncodeRange(pf.RangeSpec{
		KeyBegin:    0,
		KeyEnd:      123456,
		RClockBegin: 0,
		RClockEnd:   78910,
	}, pf.LabelSet{})

	var suffix, err = ShardSuffix(labels)
	require.NoError(t, err)
	require.Equal(t, "00000000-00000000", suffix)

	labels = EncodeRange(pf.RangeSpec{
		KeyBegin:    0x123,
		KeyEnd:      0x456,
		RClockBegin: 0x789,
		RClockEnd:   0x1011,
	}, labels)

	suffix, err = ShardSuffix(labels)
	require.NoError(t, err)
	require.Equal(t, "00000123-00000789", suffix)

	// Error case:
	labels.Remove(RClockBegin)
	_, err = ShardSuffix(labels)
	require.EqualError(t, err, "expected one estuary.dev/rclock-begin: []")

	labels.Remove(KeyBegin)
	_, err = ShardSuffix(labels)
	require.EqualError(t, err, "expected one estuary.dev/key-begin: []")
}
