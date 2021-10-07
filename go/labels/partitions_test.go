package labels

import (
	"testing"

	"github.com/estuary/protocols/fdb/tuple"
	pf "github.com/estuary/protocols/flow"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
)

func TestPartitionEncoding(t *testing.T) {
	var b []byte
	b = append(EncodePartitionValue(b, nil), '|')
	b = append(EncodePartitionValue(b, true), '|')
	b = append(EncodePartitionValue(b, false), '|')
	b = append(EncodePartitionValue(b, int(123)), '|')
	b = append(EncodePartitionValue(b, int(-321)), '|')
	b = append(EncodePartitionValue(b, int64(-867)), '|')
	b = append(EncodePartitionValue(b, uint64(5309)), '|')
	b = append(EncodePartitionValue(b, "NJ"), '|')
	b = append(EncodePartitionValue(b, "foo bar"), '|')
	b = append(EncodePartitionValue(b, "Baz!@\"Bing\""), '|')
	b = append(EncodePartitionValue(b, "http://example/path?q1=v1&q2=v2;ex%20tra"), '|')

	require.Equal(t,
		"null|true|false|123|-321|-867|5309|NJ|foo+bar|Baz%21%40%22Bing%22|http%3A%2F%2Fexample%2Fpath%3Fq1%3Dv1%26q2%3Dv2%3Bex%2520tra|",
		string(b))
}

func TestPartitionLabelGeneration(t *testing.T) {
	require.Equal(t,
		pb.MustLabelSet(
			"pass", "through",
			FieldPrefix+"Loo", "Ba%2Bz%21%40_%22Bi.n%2Fg%22+http%3A%2F%2Fexample-host%2Fpath%3Fq1%3Dv1%26q2%3Dv%2B2%3Bex%25%25tra",
			FieldPrefix+"bar", "-123",
			FieldPrefix+"foo", "true",
		),
		EncodePartitionLabels(
			[]string{"Loo", "bar", "foo"},
			tuple.Tuple{"Ba+z!@_\"Bi.n/g\" http://example-host/path?q1=v1&q2=v+2;ex%%tra", int64(-123), true},
			pb.MustLabelSet("pass", "through"),
		),
	)
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
	require.Equal(t, "bar=hi+there/foo=32/pivot=00", suffix)

	// Case: KeyBegin is non-zero
	set = EncodeHexU32Label(KeyBegin, 6152432, set)
	set = EncodeHexU32Label(KeyEnd, 7891011, set)

	suffix, err = PartitionSuffix(set)
	require.NoError(t, err)
	require.Equal(t, "bar=hi+there/foo=32/pivot=005de0f0", suffix)

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
