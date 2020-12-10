package labels

import (
	"strings"
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
)

func TestReaderConfigParsingCases(t *testing.T) {
	// Case: success.
	require.Equal(t, pf.RangeSpec{
		KeyBegin:    []byte{0xcc, 0xdd, 0xee, 0xff},
		KeyEnd:      []byte{0xff, 0x00, 0xaa, 0xbb, 0xcc, 0xdd},
		RClockBegin: 1 << 62,
		RClockEnd:   1 << 63,
	}, MustParseRangeSpec(pb.MustLabelSet(
		KeyBegin, "ccddeeff",
		KeyEnd, "ff00aabbccdd",
		RClockBegin, "4000000000000000",
		RClockEnd, "8000000000000000")))

	// Case: key is malformed hex.
	var _, err = ParseRangeSpec(pb.MustLabelSet(
		KeyBegin, "zz",
		KeyEnd, "ff",
		RClockBegin, "0000000000000000",
		RClockEnd, "8000000000000000"))
	require.True(t, strings.HasPrefix(err.Error(), "decoding hex label "+KeyBegin+", value zz: encoding/hex: invalid byte"))
	_, err = ParseRangeSpec(pb.MustLabelSet(
		KeyBegin, "aa",
		KeyEnd, "zz",
		RClockBegin, "0000000000000000",
		RClockEnd, "8000000000000000"))
	require.True(t, strings.HasPrefix(err.Error(), "decoding hex label "+KeyEnd+", value zz: encoding/hex: invalid byte"))

	// Case: clock is malformed hex.
	_, err = ParseRangeSpec(pb.MustLabelSet(
		KeyBegin, "aa",
		KeyEnd, "ff",
		RClockBegin, "zz",
		RClockEnd, "8000000000000000"))
	require.True(t, strings.HasPrefix(err.Error(), "decoding hex label "+RClockBegin+", value zz: encoding/hex: invalid byte"))
	_, err = ParseRangeSpec(pb.MustLabelSet(
		KeyBegin, "aa",
		KeyEnd, "ff",
		RClockBegin, "0000000000000000",
		RClockEnd, "zz"))
	require.True(t, strings.HasPrefix(err.Error(), "decoding hex label "+RClockEnd+", value zz: encoding/hex: invalid byte"))

	// Case: clock is wrong length.
	_, err = ParseRangeSpec(pb.MustLabelSet(
		KeyBegin, "aa",
		KeyEnd, "ff",
		RClockBegin, "01",
		RClockEnd, "8000000000000000"))
	require.EqualError(t, err, "label "+RClockBegin+" value 01 has unexpected length (1; expected 8)")
	_, err = ParseRangeSpec(pb.MustLabelSet(
		KeyBegin, "aa",
		KeyEnd, "ff",
		RClockBegin, "0000000000000000",
		RClockEnd, "02"))
	require.EqualError(t, err, "label "+RClockEnd+" value 02 has unexpected length (1; expected 8)")

	// Case: missing labels.
	_, err = ParseRangeSpec(pb.LabelSet{})
	require.EqualError(t, err, "missing required label: "+KeyBegin)

	// Case: parses okay, but invalid range.
	_, err = ParseRangeSpec(pb.MustLabelSet(
		KeyBegin, "cc",
		KeyEnd, "ff",
		RClockBegin, "0000000000000001",
		RClockEnd, "0000000000000000"))
	require.EqualError(t, err, "expected RClockBegin < RClockEnd (1 vs 0)")
}
