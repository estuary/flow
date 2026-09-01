package keyhash

import (
	"testing"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	"github.com/stretchr/testify/require"
)

// These vectors are also pinned by Rust's test_packed_hash_regression.
// The two implementations must never diverge.
func TestHighwayHashRegression(t *testing.T) {
	var cases = []struct {
		expect uint32
		given  tuple.Tuple
	}{
		// Expect that small (e.x. single bit) changes to the input wildly change the output.
		{0xb9f08d38, tuple.Tuple{true}},
		{0x1505e3cb, tuple.Tuple{false}},
		{0x6ae719f3, tuple.Tuple{"foo", "bar"}},
		{0x8adddd61, tuple.Tuple{"foobar"}},
		{0x7273e587, tuple.Tuple{"foobas"}},
		{0xf4ec4d33, tuple.Tuple{"1"}},
		{0x1e023d95, tuple.Tuple{"2"}},
		{0x38a34efe, tuple.Tuple{"3"}},
		{0x17751bae, tuple.Tuple{"10"}},
		{0x87d93806, tuple.Tuple{"11"}},
		{0x3c90c1d9, tuple.Tuple{1}},
		{0x97901bac, tuple.Tuple{2}},
		{0xcbc7f1e2, tuple.Tuple{3}},
		{0xd1d3f3eb, tuple.Tuple{10}},
	}
	for _, tc := range cases {
		require.Equal(t, tc.expect, PackedKeyHash_HH64(tc.given.Pack()))
	}
}
