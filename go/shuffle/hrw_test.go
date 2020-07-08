package shuffle

import (
	"hash/fnv"
	"math/bits"
	"testing"

	pf "github.com/estuary/flow/go/protocol"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/message"
)

func TestStableWeightsRegression(t *testing.T) {
	var expect = []uint32{
		0xd3093495, 0xeefb0fc7, 0x49595118, 0xb02e4125,
		0x529c82a4, 0xd5ae684a, 0x509754c5, 0x7c1b60d2,
		0xc26ff1f1, 0x32ed8df0, 0xd881c0f7, 0x129f0565,
		0xf3ce95a1, 0xe06c7873, 0x4315653f, 0x2d371fa0,
		0xade0e428, 0x10ab0285, 0x365aff82, 0xc44ee188,
		0x1f0d096f, 0xd84bd42c, 0xcc51a130, 0x66281834,
		0xc1ee27a2, 0x4c1edb77, 0xc3f077d1, 0xb8e1e80d,
		0xc4a7746f, 0x3f3a93c5, 0xd6f138c0, 0x71924578}

	// Generated weights are stable.
	require.Equal(t, expect, generateStableWeights(len(expect)))
	// They're also invariant to length (weights N is a prefix of M for N < M).
	require.Equal(t, expect[:4], generateStableWeights(4))
	// They're also invariant to length (weights N is a prefix of M for N < M).
	require.Equal(t, expect[:12], generateStableWeights(12))

	// Weight bits are uniformly distributed.
	var total int
	for _, n := range expect {
		total += bits.OnesCount32(n)
	}
	require.Equal(t, 490, total) // Ideal is 512 (32 * 16).

}

func TestRankingCases(t *testing.T) {
	var cfg = pf.ShuffleConfig{
		Journal: "a/journal",
		Ring: pf.Ring{
			Name: "a-ring",
			Members: []pf.Ring_Member{
				{MinMsgClock: 0, MaxMsgClock: 0},
				{MinMsgClock: 0, MaxMsgClock: 0},
				{MinMsgClock: 0, MaxMsgClock: 0},
				{MinMsgClock: 1000, MaxMsgClock: 0},
				{MinMsgClock: 0, MaxMsgClock: 3000},
			},
		},
		Shuffles: []pf.ShuffleConfig_Shuffle{
			{ShuffleKeyPtr: []string{"/foo"}, BroadcastTo: 3},
			{ShuffleKeyPtr: []string{"/bar"}, ChooseFrom: 3},
		},
	}
	var r = newRendezvous(cfg)

	// FNVa with single-letter inputs, as below, produces a pretty low quality hash
	// (many identical bit values in outputs). We expect to still do a decent job of
	// mixing across processors.
	var hash = func(s string) uint32 {
		var h = fnv.New32a()
		_, _ = h.Write([]byte(s))
		return h.Sum32()
	}

	var cases = []struct {
		hash   uint32
		clock  message.Clock
		expect []rank
	}{
		// Regression tests, demonstrating mixing.
		{
			hash:   hash("a"),
			clock:  2000,
			expect: []rank{{ind: 4, hrw: 0xc8ed7884}, {ind: 2, hrw: 0xc7920930}, {ind: 3, hrw: 0x6e7f3905}},
		},
		{
			hash:   hash("b"),
			clock:  2000,
			expect: []rank{{ind: 3, hrw: 0xac381272}, {ind: 0, hrw: 0x89031ee2}, {ind: 1, hrw: 0x6d0d23dc}},
		},
		{
			hash:   hash("c"),
			clock:  2000,
			expect: []rank{{ind: 3, hrw: 0xecfff620}, {ind: 0, hrw: 0xcbc2e1b0}, {ind: 1, hrw: 0xafcc8546}},
		},
		{
			hash:   hash("d"),
			clock:  2000,
			expect: []rank{{ind: 1, hrw: 0xe9728b2f}, {ind: 4, hrw: 0x8d2c064a}, {ind: 2, hrw: 0x83d0d4de}},
		},
		{
			hash:   hash("e"),
			clock:  2000,
			expect: []rank{{ind: 4, hrw: 0xcbd39ff5}, {ind: 2, hrw: 0xc290a969}, {ind: 3, hrw: 0x697d5976}},
		},

		// Index 3 is rank-one for this value, but the clock falls outside its minimum bound.
		{
			hash:   hash("b"),
			clock:  500,
			expect: []rank{{ind: 0, hrw: 0x89031ee2}, {ind: 1, hrw: 0x6d0d23dc}, {ind: 4, hrw: 0xaaeacf3}},
		},
		// Index 4 is rank-two, but the clock falls outside its maximum bound.
		{
			hash:   hash("d"),
			clock:  3500,
			expect: []rank{{ind: 1, hrw: 0xe9728b2f}, {ind: 2, hrw: 0x83d0d4de}, {ind: 3, hrw: 0x28bdc4c9}},
		},
	}

	for _, tc := range cases {
		t.Logf("hash %x clock %v", tc.hash, tc.clock)
		require.Equal(t, tc.expect, r.pick(0, tc.hash, tc.clock))
	}

	// For these cases, use the second "choose" shuffle.
	cases = []struct {
		hash   uint32
		clock  message.Clock
		expect []rank
	}{
		// One of the top-N processers is selected, depending on the clock.
		{
			hash:   hash("a"),
			clock:  2000,
			expect: []rank{{ind: 3, hrw: 0x6e7f3905}},
		},
		{
			hash:   hash("a"),
			clock:  2001,
			expect: []rank{{ind: 4, hrw: 0xc8ed7884}},
		},
		{
			hash:   hash("a"),
			clock:  2002,
			expect: []rank{{ind: 2, hrw: 0xc7920930}},
		},
		{
			hash:   hash("a"),
			clock:  2003,
			expect: []rank{{ind: 3, hrw: 0x6e7f3905}},
		},
	}

	for _, tc := range cases {
		t.Logf("hash %x clock %v", tc.hash, tc.clock)
		require.Equal(t, tc.expect, r.pick(1, tc.hash, tc.clock))
	}
}
