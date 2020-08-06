package shuffle

import (
	"crypto/sha256"
	"encoding/binary"
	"math/bits"
	"testing"

	pf "github.com/estuary/flow/go/protocol"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/message"
)

func TestStableWeightsRegression(t *testing.T) {
	var expect = []uint64{
		0xeefb0fc7d3093495, 0xb02e412549595118, 0xd5ae684a529c82a4, 0x7c1b60d2509754c5,
		0x32ed8df0c26ff1f1, 0x129f0565d881c0f7, 0xe06c7873f3ce95a1, 0x2d371fa04315653f,
		0x10ab0285ade0e428, 0xc44ee188365aff82, 0xd84bd42c1f0d096f, 0x66281834cc51a130,
		0x4c1edb77c1ee27a2, 0xb8e1e80dc3f077d1, 0x3f3a93c5c4a7746f, 0x71924578d6f138c0,
		0x68422be1c5d0b5d1, 0x228a5f52192f3f37, 0xde5e93def103c9b9, 0x0316810eebcc3715,
		0xe9bab156b3707651, 0x18fd96e496ee2e1d, 0xdc12b3615b690121, 0x05a69742caa2b5b0,
		0xdd310fe134095423, 0x90b60b55796fb05c, 0x8c3a4363a674f82d, 0x81bd243447bb354e,
		0x8a6c3fbc9caf37ec, 0x21a5e2acef624771, 0xf7db158f6199a386, 0x292ad7bea31319b2,
	}

	// Generated weights are stable.
	require.Equal(t, expect, generateStableWeights(len(expect)))
	// They're also invariant to length (weights N is a prefix of M for N < M).
	require.Equal(t, expect[:4], generateStableWeights(4))
	// They're also invariant to length (weights N is a prefix of M for N < M).
	require.Equal(t, expect[:12], generateStableWeights(12))

	// Weight bits are uniformly distributed.
	var total int
	for _, n := range expect {
		total += bits.OnesCount64(n)
	}
	require.Equal(t, 1000, total) // Ideal is 1024 (64 * 16).
}

func TestRankingCases(t *testing.T) {
	var cfg = newTestShuffleConfig()
	var r = newRendezvous(cfg)

	var hash = func(s string) uint64 {
		var b = sha256.Sum256([]byte(s))
		return binary.LittleEndian.Uint64(b[:8])
	}

	var cases = []struct {
		hash   uint64
		clock  message.Clock
		expect []rank
	}{
		// Regression tests, demonstrating mixing.
		{
			hash:  hash("a"),
			clock: 2000,
			expect: []rank{
				{index: 4, hrw: 0xf850963ad0ee663b},
				{index: 3, hrw: 0xb6a67b184216c30f},
				{index: 1, hrw: 0x7a935aef5bd8c6d2},
			},
		},
		{
			hash:  hash("b"),
			clock: 2000,
			expect: []rank{
				{index: 1, hrw: 0xfa7778255fb17226},
				{index: 0, hrw: 0xa4a236c7c5e117ab},
				{index: 2, hrw: 0x9ff7514a4474a19a},
			},
		},
		{
			hash:  hash("c"),
			clock: 2000,
			expect: []rank{
				{index: 4, hrw: 0xd097dd59c1438cdf},
				{index: 3, hrw: 0x9e61307b53bb29eb},
				{index: 1, hrw: 0x5254118c4a752c36},
			},
		},
		{
			hash:  hash("d"),
			clock: 2000,
			expect: []rank{
				{index: 3, hrw: 0xf50d909123a9f8dd},
				{index: 4, hrw: 0xbbfb7db3b1515de9},
				{index: 0, hrw: 0x67edff84a037988d},
			},
		},
		{
			hash:  hash("e"),
			clock: 2000,
			expect: []rank{
				{index: 2, hrw: 0xe7ab33092927fb9b},
				{index: 0, hrw: 0xdcfe5484a8b24daa},
				{index: 1, hrw: 0x822b1a6632e22827},
			},
		},

		// Index 3 is rank-one for this value, but the clock falls outside its minimum bound.
		{
			hash:  hash("c"),
			clock: 500,
			expect: []rank{
				{index: 4, hrw: 0xd097dd59c1438cdf},
				// {index: 3, hrw: 0x9e61307b53bb29eb},
				{index: 1, hrw: 0x5254118c4a752c36},
				{index: 2, hrw: 0x37d438e351b0ff8a},
			},
		},
		// Index 4 is rank-two, but the clock falls outside its maximum bound.
		{
			hash:  hash("d"),
			clock: 3500,
			expect: []rank{
				{index: 3, hrw: 0xf50d909123a9f8dd},
				// {index: 4, hrw: 0xbbfb7db3b1515de9},
				{index: 0, hrw: 0x67edff84a037988d},
				{index: 2, hrw: 0x5cb8980921a22ebc},
			},
		},
		// Clock falls outside *any* member bound.
		{
			hash:   hash("d"),
			clock:  100,
			expect: []rank{},
		},
	}

	for _, tc := range cases {
		t.Logf("hash %x clock %v", tc.hash, tc.clock)
		require.Equal(t, tc.expect, r.pick(tc.hash, tc.clock))
	}

	// For these cases, switch from "broadcast" to "choose" mode.
	r.cfg.Shuffle.ChooseFrom, r.cfg.Shuffle.BroadcastTo =
		r.cfg.Shuffle.BroadcastTo, r.cfg.Shuffle.ChooseFrom

	cases = []struct {
		hash   uint64
		clock  message.Clock
		expect []rank
	}{
		// One of the top-N processers is selected, depending on the clock.
		{
			hash:   hash("a"),
			clock:  2000,
			expect: []rank{{index: 1, hrw: 0x7a935aef5bd8c6d2}},
		},
		{
			hash:   hash("a"),
			clock:  2001,
			expect: []rank{{index: 4, hrw: 0xf850963ad0ee663b}},
		},
		{
			hash:   hash("a"),
			clock:  2002,
			expect: []rank{{index: 3, hrw: 0xb6a67b184216c30f}},
		},
		{
			hash:   hash("a"),
			clock:  2003,
			expect: []rank{{index: 1, hrw: 0x7a935aef5bd8c6d2}},
		},
		{
			hash:   hash("d"),
			clock:  100, // Outside *any* member bound.
			expect: []rank{},
		},
	}

	for _, tc := range cases {
		t.Logf("hash %x clock %v", tc.hash, tc.clock)
		require.Equal(t, tc.expect, r.pick(tc.hash, tc.clock))
	}
}

func newTestShuffleConfig() pf.ShuffleConfig {
	return pf.ShuffleConfig{
		Journal: "a/journal",
		Ring: pf.Ring{
			Name: "a-ring",
			Members: []pf.Ring_Member{
				{MinMsgClock: 500, MaxMsgClock: 0},
				{MinMsgClock: 500, MaxMsgClock: 0},
				{MinMsgClock: 500, MaxMsgClock: 0},
				{MinMsgClock: 1000, MaxMsgClock: 0},
				{MinMsgClock: 500, MaxMsgClock: 3000},
			},
		},
		Shuffle: pf.Shuffle{
			Transform:     "a-transform",
			ShuffleKeyPtr: []string{"/foo", "/bar"},
			BroadcastTo:   3,
		},
	}
}
