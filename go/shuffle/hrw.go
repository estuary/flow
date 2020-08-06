package shuffle

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"

	pf "github.com/estuary/flow/go/protocol"
	"go.gazette.dev/core/message"
)

type rendezvous struct {
	cfg pf.ShuffleConfig
	// Weights of each ring member, of length len(cfg.Ring.Members).
	weights []uint64
	// Ranks of a pick() operation, which is reset with each call to pick().
	// Size to have capacity of BroadcastTo / ChooseFrom.
	ranks []rank
}

type rank struct {
	// Index within the worker ring to which the document is shuffled.
	index int
	// Highest-random weight from rendezvous hashing the document into the ring.
	hrw uint64
}

func newRendezvous(cfg pf.ShuffleConfig) rendezvous {
	if err := cfg.Validate(); err != nil {
		panic(err)
	}
	var r = rendezvous{
		cfg:     cfg,
		weights: generateStableWeights(len(cfg.Ring.Members)),
		ranks:   make([]rank, cfg.Shuffle.BroadcastTo+cfg.Shuffle.ChooseFrom),
	}
	return r
}

func (m *rendezvous) pick(hash uint64, clock message.Clock) []rank {
	m.ranks = m.ranks[:0]

	// Rendezvous-hash to accumulate a window of size no larger than |end-begin|,
	// holding the top-ranked mappings of this hash to ring members.
	for i, bounds := range m.cfg.Ring.Members {
		var cur = rank{
			index: i,
			hrw:   hash ^ m.weights[i],
		}

		var r = len(m.ranks)
		for ; r != 0 && m.ranks[r-1].hrw < cur.hrw; r-- {
		}

		if r == cap(m.ranks) {
			// Member is too low-rank to be placed within our window.
		} else if bounds.MinMsgClock != 0 && bounds.MinMsgClock > clock {
			// Outside minimum clock bound.
		} else if bounds.MaxMsgClock != 0 && bounds.MaxMsgClock < clock {
			// Outside maximum clock bound.
		} else {
			if len(m.ranks) != cap(m.ranks) {
				m.ranks = append(m.ranks, cur)
			}
			// Shift, discarding bottom entry.
			copy(m.ranks[r+1:], m.ranks[r:])
			m.ranks[r] = cur
		}
	}

	if m.cfg.Shuffle.ChooseFrom != 0 && len(m.ranks) != 0 {
		// We're choosing 1 member from among the window. Use |clock|, which is
		// unrelated to |hash|, to derive a pseudo-random, deterministic selection.
		var ind = int(clock) % len(m.ranks)
		return m.ranks[ind : ind+1]
	}

	return m.ranks
}

func generateStableWeights(n int) []uint64 {
	// Use a fixed AES key and IV to generate a stable sequence.
	var aesKey = [32]byte{
		0xb8, 0x3d, 0xb8, 0x33, 0x2f, 0x6c, 0x4c, 0xef,
		0x85, 0x45, 0xa1, 0xe3, 0xcd, 0x22, 0x9f, 0xec,
		0x3e, 0x72, 0x8f, 0xb4, 0x37, 0x04, 0xaa, 0x8b,
		0xc2, 0xf4, 0xcc, 0x3e, 0x03, 0xcc, 0x03, 0x6d,
	}
	var aesIV = [aes.BlockSize]byte{
		0x1c, 0x72, 0xf8, 0x28, 0x51, 0xe5, 0xa5, 0x0f,
		0x57, 0x75, 0x5f, 0x36, 0x5f, 0x1b, 0x84, 0xca,
	}

	var aesCipher, err = aes.NewCipher(aesKey[:])
	if err != nil {
		panic(err) // Should never error (given correct |key| size).
	}

	var b = make([]byte, n*8)
	cipher.NewCTR(aesCipher, aesIV[:]).XORKeyStream(b, b)

	var out = make([]uint64, n)
	for i := range out {
		out[i] = binary.LittleEndian.Uint64(b[i*8:])
	}
	return out
}
