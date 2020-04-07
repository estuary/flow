package shuffle

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"

	"go.gazette.dev/core/message"
)

type rendezvous struct {
	cfg     Config
	weights []uint32
	ranks   []rank
}

type rank struct {
	ind int32
	hrw uint32
}

func newRendezvous(cfg Config) rendezvous {
	if err := cfg.Validate(); err != nil {
		panic(err)
	}
	var r = rendezvous{
		cfg:     cfg,
		weights: generateStableWeights(len(cfg.Processors)),
	}
	if cfg.BroadcastTo != 0 {
		r.ranks = make([]rank, 0, cfg.BroadcastTo)
	} else {
		r.ranks = make([]rank, 0, cfg.ChooseFrom)
	}
	return r
}

func (m *rendezvous) pick(hash uint32, clock message.Clock) []rank {
	// Invariant: processor at index zero may never have a min/max clock.
	m.ranks = append(m.ranks[:0], rank{hrw: hashCombine(hash, m.weights[0]), ind: 0})

	for i, bounds := range m.cfg.Processors[1:] {
		var cur = rank{hrw: hashCombine(hash, m.weights[i+1]), ind: int32(i) + 1}

		var r = len(m.ranks)
		for ; r != 0 && m.ranks[r-1].hrw < cur.hrw; r-- {
		}

		if r >= cap(m.ranks) {
			// Index is outside of top N.
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
	if m.cfg.ChooseFrom != 0 {
		var ind = int(clock) % len(m.ranks)
		return m.ranks[ind : ind+1]
	}
	return m.ranks
}

func hashCombine(a, b uint32) uint32 {
	// Drawn from boost::hash_combine(). The constant is the inverse of the golden ratio.
	// See https://stackoverflow.com/questions/5889238/why-is-xor-the-default-way-to-combine-hashes
	return a ^ (b + 0x9e3779b9 + (a << 6) + (a >> 2))
}

func generateStableWeights(n int) []uint32 {
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

	var b = make([]byte, n*4)
	cipher.NewCTR(aesCipher, aesIV[:]).XORKeyStream(b, b)

	var out = make([]uint32, n)
	for i := range out {
		out[i] = binary.LittleEndian.Uint32(b[i*4:])
	}
	return out
}
