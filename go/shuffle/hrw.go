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
	weights []uint32
}

func newRendezvous(cfg pf.ShuffleConfig) rendezvous {
	if err := cfg.Validate(); err != nil {
		panic(err)
	}
	var r = rendezvous{
		cfg:     cfg,
		weights: generateStableWeights(len(cfg.Ring.Members)),
	}
	return r
}

func (m *rendezvous) pick(shuffle int, hash uint32, clock message.Clock, out []pf.Document_Shuffle) []pf.Document_Shuffle {
	var (
		B = m.cfg.Shuffles[shuffle].BroadcastTo
		C = m.cfg.Shuffles[shuffle].ChooseFrom
		// First and last index of |out| to accumulate into (exclusive).
		begin = len(out)
		end   = begin + int(B+C) // Note that B or C must be zero.
	)

	// Rendezvous-hash to accumulate a window of size no larger than |end-begin|,
	// holding the top-ranked mappings of this hash to ring members.
	for i, bounds := range m.cfg.Ring.Members {
		var cur = pf.Document_Shuffle{
			RingIndex:   uint32(i),
			TransformId: m.cfg.Shuffles[shuffle].TransformId,
			Hrw:         hashCombine(hash, m.weights[i]),
		}

		var r = len(out)
		for ; r != begin && out[r-1].Hrw < cur.Hrw; r-- {
		}

		if r == end {
			// Member is too low-rank to be placed within our ranking window.
		} else if bounds.MinMsgClock != 0 && bounds.MinMsgClock > clock {
			// Outside minimum clock bound.
		} else if bounds.MaxMsgClock != 0 && bounds.MaxMsgClock < clock {
			// Outside maximum clock bound.
		} else {
			if len(out) != end {
				out = append(out, cur)
			}
			// Shift, discarding bottom entry.
			copy(out[r+1:], out[r:])
			out[r] = cur
		}
	}

	if B != 0 {
		return out // Broadcast to the entire ranked window.
	}

	// We're choosing 1 member from amoung the window. Select a pseudo-random
	// member (via Clock modulo), pivot it to |begin|, and truncate the remainder
	// of the returned window. Note that there may be fewer members than |C|.

	var swap = int(clock) % (len(out) - begin)
	out[begin] = out[begin+swap]
	return out[:begin+1]
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
