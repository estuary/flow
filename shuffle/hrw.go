package shuffle

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"hash/fnv"

	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/message"
)

type shuffleShard struct {
	spec     *pc.ShardSpec
	minClock message.Clock
}

func numEffectiveShards(clock message.Clock, shards []shuffleShard) int {
	// Determine max shard index to which this message may map.
	// This relies on shards being ordered on ascending |minClock|.
	var N = len(shards)
	for ; N != 0 && shards[N-1].minClock <= clock; N-- {
	}
	return N
}

func messageIndex(msg message.Mappable, keyFn message.MappingKeyFunc, N int) int {
	var hasher = fnv.New32a()
	keyFn(msg, hasher)
	var keyHash = hasher.Sum32()

	var hrw uint32
	var ind int

	if N > len(weights) {
		N = len(weights)
	}
	for i := 0; i != N; i++ {
		if w := keyHash ^ weights[i]; w > hrw {
			hrw, ind = w, i
		}
	}
	return ind
}

func generateStableWeights() [maxEffectivePartitions]uint32 {
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

	var out [maxEffectivePartitions]uint32
	var b = make([]byte, len(out)*8)
	cipher.NewCTR(aesCipher, aesIV[:]).XORKeyStream(b, b)

	for i := range out {
		out[i] = binary.LittleEndian.Uint32(b[i*8:])
	}
	return out
}

const maxEffectivePartitions = 4096

var weights = generateStableWeights()
