// Package keyhash computes Flow's canonical packed-key hash.
//
// Neither the algorithm nor its key may ever change. Rust reimplements the same
// hash as doc::Extractor::packed_hash, and golden vectors pin both.
package keyhash

import (
	"encoding/hex"

	"github.com/minio/highwayhash"
)

// PackedKeyHash_HH64 builds a packed key hash from the top 32-bits of a
// HighwayHash 64-bit checksum computed using a fixed key.
func PackedKeyHash_HH64(packedKey []byte) uint32 {
	return uint32(highwayhash.Sum64(packedKey, highwayHashKey) >> 32)
}

// highwayHashKey is a fixed 32 bytes (as required by HighwayHash) read from /dev/random.
var highwayHashKey, _ = hex.DecodeString("ba737e89155238d47d8067c35aad4d25ecdd1c3488227e011ffa480c022bd3ba")
