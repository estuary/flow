# keyhash

Flow's canonical packed-key hash: the top 32 bits of a HighwayHash-64 over a
packed key tuple, using a fixed public key.

## Purpose

The runtime routes a document by hashing its packed key and comparing that hash
against the `KeyBegin` / `KeyEnd` range labels of a shard or physical partition.
Every implementation of that routing must produce bit-identical hashes.

This package holds the hash on its own so that other projects — notably
connectors which reproduce shard routing in a keyed sink — can import it without
`go/flow`, which pulls in the gazette consumer stack.

## Entry points

- `PackedKeyHash_HH64(packedKey []byte) uint32` — the hash. Feed it a packed
  tuple, such as `tuple.Tuple{...}.Pack()`.

## Non-obvious details

- The Rust implementation is `doc::Extractor::packed_hash` in
  `crates/doc/src/extractor.rs`.
- `keyhash_test.go` and Rust's `test_packed_hash_regression` pin the same 14
  golden vectors. Both must be updated together, and neither should ever change:
  a new hash would re-route every existing collection and task.
