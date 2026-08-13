# disk-daemon

A general-purpose service which gives a client a mounted filesystem whose
state advances atomically with the client's own commit, and whose durable copy
lives in a Gazette journal. The daemon exposes a sparse local image through
Linux `ublk`, appends every accepted device mutation to a per-disk journal as
the device accepts it, and rebuilds the image from that journal after a
failure. The local image is disposable and the journal is the disk. Nothing
here knows about Flow, so the Flow runtime is a client like any other.

Design: [`plans/block-backed-connector-disks.md`](../../plans/block-backed-connector-disks.md).
Build order: [`plans/block-backed-connector-disks-phases.md`](../../plans/block-backed-connector-disks-phases.md).

## Scope today

The durable format only: the wire protocol, the chunk codec, and the block
bitmaps. There is no journal I/O, no `ublk` device, no session service, and no
binary yet, so nothing in the crate performs a network operation. Gazette's
fixed record framing is not here either; it belongs in `crates/gazette`
alongside the other framings, and arrives with the journal writer.

## Key types and entry points

| Item | Role |
| --- | --- |
| [`proto`] (`proto_flow::disk`) | Session RPC (`Open`/`Publish`/`Commit`/`Broker`) and journal records (`DiskRecord`, `Chunk`), from `go/protocols/disk/disk.proto` |
| [`chunk::encode_write`] / [`chunk::encode_punch`] | Device mutation → journal chunks |
| [`chunk::apply`] | Journal chunk → image bytes and allocated bits (replay) |
| [`chunk::covered_blocks`] | Block range a chunk covers, which is the shared rule both halves obey |
| [`bitmap::Bitmap`] | Allocated and horizon block sets |

## Non-obvious details

- **Empty `data` is one allocated block of zeroes, not a hole.** Only a
  `punch` deallocates. A device write of zeroes still allocated blocks, so the
  encoding must reproduce that or a rebuilt image would be sparser than the one
  the client saw.

- **Trailing-zero trimming makes zero-fill mandatory on replay.**
  `encode_write` drops trailing zero *bytes*, so a data chunk can end within
  its last block, and `apply` must zero through the end of the covered range.
  Both halves take that range from `covered_blocks` rather than repeating the
  arithmetic. Trimming is only a wire-size optimization: encode-then-replay is
  byte- and allocation-identical to the raw mutation, which is what the
  property test in `chunk/test.rs` asserts against two real files.

- **Bitmaps are not atomic.** A disk is owned by exactly one thread, which is
  also what makes journal order equal image-application order.

- **Block size is per-disk and durable.** It shapes chunk coverage and bitmap
  extent, so it is fixed once a disk first publishes. Device size and block
  size are `Open` parameters rather than crate constants, and `u32` block
  indexing caps a device at 2³² blocks.
