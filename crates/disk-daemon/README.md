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

The durable format, the local device, and the journal writer: the wire
protocol, the chunk codec, the block bitmaps, the sparse image, the `ublk`
device server, the owner pool which serves it, and the writer which fences a
journal, appends every captured mutation, and constructs the acknowledgement
which commits a delta.

There is no session service and no daemon binary, so a caller drives a disk and
its writer directly. Recovery is not built: nothing reads a journal back, so a
session which opens with committed state is told so rather than deriving it.
Horizons do not exist, so no record opens one and no floor label is written.

## Key types and entry points

| Item | Role |
| --- | --- |
| [`proto`] (`proto_flow::disk`) | Session RPC (`Open`/`Publish`/`Commit`/`Broker`) and journal records (`DiskRecord`, `Chunk`), from `go/protocols/disk/disk.proto` |
| [`disk::Disk`] | One disk's lifecycle: image, device, and owner assignment |
| [`image::Image`] | The `O_TMPFILE` sparse image and its allocated bitmap |
| [`ublk::Control`] | `/dev/ublk-control`: add, configure, start, stop, delete |
| [`owner::Pool`] | Owner threads; each disk is owned by exactly one |
| [`capture::channel`] | The mutation-capture seam between an owner and the writer |
| [`journal::Writer`] | One session's journal: fence, append pipeline, publish and commit |
| [`journal::Config`] | Daemon-wide append bounds, spec fallbacks, and shared client |
| [`journal::fence`] | The `author` register: probe, claim, and the fence record |
| [`journal::spec`] | `JournalConfig` over daemon defaults, and insert-only creation |
| [`journal::record`] | Chunks into records, records into appends, and their bounds |
| [`inflight::InFlight`] | Serializes overlapping mutations against one image |
| [`chunk::encode_write`] / [`chunk::encode_punch`] | Device mutation → journal chunks |
| [`chunk::apply`] | Journal chunk → image bytes and allocated bits (replay) |
| [`chunk::covered_blocks`] | Block range a chunk covers, which is the shared rule both halves obey |
| [`bitmap::Bitmap`] | Allocated and horizon block sets |

## The serving path

An owner drives one `io_uring` for all of its disks. A device request arrives as
a fetch completion, and each of its steps is another completion on that ring, so
the owner never blocks:

```text
fetch  ──►  read  ──►  write ──►  commit        (device read)
            image      /dev/ublkcN
                       and fetch the next request

fetch  ──►  read       ──►  offer chunks ──►  write  ──►  commit
            /dev/ublkcN      to capture       image
```

A discard or write-zeroes request skips the data transfer and punches instead of
writing. `Step` in `owner.rs` is the completion's place in that sequence, packed
into its `user_data` with the disk and the request tag.

## The writing path

A [`journal::Writer`] is a handle onto a task which owns the journal for the
length of a session. The task alternates between the two things which can
happen to a disk, and never abandons an append part-way:

```text
select ──►  request        ──►  publish  ──►  drain, confirm, return the ack
                                commit   ──►  append the ack, await it
                                broker   ──►  replace endpoint and credential

       ──►  mutation       ──►  pack into records ──►  fill a batch ──►  append
```

Requests win that race, so a cut observes every mutation queued before it.
`publish` drains the capture channel itself, which is exact only because the
caller has already stopped admitting mutations and awaited the ones in flight.

## Non-obvious details

- **A mutation is captured before it is applied.** The chunks of every accepted
  write, discard, and write-zeroes go to the capture channel before the image
  operation is submitted, so the order the journal records is the order the
  image is modified. [`inflight::InFlight`] is what preserves that under
  overlap, and a filesystem never issues overlapping concurrent writes, so it is
  expected to stay empty. Backpressure is the channel's bound: a request whose
  chunks do not fit parks until the consumer takes some, in arrival order.

- **A request completes once its image operation lands.** A block device which
  completed a write must serve that data back, so the image write is awaited.
  Durable capture is not.

- **The chunk encoding is allocation-exact, not only content-exact.** Its two
  rules are stated at the top of `chunk.rs`: trailing zeroes are trimmed so
  replay must zero-fill, and empty `data` is an allocated zero block rather than
  a hole. Encode-then-replay therefore matches the raw mutation in bytes and in
  footprint, which the property test in `chunk.rs` asserts against two files and
  the privileged tests assert against a real filesystem.

- **Bitmaps are not atomic.** A disk is owned by exactly one thread.

- **Block size is per-disk and durable.** It shapes chunk coverage and bitmap
  extent, so it is fixed once a disk first publishes. Device size and block
  size are `Open` parameters rather than crate constants, and `u32` block
  indexing caps a device at 2³² blocks.

- **The kernel ABI is generated, not transcribed.** `build.rs` runs bindgen
  over the vendored `ublk_cmd.h`, a copy of the uapi header, and `ublk/sys.rs`
  only renames the result and adds the offsets and helpers this crate layers on
  it. Picking up a newer kernel's features means replacing that header. The
  `libublk` crate is deliberately not used: its queue layer assumes one queue
  per thread and gives its own commands no room for a device id, which does not
  fit an owner serving many disks on one ring.

- **Only ioctl-encoded `UBLK_U_CMD_*` opcodes are used.** The legacy raw opcodes
  need `CONFIG_BLKDEV_UBLK_LEGACY_OPCODES`, which current kernels do not enable.
  A control command does not fit an ordinary SQE, so that ring alone is built
  with `IORING_SETUP_SQE128`.

- **`UBLK_F_USER_COPY` moves request data by `pread`/`pwrite` against
  `/dev/ublkcN`**, at offsets which encode the queue and tag, rather than through
  a mapped per-queue buffer area. Only the array of pending request descriptors
  is mapped.

- **Teardown order is forced by the kernel.** `STOP_DEV` aborts the queue's
  fetches, which is how an owner learns to quiesce; `DEL_DEV` then waits for
  every reference to the device, so the owner must have closed `/dev/ublkcN`
  first. [`disk::Disk`] runs that order on drop as well as on `stop`, so a
  failure leaves no device node behind.

- **`ublks_max` counts unprivileged devices only**, so it never binds these and
  no host needs an `/etc/modprobe.d` entry for this crate's sake. It is still
  reported in errors and logs, because it is the first thing an operator
  reaches for.

- **Nothing assumes it is root.** No `chown`, and no writes outside the
  configured image directory. Production may run as a dedicated UID with ambient
  `CAP_SYS_ADMIN`, which needs a udev rule granting that UID `/dev/ublk-control`
  and `/dev/ublkc*`, since `CAP_SYS_ADMIN` does not bypass file permissions.

- **The author register is read once.** A session reads it at open and never
  refreshes it, which is what stops a session that was displaced from taking the
  journal back. The consequence is that a claim can only ever be attempted once,
  so an append which may or may not have landed is resolved by re-probing for
  the session's own epoch rather than by picking a fresh one.

- **Journal creation is driven by the first append**, not by opening a session,
  because a disk which is never written carries no information. The spec is
  built and validated at open so that a journal which could not be created — no
  fragment store, or a codec this crate cannot decompress — fails before a
  device exists.

- **Appends are issued one at a time and awaited**, so "every chunk of this
  delta is confirmed" is the same statement as "the writer has no work". Records
  and appends are bounded because Gazette serializes appends to a journal, and
  an unbounded one would block every later append of that disk.

- **The acknowledgement is built but not appended.** `publish` returns its exact
  bytes and holds them; `commit` appends those same bytes and awaits the
  broker. Between the two, mutations are not taken, which is what keeps Gazette
  from grouping two deltas into one pending transaction.

- **Credentials and endpoints travel together.** A session's `Broker` replaces
  both at once through a `tokens` watch, matching how a journal client extracts
  the pair from one token. The daemon mints nothing, and a session without a
  credential connects anonymously.

## Testing

`cargo nextest run -p disk-daemon` runs everything, privileged tests included.
`tests/ublk.rs` drives `src/bin/scenario.rs` through `sudo -n`, so cargo never
runs as root and the target directory stays the user's; the scenario prints
observations as JSON which the test asserts against. A missing prerequisite
(`ublk_drv`, `/dev/ublk-control`, passwordless sudo) fails those tests with an
actionable message rather than skipping them, and a nextest test group
serializes them because they contend on the host-wide control device. From
Phase 4 the daemon binary replaces the scenario helper, so tests exercise what
ships.

`tests/journal.rs` works against a real broker, which `crates/e2e-support`
spawns over Unix sockets and which `mise run build:gazette` must have installed
into `$GOBIN`. It needs no privileges.
