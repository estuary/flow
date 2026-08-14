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

A `flow-disk-daemon` binary which serves disks end to end: a session creates a
disk, formats it or rebuilds it from its journal, mounts it, publishes and
commits its deltas, and destroys it when the session ends. A client which
recovered an acknowledgement hands it back, and the session repairs it.

Horizons do not exist, so no record opens one, a replay rejects one it finds,
and the configured floor label is read but never written.

## Key types and entry points

| Item | Role |
| --- | --- |
| [`proto`] (`proto_flow::disk`) | Session RPC (`Open`/`Publish`/`Commit`/`Broker`) and journal records (`DiskRecord`, `Chunk`), from `go/protocols/disk/disk.proto` |
| [`args::Args`] | The command line, which is every knob the daemon has |
| [`daemon::run`] | Host validation, wiring, the socket, and the drain |
| [`session::Service`] | The session state machine, one stream per disk |
| [`filesystem`] | The one place a filesystem type is decided: how to format, and what to mount with |
| [`disk::Disk`] | One disk's lifecycle: image, device, and the owner serving it |
| [`image::Image`] | The `O_TMPFILE` sparse image and its allocated bitmap |
| [`ublk::Control`] | `/dev/ublk-control`: add, configure, start, stop, delete |
| [`owner::spawn`] | The thread which serves one disk, and the ring it drives |
| [`capture::channel`] | The mutation-capture seam between an owner and the writer |
| [`journal::Opening`] / [`journal::Writer`] | One session's journal: fence, recovery, append pipeline, publish and commit |
| [`journal::replay`] | Journal into a rebuilt image, which is the durability guarantee |
| [`journal::fence`] | The `author` register: probe, claim, and the fence record |
| [`journal::floor`] | The label a replay seeks from |
| [`journal::spec`] | `JournalConfig` into a `JournalSpec`, and insert-only creation |
| [`inflight::InFlight`] | Serializes overlapping mutations against one image |
| [`chunk::encode_write`] / [`chunk::encode_punch`] | Device mutation → journal chunks |
| [`chunk::apply`] | Journal chunk → image bytes and allocated bits (replay) |
| [`chunk::covered_blocks`] | Block range a chunk covers, which is the shared rule both halves obey |
| [`bitmap::Bitmap`] | Allocated and horizon block sets |

## A session

```text
Open    ──►  image ──► fence, repair and replay ──► device ──► mount ──► Opened(path)
             a disk with no committed state is formatted instead
Publish ──►  syncfs ──► close admission ──► finish the delta ──► Published(ack)
Commit  ──►  append that ack ──► await the broker ──► Committed
Broker  ──►  replace the endpoint and credential
close   ──►  unmount ──► stop and delete the device ──► drop the image
```

Everything a session does is terminal on failure, and every way a stream ends
runs the same teardown.

## The serving path

Each disk has one owner thread driving one `io_uring`. A device request arrives
as a fetch completion, and each of its steps is another completion on that ring,
so the owner never blocks:

```text
fetch  ──►  read  ──►  write ──►  commit        (device read)
            image      /dev/ublkcN
                       and fetch the next request

fetch  ──►  read       ──►  offer chunks ──►  write  ──►  commit
            /dev/ublkcN      to capture       image
```

A discard or write-zeroes request skips the data transfer and punches instead of
writing. `Step` in `owner.rs` is the completion's place in that sequence, packed
into its `user_data` with the request tag.

## The writing path

A [`journal::Writer`] is a handle onto a task which owns the journal for the
length of a session. The task alternates between the two things which can
happen to a disk, and never abandons an append part-way:

```text
select ──►  request        ──►  publish  ──►  drain, confirm, return the ack
                                commit   ──►  append the ack, await it
                                broker   ──►  replace endpoint and credential
                                abandon  ──►  take what follows and discard it

       ──►  mutation       ──►  pack into records ──►  fill a batch ──►  append
                                a fresh disk's first append snapshots its image
```

Requests win that race, so a cut observes every mutation queued before it.
`publish` drains the capture channel itself, which is exact only because the
caller has already stopped admitting mutations and awaited the ones in flight.

## Non-obvious details

- **The cut of a publication is the owner's.** Closing a disk's admission parks
  its arriving mutations exactly as a full capture channel does, and the owner
  reports the cut once every mutation it did admit has reached the image. Reads
  continue throughout.

- **A session which is ending appends nothing more.** Unmounting writes, and
  those writes could never be committed by an acknowledgement, so the writer is
  abandoned first: it keeps taking mutations, because a device whose mutations
  nothing takes cannot be unmounted, and discards them.

- **A killed daemon is cleaned up by the next one.** The kernel frees the image,
  which has no directory entry, and removes the block device when the process
  serving it dies. The mount over that device and its character device both
  survive, so the next daemon to take the mount directory unmounts what it finds
  and deletes the device each mount point names, once the kernel confirms the
  process which served it is gone. A device this daemon cannot prove was its own
  is left alone, because another application's abandoned device may be one it
  means to recover.

- **The session socket is reachable by any user.** The daemon's clients are not
  the privileged user it runs as, and it has no user model of its own, so which
  of them may reach it is the socket directory's permissions to decide.

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

- **A disk is served by a thread, and the kernel requires it.** `ublk` binds a
  device's queue to the thread which arms its first fetch, and rejects every
  later command from any other thread with `EINVAL`. A tokio task migrates
  between workers, so it cannot serve a queue — this was tried, and the second
  command a migrated task sent came back `-22`. So `owner::spawn` builds and
  arms the ring *on* the serving thread, never on its caller, and that thread
  is the only submitter for the life of the disk. It also makes the bitmaps
  safe without atomics.

- **Threads and rings are per disk, but kernel workers are not.** A parked
  owner costs a kernel stack and 256 KiB of reserved address space, and its
  128-entry ring about 12 KiB, so a hundred disks cost single-digit megabytes.
  What does not scale that way is `io_uring`'s own worker pool, which serves
  operations it cannot complete inline — punches reach it in ordinary use — and
  whose default size comes from the CPU count and `RLIMIT_NPROC`. Every disk's
  ring therefore attaches to one anchor ring with `IORING_SETUP_ATTACH_WQ`, and
  that pool's size is registered rather than inherited. A unit file should set
  `TasksMax`, whose default is derived from the host's `kernel.pid_max`.

- **Block size is per-disk and durable.** It shapes chunk coverage and bitmap
  extent, so it is fixed once a disk first publishes. Device size and block
  size are `Open` parameters rather than crate constants, and `u32` block
  indexing caps a device at 2³² blocks.

- **The kernel ABI is generated, not transcribed.** `build.rs` runs bindgen
  over the vendored `ublk_cmd.h`, a copy of the uapi header, and `ublk/sys.rs`
  only renames the result and adds the offsets and helpers this crate layers on
  it. Picking up a newer kernel's features means replacing that header. The
  `libublk` crate is deliberately not used: its own commands leave no room for
  our `Step` packing, and its queue layer is a thread-local ring, which is not
  a design choice of theirs but the kernel's affinity rule above.

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

- **A journal's spec has no daemon defaults.** Every `JournalConfig` field is
  required, because the spec is created once and never converged: a value the
  daemon invented would be one the disk carries for life, and changing the
  daemon later would not reach it. A missing field is rejected at `Open`, which
  names it. The two Gazette reads zero as a choice for are `optional` on the
  wire, so no append ceiling and no timed flush stay reachable, but only by
  being asked for. That is why this proto has no Go bindings: `protoc-gen-gogo`
  refuses proto3 `optional`, and nothing in Go consumes this protocol.

- **A fresh disk holds nothing between its format and its first write.** Format
  and mount output is taken from the capture channel and dropped, and the first
  append instead asks the owner for a snapshot of the image: the allocated
  blocks, read back and encoded as chunks. The image already holds exactly that
  content, so nothing has to be kept, and a block written twice while formatting
  collapses to one chunk.

  The snapshot is taken after some mutations may already have been applied, and
  that is benign: every mutation captured since the mount is appended after it,
  so one already reflected in it is simply applied again, and a delta is durable
  only once its acknowledgement commits.

  A recovered disk takes no snapshot. Its journal already holds the filesystem,
  and the writes its mount issues belong to the next delta like any other.

- **Journal creation is driven by the first append**, not by opening a session,
  because a disk which is never written carries no information. The spec is
  built and validated at open so that a journal which could not be created — no
  fragment store, or a codec this crate cannot decompress — fails before a
  device exists.

- **Recovery claims the fence for any journal which exists.** Whether a
  journal's records are committed is only knowable by reading them, and reading
  must exclude the previous writer first. A journal holding only the orphans of
  a failed first use therefore gains a fence record and then yields a fresh,
  formatted disk — which is the fence this session would have installed with its
  own first append anyway.

- **A replay buffers nothing.** It applies every delta as it reads it and finds
  a delta which was never acknowledged only at the end of the range. The image
  is then discarded and the range read again, reading over that delta's records.
  Which costs one extra read exactly when a session did not shut down cleanly,
  and removes a spill file with its own budget, `fsync` and validation.

  An unacknowledged delta is not always the last thing in the range: a
  replacement session appends after the orphan records of the one it displaced.
  So the rule is by producer and clock rather than by offset, and it covers both
  cases with one mechanism.

- **Appends are issued one at a time and awaited**, so "every chunk of this
  delta is confirmed" is the same statement as "the writer has no work".

- **One device request is one record, and one drain is one append.** Neither is
  bounded by a tunable, because the capture channel already bounds both: a
  mutation is capped by the device at `MAX_IO_BUF_BYTES`, and a drain sees at
  most the channel's capacity, so an append is at most `queue_depth` mutations
  wide. Splitting either would only give replay more to reassemble. That is
  larger than a broker's gRPC message limit, so the append's byte stream is cut
  into chunks of `CHUNK_BYTES` as `publisher::Appender` does. That is a
  transport detail and not a boundary a record or a delta can see. The cost is that a failed append retries its
  whole content, which is bandwidth under a flaky broker and never correctness.

- **The acknowledgement is built but not appended.** `publish` returns its exact
  bytes and holds them; `commit` appends those same bytes and awaits the
  broker. Between the two, mutations are not taken, which is what keeps Gazette
  from grouping two deltas into one pending transaction.

- **Credentials and endpoints travel together.** A session's `Broker` replaces
  both at once through a `tokens` watch, matching how a journal client extracts
  the pair from one token. Both are the session's to supply and neither has a
  daemon-wide default: an endpoint is required, while a session without a
  credential connects anonymously. The daemon mints nothing.

## Testing

`cargo nextest run -p disk-daemon` runs everything, privileged tests included.
Privilege lives in `sudo -n` child processes rather than in cargo, so the target
directory stays the user's. A missing prerequisite (`ublk_drv`,
`/dev/ublk-control`, passwordless sudo) fails those tests with an actionable
message rather than skipping them, and a nextest test group serializes them
because they contend on the host-wide control device.

`tests/daemon.rs` drives `flow-disk-daemon` itself, so it exercises what ships:
it spawns the daemon under `sudo`, speaks the session gRPC over its socket, and
reaches the root-owned mounts it returns through `sudo` of its own. It replays
each journal it commits into an image and loop-mounts that to compare content.
Its crash matrix reopens a disk after committing, after publishing without a
commit, with a recovered acknowledgement, and after a cut taken mid-writeback.

`tests/ublk.rs` drives `src/bin/scenario.rs`, which works a disk with no session
around it, printing observations as JSON the test asserts against. It needs no
broker. This second binary is not scaffolding: privilege needs a process
boundary, and using the crate as a library is what lets a scenario set a queue
depth shallow enough to force backpressure, and report the image digests and
extent lists which show a replay matching in holes as well as in bytes.

`tests/daemon.rs` and `tests/journal.rs` work against a real broker, which
`crates/e2e-support` spawns over Unix sockets and which `mise run build:gazette`
must have installed into `$GOBIN`. Only the daemon's tests need privileges.
