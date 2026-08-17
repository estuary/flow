# Block-Backed Connector Disks

![At a glance, in four panels. 1: where the disk lives — a connector's POSIX
operations reach a host ext4 filesystem over virtio-fs or a bind mount, which
the privileged disk daemon serves from a sparse local image through a ublk
block device, appending block deltas to a per-shard Gazette journal. 2: how a
boundary commits — Publish, the device cut, acknowledgement bytes returned but
not appended, Persist of the checkpoint and its AI: obligation to the Flow
recovery log, then Commit to append the acknowledgement. 3: why the journal
stays bounded — recovery floor, horizon, and write head on a journal offset
axis, with the r and k compaction constants. 4: which task types get a
checkpoint-aligned disk.](block-backed-connector-disks.png)

## Solution overview

A shard is one running partition of a task. Flow gives each live connector
shard a private 10 GiB filesystem at a stable path inside its sandbox. A
connector uses ordinary POSIX file operations and does not need to configure or
negotiate for the disk. The disk is private to one shard and is not shared with
other shards. It can hold embedded databases, indexes, staged files, caches,
and other working state without encoding file contents in a connector
checkpoint.

The filesystem advances with Flow transactions. When a transaction commits,
its connector checkpoint and disk changes become durable together. After a
failure, Flow creates a new local filesystem from the disk state associated
with the last committed checkpoint. Changes from an uncommitted transaction
are discarded.

A privileged disk daemon provides the filesystem. It stores the current disk
in a sparse local image and exposes that image to the host kernel through Linux
`ublk`. The host mounts an ext4 filesystem and passes its directory to the
connector sandbox. Connectors never receive a block device or elevated host
privileges.

The disk daemon is designed as a general-purpose service. Its core primitive is
a block device whose state advances atomically with an external commit and
whose history is stored in Gazette. That primitive does not depend on Flow, so
the daemon can also be used as a stand-alone Gazette-backed utility by other
systems.

The daemon appends each 4 KiB block the device writes to a per-shard Gazette
journal, as the write arrives. These block deltas are the durable copy of the
disk; the local image is disposable.
The runtime commits a delta by recording its acknowledgement in the same
recovery-log transaction as the connector checkpoint. The daemon also copies a
small amount of unchanged data with each delta. These copies move the start of
the required recovery range forward, which keeps recovery time and retained
journal data bounded.

The product behavior is:

| Area | Behavior |
| --- | --- |
| Provisioning | Every live capture, derivation, and runtime-authoritative materialization shard receives a disk before its connector opens. |
| Capacity | Each disk has a fixed logical size of 10 GiB. The sparse local image consumes space only for allocated blocks. |
| Configuration | There is no catalog setting, connector capability, or opt-in. |
| Durability | Disk contents commit atomically with the Flow transaction that uses them. |
| Recovery | Flow rebuilds a fresh local image from the last committed disk state. Recovery reproduces filesystem contents, not byte-for-byte ext4 metadata. |
| Derivations | The terminal flush response provides a checkpoint-aligned disk boundary. |
| Materializations | Runtime-authoritative materializations use their commit and acknowledge exchange. Remote-authoritative materializations are not supported. |
| Captures | A capture gets checkpoint-aligned recovery only when it stops writing the disk between a checkpoint and its acknowledgement. Other captures may use the disk for cache or scratch data, but not for resume-critical state. |
| Other connector calls | Validation, discovery, `Apply`, and other non-transactional calls do not receive a disk. |
| Platform | The disk daemon requires Linux 6.0 or later, `ublk_drv`, and `CAP_SYS_ADMIN`. Reactors and connectors remain unprivileged. |
| Shard Splits | Splitting shards with disk journals is TBD, but will require a mechanism similar to splitting tasks with per-shard recovery logs (captures) |
| Recovery Logs | A possible future candidate for replacement of the existing gazette RocksDB store mechanism with these general purpose journal backed disks |

The first transaction that uses a fresh disk publishes the filesystem's initial
metadata as well as the connector's changes. Later transactions publish the
blocks the connector writes, plus the bounded amount of unchanged data needed to
advance recovery.
With the policy in this design, the required recovery range is approximately
bounded to five times the disk's live allocated size.

## Scope and guarantees

### Provisioning

A live task call is a connector process that participates in Flow
transactions. Before opening such a connector, the runtime asks the daemon for
a disk and places the returned directory at a stable path in the connector
sandbox. This applies to captures, derivations, and runtime-authoritative
materializations.

Non-transactional calls do not receive a disk. Their filesystem changes cannot
be associated with a Flow commit. This includes validation, discovery,
`Apply`, and similar calls.

The 10 GiB capacity is a platform constant. It is not part of a catalog model
or connector configuration. Compaction, spill limits, and local placement can
therefore rely on a small, known maximum.

The mount works with both supported sandbox types:

- A `libkrun` guest receives the directory through `virtio-fs`.
- A plain container receives the directory through a bind mount.

The durable design does not depend on the sandbox type.

### Filesystem guarantee

Recovery presents the same filesystem contents that the connector saw at the
committed boundary. It does not reproduce an identical block image. Mounting,
filesystem-journal replay, and unmounting can change ext4 bookkeeping such as
timestamps and mount counts.

The daemon guarantees that each published delta is a coherent point-in-time
device state. The runtime and connector guarantee that this point is aligned
with a connector checkpoint. If the connector violates its boundary contract,
the disk remains a valid filesystem but may contain state from before or after
the checkpoint.

### Connector boundary contract

At a boundary where its disk changed, a connector must:

1. `fsync` the state that belongs to the transaction.
2. Return the protocol response that marks the boundary.
3. Stop writing the disk until the runtime releases it.

Skipping `fsync` gives the connector power-loss semantics for buffered data.
Writing during the quiet period can move the disk ahead of its checkpoint.
Connector messages and block I/O are buffered independently, so neither the
runtime nor the daemon can detect these violations.

The exact boundary and release messages depend on the task type. They are
defined in [Task-specific boundaries](#task-specific-boundaries).

## Architecture

The disk daemon is a machine-level singleton with two interfaces:

- a userspace block-device server that handles Linux `ublk` I/O through
  `io_uring`; and
- a gRPC service on a Unix domain socket for the runtime.

The daemon's protocol uses opaque acknowledgements and generic journal metadata.
It does not interpret Flow transactions, labels, catalog models, or connector
filesystems.

```text
connector process
       │ POSIX file operations
       ▼
virtio-fs or bind mount
       │
       ▼
host ext4 filesystem
       │ block requests
       ▼
Linux ublk device ◄──── disk daemon ────► sparse local image
                           │
                           │ block deltas
                           ▼
                  per-shard Gazette journal
```

### Disk ownership

Each disk is served by exactly one **owner**: the thread that reaps its
`io_uring` completions. The owner is the only mutator of that disk's image and
bitmaps. Single ownership is what keeps the capture rules below simple. Every
decision about a block is serialized, so no lock protects a bitmap and the
order in which mutations are journaled is the order in which they are applied.

Ownership is per disk, not per thread. Owners are pooled and one owner serves
many disks, so a machine hosting a hundred disks runs a handful of owner
threads rather than a hundred. An owner never blocks: all image I/O is
submitted to its ring and reaped later, so a slow operation on one disk cannot
stall another.

Journal I/O does not run on an owner. The owner hands each mutation's chunk to
an async task over a bounded channel, and that task performs the appends. The
channel bound is the backpressure on the device: a request whose chunk cannot
be queued waits, which is what stops an unconfirmed delta from growing without
limit.

### Division of responsibility

The daemon owns the device image, change-tracking bitmaps, boundary cuts, delta
capture, journal format, compaction horizons, and recovery.

The runtime owns Flow-specific coordination:

| Runtime responsibility | Reason |
| --- | --- |
| Connector protocol timing | Only the runtime knows when a connector has reached a transaction boundary. |
| `AI:` recovery-log entries | The Flow recovery log is the authority for a committed transaction. |
| `estuary.dev/truncated-at` | The label is part of Flow's journal conventions. |
| `JournalSpec` construction | The spec comes from task and catalog templates. |

### Local and durable state

Each live disk has three local data structures:

- a sparse image file;
- an allocated bitmap; and
- a horizon bitmap.

The local structures are disposable. A disk that has published state has a
per-shard Gazette journal. Its acknowledged deltas, plus any pending delta
identified by a recovered `AI:`, define the durable disk state.

The **recovery floor** is the earliest journal offset needed to rebuild the
disk. The daemon derives it from journal contents. The runtime stores the same
position as a message clock in `estuary.dev/truncated-at` so the daemon can seek
near it on the next recovery.

## Daemon session protocol

One device corresponds to one bidirectional gRPC stream. The device exists for
the lifetime of the stream. If the stream closes, the daemon unmounts the
filesystem and destroys the device. This prevents an orphaned process from
writing to a journal after it loses ownership.

The daemon returns a mounted directory, not a block-device node. It owns
formatting, mount options, and filesystem tuning. The runtime only needs a path
that it can pass into a sandbox.

The stream has four request types:

| Request | Reply | Purpose |
| --- | --- | --- |
| `Open` | `Opened` | Create or recover a disk and return its mount path. |
| `Publish` | `Published` | Cut one point-in-time delta and return its acknowledgement. |
| `Commit` | `Committed` | Append an acknowledgement that the runtime has made durable. |
| `Credentials` | none | Replace the broker credential. |

`Open` includes the device size, `JournalSpec`, broker address, credential,
recovered acknowledgements, and an optional recovery floor. `Opened` returns
the mount path and the floor derived by the daemon. It does not report whether
the disk is fresh because the daemon handles fresh-disk formatting itself.

The daemon uses the supplied `JournalSpec` only when it first needs to append
and the journal does not exist. Journal creation is lazy.

`Publish` establishes the boundary and finishes publication of its delta. If
the disk has no changes and no horizon work was published, `Published`
contains no acknowledgement. `Committed` includes a new floor only when that
commit completes a horizon.

The stream contains request and reply messages only. The daemon does not send
unsolicited events. Device I/O and an in-flight copy can continue while the
stream is otherwise idle. A terminal device error closes the stream.

### Opening a session

The runtime opens a disk in this order:

1. Acquire and recover the shard recovery log.
2. Read `estuary.dev/truncated-at` from the disk journal.
3. Send `Open` with every recovered `AI:` acknowledgement and the label value.
4. Place the returned mount path in the sandbox through `virtio-fs` or a bind
   mount.
5. Open the connector.

### Publishing and committing a boundary

At each transaction boundary, the runtime:

1. Brings the connector to the task-specific boundary and keeps it quiet.
2. Sends `Publish`.
3. Takes the unchanged fast path if `Published` contains no acknowledgement.
4. Otherwise records the exact acknowledgement as `AI:` in the same
   recovery-log commit as the checkpoint.
5. Sends `Commit` with the same bytes after the recovery-log commit is durable.
6. Applies `estuary.dev/truncated-at` if `Committed` returns a newer floor.
7. Releases the connector.

The runtime may hold at most one uncommitted acknowledgement for a session. It
must send `Commit` before the next `Publish`. A second `Publish`, an early
`Commit`, changed acknowledgement bytes, or connector writes during the quiet
period are protocol violations.

### Errors

The daemon retries broker errors internally. An error returned to the runtime
is terminal for the session, and the runtime tears down the shard.

A fence failure means that another process owns the shard's disk journal. It is
also terminal.

## Local filesystem implementation

### Sparse image

The daemon creates the 10 GiB image with `O_TMPFILE` and `ftruncate`. The image
has no directory entry. Byte *N* in the file is byte *N* on the block device.

The file starts with no allocated host blocks. A disk containing 200 MiB of
allocated data therefore uses about 200 MiB of local storage, plus metadata.
The daemon treats the content as opaque bytes and never parses ext4
superblocks, inodes, or directory entries.

Using one ordinary file allows block reads, writes, checksums, hole punching,
and cleanup to use normal file operations. The host filesystem must support
hole punching.

### Change tracking

All tracking uses 4 KiB blocks. A 10 GiB image has 2,621,440 blocks, so each
bitmap is 320 KiB. A disk's owner is the only writer of its bitmaps, so they
are plain word arrays rather than atomics.

The **allocated bitmap** identifies blocks that currently occupy space in the
image. Writes set allocated bits. Successful aligned discards clear them. It is
the exact measure of physical use which the compaction threshold compares
against, and it initializes the horizon bitmap. `st_blocks` cannot serve
either purpose: ext4 delays allocation, so a just-written block may not be
charged until writeback.

The **horizon bitmap** identifies allocated blocks whose newest durable copy is
before the active recovery horizon. The daemon fills it when a horizon starts,
then clears bits as later deltas cover those blocks. It is empty when no horizon
is active.

There is no dirty bitmap. Because every mutation is appended as it is accepted,
nothing is ever owed to a delta, and there is nothing for such a bitmap to
track. Reintroducing one is the coalescing optimization described in
[Appending as writes arrive](#appending-as-writes-arrive).

The daemon also tracks the block ranges of mutations currently in flight
against the image, so that two overlapping mutations are never applied
concurrently. That is what makes journal order equal application order. It is
not a bitmap: a filesystem does not issue overlapping concurrent writes, so the
set is expected to stay empty in practice.

### Block transport

The daemon creates one ephemeral `ublk` device for each disk:

1. Loading `ublk_drv` exposes the host-wide `/dev/ublk-control` device.
2. The daemon sends `UBLK_U_CMD_ADD_DEV` and lets the kernel select device
   number `N`. Devtmpfs and udev create `/dev/ublkcN`, which is the character
   device served by the daemon.
3. The daemon configures the size and queue limits and starts the I/O queue.
   `UBLK_U_CMD_START_DEV` exposes `/dev/ublkbN`, which is the block device
   mounted by the daemon.
4. Session teardown stops the block device and deletes the character device.
   The kernel can immediately reuse `N`.

The daemon uses the ioctl-encoded `UBLK_U_CMD_*` opcodes. The legacy raw
opcodes require `CONFIG_BLKDEV_UBLK_LEGACY_OPCODES`, which current kernels do
not enable.

Both device nodes stay in the daemon's namespace. Device numbers, mount paths,
and mount options never appear in the durable format.

Each device is configured with a single queue. Concurrency comes from queue
depth rather than queue count: the owner keeps many operations in flight on its
ring. One queue is what gives each disk one owner, and the block path has ample
headroom over the rate at which a delta can be appended.

The device advertises no volatile write cache, so the kernel issues no flush or
force-unit-access requests and the daemon implements neither. This is not a
weakened guarantee. The local image is disposable, durability belongs to the
journal, and a completed write is exactly what a delta captures. Ext4 orders
its own journal by write completion, which still makes the image a valid
power-loss point at every instant.

The daemon requests `UBLK_F_USER_COPY`, so request data moves by `pread` and
`pwrite` against the character device rather than through a mapped per-queue
buffer area. The daemon needs owned buffers for journal chunks in any case.

The served request types are therefore reads, writes, discards, and write-zero
requests.

### Format and mount

The daemon formats only a fresh image. Recovery treats all filesystem
structures as data and never formats a rebuilt image.

Fresh disks use ext4 with:

- a 4 KiB block size, matching the tracking granularity;
- zero reserved blocks;
- the default inode density;
- e2fsprogs 1.47.0 or later with `assume_storage_prezeroed=1`; and
- whole-device discard disabled during format with `nodiscard`.

A fresh sparse image is logically zero-filled.
`assume_storage_prezeroed=1` lets `mkfs` avoid writing zeroes across unused inode
tables and the internal journal. It also marks the inode tables initialized.
Those reserved ranges remain holes, do not enter the allocated bitmap, and do
not cause later background initialization writes. This reduces the first
delta, which is on the first disk-writing transaction's critical path.

Fresh and recovered disks use the same mount options:

- `noatime`, so reads do not create deltas;
- `nodev`, `nosuid`, and `noexec`; and
- `discard`, so ext4 releases blocks when it frees them.

The sandbox applies `nodev`, `nosuid`, and `noexec` again when it re-exports the
directory. Host mount options do not propagate through a `virtio-fs` mount or a
container bind mount.

Formatting and mounting write through the served device. The daemon retains
those writes. Formatting and mounting reach the device as ordinary requests, so
they are appended like any other mutation. The first delta therefore contains
all allocated filesystem metadata as well as connector data, and it accrues
during the format rather than as a burst at the boundary. Unwritten zero ranges
are never written, so they are never appended.

The daemon also appends the changes made while mounting a recovered disk. Ext4
journal replay and mount bookkeeping change the rebuilt committed baseline, so
the next delta must include them, and they are appended as the mount issues
them like any other write.

## Disk journal

### Journal creation and format

The daemon creates the per-shard journal when the disk first needs durable
state. A fresh connector that never causes a disk transaction does not create a
journal.

The `JournalSpec` uses `SNAPPY`, matching recovery logs. Disk content may be
plain text or data that is already compressed. Snappy passes incompressible
data through at low cost. The broker performs compression while spooling a
fragment; appends and replication to peers carry uncompressed bytes.

Journal offsets count uncompressed bytes. Fragment readers select the codec
recorded on each fragment, so the codec can change later without changing the
record format or recovery arithmetic. Removing zero tails from block chunks
reduces append, replication, and storage volume before compression.

Records use Gazette's fixed Protobuf framing and one disk-specific message:

```text
DiskRecord
┌────────────────┬────────────────────────────────────────────────┐
│ chunks         │ zero or more block chunks                      │
│ opens_horizon  │ true only on the first record of a transaction │
│ installs_epoch │ present only on a fence record                 │
└────────────────┴────────────────────────────────────────────────┘
```

The Gazette UUID supplies the producer, clock, and transaction flag.

- A **fence record** is `OUTSIDE_TXN`. It has `installs_epoch`, contains no
  chunks, and does not change disk contents.
- A **delta** has one or more `CONTINUE_TXN` records followed by one `ACK_TXN`
  record. The acknowledgement contains no chunks and commits the delta.

The daemon bounds record size and append-batch size. Gazette serializes appends
to a journal, so these limits keep one large disk transaction from blocking
later work indefinitely.

Before constructing an acknowledgement, the daemon asserts that no mutation
remains in flight and every data append is confirmed. This catches a missing
chunk at the writer. The daemon returns the exact serialized acknowledgement
to the runtime. It appends that acknowledgement only after the runtime returns
it in `Commit`.

### Chunk encoding

Each chunk has this form:

```text
Chunk
┌───────────────┬───────────────────────────────────────────────────┐
│ block: u32    │ starting 4 KiB block index                        │
│ one of                                                            │
│   bytes       │ content beginning at that block                   │
│   punch: u32  │ number of blocks to discard                       │
└───────────────┴───────────────────────────────────────────────────┘
```

Using a block index enforces 4 KiB alignment and maps directly to the bitmaps.
A `u32` covers up to 17 TiB at this block size.

Protobuf already carries the length of `bytes`. The length can end within the
last block. The daemon removes trailing zeroes from a data run, and the chunk
covers `ceil(len(bytes) / 4096)` blocks. Replay must explicitly write zeroes
from the end of `bytes` through the end of its last block. Otherwise an older
value in that block's tail would survive.

An empty `bytes` value represents one allocated block filled with zeroes. A
`punch` represents unallocated blocks. Replay writes the former and
hole-punches the latter.

At a boundary:

- a write becomes a data chunk carrying the incoming bytes;
- a discard or write-zero request becomes a punch; and
- an unchanged block copied for a horizon becomes an ordinary data chunk.

Chunks have no additional checksum. TLS protects transport and Gazette fragment
content sums protect stored fragments. Writer assertions validate that every
mutation was appended. These checks cover the spans in which the daemon can
detect corruption.

Chunks in one delta are not required to be sorted or unique. Concurrent writes
can make the mutation path capture a block before the normal copy cursor reaches
it, and a block written repeatedly appears once per write. Replay
applies chunks in journal order, so the last chunk for a block wins.

### Record sequencing

A shard session uses one Gazette producer for all its deltas. A replacement
session uses a new producer. Each fence uses a separate producer and carries
the shard-session producer that it installs as the writer epoch.

Readers use Gazette `message.Sequencer` semantics. The sequencer groups records
by producer and clock, removes duplicate UUIDs, and releases only acknowledged
transactions.

## Commit authority and writer fencing

### Recovery-log authority

The local image is a working copy. The acknowledged disk-journal deltas are its
durable state. The Flow recovery log decides which delta acknowledgements must
exist.

The recovery log stores one disk entry:

- `AI:{journal}` is the exact serialized `ACK_TXN` record required by a
  committed Flow transaction.

An unacknowledged delta is not committed disk state. If a Flow commit becomes
durable before its disk acknowledgement is appended, the recovered `AI:` entry
requires the next session to append those exact bytes.

```text
connector checkpoint and state ─┐
                                ├─ Flow recovery-log transaction
AI: pending disk acknowledgement ┘             │
                                                │ commits authority
                                                ▼
                                  acknowledged disk-journal delta
                                                │
                                                │ replay
                                                ▼
                                      rebuilt local image
```

Derivations and materializations have one recovery log owned by the Shuffle
Leader. One `Persist` records the checkpoint and every shard's disk obligation.
Each shard then appends its acknowledgement to its own disk journal. Captures
perform the same coordination in their local transaction state machine.

The shared recovery log and per-shard disk journals require this two-step
commit. The `AI:` entries connect the authoritative Flow commit to its separate
disk-journal appends.

### One writer per journal

Owning a shard recovery log prevents an old primary from committing another
`Persist`, but the disk journal is separate. Each disk journal therefore has an
`author` register.

When a daemon opens a journal, it chooses a fresh epoch `E`, reads the current
`author` value once as `R`, and never refreshes `R`. It claims the journal by
appending `Fence(E)` while atomically changing `author` from `R` to `E`. The
daemon waits for broker confirmation before other journal operations.

The one-time comparison prevents a stale session from reading a replacement's
epoch and then taking ownership back. A session may only replace the value it
observed during startup.

Claim timing depends on durable state:

- If the journal contains an acknowledged delta or `Open` carries a recovered
  `AI:`, the daemon claims it before repairing acknowledgements, selecting a
  recovery range, replaying, or returning from `Open`.
- If the journal has no acknowledged delta and `Open` carries no `AI:`, the
  daemon only records `R`. This includes an absent journal and a journal
  containing records from a failed first use. Creation and the `R` to `E` claim
  occur when the next transaction first publishes disk state.

An ambiguous fence append is retried idempotently by checking for `E`. It is
not retried with a new epoch. A register mismatch ends the session. Every later
append requires `author=E`.

The epoch is internal to the daemon. The runtime stores acknowledgements as
opaque bytes and does not validate epochs.

The register fences cooperative writers but is not commit authority. Register
state can be lost independently of journal contents. Committed records and
recovered `AI:` entries remain authoritative. Gazette orders the fence with all
other appends, so an old append ordered after the fence fails its author check.

## Capturing a transaction

A **delta** is the set of block chunks published for one Flow transaction. It
contains every mutation the device accepted during the transaction, and may
include unchanged blocks copied for an active horizon. The encoding treats both
kinds of data chunks identically.

### Appending as writes arrive

The daemon does not accumulate changes and publish them at the boundary. It
appends each mutation to the journal as the device accepts it, so the journal
is a log of the device's write stream rather than a snapshot of what changed.

For a write, the daemon appends the incoming data; for a discard or write-zero
request, it appends a punch. Both are appended **before** the image is modified,
and the request completes once its chunk is queued. Because the daemon appends
the bytes the kernel just handed it, publication needs no image read at all.

Two properties make this correct:

> Chunks are appended in the order the owner admitted their requests, and the
> image is modified in that same order.

Journal order is the order replay will apply. If the image were modified in a
different order than the journal records, a rebuilt disk would not match what
the connector saw. The owner admits requests one at a time, so appending in
admission order is free; the requirement is that it must not allow two
overlapping mutations to be in flight against the image at once. A filesystem
does not issue overlapping concurrent writes to the same block, so this guard
is expected never to fire, but it is what the ordering rests on.

> The last chunk for a block is that block's value at the transaction boundary.

This already held for the previous design and still does: a block written
repeatedly appears once per write, and replay applies chunks in journal order.

Deleting the accumulate-then-publish model removes the dirty bitmap, the owed
set, copy before overwrite, and the capture reads that served it. Nothing is
owed at the boundary, so nothing has to be protected from being overwritten
before it is published.

**The cost is coalescing.** The journal now records write *operations* rather
than block *states*, so a block rewritten many times within one transaction is
appended many times. The filesystem's own rewrite traffic -- the ext4 journal
area, superblock, and group descriptors -- is the bulk of it. Two consequences
follow, and both are accepted:

- Journal volume scales with total write traffic rather than with the changed
  set, which also makes the recovery range grow faster and horizons open more
  often.
- Backpressure now applies to writes. The append queue is bounded, and a
  request whose chunk cannot be queued waits, so a write-heavy connector is
  limited by the journal's append rate rather than by its rate of *distinct*
  block changes.

Reintroducing a dirty bitmap is the optimization that recovers coalescing: the
daemon would mark blocks dirty, publish them from the image in the background,
and republish at the boundary anything dirtied again since its early copy. That
restores copy before overwrite and the owed set along with it. It is worth
doing if measurement shows the rewrite factor is material, and it changes no
durable format -- the journal, the replay rules, and the horizon machinery are
identical either way.

### Establishing the boundary

The connector first flushes and stops its own writes. Ext4 can still issue
journal and writeback requests, so the daemon establishes the exact device
boundary:

1. Call `syncfs` on the daemon's mount.
2. Stop admitting new mutations for this disk.
3. Wait for every admitted mutation to finish, and for every chunk it appended
   to be confirmed.
4. Construct the acknowledgement.
5. Resume admitting mutations, holding them until the acknowledgement commits.

The daemon issues `syncfs` itself so correctness does not depend on how a
connector's `fsync` propagates through `virtio-fs` or a bind mount. Connector
flushes normally keep this work small.

Closing admission is the point-in-time cut. Each mutation is entirely before or
after it, because a mutation's chunk is appended before its image write is
issued. Reads continue, and mutations arriving while admission is stopped wait
until it resumes.

Mutations must then keep waiting until the acknowledgement is committed, since
under this model a mutation *is* a publication and the next delta's first
record may not be appended before the previous acknowledgement confirms. The
connector is already required to be quiet across this window by its boundary
contract, so what waits here is ext4 background writeback rather than connector
work.

The cut is per-disk state held by that disk's owner, not a barrier across the
owner's other disks: an owner reaching a cut for one disk keeps serving every
other disk it owns. Because the owner alone admits mutations, closing admission
is a local decision that continuous writeback cannot starve.

An ext4 journal operation can span the cut as several block requests. The
image is still a valid power-loss point. On recovery, ext4 either replays a
committed filesystem-journal operation or discards an incomplete one. Each
recovered image is therefore mounted as a dirty filesystem and runs ext4
journal replay.

### Finishing a delta

By the cut, every mutation of the transaction has already been appended, so
finishing a delta is only a matter of confirmation:

1. Wait for every queued chunk to be confirmed by the broker.
2. Assert that no mutation remains in flight and no chunk remains unconfirmed.
   This catches a lost chunk at the writer rather than at recovery.
3. Construct the acknowledgement, which is returned to the runtime but not
   appended.

Adjacent blocks within a single request coalesce into one chunk, and a request
larger than the record limit spans several records. The daemon bounds record
and batch size because Gazette serializes appends to a journal: without a
bound, one large transaction would block later work on that journal
indefinitely.

Writes, filesystem-journal writes, writeback, discards, and write-zero requests
are all handled the same way. A discard appends a punch and clears the live
allocated bit; a write appends its data and sets the bit.

Horizon copies are the one remaining case where the daemon publishes from the
image rather than from an incoming request, because an unchanged block appears
in no write. They are described in
[Selecting horizon blocks](#selecting-horizon-blocks).

### Commit sequence

An ordinary disk-changing transaction commits in this order:

1. The connector flushes its application state and reaches its protocol
   boundary.
2. The runtime sends `Publish`.
3. The daemon establishes the device cut, having already appended every
   mutation of the transaction.
4. The daemon adds eligible horizon blocks and waits for every data append to
   be confirmed.
5. The daemon returns the exact, not-yet-appended acknowledgement in
   `Published`.
6. The runtime reports that acknowledgement to the transaction coordinator.
7. `Persist` atomically records the checkpoint, connector state, and `AI:`
   obligation.
8. After `Persisted`, the coordinator tells each shard to send `Commit` with
   its recorded acknowledgement.
9. Each daemon appends the acknowledgement, waits for its append barrier, and
   returns `Committed`.
10. The coordinator waits for every shard before allowing the next transaction
    to reach its closing boundary.

If the disk has no changed blocks and no horizon chunks were published during
the transaction, `Published` has no acknowledgement and steps 4 through 10
have no disk work.

```text
daemon                  runtime                     coordinator
   │                       │                             │
   ◄──── Publish ──────────┤                             │
   ├─ finish data appends  │                             │
   ├─ return ack bytes ───►│                             │
   │                       ├─ report ack ───────────────►│
   │                       │                  Persist(checkpoint,
   │                       │                          state, AI)
   │                       │                             │
   │                       │◄──────────────────── Persisted
   ◄──── Commit(ack) ──────┤                             │
   ├─ append ack           │                             │
   ├─ wait for barrier     │                             │
   ├─ Committed ──────────►│                             │
   │                       ├─ confirm durable ──────────►│
   │                       │                             │
   │                       │◄──── next txn may close ────┘
```

The acknowledgement barrier is part of the correctness model. The next
delta's first record cannot be appended until the previous acknowledgement is
confirmed. This keeps Gazette from grouping two deltas into one pending
transaction and advancing the disk farther than the Flow recovery-log commit.
Local connector work may resume when its task protocol permits; only the next
delta publication waits.

For a multi-shard task, one recovery-log `Persist` contains all disk
obligations. The acknowledgement appends are independent and idempotent:

- If `Persist` does not commit, none of the disk deltas commit.
- If `Persist` commits, recovered `AI:` entries bring every disk to that
  transaction even when some acknowledgement appends failed.

The next `Persist` replaces the previous obligation set. The coordinator must
not construct it while an earlier disk acknowledgement remains outstanding.

Failures before `Persist` leave unacknowledged delta records, which recovery
ignores. Failures after `Persist` cannot roll back the Flow transaction, so
recovery appends the exact `AI:` bytes before normal journal use. Gazette
deduplicates a repaired acknowledgement by its original UUID.

## Bounded recovery with horizons

Without compaction, the journal range and recovery time would grow with every
disk change. A **horizon** incrementally moves the recovery floor forward.

A horizon is a journal position that satisfies this invariant:

> Every allocated block has a committed copy at or after the horizon.

Once the invariant holds, recovery can start at the horizon. Older fragments
are no longer needed for this disk.

### Tracking a horizon

When a horizon starts, the daemon copies the allocated bitmap into the horizon
bitmap. A set bit means that the newest committed copy of that block is before
the horizon.

A committed delta clears a bit when either:

- the connector changed that block and the delta published it; or
- the daemon copied the unchanged block into the delta for compaction.

Bits clear only after the delta commits. A failed delta leaves the bitmap
unchanged, matching what recovery derives when it discards the same delta. Bits
never become set again during one horizon. The bitmap therefore decreases until
it is empty.

The daemon scans the horizon bitmap with a forward cursor. Bits behind the
cursor are clear, so each horizon needs at most one full pass through the
bitmap.

The live allocated snapshot can differ from the committed allocated set at the
same journal position:

- A live-only block has an unpublished write. A later delta publishes it and
  clears the bit.
- A committed-only block has a later unpublished discard. That delta records a
  punch and clears the bit.

These differences add limited redundant work but do not change the recovered
state.

### Selecting horizon blocks

The daemon reads horizon blocks from the local image. This avoids fetching old
journal fragments that may already be in cloud storage.

Only a block whose current image value is already committed can be copied as
unchanged data. The candidate set is:

> horizon bitmap minus blocks this delta has already published minus blocks
> with a mutation in flight

Because every mutation is appended as it is accepted, a block's image value and
its newest committed value agree unless a mutation is still in flight. The
candidate set is therefore live: the daemon can interleave horizon copies with
ordinary traffic throughout the transaction rather than saving them for the
cut, which spreads compaction work off the boundary. The budget grows with the
delta's published bytes, so early in a transaction there is little to spend.

If a mutation reaches a selected horizon block while it is being copied, the
daemon removes that block from the horizon work for this delta and lets the
mutation proceed. The next delta publishes the new value and clears its horizon
bit. A horizon copy is therefore never protected against an incoming mutation:
the mutation is itself a publication, and it supersedes the copy.

### Encoding and resuming a horizon

The first record of the delta that starts a horizon sets `opens_horizon`. The
horizon position is that record's offset. The flag must appear on the first
record because recovery snapshots the allocated set before applying any chunk
from that delta. Placing it later would let earlier chunks clear bits even
though a reader starting at the horizon could not see those chunks.

The daemon therefore decides whether a horizon opens at the moment it appends a
delta's *first* record -- the transaction's first mutation -- not at the cut. It
compares the journal range from the current floor against the live allocated
size then, and if a horizon opens it snapshots the allocated bitmap before that
record's chunks are applied, mirroring what replay does at the same record.

The horizon is part of its opening delta. If that delta does not commit, the
horizon does not exist. A writer can have only one open horizon. Recovery may
encounter several completed horizons when the journal label is behind; each new
horizon replaces the previous snapshot.

The horizon bitmap is local memory. Recovery rebuilds it during normal replay:

1. Rebuild the image from the current floor.
2. At `opens_horizon`, copy the current allocated set into the horizon bitmap.
3. Clear bits for chunks applied at and after the horizon.

The ordinary data chunks are also the durable record of compaction progress. A
replacement session resumes the open horizon instead of restarting it.

### Compaction policy

Each disk-changing delta may copy unchanged data in proportion to its changed
data:

> unchanged bytes copied = `k` × changed bytes

The initial policy is `k = 0.5`. Total journal write amplification during
compaction is therefore at most `1 + k`, excluding framing. A transaction that
does not change the disk appends nothing and performs no horizon work.

The daemon opens a horizon when the journal range from the current floor to the
write head exceeds `r` times the live allocated size. The policy uses `r = 2`
and a minimum threshold of 1 GiB. The minimum prevents frequent horizons on a
small disk.

`k` controls write amplification and completion speed. `r` controls the range
size at which compaction starts. With `k = 0.5` and `r = 2`, the required
recovery range is approximately bounded to five times the live allocated size.
Both values are platform constants.

Connector rewrites also clear horizon bits. A connector that rewrites most of
its disk therefore needs little extra copying. If a connector stops changing
the disk, an open horizon pauses. The journal also stops growing, so the
recovery range remains fixed until disk writes resume.

### Completing a horizon

The floor moves after a committed delta empties the horizon bitmap:

1. `Commit` appends the delta acknowledgement.
2. The daemon waits for broker confirmation.
3. `Committed` returns the message clock of the record that opened the horizon.
4. The runtime writes that clock to `estuary.dev/truncated-at`.

No completion record is needed. Every reader derives completion from the same
chunks and allocated bitmap.

The runtime updates the label monotonically and retries at startup and during
normal operation. The label can be absent or behind the derived floor. It must
never be ahead. A stale value increases replay work but does not change the
result. Physical fragment deletion is a separate operation.

`estuary.dev/truncated-at` is also used by capture backfills. In both cases it
means that readers need no records before the given message clock. The label is
preserved through journal-spec convergence by `go/labels/labels.go`; its
documentation and convergence comment must cover both uses.

A capture backfill floor is a runtime decision that is not recoverable from the
collection journal, so the recovery log stores it as `AB:{state_key}`. A disk
floor is derived from disk-journal contents during every replay and needs no
additional recovery-log entry.

## Task-specific boundaries

All task types use the same journal format, capture algorithm, commit authority,
and recovery path. They differ in how they make a connector quiet and when they
release it.

### Derivations

Terminal `Flushed { more: false }` marks the boundary. A connector that changed
its disk must flush that state before returning the response. It must not write
again until the runtime sends the next request.

`Flushed { more: true }` is not a boundary because another flush iteration is
part of the same transaction.

`derive-sqlite` uses its recorded SQLite VFS and does not use a connector disk.

### Materializations

This design supports runtime-authoritative materializations.
`C:StartedCommit` confirms that the connector processed all preceding `C:Store`
requests. The connector flushes prior disk changes before returning
`C:StartedCommit` and does not mutate the disk while handling `C:StartCommit`.

The runtime sends `Publish` after `C:StartedCommit` and before `Persist`. The
connector remains quiet until `Acknowledge`. `Acknowledge` releases
the connector after the recovery-log commit and is ordered before every
connector request for the next transaction. The runtime does not need to wait
for `Acknowledged`.

Remote-authoritative materializations do not have the required runtime commit
boundary and are not supported.

### Captures

A capture emits documents and checkpoints continuously. The runtime can choose
a closing checkpoint after the connector has already moved on. The disk cut
occurs after that close. Without additional connector discipline, the disk can
therefore be newer than the checkpoint committed in the same transaction.

A newer disk is unsafe for resume-critical state. On restart, the connector
could see work recorded on disk that produced documents which never committed,
then skip those documents.

A capture's explicit-acknowledgement protocol provides the required boundary.
A connector that sets `explicit_acknowledgements` in `Opened` receives an
`Acknowledge` describing how many preceding checkpoints committed. If the
connector makes no disk writes between emitting a checkpoint and receiving its
acknowledgement, it is quiet across the runtime's cut. Its disk cannot advance
past the committed checkpoint.

Capture behavior therefore determines the guarantee:

| Connector behavior | Disk guarantee |
| --- | --- |
| No disk writes between a checkpoint and its acknowledgement | The disk is never ahead of the committed checkpoint and may hold resume-critical state. |
| Disk writes while checkpoints are outstanding | The disk is coherent but may represent a later point in connector history. It is suitable only for caches, staged downloads, and scratch data. |

`source-http-ingest` follows the first pattern. It holds each HTTP
response until the documents produced from it are acknowledged.

Captures need no new protocol messages. The runtime cuts and publishes the disk
at the same point it uses for a derivation.

## Lifecycle and recovery

### Durable lifecycle

A disk journal has two durable states:

| State | Meaning |
| --- | --- |
| No committed Flow disk state | The journal has no acknowledged delta and the recovery log has no `AI:`. An absent journal is normal. Any data records are orphans from a failed first use. |
| Committed Flow disk state | The journal has an acknowledged delta or the recovery log has an `AI:` for a pending delta. Recovery first repairs each acknowledgement and then rebuilds the image. |

The first committed delta contains the allocated `mkfs` and mount output, so it
cannot be empty. No separate initialization marker is required.

```text
fresh local image
       │ first disk-changing Flow commit
       ▼
committed disk journal
       │ later deltas and horizons
       ▼
new committed states with an advancing recovery floor
```

### Fresh startup and first commit

With no acknowledged delta and no recovered `AI:`, `Open`:

1. Creates a fresh sparse image.
2. Creates a `ublk` device over it.
3. Formats the device.
4. Mounts the filesystem at a daemon-owned path.
5. Chooses epoch `E` and reads the current author value `R` without claiming
   it.
6. Returns the mount path.

The daemon creates no journal append until the first disk transaction. On that
first transaction it:

1. Applies the supplied `JournalSpec` if the journal is absent.
2. Claims the author register with the deferred `R` to `E` fence.
3. Has already appended every mutation of the transaction, including filesystem
   initialization and mount output.
4. Returns the acknowledgement to be included in `Persist`.
5. Appends the acknowledgement only after the runtime sends `Commit`.

The first data publication is on the transaction's critical path because no
older committed disk can recover that transaction.

A failure before `Persist` can leave a fence and unacknowledged data records.
They are not committed disk state. The next startup ignores them and creates a
fresh image.

### Startup with committed state

When the journal contains an acknowledged delta or `Open` carries a recovered
`AI:`, `Open`:

1. Chooses epoch `E`, reads `R`, and claims the journal.
2. Appends every recovered `AI:` acknowledgement exactly and waits for its
   barrier.
3. Selects a fixed replay range and rebuilds a fresh sparse image.
4. Creates a `ublk` device over the rebuilt image.
5. Mounts the filesystem and returns its path.

The runtime then places the directory in the sandbox and opens the connector.
This sequence also repairs the crash window in which the first `Persist`
committed but its disk acknowledgement did not reach the journal.

A startup with neither acknowledged state nor a recovered `AI:` does not mutate
an orphan journal. It only reads the author value needed for a later first-use
fence.

### Selecting the replay range

After fencing and acknowledgement repair, the daemon obtains a broker-confirmed
write head `H`. `H` fixes the end of this recovery attempt.

The runtime supplies `estuary.dev/truncated-at` as the floor hint. The daemon
resolves its message clock to offset `O` and reads `[O, H)`. If the label is
absent, it starts at the first available fragment.

The floor controls the seek position, not record filtering. A seek that starts
early causes extra replay. Filtering by clock could remove a record from the
middle of a delta and is not allowed.

The connector is not yet open, the new fence excludes the previous writer, and
all recovered acknowledgements are before `H`. Later appends do not affect the
fixed range.

Recovery uses a disk-specific reader for one journal. The append barrier allows
at most one unacknowledged delta, so spill storage only needs to hold one delta.

### Replay rules

The reader makes one forward pass through the range:

1. Validate fence records without changing the image.
2. Use Gazette `message.Sequencer` behavior to group records by producer and
   clock, remove duplicate UUIDs, and release only acknowledged transactions.
3. Spill delta chunks until their acknowledgement arrives. Applying
   unacknowledged chunks directly would require an undo image.
4. Apply acknowledged deltas in physical journal order. The live append
   barrier makes this the commit order.
5. At `opens_horizon`, copy the current allocated bitmap into the horizon
   bitmap before applying that record's chunks.
6. Clear horizon bits as chunks at or after the horizon are applied. When the
   bitmap becomes empty, record that horizon's offset as the new floor.
7. Accept later horizons in the range. Each one replaces the previous horizon
   snapshot.
8. Treat a new session producer as abandoning an older producer's
   unacknowledged delta. A later acknowledgement of an abandoned delta is an
   ordering error.
9. Allow the range to begin within an older delta. Records before the floor are
   unnecessary because the completed-horizon invariant places a copy of every
   allocated block at or after the floor.
10. At `H`, require every acknowledged delta to have been applied and delete
    any remaining unacknowledged spill.

Data chunks are written into a fresh sparse image. Punch chunks hole-punch their
ranges. Applying both kinds rebuilds the allocated bitmap. Replay does not set
tracking of its own beyond the allocated and horizon bitmaps, because it is
reconstructing the committed baseline.

When data bytes end within a block, replay zeroes the rest of that block. Hole
punching uses the same 4 KiB granularity, so this explicit zero fill does not
remove any smaller sparse region.

At the end of replay, the daemon holds the rebuilt image, allocated bitmap,
reconstructed horizon bitmap, and derived floor. `Opened`
returns that floor. If it is ahead of the journal label, the runtime updates
the label.

### Mounting the rebuilt image

The daemon mounts the image with the standard options, and the writes the mount
issues are appended like any other. Ext4 may update its superblock, set recovery
flags, or replay its filesystem journal. These changes are newer than the
replayed baseline and so belong to the next delta.

This produces the filesystem-level recovery guarantee: once mounted, the disk
presents the same files and contents visible at the committed boundary, while
incidental filesystem metadata may differ.

## Security and operations

### Process privileges

The daemon runs as a systemd-supervised machine singleton. It requires:

- Linux 6.0 or later with `ublk_drv` loaded;
- `CAP_SYS_ADMIN` for its mounts and `ublk` devices;
- access to `/dev/ublk-control`;
- seccomp rules for the required `io_uring` operations; and
- owned image and spill directories on a filesystem that supports hole
  punching.

The daemon runs under a dedicated non-root UID with `CAP_SYS_ADMIN` ambient.
It does not receive unrelated capabilities such as `CAP_SYS_MODULE`,
`CAP_NET_ADMIN`, or `CAP_DAC_OVERRIDE`. `CAP_SYS_ADMIN` is still broad, so the
daemon must remain small and single-purpose.

The reactor runs as a non-root container. It needs only:

- the daemon's Unix domain socket;
- `/dev/kvm` when it starts a `libkrun` sandbox; and
- seccomp rules for the required KVM operations.

The reactor never accesses `ublk` or performs a mount. It receives a directory
and re-exports it to a sandbox. It has no `CAP_SYS_ADMIN`, `CAP_MKNOD`, host
mount propagation, broad device access, privileged-container mode, or rootful
Podman socket.

The daemon and VMM are separate processes. A sandbox receives only its mounted
directory and connector root filesystem. It does not receive the image file
descriptor, `ublk` control device, spill directory, or journal credentials. No
trusted component runs as root inside the guest.

### Device isolation

`UBLK_U_CMD_ADD_DEV` assigns an ephemeral device number. The host creates
`ublkcN` and `ublkbN` inside the daemon's device namespace. No other process
receives those nodes, and durable state never refers to their numbers. No udev
rule or device helper is required.

Dropping a session stream unmounts its filesystem and removes both device
nodes. Keeping the daemon and VMM in separate processes prevents ublk teardown
from depending on a client served by the daemon itself.

### Host filesystem parsing

The host kernel mounts ext4 data that a connector can influence. The relevant
threat is a crafted filesystem image that exploits kernel parsing of on-disk
structures. A connector does not have the block-level access required to author
those structures directly. It can only use file operations such as `open`,
`write`, `rename`, `setxattr`, and `link`. The host ext4 driver encodes the
resulting metadata.

The host kernel therefore parses filesystem metadata written by the same host
kernel. Three exposures remain:

- A connector can issue legal but hostile file-operation patterns, including
  deep directory trees, many hard links, large attributes, severe
  fragmentation, and rename storms. This is the same class of exposure as a
  writable container volume.
- A `libkrun` sandbox adds `virtiofsd`, which translates guest file operations
  into host calls. This is the same interface used for other `libkrun` OCI
  volume mounts.
- Every recovered image represents a power-loss point, so ext4 journal replay
  runs on every session recovery rather than only after a machine failure.

## Implementation addendum

What this document says that the daemon does not, recorded as the daemon was
built. The planned departures are tabulated in
[the phase plan](block-backed-connector-disks-phases.md#departures-from-the-design-doc);
this section is what *implementation* changed, and both should be folded into
the text above before the feature ships.

**Owners are not pooled.** [Disk ownership](#disk-ownership) says one owner
thread serves many disks. It cannot: `ublk` binds a device's queue to the thread
which issues its first `FETCH_REQ` and rejects every later command for that queue
from any other thread with `EINVAL`. So each disk has a thread and an `io_uring`
of its own. This was tried the other way and the trace is unambiguous. The cost
is a kernel stack, 256 KiB of reserved address space, and about 12 KiB of ring
per disk, so a hundred disks cost single-digit megabytes. What does *not* scale
that way is the kernel's own `io_uring` worker pool, so every disk's ring attaches
to one anchor ring and that pool's size is registered rather than inherited.

**Nothing is spilled.** [Replay rules](#replay-rules) has the reader spilling a
delta's chunks until its acknowledgement arrives. The reader applies every delta
as it reads it instead, and notes the clock range of any delta still
unacknowledged when the pass reaches `H`. If there are any, the image is reset
and the range is read again, reading over exactly those records. One extra pass
always suffices, because a producer's unacknowledged delta is its last. This
costs one extra read of the range in exactly the case where a session did not
shut down cleanly, and it removes a spill file along with its memory threshold,
its disk budget, the `fsync` and validation a spill surviving a crash would need,
and the shutdown deadlock a spill budget's backpressure invites.

An unacknowledged delta is also not always the last thing in the range: a
replacement session fences and appends *after* the orphan records of the session
it displaced. Skipping by producer and clock covers the trailing and mid-range
cases with one rule.

**Format output is not retained.** [Format and mount](#format-and-mount) has the
daemon retaining the writes a `mkfs` and mount make. It drops them, and the first
append instead asks the disk's owner for a snapshot of the image: the allocated
blocks, read back and encoded as chunks. The image already holds exactly that
content, so nothing has to be kept; a block written twice while formatting
collapses to one chunk; and the snapshot is taken in bounded batches, so what a
session holds does not scale with the size of the disk it serves.

The apparent race is benign. A mutation is captured before it is applied, so the
owner may already have applied one when the snapshot is taken. Replay starts from
an empty image and every mutation captured since the mount is appended after the
snapshot, so one already reflected in it is simply applied again.

**Block size is per disk, not 4 KiB.** [Change tracking](#change-tracking) fixes
4 KiB throughout. The daemon parameterises on a block size the session supplies
at `Open`, which is durable for the life of the disk because it shapes chunk
coverage and bitmap extent. `u32` block indexing caps a device at 2³² blocks
rather than at 17 TiB.

**`ublks_max` does not bind these devices.** It counts unprivileged devices only,
and the daemon's are privileged, so no host needs an `/etc/modprobe.d` entry for
this crate's sake. It is still reported, because it is the first thing an
operator reaches for when a device will not add.

**"No udev rule or device helper is required"** ([Device isolation](#device-isolation))
holds only when the daemon runs as root. `CAP_SYS_ADMIN` does not bypass file
permissions, so a dedicated UID needs a udev rule granting it `/dev/ublk-control`
and `/dev/ublkc*`.

**A session which is over gives up its broker calls.** [Errors](#errors) says
broker errors are retried internally. They are, until the session ends: a
teardown which waited on an unreachable broker would hold that disk's device and
its mount for as long as the outage lasted, and a draining daemon would leave
both behind. A `Broker` replacement likewise does not pass through the journal
writer, which may be retrying against the very broker being replaced.

**Every append is confirmed before the next is issued.** The daemon issues one
append at a time and awaits it, so "every chunk of this delta is confirmed" is
the same statement as "the writer has no work". There is therefore nothing for an
appended-versus-confirmed measure to report, and the metrics surface has none.

## Remaining work

Two lifecycle areas must be specified before the feature is complete:

- **Local cleanup.** Define cleanup after daemon or machine crashes, removal of
  orphaned image and spill files, local-capacity enforcement, and cleanup after
  task deletion. Normal session teardown unmounts the filesystem,
  deletes the `ublk` device, and closes the anonymous image.
- **Shard splits.** Define how a child shard receives its initial disk state and
  whether tasks with committed connector disks can split. Until this is
  specified, live splits of those shards are unsupported.
