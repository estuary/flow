# Block-Backed Connector Disks

## Decision summary

Each live connector shard gets a private 10 GiB POSIX filesystem at a stable
path. The disk advances atomically with Flow transactions: after a failure, it
is rebuilt to the filesystem state associated with the last committed
transaction.

Every connector block write goes through the disk daemon, which records the
blocks that change. At a transaction boundary the daemon copies those blocks
into a per-shard Gazette journal. Each delta also carries a small number of
blocks that did not change, and these move the start of the necessary replay
range forward. Recovery therefore does not replay an unbounded history.

The daemon observes every block write through Linux `ublk`, which sits between
the host filesystem and the daemon. This is an implementation detail and is not
part of the durable format.

The derivation connector protocol fits naturally with coordinating the necessary
disk quiescense periods for snapshots to be recorded. Runtime authoritative
materializations also work well with the existing protocol, but not remote
authoritative ones. Captures are complex as well. This design describes support
for derivations, runtime-authoritative materializations, and captures.

### Design boundaries

**Connector calls.** Every live capture, derivation, and materialization shard
gets a directory at a stable path inside its sandbox before the connector opens.
The daemon formats and mounts the filesystem behind that path. There is no
opt-in or capability negotiation. Validation, discovery, `Apply`, and other
non-transactional connector calls do not get a disk because their filesystem
changes cannot be tied to a Flow commit.

**Capacity.** Every disk is 10 GiB. This is a platform constant rather than a
catalog or connector setting because the compaction, spill, and local-capacity
assumptions below depend on a small known maximum. It is a number largely picked
out of the air as "not too big, but big enough to be useful".

**Platform.** The daemon requires Linux 6.0 or later with `ublk_drv`. It is a
privileged component: mounting ext4 needs `CAP_SYS_ADMIN`, which also covers
`ublk` device creation. The reactor needs only the daemon's socket, plus
`/dev/kvm` when it uses a `libkrun` sandbox, and is unprivileged. Either a
`libkrun` guest or a plain container can consume the mount, so the design does
not depend on which one a connector gets.

**Guarantee.** Recovery reproduces the filesystem contents seen at the
committed boundary. It does not promise a byte-identical image because mounting
and unmounting change filesystem bookkeeping.

**Shard splits.** TBD. Options here may include copying the parent disk in child
shards, or simply not allowing live splits and requiring the number of shards
for a task to be specified at creation. It will almost certainly not be possible
to "merge" split shards once they have written to their disks.

## Disk daemon

The capability described here is not specific to Flow. "A block device whose
state advances atomically with an external commit, backed by a journal" is a
general-purpose primitive — roughly litestream generalized from SQLite to whole
devices, and with journal latency in place of object-store latency. It is worth
building as a stand-alone service rather than as runtime internals, and it may
be worth an open-source push.

The disk daemon is a machine singleton. It provides the `ublk` io_uring service
and a gRPC interface over a unix domain socket. There is one daemon and one
reactor per machine.

Two properties of the design make this extraction clean, and both are worth
protecting:

- The commit obligation is **opaque** to the caller. The runtime stores bytes
  and returns them without interpretation.
- The recovery floor is a **hint** and not an authority. The caller can pass it,
  omit it, or pass a stale one, and the daemon rebuilds the same disk.

If either were otherwise, the daemon would have to understand what a Flow
transaction is, or what a Flow label means.

### Division of responsibility

Most of this document describes the daemon: the image, the bitmaps, the device
gate, copy before overwrite, the journal format, horizons, and recovery.

These four things stay with the runtime:

| Runtime responsibility | Reason |
| --- | --- |
| Connector protocol timing | The daemon cannot know when a connector is quiet |
| `AI:` acks | The commit is the runtime's, in its own recovery log |
| `estuary.dev/truncated-at` | A Flow label name does not belong in a general service |
| The `JournalSpec` | Deriving it from a task template is catalog knowledge |

A rule that keeps the boundary honest: anything named `estuary.dev/` stays in
the runtime.

### Session protocol

One device is one session, and one session is one bidirectional stream. The
device lives for exactly as long as the stream. A dropped stream unmounts the
filesystem and destroys the device, which is what stops an orphaned process from
writing to a journal it no longer owns.

A session hands back a **mounted directory** rather than a block device node.
The daemon owns `mkfs`, the mount options, and every filesystem tuning decision.
A caller receives a path it can use, and the sandbox it uses is its own
business.

| Request | Reply | Purpose |
| --- | --- | --- |
| `Open` | `Opened` | Create or recover a device |
| `Cut` | `Boundary` | Establish a boundary and retain it locally |
| `Publish` | `Published` | Publish one delta and return its ack |
| `Commit` | `Committed` | Append an ack the caller made durable |
| `Credentials` | — | Replace the broker credential |

`Open` carries the device size, the `JournalSpec`, the broker address, a
credential, every recovered ack, and an optional floor. `Opened` returns the
mount path and the floor that the daemon derived.

`Opened` reports nothing about whether the disk was fresh. The daemon formats a
fresh disk itself, so a caller has no decision to make.

The daemon applies the `JournalSpec` only if it must append and the journal is
absent. Journal creation therefore stays lazy: a connector that never writes its
disk never gets a journal.

`Publish` with no boundary cuts and publishes together, which is the common
case. `Published` carries no ack when the disk did not change.

`Committed` carries a floor only when that commit completed a horizon.

The daemon never sends a message that does not answer a request, and it does no
work between calls. The stream is a lifetime scope rather than a channel for
events. A terminal condition ends the stream instead, including a local image
failure while the device is idle.

### Order of operations

At startup the caller must:

1. Acquire and recover the shard recovery log.
2. Read the `estuary.dev/truncated-at` label from the journal.
3. Send `Open` with every recovered ack and that floor.
4. Give the returned mount path to the sandbox, over `virtio-fs` for a
   `libkrun` guest or as a bind mount for a container.
5. Open the connector.

At each boundary the caller must:

1. Bring the connector to a boundary, as [Task boundaries](#task-boundaries)
   describes.
2. Send `Publish`.
3. Stop here if the reply carries no ack.
4. Record the ack as `AI:` in the same commit as the checkpoint.
5. Send `Commit` with that ack, after that commit is durable.
6. Apply the label if `Committed` returns a floor.
7. Release the connector.

Captures send `Cut` at each disk sync point. They then send `Publish` through
the boundary that matches the closing checkpoint. Derivations and
materializations never need `Cut`.

### Rules the caller must obey

- Hold only one ack. Send `Commit` before the next `Publish`. A second `Publish`
  is a protocol error and ends the session.
- Send `Commit` only after the ack is durable in the recovery log.
- Return the exact ack bytes. Do not serialize them again.
- Keep the connector quiet from the boundary until release.

`Cut` is legal while an ack is outstanding, because it appends nothing.
`Publish` is not.

That asymmetry is not arbitrary. A capture connector keeps running after a
transaction closes, so a disk sync point can arrive while the previous ack is
still outstanding — and the runtime never declines a requested sync. Forbidding
an overlapping `Publish` also keeps the candidate rule in
[Horizons](#horizons) to two terms rather than three: with no in-flight delta to
account for, the daemon never has to exclude blocks it has published but not yet
committed.

### What each side promises

The daemon promises **coherence**. What it publishes is a valid point-in-time
state of the device.

The caller promises **alignment**. The boundary is at a connector checkpoint.

The daemon keeps its promise even when a connector breaks the caller's. The disk
is then still valid, but it can hold more state than the checkpoint, or less.

### Errors

The daemon retries a broker failure by itself and does not report it.

Any error that reaches the caller is terminal. The session is over, and the
caller must tear down the shard.

A fence failure means that another process owns the shard and holds the journal.


## Correctness model

The local image is a disposable working copy. The durable disk state is the set
of committed deltas in the disk journal.

A **delta** is the set of chunks that one transaction commits. It carries
every block that changed at that boundary, and it may carry some blocks that
did not — extra copies the daemon includes to move the recovery floor forward.
[Horizons](#horizons) explains why.

Nothing in the encoding separates the two. An extra copy is an ordinary data
chunk, so a reader treats every chunk identically, which is what keeps replay
simple.

The **recovery floor** is the offset of the first record that recovery must
read. Recovery calculates the floor from the journal contents. The
`estuary.dev/truncated-at` journal label holds the floor as a message clock.

The label is a bookmark rather than an authority, and that distinction carries
more weight than it first appears. The floor is a fact about what the journal
contains, so any reader rederives it from a replay it must perform anyway. A
label that is absent, or behind the true floor, costs replay work and nothing
else.

The recovery log carries one kind of disk state:

- `AI:{journal}` is the exact serialized acknowledgement record that a
  committed transaction still requires the daemon to append.

`AI:` makes post-commit publication recoverable. A delta without an
acknowledgement is not committed.

### What the connector must do

The daemon can tell whether blocks changed and can capture a coherent image
while writes continue. It cannot tell whether a connector has finished an
application-level transaction or flushed its own buffers.

At a boundary where its disk changed, a connector must:

1. `fsync` the state that belongs to the boundary;
2. report the appropriate protocol response; and
3. avoid further disk writes until the runtime signals that the boundary has
   been taken.

A connector that skips `fsync` gets power-cut semantics for its own disk. A
connector that writes during the quiet interval can put its disk ahead of its
checkpoint. Neither the runtime nor the daemon can detect either violation,
because connector messages and block I/O are independently buffered.

The connector contract aligns the disk snapshot with the connector checkpoint.
The daemon produces a coherent snapshot even if the connector violates that
contract, but the snapshot may then be missing expected state or include state
newer than the checkpoint.

## Components and durable state

The block store for each live shard incarnation has four local data
structures:

- a sparse image file;
- a dirty bitmap;
- an allocated bitmap; and
- a horizon bitmap.

If the disk has ever committed, it also has one per-shard Gazette journal.

### Sparse image

The image is a sparse file created with `O_TMPFILE` and sized to 10 GiB with
`ftruncate`. It never has a directory entry. Byte *N* of the file is byte *N*
of the block device.

The image starts with no allocated host blocks. A connector using 200 MiB
therefore consumes about 200 MiB rather than 10 GiB. The daemon treats the
contents as opaque bytes. It never parses superblocks, inodes, or directory
entries.

Keeping the image as one ordinary file allows the daemon to use normal file
operations for block reads, writes, checksums, hole punching, and final
cleanup.

### Change tracking

The daemon keeps three bitmaps. Each bitmap has one bit for each 4 KiB block. A
10 GiB image has 2,621,440 blocks, so each bitmap is 320 KiB. The bitmaps are
arrays of atomic words. The write path sets the necessary bits before it issues
the image write.

The **dirty bitmap** answers this question:

> Did this block change after the last transaction boundary?

At a boundary the daemon replaces this bitmap with an empty bitmap. The old
bitmap becomes the block set for the delta.

The **allocated bitmap** answers this question:

> Does this block occupy space in the image now?

The allocated bitmap gives the initial content of the horizon bitmap. It also
gives placement an exact measure of physical use. A write sets allocated bits. A
successful aligned discard clears them.

The **horizon bitmap** answers this question:

> Is the newest copy of this block before the current horizon?

The daemon fills this bitmap when it starts a horizon. Deltas clear its bits.
The bitmap is empty when no horizon is open.

The bitmaps are independent: a block can be allocated without having changed
since the last boundary.

### Block transport

The block path is:

```text
connector process (unprivileged)
                │ ordinary file I/O
                ▼
   virtio-fs  ──or──  bind mount
                │
                ▼
    ext4 filesystem (host kernel)
                │ the daemon's mount point
                ▼
          /dev/ublkbN
                │ ublk_drv
                ▼
        ublk server (daemon) ──► sparse image
```

The daemon formats `/dev/ublkbN` when it is fresh and mounts it at a path the
daemon owns. It then hands that path to the caller. Both `ublkcN` and `ublkbN`
stay inside the daemon; no device node reaches another process.

A `libkrun` guest receives the path over `virtio-fs`. A plain container receives
it as a bind mount. The connector sees an ordinary directory either way, and
reaches the filesystem only through file-level calls.

`/dev/ublkbN` is created for each device; it is not a persistent identifier:

1. Loading `ublk_drv` exposes the host-wide `/dev/ublk-control`.
2. The daemon sends `UBLK_CMD_ADD_DEV` and asks the kernel to choose `N`. Host
   devtmpfs and udev then create `/dev/ublkcN`, the character device the daemon
   serves.
3. The daemon sets the device size and queue limits and starts its I/O queues.
   `UBLK_CMD_START_DEV` then exposes `/dev/ublkbN`, the block device the daemon
   mounts.
4. Teardown stops the block device and deletes the character device. The kernel
   may reuse `N` immediately.

The daemon holds `/dev/ublk-control` directly and both device nodes stay in its
own namespace. See [Privilege and isolation](#privilege-and-isolation).

Everything below remains independent of this transport:

- the image and bitmaps;
- copy-before-overwrite capture;
- the disk-log format;
- commit coordination;
- compaction; and
- recovery.

Three rules keep the transport out of everything durable:

- the durable format never contains a mount path, device identity, or mount
  option;
- the daemon never interprets the connector filesystem; and
- point-in-time capture never depends on a filesystem freeze.

The host filesystem that stores images must support hole punching.

### Disk journal

The disk journal is created lazily on the first transaction that needs durable
disk state.

Its `JournalSpec` sets the `SNAPPY` compression codec, matching recovery logs.
A connector writes whatever it likes to its disk, so the payload may be text or
may be already-compressed archives. A codec that passes incompressible input
through cheaply is the right default when the input is unknown. Compression also
runs in the primary broker's spool and is paid on every append, so per-byte cost
matters more here than ratio does.

Dropping zero tails does more for volume than the codec choice does, because it
removes those bytes from the append and from replication as well as from
storage. Gazette compresses only when the primary spools a fragment, so appends
and replication to peers carry uncompressed bytes.

Journal offsets stay uncompressed byte positions, so the codec does not affect
the recovery-range arithmetic. Fragments record their own codec and readers
dispatch per fragment, so this choice can change later without a migration and
without touching the format.

Records use Gazette's fixed Protobuf framing and one disk-specific message:

```text
DiskRecord
┌────────────────┬────────────────────────────────────────────────┐
│ chunks         │ zero or more chunks                            │
│ opens_horizon  │ true only on the first record of a transaction │
│ installs_epoch │ present only on a fence record                 │
└────────────────┴────────────────────────────────────────────────┘
```

The Gazette UUID of each record supplies the producer, the clock, and the
transaction flag. There is no separate begin record and no separate end record:

- A **fence** record is `OUTSIDE_TXN`. It sets `installs_epoch`, carries no
  chunks, and does not change disk state.
- A **delta** is one or more `CONTINUE_TXN` records and one `ACK_TXN` record.
  The `ACK_TXN` record commits the delta and carries no chunks.

The acknowledgement is the only terminator a delta needs. A journal has no
interior gaps, so a reader that finds the acknowledgement holds every record of
that delta. The copy loop must find its block set empty before it appends the
acknowledgement; that local assertion catches an omitted chunk at the point
where the bug happens, which a count in the journal cannot.

A record that sets `opens_horizon` must be the first record of its transaction.
Any other position is a protocol error, for the reason
[Horizons](#horizons) gives.

A chunk has this form:

```text
Chunk
┌───────────────┬───────────────────────────────────────────────────┐
│ block: u32    │ starting block index                              │
│ one of                                                            │
│   bytes       │ content, from that block forward                  │
│   punch: u32  │ this many blocks were discarded                   │
└───────────────┴───────────────────────────────────────────────────┘
```

A block index rather than a byte offset makes 4 KiB alignment impossible to
misencode, and indexes the bitmaps with no arithmetic. A 10 GiB disk is
2,621,440 blocks, and a `u32` covers 17 TiB at this block size.

A data chunk carries no length. Protobuf already delimits `bytes`, and a second
copy of the same fact is a second thing that can be wrong.

The length of `bytes` need not be a whole number of blocks. Block contents
often end in zeroes — a file's last block, a directory block with a few entries,
an inode table block with mostly unused entries — so the daemon scans backward
for the zero tail and drops it. The chunk then covers `ceil(len(bytes) / 4096)`
blocks, and the tail of the last one is zero.

> A reader must write those zeroes explicitly. It cannot rely on the image being
> sparse, because a block rewritten by a later delta would otherwise keep the
> earlier delta's tail.

That trailing zero is the trap in this encoding. It only shows up on blocks that
are written more than once, so it survives casual testing.

A data chunk with empty `bytes` means the block is allocated and entirely zero.
That is a real state, and it differs from a punch, which means the block is not
allocated at all. Replay zeroes the first and hole-punches the second.

At a boundary, a dirty block that remains allocated becomes a data chunk. A
dirty block that is no longer allocated becomes a punch. An unchanged block that
the daemon copies to advance a horizon is an ordinary data chunk, with nothing
to mark it apart.

Chunks carry no checksum. A checksum can only detect corruption between the
moment it is computed and the moment it is verified, and TLS and Gazette's
fragment content sums already cover that span. It would not detect a wrong
block index, a mis-coalesced run, or a stale buffer, because each of those
produces a self-consistent chunk. The safeguards that do catch those are
structural: the copy loop must find its block set empty before it appends the
acknowledgement, chunks in one delta never overlap, and a horizon completes only
when every allocated block has been covered.

Chunks in one delta never overlap, but they are not guaranteed to be sorted.
Concurrent writes can force a block ahead of the copy cursor to be emitted
early. Readers must accept this order and rely only on non-overlap.

Records and append batches are bounded. Gazette serializes appends to a
journal, so one enormous append could otherwise block a later delta for an
unbounded time.

### Record groups

Gazette gives every record a producer ID and a clock, so the disk journal does
not add a separate stream ID:

- A shard session uses one producer for all its deltas. A replacement session
  uses a new producer.
- Each fence record uses its own producer and carries the shard-session
  producer that it installs as the writer epoch.

Readers follow Gazette `message.Sequencer` semantics: the sequencer groups
records by producer and clock, discards duplicate UUIDs, and releases only
acknowledged records.

### Recovery-log authority

The task recovery log commits the checkpoint, connector state, and disk
obligations together:

```text
                        transaction boundary
                                 │
                                 ▼
connector ──► guest ext4 ──► virtio + ublk ──► image
                                                  │ dirty bitmap
                                                  ▼
                                            disk journal
                                                  │ replay
                                                  ▼
connector ◄── guest ext4 ◄── rebuilt image ◄──────┘

                        recovery log
                 ┌────────────────────────┐
                 │ AI: exact pending Ack  │
                 │ checkpoint and state   │
                 └────────────────────────┘
                        decides authority
```

For derivations and materializations, the Shuffle Leader includes every
shard's disk obligations in one recovery-log `Persist`. Each disk-owning shard
then appends to its own journal. Captures perform the same coordination inside
their local transaction FSM.

This is why `AI:` exists at all. Derivations and materializations have exactly
one recovery log — shard zero's — while every shard has its own disk journal.
The commit and the disk acknowledgements are therefore unavoidably different
appends to different journals, and something has to bridge them. That follows
from the shard topology rather than from anything about the disk format.

### One writer per journal

Acquiring a shard recovery log prevents an old primary from committing another
`Persist`, but it does not by itself stop the old process from appending to the
separate disk journal. Each journal therefore has an `author` register.

When the daemon opens a device, it:

1. chooses a fresh epoch `E`;
2. reads the current `author` register once and remembers it as `R`; and
3. never refreshes `R`.

Claiming the journal appends `Fence(E)` while atomically changing `author` from
the remembered value `R` to `E`. The daemon waits for broker confirmation before
any other journal operation.

The epoch is entirely daemon-internal. The runtime cannot validate an ack it
does not parse, so it plays no part in fencing.

The one-time comparison matters. A stale session might reach its first disk
write after a replacement has taken over. If it re-read the register, it could
overwrite the replacement's epoch. It may attempt only the transition observed
at startup.

The claim happens at different times depending on journal contents:

- If the journal exists, the daemon claims it before repairing `AI:`, fixing a
  recovery range, replaying, or returning from `Open`.
- If the journal does not exist, the daemon takes only the read-only snapshot
  `R`. Journal creation and the deferred claim happen on first use.

An ambiguous fence append is retried idempotently as a check for `E`, not as a
new attempt to take ownership. A register mismatch ends the session. Every
later append checks `author=E`; no ordinary appender responds to a mismatch by
taking the register back.

Journal registers fence cooperative writers but do not become commit
authority. A lost register set can satisfy a comparison against absence, so
`AI:` and the committed journal contents remain the final authority even when
register state is empty.

Gazette orders the fence with appends. Old content before the fence matters
only if it was already committed; an old append ordered after the fence fails
its `author` check.

## Horizons

Without compaction the journal and the recovery time grow without bound. The
daemon moves the recovery floor forward with **horizons**.

A **horizon** is a point in the journal. The rule a horizon must satisfy is:

> Every allocated block has a copy at or after the horizon.

When a horizon satisfies this rule the horizon is **complete**, and the
recovery floor can move to it.

### The horizon bitmap

When the daemon starts a horizon it copies the allocated bitmap into the
horizon bitmap. A set bit shows that the newest copy of that block is before
the horizon.

Two things clear a bit:

- The connector writes the block. The delta publishes it after the horizon, so
  the bit clears at no extra cost.
- The daemon copies the block into a delta even though it did not change.

Bits clear when the delta **commits**, never while the copy runs. Clearing a bit
earlier would claim a block was covered by a copy that a failed transaction then
discarded. A failed transaction therefore leaves the bitmap untouched, which is
exactly what a replaying session derives, since it discards that delta too.

Nothing needs to change during the copy. The candidate set already subtracts the
blocks in this delta, so the daemon cannot re-select one of them.

A horizon consequently never completes in the middle of a transaction.

Bits never become set again during a horizon. The horizon bitmap therefore only
decreases, and a horizon always completes. A horizon that makes no progress
loses nothing and continues later.

The daemon finds blocks to copy with a cursor that moves forward through the
horizon bitmap. Bits behind the cursor are always clear, so a horizon completes
after exactly one pass of the cursor.

The snapshot need not be exact, which is part of what keeps this cheap. The
daemon takes the live allocated set while a reader takes the committed
allocated set at the same offset, and the two can differ. Each difference
resolves itself:

- A block the daemon holds and a reader does not is a block with an
  unpublished write. A later delta publishes it and clears the bit.
- A block a reader holds and the daemon does not is a block a later delta
  discards. That delta records a punch, which clears the bit.

An inexact snapshot therefore costs a little redundant work and never produces
a wrong result.

### Which blocks to copy

The daemon reads these blocks from the local image and never from the journal.
The image is local and hot, while the oldest fragments of a journal are the
ones most likely to have been offloaded to cloud storage.

The daemon must select only blocks whose current image content is already
committed. This is the one rule in the design that can corrupt a disk without
raising an error: if a block carries an unpublished change, its current content
belongs to a later transaction, and publishing it now puts the disk ahead of
its own checkpoint.

At the cut, the candidate set is:

> the horizon bitmap, minus the blocks in this delta, minus the blocks with
> unpublished changes.

For derivations and materializations the live dirty bitmap is empty at the cut,
so the last term is empty. For captures the set is larger, as
[Captures](#captures) describes.

If a mutation targets a selected block during the copy, the daemon drops it
from the delta and does not capture the old bytes. The mutation publishes the
block in the next delta, and that delta clears the bit. This is why copy before
overwrite does not extend to these blocks, and why no block is ever owed by two
copies at once.

### The horizon record

The first record of a delta can set `opens_horizon`. The horizon is at that
record's offset.

A record that sets `opens_horizon` must be the first record of its transaction.
The reason is worth stating, because a violation loses data silently. A reader
holds a delta's chunks until the acknowledgement, so it takes its horizon
snapshot before applying any chunk of that delta. If the flag sat on a later
record, the reader would go on to apply the earlier chunks of the same delta
and clear their bits — while a reader that started at the horizon would never
see those chunks at all. Those blocks would then have no copy after the
horizon.

The horizon is part of the delta. If the delta does not commit there is no
horizon, and the daemon starts a new one later.

A writer must not start a horizon while a previous horizon is open. This is a
protocol error and ends the session. A reader must accept more than one horizon
in its range, because the label can lag. At each horizon record the reader
takes a new snapshot that replaces the previous one.

### Why the flag lives in the journal

The horizon bitmap is in memory only. A reader rebuilds it during the replay it
must perform anyway:

1. Read from the floor and rebuild the image.
2. At a horizon record, copy the current allocated set into the horizon bitmap.
3. Clear a bit for each chunk applied after that point.

The copies are themselves the record of progress. They are ordinary chunks,
indistinguishable from connector writes, so replaying the journal
reconstructs the bookkeeping for free and a new session resumes a horizon
rather than restarting it.

Without the flag a new session could not locate the horizon. It would start a
fresh one at the write head and discard the previous session's work. That is
tolerable when restarts are rarer than horizons and pathological when they are
not: the floor never advances, the journal grows without bound, and the longer
journal makes startup slower on a shard that is already unhealthy.

### Policy

The quota for one delta is proportional to that delta's changed bytes:

> unchanged bytes copied = `k` × changed bytes

A commit that does not change the disk appends no records and pays nothing. The
fast path is unchanged.

The daemon starts a horizon when the journal range from the floor to the write
head exceeds `r` times the live allocated size, subject to an absolute floor of
1 GiB. The absolute floor prevents constant horizons on a small disk.

The two constants are independent:

- `k` sets write amplification, which is `1 + k`, and how quickly a horizon
  completes.
- `r` sets how large the journal grows before a horizon starts.

The policy is `k = 0.5` and `r = 2`, which bounds the recovery range at roughly
five times the live allocated size. These are platform constants and not user
settings.

A connector that rewrites much of its disk pays almost nothing, because its own
writes clear bits. The work scales with how much of the disk is genuinely
static, rather than with how large the disk is.

If the connector stops writing its disk, an open horizon stalls. The journal is
not growing either, so the recovery range freezes rather than deteriorating.
Nothing gets worse while the disk is quiet, and the horizon resumes when writes
resume. A lower `r` bounds the size at which a horizon can freeze, and costs
nothing in write amplification.

### Completion

The floor moves when the horizon bitmap becomes empty:

1. A delta commits, and clearing its blocks empties the daemon's horizon bitmap.
2. The daemon waits until the broker confirms that delta's acknowledgement.
3. The daemon returns the floor to the runtime in `Committed`. The floor is the
   message clock of the record that set `opens_horizon`.
4. The runtime applies the `estuary.dev/truncated-at` label with that clock.

Step 2 is a correctness requirement. If the daemon reported the floor at append
time and the transaction then failed, the label would point past a block whose
only copy lies behind it.

Nothing is written to record completion. Every reader arrives at the same
completion point from the same records, so there is nothing to announce. A
completion record would also be a claim a reader could not verify, where a
derived one cannot be wrong.

The label update is idempotent and monotonic. The runtime retries it at startup
and during normal operation. A failed update costs nothing, because the next
session observes the same completion and applies the label then.

The label may lag the true floor but must never lead it. Physical fragment
deletion is handled separately.

### The shared label

`estuary.dev/truncated-at` already exists for capture backfills. Both uses mean
that no reader needs data before that clock, and both obey the same rule that
the label may lag but must never lead. `go/labels/labels.go` already preserves
the label through spec convergence, which is exactly the protection a
runtime-maintained label on a control-plane-managed journal spec needs.

The two uses differ in one respect. A backfill clock is a decision, and nothing
in the collection journal records it, so `AB:{state_key}` must hold it in the
recovery log. The disk floor is a fact about journal contents that replay
rediscovers at every startup. The disk feature therefore adds no key to the
recovery log.

The label documentation and the convergence comment in `go/labels/labels.go`
must describe both uses.

## Lifecycle

The disk journal has only two states:

| State | Meaning |
| --- | --- |
| No committed delta | No disk state has ever committed. An absent journal is normal; any existing records are orphans. |
| One or more committed deltas | The committed deltas are authoritative. |

The first delta always carries blocks, because it holds the `mkfs` output. A
committed but empty state is therefore impossible, which is what makes these
two states unambiguous without a marker in the recovery log.

```text
no committed delta
  fresh image ── first disk-writing commit ──► committed

committed:
  recover ── transactions ──► deltas
              │
              └── horizon ──► floor moves forward
```

### Fresh disk startup

When the journal has no committed delta, `Open` does these steps:

1. Create a fresh sparse image.
2. Create a ublk device over the image.
3. Format the device.
4. Mount the filesystem at a daemon-owned path.
5. Choose `E` and take the read-only author snapshot `R`.
6. Return the mount path.

The runtime then gives that path to the sandbox and opens the connector.

The daemon creates no journal and appends nothing until first use.

The daemon does not clear the dirty bitmap after the format and the mount.
[Mount](#mount) explains why.

If the journal does not exist, `R` means “author absent.” Creating the journal
on first use produces an empty register set, after which the deferred fence can
change the absent author to `E`. If another session fenced first, that
transition fails.

#### Format

The daemon formats only a fresh image. Recovery treats filesystem structures as
payload and never formats them again.

Format and mount decisions belong to the daemon and are not caller settings.
That is much of the point of serving a mount: a caller receives a directory
whose tuning it does not have to know about or get right.

The filesystem is ext4 with:

- a 4 KiB block size, matching the tracking granularity;
- zero reserved blocks;
- ext4's default inode density;
- e2fsprogs 1.47.0 or later with
  `assume_storage_prezeroed=1`; and
- whole-device discard disabled with `nodiscard`.

`nodiscard` applies only while formatting. The mounted filesystem uses
continuous discard as described below.

The new sparse image is logically all zero. `assume_storage_prezeroed` lets
`mkfs` skip writing zeroes across the unused inode tables and internal journal,
and marks the inode tables initialized. Those ext4-reserved ranges therefore
remain holes in the host image, do not enter the allocated bitmap or the first
delta, and do not cause later background initialization writes.

This option earns its place because the first delta sits on the critical path
of the very first commit, and the `mkfs` output is that delta's contents.

The default inode density avoids imposing an unusually low file-count limit for
a small storage saving.

#### Mount

The daemon mounts fresh and recovered disks with the same options:

- `noatime`, so reads do not create deltas;
- `nodev`, `nosuid`, and `noexec`; and
- `discard`, so ext4 releases blocks as it frees them.

A sandbox that re-exports the mount applies its own options on top. A
`virtio-fs` guest mount and a container bind mount must each carry `nodev`,
`nosuid`, and `noexec` as well; the host mount's options do not propagate.

`mkfs` and the first mount write through the served device. The daemon does
**not** clear the dirty bitmap after those writes.

Blocks actually written by `mkfs` and mount are real filesystem content, and no
earlier record holds them. The dirty bitmap therefore carries the full
allocated set at the first boundary, and the first delta contains every
allocated block. Unwritten zero ranges remain sparse and are omitted. The first
delta is a complete image copy, but it needs no special record type and no
special code path to produce one.

The daemon also does not clear the dirty bitmap after mounting a recovered
disk. Mount bookkeeping and filesystem-journal replay are genuine changes from
an existing committed baseline and must appear in the next delta.

One rule therefore covers both a fresh disk and a recovered one: never clear the
dirty bitmap after a mount.

### First use

The first transaction in which a connector changes its disk creates the
initial durable state:

1. The connector reaches a valid boundary.
2. The daemon observes a non-empty dirty bitmap. On a fresh disk that bitmap
   holds the full `mkfs` and mount output.
3. The daemon applies the `JournalSpec` the runtime supplied in `Open`, through
   broker `Apply`, or confirms that the journal already exists.
4. The daemon performs the deferred `R → E` fence.
5. The daemon copies the dirty blocks as bounded records and appends the
   acknowledgement.
6. `Persist` atomically records the checkpoint, connector state, and `AI:`.

This first delta is intentionally on the transaction's critical path. There is
no older disk from which the transaction can recover.

A failure before `Persist` leaves the journal with no committed delta. The
journal may be absent, or may hold a fence and an unacknowledged delta.
Recovery ignores all of it, destroys the uncommitted local image, and starts
from a fresh disk.

Captures begin first use at their first disk sync point rather than at the
later Flow transaction boundary. Their delta ordering is described under
[Captures](#captures).

### Delta capture

Every delta must represent the disk at one exact boundary, even if the
connector resumes writing before its blocks have been copied into the journal.
A delta owes the dirty blocks taken at that boundary.

The connector flushes and stops its own writes before a boundary, but ext4 may
still issue journal or writeback requests. The daemon therefore establishes
the boundary at the block device with one gate shared by every `ublk` queue.
Writes, discards, write-zeroes requests, and flushes enter the gate before
being handled and leave only after their backing operation completes.

To establish a boundary, the daemon:

1. calls `syncfs` on its own mount;
2. closes the gate so that new requests wait;
3. waits for every admitted request to finish;
4. swaps the dirty bitmap, takes an allocated-bitmap snapshot if a horizon
   starts here, and registers the new copy; and
5. reopens the gate.

Step 1 is what makes a served mount safe. A connector's `fsync` reaches the host
through `virtio-fs` or a bind mount, and whether that forces the host filesystem
to write back is not something this design should depend on. If it did not, the
connector's data would still be in the host page cache, the cut would miss it,
and the disk would land behind its checkpoint. A `syncfs` the daemon issues
itself removes the dependency. It is bounded work, because the connector has
normally flushed already.

The bitmap operation in step 4 is the cut. A request is handled entirely before
or after it. Reads need not stop, and ext4 may continue submitting requests
while the gate is closed; those requests simply wait. The gate is held only
while the cut is established, not while the image is copied. It gives priority
to a waiting boundary so continuous writeback cannot starve it.

The ublk target must honor every flush and force-unit-access (`FUA`) write it
advertises, which preserves ext4's ordering across queues. An ext4 journal
operation may span the cut as several block requests, but the resulting image is
a valid power-loss point: recovery either replays a committed journal operation
or discards an incomplete one.

That last property has an operational consequence worth stating plainly. A
published image is never a cleanly unmounted filesystem, so every recovery
mounts a dirty one and runs journal replay. This is within the envelope the
filesystem is built for, but it happens on every session start rather than only
after a machine loses power.

The copy walks the owed-set:

1. Claim the next contiguous owed chunk.
2. Read its current bytes with `pread`.
3. Drop the trailing zeroes and move the chunk into a bounded, immutable
   output buffer.
4. Mark the blocks captured and remove them from the owed-set.
5. Append a record when the output batch reaches its limit, retaining its
   bytes until the append is confirmed.
6. Append the acknowledgement after every block is captured and every record is
   confirmed.

Repeated writes to one block therefore produce a single chunk, and adjacent
dirty blocks coalesce into one. A transaction with 4,000 writes to 2,000
distinct blocks publishes exactly 2,000 blocks rather than the write history.

The image remains writable while the copy runs. The rule that makes this safe
is **copy before overwrite**:

> If a mutation targets a block still owed by an in-flight copy, capture the
> old block into that copy before changing the image.

The copier and mutation path coordinate a block through three logical states:
`OWED → CAPTURING → CAPTURED`. A striped lock or equivalent claim prevents a
copier's `pread` from racing the backing mutation. A mutation encountering
`OWED` captures the old bytes itself; one encountering `CAPTURING` waits.

Only one copy is ever in flight, so no block is owed by two copies at once.

Captured means that an immutable output buffer owns the bytes, not that
Gazette has confirmed them. The mutation may then proceed while publication
continues. If that bounded output is full, the block request waits rather than
overwriting an uncaptured block.

This rule covers connector writes, filesystem journal and writeback requests,
discards, and write zeroes. It replaces filesystem freezing and is the reason
a copy remains coherent under concurrent write load.

Rescuing a block ahead of the normal cursor also explains why delta chunks
may be out of offset order. Non-overlap, not sorting, is the invariant.

Copy before overwrite does not extend to unchanged blocks copied to advance a
horizon, for the reason [Horizons](#horizons) gives.

Discards follow the same rule as writes. If the delta still owes the block, it
first captures the old content. The discard then clears the live allocated bit
and dirties the block. If it remains unallocated at the next boundary, that
delta records a punch.

### Committing a delta

At an ordinary boundary:

1. The connector finishes and flushes its application state.
2. The runtime sends `Publish`.
3. The daemon closes the device gate, drains admitted requests, swaps the
   dirty bitmap, and reopens the gate.
4. If the old bitmap is empty, the daemon returns no ack. The transaction takes
   the unchanged fast path and appends nothing.
5. Otherwise the daemon may add unchanged blocks to advance a horizon, as
   [Horizons](#horizons) describes, then copies and appends the delta records.
6. The daemon waits for confirmation and returns the exact ack in `Published`.
7. The runtime reports that ack to the transaction coordinator.
8. `Persist` atomically records the checkpoint, connector state, and `AI:`
   obligation.
9. After `Persisted`, the coordinator tells each shard to send `Commit` with its
   recorded ack.
10. Each daemon appends the ack, waits for the append barrier, and confirms that
    it is durable.
11. The transaction cannot begin closing its successor until every shard has
    confirmed.

The point-in-time boundary is established by the bitmap swap. Copying may
overlap later connector work when the task protocol allows it; copy before
overwrite protects the stream either way.

```text
daemon                  runtime                     coordinator
   │                       │                             │
   ◄──── Publish ──────────┤                             │
   ├─ append delta         │                             │
   ├─ wait, then ack ─────►│                             │
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

The append barrier is a correctness requirement. The next delta's first record
cannot be appended until the previous `Ack` is confirmed. Otherwise Gazette
could treat both deltas as one pending transaction, and one later ACK could
advance the recovered disk farther than the recovery-log commit.

This delays publication of the next delta, not local writes after the task
protocol has released the connector.

For a multi-shard task, the one recovery-log `Persist` carries every shard's
disk obligations. The physical ACK appends remain independent and idempotent:

- if `Persist` does not land, no disk advances;
- if it lands, recovered `AI:` values bring every disk to the transaction even
  if some appends did not complete.

The next `Persist` replaces the prior intent set, so it must not be constructed
while any prior disk ACK remains outstanding. New transaction work arriving at
a shard with an undischarged intent is an ordering error and ends the session.

Failures fall on one side of `Persist`:

- Before `Persist`, incomplete or complete delta records remain
  unacknowledged and recovery ignores them.
- After `Persist`, the transaction cannot roll back. Recovery must append the
  exact `AI:` bytes before normal journal use.

Gazette deduplicates a repaired ACK by its original UUID. A writer-register
mismatch always ends the stale session.

## Task boundaries

All task types use the same disk log, copy algorithm, commit authority, and
recovery rules. They differ only in how they identify a boundary and when the
connector may write again.

### Derivations

Terminal `Flushed { more: false }` is the boundary. Before returning it, a
connector that changed its disk must `fsync`. It must not write again until the
runtime's next request.

`Flushed { more: true }` is not a boundary because another flush iteration
still belongs to the same transaction.

`derive-sqlite` continues using its recorded SQLite VFS. Converting it to a
runtime-authoritative derivation backed by this disk is out of scope here.

### Materializations

`C:StartedCommit` establishes that all preceding `C:Store` requests were
processed. The connector must not mutate the disk while handling
`C:StartCommit`; it flushes any earlier changes before returning
`C:StartedCommit`. The runtime sends `Publish` after `StartedCommit` and before
`Persist`, and the connector remains quiet until `Acknowledge`.

The existing `Acknowledge` releases the connector after the recovery-log
commit. It is ordered before any connector-facing request for the next
transaction; the runtime need not wait for `Acknowledged`.

### Captures

Captures need an explicit disk sync point because their Flow transaction
boundary is retrospective.

A capture emits documents and checkpoints continuously. The runtime later
chooses a closing checkpoint after the connector has already moved on.
Saving the live disk at close time would make it newer than that checkpoint.
That is unsafe: a disk behind its checkpoint may repeat source work, while a
disk ahead of its checkpoint may cause the connector to skip documents that
never committed.

Only the connector can order its checkpoint messages with its disk writes.
Each checkpoint therefore describes the disk changes since the previous
checkpoint, or since `Opened` for the first one.

A capture that changed its disk during that span must:

1. `fsync` its changes;
2. set `sync_disk: true` on the checkpoint; and
3. make no further disk changes until it receives `SyncedDisk`.

A checkpoint without `sync_disk` attests that there were no disk changes since
the previous checkpoint, or since `Opened` for the first one.

The quiet rule applies only to disk writes. The connector may continue reading
its source and emitting records while it waits, although pausing entirely is
the simplest implementation.

At a requested disk sync, the runtime sends `Cut`. The daemon then:

1. uses the device gate to drain admitted requests and swap the dirty bitmap;
2. retains the dirty blocks as a local boundary; and
3. returns that boundary's identifier without waiting for journal publication.

The runtime records which checkpoint the boundary belongs to, and returns
`SyncedDisk` to the connector.

The runtime never declines a requested disk sync. Once writes after that
checkpoint overwrite a block, the checkpoint's disk state cannot be
reconstructed.

Checkpoint deltas remain local until a Flow transaction closes. They compose
block by block, with later deltas winning. The transaction publishes the
composition as one ordinary delta and one `Ack`; the disk-log and recovery
formats need no records specific to capture connectors.

```text
stream:       ... C3(sync) ── C4 ── C5(sync) ── C6 ── C7  ◄── close
disk deltas:      Δ3                Δ5
commit:       checkpoint C7, documents through C7, delta Δ3 composed with Δ5
```

This close is aligned because C6 and C7 attest that the disk did not change
after C5. A synthetic checkpoint carries no connector state and makes no
attestation, but it does not break the chain: a synthetically closed
transaction still commits pending checkpoint deltas through the last real
checkpoint.

The live dirty bitmap may be non-empty at a capture connector's transaction
boundary. It represents changes after the latest real checkpoint and must not
enter the current transaction. The fast path is therefore “no pending
checkpoint deltas,” not “empty dirty bitmap.”

The connector chooses its sync frequency. Syncing every checkpoint gives
per-checkpoint durability and pays a quiet round trip each time. Syncing less
often amortizes the cost and accepts loss of the unflushed tail after a crash,
which its checkpoint behavior must already tolerate.

A capture publishes its composed delta at the Flow transaction boundary, and
the daemon chooses which unchanged blocks to copy at that moment. The set of
blocks carrying unpublished changes is wider here than for other task types. It
holds:

- the blocks in the composed checkpoint deltas; and
- the blocks in the live dirty bitmap, which changed after the last real
  checkpoint.

The daemon must not copy any block in that set. Such a block's current image
content belongs to a later checkpoint, and copying it now would put the disk
ahead of the transaction's own checkpoint.

A horizon may begin at a capture's transaction boundary exactly as it does
elsewhere. A horizon needs no disk sync point, because its allocated-bitmap
snapshot does not have to be exact — [Horizons](#horizons) explains why.

## Recovery

Startup recovers all disk `AI:` obligations. Each session creates a new local
image. If the journal holds committed deltas, recovery repairs those
acknowledgements before fixing and replaying the journal range.

### Startup with no committed delta

Recovery:

- does not mutate an existing orphan journal;
- takes the read-only author snapshot needed for a possible later first-use
  fence; and
- starts with a fresh image.

An `AI:` value for a journal with no committed delta is invalid.

An absent journal is the common, healthy state. A journal containing records
from a failed first-use attempt is ignored until a later first use claims it.

### Startup with committed deltas

Inside `Open`, the daemon does these steps:

1. Chooses `E`, reads `R`, and claims the journal.
2. Appends every recovered `AI:` value exactly and waits for its barrier.
3. Fixes a replay range and rebuilds a fresh image.
4. Creates the ublk device over the rebuilt image.
5. Mounts the rebuilt filesystem and returns its path.

The runtime then gives that path to the sandbox and opens the connector.

### Fixing the replay range

After fencing and acknowledgement repair, the daemon obtains a broker-confirmed
write head `H`. The runtime supplies the floor in `Open`, having read it from
the `estuary.dev/truncated-at` label. The daemon resolves that clock to an
offset `O` and reads `[O, H)`. If the runtime supplied no floor, the daemon
reads from the first available fragment.

The daemon treats the floor as a seek hint and not as a message filter. A seek
that lands before the floor costs replay work and nothing else, while a filter
could drop a record from the middle of a delta.

The connector has not opened, the new fence excludes the old writer, and all
recovered intents are already inside `H`. Later irrelevant appends cannot
change what this attempt considers.

Recovery uses a disk-specific, single-journal reader. The append barrier admits
only one unacknowledged delta at a time, so the reader's spill holds at most
one delta.

### Reader rules

The reader makes one forward pass:

1. Fence records are validated and make no disk change.
2. The reader follows Gazette `message.Sequencer` semantics: producer and clock
   group transactions, duplicate UUIDs are dropped, and only acknowledged
   records are released.
3. Delta chunks are spilled, never applied before their acknowledgement. A
   `pwrite` cannot be rolled back, and this design deliberately has no shadow
   image or undo log.
4. Acknowledged deltas apply in physical order. The live append barrier makes
   this commit order.
5. The reader verifies that chunks within one delta do not overlap. It already
   tracks the allocated set, so the check is nearly free, and it catches a wrong
   block index — which no checksum could, because a mis-placed chunk is
   internally consistent.
6. At a record that sets `opens_horizon`, the reader copies its current
   allocated set into the horizon bitmap, then clears a bit for each chunk it
   applies after that point.
7. When the horizon bitmap empties, that horizon is complete and its offset
   becomes the new floor.
8. The reader accepts more than one horizon record in the range, because the
   label may lag. Each one replaces the previous snapshot.
9. A new session's delta producer abandons an older producer's unacknowledged
   delta. A later acknowledgement for an abandoned delta is an ordering error.
10. The reader may begin in the middle of a delta when the label is absent or
    lags, seeing trailing records and an acknowledgement for records it never
    read. This is safe: those records precede the floor, and the floor rule
    guarantees every allocated block has a copy at or after it.
11. At `H`, every acknowledged delta must be applied. Unacknowledged spills are
    deleted.

The reader writes data chunks into a fresh sparse image and hole-punches the
range of every punch chunk, so a rebuilt disk stays as sparse as its source.
Applying both kinds also rebuilds the allocated bitmap. Replay does not mark
blocks dirty, because it is reconstructing the committed baseline.

A data chunk whose `bytes` stop part way through its last block must have the
rest of that block written as zeroes. Leaving it untouched would keep whatever a
previous delta put there.

Hole punching is block-granular, which is the same 4 KiB, so writing those
zeroes costs no sparseness. There is no sub-block hole to preserve.

Replay leaves the daemon holding the image, the allocated bitmap, an empty dirty
bitmap, the horizon bitmap, and the floor. The daemon returns that floor in
`Opened`. If it is ahead of the label, the runtime applies the label.


### Mount after recovery

The daemon mounts the rebuilt image with the standard options. It does **not**
clear the dirty bitmap afterward, which is the rule [Mount](#mount) states.

Mounting may update the superblock, set recovery flags, or replay the
filesystem journal. These are changes relative to an existing committed
baseline. Clearing them would leave the local image permanently ahead of
replicas rebuilt from the disk journal. The next delta therefore carries them.

This is why the correctness guarantee is filesystem-level rather than
whole-image byte equality:

> Once mounted, a rebuilt disk presents the same filesystem contents the
> connector saw at the committed boundary.

Mount counts, timestamps, and similar filesystem bookkeeping may differ.

## Operations

### Privilege and isolation

The daemon is a systemd-supervised machine singleton, and it is privileged. It
needs:

- Linux 6.0 or later with `ublk_drv` loaded;
- `CAP_SYS_ADMIN`, to mount and unmount its own filesystems and to create ublk
  devices;
- access to `/dev/ublk-control`;
- seccomp rules that permit the required `io_uring` operations; and
- owned storage directories on a filesystem that supports hole punching.

`CAP_SYS_ADMIN` is unavoidable. The kernel refuses to let an unprivileged user
mount a block-backed filesystem, for the reason
[What the host kernel parses](#what-the-host-kernel-parses) describes. Running
under a dedicated non-root UID with that one capability ambient is still worth
doing, because it withholds `CAP_SYS_MODULE`, `CAP_NET_ADMIN`,
`CAP_DAC_OVERRIDE` and the rest.

The reactor runs as a non-root container and needs much less:

- the daemon's unix domain socket;
- access to `/dev/kvm`, when it uses a `libkrun` sandbox; and
- seccomp rules that permit the required KVM operations.

The reactor never touches `ublk` and never mounts anything. It receives a
directory path and re-exports it to a sandbox.

`UBLK_CMD_ADD_DEV` assigns an unpredictable device number. The host kernel
creates `ublkcN` and `ublkbN` in its own device namespace, and both stay inside
the daemon. No device node reaches another process, so no udev rule or device
helper is required. Device numbers are ephemeral and are never used as durable
identity.

The daemon is the only privileged component, and containment rests on it being
small and single-purpose rather than on narrow capabilities. What matters is
that everything running third-party code is unprivileged. The reactor receives
no `CAP_SYS_ADMIN`, no `CAP_MKNOD`, no host mount propagation, no broad device
access, no privileged container mode, and no rootful Podman socket.

The daemon and any VMM are separate processes by construction. This satisfies
libkrun's security model and prevents ublk shutdown from waiting on a client of
its own device. A sandbox gets the mount and its own connector root filesystem.
It does not get the image descriptor, the ublk control device, the spill
directory, or journal credentials.

#### What the host kernel parses

The daemon mounts the filesystem, so the host kernel's filesystem code runs on
data a connector influenced. That is a real cost and it is worth stating exactly
what it does and does not expose.

The attack class at issue is a crafted image: author on-disk structures by hand,
get a kernel to mount them, and its parsers run on your data in kernel context.
Block-level write access is the enabling condition for that class.

A connector never has block-level access. Its only path to the filesystem is
file-level calls — `open`, `write`, `rename`, `setxattr`, `link`. Every on-disk
structure is therefore encoded by the host's own ext4 driver. A connector
influences metadata by choosing names, depths, sizes, and attribute contents,
but it cannot author a structure.

> The host kernel parses only filesystem metadata that the host kernel wrote.

Three exposures remain:

- **The write path.** A connector can still drive the driver with legal but
  hostile sequences: very many hard links, maximum nesting, huge attributes,
  extreme fragmentation, rename storms. This is the same exposure as any
  container with a writable volume on the same machine.
- **`virtiofsd`**, for a `libkrun` guest. A host userspace process now
  translates guest requests into host calls. It is the same surface `libkrun`
  already uses for every OCI volume mount.
- **Journal replay on every recovery.** A published image is a power-loss point
  and never a clean unmount, so each session start mounts a dirty filesystem.
  This is the path where the claim above does the most work, and it runs far
  more often than a dirty mount normally would. Running `e2fsck -p` on a
  rebuilt image before mounting it would move that parse into a userspace
  process the daemon controls, at a startup cost proportional to filesystem
  size. Anything it repaired would appear in the next delta, exactly as mount
  bookkeeping does.

A connector runs under an unprivileged UID inside its sandbox. No trusted
component runs as root inside a guest, because the daemon does the format and
the mount before the sandbox starts.

### Local lifecycle and cleanup

TBD

## Shard splits

TBD
