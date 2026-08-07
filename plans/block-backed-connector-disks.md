# Block-Backed Connector Disks

## Decision summary

Each live connector shard gets a private 10 GiB POSIX filesystem at a stable
path. The disk advances atomically with Flow transactions: after a failure, it
is rebuilt to the filesystem state associated with the last committed
transaction.

Every connector block write goes through the runtime, which records the blocks
that change. At a transaction boundary the runtime copies those blocks into a
per-shard Gazette journal. Each delta also carries a small number of blocks
that did not change, and these move the start of the necessary replay range
forward. Recovery therefore does not replay an unbounded history.

> An important detail this design shook out: The connector sandboxing strategy
  is impactful on shaping the joint between the reactor and connector. A
  hypothetical `libkrun` based sandbox has (on paper, at least) a nice
  implementation path for a non-root reactor, and that's what I'm assuming we'll
  be working with for the rest of this document. But, if some connectors are to
  run without this sandboxing vs. some that do, a different design will be
  needed. That is the subject of further investigation.

The initial implementation uses Linux `ublk` between libkrun and the runtime.
This is an adapter for libkrun's path-based disk API, not part of the durable
format.

The derivation connector protocol fits naturally with coordinating the necessary
disk quiescense periods for snapshots to be recorded. Runtime authoritative
materializations also work well with the existing protocol, but not remote
authoritative ones. Captures are complex as well. This design describes support
for derivations, runtime-authoritative materializations, and captures.

### Design boundaries

**Connector calls.** Every live capture, derivation, and materialization shard
gets a disk mounted at a stable path inside its sandbox before the connector
opens. There is no opt-in or capability negotiation. Validation, discovery,
`Apply`, and other non-transactional connector calls do not get a disk because
their filesystem changes cannot be tied to a Flow commit.

**Capacity.** Every disk is 10 GiB. This is a platform constant rather than a
catalog or connector setting because the compaction, spill, and local-capacity
assumptions below depend on a small known maximum. It is a number largely picked
out of the air as "not too big, but big enough to be useful".

**Initial platform.** The first implementation requires Linux 6.0 or later,
KVM, libkrun sandboxing, and unprivileged `ublk`. The reactor runs as a non-root
container. A trusted process inside the guest formats and mounts the
filesystem; the host never mounts connector-controlled filesystem bytes.

**Guarantee.** Recovery reproduces the filesystem contents seen at the
committed boundary. It does not promise a byte-identical image because mounting
and unmounting change filesystem bookkeeping.

**Shard splits.** TBD. Options here may include copying the parent disk in child
shards, or simply not allowing live splits and requiring the number of shards
for a task to be specified at creation. It will almost certainly not be possible
to "merge" split shards once they have written to their disks.

## Correctness model

The local image is a disposable working copy. The durable disk state is the set
of committed deltas in the disk journal.

A **delta** is the set of chunks that one transaction commits. It carries
every block that changed at that boundary, and it may carry some blocks that
did not — extra copies the runtime includes to move the recovery floor forward.
[Horizons](#horizons) explains why.

Nothing in the encoding separates the two. An extra copy is an ordinary `DATA`
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
  committed transaction still requires the shard to append.

`AI:` makes post-commit publication recoverable. A delta without an
acknowledgement is not committed.

### What the connector must do

The runtime can tell whether blocks changed and can capture a coherent image
while writes continue. It cannot tell whether a connector has finished an
application-level transaction or flushed its own buffers.

At a boundary where its disk changed, a connector must:

1. `fsync` the state that belongs to the boundary;
2. report the appropriate protocol response; and
3. avoid further disk writes until the runtime signals that the boundary has
   been taken.

A connector that skips `fsync` gets power-cut semantics for its own disk. A
connector that writes during the quiet interval can put its disk ahead of its
checkpoint. The runtime cannot detect either violation because connector
messages and block I/O are independently buffered.

The connector contract aligns the disk snapshot with the connector checkpoint.
The runtime produces a coherent snapshot even if the connector violates that
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
therefore consumes about 200 MiB rather than 10 GiB. The runtime treats the
contents as opaque bytes. It never parses superblocks, inodes, or directory
entries.

Keeping the image as one ordinary file allows the runtime to use normal file
operations for block reads, writes, checksums, hole punching, and final
cleanup.

### Change tracking

The runtime keeps three bitmaps. Each bitmap has one bit for each 4 KiB block. A
10 GiB image has 2,621,440 blocks, so each bitmap is 320 KiB. The bitmaps are
arrays of atomic words. The write path sets the necessary bits before it issues
the image write.

The **dirty bitmap** answers this question:

> Did this block change after the last transaction boundary?

At a boundary the runtime replaces this bitmap with an empty bitmap. The old
bitmap becomes the block set for the delta.

The **allocated bitmap** answers this question:

> Does this block occupy space in the image now?

The allocated bitmap gives the initial content of the horizon bitmap. It also
gives placement an exact measure of physical use. A write sets allocated bits. A
successful aligned discard clears them.

The **horizon bitmap** answers this question:

> Is the newest copy of this block before the current horizon?

The runtime fills this bitmap when it starts a horizon. Deltas clear its bits.
The bitmap is empty when no horizon is open.

The bitmaps are independent: a block can be allocated without having changed
since the last boundary.

### Block transport

The initial block path is:

```text
connector process (guest, unprivileged)
                │ ordinary file I/O
                ▼
       ext4 filesystem (guest)
                │
                ▼
       virtio-blk (libkrun)
                │
                ▼
          /dev/ublkbN
                │ ublk_drv
                ▼
        ublk server (reactor) ──► sparse image
```

libkrun opens `/dev/ublkbN` as a raw disk with direct I/O and
`KRUN_SYNC_FULL`. The guest sees a virtio block device. A trusted guest
bootstrap formats it when fresh, mounts it at the connector's stable disk path,
and then starts the connector as an unprivileged guest process. The host treats
the filesystem as opaque bytes.

`/dev/ublkbN` is created for each sandbox; it is not a persistent identifier:

1. Loading `ublk_drv` exposes the host-wide `/dev/ublk-control`.
2. The reactor sends `UBLK_CMD_ADD_DEV` with the unprivileged-device flag. The
   reactor asks the kernel to choose `N`; host devtmpfs and udev then create
   `/dev/ublkcN`, the character device used by its ublk server.
3. The reactor sets the device size and queue limits and starts its I/O queues.
   `UBLK_CMD_START_DEV` then exposes `/dev/ublkbN`, the block device opened by
   libkrun.
4. Teardown stops the block device and deletes the character device. The kernel
   may reuse `N` immediately.

Container `/dev` namespaces do not automatically receive dynamically created
nodes. Host setup must therefore make the two exact nodes visible to their
owner, as described under [Privilege and isolation](#privilege-and-isolation).

Everything below remains independent of this transport:

- the image and bitmaps;
- copy-before-overwrite capture;
- the disk-log format;
- commit coordination;
- compaction; and
- recovery.

To allow a direct libkrun block backend later, the design follows three rules:

- the durable format never contains a mount path, device identity, or mount
  option;
- the runtime never interprets the connector filesystem; and
- point-in-time capture never depends on a filesystem freeze.

The host filesystem that stores images must support hole punching.

### Disk journal

The disk journal is created lazily on the first transaction that needs durable
disk state.

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
┌──────────────┬──────────────┬────────────┬─────────┬────────────────────┐
│ offset: u64  │ length: u32  │ crc32: u32 │ kind    │ bytes (if DATA)    │
└──────────────┴──────────────┴────────────┴─────────┴────────────────────┘
   DATA   bytes present at the boundary
   PUNCH  an allocated range was discarded
```

At a boundary, a dirty block that remains allocated becomes `DATA`. A dirty
block that is no longer allocated becomes `PUNCH`. An unchanged block that the
runtime copies to advance a horizon is also `DATA`, with nothing to mark it
apart.

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

At session start, after acquiring and recovering the shard recovery log, the
shard:

1. chooses a fresh epoch `E`;
2. reads the current `author` register once and remembers it as `R`; and
3. never refreshes `R`.

A derivation or materialization shard reports `E` to the leader when it opens,
and the leader records it for later ACK validation. A capture keeps the same
information in its local coordinator.

Claiming the journal appends `Fence(E)` while atomically changing `author` from
the remembered value `R` to `E`. The shard waits for broker confirmation before
any other journal operation.

The one-time comparison matters. A stale session might reach its first disk
write after a replacement has taken over. If it re-read the register, it could
overwrite the replacement's epoch. It may attempt only the transition observed
at startup.

The claim happens at different times depending on journal contents:

- If the journal exists, the shard claims it before repairing `AI:`, fixing a
  recovery range, replaying, or opening the connector.
- If the journal does not exist, the shard takes only the read-only snapshot
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
runtime moves the recovery floor forward with **horizons**.

A **horizon** is a point in the journal. The rule a horizon must satisfy is:

> Every allocated block has a copy at or after the horizon.

When a horizon satisfies this rule the horizon is **complete**, and the
recovery floor can move to it.

### The horizon bitmap

When the runtime starts a horizon it copies the allocated bitmap into the
horizon bitmap. A set bit shows that the newest copy of that block is before
the horizon.

Two things clear a bit:

- The connector writes the block. The delta publishes it after the horizon, so
  the bit clears at no extra cost.
- The runtime copies the block into a delta even though it did not change.

Bits clear when the delta **commits**, never while the copy runs. Clearing a bit
earlier would claim a block was covered by a copy that a failed transaction then
discarded. A failed transaction therefore leaves the bitmap untouched, which is
exactly what a replaying session derives, since it discards that delta too.

Nothing needs to change during the copy. The candidate set already subtracts the
blocks in this delta, so the runtime cannot re-select one of them.

A horizon consequently never completes in the middle of a transaction.

Bits never become set again during a horizon. The horizon bitmap therefore only
decreases, and a horizon always completes. A horizon that makes no progress
loses nothing and continues later.

The runtime finds blocks to copy with a cursor that moves forward through the
horizon bitmap. Bits behind the cursor are always clear, so a horizon completes
after exactly one pass of the cursor.

The snapshot need not be exact, which is part of what keeps this cheap. The
runtime takes the live allocated set while a reader takes the committed
allocated set at the same offset, and the two can differ. Each difference
resolves itself:

- A block the runtime holds and a reader does not is a block with an
  unpublished write. A later delta publishes it and clears the bit.
- A block a reader holds and the runtime does not is a block a later delta
  discards. That delta records `PUNCH`, which clears the bit.

An inexact snapshot therefore costs a little redundant work and never produces
a wrong result.

### Which blocks to copy

The runtime reads these blocks from the local image and never from the journal.
The image is local and hot, while the oldest fragments of a journal are the
ones most likely to have been offloaded to cloud storage.

The runtime must select only blocks whose current image content is already
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

If a mutation targets a selected block during the copy, the runtime drops it
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
horizon, and the runtime starts a new one later.

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

The runtime starts a horizon when the journal range from the floor to the write
head exceeds `r` times the live allocated size, subject to an absolute floor of
1 GiB. The absolute floor prevents constant horizons on a small disk.

The two constants are independent:

- `k` sets write amplification, which is `1 + k`, and how quickly a horizon
  completes.
- `r` sets how large the journal grows before a horizon starts.

An initial policy is `k = 0.5` and `r = 2`, which bounds the recovery range at
roughly five times the live allocated size. This policy is required, not a user
setting.

A connector that rewrites much of its disk pays almost nothing, because its own
writes clear bits. The work scales with how much of the disk is genuinely
static, rather than with how large the disk is.

If the connector stops writing its disk, an open horizon stalls. The journal is
not growing either, so the recovery range freezes rather than deteriorating.
Nothing gets worse while the disk is quiet, and the horizon resumes when writes
resume. A lower `r` bounds the size at which a horizon can freeze, and costs
nothing in write amplification.

### Completion

The runtime moves the floor when the horizon bitmap becomes empty:

1. A delta commits, and clearing its blocks empties the horizon bitmap.
2. Wait until the broker confirms that delta's acknowledgement.
3. Apply the `estuary.dev/truncated-at` label with the message clock of the
   record that set `opens_horizon`.

Step 2 is a correctness requirement. If the runtime applied the label at append
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

When the journal has no committed delta, startup does these steps:

1. Create a fresh sparse image.
2. Create an unprivileged ublk device over the image.
3. Start the libkrun sandbox with that device.
4. Let the trusted guest bootstrap format and mount the filesystem.
5. Choose `E` and take the read-only author snapshot `R`.
6. Start the connector. Do not create the journal and do not append to it.

There is no baseline step, and the runtime does not clear the dirty bitmap.

If the journal does not exist, `R` means “author absent.” Creating the journal
on first use produces an empty register set, after which the deferred fence can
change the absent author to `E`. If another session fenced first, that
transition fails.

#### Formatting

Only a fresh image is formatted. The trusted guest bootstrap runs `mkfs`;
recovery treats filesystem structures as payload and never formats them again.

The initial filesystem is ext4 with:

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

#### Guest mount

Fresh and recovered disks use the same mount options:

- `noatime`, so reads do not create deltas;
- `nodev`, `nosuid`, and `noexec`; and
- `discard`, so ext4 releases blocks as it frees them.

`mkfs` and the first mount write through the served device. The runtime does
**not** clear the dirty bitmap after those writes.

Blocks actually written by `mkfs` and mount are real filesystem content, and no
earlier record holds them. The dirty bitmap therefore carries the full
allocated set at the first boundary, and the first delta contains every
allocated block. Unwritten zero ranges remain sparse and are omitted. The first
delta is a complete image copy, but it needs no special record type and no
special code path to produce one.

The runtime also does not clear the dirty bitmap after mounting a recovered
disk. Mount bookkeeping and filesystem-journal replay are genuine changes from
an existing committed baseline and must appear in the next delta.

Fresh and recovered disks thus follow a single rule.

### First use

The first transaction in which a connector changes its disk creates the
initial durable state:

1. The connector reaches a valid boundary.
2. The runtime observes a non-empty dirty bitmap. On a fresh disk that bitmap
   holds the full `mkfs` and mount output.
3. The shard derives a `JournalSpec` from the task's disk-journal template and
   creates it through broker `Apply`, or confirms that it already exists.
4. The shard performs the deferred `R → E` fence.
5. The shard copies the dirty blocks as bounded records and appends the
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
still issue journal or writeback requests. The runtime therefore establishes
the boundary at the block device with one gate shared by every `ublk` queue.
Writes, discards, write-zeroes requests, and flushes enter the gate before
being handled and leave only after their backing operation completes.

To establish a boundary, the runtime:

1. closes the gate so that new requests wait;
2. waits for every admitted request to finish;
3. swaps the dirty bitmap, takes an allocated-bitmap snapshot if a horizon
   starts here, and registers the new copy; and
4. reopens the gate.

The bitmap operation in step 3 is the cut. A request is handled entirely before
or after it. Reads need not stop, and ext4 may continue submitting requests
while the gate is closed; those requests simply wait. The gate is held only
while the cut is established, not while the image is copied. It gives priority
to a waiting boundary so continuous writeback cannot starve it.

`KRUN_SYNC_FULL` passes guest flushes through to ublk. The ublk target must
honor every flush and force-unit-access (`FUA`) write it advertises. This
preserves ext4's ordering across queues. An ext4 journal operation may span the
cut as several block requests, but the resulting image is a valid power-loss
point: recovery either replays a committed journal operation or discards an
incomplete one.

The copy walks the owed-set:

1. Claim the next contiguous owed chunk.
2. Read its current bytes with `pread`.
3. Compute the CRC and move the chunk into a bounded, immutable output
   buffer.
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
first captures the old `DATA`. The discard then clears the live allocated bit
and dirties the block. If it remains unallocated at the next boundary, that
delta records `PUNCH`.

### Committing a delta

At an ordinary boundary:

1. The connector finishes and flushes its application state.
2. The runtime closes the device gate, drains admitted requests, swaps the
   dirty bitmap, and reopens the gate.
3. If the old bitmap is empty, the transaction takes the unchanged fast path
   and appends nothing.
4. Otherwise, the runtime may add unchanged blocks to advance a horizon, as
   [Horizons](#horizons) describes, and the shard copies and appends the delta
   records.
5. It waits for confirmation, constructs the exact `Ack` frame, and reports
   that frame to the transaction coordinator.
6. `Persist` atomically records the checkpoint, connector state, and `AI:`
   obligation.
7. After `Persisted`, the coordinator tells each shard to append its recorded
   `Ack`.
8. Each shard waits for the append barrier, then confirms that its disk `Ack`
   is durable.
9. The transaction cannot begin closing its successor until every shard has
   confirmed.

The point-in-time boundary is established by the bitmap swap. Copying may
overlap later connector work when the task protocol allows it; copy before
overwrite protects the stream either way.

```text
disk shard                                  coordinator
    │
    ├─ append delta records
    ├─ wait for confirmation
    ├─ report exact Ack ───────────────────────►
    │                                          │
    │                              Persist(checkpoint,
    │                                      state, AI)
    │                                          │
    │                              ◄──── Persisted
    ◄─ append the recorded Ack ────────────────┤
    ├─ append recorded Ack                     │
    ├─ wait for append barrier                 │
    ├─ confirm disk Ack durable ───────────────►
    │                                          │
    ◄──────── next transaction may close ──────┘
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
runtime-authoritative derivation backed by this disk is separate work after
this design is proven.

### Materializations

`C:StartedCommit` establishes that all preceding `C:Store` requests were
processed. The connector must not mutate the disk while handling
`C:StartCommit`; it flushes any earlier changes before returning
`C:StartedCommit`. The runtime takes the disk boundary after `StartedCommit`
and before `Persist`, and the connector remains quiet until `Acknowledge`.

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

At a requested disk sync, the runtime:

1. uses the device gate to drain admitted requests and swap the dirty bitmap;
2. saves the dirty blocks as a local checkpoint delta; and
3. returns `SyncedDisk` without waiting for journal publication.

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
the runtime chooses which unchanged blocks to copy at that moment. The set of
blocks carrying unpublished changes is wider here than for other task types. It
holds:

- the blocks in the composed checkpoint deltas; and
- the blocks in the live dirty bitmap, which changed after the last real
  checkpoint.

The runtime must not copy any block in that set. Such a block's current image
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

Startup does these steps:

1. Choose `E`, read `R`, and claim the journal.
2. Append every recovered `AI:` value exactly and wait for its barrier.
3. Fix a replay range and rebuild a fresh image.
4. Create the ublk device and start the libkrun sandbox.
5. Let the trusted guest bootstrap mount the rebuilt filesystem.
6. Start the connector.

### Fixing the replay range

After fencing and acknowledgement repair, the shard obtains a broker-confirmed
write head `H`. The shard reads the `estuary.dev/truncated-at` label, resolves
it to an offset `O`, and reads `[O, H)`. If the label is absent, the shard
reads from the first available fragment.

The shard treats the label as a seek hint and not as a message filter. A seek
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
5. At a record that sets `opens_horizon`, the reader copies its current
   allocated set into the horizon bitmap, then clears a bit for each chunk it
   applies after that point.
6. When the horizon bitmap empties, that horizon is complete and its offset
   becomes the new floor.
7. The reader accepts more than one horizon record in the range, because the
   label may lag. Each one replaces the previous snapshot.
8. A new session's delta producer abandons an older producer's unacknowledged
   delta. A later acknowledgement for an abandoned delta is an ordering error.
9. The reader may begin in the middle of a delta when the label is absent or
   lags, seeing trailing records and an acknowledgement for records it never
   read. This is safe: those records precede the floor, and the floor rule
   guarantees every allocated block has a copy at or after it.
10. At `H`, every acknowledged delta must be applied. Unacknowledged spills are
    deleted.

The reader writes only allocated chunks into a fresh sparse image. `PUNCH`
chunks hole-punch the recovered file. A rebuilt disk therefore remains as
sparse as the source. Applying `DATA` and `PUNCH` also rebuilds the allocated
bitmap. Replay itself does not mark blocks dirty because it is reconstructing
the committed baseline.

Replay leaves the runtime holding the image, the allocated bitmap, an empty
dirty bitmap, the horizon bitmap, and the floor. If the floor is ahead of the
label, the runtime applies the label.


### Guest mount after recovery

The trusted guest bootstrap mounts the rebuilt image with the standard options.
The runtime does **not** clear the dirty bitmap afterward, following the single
rule that [Guest mount](#guest-mount) states.

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

The reactor pod runs as a non-root host UID. It needs:

- Linux 6.0 or later with `ublk_drv` loaded;
- access to `/dev/ublk-control` and permission to create unprivileged ublk
  devices;
- access to `/dev/kvm`;
- seccomp rules that permit the required KVM and `io_uring` operations; and
- owned storage directories on a filesystem that supports hole punching.

`UBLK_CMD_ADD_DEV` assigns an unpredictable device number. The host kernel
creates `ublkcN` and `ublkbN` in its own device namespace, not automatically in
the reactor container's private `/dev`. A host udev rule or narrow helper must
therefore:

1. identify the exact nodes created for the reactor;
2. make `ublkcN` visible to the reactor and `ublkbN` visible to its VMM;
3. grant them only to the reactor's host UID; and
4. remove them during teardown.

This helper is privileged host provisioning, but the reactor is not. The
reactor receives no `CAP_SYS_ADMIN`, `CAP_MKNOD`, host mount propagation, broad
device access, privileged container mode, or rootful Podman socket. Device
numbers are ephemeral and never used as durable identity.

The ublk server and libkrun VMM run as separate processes in separate mount
namespaces. This follows libkrun's security model and prevents ublk shutdown
from waiting on a client of its own device. The VMM gets only the exact
`ublkbN`, its connector root filesystem, and the other resources required by
that invocation. It does not get the image descriptor, ublk control device,
spill directory, or journal credentials.

A trusted bootstrap runs as root inside the guest. It formats a fresh disk,
mounts it with `nodev`, `nosuid`, `noexec`, and `noatime`, and starts the
connector under an unprivileged guest UID. Guest root is not host root. The
host kernel never parses the connector-controlled ext4 filesystem; it handles
only KVM, virtio, ublk requests, and opaque image bytes.

### Local lifecycle and cleanup

TBD

## Shard splits

TBD
