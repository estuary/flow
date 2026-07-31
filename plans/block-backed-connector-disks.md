# Block-Backed Connector Disks

## Decision summary

Each live connector shard gets a private 10 GiB POSIX filesystem at a stable
path. The disk advances atomically with Flow transactions: after a failure, it
is rebuilt to the filesystem state associated with the last committed
transaction.

Every connector block write passes through the runtime, which tracks exactly
what changed. At a transaction boundary the runtime copies those blocks into a
per-shard Gazette journal. It occasionally writes a new complete copy so
recovery does not have to replay an unbounded history.

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

The local image is a disposable working copy. The durable disk state is the
journal state selected by the task recovery log:

> the designated full copy, followed by every committed delta after it.

A **full** contains every allocated block in the image at one boundary. A
**delta** contains every block that changed since the previous boundary. Both
use the same extent encoding.

The recovery log carries two kinds of disk state:

- `DR:{shard}` is the disk-journal byte offset of the `Begin` record for the
  currently designated full.
- `AI:{journal}` is the exact serialized `Ack` record that a committed
  transaction still requires the shard to append.

`DR:` chooses the recovery base. `AI:` makes post-commit publication
recoverable. A complete full that is not named by `DR:` is only an orphan, and
an unacknowledged delta is not committed.

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

The block store for each live shard incarnation has three local data
structures:

- a sparse image file;
- a dirty bitmap; and
- an allocated bitmap.

If the disk has ever committed, it also has one per-shard Gazette journal.
Authority over that journal remains in the task recovery log.

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

Both bitmaps have one bit per 4 KiB block. A 10 GiB image has 2,621,440 blocks,
so each bitmap is 320 KiB. They are arrays of atomic words, and the write path
sets the relevant bits before issuing the image write.

The **dirty bitmap** answers:

> Has this block changed since the last transaction boundary?

At a boundary the runtime swaps it for an empty bitmap. The old bitmap becomes
the owed-set for the delta being captured.

The **allocated bitmap** answers:

> Does this block currently occupy space in the image?

It supplies the owed-set for a full and gives placement an exact measure of
physical use. A write sets allocated bits. A successful aligned discard clears
them.

The two bitmaps are independent: a block can be allocated without having
changed since the last boundary. Fresh-disk initialization relies on this
distinction, as described later.

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

Records use Gazette's fixed Protobuf framing and a disk-specific message:

| Operation | Purpose |
| --- | --- |
| `Fence` | Installs the current shard session's writer epoch without changing disk state |
| `Begin` | Starts a full or delta |
| `Chunk` | Carries a bounded batch of extents |
| `End` | Finishes a stream and states its totals |
| `Ack` | Commits one complete delta through Gazette sequencing |

Full records and `Fence` records are `OUTSIDE_TXN`. A delta's `Begin`, `Chunk`,
and `End` records are `CONTINUE_TXN`; its `Ack` is `ACK_TXN`.

A `Begin` states whether the stream is a full or delta.

A `Chunk` contains extents:

```text
Extent
┌──────────────┬──────────────┬────────────┬─────────┬────────────────────┐
│ offset: u64  │ length: u32  │ crc32: u32 │ kind    │ bytes (if DATA)    │
└──────────────┴──────────────┴────────────┴─────────┴────────────────────┘
   DATA   bytes present at the boundary
   PUNCH  an allocated range was discarded
```

At a delta boundary, a dirty block that remains allocated becomes `DATA`. A
dirty block that is no longer allocated becomes `PUNCH`. A full emits `DATA`
for every allocated block.

An `End` states the stream's extent count and byte total. This catches
truncation even when all record frames before the truncation were valid.

Extents in one stream never overlap, but they are not guaranteed to be sorted.
Concurrent writes can force a block ahead of the copy cursor to be emitted
early. Readers must accept this order and rely only on non-overlap.

Records and append batches are bounded. Gazette serializes appends to a
journal, so one enormous append from a background full could otherwise block a
live transaction delta for an unbounded time.

### Grouping journal records

A full is a stream of `Begin`, `Chunk`, and `End` records. A delta uses the
same records plus an `Ack` that commits it. Fulls are copied in the background,
so their records can be interleaved with live deltas. Failed attempts can also
leave streams that never become authoritative. Recovery must know which
records belong together.

Gazette already gives every record a producer ID, so the disk journal does not
add a separate stream ID:

- A shard session uses one producer for all its deltas. `Begin` and `End`
  delimit each delta, and a replacement session uses a new producer.
- Each full attempt uses a fresh producer. All records in that full carry the
  same producer.
- Each `Fence` record uses its own producer and carries the shard-session
  producer that it installs as the writer epoch.

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
                 │ DR: designated full    │
                 │ AI: exact pending Ack  │
                 │ checkpoint and state   │
                 └────────────────────────┘
                        decides authority
```

For derivations and materializations, the Shuffle Leader includes every
shard's disk obligations in one recovery-log `Persist`. Each disk-owning shard
then appends to its own journal. Captures perform the same coordination inside
their local transaction FSM.

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

The claim happens at different times depending on durable state:

- If `DR:` exists, the shard claims the journal before repairing `AI:`, fixing
  a recovery range, replaying, or opening the connector. A missing journal is
  fatal.
- If `DR:` does not exist, the shard takes only the read-only snapshot `R`.
  Journal creation and the deferred claim happen on first use.

An ambiguous fence append is retried idempotently as a check for `E`, not as a
new attempt to take ownership. A register mismatch ends the session. Every
later append checks `author=E`; no ordinary appender responds to a mismatch by
taking the register back.

Journal registers fence cooperative writers but do not become commit
authority. A lost register set can satisfy a comparison against absence, so
`DR:` and `AI:` remain the final authority even when register state is empty.

Gazette orders the fence with appends. Old content before the fence matters
only if `DR:` or `AI:` already made it authoritative; an old append ordered
after the fence fails its `author` check.

## Lifecycle

The recovery log has only two durable disk states:

| Durable state | Meaning |
| --- | --- |
| No `DR:` | No disk state has ever committed. An absent journal is normal; any existing records are orphans. |
| `DR:` present | The named full and its committed following deltas are authoritative. The journal must exist. |

```text
no DR:
  fresh image ── first disk-writing commit ──► DR present

DR present:
  recover ── transactions ──► deltas
              │
              └── compaction ──► newer DR
```

### Starting a fresh disk

When no `DR:` exists, startup:

1. creates a fresh sparse image;
2. creates an unprivileged ublk device over it;
3. starts the libkrun sandbox with that device;
4. has the trusted guest bootstrap format and mount it;
5. establishes the clean baseline;
6. chooses `E` and takes the read-only author snapshot `R`; and
7. starts the connector without creating or appending to a journal.

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
remain holes in the host image, do not enter the allocated bitmap or first
full, and do not cause later background initialization writes.

The default inode density avoids imposing an unusually low file-count limit for
a small storage saving.

#### Guest mount and fresh baseline

Fresh and recovered disks use the same mount options:

- `noatime`, so reads do not create deltas;
- `nodev`, `nosuid`, and `noexec`; and
- `discard`, so ext4 releases blocks as it frees them.

`mkfs` and the first mount write through the served device. On a fresh disk,
the trusted guest bootstrap mounts the filesystem, calls `syncfs`, and signals
that initialization is complete. Before allowing it to start the connector,
the runtime drains the block-device queues and:

> clears the dirty bitmap and retains the allocated bitmap.

Blocks actually written by `mkfs` and mount are real filesystem content and
must be included in the first full. Unwritten zero ranges remain sparse and are
omitted. Clearing only the dirty bitmap prevents platform initialization from
counting as connector use. The runtime then releases the guest bootstrap to
start the connector.

This baseline step applies only to a fresh disk. A recovered disk already has a
committed baseline. Mount bookkeeping and filesystem-journal replay are genuine
changes from that baseline and must appear in the next delta.

### First use

The first transaction in which a connector changes its disk creates the
initial durable state:

1. The connector reaches a valid boundary.
2. The runtime observes a non-empty dirty bitmap.
3. The shard derives a `JournalSpec` from the task's disk-journal template and
   creates it through broker `Apply`, or confirms that it already exists.
4. The shard performs the deferred `R → E` fence.
5. It appends a full `Begin` under a fresh producer and records the
   broker-confirmed starting offset `O`.
6. It copies every allocated block as bounded `Chunk` records, appends `End`,
   and waits for confirmation.
7. The authoritative `Persist` writes `DR:{shard}=O`.

This first full is intentionally on the transaction's critical path. There is
no older disk from which the transaction can recover.

A failure before `Persist` leaves `DR:` absent. The journal may be absent or
may hold a fence and an incomplete or complete orphan full. Recovery ignores
all of it, destroys the uncommitted local image, and starts from a fresh disk.

Captures begin first use at their first disk sync point rather than at the
later Flow transaction boundary. Their full and delta ordering is described
under [Captures](#captures).

### Capturing a consistent delta or full

Every delta and full must represent the disk at one exact boundary, even if the
connector resumes writing before its blocks have been copied into the journal.
They use the same copy machinery and differ only in the initial owed-set:

- a delta owes the dirty blocks taken at a boundary;
- a full owes every block allocated when the full starts.

The connector flushes and stops its own writes before a boundary, but ext4 may
still issue journal or writeback requests. The runtime therefore establishes
the boundary at the block device with one gate shared by every `ublk` queue.
Writes, discards, write-zeroes requests, and flushes enter the gate before
being handled and leave only after their backing operation completes.

To establish a boundary, the runtime:

1. closes the gate so that new requests wait;
2. waits for every admitted request to finish;
3. swaps the dirty bitmap, takes any allocated-bitmap snapshot needed by a
   full, and registers the new copy; and
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

1. Claim the next contiguous owed run.
2. Read its current bytes with `pread`.
3. Compute the CRC and move the extent into a bounded, immutable output
   buffer.
4. Mark the blocks captured and remove them from the owed-set.
5. Append a `Chunk` when the output batch reaches its limit, retaining its
   bytes until the append is confirmed.
6. Append and confirm `End` after every block is captured and every `Chunk` is
   confirmed.

Repeated writes to one block therefore produce one extent, and adjacent dirty
blocks become one run. A transaction with 4,000 writes to 2,000 distinct
blocks publishes exactly 2,000 blocks rather than the write history.

The image remains writable while the copy runs. The rule that makes this safe
is **copy before overwrite**:

> If a mutation targets a block still owed by an in-flight copy, capture the
> old block into that copy before changing the image.

The copier and mutation path coordinate a block through three logical states:
`OWED → CAPTURING → CAPTURED`. A striped lock or equivalent claim prevents a
copier's `pread` from racing the backing mutation. A mutation encountering
`OWED` captures the old bytes itself; one encountering `CAPTURING` waits. If a
delta and background full both owe the block, both copies must capture it.

Captured means that an immutable output buffer owns the bytes, not that
Gazette has confirmed them. The mutation may then proceed while publication
continues. If that bounded output is full, the block request waits rather than
overwriting an uncaptured block.

This rule covers connector writes, filesystem journal and writeback requests,
discards, and write zeroes. It replaces filesystem freezing and is the reason
a copy remains coherent under concurrent write load.

Rescuing a block ahead of the normal cursor also explains why stream extents
may be out of offset order. Non-overlap, not sorting, is the invariant.

Discards follow the same rule as writes. If a full or delta still owes the
block, it first captures the old `DATA`. The discard then clears the live
allocated bit and dirties the block. If it remains unallocated at the next
boundary, that delta records `PUNCH`.

### Committing a delta

At an ordinary boundary:

1. The connector finishes and flushes its application state.
2. The runtime closes the device gate, drains admitted requests, swaps the
   dirty bitmap, and reopens the gate.
3. If the old bitmap is empty, the transaction takes the unchanged fast path.
4. Otherwise, the shard copies and appends `Begin ... Chunk ... End`.
5. It waits for `End`, constructs the exact `Ack` frame, and reports that frame
   to the transaction coordinator.
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
    ├─ append Begin ... End
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

The append barrier is a correctness requirement. The next delta's `Begin`
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

First-use and compaction fulls also begin at a disk sync point. The full is
armed at that quiet instant and remains protected by copy before overwrite
after `SyncedDisk` releases the connector. Checkpoint deltas taken after the
full's boundary advance the later delta. Pending deltas are not rewritten
around a full: changes already present in the full may also appear in that
delta and apply idempotently. On first use, `Persist` designates the confirmed
full and records the composed delta's `Ack` together.

## Compaction

Without compaction, both the journal and recovery time grow forever.
Compaction writes a new full, then moves `DR:` to that full after it is safely
complete.

The ordering rule is:

> A candidate full contains the disk at boundary `B`, and every delta needed
> after `B` begins after the candidate's confirmed `Begin` offset.

Until a later `Persist` moves `DR:`, the candidate is only an orphanable stream.
The previous full remains authoritative.

### When to compact

The runtime compares:

- the committed recovery range from the current `DR:` offset to the journal
  write head; and
- the image's live allocated size.

An initial policy is to compact when the range is at least 5 GiB and more
than four times the live data. The floor prevents constant compaction of tiny
disks. The ratio bounds normal replay work relative to useful state.

This policy is required, not a user setting. No format generation or elapsed
time independently triggers a full.

### Publishing a candidate

A compaction candidate is ordered around transaction `T` as follows:

1. Boundary `T` establishes the disk state the full will contain. If `T` has a
   delta, the shard first appends and confirms that delta through `End`.
2. Before constructing `Persist(T)`, the shard appends a full `Begin` under a
   fresh producer and records its broker-confirmed offset `O`.
3. The full begins copying in the background. It is safe to start before
   `Persist(T)` because it is not yet authoritative.
4. Transaction `T` commits normally. Its delta data is before `O`, while its
   later `Ack` is after `O`. The full already contains the delta's effect, so
   recovery correctly treats this as an ACK with no matching data in range.
5. Full `Chunk` records may interleave with deltas from later transactions.
   Every such delta has its `Begin` after `O`.
6. A broker-confirmed full `End` makes the candidate eligible for designation.
7. The first later transaction that observes completion writes
   `DR:{shard}=O` in its `Persist`.
8. After that `Persisted`, the runtime advances the journal's
   `estuary.dev/truncated-at` label to the Gazette timestamp of the designated
   `Begin`.

Transaction `T` never designates the candidate it started. Waiting for the
full's `End` would put the whole background copy on `T`'s critical path.
Allowing the next transaction to designate it costs only one transaction of
delay.

For captures, an armed full begins at the next disk sync point rather than at
the Flow transaction boundary. That sync point supplies the committed-alignable
image.

A failure of `T` cancels its candidate. An ordinary copy or append failure
abandons the candidate and leaves compaction armed for a later attempt; it does
not retroactively fail a transaction that already committed. A writer-fence
mismatch still ends the session. A candidate that stops making progress is
abandoned and counted.

After a new `DR:` commits, `estuary.dev/truncated-at` may lag authority but must
never lead it. The update is monotonic and retried on startup and during normal
operation. Physical fragment deletion is handled separately.

## Recovery

Startup first recovers `DR:` and all disk `AI:` obligations. Each session
creates a new local image. If `DR:` exists, recovery repairs committed ACKs
before fixing and replaying the journal range.

### Startup without `DR:`

No `DR:` means no disk has ever committed. Recovery:

- does not read the journal;
- does not mutate an existing orphan journal;
- takes the read-only author snapshot needed for a possible later first-use
  fence; and
- starts with a fresh image.

An `AI:` value without `DR:` is invalid.

An absent journal is the common, healthy state. A journal containing records
from a failed first-use attempt is ignored until a later first use claims it.

### Startup with `DR:`

When `DR:` exists, startup:

1. requires the journal to exist;
2. chooses `E`, reads `R`, and claims the journal;
3. appends every recovered `AI:` value exactly and waits for its barrier;
4. fixes a replay range and rebuilds a fresh image;
5. creates the ublk device and starts the libkrun sandbox;
6. has the trusted guest bootstrap mount the rebuilt filesystem; and
7. starts the connector.

### Fixing the replay range

After fencing and ACK repair, the shard obtains a broker-confirmed write head
`H`. Recovery reads exactly `[O, H)`, where `O` is the offset in `DR:`.

The connector has not opened, the new fence excludes the old writer, and all
recovered intents are already inside `H`. Later irrelevant appends cannot
change what this attempt considers.

Recovery uses a disk-specific, single-journal reader. Deltas may have to wait on
a full that is still interleaved later in the range, so the reader spills to
local storage.

```text
O                                                                  H
│                                                                  │
▼                                                                  ▼
Full Begin(P)  Delta A Begin...End  Full Chunk(P)  Ack A  ... Full End(P)
                     │                           │
                     └──── spill until base ─────┘
```

### Reader rules

The reader makes one forward pass:

1. The record exactly at `O` must be a full `Begin`. Its producer identifies
   the designated full.
2. The reader applies exactly one complete stream from that producer. Other
   full attempts are decoded and validated but ignored.
3. `Fence` records are validated and make no disk change.
4. The reader follows Gazette `message.Sequencer` semantics for delta records:
   producer and clock group transactions, duplicate UUIDs are dropped, and
   only acknowledged records are released.
5. Delta extents are spilled, never applied before their `Ack`. A `pwrite`
   cannot be rolled back, and this design deliberately has no shadow image or
   undo log.
6. An ACKed delta waits until the designated full is complete, then applies in
   physical ACK order. The live ACK barrier makes this commit order.
7. An `Ack` without pending delta records is valid. This is the expected
   compaction case where the delta data precedes `O` but its ACK follows it.
8. A new session's delta producer abandons an older producer's unacknowledged
   stream. A later ACK for an abandoned stream is an ordering error.
9. At `H`, the designated full and every ACKed delta must be complete and
   applied. Unacknowledged spills are deleted.

The full writes only allocated extents into a fresh sparse image. `PUNCH`
extents hole-punch the recovered file. A rebuilt disk therefore remains as
sparse as the source. Applying `DATA` and `PUNCH` also rebuilds the allocated
bitmap. Replay itself does not mark blocks dirty because it is reconstructing
the committed baseline.

### Opening the result in the guest

The trusted guest bootstrap mounts the rebuilt image with the standard options.
The runtime does **not** clear the dirty bitmap afterward.

Mounting may update the superblock, set recovery flags, or replay the
filesystem journal. Unlike the fresh-disk format and first mount, these are
changes relative to an existing committed baseline. Clearing them would leave
the local image permanently ahead of replicas rebuilt from the disk journal.
The next delta therefore carries them.

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
