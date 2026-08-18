# disk-daemon

`disk-daemon` serves mounted filesystems whose durable state lives in Gazette
journals. Each live disk is a sparse local image exposed through Linux `ublk`.
The daemon records every accepted block mutation in a per-disk journal. It can
later rebuild the image from that journal.

A client controls durable boundaries with a session RPC. The daemon prepares a
disk delta and returns an opaque acknowledgement. The client records that
acknowledgement in the same atomic commit as its own state. It then returns the
acknowledgement to the daemon, which commits the delta. This makes the disk
advance with an external transaction even though the two systems use different
durable stores.

The local image is disposable. **The journal is the disk.**

The daemon is Linux-specific but not Flow-specific. Flow can use it as a client,
but this crate does not know about Flow checkpoints, task types, catalog models,
or connector protocols.

## Scope and guarantees

The crate includes:

- the `flow-disk-daemon serve` service;
- `client::Disk`, a client of that API which holds its rules;
- a bidirectional gRPC session API over a Unix socket;
- sparse images, `ublk` devices, ext4 formatting, and mounts;
- writer fencing, delta capture, and acknowledgement repair;
- validation that a journal is one a disk can be recovered from;
- recovery from committed deltas;
- recovery horizons, which keep the required journal range bounded, and the
  floors they derive; and
- an admin page, metrics, graceful shutdown, and crash cleanup.

The main guarantees are:

| Area | Guarantee |
| --- | --- |
| Durable state | Acknowledged journal deltas, plus acknowledgements recovered from the client's external commit, define the disk. |
| Transaction boundary | `Prepare` returns the exact acknowledgement needed to commit one point-in-time device state. |
| Crash recovery | A new sparse image is rebuilt from committed journal state. Uncommitted mutations are discarded. |
| Filesystem result | Recovery preserves files and their contents at the committed boundary. It does not promise an identical ext4 block image. |
| Writer ownership | At most one cooperative session may append to a disk journal. Gazette's `author` register fences older sessions. |
| Journal ownership | The client creates the journal and owns its specification. The daemon never creates, converges, or deletes one. |
| Unused disks | A disk which is opened and never written appends nothing at all, so an idle journal stays suspended. |
| Local lifetime | A normal session removes its mount and device. Its image has no directory entry and disappears with the process. |
| Isolation | The client receives a mounted directory which its own user owns. It does not receive a block device, image descriptor, or daemon privilege, and it needs none of its own. |

Mounting and unmounting can change ext4 bookkeeping such as mount counts,
timestamps, and journal state. Recovery therefore promises filesystem contents,
not byte-for-byte filesystem metadata.

The daemon runs privileged, but a client of it does not have to. A session takes
the peer credential of its Unix socket, which is the one identity a client cannot
claim falsely, and hands that user the root directory of the filesystem it serves.
`mkfs` sets the owner through `root_owner` rather than a `chown` after the format,
because a `chown` is a write and an unwritten disk must append nothing. A recovery
repairs the owner instead, and only when the replayed root belongs to somebody
else. The directory holding the mounts must still be one the client can traverse,
exactly as the directory holding the socket decides who may open a session.

The client must keep `device_size` stable for the life of a disk. It is the one
durable per-disk fact, and it is not stored in the journal. Block size is the
fixed 4 KiB of `disk_daemon::BLOCK_SIZE` rather than a per-disk value. The
device must contain at least one block and at most `u32::MAX` blocks.

Flow runtime integration is outside this crate. The runtime still needs to:

- create one disk journal per task shard, before its first session, and converge
  or delete that journal afterwards;
- place the returned mount in a connector sandbox;
- store prepared acknowledgements with Flow checkpoints;
- persist the recovery floors sessions return, and hand the greatest one back at
  the next `Open`;
- delete fragments below a persisted floor;
- choose connector protocol boundaries that keep the disk quiet; and
- define shard splitting.

The daemon also has no disk-count limit or local-capacity policy. It reports
capacity and lets the deployment decide when to add hosts or reject work.

## Architecture

One session owns one disk for the lifetime of one gRPC stream.

```text
                         Unix-socket gRPC
transaction client ───────────────────────────────► Session
       ▲                                               │
       │ Opened(mount path)                            │ controls
       │                                               ▼
workload ── POSIX I/O ──► ext4 mount ──► /dev/ublkbN ──► Linux ublk
                                                        │
                                                        ▼
                                              owner thread + io_uring
                                                │                 │
                                      image I/O │                 │ mutation chunks
                                                ▼                 ▼
                                         sparse O_TMPFILE    bounded capture
                                                                  │
                                                                  ▼
                                                          journal Writer
                                                                  │
                                                                  ▼
                                                             Gazette
```

The implementation has three long-lived roles:

- **Session task.** It owns the RPC state machine, mount, disk, and journal
  writer. It opens the disk, cuts publication boundaries, and tears everything
  down.
- **Owner thread.** One thread serves one `ublk` queue. It is the only code
  that mutates that disk's image and bitmaps.
- **Journal writer task.** It owns one session's producer and writer epoch. It
  drains captured mutations, appends records, prepares acknowledgements, and
  advances recovery horizons.

A terminal error tears down that session and its disk. It does not stop other
sessions. A daemon drain ends all sessions.

### Concepts

**Image** is the sparse local file behind a device. It is a working copy, not
durable state.

**Mutation** is one accepted write, discard, or write-zeroes device request.

**Chunk** is the durable encoding of part of a mutation. A data chunk allocates
blocks. A punch chunk deallocates blocks.

**Delta** is every chunk appended since the previous commit. It may also
contain unchanged blocks copied for compaction.

**Cut** is the point-in-time boundary made by closing mutation admission after a
`syncfs`.

**Acknowledgement** is the exact serialized Gazette `ACK_TXN` record for a
delta. `Prepare` returns it without appending it.

**Fence** is an `OUTSIDE_TXN` record that atomically installs a new writer
epoch in the journal's `author` register.

**Recovery floor** is the oldest journal position still needed to rebuild a
disk. The daemon derives it and returns it. The client persists it.

**Horizon** is a candidate recovery floor. It becomes the floor after every
allocated block has a committed copy at or after that position.

## Session protocol

The protocol is defined in
[`go/protocols/disk/disk.proto`](../../go/protocols/disk/disk.proto). A
`Session` is one bidirectional stream. Requests and replies stay in order. The
daemon sends no unsolicited messages.

| Request | Reply | Meaning |
| --- | --- | --- |
| `Open` | `Opened` | Create or recover one disk, mount it, and return the absolute mount path and any floor this recovery derived. |
| `Prepare` | `Prepared` | Cut and finish one delta. Return its unappended acknowledgement, or empty bytes when nothing changed. |
| `Commit` | `Committed` | Append the exact prepared acknowledgement, wait for broker confirmation, and return any floor the commit established. |
| `Broker` | none | Replace the session's Gazette endpoint and credential. |

`Open` must be first and may be sent once. It supplies:

- the name of a journal which already exists;
- device size;
- a required broker endpoint and an optional bearer credential;
- acknowledgements that the client committed externally but may not have
  reached the journal; and
- the recovery floor the client last persisted, which seeks the replay.

The basic state machine is:

```text
new ── Open ──► opening ── Opened ──► serving
                                          │
                    ┌─────────────────────┴─────────────────────┐
                    │                                           │
                    ▼                                           ▼
               Prepare(empty)                             Prepare(ack)
                    │                                           │
                    └──────────────► serving ◄── Committed ◄── Commit

any state ── EOF, cancellation, or error ──► teardown ──► closed
```

Only one acknowledgement may be outstanding. The client must not send another
`Prepare` until it has sent `Commit`. The daemon reads requests in order, so a
`Broker` sent behind another request waits for it. The client must send
`Broker` well before its credential expires.

The endpoint and credential always change together. An empty credential means
anonymous access. The daemon does not mint tokens or hold a signing key.

### Using the client

`client::Disk` is the crate's client of this protocol, and both examples are written
on it. It holds no protocol state of its own, because the daemon is the
authority on the rules of the exchange:

- `prepare` returns the acknowledgement bytes, and `commit` takes them back. A
  `Commit` of anything else, or a second `Prepare` before its `Commit`, is refused by
  the daemon with `FAILED_PRECONDITION`.
- `Open` and `Commit` each report the recovery floor they established, as
  `Option<u64>`. The client persists the greatest one it is given; nothing caches it
  on its behalf.
- `Error` sorts a failure by what the caller may do next, from the codes
  `session::failed` produces, and `Error::is_transient` states the retry policy as
  `gazette::Error` and `catalog_stats::Error` do. The codes do not mean the same
  thing to every client here: `ABORTED` is retryable for BigTable, and for a disk it
  is a lost fence which no retry undoes.
- Every `Disk` of one `Client` shares its connection, so a program which serves many
  disks connects once. One connection is also one identity, since the daemon reads
  the peer credential of the socket to decide who owns each mount.

`tests/daemon.rs` speaks the gRPC directly rather than through this type. It has to:
its cases assert what the daemon does with a second `Prepare`, a `Commit` of bytes
that were never prepared, and a `Prepare` before `Open` — requests this type cannot
express.

### Client boundary contract

The daemon makes a coherent device cut. It cannot know whether that cut matches
the client's external transaction. The client and workload must:

1. Finish and flush application state that belongs to the transaction.
2. Stop logical disk writes at the external boundary.
3. Run `Prepare`, store the acknowledgement with external state, and run
   `Commit`.
4. Release the workload for the next transaction.

The daemon calls `syncfs`, so the cut includes writes already issued to the
filesystem. It cannot include an application write that has not been issued
yet. If the workload crosses its logical boundary, the recovered filesystem is
still coherent but may not match the client's checkpoint.

## Linear walkthrough

### 1. Open

The daemon lists the journal and validates its specification before it creates a
device. An absent journal, a specification a disk could not be recovered from, or
invalid device geometry therefore fail early.

The daemon then probes once for the write head and the current `author` register.
That probe refuses to resume a suspended journal, so it is safe to issue whatever
the listing said. A journal which answers it is awake, and its answer is the
authority. A journal which refuses is asleep, and a sleeping journal cannot
change, so its listing supplies the head Gazette recorded when it suspended.

Journal content, not journal existence, then chooses one of two paths:

**Fresh disk**

1. Create a sparse image with `O_TMPFILE` and `ftruncate`.
2. Create and start a `ublk` device over the image.
3. Format the device as ext4, giving its root directory to the client.
4. Mount it under the configured mount directory.
5. Drop all format and mount mutations from the capture channel.
6. Return the mount path, and a zero floor.

The journal is not claimed during this path. A disk that is opened and never
changed appends nothing at all.

**Recovered disk**

1. Claim the journal before reading or repairing it.
2. Append each recovered acknowledgement exactly as supplied.
3. Fix a broker-confirmed journal head for this recovery.
4. Seek from the client's floor hint and replay committed records into a new
   image.
5. Create the `ublk` device over the rebuilt image.
6. Mount the filesystem, and return its path with any floor the replay derived.
   Give its root directory to the client if the replay left it to somebody else.

A journal that contains only orphan records from a failed first use is claimed
and replayed, but it still produces a fresh filesystem. The fence is the same
fence that the session would need for its next first append.

Recovered acknowledgements require journal content that replays. A client can
hold one only for a journal whose data appends a broker confirmed, so an empty
journal means committed state was deleted — and a journal whose replay applies
no records means the same, even when its head outlived them. The acknowledged
records are the newest the disk has, so no legal floor hint can seek past them.
Either way the `Open` fails rather than serve a fresh disk over the loss.

### 2. Serve block I/O

The kernel sends reads, writes, discards, and write-zeroes requests to the owner
thread. Reads come from the image. Every mutation is encoded and offered to the
bounded capture channel before the image operation is submitted.

The owner completes a request when its image operation completes. It does not
wait for that mutation to become durable in Gazette. The capture bound applies
backpressure when the writer cannot keep up.

### 3. Prepare a boundary

`Prepare` runs these steps:

1. Call `syncfs` on the mounted filesystem.
2. Stop admitting new mutations. Reads continue.
3. Wait until every admitted mutation has reached the image.
4. Drain the capture channel and wait for every journal append to finish.
5. Sample whether this delta completed an open horizon.
6. Build the acknowledgement, but do not append it.
7. Resume device admission and return `Prepared`.

Closing admission is the exact cut. Each mutation is wholly before or after it
because capture happens before the image operation.

The writer stops taking new mutations while an acknowledgement is outstanding.
This prevents Gazette from joining two deltas into one pending transaction.
Filesystem writes after the cut may therefore wait for `Commit`. Clients
should keep this interval short.

If the delta contains no records, `Prepared.ack` is empty. The client has no
disk obligation and sends no `Commit`.

### 4. Commit with external state

The client treats the acknowledgement as opaque bytes:

```text
client                         daemon                         Gazette
   │                              │                              │
   ├──── Prepare ────────────────►│                              │
   │                              ├─ finish data appends ───────►│
   │◄── Prepared(ack bytes) ──────┤                              │
   │                              │                              │
   ├─ atomically store:           │                              │
   │    client state + ack        │                              │
   │                              │                              │
   ├──── Commit(same bytes) ─────►│                              │
   │                              ├─ append ACK_TXN ────────────►│
   │                              │◄─ broker confirmation ───────┤
   │◄── Committed ────────────────┤                              │
```

This split closes every crash window:

| Crash point | Recovery result |
| --- | --- |
| Before the client stores the acknowledgement | The data records have no acknowledgement. Replay discards that delta. |
| After the client stores it, before `Commit` reaches Gazette | The next `Open` receives it in `recovered_acks` and appends it verbatim. |
| After the acknowledgement append lands | Gazette de-duplicates a repair by UUID. Replay includes the delta once. |

The client must store the acknowledgement in the same atomic transaction as the
state that depends on the disk. It must return the exact bytes. A different
acknowledgement cannot safely commit the prepared delta.

### 5. Close

EOF, cancellation, and every error use one teardown path:

1. Abandon further journal appends.
2. Keep draining and discarding mutations so unmount cannot deadlock.
3. Unmount the filesystem.
4. Stop the `ublk` device, which aborts its pending fetches.
5. Let the owner close the character device and return the image.
6. Delete the `ublk` device.
7. Drop the anonymous image.

The writer cancels any broker call still in progress. Broker operations retry
transient failures while a session is live, but teardown must not wait for an
unreachable broker.

## Block serving

### One thread and one ring per disk

`ublk` binds a queue to the thread that arms its first fetch. A later queue
command from another thread fails with `EINVAL`. Tokio tasks may move between
worker threads, so each disk has a dedicated owner thread and `io_uring`.
`owner::spawn` creates and arms the ring on that thread.

The owner is also the only mutator of the image, allocated bitmap, and horizon
bitmap. These structures need no atomics or locks.

Each disk has one queue. Queue depth provides concurrency. A request moves
through the ring as a sequence of completion steps:

```text
read:
FETCH_REQ ──► read image ──► write /dev/ublkcN ──► COMMIT_AND_FETCH_REQ

write:
FETCH_REQ ──► read /dev/ublkcN ──► offer chunks ──► write image
                                                       │
                                                       ▼
                                             COMMIT_AND_FETCH_REQ
```

Discard and write-zeroes requests skip the data transfer and punch the image.
`Step` in `owner.rs` is packed with the request tag in `io_uring.user_data`
so one completion identifies both the request and its next operation.

Owner threads and rings are per disk. Kernel `io_uring` workers are not. Every
disk ring attaches to one anchor ring with `IORING_SETUP_ATTACH_WQ`, and the
daemon registers a process-wide worker limit. This avoids one kernel worker pool
per disk.

The `ublk` ABI comes from bindgen over the vendored Linux UAPI header
`ublk_cmd.h`. The code uses ioctl-encoded `UBLK_U_CMD_*` operations. Legacy
raw opcodes depend on a kernel option that current kernels do not enable. The
control ring uses `IORING_SETUP_SQE128`, because a control command does not fit
an ordinary submission entry.

The device uses `UBLK_F_USER_COPY`. Request data moves with `pread` and
`pwrite` on `/dev/ublkcN`; only request descriptors are mapped. The crate does
not use `libublk`, because it needs its own completion-state packing and queue
ownership.

The device advertises no volatile write cache. The kernel therefore sends no
flush or force-unit-access requests, and the owner implements neither. The
local image is disposable, so durability comes from journal publication rather
than a device cache. ext4 still orders its own journal by completed writes.

### Mutation order and backpressure

The capture path preserves one ordering rule:

> Journal order is image mutation order.

For a write, the owner reads the request data, encodes its chunks, and offers the
whole mutation to the capture channel before it submits the image write. A
discard or write-zeroes request offers a punch before it punches the image.

`inflight::InFlight` prevents overlapping mutations from being applied at the
same time. Filesystems normally avoid overlapping concurrent writes, so this set
is usually empty. The guard still makes the ordering rule explicit.

The capture channel is bounded in mutations. A full channel parks only the
request that needs room. When the writer takes a mutation, it wakes the owner,
which retries requests in arrival order. A mutation is never split across two
deltas.

A device write is complete after its image write lands. Gazette durability is
checked later at `Prepare`. This keeps normal block I/O asynchronous while the
bounded channel limits how far it may run ahead.

## Local image and filesystem

### Sparse image

An image is an `O_TMPFILE` file in `--image-dir`. It has mode `0600`, no
directory entry, and the logical size requested by `Open`. `ftruncate` leaves
the entire file as holes.

Byte `N` of the image is byte `N` of the device. Writes allocate blocks.
Aligned discards and write-zeroes punch holes. The host filesystem must support
hole punching.

The allocated bitmap records which device blocks currently occupy host space.
It is exact at the daemon's block granularity. `st_blocks` is not exact enough
because the host filesystem may delay allocation. There is no dirty bitmap:
every mutation is captured as it arrives.

The bitmap costs one bit per device block. A 10 GiB disk with 4 KiB blocks uses
320 KiB for the allocated bitmap. The horizon bitmap has the same size but
exists only while a horizon is open.

### Chunk encoding

Each journal chunk covers a contiguous block range:

```text
Chunk
┌───────────────┬──────────────────────────────────────────────┐
│ block: u32    │ first block                                 │
├───────────────┼──────────────────────────────────────────────┤
│ data: bytes   │ allocated content beginning at that block   │
│       or      │                                              │
│ punch: u32    │ number of blocks to deallocate              │
└───────────────┴──────────────────────────────────────────────┘
```

The encoder trims trailing zero bytes from data. Replay writes the remaining
bytes and explicitly zero-fills the rest of the covered blocks. An empty
`data` value means one allocated zero block. It is not a hole. Only `punch`
deallocates.

These rules reproduce both content and allocation. Chunks need not be sorted or
unique. Replay applies them in journal order, so the last chunk covering a block
wins.

### ext4

ext4 is the only filesystem implementation. The choice is isolated in
`filesystem.rs`, so no ext4 detail appears in the journal format or recovery
algorithm. Adding another filesystem would require the same crash and recovery
test matrix.

A fresh filesystem uses:

- the daemon's 4 KiB block size, so no device request can straddle a block;
- zero reserved blocks;
- `assume_storage_prezeroed=1`;
- no whole-device discard during format; and
- e2fsprogs 1.47 or later.

`assume_storage_prezeroed` leaves unused inode tables and the ext4 journal as
holes. This keeps the first publication small and avoids later background
initialization writes.

Fresh and recovered filesystems mount with:

```text
noatime,nodev,nosuid,noexec,discard
```

`noatime` prevents reads from creating deltas. `discard` lets ext4 return
freed blocks to the sparse image. A sandbox must apply `nodev`, `nosuid`, and
`noexec` again when it re-exports the directory, because host mount options do
not propagate through every bind or virtio-fs mount.

Format and first mount mutations are not retained in memory or appended as they
arrive. The first mutation after mount causes the owner to snapshot allocated
image blocks in bounded batches. The writer appends that snapshot before all
captured mutations. This has three useful properties:

- an unused fresh disk appends nothing to its journal;
- repeated format writes to one block collapse into one final block value; and
- memory does not scale with device size.

A mutation may already be present in the snapshot. This is safe. Its captured
chunk follows the snapshot and applies the same mutation again.

A recovered disk takes no snapshot. Its filesystem is already in the journal.
Writes made by mounting it, including ext4 journal replay, belong to the next
delta.

## Journal design

### Journal ownership and validation

`Open` names a journal which already exists. The client creates it, converges it,
and deletes it. The daemon never applies a journal specification, and never
writes to etcd at all.

That division follows from who knows what. Replication, fragment stores, flush
and refresh intervals, and append ceilings are a deployment's own vocabulary. A
value the daemon invented for one of them would become a permanent property of
every disk created by that daemon version.

What the daemon does know is what a disk can be recovered from. It reads the
listed specification at `Open` and refuses one that breaks a recovery rule:

| Rule | Why |
| --- | --- |
| `flags` is `NOT_SPECIFIED` or `O_RDWR` | The daemon both appends to this journal and replays it. |
| The compression codec is one `gazette::journal::read` decodes | The daemon reads the journal back to rebuild the disk. |
| `fragment.retention` is unset | Gazette deletes fragments by age, and age cannot see the recovery floor, so any retention risks deleting records a live disk needs. |
| `fragment.path_postfix_template` is empty | Date-prefixed fragment paths are what a bucket lifecycle rule keys on, which is age-based deletion by another route. |

These are refused rather than fixed. The specification belongs to the client, so
a daemon which quietly corrected one would be deciding a durable property of a
disk it only serves. Physical fragment deletion follows the recovery floor, and
belongs to the client too.

### Journals of unused disks

An idle disk must cost an etcd entry and nothing more. Gazette's `--auto-suspend`
brokers suspend a journal with an empty fragment index at `FULL`, which scales it
to zero replicas.

Any append resumes a suspended journal, because `AppendRequest.Suspend` defaults
to `SUSPEND_RESUME`. The daemon's zero-byte probe therefore carries
`SUSPEND_NO_RESUME`, which fails with `SUSPENDED` instead of waking one. Reading
a journal never wakes it.

Gazette answers a suspended journal with a status and not a head, so the head
comes from the journal's own listing. Every suspension records the head it
suspended at, and a resumption rolls forward to it, so:

| Probe | Listing | Open |
| --- | --- | --- |
| answers | — | Head and author of the probe, whatever the listing said. |
| `SUSPENDED` | suspended at offset zero | Fresh disk: head zero, and no author, because only an append carrying content sets a register. |
| `SUSPENDED` | suspended above zero | Content a recovery must read. Probed again with the resuming default, which the recovery needs anyway. |
| `SUSPENDED` | no suspension recorded | The listing predates the suspension. Listed again, which settles it. |

The probe is issued even where the listing looks empty. It costs one status,
which Gazette resolves from a broker's own key space without a replica, and it
is what makes a stale listing harmless: a journal which was resumed and written
since that listing answers with its real head, and the disk recovers instead of
serving an empty filesystem over committed state.

A disk which is opened, formatted, mounted, and never written appends nothing
across any number of sessions. Not even a fence lands, because the fence is
deferred to the session's first real append.

### Record format

`DiskRecord` is encoded with Gazette fixed Protobuf framing. The Gazette UUID
carries producer, clock, and transaction flags.

```text
fence:
  OUTSIDE_TXN { installs_epoch, no chunks }

delta:
  CONTINUE_TXN { chunks, opens_horizon? }
  CONTINUE_TXN { chunks }
  ...
  ACK_TXN      { no chunks }
```

One session producer writes all of its deltas. A replacement session uses a new
producer. Each fence record uses a separate producer and installs the session
producer as the epoch.

One device mutation becomes one record. A writer drain packs several records
into one Gazette append. The append byte stream is sent in 32 KiB transport
chunks so it does not depend on the broker's gRPC message limit. Those chunks
are not visible in the durable record format.

Appends are issued one at a time and awaited. A transient retry sends identical
bytes. Gazette de-duplicates repeated UUIDs during replay.

Record clocks follow wall time as well as record order. A recovery floor is the
opening record's clock, and a recovery turns it back into a fragment
modification-time seek. A clock that only ticked per record would drift behind
long-running sessions.

The protocol currently has Rust bindings but no Go bindings. Nothing in Go
consumes the API yet, so none are generated. The proto3 `optional` fields which
this repository's `protoc-gen-gogo` could not generate are gone, so adding
bindings is now a matter of listing the file in `mise/tasks/build/go-protobufs`.

### Writer fencing

Each journal has an `author` register. A session chooses a fresh epoch `E`
and reads the current value once as `R`. It claims the journal by appending
`Fence(E)` while atomically changing `author` from `R` to `E`. Every later
append checks that `author=E`.

The session never refreshes `R`. If another session replaces it, the old
session cannot read the new value and take the journal back.

An append RPC can fail after its fence has landed. The session resolves this by
probing for its own epoch. It never chooses a second epoch for the retry.

Fencing happens at different times:

- An empty journal is claimed on the session's first append, so an unused disk
  never claims one at all.
- A journal with content is claimed before acknowledgement repair or replay.

The compare-and-swap is also the backstop for every race the listing and the
probe could have lost. Whatever the journal was when the session looked, only
one epoch installs itself over the author that session read.

The register chooses one cooperative writer. It is not commit authority.
Committed records and recovered acknowledgements remain authoritative even if
register state is lost.

## Recovery

Recovery runs before a device can read the rebuilt image:

1. Claim the journal.
2. Append recovered acknowledgements.
3. Probe a broker-confirmed head `H`. This fixes the end of the recovery.
4. Turn `Open.floor_hint` into a fragment modification-time seek. A zero hint
   seeks zero, and reads from the first fragment still in the store.
5. Replay the fixed range up to `H` into a new sparse image.
6. Rebuild the allocated bitmap and any open horizon.
7. Mount the image, and return any floor the replay derived in `Opened.floor`.

The floor is a seek hint, not a record filter. A missing or stale hint starts
the read earlier and costs more work. Filtering by clock could remove a record
from the middle of a delta and is not valid.

Replay validates fence records, sequences each producer, removes duplicate
UUIDs, and applies chunks in physical journal order. The live writer allows
only one pending delta, so this order is also commit order.

Replay does not buffer a pending delta. It applies records during its first
pass. At the fixed head it knows which producer ranges were never acknowledged.
If any exist, it resets the image and reads the range once more while skipping
those records.

One extra pass is enough. An unacknowledged delta is the last delta of its
producer, although a replacement producer may have appended later records after
it. The second pass handles both trailing and mid-range orphan records without a
spill file.

A range may begin in the middle of an old delta. A completed horizon guarantees
that every allocated block has a newer copy at or after the floor, so records
below it are not needed.

## Bounded recovery

Without compaction, replay time and retained journal data would grow for the
life of a disk. A recovery horizon moves the floor forward.

The invariant is:

> Every allocated block has a committed copy at or after the horizon.

```text
before:
old records ──► floor ├──────── required recovery range ────────┤ head

horizon open:
old records ──► floor ├──────► horizon ├────────────────────────┤ head
                                  │
                                  └─ blocks still needing a newer copy

horizon complete:
discardable records ──────────► new floor ├─────────────────────┤ head
```

When a horizon opens, the owner copies the allocated bitmap into a horizon
bitmap. Each later captured chunk clears the blocks it covers. The chunk may
come from:

- a normal device mutation; or
- an unchanged block copied from the image.

Normal rewrites are free compaction work. The daemon copies only blocks that the
workload does not rewrite.

A delta earns copy budget from its changed data:

```text
copy budget = changed data bytes × horizon-copy-ratio
```

With the defaults, a delta may copy 0.5 bytes for each changed byte. Compaction
therefore adds at most 50% data-byte amplification to that delta. Unused budget
does not carry into the next delta.

A horizon opens when:

```text
journal bytes above floor > max(horizon-minimum-bytes,
                                allocated bytes × horizon-open-ratio)
```

The defaults are a 2.0 open ratio and a 1 GiB minimum. For a disk above the
minimum with sustained writes, the design target is a required range of roughly
five times its live allocated data. This is a policy estimate, not a hard byte
limit.

The opening flag is on the first record of a delta. Replay snapshots committed
allocation before it applies that record's chunks. The live owner opens its
horizon when the writer is about to append the record. It may already include a
block changed by the opening delta. This only makes the live pending set larger;
the writer cannot complete the horizon before replay would. A failed opening
delta creates no durable horizon.

The live owner clears a bit when it captures a chunk, before that delta commits.
This early clear cannot escape the session. A failed delta ends the session, and
the next session rebuilds the bitmap from committed records.

An incoming mutation supersedes an in-progress copy of the same block. The
mutation itself appends the newer value, so the copy does not need to block
it.

At a publication cut, the writer asks the owner how many horizon blocks remain.
If none remain, committing that delta completes the horizon. The writer moves
the floor to the opening record and asks the owner to drop the bitmap.

If a disk stops changing, an open horizon pauses. Its journal also stops growing.
A replacement session reconstructs and resumes an open horizon from ordinary
records.

### Who keeps the floor

The daemon derives the floor and returns it. The client persists it. The daemon
writes nothing durable of its own about a disk, which is what keeps it out of
etcd and out of the journal specification.

A floor travels as the opening record's message clock, a `fixed64`:

| Reply | When it is nonzero |
| --- | --- |
| `Committed.floor` | This commit completed a recovery horizon. |
| `Opened.floor` | This recovery's replayed range completed one. |

`Opened.floor` is what makes the scheme self-healing. A session which completes a
horizon and then dies before its client stores the floor loses nothing: the next
recovery reads the same records and derives the same floor, and reports it on
that session's behalf.

The client's rule is one line:

> Persist the greatest floor this daemon has returned for this journal, and hand
> it back as the next `Open.floor_hint`.

The store is best-effort. A floor which is lost, or written late, costs a later
replay some work and cannot change what that replay produces. Nothing is ever
held up waiting for one.

The one value which must never be presented is a floor ahead of the true one. A
hint seeks past fragments, so an inflated one skips records the disk still needs
and loses data silently. A client must therefore only echo values this daemon
returned for this journal, and must never invent, round, or advance one itself.

Fragments below a persisted floor are the client's to delete. That deletion is
what turns a bounded recovery range into bounded storage.

## Operating the daemon

### Host requirements

The daemon validates its host before it accepts sessions. It requires:

- Linux 6.2 or later with `ublk_drv` loaded and `UBLK_F_USER_COPY`;
- access to `/dev/ublk-control` and the `/dev/ublkcN` nodes it creates;
- e2fsprogs 1.47 or later;
- an image directory on a filesystem that supports hole punching; and
- a mount directory owned by the daemon.

Startup probes these requirements and reports an actionable error. For example,
it asks the operator to run `modprobe ublk_drv` if the module is not loaded.

On Debian and Ubuntu, `ublk_drv` is commonly in
`linux-modules-extra-$(uname -r)`, not the kernel-image package. Install or pin
the matching modules-extra package with each kernel upgrade.

### Privileges

The daemon needs `CAP_SYS_ADMIN` for `ublk` and mounts. It does not need
`CAP_DAC_OVERRIDE`, `CAP_SYS_MODULE`, or `CAP_NET_ADMIN`.

Running as root satisfies the device permissions. A dedicated UID with ambient
`CAP_SYS_ADMIN` also needs a udev rule that grants it
`/dev/ublk-control` and `/dev/ublkc*`. `CAP_SYS_ADMIN` does not bypass file
permissions.

The daemon does not call `chown`. It writes only in its configured image and
mount directories, apart from the Unix socket path.

The workload remains unprivileged. It receives only the mounted directory.

### Security boundary

The workload cannot write raw blocks or construct ext4 metadata directly. It
can only make normal filesystem calls. The host kernel therefore parses ext4
metadata that the host kernel created.

This is still the same exposure as any writable container volume. A hostile
workload can create deep trees, many links, large attributes, fragmentation, or
rename storms. A virtio-fs sandbox also adds its normal translation layer. Each
recovered image represents a power-loss point, so ext4 journal replay runs more
often than it would for an ordinary host volume.

### Socket access control

The daemon sets its session socket to mode `0666`. It has no user model, so
the parent directory controls who can reach it.

A session consumes a device, a thread, a mount, descriptors, and local storage.
A world-traversable socket directory therefore permits local denial of service.
Use a directory owned by the daemon user, grouped to authorized clients, with
mode `0750`.

Systemd socket activation could express the same policy with `SocketMode` and
`SocketGroup`, but the daemon does not support socket activation today.

### Command-line configuration

`flow-disk-daemon serve --help` is authoritative. Every flag also reads the
same unprefixed environment variable.

Required service flags:

| Flag | Meaning |
| --- | --- |
| `--uds-path` | Unix socket for session RPCs. |
| `--image-dir` | Directory for sparse images. Stripe several drives below this directory instead of exposing host topology to clients. |
| `--mount-dir` | Directory owned by the daemon for per-session mounts and startup reclaim. |

Optional service flags:

| Flag | Default | Meaning |
| --- | --- | --- |
| `--admin-port` | disabled | Serve the loopback-only admin page and Prometheus metrics. |
| `--log-format` | `text` | `text` or `json`. |
| `--horizon-open-ratio` | `2.0` | Journal-range multiple that opens a horizon. |
| `--horizon-copy-ratio` | `0.5` | Unchanged bytes copied per changed byte. |
| `--horizon-minimum-bytes` | 1 GiB | Minimum journal range before a horizon opens. |

Device size, journal name, floor hint, broker endpoint, and credential are
session inputs. They are not daemon flags. Device size is a durable per-disk
fact, and a daemon restart must not reinterpret it. Block size is neither: it is
the fixed 4 KiB of `disk_daemon::BLOCK_SIZE`.

Compaction flags are policy. They may change between restarts because replay
derives horizon state from the journal.

### Data-plane preconditions

The client, not this daemon, provisions journals. Two deployment facts follow:

- **Brokers should run `--auto-suspend`.** A disk which is provisioned and never
  written appends nothing, so its journal keeps an empty fragment index. Only
  auto-suspension turns that into zero replicas and an etcd entry. Without it,
  every provisioned disk costs broker capacity whether it is used or not.
- **Something must delete fragments below the floor.** The daemon derives floors
  and returns them, and it deletes nothing. The component which persists a
  disk's floor is the one which must act on it.

### Service limits

A unit file should set these limits explicitly:

- **`TasksMax`.** Count the Tokio workers, one owner thread per disk, the
  shared `io_uring` workers, and a small blocking pool. The default depends on
  `kernel.pid_max` and varies by host.
- **`TimeoutStopSec`.** Set it above the daemon's 30-second drain. Sixty
  seconds is a practical value.
- **`LimitNOFILE`.** A disk holds about four direct descriptors: image,
  character device, ring, and wake descriptor. Broker connections add more.
- **`Restart=on-failure`.** Restart is safe when the new process uses the same
  mount directory and can reclaim the old process's mounts.

Do not use `PrivateTmp` or `PrivateMounts`. The mount paths returned to
clients must be visible to the process that places them in a sandbox.

One soak measurement on a ten-core host produced:

| State | Total threads | Owner threads | `io_uring` workers | Descriptors |
| --- | ---: | ---: | ---: | ---: |
| Idle daemon | 12 | 0 | 1 | 13 |
| Serving 6 disks | 39 | 6 | 19 | 44 |
| After all disks close | 20 | 0 | 5 | 16 |

Some shared workers and blocking-pool threads idle after the last disk closes.
Owner threads must return to zero.

On that host, 100 disks need roughly 230 to 250 threads: the base workers, 100
owners, the shared `io_uring` pool near its ceiling, and blocking workers. Only
the owner term keeps growing after the shared pool reaches its limit.

A parked owner reserves a 256 KiB user stack and has a small kernel stack. A
128-entry ring is about 12 KiB. These are bounded per-disk costs.

The kernel's `ublks_max` parameter counts unprivileged devices. It does not
limit the privileged devices created by this daemon. The daemon still reports
it because it is a common first diagnostic.

### Shutdown and crash cleanup

`SIGTERM` and `SIGINT` start a drain. The daemon first unlinks the session
socket. It then ends every session with `UNAVAILABLE` and waits until each
session has removed its disk.

The drain lasts at most 30 seconds. If disks remain, the daemon exits non-zero
and reports each journal and session phase. If no disk remains but a client
connection is still open, it warns and exits successfully.

Prefer `SIGTERM` to `SIGKILL`. A kill during a device request can leave the
owner in uninterruptible kernel sleep while a mounted filesystem still issues
I/O. Later `ublk` control commands on the host can then block behind it. A
reboot may be the only recovery. If a hard kill is unavoidable, unmount the
served filesystems first.

After a process crash, the anonymous images disappear and the kernel removes
the block devices when their serving process exits. Mounts and character
devices can remain. At startup, the next daemon scans its own mount directory,
unmounts inherited mounts, and deletes only devices whose recorded serving
process is gone. It leaves devices it cannot prove it owns.

### Observability

With `--admin-port`, the daemon serves
`http://127.0.0.1:<port>/` and `/metrics`. Both are loopback-only and have no
authentication.

Each live session appears as a `Disk.Session` handler. Its label is the journal
and its phase is one of `opening`, `serving`, `preparing`, `committing`,
or `closing`. The admin page can change a handler's logging level at runtime.

Per-journal metrics:

| Metric | Meaning |
| --- | --- |
| `disk_daemon_allocated_bytes` | Host bytes allocated by this disk image. |
| `disk_daemon_recovery_range_bytes` | Journal bytes above the recovery floor. |
| `disk_daemon_floor_seconds` | Wall-clock second used as the replay seek. |
| `disk_daemon_horizon_pending_blocks` | Blocks still owed by the open horizon; zero when none is open. |
| `disk_daemon_horizons_completed` | Horizons completed. Each advances the floor. |
| `disk_daemon_appended_records` | Journal records appended. |
| `disk_daemon_appended_bytes` | Framed record bytes appended. |
| `disk_daemon_prepares` | Non-empty deltas prepared. |
| `disk_daemon_commits` | Acknowledgements appended and confirmed. |
| `disk_daemon_admission_stalls` | Mutations refused by a full capture channel or a closed cut. |
| `disk_daemon_parked_requests` | Device requests waiting for admission or capture capacity. |

Host metrics:

| Metric | Meaning |
| --- | --- |
| `disk_daemon_host_allocated_bytes` | Sum of allocated image bytes for all live disks. |
| `disk_daemon_image_dir_free_bytes` | Space available to the daemon in the image filesystem. |
| `disk_daemon_devices` | Devices currently served. |
| `disk_daemon_devices_max` | Reported `ublks_max`; informational for these privileged devices. |

Alert on free image space relative to live allocated bytes. The daemon reports
capacity but does not reserve it or reject sessions before `ENOSPC`.
An image-write failure errors that device request. ext4 normally contains the
failure by remounting the filesystem read-only.

### Client-visible failures

Every session failure is terminal. The gRPC code is the stable part a client can
act on:

| Code | Meaning |
| --- | --- |
| `INVALID_ARGUMENT` | The request is invalid. Retrying the same request cannot work. |
| `FAILED_PRECONDITION` | The request is out of turn, such as a second `Open`, a request before `Open`, or a `Commit` of a delta the session never prepared. |
| `ABORTED` | Another session took the journal fence. This session must not take it back. |
| `UNAUTHENTICATED` | A broker directly rejected the credential. Refresh it and open a new session. |
| `UNAVAILABLE` | The daemon is draining or a broker is unreachable. Another daemon or host may work. |
| `INTERNAL` | The daemon, device, host, or an otherwise unclassified operation failed. |

`FAILED_PRECONDITION` also covers the three writer protocol violations, which are
client state errors rather than bad requests: a second `Prepare` while one is
outstanding, a `Commit` with nothing prepared, and a `Commit` with different bytes.
`OutOfOrder` in `lib.rs` marks them, as `Invalid` marks a request which was wrong
in itself.

A client with a short-lived Gazette token must send `Broker` well before the
token expires. The daemon does not hold a delta while waiting for a
replacement, and ext4 can write on its own schedule. An expired token may fail
the broker operation rather than authentication itself, so it does not always
appear as `UNAUTHENTICATED`.

### Provisioning a disk journal

A disk journal must exist before its first session, because the daemon creates
none:

```console
$ cat <<'YAML' | gazctl journals apply --specs /dev/stdin
journals:
  - name: acmeCo/disk/scratch
    replication: 1
    fragment:
      length: 67108864
      stores: [s3://example-bucket/disks/]
      compressionCodec: SNAPPY
      refreshInterval: 5m
      flushInterval: 1h
YAML
```

A client then opens that journal as a disk with `client::Disk`, and it owns the
specification from then on. `examples/basic.rs` is a worked example, and
`examples/demo-services.sh` starts a daemon and a broker to run it against.

`Open` and `Commit` each report a recovery floor when they establish one. Persist
the greatest, hand it back as the next `Open`'s `floor_hint`, and delete fragments
below it. Losing one only makes the next recovery read more.

## Testing

Build Gazette once so broker-backed tests can find it:

```console
mise run build:gazette
cargo nextest run -p disk-daemon
```

The nextest command runs unit, property, broker-backed, and privileged tests.
Privileged work runs in `sudo -n` child processes, not in Cargo, so the target
directory stays owned by the user. Tests fail with an actionable message when
`ublk_drv`, `/dev/ublk-control`, or passwordless sudo is unavailable. A
nextest test group serializes tests that share the host-wide control device.

Lint with:

```console
cargo clippy -p disk-daemon --all-targets --no-deps
```

`--no-deps` is intentional. A dependency currently fails before Clippy reaches
this crate when dependencies are included.

`tests/daemon.rs` runs `flow-disk-daemon` as shipped. It covers fresh disks,
multi-transaction recovery, lost acknowledgements, uncommitted deltas,
mid-writeback cuts, horizons, broker outage, credential replacement, fence
takeover, shutdown under load, and concurrent soak. Recovery cases mount the
rebuilt image and compare filesystem contents. The horizon test deletes every
fragment below the floor the protocol returned, then hands that floor back to
seek the recovery which follows.

Every broker-backed case creates its journal first, through
`tests/common/mod.rs`, because the daemon creates none. The cases which do not
are the ones asserting that an absent journal, or a specification a disk cannot
be recovered from, fails the `Open`.

`tests/ublk.rs` drives `src/bin/scenario.rs`. It exercises the block-device
library without a session, including shallow-queue backpressure, sparse extent
equality, and horizon accounting. This second binary exists because privilege
needs a process boundary and these observations are not part of the session API.

`tests/journal.rs` tests journal fencing, framing, append order, and replay
against a real broker. Unit and property tests cover chunk round trips, sparse
allocation, bitmaps, and state transitions.

### The examples

Two of them, sharing `examples/common/mod.rs` for the journals a client must create
and for where the daemon and its brokers are:

- `examples/basic.rs` is the smallest use of a disk. It writes one file, commits it,
  ends the session, and then opens the same journal again to find the file rebuilt
  from it. About 60 lines, and it asserts rather than prints.
- `examples/two_phase_commit.rs` drives the daemon as a two-phase-commit participant
  over four disks at once. About 150 lines, four lines of output.

Neither starts anything. A broker, an etcd, and a daemon must already run, and the
program only sends RPCs: it creates its own journals, reads them back, serves its
disks over sessions, and deletes the journals when it is done. Nothing about it is
specific to this repository.

`examples/demo-services.sh` brings those services up and takes them down again. It
runs an etcd, one broker over a `file:///` store, and the daemon, on ports away from
the defaults so it disturbs neither a local Flow stack nor a system etcd. Everything
it makes lives under one state directory, and `stop` finds each service by that
directory in its command line, so a lost pid file strands nothing.

```console
examples/demo-services.sh start
cargo run -p disk-daemon --example basic
cargo run -p disk-daemon --example two_phase_commit
examples/demo-services.sh stop
```

Both default to the socket and broker that script starts, so neither needs
configuring. `UDS_PATH` and `BROKER_ENDPOINT` override them, and the script prints
whichever of the two it changed.

Both run unprivileged, and read and write their disks with plain `std::fs`: only the
daemon needs privilege. `BROKER_CREDENTIAL` is optional, because a broker started
without `--broker.auth-keys` uses gazette's noop authorizer and accepts an empty
credential.

`Prepare` drives every data record of a delta to broker-confirmed durability and
withholds the acknowledgement, so the delta is durable and uncommitted — a prepared
state. The example is the coordinator: it prepares three participant disks, then
stores its decision by committing a fourth, so a disk is also the coordinator's log.
It ends every session without committing a participant, leaving three deltas in
doubt. It then rebuilds the log disk from its journal, reads the decision back out of
the recovered filesystem, and replays each acknowledgement through
`Open.recovered_acks`. The decision names two of the three, so two disks recover
their files and the third discards a delta no decision covers.

Note the ordering rule that recovery depends on. A replay honors an acknowledgement
only while its producer wrote the newest data records, per `journal/replay.rs`, and
`Open` appends recovered acknowledgements before it reads the journal or mounts
anything. A recovered acknowledgement therefore belongs in the next `Open` of that
journal: a session which mounts and writes first makes it impossible to honor, and
that `Open` fails rather than applying it out of order.

Journals are named under a per-run prefix, so a run never reuses the disks of an
earlier one, and the demo deletes them at the end. It asserts every result, so a
failed run is a real failure, and it takes about a second. `tests/daemon.rs` covers
what it leaves out, including recovery horizons, broker outage, credential
replacement, and leak checks across a soak.

## Key types and entry points

| Item | Role |
| --- | --- |
| [`proto`](../../go/protocols/disk/disk.proto) | Session messages and durable `DiskRecord` / `Chunk` messages, re-exported from `proto_flow::disk`. |
| [`BLOCK_SIZE`](src/lib.rs) | The 4 KiB every disk uses, for chunks, bitmaps, hole punching, and ext4. |
| [`args::Args`](src/args.rs) | The `serve` command line. |
| [`daemon::run`](src/daemon.rs) | Host validation, process wiring, socket service, startup reclaim, and drain. |
| [`session::Service`](src/session.rs) | One session state machine per gRPC stream and the client-visible error taxonomy. |
| [`client::Client`](src/client.rs) | One connection to a daemon, which every session of it shares. |
| [`client::Disk`](src/client.rs) | A client's side of one disk: prepare, commit, replace a broker, and close. |
| [`Invalid`, `OutOfOrder`](src/lib.rs) | Marks a request which was wrong in itself, or one which came out of turn. |
| [`client::Error`](src/client.rs) | What a client may do next: invalid, fenced, unauthorized, unavailable, or failed. |
| [`disk::Disk`](src/disk.rs) | One image, `ublk` device, capture channel, and owner lifecycle. |
| [`owner::spawn`](src/owner.rs) | The dedicated serving thread and its `io_uring`. |
| [`owner::Snapshotter`](src/owner.rs) | Bounded first-publication snapshots. |
| [`owner::Compactor`](src/owner.rs) | Writer-to-owner commands for recovery horizons. |
| [`capture::channel`](src/capture.rs) | Ordered, bounded handoff from device mutations to the writer. |
| [`journal::Opening` / `journal::Writer`](src/journal/mod.rs) | Validate the listed spec, fence, repair, replay, append, prepare, and commit for one session. |
| [`journal::replay`](src/journal/replay.rs) | Rebuild a sparse image from committed journal records. |
| [`journal::fence`](src/journal/fence.rs) | Probe and claim the `author` register. |
| [`image::Image`](src/image.rs) | Sparse image plus allocated and horizon bitmaps. |
| [`chunk`](src/chunk.rs) | Mutation encoding and replay application. |
| [`horizon::Horizon`](src/horizon.rs) | Pending-block state and compaction budget. |
| [`inflight::InFlight`](src/inflight.rs) | Serialization of overlapping image mutations. |
| [`ublk::Control`](src/ublk/control.rs) | Add, configure, start, stop, and delete devices. |
| [`metrics`](src/metrics.rs) | Per-disk journal progress and host capacity. |

## Module map

- `session.rs`: RPC lifecycle and teardown.
- `daemon.rs`: process lifecycle, host checks, and crash reclaim.
- `disk.rs`, `owner.rs`, and `ublk/`: kernel block-device serving.
- `image.rs`, `bitmap.rs`, `inflight.rs`, and `chunk.rs`: local state and
  durable block encoding.
- `capture.rs` and `wake.rs`: bounded cross-thread handoff and wakeups.
- `journal/`: spec validation, fencing, the append state machine, and recovery.
- `horizon.rs`: bounded-recovery policy and state.
- `filesystem.rs`: ext4 format, mount, flush, and unmount.
- `metrics.rs`: admin and Prometheus observations.
- `client.rs`: client of the session API.
- `src/bin/daemon.rs`: shipped service entry point.
- `src/bin/scenario.rs`: privileged library-test entry point.
- `examples/basic.rs`: the smallest use of a disk.
- `examples/two_phase_commit.rs`: the daemon as a two-phase-commit participant.
- `examples/common/mod.rs`: what both examples share.
