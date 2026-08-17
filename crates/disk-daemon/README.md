# disk-daemon

`disk-daemon` serves mounted filesystems whose durable state lives in Gazette
journals. Each live disk is a sparse local image exposed through Linux `ublk`.
The daemon records every accepted block mutation in a per-disk journal. It can
later rebuild the image from that journal.

A client controls durable boundaries with a session RPC. The daemon publishes a
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
- the `flow-disk-daemon client` command for manual sessions;
- a bidirectional gRPC session API over a Unix socket;
- sparse images, `ublk` devices, ext4 formatting, and mounts;
- journal creation, writer fencing, delta capture, and acknowledgement repair;
- recovery from committed deltas;
- recovery horizons, which keep the required journal range bounded; and
- an admin page, metrics, graceful shutdown, and crash cleanup.

The main guarantees are:

| Area | Guarantee |
| --- | --- |
| Durable state | Acknowledged journal deltas, plus acknowledgements recovered from the client's external commit, define the disk. |
| Transaction boundary | `Publish` returns the exact acknowledgement needed to commit one point-in-time device state. |
| Crash recovery | A new sparse image is rebuilt from committed journal state. Uncommitted mutations are discarded. |
| Filesystem result | Recovery preserves files and their contents at the committed boundary. It does not promise an identical ext4 block image. |
| Writer ownership | At most one cooperative session may append to a disk journal. Gazette's `author` register fences older sessions. |
| Local lifetime | A normal session removes its mount and device. Its image has no directory entry and disappears with the process. |
| Isolation | The client receives a mounted directory. It does not receive a block device, image descriptor, or daemon privilege. |

Mounting and unmounting can change ext4 bookkeeping such as mount counts,
timestamps, and journal state. Recovery therefore promises filesystem contents,
not byte-for-byte filesystem metadata.

The client must keep `device_size` and `block_size` stable for the life of a
disk. They shape the durable chunk format but are not stored in the journal.
`block_size` must be a supported power of two of at least 512 bytes. The device
must contain at least one block and at most `u32::MAX` blocks.

Flow runtime integration is outside this crate. The runtime still needs to:

- provision one disk journal per task shard;
- place the returned mount in a connector sandbox;
- store published acknowledgements with Flow checkpoints;
- choose connector protocol boundaries that keep the disk quiet;
- define shard splitting; and
- converge or delete disk journal specifications.

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
  drains captured mutations, appends records, publishes acknowledgements, and
  advances recovery horizons.

A terminal error tears down that session and its disk. It does not stop other
sessions. A daemon drain ends all sessions.

### Concepts

**Image** is the sparse local file behind a device. It is a working copy, not
durable state.

**Mutation** is one accepted write, discard, or write-zeroes device request.

**Chunk** is the durable encoding of part of a mutation. A data chunk allocates
blocks. A punch chunk deallocates blocks.

**Delta** is every chunk published since the previous commit. It may also
contain unchanged blocks copied for compaction.

**Cut** is the point-in-time boundary made by closing mutation admission after a
`syncfs`.

**Acknowledgement** is the exact serialized Gazette `ACK_TXN` record for a
delta. `Publish` returns it without appending it.

**Fence** is an `OUTSIDE_TXN` record that atomically installs a new writer
epoch in the journal's `author` register.

**Recovery floor** is the oldest journal position still needed to rebuild a
disk.

**Horizon** is a candidate recovery floor. It becomes the floor after every
allocated block has a committed copy at or after that position.

## Session protocol

The protocol is defined in
[`go/protocols/disk/disk.proto`](../../go/protocols/disk/disk.proto). A
`Session` is one bidirectional stream. Requests and replies stay in order. The
daemon sends no unsolicited messages.

| Request | Reply | Meaning |
| --- | --- | --- |
| `Open` | `Opened` | Create or recover one disk, mount it, and return the absolute mount path. |
| `Publish` | `Published` | Cut and finish one delta. Return its unappended acknowledgement, or empty bytes when nothing changed. |
| `Commit` | `Committed` | Append the exact published acknowledgement and wait for broker confirmation. |
| `Broker` | none | Replace the session's Gazette endpoint and credential. |

`Open` must be first and may be sent once. It supplies:

- the journal name and all inputs needed to create its `JournalSpec`;
- device size and block size;
- a required broker endpoint and an optional bearer credential; and
- acknowledgements that the client committed externally but may not have
  reached the journal.

The basic state machine is:

```text
new ── Open ──► opening ── Opened ──► serving
                                          │
                    ┌─────────────────────┴─────────────────────┐
                    │                                           │
                    ▼                                           ▼
               Publish(empty)                             Publish(ack)
                    │                                           │
                    └──────────────► serving ◄── Committed ◄── Commit

any state ── EOF, cancellation, or error ──► teardown ──► closed
```

Only one acknowledgement may be outstanding. The client must not send another
`Publish` until it has sent `Commit`. The daemon reads requests in order, so a
`Broker` sent behind another request waits for it. The client must send
`Broker` well before its credential expires.

The endpoint and credential always change together. An empty credential means
anonymous access. The daemon does not mint tokens or hold a signing key.

### Client boundary contract

The daemon makes a coherent device cut. It cannot know whether that cut matches
the client's external transaction. The client and workload must:

1. Finish and flush application state that belongs to the transaction.
2. Stop logical disk writes at the external boundary.
3. Run `Publish`, store the acknowledgement with external state, and run
   `Commit`.
4. Release the workload for the next transaction.

The daemon calls `syncfs`, so the cut includes writes already issued to the
filesystem. It cannot include an application write that has not been issued
yet. If the workload crosses its logical boundary, the recovered filesystem is
still coherent but may not match the client's checkpoint.

## Linear walkthrough

### 1. Open

The daemon validates the request and builds the possible journal spec before it
creates a device. Missing creation fields, an unsupported compression codec, or
invalid device geometry therefore fail early.

It probes the journal once for its write head and current `author` register.
The result chooses one of two paths:

**Fresh disk**

1. Create a sparse image with `O_TMPFILE` and `ftruncate`.
2. Create and start a `ublk` device over the image.
3. Format the device as ext4.
4. Mount it under the configured mount directory.
5. Drop all format and mount mutations from the capture channel.
6. Return the mount path.

An absent journal is not created or claimed during this path. A disk that is
opened and never changed leaves no journal state.

**Recovered disk**

1. Claim the existing journal before reading or repairing it.
2. Append each recovered acknowledgement exactly as supplied.
3. Fix a broker-confirmed journal head for this recovery.
4. Read the recovery-floor label and replay committed records into a new image.
5. Create the `ublk` device over the rebuilt image.
6. Mount the filesystem and return its path.

A journal that contains only orphan records from a failed first use is claimed
and replayed, but it still produces a fresh filesystem. The fence is the same
fence that the session would need for its next first append.

Recovered acknowledgements require an existing journal. A client can hold one
only for a journal which existed, so an absent journal means committed state
was deleted. The `Open` fails rather than serve a fresh disk.

### 2. Serve block I/O

The kernel sends reads, writes, discards, and write-zeroes requests to the owner
thread. Reads come from the image. Every mutation is encoded and offered to the
bounded capture channel before the image operation is submitted.

The owner completes a request when its image operation completes. It does not
wait for that mutation to become durable in Gazette. The capture bound applies
backpressure when the writer cannot keep up.

### 3. Publish a boundary

`Publish` runs these steps:

1. Call `syncfs` on the mounted filesystem.
2. Stop admitting new mutations. Reads continue.
3. Wait until every admitted mutation has reached the image.
4. Drain the capture channel and wait for every journal append to finish.
5. Sample whether this delta completed an open horizon.
6. Build the acknowledgement, but do not append it.
7. Resume device admission and return `Published`.

Closing admission is the exact cut. Each mutation is wholly before or after it
because capture happens before the image operation.

The writer stops taking new mutations while an acknowledgement is outstanding.
This prevents Gazette from joining two deltas into one pending transaction.
Filesystem writes after the cut may therefore wait for `Commit`. Clients
should keep this interval short.

If the delta contains no records, `Published.ack` is empty. The client has no
disk obligation and sends no `Commit`.

### 4. Commit with external state

The client treats the acknowledgement as opaque bytes:

```text
client                         daemon                         Gazette
   │                              │                              │
   ├──── Publish ────────────────►│                              │
   │                              ├─ finish data appends ───────►│
   │◄── Published(ack bytes) ─────┤                              │
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
acknowledgement cannot safely commit the published delta.

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
checked later at `Publish`. This keeps normal block I/O asynchronous while the
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

- the disk's block size;
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

- an unused fresh disk creates no journal;
- repeated format writes to one block collapse into one final block value; and
- memory does not scale with device size.

A mutation may already be present in the snapshot. This is safe. Its captured
chunk follows the snapshot and applies the same mutation again.

A recovered disk takes no snapshot. Its filesystem is already in the journal.
Writes made by mounting it, including ext4 journal replay, belong to the next
delta.

## Journal design

### Journal specification and creation

`Open.JournalConfig` supplies typed inputs instead of a raw `JournalSpec`.
The daemon fixes the fields needed for recovery:

- the journal is read-write;
- the compression codec must be one the daemon can decode;
- retention is unset; and
- `path_postfix_template` is empty.

Every configuration field is required. Two fields use proto3 `optional` so
zero remains a deliberate value: zero `max_append_rate` means no ceiling, and
zero `flush_interval_seconds` means close fragments by size only.

The daemon has no journal defaults. It creates a spec once and does not converge
it later. A hidden default would become a permanent property of disks created by
one daemon version. The client remains responsible for later convergence and
deletion.

Creation is an insert-only Gazette apply. It happens on the first append, not at
`Open`. If another writer wins the creation race, the daemon accepts the
existing spec and lets fencing decide ownership.

Age-based retention is unsafe for a disk journal. It cannot know the recovery
floor and could remove live state before a horizon completes. Physical fragment
deletion must follow the floor instead.

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

Record clocks follow wall time as well as record order. A floor label stores the
opening record's clock and turns it back into a fragment modification-time seek.
A clock that only ticked per record would drift behind long-running sessions.

The protocol currently has Rust bindings but no Go bindings. Its two proto3
`optional` configuration fields are not supported by this repository's
`protoc-gen-gogo`, and no Go code consumes the API. A future Go client must
resolve that generator constraint without changing the journal semantics.

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

- An absent, unused journal is created and claimed on its first append.
- An existing journal is claimed before acknowledgement repair or replay.

The register chooses one cooperative writer. It is not commit authority.
Committed records and recovered acknowledgements remain authoritative even if
register state is lost.

## Recovery

Recovery runs before a device can read the rebuilt image:

1. Claim the journal.
2. Append recovered acknowledgements.
3. Probe a broker-confirmed head `H`. This fixes the end of the recovery.
4. Read the configured floor label. Its clock gives a fragment
   modification-time seek.
5. Replay the fixed range up to `H` into a new sparse image.
6. Rebuild the allocated bitmap and any open horizon.
7. Mount the image.

The floor is a seek hint, not a record filter. A missing or stale label starts
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
mutation itself publishes the newer value, so the copy does not need to block
it.

At a publication cut, the writer asks the owner how many horizon blocks remain.
If none remain, committing that delta completes the horizon. The writer moves
the floor to the opening record and asks the owner to drop the bitmap.

The daemon writes the opening record's clock to a configured label on the
journal spec. The value is sixteen lowercase hexadecimal digits. The update is
advance-only and uses compare-and-swap against the live spec, so unrelated spec
fields survive.

The label update runs off the commit path. A failed or stale update only makes a
future replay start earlier. It cannot change recovered content. The daemon
derives and retries the floor during later recovery. Fragment deletion is a
separate external operation.

An absent or behind floor label is safe. A label ahead of the derived floor is
unsafe because it can skip required records. The daemon therefore writes only a
floor proved by a completed horizon and never moves the label backward.

If a disk stops changing, an open horizon pauses. Its journal also stops growing.
A replacement session reconstructs and resumes an open horizon from ordinary
records.

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
| `--floor-label` | Journal-spec label that stores the recovery floor. It has no default because the daemon has no deployment-specific label vocabulary. |

Flow deployments use `estuary.dev/truncated-at` as the floor label.

Optional service flags:

| Flag | Default | Meaning |
| --- | --- | --- |
| `--admin-port` | disabled | Serve the loopback-only admin page and Prometheus metrics. |
| `--log-format` | `text` | `text` or `json`. |
| `--horizon-open-ratio` | `2.0` | Journal-range multiple that opens a horizon. |
| `--horizon-copy-ratio` | `0.5` | Unchanged bytes copied per changed byte. |
| `--horizon-minimum-bytes` | 1 GiB | Minimum journal range before a horizon opens. |

Device size, block size, journal configuration, broker endpoint, and credential
are session inputs. They are not daemon flags. Device and block size are durable
per-disk facts. Journal fields are creation-time facts. A daemon restart must not
reinterpret either.

Compaction flags are policy. They may change between restarts because replay
derives horizon state from the journal.

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
and its phase is one of `opening`, `serving`, `publishing`, `committing`,
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
| `disk_daemon_publishes` | Non-empty deltas published. |
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
| `FAILED_PRECONDITION` | The request is out of order, such as a second `Open` or a request before `Open`. |
| `ABORTED` | Another session took the journal fence. This session must not take it back. |
| `UNAUTHENTICATED` | A broker directly rejected the credential. Refresh it and open a new session. |
| `UNAVAILABLE` | The daemon is draining or a broker is unreachable. Another daemon or host may work. |
| `INTERNAL` | The daemon, device, host, or an otherwise unclassified operation failed. |

Three writer protocol violations currently report `INTERNAL`: a second
`Publish` while one is outstanding, a `Commit` with nothing published, and a
`Commit` with different bytes. They are client state errors and should
eventually report `FAILED_PRECONDITION`.

A client with a short-lived Gazette token must send `Broker` well before the
token expires. The daemon does not hold a delta while waiting for a
replacement, and ext4 can write on its own schedule. An expired token may fail
the broker operation rather than authentication itself, so it does not always
appear as `UNAUTHENTICATED`.

### Manual session

`flow-disk-daemon client` drives one session from standard input:

```console
$ flow-disk-daemon client --uds-path /run/disks/daemon.sock \
    --journal acmeCo/disk/scratch \
    --fragment-store s3://example-bucket/disks/ \
    --broker-endpoint https://broker.example \
    --broker-credential "$TOKEN"
mounted /var/lib/disks/disk-3
publish
published 220
commit
committed
quit
closed
```

The command holds the acknowledgement itself, so `commit` needs no pasted
bytes. `quit`, end-of-input, and `SIGINT` close the session and remove its
disk.

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
fragment below the derived floor before recovery.

`tests/ublk.rs` drives `src/bin/scenario.rs`. It exercises the block-device
library without a session, including shallow-queue backpressure, sparse extent
equality, and horizon accounting. This second binary exists because privilege
needs a process boundary and these observations are not part of the session API.

`tests/journal.rs` tests journal fencing, framing, append order, and replay
against a real broker. Unit and property tests cover chunk round trips, sparse
allocation, bitmaps, and state transitions.

## Key types and entry points

| Item | Role |
| --- | --- |
| [`proto`](../../go/protocols/disk/disk.proto) | Session messages and durable `DiskRecord` / `Chunk` messages, re-exported from `proto_flow::disk`. |
| [`args::Args`](src/args.rs) | `serve` and `client` command lines. |
| [`daemon::run`](src/daemon.rs) | Host validation, process wiring, socket service, startup reclaim, and drain. |
| [`session::Service`](src/session.rs) | One session state machine per gRPC stream and the client-visible error taxonomy. |
| [`client::run`](src/client.rs) | Interactive one-session client. |
| [`disk::Disk`](src/disk.rs) | One image, `ublk` device, capture channel, and owner lifecycle. |
| [`owner::spawn`](src/owner.rs) | The dedicated serving thread and its `io_uring`. |
| [`owner::Snapshotter`](src/owner.rs) | Bounded first-publication snapshots. |
| [`owner::Compactor`](src/owner.rs) | Writer-to-owner commands for recovery horizons. |
| [`capture::channel`](src/capture.rs) | Ordered, bounded handoff from device mutations to the writer. |
| [`journal::Opening` / `journal::Writer`](src/journal/mod.rs) | Fence, repair, replay, append, publish, and commit for one session. |
| [`journal::replay`](src/journal/replay.rs) | Rebuild a sparse image from committed journal records. |
| [`journal::fence`](src/journal/fence.rs) | Probe and claim the `author` register. |
| [`journal::floor`](src/journal/floor.rs) | Read and advance the recovery-floor label. |
| [`journal::spec`](src/journal/spec.rs) | Validate `JournalConfig`, build a spec, and create it insert-only. |
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
- `journal/`: specification, fencing, append state machine, floor labels, and
  recovery.
- `horizon.rs`: bounded-recovery policy and state.
- `filesystem.rs`: ext4 format, mount, flush, and unmount.
- `metrics.rs`: admin and Prometheus observations.
- `client.rs`: manual session driver.
- `src/bin/daemon.rs`: shipped service entry point.
- `src/bin/scenario.rs`: privileged library-test entry point.
