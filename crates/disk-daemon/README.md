# disk-daemon

`disk-daemon` serves Linux block devices whose contents commit atomically with a
client's own transaction. The daemon records each block change in a Gazette
journal, and the client commits those changes together with its own durable
state, such as a checkpoint. After a failure, a new attachment rebuilds the
device from the journal as of the last commit.

This lets connectors keep databases, indexes, and other working state in
ordinary files. The daemon records below the filesystem, so the journal doesn't
depend on the filesystem or storage engine above it.

## Concepts

**Disk**: A block device whose durable state is one journal, with content type
`application/x-journal-backed-disk`. The journal's name identifies the disk.

**Block**: The 4096-byte unit of a disk (`BLOCK_SIZE`).

**Attachment**: One `Attach` stream, and the disk it serves.

**Image**: An attachment's sparse local copy of a disk.

**Standby**: An attachment that replays the journal. It has no device and
appends nothing.

**Primary**: The promoted attachment that serves a block device and appends its
changes to the journal.

**Device**: The Linux block device (`/dev/ublkbN`) that serves a primary's
image.

**Chunk**: A write or punch (deallocation) of a range of blocks.

**Delta**: The chunks a primary records between two commits. It's the unit of
commit.

**ACK intent**: The encoded, unappended acknowledgement of a prepared delta,
which the client stores.

**Fence**: A primary's first record, whose append excludes every earlier primary
from appending to the journal.

**Recovery floor**: The journal offset replay starts from.

**Recovery horizon**: An offset that becomes the floor once later chunks cover
every block allocated before it.

## Architecture

A disk's durable state is a Gazette journal of block changes. An attachment
keeps a local copy of the disk in a sparse image file. The image is disposable,
since the journal can always rebuild it.

An attachment serves one disk to one client for the life of an `Attach` gRPC
stream over a Unix socket. It starts as a standby, which replays the journal
into its image but has no device. When the client promotes it, it becomes the
disk's primary. A primary serves its image as a block device and appends each
change to the journal.

The block device uses **ublk**, a Linux driver that hands requests to a server
process in user space. ublk delivers requests through **io_uring**, Linux's
interface for submitting I/O commands and receiving completions.

Each attachment has a tokio task, which owns the gRPC stream and all broker
calls. It also has one OS thread, which does all image and ublk I/O: a
**rebuilder thread** while it stands by, then an **owner thread** once promoted.

A standby's rebuilder thread applies the journal to the image:

```
  disk journal (Gazette)
     |  read from the recovery floor
     v
  attachment task
     |  journal content
     v
  rebuilder thread ---apply---> image
```

A primary's owner thread sends the chunks it applies as **live chunks**, which
the attachment task appends to the journal:

```
  workload, such as a filesystem
     |  block I/O
     v
  /dev/ublkbN (kernel ublk device)
     |  requests, over io_uring
     v
  owner thread ---apply---> image
     |  live chunks
     v
  attachment task <---Prepare, Acknowledge--- client
     |  append
     v
  disk journal (Gazette)
```

Promotion ends the rebuild and hands the rebuilt image to the owner thread. Both
threads apply chunks with `device::chunk::apply`, so a replayed chunk changes
the image exactly as the served chunk did.

The wire protocol and journal record schema are in
[`disk.proto`](../../go/protocols/disk/disk.proto).

## Attachment lifecycle

A client drives an attachment with requests on its `Attach` stream. The daemon
answers each one, in order:

1. **`Open`** names the journal and the device size. The daemon replays from the
   recovery floor through the head it confirms at open, then replies `Opened`
   (`attachment::standby::Standby::open`).
2. Until the next request, the standby tails the journal (`Standby::tail`). A
   standby tailing a journal that another primary writes is a **hot standby**,
   whose promotion has little left to replay.
3. **`Promote`** carries the client's stored ACK intent, if it has one. The
   daemon fences the journal with that ACK intent in the same append, and
   replays through the head the fence confirms. Then it ends the rebuild, starts
   the device, and replies `Promoted` with its path and diskseq
   (`Standby::promote`, `Promoted::serve`).
4. **`Prepare`** and **`Acknowledge`** then repeat. See
   [Transactions](#transactions).

The daemon's broker token selects only disk journals, so `Open` refuses a
journal of any other content type as absent.

## Transactions

### Journal records

Every record is a `DiskRecord` in Gazette's fixed protobuf framing.
[`disk.proto`](../../go/protocols/disk/disk.proto) documents the fence, delta,
and acknowledgement records, and the `Chunk` each delta record carries.

The writer's clock starts at wall time when it fences, ticks once per record,
and catches up to wall time at each `Prepare`.

### Commit sequence

A primary commits a delta in these steps:

1. The workload writes to the device. The owner thread applies each write to the
   image, and sends its live chunks to the attachment before completing it
   (`device/owner.rs`).
2. The attachment appends each live chunk as a delta record, without waiting for
   the broker to confirm it (`Writer::append`).
3. The client flushes the writes the delta must hold, as with a `syncfs`, pauses
   writes, and sends `Prepare`.
4. The attachment appends any chunks still in its channel, and waits for the
   broker to confirm all of the delta's records (`Writer::prepare`). It replies
   `Prepared` with the delta's ACK intent: an encoded `ACK_TXN` record it hasn't
   appended.
5. The client stores the ACK intent atomically with its own state.
6. The client sends `Acknowledge` and resumes writes.
7. The attachment appends the ACK intent, waits for confirmation, and replies
   `Acknowledged` (`Writer::acknowledge`). The delta is committed.

The flush in step 3 suffices because the owner thread sends a write's chunks
before completing it. Every write the flush waited on has its chunks in the
channel or the journal by the time `Prepare` arrives.

`Disk::prepare` states the write pause, from step 3 until `Acknowledge` is sent.
[How the device pauses](#how-the-device-pauses) explains why a write in that
window stalls the device.

A delta larger than twice the device size fails the attachment
(`Writer::append`), so a runaway delta never commits.

## Recovery and failover

Promotion must fence the old primary and recover the client's last committed
state before it starts the device.

### Gazette registers and fencing

The daemon keeps one register, `author`, which names the current primary's
producer. Promotion fences the journal as follows (`journal::Writer::fence`):

1. A zero-byte append returns the journal's head and registers
   (`journal::confirm_head`).
2. The fence's append checks that `author` is unchanged since step 1. If the
   journal has no author, it checks that there's still none, and pins its offset
   to the head from step 1 (`fence_check`).
3. The same append sets `author` to a new random producer.
4. Every later append of the primary checks that `author` still names it
   (`publisher::Appender::with_check_registers`).

So no append of an earlier primary lands after a fence. If another fence lands
between steps 1 and 2, the broker refuses this one and the client gets
`client::Error::Fenced`. Promotions that don't overlap both succeed: the later
fence wins, and the earlier primary fails at its next append.

### Re-acknowledgement

Promotion appends the client's stored ACK intent in the fence's own append. If
the delta it acknowledges never committed, this commits it. If the old primary
had begun a later delta, this rolls it back. Either way, the promoted disk
matches the client's stored state.

That's why `Promote` takes the last ACK intent the client stored
([`disk.proto`](../../go/protocols/disk/disk.proto)). An older one may
acknowledge a delta that a later primary rolled back. Replay commits nothing for
it, so the disk and the client's state would disagree.

The client also chooses which attachment to promote. The daemon doesn't
coordinate standbys.

### Replay

Replay rebuilds an image from the recovery floor, applying only committed
deltas. It sequences records with `proto_gazette::uuid::sequence`.
`replay::Replay` holds one uncommitted delta in `replay::buffer::Buffer`,
spilling to an unnamed file past 1 MiB. `Replay::next_committed` returns
committed chunks in journal order.

Replay rolls back a held delta when a record of a new producer arrives, because
a fence stopped the old producer and its delta can never commit. The fence
record itself rolls nothing back, since its append may carry a
re-acknowledgement. An acknowledgement with a clock before the held delta, which
re-acknowledges an earlier commit, also rolls it back.

Only a read still at its floor may skip forward, past fragments pruned below the
floor. Once the read leaves its floor, a gap fails the attachment.

## Compaction and recovery floors

The journal is a disk's only durable state. Replay must start at an offset from
which the committed chunks rebuild every block. That offset is the recovery
floor, and fragments can be deleted only below it.

With a floor of zero, replay reads every write the disk ever took, and no
fragment is ever deleted. A snapshot would move the floor: write every allocated
block to the journal, then start replay there. But a snapshot writes the whole
disk at once, including blocks the workload is about to overwrite. A recovery
horizon gets the same result incrementally. It copies blocks a few at a time, in
proportion to the workload's writes, and skips any block the workload rewrites
first.

### What the compactor tracks

The **compactor** (`device::compactor::Compactor`) decides when to open a
horizon and which blocks to copy. It follows every chunk the image applies,
served or replayed, and keeps:

- The **allocated blocks**: a bitmap of blocks whose last chunk was a write. A
  punch clears them.
- The **bytes since the floor**: the encoded size of the chunks applied since
  the floor. This approximates what replay reads.
- The open horizon, if any: the blocks it still needs to cover, how many bytes
  it may copy, and its position in the copy scan.

### Opening a horizon

A served chunk opens a horizon when no horizon is open, and the bytes since the
floor exceed both:

- `--horizon-minimum-bytes`, so a small disk doesn't compact constantly.
- `--horizon-open-ratio` times the allocated bytes, so replay stays within a
  multiple of the disk's live size.

That chunk is the horizon's **opener**. The horizon's **pending blocks** are the
blocks allocated just before the opener (`Compactor::apply`). Replay never opens
a horizon by policy. It opens one only where a committed record shows that a
primary did (see [Floors after recovery](#floors-after-recovery)).

### Completing a horizon

Every chunk after the opener clears the pending blocks it covers, whether it's a
workload write, a punch, or a copy. Blocks allocated after the opener are never
pending.

The workload may never touch some pending blocks, so the horizon also copies
them. Each data byte the workload writes while the horizon is open earns
`--horizon-copy-ratio` bytes of copy budget. After each served write,
`Image::serve` spends the budget:

1. `Compactor::next_copy_range` finds the next pending block at or after the
   scan position, and extends the range over the pending blocks that follow it.
   The range stops at the budget, at a block that isn't pending, at 32 blocks
   (128 KiB), or at the end of the disk.
2. `Image::serve` reads that range unchanged from the image, and encodes it as a
   write chunk: a **copy**.
3. `Compactor::on_copied` applies the copy, which clears its blocks.
4. Repeat until less than a block of budget remains.

Copies land in the journal right after the write that earned them, in the same
delta. The scan only moves forward, so a horizon costs one pass over its bitmap.
Only writes earn budget, so an idle disk copies nothing.

When the last pending block clears, the horizon is complete. The chunk that
cleared it names the opener as the new floor, and the bytes since the floor drop
to the bytes since the opener.

### Example

With a copy ratio of 1, each one-block write of non-zero data earns one block of
copy budget. Blocks 0, 1, 2, 3, and 5 are allocated when a write to block 6
opens a horizon:

```
  chunk              pending after    budget after
  write 6 (opener)   0 1 2 3 5        1 block
  copy 0             1 2 3 5          0
  write 2            1 3 5            1 block
  copy 1             3 5              0
  punch 3            5                0
  write 7            5                1 block
  copy 5             (none)           0             <- completes the horizon
```

Replay from the opener applies these seven chunks in order. They rebuild blocks
0, 1, 2, 5, 6, and 7, and leave block 3 punched. Block 4 was never allocated. No
chunk before the opener is needed.

### Why the opener is a valid floor

1. Every block allocated before the opener was pending when it opened.
2. A block stops being pending only when a chunk after the opener writes or
   punches it.
3. Every block allocated after the opener was allocated by a chunk after the
   opener.
4. Every other block is unallocated, and a fresh image reads it as zeroes.
5. So the committed chunks from the opener's offset rebuild every block.

### Cost

Let A be the allocated bytes when a horizon opens, and c the copy ratio. The
horizon completes by the time the workload has written about A/c bytes:

1. Copies cover only pending blocks, which total at most A.
2. Each byte the workload writes earns c bytes of budget, which `Image::serve`
   spends right after the write.
3. So after A/c bytes of workload writes, copies have covered every pending
   block that writes didn't.

The journal grows by at most about A/c + A while the horizon is open: 3A at the
default copy ratio of 0.5. Until the horizon completes, replay reads at most the
bytes since the floor when it opened, plus that growth. A workload that rewrites
its own pending blocks finishes a horizon sooner. These figures ignore record
framing and trimmed zeroes.

Compaction writes at most 1 + c times the workload's bytes, since copies never
exceed the budget the workload earns.

### Policy

| Flag | Default | Effect |
| --- | --- | --- |
| `--horizon-open-ratio` | 2.0 | Opens a horizon once bytes since the floor exceed this multiple of allocated bytes. |
| `--horizon-minimum-bytes` | 1 GiB | No horizon opens below this many bytes since the floor. |
| `--horizon-copy-ratio` | 0.5 | Bytes copied per byte the workload writes. |

Bytes since the floor are a policy target. They don't bound a transaction's size
or the daemon's memory.

### Floors in the journal

The device never learns where a record lands, so naming a floor takes two
chunks:

1. The device marks the opener as a **floor candidate**
   (`LiveChunk::floor_candidate`).
2. The writer confirms every earlier record before appending a candidate, so it
   knows the candidate's exact offset (`Writer::append`). The candidate's record
   sets `opens_horizon`.
3. The chunk that completes the horizon names the candidate as the floor
   (`LiveChunk::floor`). One chunk may be both.
4. `Writer::prepare` returns that floor alongside the ACK intent, and once the
   delta commits, `Writer::acknowledge` stores it (`journal::floor::store`).

The floor lives in the journal's `estuary.dev/disk-replay-from` label, as a
16-digit hex offset. The daemon only ever advances it. `floor::store` updates
the spec against its listed revision, and retries on the next listing if another
edit wins. A floor that can't be stored fails the attachment.

### Floors after recovery

Replay rebuilds the compactor's state too. It reports the offset of each
committed record that sets `opens_horizon`, and the rebuilder opens a horizon
there, just as the primary did. If replay leaves a horizon open, the new primary
carries it on, and the chunk that completes it names the replayed opener's
offset (`Floor::Replayed`).

A primary holds a floor only in memory, from `Prepare` until it stores the
label. If it fails in between, the delta can still commit, by its own
acknowledgement or the client's re-acknowledgement. Then the next rebuild
completes the same horizon during replay, and the first chunk the new primary
serves names that floor again (`Compactor::on_served`). The next commit stores
it. Storing a floor at or behind the label changes nothing.

## Device implementation

### Sparse images and hole punching

Each image is an unnamed file in `--image-dir`, which `tempfile::tempfile_in`
creates with `O_TMPFILE`. The daemon sets its length to the device size, and it
stays sparse until chunks write to it.

`device::chunk::apply` applies a write chunk with `pwrite`, zeroing the rest of
its range past the data. `chunk::encode_write` trims trailing zeroes from a
write's data, so mostly-empty writes make small records. A punch is
`fallocate(PUNCH_HOLE | KEEP_SIZE)`, and a punched range reads as zeroes. So a
filesystem's discard becomes a hole in the image and a tiny record in the
journal, and the compactor counts the punched blocks as free.

Replay also spills large uncommitted deltas to unnamed files in the same
directory (`replay/buffer.rs`).

### ublk

The kernel hands each device's block requests to the daemon through
`/dev/ublkcN`, as io_uring passthrough commands. `device/ublk.rs` documents the
device nodes and the kernel ABI, and `device/owner.rs` documents the request
loop.

Each device is configured as follows (`device/ublk.rs`):

- One queue with a depth of one, so one request is in flight at a time.
- `UBLK_F_USER_COPY`: the owner thread moves request data itself, with `pread`
  and `pwrite` on `/dev/ublkcN`.
- `UBLK_F_NO_AUTO_PART_SCAN`: the kernel doesn't scan for partitions.
- 4096-byte logical and physical blocks. Requests of up to 128 KiB, and discards
  and write-zeroes of up to 16 MiB.
- No volatile cache, so the kernel sends no flush or FUA requests.

The owner thread turns discards and write-zeroes into punches. Teardown sends
`UBLK_U_CMD_STOP_DEV`, then `UBLK_U_CMD_DEL_DEV_ASYNC`, which doesn't wait for
holders of the device to release it.

### io_uring

ublk has no other interface. Each of its commands is an `IORING_OP_URING_CMD`,
which io_uring passes through to the driver. The daemon uses io_uring only to
carry these commands.

A ring isn't tied to a device. Each entry names a file descriptor, and io_uring
hands the command to that file's driver. So a ring talks to `/dev/ublk-control`
or to `/dev/ublkcN` according to the descriptor in its entries.

The daemon has two kinds of ring. Each holds a single entry, and the thread that
uses it blocks in `submit_and_wait` until its command completes.

- **Control rings.** There's one control device, but no shared control ring.
  Each `Control` opens its own handle on `/dev/ublk-control`, and creates a
  private ring.
  `Control::issue` encodes a command into an entry that names that handle,
  submits it, and waits on whichever blocking thread called it. The command is
  still in flight after submission, since the kernel runs control commands on a
  worker thread. A device's `Control` lives as long as the device. Control
  rings use 128-byte entries (`Entry128`), because a standard entry has room
  for a 16-byte command and a `ublksrv_ctrl_cmd` is 32 bytes.
- **I/O rings.** Each owner thread creates one, whose entries name its handle on
  `/dev/ublkcN`. It pushes `UBLK_U_IO_FETCH_REQ` to arm the queue's one tag, then
  loops:
  1. Wait for a completion. A result of 0 means a request is ready, and
     `-ENODEV` means the device stopped.
  2. Read the request's descriptor and serve it.
  3. Push `UBLK_U_IO_COMMIT_AND_FETCH_REQ`, which answers the request and re-arms
     the tag in one command.

The daemon doesn't currently use these parts of io_uring:

- Async completion. Every wait blocks its thread, and no ring is polled from an
  async runtime.
- File or network operations. Request data moves by `pread` and `pwrite` on
  `/dev/ublkcN`, and image I/O is plain `pwrite` and `fallocate`.
- Batching. A ring holds one entry, so each submission carries one command.
- Kernel-side polling: `IORING_SETUP_SQPOLL` or `IORING_SETUP_IOPOLL`.
- Registered files or buffers.
- Linked, multishot, or provided-buffer operations.
- Setup flags beyond `IORING_SETUP_SQE128`, which control rings need for their
  128-byte entries. For example, `IORING_SETUP_SINGLE_ISSUER` and
  `IORING_SETUP_DEFER_TASKRUN` are unset.

Nor does it use ublk zero-copy (yet), or more than one tag or queue.

### I/O flusher threads

The owner thread marks itself with `PR_SET_IO_FLUSHER` (`device/owner.rs`). Its
memory allocations then never wait on I/O, and only the image's backing device
throttles its writes. Without the mark, the owner thread could block on
writeback of dirty pages that only it can clean: those of a filesystem on its own
device. `PR_SET_IO_FLUSHER` requires `CAP_SYS_RESOURCE`.

### Threads

An attachment task awaits the client and the brokers, but never the device.

**Owner thread** (`device/owner.rs`): One per primary. It's a thread because:

- The kernel binds the device's tag to one thread (see `device/owner.rs`).
- It blocks in `submit_and_wait` for each request, and serves it with blocking
  `pread`, `pwrite`, and `fallocate`.
- It blocks when the one-slot live-chunk channel fills. This is how a prepared
  delta pauses the device (see [How the device pauses](#how-the-device-pauses)).
- `PR_SET_IO_FLUSHER` is a per-thread attribute, and a dedicated thread confines
  it to the device's I/O path.
- One thread serves the device's one tag, applies each chunk, and sends it. So
  the journal records chunks in the order the image applied them, and the image
  and compactor need no lock.

**Rebuilder thread** (`device/mod.rs`): One per standby. Replay frames and
sequences records, spills and reads back large deltas, and applies chunks, all
with blocking file I/O. The thread owns the `Replay` and the image outright. The
attachment task feeds it through a one-slot channel, so a slow rebuild slows the
journal read rather than buffering it in memory.

**Blocking tasks** (`tokio::task::spawn_blocking`): The attachment runs the
device's other blocking steps here:

- Device start. `Control::start` blocks until the owner thread has a fetch in
  flight, so it must run on another thread.
- Device stop. `Control::stop` blocks until in-flight requests complete, which
  needs the owner thread to keep serving.
- The join of a discarded standby's rebuilder thread.

### How the device pauses

The write pause (`Disk::prepare`) follows from journal ordering and the device's
single tag:

1. The acknowledgement must land before the next delta's records. Replay commits
   nothing for an acknowledgement that lands behind records with a later clock
   (`replay/sequencer.rs`).
2. So the attachment takes no live chunks while a delta is prepared.
3. The live-chunk channel holds one chunk. Once it fills, the next send blocks
   the owner thread.
4. The device has a single tag, so it then serves no request, reads included,
   until the acknowledgement lands.

## Teardown

### How an attachment ends

Every attachment tears its disk down before its stream closes
(`attachment::run`). Dropping a `Standby` or `Disk` cancels its attachment and
abandons its broker calls.

How teardown goes depends on how far the attachment got:

- A standby that fails is discarded, joining its rebuilder thread
  (`Standby::discard`). A failed rebuild becomes the attachment's failure.
- A standby that's cancelled, or whose client leaves before `Promote`, is simply
  dropped. Its rebuilder thread applies what it holds and exits.
- A primary stops and deletes its device (`Promoted::stop`), first waiting out a
  device start still in flight. A failed stop or delete fails the attachment, so
  `Disk::close` reports it.

After the stop, a filesystem still mounted on the device fails every operation
with an I/O error. The delete doesn't wait for an unmount, or for the client to
close the device, but the kernel frees the device number only after both.

### Shutdown and drain

`SIGTERM` or `SIGINT` drains the daemon (`daemon::run`). It removes the socket
so clients can't reconnect, stops accepting attachments, and cancels each one.
It exits once every disk is torn down, so the supervisor's stop timeout bounds
the drain.

A cancelled primary abandons its appends. Its device keeps applying writes to
the image until it stops, but records none of them.

### Orphaned devices

A daemon killed without draining leaves its devices behind. The kernel stops
them, so their I/O fails, but the devices remain until the host reboots.

## Caller responsibilities

The protocol's own obligations, such as the device size and the ACK intent, are
in [`disk.proto`](../../go/protocols/disk/disk.proto) and the `client.rs` docs.
These are the rest.

### Journal

The caller creates each disk journal with the disk content type, and owns its
spec.

The daemon changes only the replay-from label, and only advances it. The
caller's spec updates must preserve that label, or replay falls back to offset
zero. The spec must not set fragment retention, since Gazette deletes fragments
by age and can't see the floor.

An external pruner may delete fragments wholly below the floor, leaving an age
margin for standbys to finish their backfill. The daemon deletes no fragments.

A label that isn't a hex offset fails `Open`, but a wrong offset goes
undetected. One at the head yields an empty device over committed data. One past
the head hangs `Promote`: the fence appends below the floor, and replay waits
for content at the floor that nothing will write.

### Filesystem

The daemon can't see the filesystem, so it relies on it to:

- Use the whole device, which has no partition table.
- Use blocks of 4096 bytes or more.
- Discard freed blocks, with the `discard` mount option or an `fstrim` before
  each `Prepare`. Otherwise the compactor counts freed blocks as allocated, and
  every horizon copies them.
- Mount with `noatime`, so reads make no deltas.
- Do no background zeroing after it's formatted. For ext4, format with
  `-E assume_storage_prezeroed=1`.

## System requirements

The daemon checks none of these at startup. An attachment fails on whatever its
host lacks.

### Kernel

- Linux 6.11 or later, with `ublk_drv` loaded. Teardown uses
  `UBLK_U_CMD_DEL_DEV_ASYNC`, which 6.11 added. On older kernels every delete
  fails, and every device is left behind.
- On Ubuntu, `ublk_drv` ships in `linux-modules-extra-$(uname -r)`.
  `mise run bootstrap:ublk-drv` installs and loads it.
- On Linux 6.17 and 6.18, a daemon in its own PID namespace can't start a
  device. `UBLK_U_CMD_START_DEV` passes the daemon's PID, and these kernels
  compare it against the host namespace's. Linux 6.19 translates between the
  two.
- io_uring must be enabled: `kernel.io_uring_disabled` must be 0 or 1. At 1,
  io_uring requires `CAP_SYS_ADMIN`.

### Privileges

The daemon needs:

- **`CAP_SYS_ADMIN`**, for ublk control commands: adding, configuring, starting,
  stopping, and deleting devices. ublk requires it for any device not
  created with `UBLK_F_UNPRIVILEGED_DEV`, which the daemon doesn't use. It also
  lets the daemon create io_uring rings when `kernel.io_uring_disabled` is 1.
- **`CAP_SYS_RESOURCE`**, for `PR_SET_IO_FLUSHER` on each owner thread. Without
  it the owner thread exits as it starts, and promotion fails.
- **Read and write access to `/dev/ublk-control` and `/dev/ublkcN`**: the control
  device, and the server side of each device the daemon adds. These nodes belong
  to root by default.
- **The host's devtmpfs at `/dev`.** The daemon opens `/dev/ublkcN` as soon as it
  adds device N, and the client opens `/dev/ublkbN` after `Promoted`. Only the
  host's devtmpfs gains these nodes. A container with its own `/dev` sees only
  the nodes present when it started.
- **A seccomp profile that allows `io_uring_setup` and `io_uring_enter`.**
  Docker's default profile blocks them, and `--privileged` disables seccomp.
- **The host's PID namespace, on Linux 6.17 and 6.18.** See [Kernel](#kernel).

### Image directory

`--image-dir` must be on a filesystem with sparse files and hole punching, such
as ext4, XFS, or tmpfs. Images and spill files are unnamed, so the kernel frees
them when the daemon exits.

### Resources

Each attachment holds an image, which can grow to its device size, and one
thread: a rebuilder while standing by, then an owner once promoted. A standby
also holds a blocking journal read, and a spill file while an uncommitted delta
exceeds 1 MiB. A primary also holds a kernel device, two io_uring rings, and
several file descriptors.

The daemon reserves no space and limits no disk count: capacity and placement
are the deployment's job.

## Operating the daemon

`flow-disk-daemon --help` describes the configuration flags.

Only one daemon runs per `--uds-path`. At start, the daemon replaces any socket
left there by a daemon killed without draining (`daemon::listen`). A drain
removes whatever socket is at the path, so if two daemons overlapped, the
earlier one's drain would leave the later one unreachable.

### Socket trust boundary

The socket grants raw block writes to every disk journal in the data plane, and
whatever later mounts one of those disks parses what was written. So only
trusted, host-level clients may reach the socket, and neither it nor a device is
ever passed to a connector. The socket is mode `0666`, so restrict traversal of
its parent directory. Device nodes belong to root, and granting access to them
is the deployment's business.

The daemon signs its own broker token with the data plane's HMAC key. The token
is scoped to disk journals. Clients supply neither broker endpoints nor
credentials. Rotating the key requires a restart.

## Modules

Paths are relative to `src/`.

- `lib.rs`: Protocol export, `BLOCK_SIZE`, `CONTENT_TYPE_DISK`, `LiveChunk`, and
  `Floor`.
- `daemon.rs`: Flags, broker client and token, socket, `Disk` service, and
  drain.
- `client.rs`: The Rust client: `Client`, `Standby`, `Disk`, and `Error`.
- `attachment/`: An attachment's lifecycle. `run` and `serve` handle request
  order, validation, and how an attachment ends.
  - `standby.rs`: A standby's open, tail, promotion, and discard.
  - `promoted.rs`: A primary's device start, `Prepare` and `Acknowledge`, and
    teardown.
- `journal/`: Journal I/O, and `confirm_head`.
  - `reader.rs`: `open`, and the blocking read with its gap check.
  - `writer.rs`: `Writer`: fence, append, prepare, and acknowledge.
  - `floor.rs`: Parsing and storing the replay-from label.
- `replay/`: `Replay`, which frames records and returns committed chunks.
  - `sequencer.rs`: Per-record hold, commit, skip, and rollback decisions.
  - `buffer.rs`: The uncommitted delta, spilling past 1 MiB.
- `device/`: `Standby` and its rebuilder thread, and `Device` start and stop.
  - `owner.rs`: The owner thread: its I/O ring and request loop.
  - `ublk.rs`: `Control`, device parameters, request decoding, and the kernel
    ABI.
  - `image.rs`: `Image`: replay, serving with copies, and reads.
  - `chunk.rs`: Chunk encoding and `apply`.
  - `compactor.rs`: `Compactor` and `Policy`: allocation, horizons, copies, and
    floors.

## Testing

### Unit tests

Unit and property tests need no data plane, devices, or privilege. A property
test in `device/image.rs` checks that replaying the live chunks rebuilds the
served image.

```console
mise exec -- cargo nextest run -p disk-daemon
```

### Integration tests

`tests/` drives the real binary through the client, against a real data plane
from `e2e-support`. Cases use real ublk devices, raw device I/O, ext4 on a disk,
and an independent Gazette client. Most compare a recovered device byte for byte
with what the case wrote. Cases with random writes use a fixed seed.

| File | Covers |
| --- | --- |
| `protocol.rs` | Request order and refusals. |
| `lifecycle.rs` | Open and close, and suspended and absent journals. |
| `durability.rs` | Recovery of raw writes and ext4 trees, interrupted transactions, the write pause, runaway deltas. |
| `failover.rs` | Repeated failover, and promotion across fences a standby never saw. |
| `pruning.rs` | Floors that replay finds, and deleting fragments below the floor. |
| `resilience.rs` | Broker outages, including a prepare behind a stalled attachment, drains, killed daemons, a many-disk soak of random writes, and commits racing random writes. |

```console
mise run ci:disk-daemon-e2e  # unit tests, then integration tests
```

The default nextest profile excludes the integration tests, which run under the
`disk-daemon-e2e` profile. Their binaries run as root through a `sudo -n` Cargo
target runner, while Cargo itself stays unprivileged. They run one at a time,
since each checks the whole host for leftover devices and mounts.

Beyond the [kernel requirements](#kernel), they need passwordless `sudo`,
`e2fsprogs` for the ext4 cases, and `etcd` and `gazette`. The task loads
`ublk_drv` and builds `gazette`, and mise provides `etcd`. CI runs the task in
`.github/workflows/disk-daemon-test.yaml`.
