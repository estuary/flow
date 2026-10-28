# disk-daemon

`disk-daemon` serves mounted filesystems whose contents commit atomically with an
external transaction. It lets Flow connectors keep databases, indexes, and other
working state in ordinary files and recover them alongside their checkpoints.

Each disk is a disposable sparse image exposed through Linux `ublk` and mounted
as ext4. Block mutations go to a Gazette journal; the client coordinates their
commit with its own durable state. Recording below the filesystem keeps the
journal independent of the application's storage engine.

The crate provides `flow-disk-daemon` and its Rust client, independently of Flow's
runtime and connector protocols. Flow integration still needs to place mounts in
connector sandboxes, persist ACK intents with checkpoints, manage journal
and fragment lifetimes, and define shard splitting.

[GLOSSARY.md](GLOSSARY.md) defines the crate's terms.

## Entry points and ownership

An **attachment** owns one disk for the life of a bidirectional gRPC stream. It
starts as a standby and becomes the primary on promotion. The client controls it over
a Unix socket; the workload uses the returned mount through ordinary file I/O.

An attachment ends in one of three ways, each of which tears its disk down before
its stream closes. The client **closes** it with `Disk::close`. A refused request
or a failed journal operation **fails** it with a status. A daemon drain, or a client
which drops its stream, **cancels** it, giving up its broker calls.

```text
transaction client ── Attach RPC ──► attachment task
       │                                  │
       │ passes mount to workload         │ opens, promotes, prepares, tears down
       ▼                                  ▼
    workload ──► ext4 ──► ublk ──► owner thread + io_uring
                                        │              │
                                   image I/O      live chunks
                                        ▼              ▼
                                   sparse image    attachment's appends ──► Gazette
                                        ▲
                                   image I/O
                                        │
     Gazette ──► standby ──► rebuilder thread's replay
               journal content
```

An attachment is a typestate which mirrors its device's. An `attachment::Standby`
holds a `device::Standby` and a `journal::Read`, its one read of the journal
from its floor, which serves the standby throughout: it reads the journal to the head it
held at `Open`, and tails it, sending what it reads to the device's rebuilder,
which replays it. Its `promote` fences the journal, creating a `journal::Writer`,
with any recovered ACK intent in the fence's own append, and reads through
the head that confirms. Then it begins starting the device,
returning an `attachment::Promoted`, which holds the writer. Nothing a
standby holds outlives it, so a standby which is cancelled, or which its client
leaves before `Promote`, is simply dropped.

A `Promoted` attachment's `serve` is one loop over the client's requests, the
device's live chunks, and the step it has in flight, which its `Phase` holds. It
waits for the device to start, then formats a fresh disk and mounts it, then syncs
it. The loop appends the live chunks of that setup like any other, and commits them
as the attachment's own delta before it replies `Promoted`, so a later attachment
recovers the filesystem rather than formatting another. It then serves each `Prepare`
by starting a sync, and replies `Prepared` once the sync finishes and the delta holds
every live chunk it waited on. Each `Acknowledge` is awaited in its branch until it
lands; for an empty delta it is empty and commits nothing. A second `Prepare` before
the last `Prepared` is acknowledged is refused before its sync, which for a delta
awaiting its acknowledgement would wait on writes that nothing takes. A request is
read only once the last is answered, so a pipelined request waits in the stream until
the phase which serves it. A branch awaits the broker and the client, but never the
device: anything which waits on the device is a step, while the loop takes its live
chunks. An in-flight device start or mount stays in the `Phase`, outside the
future which a failure or cancellation drops, because the device or mount it creates
would outlive the attachment. `Promoted::stop` finishes either before it tears down.
A sync in flight is not waited for: once the device stops, its writes complete.
Nothing in `attachment` spawns a task but blocking work.

The device owns the disk's image from `Open` to teardown. A `device::Standby`'s
rebuilder thread replays the journal content the standby reads, and applies the
chunk of each committed record unchanged. Once the device starts, a recovered
ACK intent where replay applied nothing is refused before the disk would be
formatted: that state was lost. Starting the device ends that rebuild and turns the standby
into a `device::Device`, whose owner thread serves the image as a ublk device,
sending each write, punch, and copy it applies to the attachment as a `LiveChunk`,
at the crate root. The rebuilder owns the disk's `Replay`, so all of replay's work
runs on it, off the async runtime. Both threads apply a chunk the same way. The attachment runs the device's blocking steps on blocking
threads: a device's start and stop, a filesystem sync, the ownership of a recovered
filesystem's root, and the join of a discarded rebuild. `replay` owns what the journal's records mean as they are read
back: their sequencing into deltas, and which deltas committed. It does no I/O but
spill a held delta to a file. `journal` owns the journal's I/O: `open` lists
the journal and begins its `Read`, and a `Writer` fences it, appends to it, and holds the
delta being written. The attachment decides only when to take live chunks, as
Gazette's sequencing of acknowledgements requires.

Paths below are relative to `src/`.

| Path | Start here for |
| --- | --- |
| `client.rs` | `Client`, `Standby`, `Disk`, and `Error`; the caller's transaction and recovery contract. |
| `lib.rs` | Protocol exports, block size, minimum device size, disk content type, the `LiveChunk` which the device sends the promoted attachment and the `Floor` it may name, and attachment status codes. |
| `main.rs` | Process setup: crypto provider, tracing, the open file limit, and the runtime. |
| `daemon.rs` | Flags, configuration, the `Disk` gRPC service and its socket, shared broker credentials, and process drain. |
| `attachment/mod.rs` | The protocol's entry. `run`, which catches a panic or cancel of serving, stops a `Promoted` attachment however serving ends, and replies with the final status, which a failed teardown fails. `serve`, which moves one attachment's stream through its standby as a straight line: `Open`, a `Standby` which replies `Opened` and tails until `Promote`, then promotion, after which `Promoted::serve` takes the stream. |
| `attachment/standby.rs` | `Standby`: open, tailing the journal until a future resolves (for `serve`, the client's next request), promotion, and discard; its reads, which send each response to the device's rebuilder and stop once one reaches a head, failing as soon as the rebuilder does. |
| `attachment/promoted.rs` | `Promoted`: its `Phase`, which holds the step in flight where no failure or cancellation drops it; `start`, which promotion calls to begin starting the device; `serve`, which awaits the device and then loops over the finished step, the client's next request, and the device's live chunks, handing the step to `on_mounted` or `on_synced` and the request to `on_request`; and `stop`. |
| `journal/mod.rs` | `confirm_head`, a zero-byte append which returns the journal's head and registers, for `open` and `Writer::fence`. |
| `journal/reader.rs` | `open`, which lists the journal, resolves its floor, confirms its head with `confirm_head`, leaving a suspended journal suspended, and begins one blocking read from the floor; and `read`, the coroutine which yields that read's responses as a `Read`, each with the offset the read has reached, retrying transient failures and waiting out a suspension, and refusing a gap: only a read still at its floor may skip ahead. |
| `journal/writer.rs` | `Writer`, which owns the producer, clock, and last floor candidate of the records this attachment writes, and the appender they go through, each append checking the journal's author registers. `fence`, which creates it by appending a fence record which installs a new author, followed in the same append by any recovered ACK intent, and returns the head that append confirmed. Its `fence_check` derives what that append requires from a `confirm_head` just before it: the author it read, or with none, its head; `append`, which appends a live chunk, confirming the delta's earlier records before a candidate, and takes any floor it names; `prepare`, which prepares the delta, clocking its acknowledgement after its records, confirms them, and returns its ACK intent and the floor its commit installs; and `acknowledge`, which appends that ACK intent and stores the floor. Beside it, the writer's random producer. |
| `journal/floor.rs` | The replay-from label: `parse`, with which `journal::open` resolves the floor, refusing a label which is not an offset; `change`, the spec change at the listed revision which advances the label, or none where it is already at or past the floor; and `store`, with which `Writer::acknowledge` stores a floor, listing the journal through a watch, retrying a change which lost to another edit of its spec on the listing which follows, and retrying a transient failure with the publisher's backoff for its truncated-at labels, eight attempts in all. |
| `device/mod.rs` | `Standby`: open; the rebuilder thread, which replays the journal content sent to it; start, which also reports whether replay applied nothing; and discard. `Device`: stop. |
| `device/owner.rs` | The owner thread, which arms and serves the ublk queue: tags, the ring, and character-device data copies. It sends each write's live chunks before answering it, and exits on a failure of the image or of a data copy, which it sends after them so the attachment fails with it. |
| `device/image.rs` | `Image`, the sparse image file and its compactor: `replay` of a committed chunk, `serve` of a write or punch and the copies it earns into the live chunks to send, and reads. Each chunk is applied through `chunk::apply`, served and recovered alike, testable without a real device. |
| `device/ublk.rs` | `Control`, which adds, starts, stops, and deletes one device; queue parameters; the request descriptor mapping, which decodes each request into an `Op`; the queue's fetch and commit commands; and the kernel ABI, hand-transcribed from Linux's `include/uapi/linux/ublk_cmd.h`. |
| `device/compactor.rs` | `Compactor`: allocation, bytes since the floor, and the open recovery horizon, as pure state over the chunks applied. Where a horizon opens, which ranges to copy, and which chunk completes it. Each opener is a floor candidate, and the chunk which completes its horizon names it as the floor: the last candidate, or a replayed opener's offset. The first chunk served also names the floor of the last horizon replay completed. `Policy`, re-exported as `device::Policy`, of when a horizon opens and how fast copies complete it, and `Policy::DEFAULT`, which the daemon's flags start from. |
| `device/chunk.rs` | The durable write/punch codec: one chunk per device write, punch, or copy. `encode_write` trims a write's trailing zeroes, and `apply`, which serving and replay share, writes a chunk to the image with `pwrite`, zeroing the rest of its range, or punches it with `fallocate`. |
| `filesystem.rs` | ext4 format, mount, ownership, and `syncfs`, which fails once ext4 has found an error. |
| `replay/mod.rs` | `Replay`, whose `push` sequences each read response's records and hands each committed delta's records, with the journal offsets they begin at, to a callback, which the rebuilder applies. |
| `replay/sequencer.rs`, `replay/buffer.rs` | Pure sequencing decisions; and the `Buffer` of the uncommitted delta, a store of the journal's own frames and their offsets which spills to a file and yields them back in order, which `Replay` decodes. |

The wire contract and durable record schema are in
[`disk.proto`](../../go/protocols/disk/disk.proto). Public modules are `client`
and `daemon`; the implementation modules are private.

## Commit and recovery boundaries

A **delta** contains block changes since the previous commit, including any
compaction copies. `Prepare` confirms its data in Gazette and returns its **ACK
intent**: the opaque bytes of its unappended acknowledgement. The client stores
those bytes atomically with its own state, then sends `Acknowledge`. A delta with
no changes has an empty ACK intent, which is acknowledged too and appends nothing.
After failure, promotion re-acknowledges the saved ACK intent, appending it again
with its fence, as Gazette consumers write the ACK intents of a recovered
checkpoint. It commits the delta it
acknowledges if that never committed, and rolls back any later delta its primary had
begun. The client must never hand back the ACK intent of a delta which another
attachment's records rolled back: a fence excludes the primary it replaces, so no
later record or acknowledgement of that primary lands, and replay would commit
nothing for one. The
client must hand back its
saved ACK intent on every recovery: a replay which applies nothing under one
is how promotion tells lost committed state from a fresh disk. See `Disk::prepare` for the
application flush and write-pause requirements.

The device records each write before it completes, and has no volatile cache, so
any prefix of its stream is a crash-consistent image. The attachment chooses where a
delta finishes: after the filesystem's `syncfs`, it takes everything recorded so far.
The filesystem mounts with `errors=remount-ro`, so an error ext4 finds stops its
writes, including those still in the page cache. The sync then fails, by ext4's
count of its errors, since before Linux 6.13 `syncfs` itself succeeds. Only
one prepared delta may await acknowledgement. The attachment stops consuming
live chunks until the acknowledgement is confirmed, and once the bounded recording
channel fills, the device serves no request until it drains. An acknowledgement is
clocked after its delta's records, and replay commits nothing for one that lands
behind records of a later delta, so the next delta's records wait until it lands.
The attachment commits its own format and mount writes before it replies `Promoted`.

Replay hands only acknowledged deltas to the device, holding an uncommitted delta
in memory and spilling beyond 1 MiB. The same replay serves immediate opens and hot
standbys. A committed delta is read back and applied whole before a later record
is taken. A standby is ready, and replies `Opened`, once it has sent the rebuilder
the content through the head `Open` observed. A standby never wakes a suspended
journal: `Open` confirms the head with an append which keeps the suspension, and a
fully suspended journal holds no content, so its standby is ready at once.
Promotion resumes it just before its fence. A standby reads its journal with one blocking read,
from open through promotion, and tails a journal with no content too: while the
journal is fully suspended, the read waits for the append which resumes it. A `Promote` stops the standby's tail between deltas, never within one.
Promotion then fences, re-acknowledging a recovered ACK intent in the same
append, replays through them, and only then begins starting the device, which ends
its recovery. The client chooses which attachment to promote. A promotion's fence
requires the author an empty append read just before it, or, with none, that
append's head. A promotion fails as fenced if another fence lands in between.
Otherwise concurrent promotions may both succeed, with the later fence winning. See `attachment/` and `replay/mod.rs` for the
ownership and recovery invariants.

Recovery preserves committed filesystem contents. Mounting and ext4 journal
recovery may change bookkeeping, so a recovered block image need not be
byte-for-byte identical. The caller must keep device size stable across attachments;
it is not stored in the journal.

A **recovery horizon** becomes a **recovery floor** once committed changes and
copies cover every allocated block at or after its opening offset. Compaction
trades write amplification for less replay, with no copying while the disk is
idle. A disk's **bytes since the floor** are the encoded size of the chunks applied
after it, which the device measures exactly, both while it recovers and while it
serves. They are a policy target, not a hard bound on transaction size or process
memory. The device opens a horizon at any live chunk once they pass the policy.

The journal speaks of floors rather than horizons. The device marks an opener as a
floor candidate, whose record the writer flags as opening a horizon, and the chunk
which completes the horizon names the opener as the floor. The writer confirms the
delta's earlier records before appending a candidate, so the candidate's offset is
exact, and stores a named floor once the delta which named it commits. Recovery
rebuilds the same pending blocks from a floor. Replay reports the offset of each
committed record, and the device opens a horizon at each flagged one. It carries on a
horizon its rebuild left open, and the chunk which completes it names that opener's
offset as the floor.

A writer holds a floor only in memory from the delta's prepare until the label is
stored, so a failure in between loses it although the delta commits, by its own
acknowledgement or the client's re-acknowledgement. The rebuild then completes that
horizon itself, and the first chunk the device serves names its opener as the floor,
so the next commit stores it. That floor is usually already stored, and a store at or
behind the label changes nothing. See `device/compactor.rs` and `journal/writer.rs`.

## Journal and host responsibilities

The caller creates the journal and owns its specification, including replication,
fragment stores, and compression. The daemon checks none of it, and updates only
the replay-from label, which it only advances. An attachment which can't store a
floor fails. Spec convergence should preserve that label, which bounds
replay and pruning; without it both start from zero. The spec must set no fragment
retention: Gazette deletes fragments by age, which cannot see the floor.

An external pruner may delete fragments wholly below the floor, leaving an age
margin for standbys to finish backfill. A gap after the read leaves its floor fails
the attachment; a replacement starts from the current floor. The daemon deletes no
fragments. A floor label which is not a hex offset fails the attachment. A wrong
offset goes undetected: replay from one at or past the head reads nothing and
formats a fresh disk over committed data, and one copied from another journal may
fail replay or skip committed history. An operator may correct a known-wrong
label only if no fragments below it were deleted.

The host needs Linux 6.5 or later with `ublk` support, for `UBLK_F_USER_COPY`. On
Ubuntu, `ublk_drv` ships in `linux-modules-extra-$(uname -r)`. Before
Linux 6.11, deleting a device waits for every reference to it, so a teardown waits on a
filesystem that a lazy unmount left open, or that a sandbox still re-exports. It also
needs e2fsprogs with `assume_storage_prezeroed`, sysfs at `/sys`, where it reads each
disk's ext4 error count, and an image filesystem supporting
hole punching. Images and spilled deltas are unnamed files, made with `O_TMPFILE`
where the filesystem supports it. Each delta which spills gets its own, released as
replay drains it. Required privileges are `CAP_SYS_ADMIN` for devices and
mounts, `CAP_SYS_RESOURCE` for owner-thread I/O flusher status, and `CAP_CHOWN` for
recovered mount ownership. The daemon also needs access to `/dev/ublk-control` and its
`/dev/ublkc*` nodes. It checks none of this at startup: an attachment fails on what
its host lacks, and on a kernel without user copy, its device may hang starting.

Mounts belong to the Unix-socket peer UID/GID, so clients need no privilege. Socket
access authorizes access to all disk journals in the data plane. The socket is
mode `0666`; restrict traversal of its parent directory. The daemon shares a
refreshing, self-signed broker token scoped to the disk content type. Clients
supply neither broker endpoints nor credentials; key rotation requires a restart.

`SIGTERM` and `SIGINT` drain the daemon: it stops accepting attachments and
cancels existing ones. Teardown of a promoted disk follows the end of its serving
loop, which abandons its appends and drops its receiver of live chunks, so the
device applies later writes unsent. It then finishes a device start or mount still in
flight, unmounts, and stops and deletes the device. This order is required because
unmount writes, and a device stop waits for every request in flight. A failed unmount
still deletes the device, since its writes no longer reach the journal, and fails the
attachment, so the client's `close` reports it. A standby which fails is discarded,
joining its rebuilder thread, whose failure, if any, is the attachment's. A
standby which is cancelled, or which its client leaves, is dropped: its rebuilder
thread applies what it holds and exits on its own. See `attachment/` and `device/mod.rs`.
The daemon waits for every teardown, so the supervisor's stop timeout bounds a
drain.

A daemon killed without draining leaves its disks behind. The kernel stops their devices,
so their I/O fails, but the devices and their mounts remain until the host reboots. The
next daemon on the same `--uds-path` replaces the socket the killed one left. A daemon
started on a path that another still serves takes the path over, and the other keeps
only the attachments it already holds.

Deployment owns capacity and placement: the daemon reserves no space and limits
no disk count. Every attachment holds an image and a thread from `Open`: a rebuilder
thread while it stands by, and an owner thread and a kernel device once promoted.
Mounts must remain visible to the process passing them into a
sandbox, which must apply `nodev,nosuid,noexec` when re-exporting them. The daemon's
help describes its configuration.

## Examples and tests

`examples/` covers commit and reopen, multi-disk transactions, and hot standby
promotion. `examples/demo-services.sh` runs their local services until Ctrl-C;
each example documents its invocation.

`tests/` exercises the shipped binary through the client, ordinary filesystem I/O,
and an independent Gazette client. `tests/support/` provides the data-plane
`Fixture`, built on `e2e-support`, privileged `Daemon` child, raw `Attachment` stream, and expected file
`Tree`. Cases compare recovered files with the workload's tree rather than a second
implementation of replay.

Inline unit and property tests cover internal modules in memory, without a data
plane, devices, or privilege, each building the records it needs. Where a
floor lands in the journal depends on the offsets brokers confirm, so
`tests/pruning.rs` checks it: each stored floor is a candidate's record, and a
recovery installs the floors its replay found.

```console
mise exec -- cargo nextest run -p disk-daemon  # unit tests
mise run ci:disk-daemon-e2e-run                # unit, then integration tests
```

The integration suite serves real `ublk` devices, so the default nextest profile
excludes it, and it runs under its own profile and CI workflow
(`disk-daemon-test.yaml`). It runs the daemon through `sudo -n`, leaving Cargo
unprivileged. It runs its tests one at a time, since each checks the whole host
for leaked devices and mounts. The host prerequisites above still apply.
